package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class LifecycleTokenOwnerTest {
    @Test
    fun closeBeforeInstallRejectsWithoutPublicationAndCleansResource() {
        val owner = LifecycleTokenOwner<FakeResource>()
        val resource = FakeResource()
        val activationCalled = AtomicBoolean(false)
        owner.close()

        val installed = owner.installOrCleanup(
            resource,
            onInstalled = { activationCalled.set(true) },
            cleanupRejected = FakeResource::cleanup,
        )

        assertFalse(installed)
        assertFalse(activationCalled.get())
        assertEquals(1, resource.cleanupCount)
        assertNull(owner.current())
        assertFalse(owner.isOpen())
    }

    @Test
    fun successfulInstallDoesNotRunRejectedCleanup() {
        val owner = LifecycleTokenOwner<FakeResource>()
        val resource = FakeResource()

        assertTrue(owner.installOrCleanup(resource, cleanupRejected = FakeResource::cleanup))

        assertSame(resource, owner.current())
        assertEquals(0, resource.cleanupCount)
        assertTrue(lifecycleTokenActiveAndCurrent(owner, resource, runtimeActive = true))
    }

    @Test
    fun installActivationFinishesBeforeConcurrentCloseReturnsToken() {
        val owner = LifecycleTokenOwner<Any>()
        val token = Any()
        val activationEntered = CountDownLatch(1)
        val finishActivation = CountDownLatch(1)
        val closeStarted = CountDownLatch(1)
        val installResult = AtomicBoolean(false)
        val closedToken = AtomicReference<Any?>()
        val installThread = Thread {
            installResult.set(
                owner.install(token) {
                    activationEntered.countDown()
                    check(finishActivation.await(2, TimeUnit.SECONDS))
                },
            )
        }
        installThread.start()
        check(activationEntered.await(2, TimeUnit.SECONDS))
        val closeThread = Thread {
            closeStarted.countDown()
            closedToken.set(owner.close())
        }
        closeThread.start()
        check(closeStarted.await(2, TimeUnit.SECONDS))
        assertNull(closedToken.get())

        finishActivation.countDown()
        installThread.join(2_000)
        closeThread.join(2_000)

        assertTrue(installResult.get())
        assertSame(token, closedToken.get())
        assertNull(owner.current())
        assertFalse(owner.isOpen())
    }

    @Test
    fun closeWinsWhileAcquisitionIsParkedBeforeInstallBoundary() {
        val owner = LifecycleTokenOwner<FakeResource>()
        val resource = FakeResource()
        val acquisitionReady = CountDownLatch(1)
        val finishAcquisition = CountDownLatch(1)
        val installed = AtomicBoolean(true)
        val installer = Thread {
            acquisitionReady.countDown()
            check(finishAcquisition.await(2, TimeUnit.SECONDS))
            installed.set(owner.installOrCleanup(resource, cleanupRejected = FakeResource::cleanup))
        }
        installer.start()
        check(acquisitionReady.await(2, TimeUnit.SECONDS))

        assertNull(owner.close())
        finishAcquisition.countDown()
        installer.join(2_000)

        assertFalse(installed.get())
        assertEquals(1, resource.cleanupCount)
        assertNull(owner.current())
    }

    @Test
    fun normalExactReleaseAllowsReplacementUntilClose() {
        val owner = LifecycleTokenOwner<Any>()
        val first = Any()
        val second = Any()
        assertTrue(owner.install(first))
        assertTrue(owner.isCurrent(first))

        assertSame(first, owner.release(first))
        assertTrue(owner.isOpen())
        assertTrue(owner.install(second))
        assertSame(second, owner.current())
        assertSame(second, owner.close())

        val rejected = FakeResource()
        val closedOwner = LifecycleTokenOwner<FakeResource>()
        closedOwner.close()
        assertFalse(closedOwner.installOrCleanup(rejected, cleanupRejected = FakeResource::cleanup))
        assertEquals(1, rejected.cleanupCount)
    }

    @Test
    fun staleReleaseCannotDetachCurrentToken() {
        val owner = LifecycleTokenOwner<Any>()
        val current = Any()
        val stale = Any()
        owner.install(current)

        assertNull(owner.release(stale))
        var staleCleanupCalled = false
        assertNull(
            owner.releaseAfterCleanup(stale) {
                staleCleanupCalled = true
            },
        )
        assertFalse(staleCleanupCalled)
        assertFalse(owner.isCurrent(stale))
        assertFalse(lifecycleTokenActiveAndCurrent(owner, stale, runtimeActive = true))
        assertTrue(owner.isCurrent(current))
        assertSame(current, owner.current())
        assertSame(current, owner.release(current))
        assertNull(owner.release(current))
    }

    @Test
    fun installedSetupResumingAfterDestroyCannotPublishLifecycleEvent() {
        val owner = LifecycleTokenOwner<FakeResource>()
        val resource = FakeResource()
        val runtimeActive = AtomicBoolean(true)
        val installFinished = CountDownLatch(1)
        val resumeSetup = CountDownLatch(1)
        val setupFinished = CountDownLatch(1)
        val publishedEvents = AtomicInteger()
        val setup = Thread {
            check(owner.install(resource))
            installFinished.countDown()
            check(resumeSetup.await(2, TimeUnit.SECONDS))
            if (lifecycleTokenActiveAndCurrent(owner, resource, runtimeActive.get())) {
                publishedEvents.incrementAndGet()
            }
            setupFinished.countDown()
        }
        setup.start()
        check(installFinished.await(2, TimeUnit.SECONDS))

        runtimeActive.set(false)
        owner.close()?.cleanup()
        resumeSetup.countDown()

        check(setupFinished.await(2, TimeUnit.SECONDS))
        setup.join(2_000)
        assertEquals(0, publishedEvents.get())
        assertEquals(1, resource.cleanupCount)
        assertNull(owner.current())
        assertFalse(lifecycleTokenActiveAndCurrent(owner, resource, runtimeActive.get()))
    }

    @Test
    fun normalCancellationAfterWakeInstallCleansOnceAndAllowsReplacementWithoutStaleEvent() {
        val owner = LifecycleTokenOwner<FakeResource>()
        val wakeLease = FakeResource()
        assertTrue(owner.install(wakeLease))

        // Successful setup returned this exact token before cancellation could
        // throw, so normal runtime finally can always release it.
        val runtimeWakeLease = wakeLease
        val released = owner.release(runtimeWakeLease)
        released?.cleanup()

        assertSame(wakeLease, released)
        assertEquals(1, wakeLease.cleanupCount)
        assertTrue(owner.isOpen())
        assertNull(owner.current())

        val replacement = FakeResource()
        assertTrue(owner.install(replacement))
        assertFalse(lifecycleTokenActiveAndCurrent(owner, wakeLease, runtimeActive = true))
        assertTrue(lifecycleTokenActiveAndCurrent(owner, replacement, runtimeActive = true))
        assertNull(owner.release(wakeLease))
        assertEquals(1, wakeLease.cleanupCount)
    }

    @Test
    fun normalCancellationAfterNetworkInstallCleansOnceAndAllowsReplacementWithoutStaleCallback() {
        val owner = LifecycleTokenOwner<FakeResource>()
        val networkCallback = FakeResource()
        assertTrue(owner.install(networkCallback))

        // A callback dispatched after normal runtime cleanup must lose identity
        // even though the permanently reusable service owner remains open.
        val runtimeCallback = networkCallback
        owner.release(runtimeCallback)?.cleanup()
        assertTrue(owner.isOpen())
        assertNull(owner.current())
        val replacement = FakeResource()
        assertTrue(owner.install(replacement))
        val staleCallbackCount = AtomicInteger()
        if (lifecycleTokenActiveAndCurrent(owner, networkCallback, runtimeActive = true)) {
            staleCallbackCount.incrementAndGet()
        }

        assertEquals(1, networkCallback.cleanupCount)
        assertEquals(0, staleCallbackCount.get())
        assertTrue(owner.isOpen())
        assertSame(replacement, owner.current())
        assertNull(owner.release(networkCallback))
        assertEquals(1, networkCallback.cleanupCount)
    }

    @Test
    fun runtimeCleanupWinningDestroyRaceCleansExactlyOnce() {
        val owner = LifecycleTokenOwner<Any>()
        val token = Any()
        owner.install(token)
        val cleanupEntered = CountDownLatch(1)
        val allowCleanup = CountDownLatch(1)
        val destroyStarted = CountDownLatch(1)
        val destroyFinished = CountDownLatch(1)
        val cleanupAttempts = AtomicInteger()
        val runtimeReleased = AtomicReference<Any?>()
        val destroyReleased = AtomicReference<Any?>()
        val runtimeFailure = AtomicReference<Throwable?>()
        val destroyFailure = AtomicReference<Throwable?>()

        val runtime = Thread {
            runCatching {
                runtimeReleased.set(
                    owner.releaseAfterCleanup(token) { exact ->
                        assertSame(token, exact)
                        cleanupAttempts.incrementAndGet()
                        cleanupEntered.countDown()
                        check(allowCleanup.await(2, TimeUnit.SECONDS))
                    },
                )
            }.onFailure(runtimeFailure::set)
        }
        runtime.start()
        check(cleanupEntered.await(2, TimeUnit.SECONDS))
        val destroy = Thread {
            destroyStarted.countDown()
            runCatching {
                destroyReleased.set(owner.closeAfterCleanup { cleanupAttempts.incrementAndGet() })
            }.onFailure(destroyFailure::set)
            destroyFinished.countDown()
        }
        destroy.start()
        check(destroyStarted.await(2, TimeUnit.SECONDS))
        assertFalse(destroyFinished.await(100, TimeUnit.MILLISECONDS))

        allowCleanup.countDown()
        runtime.join(2_000)
        destroy.join(2_000)

        assertFalse(runtime.isAlive)
        assertFalse(destroy.isAlive)
        assertNull(runtimeFailure.get())
        assertNull(destroyFailure.get())
        assertSame(token, runtimeReleased.get())
        assertNull(destroyReleased.get())
        assertEquals(1, cleanupAttempts.get())
        assertNull(owner.current())
        assertFalse(owner.isOpen())
    }

    @Test
    fun destroyCleanupWinningRuntimeRaceCleansExactlyOnce() {
        val owner = LifecycleTokenOwner<Any>()
        val token = Any()
        owner.install(token)
        val cleanupEntered = CountDownLatch(1)
        val allowCleanup = CountDownLatch(1)
        val runtimeStarted = CountDownLatch(1)
        val runtimeFinished = CountDownLatch(1)
        val cleanupAttempts = AtomicInteger()
        val runtimeReleased = AtomicReference<Any?>()
        val destroyReleased = AtomicReference<Any?>()
        val runtimeFailure = AtomicReference<Throwable?>()
        val destroyFailure = AtomicReference<Throwable?>()

        val destroy = Thread {
            runCatching {
                destroyReleased.set(
                    owner.closeAfterCleanup { exact ->
                        assertSame(token, exact)
                        cleanupAttempts.incrementAndGet()
                        cleanupEntered.countDown()
                        check(allowCleanup.await(2, TimeUnit.SECONDS))
                    },
                )
            }.onFailure(destroyFailure::set)
        }
        destroy.start()
        check(cleanupEntered.await(2, TimeUnit.SECONDS))
        val runtime = Thread {
            runtimeStarted.countDown()
            runCatching {
                runtimeReleased.set(owner.releaseAfterCleanup(token) { cleanupAttempts.incrementAndGet() })
            }.onFailure(runtimeFailure::set)
            runtimeFinished.countDown()
        }
        runtime.start()
        check(runtimeStarted.await(2, TimeUnit.SECONDS))
        assertFalse(runtimeFinished.await(100, TimeUnit.MILLISECONDS))

        allowCleanup.countDown()
        destroy.join(2_000)
        runtime.join(2_000)

        assertFalse(destroy.isAlive)
        assertFalse(runtime.isAlive)
        assertNull(destroyFailure.get())
        assertNull(runtimeFailure.get())
        assertSame(token, destroyReleased.get())
        assertNull(runtimeReleased.get())
        assertEquals(1, cleanupAttempts.get())
        assertNull(owner.current())
        assertFalse(owner.isOpen())
    }

    @Test
    fun failedRuntimeCleanupRetainsTokenForConcurrentDestroyRetry() {
        val owner = LifecycleTokenOwner<Any>()
        val token = Any()
        owner.install(token)
        val firstCleanupEntered = CountDownLatch(1)
        val allowFirstFailure = CountDownLatch(1)
        val destroyStarted = CountDownLatch(1)
        val cleanupAttempts = AtomicInteger()
        val runtimeFailure = AtomicReference<Throwable?>()
        val destroyFailure = AtomicReference<Throwable?>()
        val destroyReleased = AtomicReference<Any?>()

        val runtime = Thread {
            runCatching {
                owner.releaseAfterCleanup(token) { exact ->
                    assertSame(token, exact)
                    cleanupAttempts.incrementAndGet()
                    firstCleanupEntered.countDown()
                    check(allowFirstFailure.await(2, TimeUnit.SECONDS))
                    error("first unregister failed")
                }
            }.onFailure(runtimeFailure::set)
        }
        runtime.start()
        check(firstCleanupEntered.await(2, TimeUnit.SECONDS))
        val destroy = Thread {
            destroyStarted.countDown()
            runCatching {
                destroyReleased.set(
                    owner.closeAfterCleanup { exact ->
                        assertSame(token, exact)
                        cleanupAttempts.incrementAndGet()
                    },
                )
            }.onFailure(destroyFailure::set)
        }
        destroy.start()
        check(destroyStarted.await(2, TimeUnit.SECONDS))

        allowFirstFailure.countDown()
        runtime.join(2_000)
        destroy.join(2_000)

        assertFalse(runtime.isAlive)
        assertFalse(destroy.isAlive)
        assertEquals("first unregister failed", runtimeFailure.get()?.message)
        assertNull(destroyFailure.get())
        assertSame(token, destroyReleased.get())
        assertEquals(2, cleanupAttempts.get())
        assertNull(owner.current())
        assertFalse(owner.isOpen())
    }

    @Test
    fun failedDestroyCleanupRetainsClosedTokenForConcurrentRuntimeRetry() {
        val owner = LifecycleTokenOwner<Any>()
        val token = Any()
        owner.install(token)
        val firstCleanupEntered = CountDownLatch(1)
        val allowFirstFailure = CountDownLatch(1)
        val runtimeStarted = CountDownLatch(1)
        val cleanupAttempts = AtomicInteger()
        val runtimeFailure = AtomicReference<Throwable?>()
        val destroyFailure = AtomicReference<Throwable?>()
        val runtimeReleased = AtomicReference<Any?>()

        val destroy = Thread {
            runCatching {
                owner.closeAfterCleanup { exact ->
                    assertSame(token, exact)
                    cleanupAttempts.incrementAndGet()
                    firstCleanupEntered.countDown()
                    check(allowFirstFailure.await(2, TimeUnit.SECONDS))
                    error("destroy unregister failed")
                }
            }.onFailure(destroyFailure::set)
        }
        destroy.start()
        check(firstCleanupEntered.await(2, TimeUnit.SECONDS))
        val runtime = Thread {
            runtimeStarted.countDown()
            runCatching {
                runtimeReleased.set(
                    owner.releaseAfterCleanup(token) { exact ->
                        assertSame(token, exact)
                        cleanupAttempts.incrementAndGet()
                    },
                )
            }.onFailure(runtimeFailure::set)
        }
        runtime.start()
        check(runtimeStarted.await(2, TimeUnit.SECONDS))

        allowFirstFailure.countDown()
        destroy.join(2_000)
        runtime.join(2_000)

        assertFalse(destroy.isAlive)
        assertFalse(runtime.isAlive)
        assertEquals("destroy unregister failed", destroyFailure.get()?.message)
        assertNull(runtimeFailure.get())
        assertSame(token, runtimeReleased.get())
        assertEquals(2, cleanupAttempts.get())
        assertNull(owner.current())
        assertFalse(owner.isOpen())
    }

    @Test
    fun activationFailureRollsBackPublication() {
        val owner = LifecycleTokenOwner<FakeResource>()
        val token = FakeResource()
        val failure = runCatching {
            owner.installOrCleanup(
                token,
                onInstalled = { error("activation failed") },
                cleanupRejected = FakeResource::cleanup,
            )
        }.exceptionOrNull()

        assertEquals("activation failed", failure?.message)
        assertEquals(1, token.cleanupCount)
        assertNull(owner.current())
        assertTrue(owner.isOpen())
        assertTrue(owner.install(token))
    }

    @Test(expected = IllegalStateException::class)
    fun duplicateInstallIsRejected() {
        val owner = LifecycleTokenOwner<Any>()
        owner.install(Any())
        owner.install(Any())
    }

    private class FakeResource {
        var cleanupCount = 0

        fun cleanup() {
            cleanupCount += 1
        }
    }
}
