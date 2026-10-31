package com.vineyardfinance.sensors.auth

import com.vineyardfinance.sensors.network.HttpJson
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException

@OptIn(ExperimentalCoroutinesApi::class)
class EnrollmentManagerTest {
    private val token = "a".repeat(43)

    @Test fun successfulSubmissionUsesOneExchangeAndExposesNoToken() {
        val scope = TestScope(StandardTestDispatcher())
        val fake = FakeExchange()
        val manager = EnrollmentManager(
            fake,
            scope,
            newExchangeId = { "11111111-1111-4111-8111-111111111111" },
        )

        manager.submit(token)
        scope.advanceUntilIdle()

        assertTrue(manager.state.value is EnrollmentState.Succeeded)
        assertEquals(listOf(token to "11111111-1111-4111-8111-111111111111"), fake.attempts)
        assertFalse(manager.state.value.toString().contains(token))
        assertFalse(manager.state.value.toString().contains("22222222-2222-4222-8222-222222222222"))
    }

    @Test fun managerStartsIdleAndNeverManufacturesSuccessFromStoredCredentialStatus() {
        val manager = EnrollmentManager(FakeExchange(), TestScope(StandardTestDispatcher()))

        assertSame(EnrollmentState.Idle, manager.state.value)
    }

    @Test fun onlyTheExactRenderedSuccessCanBeAcknowledged() {
        val scope = TestScope(StandardTestDispatcher())
        val fake = FakeExchange()
        val manager = EnrollmentManager(fake, scope)

        manager.submit(token)
        scope.advanceUntilIdle()
        val first = manager.state.value as EnrollmentState.Succeeded
        manager.submit("b".repeat(43))
        scope.advanceUntilIdle()
        val second = manager.state.value as EnrollmentState.Succeeded

        assertNotSame(first, second)
        manager.acknowledgeSuccess(first)
        assertSame(second, manager.state.value)
        manager.acknowledgeSuccess(second)
        assertSame(EnrollmentState.Idle, manager.state.value)
    }

    @Test fun duplicateDeliveryOfConsumedLinkCannotStartAChangedReplay() {
        val scope = TestScope(StandardTestDispatcher())
        val fake = FakeExchange()
        var generated = 0
        val manager = EnrollmentManager(
            fake,
            scope,
            newExchangeId = {
                "00000000-0000-4000-8000-${(++generated).toString().padStart(12, '0')}"
            },
        )

        manager.submit(token)
        manager.submit(token)
        scope.advanceUntilIdle()

        assertTrue(manager.state.value is EnrollmentState.Succeeded)
        assertEquals(1, fake.attempts.size)
    }

    @Test fun ambiguousFailureRetriesSameTokenAndExchangeId() {
        val scope = TestScope(StandardTestDispatcher())
        val fake = FakeExchange().apply { failures += IOException("offline") }
        var generated = 0
        val manager = EnrollmentManager(
            fake,
            scope,
            newExchangeId = {
                "00000000-0000-4000-8000-${(++generated).toString().padStart(12, '0')}"
            },
        )

        manager.submit(token)
        scope.advanceUntilIdle()
        assertTrue((manager.state.value as EnrollmentState.Failed).canRetry)
        manager.retry()
        scope.advanceUntilIdle()

        assertTrue(manager.state.value is EnrollmentState.Succeeded)
        assertEquals(2, fake.attempts.size)
        assertEquals(fake.attempts[0], fake.attempts[1])
        assertEquals(1, generated)
    }

    @Test fun terminalHttpFailureDropsAttemptAndRequiresFreshLink() {
        val scope = TestScope(StandardTestDispatcher())
        val fake = FakeExchange().apply {
            failures += HttpJson.HttpStatusException(
                400,
                null,
                EnrollmentManager.ENROLLMENT_LINK_TERMINAL_CODE,
            )
        }
        val manager = EnrollmentManager(fake, scope)

        manager.submit(token)
        scope.advanceUntilIdle()
        val failed = manager.state.value as EnrollmentState.Failed
        assertFalse(failed.canRetry)
        assertTrue(failed.message.contains("new link"))
        manager.retry()
        scope.advanceUntilIdle()
        assertEquals(1, fake.attempts.size)
    }

    @Test fun unknown400PreservesExactTokenAndExchangeIdForRetry() {
        val scope = TestScope(StandardTestDispatcher())
        val fake = FakeExchange().apply {
            failures += HttpJson.HttpStatusException(400, null, "invalid_request")
        }
        val manager = EnrollmentManager(
            fake,
            scope,
            newExchangeId = { "44444444-4444-4444-8444-444444444444" },
        )

        manager.submit(token)
        scope.advanceUntilIdle()
        assertTrue((manager.state.value as EnrollmentState.Failed).canRetry)
        manager.retry()
        scope.advanceUntilIdle()

        assertTrue(manager.state.value is EnrollmentState.Succeeded)
        assertEquals(2, fake.attempts.size)
        assertEquals(fake.attempts[0], fake.attempts[1])
    }

    @Test fun rateLimitAndServerFailureRemainRetryable() {
        listOf(408, 429, 500).forEach { status ->
            val scope = TestScope(StandardTestDispatcher())
            val fake = FakeExchange().apply { failures += HttpJson.HttpStatusException(status, null) }
            val manager = EnrollmentManager(fake, scope)
            manager.submit(token)
            scope.advanceUntilIdle()
            assertTrue("status=$status", (manager.state.value as EnrollmentState.Failed).canRetry)
        }
    }

    @Test fun unexpectedFailureWithoutMessageUsesSafeFallback() {
        val scope = TestScope(StandardTestDispatcher())
        val fake = FakeExchange().apply { failures += IllegalStateException() }
        val manager = EnrollmentManager(fake, scope)

        manager.submit(token)
        scope.advanceUntilIdle()

        assertEquals("Connection failed", (manager.state.value as EnrollmentState.Failed).message)
    }

    @Test fun nativeLoginDeduplicatesAndRetriesExactExchangeWithoutPassword() {
        val scope = TestScope(StandardTestDispatcher())
        val exchange = FakeExchange().apply { failures += IOException("lost response") }
        var logins = 0
        val manager = EnrollmentManager(exchange, scope, passwordEnrollment = PasswordEnrollment { email, password ->
            assertEquals("test@example.com", email)
            assertEquals("secret", password)
            logins++
            token
        })
        manager.signIn("test@example.com", "secret")
        manager.signIn("test@example.com", "secret")
        scope.advanceUntilIdle()
        assertEquals(1, logins)
        assertTrue((manager.state.value as EnrollmentState.Failed).canRetry)
        manager.retry()
        scope.advanceUntilIdle()
        assertTrue(manager.state.value is EnrollmentState.Succeeded)
        assertEquals(exchange.attempts[0], exchange.attempts[1])
        assertEquals(1, logins)
    }

    @Test fun passwordFailuresAreSafeAndRequireFreshSignIn() {
        val errors = listOf(
            HttpJson.HttpStatusException(401, "secret"), HttpJson.HttpStatusException(429, "secret"),
            HttpJson.HttpStatusException(500, "secret"), IOException("secret"), IllegalStateException("secret"),
        )
        for (error in errors) {
            val scope = TestScope(StandardTestDispatcher())
            val fake = FakeExchange()
            val manager = EnrollmentManager(fake, scope, passwordEnrollment = PasswordEnrollment { _, _ -> throw error })
            manager.signIn("test@example.com", "secret")
            scope.advanceUntilIdle()
            val state = manager.state.value as EnrollmentState.Failed
            assertFalse(state.canRetry)
            assertFalse(state.message.contains("secret"))
            manager.retry()
            scope.advanceUntilIdle()
            assertTrue(fake.attempts.isEmpty())
        }
    }

    @Test fun logoutClearsRetryStateOnlyAfterCredentialRemovalSucceeds() = kotlinx.coroutines.test.runTest {
        val fake = FakeExchange().apply { failures += IOException("offline") }
        val manager = EnrollmentManager(fake, this)
        manager.submit(token)
        advanceUntilIdle()
        assertTrue(runCatching { manager.logOut { error("pending uploads") } }.isFailure)
        assertTrue(manager.state.value is EnrollmentState.Failed)
        var cleared = false
        manager.logOut { cleared = true }
        assertTrue(cleared)
        assertSame(EnrollmentState.Idle, manager.state.value)
        manager.retry()
        advanceUntilIdle()
        assertEquals(1, fake.attempts.size)
    }

    private class FakeExchange : EnrollmentExchange {
        val attempts = mutableListOf<Pair<String, String>>()
        val failures = ArrayDeque<Exception>()
        override suspend fun exchange(enrollmentToken: String, exchangeId: String): EnrollmentResult {
            attempts += enrollmentToken to exchangeId
            if (failures.isNotEmpty()) throw failures.removeFirst()
            return EnrollmentResult("22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333")
        }
    }
}
