package com.vineyardfinance.sensors.collector

/**
 * Publishes at most one identity token until permanently closed.
 *
 * Installation and its bounded activation callback share the owner monitor, so
 * close cannot observe a half-published resource. Exact-token release prevents
 * stale runtime cleanup from detaching a replacement.
 */
internal class LifecycleTokenOwner<T : Any> {
    private var current: T? = null
    private var closed = false

    @Synchronized
    fun isOpen(): Boolean = !closed

    @Synchronized
    fun current(): T? = current

    @Synchronized
    fun isCurrent(token: T): Boolean = current === token

    @Synchronized
    fun install(token: T, onInstalled: (T) -> Unit = {}): Boolean {
        if (closed) return false
        check(current == null) { "lifecycle token is already installed" }
        current = token
        try {
            onInstalled(token)
        } catch (error: Throwable) {
            current = null
            throw error
        }
        return true
    }

    fun installOrCleanup(
        token: T,
        onInstalled: (T) -> Unit = {},
        cleanupRejected: (T) -> Unit,
    ): Boolean {
        val installed = try {
            install(token, onInstalled)
        } catch (error: Throwable) {
            cleanupRejected(token)
            throw error
        }
        if (!installed) cleanupRejected(token)
        return installed
    }

    @Synchronized
    fun release(token: T): T? {
        if (current !== token) return null
        current = null
        return token
    }

    /**
     * Cleans and releases [token] as one owner-serialized operation.
     *
     * Cleanup intentionally runs while holding the owner monitor. A concurrent
     * close therefore cannot select the same token between a platform cleanup
     * and its release. If cleanup throws, [current] remains unchanged so the
     * exact token can be retried by the closing lifecycle path.
     */
    fun releaseAfterCleanup(token: T, cleanup: (T) -> Unit): T? =
        cleanupOwned(expected = token, closeOwner = false, cleanup = cleanup)

    /**
     * Permanently closes the owner and atomically cleans its current token.
     * A failed cleanup leaves that token current (but the owner closed), which
     * permits an already-running exact-token cleanup path to retry it.
     */
    fun closeAfterCleanup(cleanup: (T) -> Unit): T? =
        cleanupOwned(expected = null, closeOwner = true, cleanup = cleanup)

    @Synchronized
    private fun cleanupOwned(
        expected: T?,
        closeOwner: Boolean,
        cleanup: (T) -> Unit,
    ): T? {
        if (closeOwner) closed = true
        val token = current ?: return null
        if (expected != null && token !== expected) return null
        cleanup(token)
        current = null
        return token
    }

    @Synchronized
    fun close(): T? {
        closed = true
        val token = current
        current = null
        return token
    }
}

internal fun <T : Any> lifecycleTokenActiveAndCurrent(
    owner: LifecycleTokenOwner<T>,
    token: T,
    runtimeActive: Boolean,
): Boolean = runtimeActive && owner.isCurrent(token)
