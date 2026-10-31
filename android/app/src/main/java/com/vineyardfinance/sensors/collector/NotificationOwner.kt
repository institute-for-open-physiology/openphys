package com.vineyardfinance.sensors.collector

/**
 * Owns the single foreground-notification decision for the H10 collector.
 *
 * The Android service feeds complete [Snapshot] values from one serialized
 * [CollectorStatus] observer. Applying the whole snapshot before making a
 * decision guarantees that one status emission produces at most one call to
 * [onNotify], even when state, failure, freshness, and telemetry all change.
 *
 * This class has no Android framework dependency and is host-JVM testable. Its
 * public methods must be called serially; [SerializedCollectorStatus] provides
 * that guarantee in production.
 */
class NotificationOwner(
    private val onNotify: (NotificationContent) -> Unit,
    private val telemetryMinIntervalMs: Long = DEFAULT_TELEMETRY_INTERVAL_MS,
    private val clock: () -> Long = { monotonicClockMs() },
    initialSnapshot: Snapshot = Snapshot(),
) {
    data class Snapshot(
        val state: String = "Stopped",
        val detail: String = "Collection is disabled",
        val lastError: String? = null,
        val backlogRecords: Long = 0,
        val backlogBytes: Long = 0,
        val backlogKnown: Boolean = false,
        val backlogStatusError: String? = null,
        val batteryPercent: Int? = null,
        val rssiDbm: Int? = null,
        val connected: Boolean = false,
        val ecgAgeMs: Long? = null,
        val accAgeMs: Long? = null,
        val hrAgeMs: Long? = null,
    )

    private var current = initialSnapshot
    private var lastNotifiedContent: NotificationContent? = null
    private var lastTelemetryNotifyTimeMs: Long = Long.MIN_VALUE + telemetryMinIntervalMs

    init {
        require(telemetryMinIntervalMs >= 0) { "telemetry interval must not be negative" }
    }

    /** Current content, used by the synchronous foreground-service start. */
    fun content(): NotificationContent = buildContent(current)

    /**
     * Records that [content] was posted by `startForeground`. This prevents the
     * initial status subscription from redundantly enqueueing identical content.
     */
    fun markCurrentContentPosted() {
        lastNotifiedContent = content()
    }

    /**
     * Atomically applies one status snapshot and makes at most one notify
     * decision. State/detail/error transitions are urgent. Telemetry and
     * freshness-only transitions are coalesced to the fixed cadence.
     */
    fun onSnapshot(next: Snapshot) {
        val urgent = next.state != current.state ||
            next.detail != current.detail ||
            next.lastError != current.lastError ||
            next.backlogStatusError != current.backlogStatusError
        current = next

        val content = buildContent(next)
        if (content == lastNotifiedContent) return

        if (urgent) {
            publish(content, telemetryTimer = false)
            return
        }

        val now = clock()
        if (now >= lastTelemetryNotifyTimeMs + telemetryMinIntervalMs) {
            publish(content, telemetryTimer = true, now = now)
        }
    }

    private fun buildContent(snapshot: Snapshot): NotificationContent =
        NotificationContent(
            title = "H10: ${snapshot.state}",
            text = buildText(snapshot),
            showPairingAction = snapshot.state == "Pairing required",
        )

    private fun buildText(snapshot: Snapshot): String {
        val visibleDetail = when {
            snapshot.lastError == null -> snapshot.detail
            snapshot.lastError == snapshot.detail -> "⚠ ${snapshot.detail}"
            else -> "⚠ ${snapshot.lastError} · ${snapshot.detail}"
        }
        val extras = buildString {
            snapshot.batteryPercent?.let { append(" · battery $it%") }
            snapshot.rssiDbm?.let { append(" · RSSI ${it}dBm") }
        }
        val backlog = when {
            snapshot.backlogKnown ->
                "queued ${snapshot.backlogRecords} records / ${snapshot.backlogBytes} bytes"
            snapshot.backlogStatusError != null ->
                "queue status unavailable: ${snapshot.backlogStatusError}"
            else -> "queue status checking"
        }
        return "$visibleDetail · $backlog$extras${freshnessText(snapshot)}"
    }

    private fun freshnessText(snapshot: Snapshot): String {
        val parts = buildList {
            snapshot.ecgAgeMs?.let { add("ECG ${ageLabel(it)}") }
            snapshot.accAgeMs?.let { add("ACC ${ageLabel(it)}") }
            snapshot.hrAgeMs?.let { add("HR ${ageLabel(it)}") }
        }
        return if (parts.isEmpty()) "" else " · ${parts.joinToString(" ")}"
    }

    private fun ageLabel(ageMs: Long): String =
        if (ageMs < 1_000) "${ageMs}ms" else "${ageMs / 1_000}s"

    private fun publish(
        content: NotificationContent,
        telemetryTimer: Boolean,
        now: Long = clock(),
    ) {
        lastNotifiedContent = content
        if (telemetryTimer) lastTelemetryNotifyTimeMs = now
        onNotify(content)
    }

    companion object {
        const val DEFAULT_TELEMETRY_INTERVAL_MS = 2_000L

        /** Monotonic host/Android clock used only for notification throttling. */
        fun monotonicClockMs(): Long = System.nanoTime() / 1_000_000L
    }
}

/** Pure atomic mapping from one collector snapshot to one owner snapshot. */
class CollectorNotificationMapper(
    private val owner: NotificationOwner,
    private val wallClockMs: () -> Long = { System.currentTimeMillis() },
) {
    fun onSnapshot(snapshot: CollectorSnapshot) {
        owner.onSnapshot(toOwnerSnapshot(snapshot, wallClockMs()))
    }

    companion object {
        fun toOwnerSnapshot(snapshot: CollectorSnapshot, nowMs: Long): NotificationOwner.Snapshot {
            fun age(unixNs: Long?): Long? =
                unixNs?.let { (nowMs - it / 1_000_000L).coerceAtLeast(0) }

            return NotificationOwner.Snapshot(
                state = snapshot.state,
                detail = snapshot.detail,
                lastError = snapshot.lastError,
                backlogRecords = snapshot.backlogRecords,
                backlogBytes = snapshot.backlogBytes,
                backlogKnown = snapshot.backlogKnown,
                backlogStatusError = snapshot.backlogStatusError,
                batteryPercent = snapshot.batteryPercent,
                rssiDbm = snapshot.rssiDbm,
                connected = snapshot.connected,
                ecgAgeMs = age(snapshot.ecgUnixNs),
                accAgeMs = age(snapshot.accUnixNs),
                hrAgeMs = age(snapshot.hrUnixNs),
            )
        }
    }
}

/**
 * Owns the one service-lifetime status registration. Closing is idempotent;
 * the source registration is generation-safe, so a late old-service close
 * cannot detach a newer service observer.
 */
class StatusNotificationBridge(
    private val subscribe: ((CollectorSnapshot) -> Unit) -> AutoCloseable,
    private val mapper: CollectorNotificationMapper,
) : AutoCloseable {
    private val lock = Any()
    private var registration: AutoCloseable? = null

    fun onCreate() {
        synchronized(lock) {
            check(registration == null) { "status notification observer already active" }
            registration = subscribe(mapper::onSnapshot)
        }
    }

    override fun close() {
        val current = synchronized(lock) {
            val value = registration
            registration = null
            value
        }
        current?.close()
    }
}

/**
 * Moves platform notification I/O onto the service's lifecycle thread.
 *
 * [offer] is always queue-only: it never calls [onNotify], even when invoked on
 * the lifecycle thread. [drainOnLifecycleThread] is the only platform-I/O
 * boundary and is called only after the serialized status monitor has been
 * released. Closing clears pending content and makes already-posted drains
 * harmless, preventing post-destroy notification work.
 */
class LifecycleNotificationSink(
    private val isDispatchThread: () -> Boolean,
    private val dispatch: (() -> Unit) -> Unit,
    private val onNotify: (NotificationContent) -> Unit,
) : AutoCloseable {
    private val lock = Any()
    private val pending = ArrayDeque<NotificationContent>()
    private var active = true
    private var drainScheduled = false

    fun offer(content: NotificationContent) {
        val shouldDispatch = synchronized(lock) {
            if (!active) return
            pending.addLast(content)
            if (drainScheduled) {
                false
            } else {
                drainScheduled = true
                true
            }
        }
        if (shouldDispatch) dispatch(::drainOnLifecycleThread)
    }

    fun drainOnLifecycleThread() {
        check(isDispatchThread()) { "notification drain must run on the lifecycle thread" }
        while (true) {
            val content = synchronized(lock) {
                if (!active) {
                    pending.clear()
                    drainScheduled = false
                    null
                } else if (pending.isEmpty()) {
                    drainScheduled = false
                    null
                } else {
                    pending.removeFirst()
                }
            } ?: return
            onNotify(content)
        }
    }

    override fun close() {
        synchronized(lock) {
            active = false
            pending.clear()
            drainScheduled = false
        }
    }
}

/** Platform-independent foreground-notification content. */
data class NotificationContent(
    val title: String,
    val text: String,
    val showPairingAction: Boolean,
)
