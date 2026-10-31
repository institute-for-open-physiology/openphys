package com.vineyardfinance.sensors.ui

import com.vineyardfinance.sensors.AppGraph
import com.vineyardfinance.sensors.collector.*
import com.vineyardfinance.sensors.data.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

data class TimelineEvent(val time: Double, val title: String, val detail: String)

data class MobileState(
    val time: Double = System.currentTimeMillis() / 1000.0,
    val status: CollectorSnapshot = CollectorSnapshot(),
    val signals: SignalSnapshot = SignalSnapshot(),
    val events: List<SensorEvent> = emptyList(),
    val timeline: List<TimelineEvent> = emptyList(),
    val recent: List<HistoryBin> = emptyList(),
    val accountConnected: Boolean = false,
    val syncError: String? = null,
    val error: String? = null,
)

class MobileModel(val graph: AppGraph, private val scope: CoroutineScope) {
    private val mutableState = MutableStateFlow(MobileState())
    val state = mutableState.asStateFlow()

    suspend fun observe() =
        withContext(Dispatchers.IO) {
            var lastRead = 0.0
            while (isActive) {
                val now = System.currentTimeMillis() / 1000.0
                val status = CollectorStatus.updates.value
                val previous = state.value
                val timeline =
                    if (status.state != previous.status.state)
                        (listOf(TimelineEvent(now, status.state, status.detail)) +
                                previous.timeline)
                            .take(15)
                    else previous.timeline
                var recent = previous.recent
                var events = previous.events
                var error: String? = null
                if (now - lastRead >= 1) {
                    try {
                        recent = graph.signalStore.readBins(now - 600, now, 120)
                        recent = mergeCurrentBin(recent, graph.signals.currentBin(), now - 600, now)
                        events = graph.queue.readEvents()
                        lastRead = now
                    } catch (_: Exception) {
                        error = "Saved readings could not be opened."
                    }
                }
                val upload = graph.uploader.vitals.snapshot()
                mutableState.update { current ->
                    current.copy(
                        time = now,
                        status = status,
                        signals = graph.signals.snapshot(now),
                        events = events,
                        timeline = timeline,
                        recent = recent,
                        accountConnected = graph.credentials.connected.value,
                        syncError = upload.lastOutcome.takeIf { upload.consecutiveFailures > 0 },
                        error = error ?: current.error,
                    )
                }
                delay(100)
            }
        }

    fun saveEvent(label: String, time: Double, id: String? = null) = perform {
        graph.queue.saveEvent(label, time, id)
    }

    fun deleteEvent(id: String) = perform { graph.queue.deleteEvent(id) }

    fun dismissError() {
        mutableState.update { it.copy(error = null) }
    }

    private fun perform(action: suspend () -> Unit) {
        scope.launch(Dispatchers.IO) {
            try {
                action()
                graph.enqueueUpload()
                val events = graph.queue.readEvents()
                mutableState.update { it.copy(events = events) }
            } catch (_: Exception) {
                mutableState.update { it.copy(error = "The event could not be saved.") }
            }
        }
    }
}
