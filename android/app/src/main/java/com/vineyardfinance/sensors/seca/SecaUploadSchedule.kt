package com.vineyardfinance.sensors.seca

import java.io.File

/** Only the uploader mutex owner calls this; scheduling never mutates archive files. */
class SecaUploadSchedule {
    data class Selection(val file: File, val session: String, val priority: Boolean)
    private var priorityAcknowledgements = 0
    private var lastHistoricalSession: String? = null

    fun select(pending: List<File>, activeSession: String?): Selection? {
        // Queue filenames contain UUID + zero-padded batch index. Always take a
        // session's lowest index, regardless of directory enumeration order.
        val heads = pending.sortedBy { it.name }.distinctBy { it.name.substringBefore('_') }
        val active = heads.firstOrNull { it.name.substringBefore('_') == activeSession }
        val historical = heads.filter { it !== active }
        val priority = active != null && (priorityAcknowledgements < 3 || historical.isEmpty())
        val after = lastHistoricalSession
        val file = if (priority) active else historical.firstOrNull {
            after == null || it.name.substringBefore('_') > after
        } ?: historical.firstOrNull()
        return file?.let { Selection(it, it.name.substringBefore('_'), priority) }
    }

    /** Call only after exact ACK verification and durable removal succeeded. */
    fun acknowledged(selection: Selection) {
        if (selection.priority) priorityAcknowledgements = (priorityAcknowledgements + 1).coerceAtMost(3)
        else {
            priorityAcknowledgements = 0
            lastHistoricalSession = selection.session
        }
    }
}
