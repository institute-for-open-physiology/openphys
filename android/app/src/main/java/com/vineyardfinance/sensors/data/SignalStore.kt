package com.vineyardfinance.sensors.data

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper

data class HistoryBin(val time: Double, val values: List<Double?>)

/** Display history is separate from the acknowledged upload queue. */
class SignalStore(context: Context) : SQLiteOpenHelper(context, "signal-history.sqlite3", null, 1) {
    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL(
            "CREATE TABLE history (time REAL PRIMARY KEY, heartRate REAL, rr REAL, activity REAL, rssi REAL, delay REAL)"
        )
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) = Unit

    fun saveBin(bin: HistoryBin) {
        writableDatabase.execSQL(
            "INSERT OR REPLACE INTO history VALUES (?, ?, ?, ?, ?, ?)",
            arrayOf(bin.time, *bin.values.toTypedArray()),
        )
        writableDatabase.execSQL(
            "DELETE FROM history WHERE time < ?",
            arrayOf(bin.time - 8 * 86400),
        )
    }

    fun readBins(start: Double, end: Double, count: Int): List<HistoryBin> {
        if (end <= start || count <= 0) return emptyList()
        val step = (end - start) / count
        val sql =
            "SELECT CAST((time - ?) / ? AS INTEGER), AVG(heartRate), AVG(rr), AVG(activity), AVG(rssi), AVG(delay) " +
                "FROM history WHERE time >= ? AND time < ? GROUP BY 1 ORDER BY 1"
        return readableDatabase
            .rawQuery(sql, arrayOf(start, step, start, end).map(Double::toString).toTypedArray())
            .use { cursor ->
                val bins =
                    MutableList(count) { HistoryBin(start + (it + 0.5) * step, List(5) { null }) }
                while (cursor.moveToNext()) {
                    val slot = cursor.getInt(0)
                    if (slot in bins.indices)
                        bins[slot] =
                            bins[slot].copy(
                                values =
                                    (1..5).map {
                                        if (cursor.isNull(it)) null else cursor.getDouble(it)
                                    }
                            )
                }
                bins
            }
    }
}

fun mergeCurrentBin(
    bins: List<HistoryBin>,
    current: HistoryBin?,
    start: Double,
    end: Double,
): List<HistoryBin> {
    if (current == null || bins.isEmpty() || current.time < start || current.time >= end)
        return bins
    val index = ((current.time - start) / (end - start) * bins.size).toInt().coerceIn(bins.indices)
    return bins.toMutableList().apply { this[index] = this[index].copy(values = current.values) }
}
