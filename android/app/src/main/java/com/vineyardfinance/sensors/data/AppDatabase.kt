package com.vineyardfinance.sensors.data

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.migration.Migration
import androidx.room.RoomDatabase
import androidx.sqlite.db.SupportSQLiteDatabase

@Database(
    entities = [CounterEntity::class, PendingRecordEntity::class, UploadBatchEntity::class, RuntimeStateEntity::class, SensorEvent::class],
    version = 2,
    exportSchema = true,
)
abstract class AppDatabase : RoomDatabase() {
    abstract fun queueDao(): QueueDao

    companion object {
        val MIGRATION_1_2 = object : Migration(1, 2) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL("CREATE TABLE IF NOT EXISTS events (id TEXT NOT NULL PRIMARY KEY, time REAL NOT NULL, label TEXT NOT NULL)")
            }
        }

        fun build(context: Context, name: String = "sensor-spool.sqlite3"): AppDatabase =
            Room.databaseBuilder(context, AppDatabase::class.java, name)
                .setJournalMode(JournalMode.WRITE_AHEAD_LOGGING)
                .addMigrations(MIGRATION_1_2)
                .addCallback(object : Callback() {
                    override fun onOpen(db: SupportSQLiteDatabase) {
                        super.onOpen(db)
                        db.execSQL("PRAGMA synchronous=FULL")
                        db.execSQL("PRAGMA foreign_keys=ON")
                    }
                })
                .build()
    }
}
