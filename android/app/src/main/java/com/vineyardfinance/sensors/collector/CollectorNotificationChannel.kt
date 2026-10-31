package com.vineyardfinance.sensors.collector

import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import com.vineyardfinance.sensors.R

object CollectorNotificationChannel {
    const val ID = "h10_collection"

    fun ensure(context: Context) {
        context.getSystemService(NotificationManager::class.java).createNotificationChannel(
            NotificationChannel(
                ID,
                context.getString(R.string.collector_channel_name),
                NotificationManager.IMPORTANCE_LOW,
            ).apply {
                description = context.getString(R.string.collector_channel_description)
                setShowBadge(false)
            },
        )
    }

    fun isEnabled(context: Context): Boolean {
        val manager = context.getSystemService(NotificationManager::class.java)
        val channel = manager.getNotificationChannel(ID) ?: return false
        return channel.importance != NotificationManager.IMPORTANCE_NONE
    }
}
