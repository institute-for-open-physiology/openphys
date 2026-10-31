package com.vineyardfinance.sensors.data

import android.content.Context
import java.util.UUID

class AppIdentity(context: Context, preferencesName: String = "installation_identity") {
    val bootCount = android.provider.Settings.Global.getInt(context.contentResolver, android.provider.Settings.Global.BOOT_COUNT, -1)
    private val preferences = context.getSharedPreferences(preferencesName, Context.MODE_PRIVATE)

    @get:Synchronized
    val installationId: String
        get() = preferences.getString(INSTALLATION_ID, null) ?: UUID.randomUUID().toString().also {
            check(preferences.edit().putString(INSTALLATION_ID, it).commit()) { "failed to persist installation ID" }
        }

    companion object { private const val INSTALLATION_ID = "installation_id" }
}
