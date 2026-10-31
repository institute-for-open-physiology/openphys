package com.vineyardfinance.sensors

import android.content.Context
import com.vineyardfinance.sensors.collector.H10Identity

class AppSettings(context: Context) {
    private val preferences = context.getSharedPreferences("collector_settings", Context.MODE_PRIVATE)

    var collectionEnabled: Boolean
        get() = preferences.getBoolean(COLLECTION_ENABLED, false)
        set(value) { check(preferences.edit().putBoolean(COLLECTION_ENABLED, value).commit()) }

    var target: String
        // Deliberately no built-in default: every installation must enter the
        // serial printed on its own strap, so a tester's collector can never
        // silently scan forever for someone else's H10.
        get() = preferences.getString(TARGET, "") ?: ""
        set(value) { check(preferences.edit().putString(TARGET, H10Identity.configuredSerial(value)).commit()) }

    var locationCollectionEnabled: Boolean
        // Per-installation location opt-in (docs/gps_tracking_proposal_20260808.md,
        // docs/agreed_app_changes_20260808.md item 9). Deliberately defaults to
        // OFF: no location data may ever leave the phone while this is false.
        get() = preferences.getBoolean(LOCATION_COLLECTION_ENABLED, false)
        set(value) {
            check(preferences.edit().putBoolean(LOCATION_COLLECTION_ENABLED, value).commit()) {
                "failed to persist location opt-in"
            }
        }

    var collectorFailureDetail: String?
        get() = preferences.getString(COLLECTOR_FAILURE_DETAIL, null)
        set(value) {
            val edit = preferences.edit()
            if (value == null) edit.remove(COLLECTOR_FAILURE_DETAIL)
            else edit.putString(COLLECTOR_FAILURE_DETAIL, value.take(MAX_FAILURE_DETAIL_LENGTH))
            check(edit.commit()) { "failed to persist collector failure state" }
        }

    fun recordCollectorStopped() {
        check(
            preferences.edit()
                .putBoolean(COLLECTION_ENABLED, false)
                .remove(COLLECTOR_FAILURE_DETAIL)
                .commit(),
        ) { "failed to persist collector stop" }
    }

    fun manufacturerBackgroundConfirmed(manufacturer: String): Boolean =
        preferences.getString(BACKGROUND_CONFIRMED_MANUFACTURER, null) == manufacturer.trim().lowercase()

    fun setManufacturerBackgroundConfirmed(manufacturer: String, confirmed: Boolean) {
        val edit = preferences.edit()
        if (confirmed) edit.putString(BACKGROUND_CONFIRMED_MANUFACTURER, manufacturer.trim().lowercase())
        else edit.remove(BACKGROUND_CONFIRMED_MANUFACTURER)
        check(edit.commit()) { "failed to persist manufacturer background confirmation" }
    }

    companion object {
        private const val COLLECTION_ENABLED = "collection_enabled"
        private const val LOCATION_COLLECTION_ENABLED = "location_collection_enabled_v1"
        private const val TARGET = "h10_target"
        private const val BACKGROUND_CONFIRMED_MANUFACTURER = "background_confirmed_manufacturer_v1"
        private const val COLLECTOR_FAILURE_DETAIL = "collector_failure_detail_v1"
        private const val MAX_FAILURE_DETAIL_LENGTH = 240
    }
}
