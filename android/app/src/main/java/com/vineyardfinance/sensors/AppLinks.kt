package com.vineyardfinance.sensors

import java.net.URI

internal object AppLinks {
    private const val HOST = "sensors-poc.vineyard-finance.com"
    private val TOKEN = Regex("^[A-Za-z0-9_-]{43}$")

    /** Enrollment secrets live in the fragment so proxies and HTTP logs never receive them. */
    fun enrollmentToken(rawUri: String?): String? {
        val uri = rawUri?.let { runCatching { URI(it) }.getOrNull() } ?: return null
        if (!uri.scheme.equals("https", ignoreCase = true) ||
            !uri.host.equals(HOST, ignoreCase = true) ||
            uri.userInfo != null ||
            (uri.port != -1 && uri.port != 443) ||
            uri.rawPath != "/enroll" ||
            uri.rawQuery != null
        ) {
            return null
        }
        return uri.rawFragment?.takeIf(TOKEN::matches)
    }
}
