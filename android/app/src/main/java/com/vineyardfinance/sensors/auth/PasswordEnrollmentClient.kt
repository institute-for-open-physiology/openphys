package com.vineyardfinance.sensors.auth

import com.google.gson.JsonObject
import com.vineyardfinance.sensors.AppLinks
import com.vineyardfinance.sensors.network.HttpJson
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import java.io.IOException

fun interface PasswordEnrollment {
    suspend fun createToken(email: String, password: String): String
}

/** A fresh, memory-only website session per sign-in; never shared with the upload client. */
class PasswordEnrollmentClient(
    private val baseUrl: String,
    private val client: OkHttpClient = HttpJson.defaultClient(),
) : PasswordEnrollment {
    override suspend fun createToken(email: String, password: String): String {
        val origin = baseUrl.trimEnd('/')
        val server = origin.toHttpUrl()
        val cookies = SessionCookies(server)
        val sessionClient = client.newBuilder()
            .cookieJar(cookies)
            .followRedirects(false)
            .followSslRedirects(false)
            .addInterceptor { chain ->
                val request = chain.request().newBuilder().header("Origin", origin)
                cookies.csrf()?.let { request.header("X-CSRF-Token", it) }
                chain.proceed(request.build())
            }.build()
        val http = HttpJson(origin, sessionClient)
        try {
            val login = JsonObject().apply {
                addProperty("email", email.trim())
                addProperty("password", password)
            }
            http.post("/api/v1/auth/password/login", login.toString()).requireSuccess()
            val response = http.post("/api/v1/device-enrollments", "{}").requireSuccess()
            val link = HttpJson.requiredString(http.jsonObject(response.body), "enrollment_url")
            return AppLinks.enrollmentToken(link) ?: throw IOException("Invalid enrollment response")
        } finally {
            cookies.clear()
        }
    }

    private class SessionCookies(private val server: HttpUrl) : CookieJar {
        private val cookies = mutableListOf<Cookie>()

        @Synchronized override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
            if (url.host != server.host || url.port != server.port || url.scheme != server.scheme) return
            for (cookie in cookies) {
                this.cookies.removeAll { it.name == cookie.name && it.domain == cookie.domain && it.path == cookie.path }
                this.cookies.add(cookie)
            }
        }

        @Synchronized override fun loadForRequest(url: HttpUrl): List<Cookie> =
            if (url.host == server.host && url.port == server.port && url.scheme == server.scheme) {
                cookies.filter { it.matches(url) && it.expiresAt > System.currentTimeMillis() }
            } else emptyList()

        @Synchronized fun csrf(): String? = loadForRequest(server).firstOrNull { it.name == "sensors_csrf" }?.value
        @Synchronized fun clear() = cookies.clear()
    }
}
