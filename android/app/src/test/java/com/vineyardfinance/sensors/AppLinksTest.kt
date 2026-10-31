package com.vineyardfinance.sensors

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class AppLinksTest {
    private val token = "a".repeat(43)

    @Test fun exactEnrollmentFragmentIsAccepted() {
        assertEquals(token, AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/enroll#$token"))
        assertEquals(token, AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com:443/enroll#$token"))
    }

    @Test fun inviteResetPathQueryAndExtraPathNeverReachTheAppExchange() {
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/invite#$token"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/reset-password#$token"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/enroll/$token"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/%65nroll#$token"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/enroll?token=$token"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/enroll?x=1#$token"))
    }

    @Test fun foreignAmbiguousAndMalformedLinksAreRejected() {
        assertNull(AppLinks.enrollmentToken("https://attacker.example/enroll#$token"))
        assertNull(AppLinks.enrollmentToken("http://sensors-poc.vineyard-finance.com/enroll#$token"))
        assertNull(AppLinks.enrollmentToken("https://user@sensors-poc.vineyard-finance.com/enroll#$token"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com:444/enroll#$token"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/enroll#short"))
        assertNull(AppLinks.enrollmentToken("https://sensors-poc.vineyard-finance.com/enroll#${"a".repeat(42)}%2F"))
        assertNull(AppLinks.enrollmentToken("not a uri"))
        assertNull(AppLinks.enrollmentToken(null))
    }
}
