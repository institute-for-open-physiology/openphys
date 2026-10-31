package com.vineyardfinance.sensors

import android.app.Application
import android.content.Intent
import android.net.Uri
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(application = Application::class)
class EnrollmentIntentTest {
    private val token = "A_-z09" + "b".repeat(37)
    private val link = "https://sensors-poc.vineyard-finance.com/enroll#$token"

    @Test fun phoneTapAndQrDispatchUseTheSameConsumedEnrollmentMechanism() {
        val phoneTap = Intent(Intent.ACTION_VIEW, Uri.parse(link))
        val qrDispatch = Intent(Intent.ACTION_VIEW, Uri.parse(link)).putExtra("source", "camera")

        listOf(phoneTap, qrDispatch).forEach { intent ->
            val consumed = EnrollmentIntent.consume(intent)
            assertTrue(consumed is ConsumedEnrollmentLink.Valid)
            assertEquals(token, (consumed as ConsumedEnrollmentLink.Valid).token)
            assertFalse(consumed.toString().contains(token))
            assertNull(intent.data)
            assertNull(intent.dataString)
            assertFalse(intent.toUri(0).contains(token))
        }
    }

    @Test fun invalidUriIsStillScrubbedBeforeReturningErrorState() {
        val intent = Intent(Intent.ACTION_VIEW, Uri.parse("https://sensors-poc.vineyard-finance.com/enroll?token=$token"))

        val consumed = EnrollmentIntent.consume(intent)

        assertSame(ConsumedEnrollmentLink.Invalid, consumed)
        assertNull(intent.data)
        assertFalse(intent.toUri(0).contains(token))
    }

    @Test fun absentIntentOrDataProducesNoEnrollmentState() {
        assertSame(ConsumedEnrollmentLink.Absent, EnrollmentIntent.consume(null))
        assertSame(ConsumedEnrollmentLink.Absent, EnrollmentIntent.consume(Intent(Intent.ACTION_MAIN)))
    }
}
