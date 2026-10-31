package com.vineyardfinance.sensors.collector

import android.app.Application
import android.os.Handler
import android.os.Looper
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import org.robolectric.annotation.LooperMode

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34], application = Application::class)
@LooperMode(LooperMode.Mode.PAUSED)
class HandlerFunctionDispatcherTest {
    @Test
    fun offMainOfferPostsRunnableAndDrainsOnAndroidMainLooper() {
        val mainLooper = Looper.getMainLooper()
        val deliveredLooper = AtomicReference<Looper?>()
        val sink = LifecycleNotificationSink(
            isDispatchThread = { Looper.myLooper() == mainLooper },
            dispatch = handlerFunctionDispatcher(Handler(mainLooper)),
            onNotify = { deliveredLooper.set(Looper.myLooper()) },
        )
        val producer = Executors.newSingleThreadExecutor()

        try {
            val offeredOffMain = producer.submit<Boolean> {
                assertNull(Looper.myLooper())
                sink.offer(NotificationContent("Streaming", "fresh", false))
                deliveredLooper.get() == null
            }.get(2, TimeUnit.SECONDS)

            assertTrue("offer must return before main-thread notification work", offeredOffMain)
            shadowOf(mainLooper).idle()
            assertSame(mainLooper, deliveredLooper.get())
        } finally {
            sink.close()
            producer.shutdownNow()
        }
    }
}
