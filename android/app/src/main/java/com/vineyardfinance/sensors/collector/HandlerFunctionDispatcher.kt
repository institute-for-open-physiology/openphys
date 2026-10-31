package com.vineyardfinance.sensors.collector

import android.os.Handler

/**
 * Adapts a Kotlin function to the Java [Runnable] contract required by
 * [Handler.post]. Keep the wrapper explicit: a `Handler::post` callable
 * reference can compile on the host while casting `Function0` directly to
 * `Runnable` on Android.
 */
internal fun handlerFunctionDispatcher(handler: Handler): ((() -> Unit) -> Unit) = { callback ->
    handler.post(Runnable { callback() })
    Unit
}
