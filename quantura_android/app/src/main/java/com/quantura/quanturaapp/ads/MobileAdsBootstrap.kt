package com.quantura.quanturaapp.ads

import android.content.Context
import android.util.Log
import com.google.android.gms.ads.MobileAds

object MobileAdsBootstrap {
    private val lock = Any()

    @Volatile
    private var initializeStarted = false

    @Volatile
    private var initialized = false

    private val pendingCallbacks = mutableListOf<() -> Unit>()

    fun initialize(context: Context) {
        synchronized(lock) {
            if (initializeStarted) return
            initializeStarted = true
        }

        MobileAds.initialize(context) { status ->
            val callbacks = synchronized(lock) {
                initialized = true
                pendingCallbacks.toList().also { pendingCallbacks.clear() }
            }
            Log.i(
                "MobileAdsBootstrap",
                "[Ads][Android] Mobile Ads initialized adapters=${status.adapterStatusMap.size}"
            )
            callbacks.forEach { callback ->
                runCatching { callback() }
                    .onFailure { error ->
                        Log.w(
                            "MobileAdsBootstrap",
                            "[Ads][Android] Post-init callback failed: ${error.message}"
                        )
                    }
            }
        }
    }

    fun isInitialized(): Boolean = initialized

    fun runWhenInitialized(callback: () -> Unit) {
        if (initialized) {
            callback()
            return
        }
        synchronized(lock) {
            if (initialized) {
                callback()
            } else {
                pendingCallbacks += callback
            }
        }
    }
}
