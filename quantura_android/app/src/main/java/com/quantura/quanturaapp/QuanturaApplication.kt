package com.quantura.quanturaapp

import android.app.Application
import android.content.pm.ApplicationInfo
import android.util.Log
import com.google.android.gms.ads.AdRequest
import com.quantura.quanturaapp.di.AppContainer
import com.google.android.gms.ads.MobileAds
import com.google.android.gms.ads.RequestConfiguration
import com.google.firebase.FirebaseApp
import com.quantura.quanturaapp.auth.AuthSessionManager

class QuanturaApplication : Application() {
    lateinit var container: AppContainer
        private set

    override fun onCreate() {
        super.onCreate()
        val firebaseReady = try {
            FirebaseApp.initializeApp(this) != null
        } catch (_: Exception) {
            false
        }
        if (!firebaseReady) {
            Log.w("QuanturaApplication", "Firebase disabled: missing google-services.json for local build.")
        }
        val isDebuggable = (applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0
        try {
            if (isDebuggable) {
                MobileAds.setRequestConfiguration(
                    RequestConfiguration.Builder()
                        .setTestDeviceIds(listOf(AdRequest.DEVICE_ID_EMULATOR))
                        .build()
                )
            }
            MobileAds.initialize(this) { status ->
                Log.i("QuanturaApplication", "Mobile Ads initialized adapters=${status.adapterStatusMap.size}")
            }
        } catch (_: Exception) {
            Log.w("QuanturaApplication", "Google Mobile Ads init skipped for this build.")
        }
        container = AppContainer(this, firebaseReady)
        AuthSessionManager.start(firebaseReady)
        container.appOpenAdManager.start()
    }
}
