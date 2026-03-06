package com.quantura.quanturaapp

import android.app.Application
import android.content.pm.ApplicationInfo
import android.os.Build
import android.util.Log
import com.google.android.gms.ads.AdRequest
import com.quantura.quanturaapp.di.AppContainer
import com.google.android.gms.ads.MobileAds
import com.google.android.gms.ads.RequestConfiguration
import com.google.android.gms.common.ConnectionResult
import com.google.android.gms.common.GoogleApiAvailability
import com.google.firebase.FirebaseApp
import com.quantura.quanturaapp.auth.AuthSessionManager

class QuanturaApplication : Application() {
    lateinit var container: AppContainer
        private set
    var firebaseReady: Boolean = false
        private set

    override fun onCreate() {
        super.onCreate()
        firebaseReady = try {
            FirebaseApp.initializeApp(this) != null
        } catch (_: Exception) {
            false
        }
        if (!firebaseReady) {
            Log.w("QuanturaApplication", "Firebase disabled: missing google-services.json for local build.")
        }
        val isDebuggable = (applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0
        val isEmulator = detectEmulator()
        logGooglePlayServicesHealth(isEmulator = isEmulator)
        try {
            if (isDebuggable || isEmulator) {
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

    private fun detectEmulator(): Boolean {
        val fingerprint = Build.FINGERPRINT.lowercase()
        val model = Build.MODEL.lowercase()
        val product = Build.PRODUCT.lowercase()
        val hardware = Build.HARDWARE.lowercase()
        return fingerprint.contains("generic") ||
            model.contains("emulator") ||
            model.contains("sdk_gphone") ||
            hardware.contains("ranchu") ||
            hardware.contains("goldfish") ||
            product.contains("sdk")
    }

    private fun logGooglePlayServicesHealth(isEmulator: Boolean) {
        val availability = GoogleApiAvailability.getInstance()
        val code = availability.isGooglePlayServicesAvailable(this)
        if (code == ConnectionResult.SUCCESS) {
            Log.i("QuanturaApplication", "[Ads][Android] Google Play services available.")
            return
        }
        val reason = availability.getErrorString(code)
        Log.w(
            "QuanturaApplication",
            "[Ads][Android] Google Play services issue code=$code reason=$reason. " +
                "Ad loading can be unreliable on this ${if (isEmulator) "emulator" else "device"} until Play services are updated."
        )
    }
}
