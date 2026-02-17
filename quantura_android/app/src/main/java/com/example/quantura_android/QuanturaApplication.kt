package com.example.quantura_android

import android.app.Application
import android.util.Log
import com.example.quantura_android.di.AppContainer
import com.google.android.gms.ads.MobileAds
import com.google.firebase.FirebaseApp

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
        try {
            MobileAds.initialize(this) {}
        } catch (_: Exception) {
            Log.w("QuanturaApplication", "Google Mobile Ads init skipped for this build.")
        }
        container = AppContainer(this, firebaseReady)
    }
}
