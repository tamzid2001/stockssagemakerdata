package com.example.quantura_android

import android.app.Application
import com.example.quantura_android.di.AppContainer
import com.google.android.gms.ads.MobileAds
import com.google.firebase.FirebaseApp

class QuanturaApplication : Application() {
    lateinit var container: AppContainer
        private set

    override fun onCreate() {
        super.onCreate()
        FirebaseApp.initializeApp(this)
        MobileAds.initialize(this) {}
        container = AppContainer(this)
    }
}
