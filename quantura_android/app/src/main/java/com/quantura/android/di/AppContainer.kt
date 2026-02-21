package com.quantura.android.di

import android.app.Application
import android.content.pm.ApplicationInfo
import com.quantura.android.ads.AdManager
import com.quantura.android.config.RemoteConfigManager

// Simple DI container for shared app services.
class AppContainer(
    application: Application,
    firebaseReady: Boolean,
) {
    private val isDebuggable =
        (application.applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0

    val remoteConfigManager = RemoteConfigManager.create(
        firebaseReady = firebaseReady,
        isDebug = isDebuggable,
    )
    val adManager = AdManager(remoteConfigManager)
}
