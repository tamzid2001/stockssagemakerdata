package com.quantura.quanturaapp.ads

import android.app.Activity
import android.app.Application
import android.os.SystemClock
import android.util.Log
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner
import com.google.android.gms.ads.AdRequest
import com.google.android.gms.ads.FullScreenContentCallback
import com.google.android.gms.ads.LoadAdError
import com.google.android.gms.ads.appopen.AppOpenAd
import com.quantura.quanturaapp.MainActivity
import com.quantura.quanturaapp.config.RemoteConfigManager

class AppOpenAdManager(
    private val application: Application,
    private val remoteConfigManager: RemoteConfigManager,
    private val adManager: AdManager,
) : Application.ActivityLifecycleCallbacks, DefaultLifecycleObserver {
    private val tag = "AppOpenAdManager"
    private val maxCacheAgeMs = 4 * 60 * 60 * 1000L

    @Volatile
    private var appOpenAd: AppOpenAd? = null
    @Volatile
    private var isLoadingAd = false
    @Volatile
    private var isShowingAd = false
    @Volatile
    private var loadedAtMs = 0L
    @Volatile
    private var currentActivity: Activity? = null

    fun start() {
        application.registerActivityLifecycleCallbacks(this)
        ProcessLifecycleOwner.get().lifecycle.addObserver(this)
        loadAdIfNeeded()
    }

    override fun onStart(owner: LifecycleOwner) {
        showAdIfAvailable()
    }

    fun loadAdIfNeeded() {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) {
            Log.d(tag, "App open ads disabled by feature flag.")
            return
        }
        if (isLoadingAd || isAdAvailable()) return
        val unitId = remoteConfigManager.getAdUnitIds().appOpen
        Log.d(tag, "Loading app open ad unit=$unitId")
        isLoadingAd = true
        AppOpenAd.load(
            application,
            unitId,
            AdRequest.Builder().build(),
            object : AppOpenAd.AppOpenAdLoadCallback() {
                override fun onAdLoaded(ad: AppOpenAd) {
                    isLoadingAd = false
                    appOpenAd = ad
                    loadedAtMs = SystemClock.elapsedRealtime()
                    Log.d(tag, "App open ad load succeeded.")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    isLoadingAd = false
                    appOpenAd = null
                    loadedAtMs = 0L
                    Log.w(tag, "App open ad load failed: ${loadAdError.message}")
                }
            }
        )
    }

    fun showAdIfAvailable() {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        if (isShowingAd) return
        if (adManager.isShowingFullScreenAd()) {
            Log.d(tag, "Skipping app open show; another fullscreen ad is visible.")
            return
        }

        val activity = currentActivity
        if (!canShowOn(activity)) {
            Log.d(tag, "Skipping app open show; activity not ready (${activity?.javaClass?.simpleName ?: "none"}).")
            return
        }

        val cached = appOpenAd
        if (!isAdAvailable() || cached == null) {
            Log.d(tag, "No fresh app open ad available; loading.")
            loadAdIfNeeded()
            return
        }

        isShowingAd = true
        cached.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdShowedFullScreenContent() {
                Log.d(tag, "App open ad shown.")
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                Log.w(tag, "App open ad failed to show: ${adError.message}")
                isShowingAd = false
                appOpenAd = null
                loadedAtMs = 0L
                loadAdIfNeeded()
            }

            override fun onAdDismissedFullScreenContent() {
                Log.d(tag, "App open ad dismissed.")
                isShowingAd = false
                appOpenAd = null
                loadedAtMs = 0L
                loadAdIfNeeded()
            }
        }
        Log.d(tag, "Presenting app open ad.")
        cached.show(activity!!)
    }

    private fun isAdAvailable(): Boolean {
        if (appOpenAd == null) return false
        val age = SystemClock.elapsedRealtime() - loadedAtMs
        return age in 0..<maxCacheAgeMs
    }

    private fun canShowOn(activity: Activity?): Boolean {
        if (activity == null) return false
        if (activity !is MainActivity) return false
        if (activity.isFinishing || activity.isDestroyed) return false
        return true
    }

    override fun onActivityCreated(activity: Activity, savedInstanceState: android.os.Bundle?) {}

    override fun onActivityStarted(activity: Activity) {
        currentActivity = activity
        if (activity is MainActivity) {
            showAdIfAvailable()
        }
    }

    override fun onActivityResumed(activity: Activity) {
        currentActivity = activity
    }

    override fun onActivityPaused(activity: Activity) {}

    override fun onActivityStopped(activity: Activity) {}

    override fun onActivitySaveInstanceState(activity: Activity, outState: android.os.Bundle) {}

    override fun onActivityDestroyed(activity: Activity) {
        if (currentActivity === activity) {
            currentActivity = null
        }
    }
}
