package com.quantura.quanturaapp.ads

import android.app.Activity
import android.app.Application
import android.os.Handler
import android.os.Looper
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
import com.quantura.quanturaapp.config.AdFormat
import com.quantura.quanturaapp.config.AdPlatform
import com.quantura.quanturaapp.config.RemoteConfigManager

class AppOpenAdManager(
    private val application: Application,
    private val remoteConfigManager: RemoteConfigManager,
    private val adManager: AdManager,
) : Application.ActivityLifecycleCallbacks, DefaultLifecycleObserver {
    private val tag = "AppOpenAdManager"
    private val maxCacheAgeMs = 4 * 60 * 60 * 1000L
    private val mainHandler = Handler(Looper.getMainLooper())

    @Volatile
    private var appOpenAd: AppOpenAd? = null
    @Volatile
    private var loadedAdUnitId: String = ""
    @Volatile
    private var isLoadingAd = false
    @Volatile
    private var isShowingAd = false
    @Volatile
    private var loadedAtMs = 0L
    @Volatile
    private var currentActivity: Activity? = null
    @Volatile
    private var currentActivityResumed = false
    @Volatile
    private var presentationBlockedByAuthGate = false
    @Volatile
    private var canPresentInCurrentForeground = true

    fun start() {
        application.registerActivityLifecycleCallbacks(this)
        ProcessLifecycleOwner.get().lifecycle.addObserver(this)
    }

    override fun onStart(owner: LifecycleOwner) {
        canPresentInCurrentForeground = true
        showAdIfAvailable()
    }

    override fun onStop(owner: LifecycleOwner) {
        canPresentInCurrentForeground = true
    }

    fun setPresentationBlockedByAuthGate(blocked: Boolean) {
        presentationBlockedByAuthGate = blocked
        if (blocked) {
            Log.d(tag, "Auth gate is visible; app open presentation is deferred.")
        }
    }

    fun loadAdIfNeeded() {
        if (!remoteConfigManager.areAdsEnabled()) {
            Log.d(tag, "App open ads disabled by feature flag.")
            AdDebugStatusRegistry.updateLoad("app_open", "disabled")
            return
        }
        if (!remoteConfigManager.isAdFormatEnabled(platform = AdPlatform.ANDROID, format = AdFormat.APP_OPEN)) {
            Log.d(tag, "App open ads disabled by format flag.")
            AdDebugStatusRegistry.updateLoad("app_open", "disabled:format_off")
            appOpenAd = null
            loadedAdUnitId = ""
            loadedAtMs = 0L
            return
        }
        if (!MobileAdsBootstrap.isInitialized()) {
            Log.d(tag, "App open load deferred until Mobile Ads finishes initializing.")
            AdDebugStatusRegistry.updateLoad("app_open", "waiting:sdk_init")
            MobileAdsBootstrap.runWhenInitialized {
                mainHandler.post { loadAdIfNeeded() }
            }
            return
        }
        if (isLoadingAd || isAdAvailable()) return
        val unitId = remoteConfigManager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.APP_OPEN
        )
        Log.d(tag, "Loading app open ad unit=$unitId")
        AdDebugStatusRegistry.updateLoad("app_open", "loading")
        isLoadingAd = true
        AppOpenAd.load(
            application,
            unitId,
            AdRequest.Builder().build(),
            object : AppOpenAd.AppOpenAdLoadCallback() {
                override fun onAdLoaded(ad: AppOpenAd) {
                    isLoadingAd = false
                    val desiredUnitId = remoteConfigManager.resolveAdUnitId(
                        platform = AdPlatform.ANDROID,
                        format = AdFormat.APP_OPEN
                    )
                    if (unitId != desiredUnitId) {
                        appOpenAd = null
                        loadedAdUnitId = ""
                        loadedAtMs = 0L
                        Log.i(
                            tag,
                            "[Ads][Android] Discarding stale app open load oldUnit=$unitId newUnit=$desiredUnitId"
                        )
                        loadAdIfNeeded()
                        return
                    }
                    appOpenAd = ad
                    loadedAdUnitId = unitId
                    loadedAtMs = SystemClock.elapsedRealtime()
                    Log.d(tag, "App open ad load succeeded.")
                    Log.i(tag, "[Ads][Android] Load success for app_open")
                    AdDebugStatusRegistry.updateLoad("app_open", "loaded")
                    // If app is already foregrounded, attempt an immediate presentation.
                    showAdIfAvailable()
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    isLoadingAd = false
                    appOpenAd = null
                    loadedAdUnitId = ""
                    loadedAtMs = 0L
                    Log.w(tag, "App open ad load failed: ${loadAdError.message}")
                    Log.w(tag, "[Ads][Android] Load fail for app_open: ${loadAdError.message}")
                    AdDebugStatusRegistry.updateLoad("app_open", "failed:${loadAdError.message}")
                }
            }
        )
    }

    fun showAdIfAvailable() {
        if (!remoteConfigManager.areAdsEnabled()) return
        if (isShowingAd) return
        if (!remoteConfigManager.isAdFormatEnabled(platform = AdPlatform.ANDROID, format = AdFormat.APP_OPEN)) {
            Log.d(tag, "Skipping app open show; format flag is disabled.")
            AdDebugStatusRegistry.updateShow("app_open", "skipped:format_off")
            return
        }
        if (!canPresentInCurrentForeground) {
            Log.d(tag, "Skipping app open show; already presented in current foreground.")
            AdDebugStatusRegistry.updateShow("app_open", "skipped:already_presented")
            return
        }
        if (presentationBlockedByAuthGate) {
            Log.d(tag, "Skipping app open show; auth gate is visible.")
            AdDebugStatusRegistry.updateShow("app_open", "skipped:auth_gate")
            return
        }
        if (adManager.isShowingFullScreenAd()) {
            Log.d(tag, "Skipping app open show; another fullscreen ad is visible.")
            AdDebugStatusRegistry.updateShow("app_open", "skipped:fullscreen_visible")
            return
        }

        val activity = currentActivity
        if (!canShowOn(activity)) {
            Log.d(tag, "Skipping app open show; activity not ready (${activity?.javaClass?.simpleName ?: "none"}).")
            AdDebugStatusRegistry.updateShow("app_open", "skipped:activity_not_ready")
            return
        }
        val presenterActivity = activity ?: return

        val cached = appOpenAd
        if (!isAdAvailable() || cached == null) {
            Log.d(tag, "No fresh app open ad available; loading.")
            AdDebugStatusRegistry.updateShow("app_open", "skipped:not_ready")
            loadAdIfNeeded()
            return
        }

        isShowingAd = true
        canPresentInCurrentForeground = false
        cached.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdShowedFullScreenContent() {
                Log.d(tag, "App open ad shown.")
                Log.i(tag, "[Ads][Android] Show success for app_open")
                AdDebugStatusRegistry.updateShow("app_open", "shown")
            }

            override fun onAdImpression() {
                val adUnitId = remoteConfigManager.resolveAdUnitId(
                    platform = AdPlatform.ANDROID,
                    format = AdFormat.APP_OPEN
                )
                AdImpressionReporter.report(
                    context = application.applicationContext,
                    adFormat = "app_open",
                    adUnitId = adUnitId,
                    placement = "app_open"
                )
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                Log.w(tag, "App open ad failed to show: ${adError.message}")
                isShowingAd = false
                canPresentInCurrentForeground = true
                appOpenAd = null
                loadedAtMs = 0L
                AdDebugStatusRegistry.updateShow("app_open", "failed:${adError.message}")
                loadAdIfNeeded()
            }

            override fun onAdDismissedFullScreenContent() {
                Log.d(tag, "App open ad dismissed.")
                isShowingAd = false
                appOpenAd = null
                loadedAtMs = 0L
                AdDebugStatusRegistry.updateShow("app_open", "dismissed")
                loadAdIfNeeded()
            }
        }
        Log.d(tag, "Presenting app open ad.")
        try {
            cached.show(presenterActivity)
        } catch (error: Exception) {
            isShowingAd = false
            canPresentInCurrentForeground = true
            appOpenAd = null
            loadedAtMs = 0L
            Log.w(tag, "App open ad show threw exception: ${error.message}")
            AdDebugStatusRegistry.updateShow("app_open", "failed:${error.message}")
            loadAdIfNeeded()
        }
    }

    fun refreshForRemoteConfigChange() {
        if (!remoteConfigManager.areAdsEnabled() || !remoteConfigManager.isAdFormatEnabled(
                platform = AdPlatform.ANDROID,
                format = AdFormat.APP_OPEN
            )
        ) {
            appOpenAd = null
            loadedAdUnitId = ""
            loadedAtMs = 0L
            return
        }
        val desiredUnitId = remoteConfigManager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.APP_OPEN
        )
        if (appOpenAd != null && loadedAdUnitId != desiredUnitId) {
            Log.i(
                tag,
                "[Ads][Android] Clearing cached app open after RC change oldUnit=$loadedAdUnitId newUnit=$desiredUnitId"
            )
            appOpenAd = null
            loadedAdUnitId = ""
            loadedAtMs = 0L
        }
    }

    fun debugState(): AdFormatStatusSnapshot {
        return AdDebugStatusRegistry.snapshot(listOf("app_open")).first()
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
        if (!currentActivityResumed) return false
        return true
    }

    override fun onActivityCreated(activity: Activity, savedInstanceState: android.os.Bundle?) {}

    override fun onActivityStarted(activity: Activity) {
        currentActivity = activity
    }

    override fun onActivityResumed(activity: Activity) {
        currentActivity = activity
        currentActivityResumed = true
        if (activity is MainActivity) {
            showAdIfAvailable()
        }
    }

    override fun onActivityPaused(activity: Activity) {
        if (currentActivity === activity) {
            currentActivityResumed = false
        }
    }

    override fun onActivityStopped(activity: Activity) {}

    override fun onActivitySaveInstanceState(activity: Activity, outState: android.os.Bundle) {}

    override fun onActivityDestroyed(activity: Activity) {
        if (currentActivity === activity) {
            currentActivity = null
            currentActivityResumed = false
            isShowingAd = false
        }
    }
}
