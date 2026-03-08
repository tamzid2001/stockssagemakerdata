package com.quantura.quanturaapp.ads

import android.app.Activity
import android.content.Context
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.drawable.BitmapDrawable
import android.graphics.drawable.Drawable
import android.net.Uri
import android.os.SystemClock
import android.os.Bundle
import android.util.Base64
import android.util.Log
import com.quantura.quanturaapp.config.AdFormat
import com.quantura.quanturaapp.config.AdPlatform
import com.quantura.quanturaapp.config.RemoteConfigManager
import com.google.android.gms.ads.AdListener
import com.google.android.gms.ads.AdLoader
import com.google.android.gms.ads.AdRequest
import com.google.android.gms.ads.FullScreenContentCallback
import com.google.android.gms.ads.LoadAdError
import com.google.android.gms.ads.interstitial.InterstitialAd
import com.google.android.gms.ads.interstitial.InterstitialAdLoadCallback
import com.google.android.gms.ads.nativead.NativeAd
import com.google.android.gms.ads.rewarded.RewardItem
import com.google.android.gms.ads.rewarded.RewardedAd
import com.google.android.gms.ads.rewarded.RewardedAdLoadCallback
import com.google.android.gms.ads.rewarded.ServerSideVerificationOptions
import com.google.android.gms.ads.rewardedinterstitial.RewardedInterstitialAd
import com.google.android.gms.ads.rewardedinterstitial.RewardedInterstitialAdLoadCallback
import com.google.firebase.analytics.FirebaseAnalytics
import com.google.firebase.auth.FirebaseAuth
import org.json.JSONObject
import java.io.ByteArrayOutputStream

class AdManager(
    private val remoteConfigManager: RemoteConfigManager,
) {
    private val tag = "AdManager"
    private val knownFormats = listOf(
        AdFormat.APP_OPEN,
        AdFormat.BANNER,
        AdFormat.INTERSTITIAL,
        AdFormat.REWARDED,
        AdFormat.REWARDED_INTERSTITIAL,
        AdFormat.NATIVE,
    )

    @Volatile
    private var interstitialAd: InterstitialAd? = null

    @Volatile
    private var rewardedAd: RewardedAd? = null

    @Volatile
    private var rewardedInterstitialAd: RewardedInterstitialAd? = null

    @Volatile
    private var isShowingFullScreenAdInternal = false
    private var lastNavigationInterstitialAt = 0L
    private var lastNavigationInterstitialRoute = ""

    @Volatile
    private var nativePreloadInFlight = false

    @Volatile
    private var preloadedNativePayload: JSONObject? = null

    @Volatile
    private var preloadedNativeAdUnitId: String = ""

    fun primeAds(context: Context) {
        if (!remoteConfigManager.areAdsEnabled()) {
            Log.i(tag, "[Ads][Android] Prime skipped because ads are disabled.")
            return
        }
        Log.i(
            tag,
            "[Ads][Android] Prime begin usingTestAds=${remoteConfigManager.isUsingTestAds()} " +
                "debug=${remoteConfigManager.getAdsEnvironment().isDebugBuild} " +
                "emulator=${remoteConfigManager.getAdsEnvironment().isSimulatorOrEmulator}"
        )
        Log.d(tag, "Priming interstitial and rewarded ads.")
        loadInterstitial(context)
        loadRewarded(context)
        loadRewardedInterstitial(context)
        preloadNative(context)
    }

    fun preloadAllFormatsForQa(activity: Activity) {
        primeAds(activity)
        requestNativeFeedAd(
            activity = activity,
            slotId = "qa-native-slot",
            placement = "qa_panel",
            variant = "nativeAdvanced"
        ) { result ->
            val ok = result.optBoolean("ok", false)
            val reason = result.optString("error", "")
            if (ok) {
                AdDebugStatusRegistry.updateLoad("native", "loaded")
            } else {
                val normalized = reason.ifBlank { "native_request_failed" }
                AdDebugStatusRegistry.updateLoad("native", "failed:$normalized")
            }
        }
    }

    fun debugStatusSnapshots(): List<AdFormatStatusSnapshot> {
        return AdDebugStatusRegistry.snapshot(knownFormats.map { it.name.lowercase() })
    }

    fun showInterstitial(
        activity: Activity,
        requestId: String = "",
        callback: (JSONObject) -> Unit = {},
    ) {
        if (!remoteConfigManager.areAdsEnabled()) {
            emitAdActionResult(
                callback = callback,
                requestId = requestId,
                adFormat = "interstitial",
                status = "skipped:ads_disabled",
                message = "Ads are disabled."
            )
            return
        }
        if (isShowingFullScreenAdInternal) {
            Log.d(tag, "Interstitial show skipped; fullscreen ad already visible.")
            AdDebugStatusRegistry.updateShow("interstitial", "skipped:fullscreen_visible")
            emitAdActionResult(
                callback = callback,
                requestId = requestId,
                adFormat = "interstitial",
                status = "skipped:fullscreen_visible",
                message = "Another fullscreen ad is currently visible."
            )
            return
        }
        val cachedAd = interstitialAd
        if (cachedAd == null) {
            Log.d(tag, "Interstitial unavailable; loading.")
            AdDebugStatusRegistry.updateShow("interstitial", "skipped:not_ready")
            emitAdActionResult(
                callback = callback,
                requestId = requestId,
                adFormat = "interstitial",
                status = "skipped:not_ready",
                message = "Interstitial ad is still loading."
            )
            loadInterstitial(activity)
            return
        }

        cachedAd.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdShowedFullScreenContent() {
                isShowingFullScreenAdInternal = true
                Log.d(tag, "Interstitial shown.")
                Log.i(tag, "[Ads][Android] Show success for interstitial")
                AdDebugStatusRegistry.updateShow("interstitial", "shown")
                emitAdActionResult(
                    callback = callback,
                    requestId = requestId,
                    adFormat = "interstitial",
                    status = "shown"
                )
            }

            override fun onAdImpression() {
                val adUnitId = remoteConfigManager.resolveAdUnitId(
                    platform = AdPlatform.ANDROID,
                    format = AdFormat.INTERSTITIAL
                )
                AdImpressionReporter.report(
                    context = activity,
                    adFormat = "interstitial",
                    adUnitId = adUnitId,
                    placement = "navigation"
                )
            }

            override fun onAdDismissedFullScreenContent() {
                isShowingFullScreenAdInternal = false
                Log.d(tag, "Interstitial dismissed.")
                interstitialAd = null
                AdDebugStatusRegistry.updateShow("interstitial", "dismissed")
                emitAdActionResult(
                    callback = callback,
                    requestId = requestId,
                    adFormat = "interstitial",
                    status = "dismissed"
                )
                loadInterstitial(activity)
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                isShowingFullScreenAdInternal = false
                Log.w(tag, "Interstitial failed to show: ${adError.message}")
                interstitialAd = null
                Log.w(tag, "[Ads][Android] Show fail for interstitial: ${adError.message}")
                AdDebugStatusRegistry.updateShow("interstitial", "failed:${adError.message}")
                emitAdActionResult(
                    callback = callback,
                    requestId = requestId,
                    adFormat = "interstitial",
                    status = "failed",
                    message = adError.message
                )
                loadInterstitial(activity)
            }
        }
        Log.d(tag, "Presenting interstitial ad.")
        cachedAd.show(activity)
    }

    fun showRewarded(
        activity: Activity,
        requestId: String = "",
        preferRewardedInterstitial: Boolean = true,
        onReward: (RewardItem) -> Unit = {},
        callback: (JSONObject) -> Unit = {},
    ) {
        if (!remoteConfigManager.areAdsEnabled()) {
            emitAdActionResult(
                callback = callback,
                requestId = requestId,
                adFormat = if (preferRewardedInterstitial) "rewarded_interstitial" else "rewarded",
                status = "skipped:ads_disabled",
                message = "Ads are disabled."
            )
            return
        }
        if (isShowingFullScreenAdInternal) {
            Log.d(tag, "Rewarded show skipped; fullscreen ad already visible.")
            AdDebugStatusRegistry.updateShow("rewarded", "skipped:fullscreen_visible")
            AdDebugStatusRegistry.updateShow("rewarded_interstitial", "skipped:fullscreen_visible")
            emitAdActionResult(
                callback = callback,
                requestId = requestId,
                adFormat = if (preferRewardedInterstitial) "rewarded_interstitial" else "rewarded",
                status = "skipped:fullscreen_visible",
                message = "Another fullscreen ad is currently visible."
            )
            return
        }
        val rewardedInterstitial = if (preferRewardedInterstitial) rewardedInterstitialAd else null
        if (rewardedInterstitial != null) {
            rewardedInterstitial.fullScreenContentCallback = object : FullScreenContentCallback() {
                override fun onAdShowedFullScreenContent() {
                    isShowingFullScreenAdInternal = true
                    Log.d(tag, "Rewarded interstitial ad shown.")
                    Log.i(tag, "[Ads][Android] Show success for rewarded_interstitial")
                    AdDebugStatusRegistry.updateShow("rewarded_interstitial", "shown")
                    emitAdActionResult(
                        callback = callback,
                        requestId = requestId,
                        adFormat = "rewarded_interstitial",
                        status = "shown"
                    )
                }

                override fun onAdImpression() {
                    val adUnitId = remoteConfigManager.resolveAdUnitId(
                        platform = AdPlatform.ANDROID,
                        format = AdFormat.REWARDED_INTERSTITIAL
                    )
                    AdImpressionReporter.report(
                        context = activity,
                        adFormat = "rewarded_interstitial",
                        adUnitId = adUnitId,
                        placement = "reward_action"
                    )
                }

                override fun onAdDismissedFullScreenContent() {
                    isShowingFullScreenAdInternal = false
                    Log.d(tag, "Rewarded interstitial ad dismissed.")
                    rewardedInterstitialAd = null
                    AdDebugStatusRegistry.updateShow("rewarded_interstitial", "dismissed")
                    emitAdActionResult(
                        callback = callback,
                        requestId = requestId,
                        adFormat = "rewarded_interstitial",
                        status = "dismissed"
                    )
                    loadRewardedInterstitial(activity)
                }

                override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                    isShowingFullScreenAdInternal = false
                    Log.w(tag, "Rewarded interstitial failed to show: ${adError.message}")
                    rewardedInterstitialAd = null
                    Log.w(tag, "[Ads][Android] Show fail for rewarded_interstitial: ${adError.message}")
                    AdDebugStatusRegistry.updateShow("rewarded_interstitial", "failed:${adError.message}")
                    emitAdActionResult(
                        callback = callback,
                        requestId = requestId,
                        adFormat = "rewarded_interstitial",
                        status = "failed",
                        message = adError.message
                    )
                    loadRewardedInterstitial(activity)
                }
            }
            Log.d(tag, "Presenting rewarded interstitial ad.")
            rewardedInterstitial.show(activity) { reward ->
                emitAdActionResult(
                    callback = callback,
                    requestId = requestId,
                    adFormat = "rewarded_interstitial",
                    status = "rewarded",
                    rewardType = reward.type,
                    rewardAmount = reward.amount.toDouble()
                )
                onReward(reward)
            }
            return
        }

        val cachedAd = rewardedAd
        if (cachedAd == null) {
            Log.d(tag, "Rewarded unavailable; loading.")
            AdDebugStatusRegistry.updateShow("rewarded", "skipped:not_ready")
            emitAdActionResult(
                callback = callback,
                requestId = requestId,
                adFormat = "rewarded",
                status = "skipped:not_ready",
                message = "Rewarded ad is still loading."
            )
            loadRewarded(activity)
            loadRewardedInterstitial(activity)
            return
        }

        cachedAd.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdShowedFullScreenContent() {
                isShowingFullScreenAdInternal = true
                Log.d(tag, "Rewarded ad shown.")
                Log.i(tag, "[Ads][Android] Show success for rewarded")
                AdDebugStatusRegistry.updateShow("rewarded", "shown")
                emitAdActionResult(
                    callback = callback,
                    requestId = requestId,
                    adFormat = "rewarded",
                    status = "shown"
                )
            }

            override fun onAdImpression() {
                val adUnitId = remoteConfigManager.resolveAdUnitId(
                    platform = AdPlatform.ANDROID,
                    format = AdFormat.REWARDED
                )
                AdImpressionReporter.report(
                    context = activity,
                    adFormat = "rewarded",
                    adUnitId = adUnitId,
                    placement = "reward_action"
                )
            }

            override fun onAdDismissedFullScreenContent() {
                isShowingFullScreenAdInternal = false
                Log.d(tag, "Rewarded ad dismissed.")
                rewardedAd = null
                AdDebugStatusRegistry.updateShow("rewarded", "dismissed")
                emitAdActionResult(
                    callback = callback,
                    requestId = requestId,
                    adFormat = "rewarded",
                    status = "dismissed"
                )
                loadRewarded(activity)
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                isShowingFullScreenAdInternal = false
                Log.w(tag, "Rewarded failed to show: ${adError.message}")
                rewardedAd = null
                Log.w(tag, "[Ads][Android] Show fail for rewarded: ${adError.message}")
                AdDebugStatusRegistry.updateShow("rewarded", "failed:${adError.message}")
                emitAdActionResult(
                    callback = callback,
                    requestId = requestId,
                    adFormat = "rewarded",
                    status = "failed",
                    message = adError.message
                )
                loadRewarded(activity)
            }
        }
        Log.d(tag, "Presenting rewarded ad.")
        cachedAd.show(activity) { reward ->
            emitAdActionResult(
                callback = callback,
                requestId = requestId,
                adFormat = "rewarded",
                status = "rewarded",
                rewardType = reward.type,
                rewardAmount = reward.amount.toDouble()
            )
            onReward(reward)
        }
    }

    fun onPrimaryNavigation(activity: Activity, url: String) {
        val normalized = url.trim()
        if (normalized.isEmpty()) return
        val uri = runCatching { Uri.parse(normalized) }.getOrNull() ?: return
        if (!shouldTriggerPassiveInterstitial(uri)) return
        val now = SystemClock.elapsedRealtime()
        val routeKey = buildString {
            append(uri.host?.lowercase().orEmpty())
            append(uri.path.orEmpty().lowercase())
        }
        if (routeKey == lastNavigationInterstitialRoute && now - lastNavigationInterstitialAt < 20_000L) return
        if (now - lastNavigationInterstitialAt < PASSIVE_INTERSTITIAL_COOLDOWN_MS) return
        lastNavigationInterstitialAt = now
        lastNavigationInterstitialRoute = routeKey
        Log.d(tag, "Passive navigation trigger for interstitial url=$normalized")
        showInterstitial(activity)
    }

    fun isShowingFullScreenAd(): Boolean = isShowingFullScreenAdInternal

    fun onPause() {
        // Hook point for future ad SDK pause logic.
    }

    fun onResume(context: Context) {
        if (interstitialAd == null || rewardedAd == null || rewardedInterstitialAd == null) {
            primeAds(context)
        }
    }

    fun requestNativeFeedAd(
        activity: Activity,
        slotId: String,
        placement: String,
        variant: String = "nativeAdvanced",
        callback: (JSONObject) -> Unit,
    ) {
        val trimmedSlotId = slotId.trim()
        val normalizedPlacement = placement.trim().ifEmpty { "feed" }
        if (trimmedSlotId.isEmpty()) {
            AdDebugStatusRegistry.updateLoad("native", "failed:slot_id_missing")
            callback(buildNativeAdError("", normalizedPlacement, "slot_id_missing"))
            return
        }
        if (!remoteConfigManager.areAdsEnabled()) {
            AdDebugStatusRegistry.updateLoad("native", "failed:ads_disabled")
            callback(buildNativeAdError(trimmedSlotId, normalizedPlacement, "ads_disabled"))
            return
        }

        val cachedPayload = preloadedNativePayload
        if (cachedPayload != null) {
            preloadedNativePayload = null
            Log.i(tag, "[Ads][Android] Served native ad from preload cache slot=$trimmedSlotId")
            callback(
                JSONObject()
                    .put("ok", true)
                    .put("slotId", trimmedSlotId)
                    .put("placement", normalizedPlacement)
                    .put("adUnitId", preloadedNativeAdUnitId)
                    .put("ad", cachedPayload)
            )
            preloadNative(activity.applicationContext)
            return
        }

        val adUnitId = if (variant.trim().equals("nativeVideo", ignoreCase = true)) {
            remoteConfigManager.getAdUnitIds().nativeVideo
        } else {
            remoteConfigManager.resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.NATIVE
            )
        }

        logNativeAdEvent(
            context = activity,
            eventName = "ad_request",
            placement = normalizedPlacement,
            adUnitId = adUnitId,
            slotId = trimmedSlotId
        )

        try {
            val adLoader = AdLoader.Builder(activity, adUnitId)
                .forNativeAd { nativeAd ->
                    logNativeAdEvent(
                        context = activity,
                        eventName = "ad_loaded",
                        placement = normalizedPlacement,
                        adUnitId = adUnitId,
                        slotId = trimmedSlotId
                    )
                    Log.i(tag, "[Ads][Android] Load success for native")
                    AdDebugStatusRegistry.updateLoad("native", "loaded")
                    val response = JSONObject()
                        .put("ok", true)
                        .put("slotId", trimmedSlotId)
                        .put("placement", normalizedPlacement)
                        .put("adUnitId", adUnitId)
                        .put("ad", serializeNativeAd(nativeAd, adUnitId))
                    callback(response)
                    nativeAd.destroy()
                }
                .withAdListener(object : AdListener() {
                    override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                        val reason = loadAdError.message.ifBlank { "native_load_failed" }
                        logNativeAdEvent(
                            context = activity,
                            eventName = "ad_failed",
                            placement = normalizedPlacement,
                            adUnitId = adUnitId,
                            slotId = trimmedSlotId,
                            reason = reason
                        )
                        Log.w(tag, "[Ads][Android] Load fail for native: $reason")
                        AdDebugStatusRegistry.updateLoad("native", "failed:$reason")
                        callback(buildNativeAdError(trimmedSlotId, normalizedPlacement, reason))
                    }
                })
                .build()
            adLoader.loadAd(AdRequest.Builder().build())
        } catch (error: Exception) {
            val reason = error.message?.trim().orEmpty().ifEmpty { "native_request_failed" }
            logNativeAdEvent(
                context = activity,
                eventName = "ad_failed",
                placement = normalizedPlacement,
                adUnitId = adUnitId,
                slotId = trimmedSlotId,
                reason = reason
            )
            Log.w(tag, "[Ads][Android] Load fail for native: $reason")
            AdDebugStatusRegistry.updateLoad("native", "failed:$reason")
            callback(buildNativeAdError(trimmedSlotId, normalizedPlacement, reason))
        }
    }

    private fun preloadNative(context: Context) {
        if (!remoteConfigManager.areAdsEnabled()) return
        if (nativePreloadInFlight || preloadedNativePayload != null) return

        val adUnitId = remoteConfigManager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.NATIVE
        )
        nativePreloadInFlight = true
        Log.d(tag, "Preloading native ad unit=$adUnitId")
        AdDebugStatusRegistry.updateLoad("native", "loading")
        try {
            AdLoader.Builder(context, adUnitId)
                .forNativeAd { nativeAd ->
                    preloadedNativePayload = serializeNativeAd(nativeAd, adUnitId)
                    preloadedNativeAdUnitId = adUnitId
                    nativePreloadInFlight = false
                    Log.i(tag, "[Ads][Android] Load success for native preload")
                    AdDebugStatusRegistry.updateLoad("native", "loaded")
                    nativeAd.destroy()
                }
                .withAdListener(object : AdListener() {
                    override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                        nativePreloadInFlight = false
                        val reason = loadAdError.message.ifBlank { "native_preload_failed" }
                        Log.w(tag, "[Ads][Android] Load fail for native preload: $reason")
                        AdDebugStatusRegistry.updateLoad("native", "failed:$reason")
                    }
                })
                .build()
                .loadAd(AdRequest.Builder().build())
        } catch (error: Exception) {
            nativePreloadInFlight = false
            val reason = error.message?.trim().orEmpty().ifEmpty { "native_preload_exception" }
            Log.w(tag, "[Ads][Android] Native preload failed: $reason")
            AdDebugStatusRegistry.updateLoad("native", "failed:$reason")
        }
    }

    private fun shouldTriggerPassiveInterstitial(uri: Uri): Boolean {
        val host = uri.host?.lowercase().orEmpty()
        if (host.isNotEmpty() && host != "quantura.studio" && host != "www.quantura.studio") {
            return false
        }
        val path = uri.path.orEmpty().lowercase()
        if (path.isBlank()) return false
        return path.startsWith("/blog") ||
            path.startsWith("/news") ||
            path.startsWith("/research") ||
            path.startsWith("/terminal/fx") ||
            path.startsWith("/currency") ||
            path.startsWith("/terminal/currency")
    }

    fun reportNativeFeedAdImpression(
        context: Context,
        slotId: String,
        placement: String,
        adUnitId: String,
    ) {
        val normalizedPlacement = placement.trim().ifEmpty { "feed" }
        val unit = adUnitId.trim().ifEmpty {
            remoteConfigManager.resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.NATIVE
            )
        }
        AdImpressionReporter.report(
            context = context,
            adFormat = "native",
            adUnitId = unit,
            placement = normalizedPlacement
        )
        AdDebugStatusRegistry.updateShow("native", "impression")
        logNativeAdEvent(
            context = context,
            eventName = "ad_impression",
            placement = normalizedPlacement,
            adUnitId = unit,
            slotId = slotId.trim()
        )
    }

    fun reportNativeFeedAdClick(
        context: Context,
        slotId: String,
        placement: String,
        adUnitId: String,
    ) {
        val normalizedPlacement = placement.trim().ifEmpty { "feed" }
        val unit = adUnitId.trim().ifEmpty {
            remoteConfigManager.resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.NATIVE
            )
        }
        logNativeAdEvent(
            context = context,
            eventName = "ad_click",
            placement = normalizedPlacement,
            adUnitId = unit,
            slotId = slotId.trim()
        )
    }

    private fun loadInterstitial(context: Context) {
        val adUnitId = remoteConfigManager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.INTERSTITIAL
        )
        Log.d(tag, "Loading interstitial unit=$adUnitId")
        AdDebugStatusRegistry.updateLoad("interstitial", "loading")
        InterstitialAd.load(
            context,
            adUnitId,
            AdRequest.Builder().build(),
            object : InterstitialAdLoadCallback() {
                override fun onAdLoaded(ad: InterstitialAd) {
                    interstitialAd = ad
                    Log.d(tag, "Interstitial load succeeded.")
                    Log.i(tag, "[Ads][Android] Load success for interstitial")
                    AdDebugStatusRegistry.updateLoad("interstitial", "loaded")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    interstitialAd = null
                    Log.w(tag, "Interstitial load failed: ${loadAdError.message}")
                    Log.w(tag, "[Ads][Android] Load fail for interstitial: ${loadAdError.message}")
                    AdDebugStatusRegistry.updateLoad("interstitial", "failed:${loadAdError.message}")
                }
            }
        )
    }

    private fun loadRewarded(context: Context) {
        val adUnitId = remoteConfigManager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.REWARDED
        )
        Log.d(tag, "Loading rewarded unit=$adUnitId")
        AdDebugStatusRegistry.updateLoad("rewarded", "loading")
        RewardedAd.load(
            context,
            adUnitId,
            AdRequest.Builder().build(),
            object : RewardedAdLoadCallback() {
                override fun onAdLoaded(ad: RewardedAd) {
                    rewardedAd = ad
                    configureRewardedSsv(ad, "rewarded")
                    Log.d(tag, "Rewarded load succeeded.")
                    Log.i(tag, "[Ads][Android] Load success for rewarded")
                    AdDebugStatusRegistry.updateLoad("rewarded", "loaded")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    rewardedAd = null
                    Log.w(tag, "Rewarded load failed: ${loadAdError.message}")
                    Log.w(tag, "[Ads][Android] Load fail for rewarded: ${loadAdError.message}")
                    AdDebugStatusRegistry.updateLoad("rewarded", "failed:${loadAdError.message}")
                }
            }
        )
    }

    private fun loadRewardedInterstitial(context: Context) {
        val adUnitId = remoteConfigManager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.REWARDED_INTERSTITIAL
        )
        Log.d(tag, "Loading rewarded interstitial unit=$adUnitId")
        AdDebugStatusRegistry.updateLoad("rewarded_interstitial", "loading")
        RewardedInterstitialAd.load(
            context,
            adUnitId,
            AdRequest.Builder().build(),
            object : RewardedInterstitialAdLoadCallback() {
                override fun onAdLoaded(ad: RewardedInterstitialAd) {
                    rewardedInterstitialAd = ad
                    configureRewardedInterstitialSsv(ad, "rewarded_interstitial")
                    Log.d(tag, "Rewarded interstitial load succeeded.")
                    Log.i(tag, "[Ads][Android] Load success for rewarded_interstitial")
                    AdDebugStatusRegistry.updateLoad("rewarded_interstitial", "loaded")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    rewardedInterstitialAd = null
                    Log.w(tag, "Rewarded interstitial load failed: ${loadAdError.message}")
                    Log.w(
                        tag,
                        "[Ads][Android] Load fail for rewarded_interstitial: ${loadAdError.message}"
                    )
                    AdDebugStatusRegistry.updateLoad(
                        "rewarded_interstitial",
                        "failed:${loadAdError.message}"
                    )
                }
            }
        )
    }

    private fun configureRewardedSsv(ad: RewardedAd, adFormat: String) {
        val uid = FirebaseAuth.getInstance().currentUser?.uid?.trim().orEmpty()
        val optionsBuilder = ServerSideVerificationOptions.Builder()
            .setCustomData(buildSsvCustomData(uid, adFormat))
        if (uid.isNotEmpty()) optionsBuilder.setUserId(uid.take(120))
        ad.setServerSideVerificationOptions(optionsBuilder.build())
    }

    private fun configureRewardedInterstitialSsv(ad: RewardedInterstitialAd, adFormat: String) {
        val uid = FirebaseAuth.getInstance().currentUser?.uid?.trim().orEmpty()
        val optionsBuilder = ServerSideVerificationOptions.Builder()
            .setCustomData(buildSsvCustomData(uid, adFormat))
        if (uid.isNotEmpty()) optionsBuilder.setUserId(uid.take(120))
        ad.setServerSideVerificationOptions(optionsBuilder.build())
    }

    private fun buildSsvCustomData(uid: String, adFormat: String): String {
        return JSONObject()
            .put("platform", "android")
            .put("uid", uid)
            .put("adFormat", adFormat)
            .put("ts", System.currentTimeMillis())
            .toString()
            .take(450)
    }

    private fun buildNativeAdError(slotId: String, placement: String, reason: String): JSONObject {
        return JSONObject()
            .put("ok", false)
            .put("slotId", slotId)
            .put("placement", placement)
            .put("error", reason.take(220))
    }

    private fun serializeNativeAd(ad: NativeAd, adUnitId: String): JSONObject {
        val iconDataUrl = drawableToDataUrl(ad.icon?.drawable)
        val mediaDrawable = ad.images.firstOrNull()?.drawable
        val mediaDataUrl = drawableToDataUrl(mediaDrawable)
        return JSONObject()
            .put("headline", ad.headline ?: "")
            .put("body", ad.body ?: "")
            .put("callToAction", ad.callToAction ?: "")
            .put("advertiser", ad.advertiser ?: "")
            .put("store", ad.store ?: "")
            .put("price", ad.price ?: "")
            .put("starRating", ad.starRating ?: JSONObject.NULL)
            .put("iconDataUrl", iconDataUrl)
            .put("mediaDataUrl", mediaDataUrl)
            .put("hasVideoContent", ad.mediaContent?.hasVideoContent() ?: false)
            .put("adUnitId", adUnitId)
    }

    private fun drawableToDataUrl(drawable: Drawable?): String {
        if (drawable == null) return ""
        val bitmap = when (drawable) {
            is BitmapDrawable -> drawable.bitmap
            else -> {
                val width = if (drawable.intrinsicWidth > 0) drawable.intrinsicWidth else 200
                val height = if (drawable.intrinsicHeight > 0) drawable.intrinsicHeight else 120
                val target = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)
                val canvas = Canvas(target)
                drawable.setBounds(0, 0, canvas.width, canvas.height)
                drawable.draw(canvas)
                target
            }
        }
        val out = ByteArrayOutputStream()
        return try {
            bitmap.compress(Bitmap.CompressFormat.PNG, 90, out)
            val encoded = Base64.encodeToString(out.toByteArray(), Base64.NO_WRAP)
            "data:image/png;base64,$encoded"
        } catch (_: Exception) {
            ""
        } finally {
            try {
                out.close()
            } catch (_: Exception) {
                // Ignore close errors.
            }
        }
    }

    private fun logNativeAdEvent(
        context: Context,
        eventName: String,
        placement: String,
        adUnitId: String,
        slotId: String,
        reason: String = "",
    ) {
        val payload = Bundle().apply {
            putString(FirebaseAnalytics.Param.AD_PLATFORM, "admob")
            putString(FirebaseAnalytics.Param.AD_SOURCE, "admob")
            putString(FirebaseAnalytics.Param.AD_FORMAT, "native")
            putString(FirebaseAnalytics.Param.AD_UNIT_NAME, adUnitId)
            putString("placement", placement.take(80))
            putString("slot_id", slotId.take(80))
            if (reason.isNotBlank()) putString("reason", reason.take(120))
        }
        FirebaseAnalytics.getInstance(context).logEvent(eventName, payload)
    }

    private fun emitAdActionResult(
        callback: (JSONObject) -> Unit,
        requestId: String,
        adFormat: String,
        status: String,
        message: String = "",
        rewardType: String = "",
        rewardAmount: Double? = null,
    ) {
        runCatching {
            callback(
                JSONObject()
                    .put("requestId", requestId)
                    .put("adFormat", adFormat)
                    .put("status", status)
                    .put("message", message)
                    .put("rewardType", rewardType)
                    .put("rewardAmount", rewardAmount ?: JSONObject.NULL)
            )
        }
    }

    companion object {
        private const val PASSIVE_INTERSTITIAL_COOLDOWN_MS = 45_000L
    }
}
