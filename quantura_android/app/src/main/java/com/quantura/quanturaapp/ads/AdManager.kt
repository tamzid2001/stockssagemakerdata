package com.quantura.quanturaapp.ads

import android.app.Activity
import android.content.Context
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.drawable.BitmapDrawable
import android.graphics.drawable.Drawable
import android.os.SystemClock
import android.os.Bundle
import android.util.Base64
import android.util.Log
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

    @Volatile
    private var interstitialAd: InterstitialAd? = null

    @Volatile
    private var rewardedAd: RewardedAd? = null

    @Volatile
    private var rewardedInterstitialAd: RewardedInterstitialAd? = null

    @Volatile
    private var isShowingFullScreenAdInternal = false
    private var lastNavigationInterstitialAt = 0L

    fun primeAds(context: Context) {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        Log.d(tag, "Priming interstitial and rewarded ads.")
        loadInterstitial(context)
        loadRewarded(context)
        loadRewardedInterstitial(context)
    }

    fun showInterstitial(activity: Activity) {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        if (isShowingFullScreenAdInternal) {
            Log.d(tag, "Interstitial show skipped; fullscreen ad already visible.")
            return
        }
        val cachedAd = interstitialAd
        if (cachedAd == null) {
            Log.d(tag, "Interstitial unavailable; loading.")
            loadInterstitial(activity)
            return
        }

        cachedAd.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdShowedFullScreenContent() {
                isShowingFullScreenAdInternal = true
                Log.d(tag, "Interstitial shown.")
            }

            override fun onAdImpression() {
                val adUnitId = remoteConfigManager.getAdUnitIds().interstitial
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
                loadInterstitial(activity)
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                isShowingFullScreenAdInternal = false
                Log.w(tag, "Interstitial failed to show: ${adError.message}")
                interstitialAd = null
                loadInterstitial(activity)
            }
        }
        Log.d(tag, "Presenting interstitial ad.")
        cachedAd.show(activity)
    }

    fun showRewarded(activity: Activity, onReward: (RewardItem) -> Unit = {}) {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        if (isShowingFullScreenAdInternal) {
            Log.d(tag, "Rewarded show skipped; fullscreen ad already visible.")
            return
        }
        val rewardedInterstitial = rewardedInterstitialAd
        if (rewardedInterstitial != null) {
            rewardedInterstitial.fullScreenContentCallback = object : FullScreenContentCallback() {
                override fun onAdShowedFullScreenContent() {
                    isShowingFullScreenAdInternal = true
                    Log.d(tag, "Rewarded interstitial ad shown.")
                }

                override fun onAdImpression() {
                    val adUnitId = remoteConfigManager.getAdUnitIds().rewardedInterstitial
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
                    loadRewardedInterstitial(activity)
                }

                override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                    isShowingFullScreenAdInternal = false
                    Log.w(tag, "Rewarded interstitial failed to show: ${adError.message}")
                    rewardedInterstitialAd = null
                    loadRewardedInterstitial(activity)
                }
            }
            Log.d(tag, "Presenting rewarded interstitial ad.")
            rewardedInterstitial.show(activity) { reward -> onReward(reward) }
            return
        }

        val cachedAd = rewardedAd
        if (cachedAd == null) {
            Log.d(tag, "Rewarded unavailable; loading.")
            loadRewarded(activity)
            loadRewardedInterstitial(activity)
            return
        }

        cachedAd.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdShowedFullScreenContent() {
                isShowingFullScreenAdInternal = true
                Log.d(tag, "Rewarded ad shown.")
            }

            override fun onAdImpression() {
                val adUnitId = remoteConfigManager.getAdUnitIds().rewarded
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
                loadRewarded(activity)
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                isShowingFullScreenAdInternal = false
                Log.w(tag, "Rewarded failed to show: ${adError.message}")
                rewardedAd = null
                loadRewarded(activity)
            }
        }
        Log.d(tag, "Presenting rewarded ad.")
        cachedAd.show(activity) { reward -> onReward(reward) }
    }

    fun onPrimaryNavigation(activity: Activity, url: String) {
        val normalized = url.trim()
        if (normalized.isEmpty()) return
        val now = SystemClock.elapsedRealtime()
        if (now - lastNavigationInterstitialAt < 3_000L) return
        lastNavigationInterstitialAt = now
        Log.d(tag, "Navigation trigger for interstitial url=$normalized")
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
            callback(buildNativeAdError("", normalizedPlacement, "slot_id_missing"))
            return
        }
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) {
            callback(buildNativeAdError(trimmedSlotId, normalizedPlacement, "ads_disabled"))
            return
        }

        val adUnits = remoteConfigManager.getAdUnitIds()
        val adUnitId = if (variant.trim().equals("nativeVideo", ignoreCase = true)) {
            adUnits.nativeVideo
        } else {
            adUnits.nativeAdvanced
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
            callback(buildNativeAdError(trimmedSlotId, normalizedPlacement, reason))
        }
    }

    fun reportNativeFeedAdImpression(
        context: Context,
        slotId: String,
        placement: String,
        adUnitId: String,
    ) {
        val normalizedPlacement = placement.trim().ifEmpty { "feed" }
        val unit = adUnitId.trim().ifEmpty { remoteConfigManager.getAdUnitIds().nativeAdvanced }
        AdImpressionReporter.report(
            context = context,
            adFormat = "native",
            adUnitId = unit,
            placement = normalizedPlacement
        )
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
        val unit = adUnitId.trim().ifEmpty { remoteConfigManager.getAdUnitIds().nativeAdvanced }
        logNativeAdEvent(
            context = context,
            eventName = "ad_click",
            placement = normalizedPlacement,
            adUnitId = unit,
            slotId = slotId.trim()
        )
    }

    private fun loadInterstitial(context: Context) {
        val adUnits = remoteConfigManager.getAdUnitIds()
        Log.d(tag, "Loading interstitial unit=${adUnits.interstitial}")
        InterstitialAd.load(
            context,
            adUnits.interstitial,
            AdRequest.Builder().build(),
            object : InterstitialAdLoadCallback() {
                override fun onAdLoaded(ad: InterstitialAd) {
                    interstitialAd = ad
                    Log.d(tag, "Interstitial load succeeded.")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    interstitialAd = null
                    Log.w(tag, "Interstitial load failed: ${loadAdError.message}")
                }
            }
        )
    }

    private fun loadRewarded(context: Context) {
        val adUnits = remoteConfigManager.getAdUnitIds()
        Log.d(tag, "Loading rewarded unit=${adUnits.rewarded}")
        RewardedAd.load(
            context,
            adUnits.rewarded,
            AdRequest.Builder().build(),
            object : RewardedAdLoadCallback() {
                override fun onAdLoaded(ad: RewardedAd) {
                    rewardedAd = ad
                    configureRewardedSsv(ad, "rewarded")
                    Log.d(tag, "Rewarded load succeeded.")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    rewardedAd = null
                    Log.w(tag, "Rewarded load failed: ${loadAdError.message}")
                }
            }
        )
    }

    private fun loadRewardedInterstitial(context: Context) {
        val adUnits = remoteConfigManager.getAdUnitIds()
        Log.d(tag, "Loading rewarded interstitial unit=${adUnits.rewardedInterstitial}")
        RewardedInterstitialAd.load(
            context,
            adUnits.rewardedInterstitial,
            AdRequest.Builder().build(),
            object : RewardedInterstitialAdLoadCallback() {
                override fun onAdLoaded(ad: RewardedInterstitialAd) {
                    rewardedInterstitialAd = ad
                    configureRewardedInterstitialSsv(ad, "rewarded_interstitial")
                    Log.d(tag, "Rewarded interstitial load succeeded.")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    rewardedInterstitialAd = null
                    Log.w(tag, "Rewarded interstitial load failed: ${loadAdError.message}")
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
}
