package com.quantura.quanturaapp.ads

import android.app.Activity
import android.content.Context
import android.os.SystemClock
import android.util.Log
import com.quantura.quanturaapp.config.RemoteConfigManager
import com.google.android.gms.ads.AdRequest
import com.google.android.gms.ads.FullScreenContentCallback
import com.google.android.gms.ads.LoadAdError
import com.google.android.gms.ads.interstitial.InterstitialAd
import com.google.android.gms.ads.interstitial.InterstitialAdLoadCallback
import com.google.android.gms.ads.rewarded.RewardItem
import com.google.android.gms.ads.rewarded.RewardedAd
import com.google.android.gms.ads.rewarded.RewardedAdLoadCallback

class AdManager(
    private val remoteConfigManager: RemoteConfigManager,
) {
    private val tag = "AdManager"

    @Volatile
    private var interstitialAd: InterstitialAd? = null

    @Volatile
    private var rewardedAd: RewardedAd? = null

    @Volatile
    private var isShowingFullScreenAdInternal = false
    private var lastNavigationInterstitialAt = 0L

    fun primeAds(context: Context) {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        Log.d(tag, "Priming interstitial and rewarded ads.")
        loadInterstitial(context)
        loadRewarded(context)
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
        val cachedAd = rewardedAd
        if (cachedAd == null) {
            Log.d(tag, "Rewarded unavailable; loading.")
            loadRewarded(activity)
            return
        }

        cachedAd.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdShowedFullScreenContent() {
                isShowingFullScreenAdInternal = true
                Log.d(tag, "Rewarded ad shown.")
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
        if (interstitialAd == null || rewardedAd == null) {
            primeAds(context)
        }
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
                    Log.d(tag, "Rewarded load succeeded.")
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    rewardedAd = null
                    Log.w(tag, "Rewarded load failed: ${loadAdError.message}")
                }
            }
        )
    }
}
