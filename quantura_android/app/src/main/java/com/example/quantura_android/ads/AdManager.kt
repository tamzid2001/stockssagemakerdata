package com.example.quantura_android.ads

import android.app.Activity
import android.content.Context
import com.example.quantura_android.config.RemoteConfigManager
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
    @Volatile
    private var interstitialAd: InterstitialAd? = null

    @Volatile
    private var rewardedAd: RewardedAd? = null

    fun primeAds(context: Context) {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        loadInterstitial(context)
        loadRewarded(context)
    }

    fun showInterstitial(activity: Activity) {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        val cachedAd = interstitialAd
        if (cachedAd == null) {
            loadInterstitial(activity)
            return
        }

        cachedAd.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdDismissedFullScreenContent() {
                interstitialAd = null
                loadInterstitial(activity)
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                interstitialAd = null
                loadInterstitial(activity)
            }
        }
        cachedAd.show(activity)
    }

    fun showRewarded(activity: Activity, onReward: (RewardItem) -> Unit = {}) {
        if (!remoteConfigManager.isFeatureEnabled("ads_enabled")) return
        val cachedAd = rewardedAd
        if (cachedAd == null) {
            loadRewarded(activity)
            return
        }

        cachedAd.fullScreenContentCallback = object : FullScreenContentCallback() {
            override fun onAdDismissedFullScreenContent() {
                rewardedAd = null
                loadRewarded(activity)
            }

            override fun onAdFailedToShowFullScreenContent(adError: com.google.android.gms.ads.AdError) {
                rewardedAd = null
                loadRewarded(activity)
            }
        }
        cachedAd.show(activity) { reward -> onReward(reward) }
    }

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
        InterstitialAd.load(
            context,
            adUnits.interstitial,
            AdRequest.Builder().build(),
            object : InterstitialAdLoadCallback() {
                override fun onAdLoaded(ad: InterstitialAd) {
                    interstitialAd = ad
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    interstitialAd = null
                }
            }
        )
    }

    private fun loadRewarded(context: Context) {
        val adUnits = remoteConfigManager.getAdUnitIds()
        RewardedAd.load(
            context,
            adUnits.rewarded,
            AdRequest.Builder().build(),
            object : RewardedAdLoadCallback() {
                override fun onAdLoaded(ad: RewardedAd) {
                    rewardedAd = ad
                }

                override fun onAdFailedToLoad(loadAdError: LoadAdError) {
                    rewardedAd = null
                }
            }
        )
    }
}
