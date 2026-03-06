package com.quantura.quanturaapp.ads

import android.content.Context
import android.util.AttributeSet
import android.util.Log
import android.widget.FrameLayout
import com.google.android.gms.ads.AdRequest
import com.google.android.gms.ads.AdSize
import com.google.android.gms.ads.AdView
import com.google.android.gms.ads.AdListener
import com.google.android.gms.ads.LoadAdError
import com.quantura.quanturaapp.config.AdFormat
import com.quantura.quanturaapp.config.AdPlatform
import com.quantura.quanturaapp.config.RemoteConfigManager

/**
 * Adaptive banner ad view. Shown when ads_enabled feature flag is true.
 * Uses demo ad unit ID from Remote Config (or default test ID).
 */
class BannerAdView @JvmOverloads constructor(
    context: Context,
    attrs: AttributeSet? = null,
    defStyleAttr: Int = 0,
) : FrameLayout(context, attrs, defStyleAttr) {
    private val tag = "BannerAdView"

    private var adView: AdView? = null
    private var remoteConfigManager: RemoteConfigManager? = null

    fun setRemoteConfigManager(manager: RemoteConfigManager?) {
        remoteConfigManager = manager
    }

    fun loadAd(manager: RemoteConfigManager) {
        remoteConfigManager = manager
        if (!manager.areAdsEnabled()) {
            Log.d(tag, "Banner hidden because ads feature flag is disabled.")
            AdDebugStatusRegistry.updateLoad("banner", "disabled")
            hideAd()
            return
        }
        val adUnitId = manager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.BANNER
        )
        removeAllViews()
        adView?.destroy()
        adView = AdView(context).apply {
            setAdSize(AdSize.getCurrentOrientationAnchoredAdaptiveBannerAdSize(context, resources.displayMetrics.widthPixels))
            setAdUnitId(adUnitId)
            adListener = object : AdListener() {
                override fun onAdLoaded() {
                    Log.d(this@BannerAdView.tag, "Banner load succeeded.")
                    Log.i(this@BannerAdView.tag, "[Ads][Android] Load success for banner")
                    AdDebugStatusRegistry.updateLoad("banner", "loaded")
                }

                override fun onAdFailedToLoad(error: LoadAdError) {
                    Log.w(this@BannerAdView.tag, "Banner load failed: ${error.message}")
                    Log.w(this@BannerAdView.tag, "[Ads][Android] Load fail for banner: ${error.message}")
                    AdDebugStatusRegistry.updateLoad("banner", "failed:${error.message}")
                }

                override fun onAdImpression() {
                    Log.d(this@BannerAdView.tag, "Banner impression recorded.")
                    AdDebugStatusRegistry.updateShow("banner", "impression")
                    AdImpressionReporter.report(
                        context = context,
                        adFormat = "banner",
                        adUnitId = adUnitId,
                        placement = "bottom_banner"
                    )
                }
            }
        }
        addView(adView)
        Log.d(tag, "Loading banner unit=$adUnitId")
        AdDebugStatusRegistry.updateLoad("banner", "loading")
        adView?.loadAd(AdRequest.Builder().build())
        visibility = VISIBLE
    }

    fun refreshAdVisibility() {
        val manager = remoteConfigManager ?: return
        if (!manager.areAdsEnabled()) {
            hideAd()
            return
        }
        if (adView == null) {
            loadAd(manager)
        } else {
            visibility = VISIBLE
        }
    }

    fun hideAd() {
        removeAllViews()
        adView?.destroy()
        adView = null
        visibility = GONE
    }

    override fun onDetachedFromWindow() {
        adView?.destroy()
        adView = null
        super.onDetachedFromWindow()
    }
}
