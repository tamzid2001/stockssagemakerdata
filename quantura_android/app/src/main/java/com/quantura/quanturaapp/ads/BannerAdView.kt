package com.quantura.quanturaapp.ads

import android.content.Context
import android.util.AttributeSet
import android.widget.FrameLayout
import com.google.android.gms.ads.AdRequest
import com.google.android.gms.ads.AdSize
import com.google.android.gms.ads.AdView
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

    private var adView: AdView? = null
    private var remoteConfigManager: RemoteConfigManager? = null

    fun setRemoteConfigManager(manager: RemoteConfigManager?) {
        remoteConfigManager = manager
    }

    fun loadAd(manager: RemoteConfigManager) {
        remoteConfigManager = manager
        if (!manager.isFeatureEnabled("ads_enabled")) {
            visibility = GONE
            return
        }
        val adUnitId = manager.getAdUnitIds().adaptiveBanner
        removeAllViews()
        adView = AdView(context).apply {
            setAdSize(AdSize.getCurrentOrientationAnchoredAdaptiveBannerAdSize(context, resources.displayMetrics.widthPixels))
            setAdUnitId(adUnitId)
        }
        addView(adView)
        adView?.loadAd(AdRequest.Builder().build())
        visibility = VISIBLE
    }
}
