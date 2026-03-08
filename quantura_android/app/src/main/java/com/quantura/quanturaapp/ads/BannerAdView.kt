package com.quantura.quanturaapp.ads

import android.content.Context
import android.util.AttributeSet
import android.util.Log
import android.view.ViewTreeObserver
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
    private val retryDelayMs = 30_000L
    private val minReservedHeightPx = (56f * resources.displayMetrics.density).toInt()

    private var adView: AdView? = null
    private var remoteConfigManager: RemoteConfigManager? = null
    private var waitingForLayout = false
    private var pendingRetry: Runnable? = null

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
        if (!MobileAdsBootstrap.isInitialized()) {
            Log.d(tag, "Banner load deferred until Mobile Ads finishes initializing.")
            AdDebugStatusRegistry.updateLoad("banner", "waiting:sdk_init")
            MobileAdsBootstrap.runWhenInitialized {
                post {
                    if (remoteConfigManager === manager) {
                        loadAd(manager)
                    }
                }
            }
            return
        }
        if (width <= 0) {
            deferLoadUntilMeasured(manager)
            return
        }
        pendingRetry?.let { removeCallbacks(it) }
        pendingRetry = null
        val adUnitId = manager.resolveAdUnitId(
            platform = AdPlatform.ANDROID,
            format = AdFormat.BANNER
        )
        val metrics = resources.displayMetrics
        val rawWidthPx = width.coerceAtLeast(1)
        val density = metrics.density.coerceAtLeast(1f)
        val bannerWidthDp = (rawWidthPx / density).toInt().coerceAtLeast(1)
        val adaptiveSize = AdSize.getCurrentOrientationAnchoredAdaptiveBannerAdSize(context, bannerWidthDp)
        val requestedHeightPx = adaptiveSize.getHeightInPixels(context).coerceAtLeast(minReservedHeightPx)
        Log.i(
            tag,
            "[Ads][Android] Banner sizing rawPx=$rawWidthPx density=$density widthDp=$bannerWidthDp " +
                "heightPx=${adaptiveSize.getHeightInPixels(context)}"
        )
        layoutParams = (layoutParams ?: LayoutParams(LayoutParams.MATCH_PARENT, requestedHeightPx)).apply {
            width = LayoutParams.MATCH_PARENT
            height = requestedHeightPx
        }
        removeAllViews()
        adView?.destroy()
        adView = AdView(context).apply {
            setAdSize(adaptiveSize)
            setAdUnitId(adUnitId)
            layoutParams = LayoutParams(LayoutParams.MATCH_PARENT, LayoutParams.WRAP_CONTENT)
            adListener = object : AdListener() {
                override fun onAdLoaded() {
                    Log.d(this@BannerAdView.tag, "Banner load succeeded.")
                    Log.i(this@BannerAdView.tag, "[Ads][Android] Load success for banner")
                    AdDebugStatusRegistry.updateLoad("banner", "loaded")
                    visibility = VISIBLE
                }

                override fun onAdFailedToLoad(error: LoadAdError) {
                    Log.w(this@BannerAdView.tag, "Banner load failed: ${error.message}")
                    Log.w(this@BannerAdView.tag, "[Ads][Android] Load fail for banner: ${error.message}")
                    AdDebugStatusRegistry.updateLoad("banner", "failed:${error.message}")
                    scheduleRetry()
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
        minimumHeight = minReservedHeightPx
        visibility = INVISIBLE
    }

    override fun onDetachedFromWindow() {
        pendingRetry?.let { removeCallbacks(it) }
        pendingRetry = null
        adView?.destroy()
        adView = null
        super.onDetachedFromWindow()
    }

    private fun scheduleRetry() {
        val manager = remoteConfigManager ?: return
        pendingRetry?.let { removeCallbacks(it) }
        pendingRetry = Runnable {
            if (!isAttachedToWindow) return@Runnable
            loadAd(manager)
        }
        postDelayed(pendingRetry, retryDelayMs)
    }

    private fun deferLoadUntilMeasured(manager: RemoteConfigManager) {
        if (waitingForLayout) return
        waitingForLayout = true
        val listener = object : ViewTreeObserver.OnGlobalLayoutListener {
            override fun onGlobalLayout() {
                if (width <= 0) return
                if (viewTreeObserver.isAlive) {
                    viewTreeObserver.removeOnGlobalLayoutListener(this)
                }
                waitingForLayout = false
                loadAd(manager)
            }
        }
        viewTreeObserver.addOnGlobalLayoutListener(listener)
        postDelayed({
            if (!waitingForLayout || width > 0) return@postDelayed
            if (viewTreeObserver.isAlive) {
                viewTreeObserver.removeOnGlobalLayoutListener(listener)
            }
            waitingForLayout = false
            Log.w(tag, "[Ads][Android] Banner width not measured yet; retrying after layout.")
            scheduleRetry()
        }, 1000L)
    }
}
