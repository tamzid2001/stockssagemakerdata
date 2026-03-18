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
 * Respects the bundled live IDs plus Remote Config overrides.
 */
class BannerAdView @JvmOverloads constructor(
    context: Context,
    attrs: AttributeSet? = null,
    defStyleAttr: Int = 0,
) : FrameLayout(context, attrs, defStyleAttr) {
    enum class BannerSlot {
        TOP,
        BOTTOM,
    }

    private val tag = "BannerAdView"
    private val retryDelayMs = 30_000L
    private val minReservedHeightPx = (56f * resources.displayMetrics.density).toInt()

    private var adView: AdView? = null
    private var remoteConfigManager: RemoteConfigManager? = null
    private var waitingForLayout = false
    private var pendingRetry: Runnable? = null
    private var waitingForSdkInit = false
    private var loadedAdUnitId: String? = null
    private var onAdHeightChanged: ((Int) -> Unit)? = null
    private var bannerSlot: BannerSlot = BannerSlot.TOP

    fun setRemoteConfigManager(manager: RemoteConfigManager?) {
        remoteConfigManager = manager
    }

    fun setBannerSlot(slot: BannerSlot) {
        bannerSlot = slot
    }

    fun setOnAdHeightChanged(listener: ((Int) -> Unit)?) {
        onAdHeightChanged = listener
        notifyAdHeightChanged(currentContainerHeightPx())
    }

    fun loadAd(manager: RemoteConfigManager) {
        remoteConfigManager = manager
        if (!manager.areAdsEnabled()) {
            Log.d(tag, "Banner hidden because ads feature flag is disabled.")
            AdDebugStatusRegistry.updateLoad("banner", "disabled")
            hideAd()
            return
        }
        if (!manager.isAdFormatEnabled(platform = AdPlatform.ANDROID, format = AdFormat.BANNER)) {
            Log.d(tag, "Banner hidden because banner format flag is disabled.")
            AdDebugStatusRegistry.updateLoad("banner", "disabled:format_off")
            hideAd()
            return
        }
        if (!MobileAdsBootstrap.isInitialized()) {
            AdDebugStatusRegistry.updateLoad("banner", "waiting:sdk_init")
            minimumHeight = minReservedHeightPx
            visibility = VISIBLE
            notifyAdHeightChanged(minReservedHeightPx)
            if (!waitingForSdkInit) {
                waitingForSdkInit = true
                MobileAdsBootstrap.runWhenInitialized {
                    post {
                        waitingForSdkInit = false
                        if (!isAttachedToWindow) return@post
                        loadAd(manager)
                    }
                }
            }
            return
        }
        if (width <= 0) {
            minimumHeight = minReservedHeightPx
            visibility = VISIBLE
            notifyAdHeightChanged(minReservedHeightPx)
            deferLoadUntilMeasured(manager)
            return
        }
        waitingForSdkInit = false
        pendingRetry?.let { removeCallbacks(it) }
        pendingRetry = null
        val adUnitId = when (bannerSlot) {
            BannerSlot.TOP -> manager.resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.BANNER
            )
            BannerSlot.BOTTOM -> manager.resolveAndroidBottomBannerAdUnitId()
        }
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
        minimumHeight = requestedHeightPx
        notifyAdHeightChanged(requestedHeightPx)
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
                    notifyAdHeightChanged(currentContainerHeightPx())
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
                        placement = if (bannerSlot == BannerSlot.BOTTOM) "bottom_banner" else "top_banner"
                    )
                }
            }
        }
        loadedAdUnitId = adUnitId
        addView(adView)
        Log.d(tag, "Loading banner unit=$adUnitId")
        AdDebugStatusRegistry.updateLoad("banner", "loading")
        adView?.loadAd(AdRequest.Builder().build())
        visibility = VISIBLE
    }

    fun refreshAdVisibility() {
        val manager = remoteConfigManager ?: return
        if (!manager.areAdsEnabled() || !manager.isAdFormatEnabled(platform = AdPlatform.ANDROID, format = AdFormat.BANNER)) {
            hideAd()
            return
        }
        val desiredAdUnitId = when (bannerSlot) {
            BannerSlot.TOP -> manager.resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.BANNER
            )
            BannerSlot.BOTTOM -> manager.resolveAndroidBottomBannerAdUnitId()
        }
        val currentAdUnitId = loadedAdUnitId?.takeIf { it.isNotBlank() }
        if (adView == null || currentAdUnitId != desiredAdUnitId) {
            Log.i(
                tag,
                "[Ads][Android] Reloading banner after config refresh currentUnit=${currentAdUnitId ?: "(none)"} desiredUnit=$desiredAdUnitId"
            )
            loadAd(manager)
        } else {
            visibility = VISIBLE
        }
    }

    fun hideAd() {
        removeAllViews()
        adView?.destroy()
        adView = null
        loadedAdUnitId = null
        minimumHeight = 0
        waitingForSdkInit = false
        visibility = GONE
        notifyAdHeightChanged(0)
    }

    override fun onDetachedFromWindow() {
        pendingRetry?.let { removeCallbacks(it) }
        pendingRetry = null
        adView?.destroy()
        adView = null
        loadedAdUnitId = null
        notifyAdHeightChanged(0)
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

    private fun currentContainerHeightPx(): Int {
        val layoutHeight = layoutParams?.height ?: 0
        return when {
            visibility != VISIBLE -> 0
            layoutHeight > 0 -> layoutHeight
            height > 0 -> height
            minimumHeight > 0 -> minimumHeight
            else -> minReservedHeightPx
        }
    }

    private fun notifyAdHeightChanged(heightPx: Int) {
        onAdHeightChanged?.invoke(heightPx.coerceAtLeast(0))
    }
}
