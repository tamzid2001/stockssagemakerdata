package com.quantura.quanturaapp.web

import android.content.Intent
import android.net.Uri
import android.util.Log
import android.webkit.JavascriptInterface
import androidx.activity.ComponentActivity
import androidx.core.os.bundleOf
import com.quantura.quanturaapp.ads.AdManager
import com.google.firebase.analytics.FirebaseAnalytics
import org.json.JSONObject

/**
 * Receives Quantura bridge messages from the web layer and dispatches to native actions.
 * All UI-affecting work is executed on the Activity UI thread.
 */
class QuanturaJavascriptBridge(
    private val activity: ComponentActivity,
    private val adManager: AdManager,
    private val onNativeAuthMessage: (type: String, payload: JSONObject) -> Unit,
    private val isAuthGateVisible: () -> Boolean = { false },
    private val onBridgeEvent: (eventName: String, payload: JSONObject) -> Unit = { _, _ -> },
) {
    private val tag = "QuanturaJsBridge"

    @JavascriptInterface
    fun postMessage(rawPayload: String?) {
        val payload = parsePayload(rawPayload)
        val messageType = payload.optString("type").trim().uppercase()
        val action = payload.optString("action").trim()
        if (action.isEmpty() && messageType.isEmpty()) return

        activity.runOnUiThread {
            if (messageType.isNotEmpty()) {
                onNativeAuthMessage(messageType, payload)
                return@runOnUiThread
            }
            when (action) {
                "showInterstitialAd", "showInterstitial" -> showInterstitialIfAllowed(payload)
                "showRewardedAd" -> showRewardedIfAllowed(payload, preferRewardedInterstitial = false)
                "showRewardedInterstitial" -> showRewardedIfAllowed(payload, preferRewardedInterstitial = true)
                "showAuthGate" -> onNativeAuthMessage("REQUEST_SIGN_IN", payload)
                "openNewsLink" -> openNewsLink(payload.optString("url"))
                "handleButtonClick" -> handleButtonClick(payload.optString("buttonId"))
                "share" -> openNativeShare(payload.optString("url"), payload.optString("title"), payload.optString("text"))
                "authSignIn" -> onNativeAuthMessage(
                    "REQUEST_SIGN_IN",
                    JSONObject().put("provider", payload.optString("provider"))
                )
                "authSignOut" -> onNativeAuthMessage("SIGN_OUT", JSONObject())
                "startNativePurchase" -> onNativeAuthMessage("NATIVE_PURCHASE", payload)
                "openNativeSubscriptionManager" -> onNativeAuthMessage("OPEN_NATIVE_SUBSCRIPTIONS", payload)
                "requestNativeFeedAd" -> handleNativeFeedAdRequest(payload)
                "nativeFeedAdImpression" -> reportNativeFeedAdImpression(payload)
                "nativeFeedAdClick" -> reportNativeFeedAdClick(payload)
            }
        }
    }

    private fun parsePayload(rawPayload: String?): JSONObject {
        val text = rawPayload?.trim().orEmpty()
        if (text.isEmpty()) return JSONObject()
        return try {
            JSONObject(text)
        } catch (_: Exception) {
            // Accept plain action strings as a fallback.
            JSONObject().put("action", text)
        }
    }

    private fun openNewsLink(url: String) {
        val normalized = url.trim()
        if (!normalized.startsWith("http")) return
        Log.d(tag, "News link trigger interstitial url=$normalized")
        showInterstitialIfAllowed(JSONObject())
        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(normalized))
        if (intent.resolveActivity(activity.packageManager) != null) {
            activity.startActivity(intent)
        }
    }

    private fun handleButtonClick(buttonId: String) {
        Log.d(tag, "Button trigger rewarded buttonId=${buttonId.trim()}")
        showRewardedIfAllowed(JSONObject(), preferRewardedInterstitial = true)
        FirebaseAnalytics.getInstance(activity).logEvent(
            "native_bridge_button_click",
            bundleOf("button_id" to buttonId)
        )
    }

    private fun showInterstitialIfAllowed(payload: JSONObject) {
        val requestId = payload.optString("requestId").trim()
        if (isAuthGateVisible()) {
            Log.d(tag, "Interstitial skipped; auth gate visible.")
            emitAdResult(
                requestId = requestId,
                adFormat = "interstitial",
                status = "skipped:auth_gate",
                message = "Auth gate is visible."
            )
            return
        }
        adManager.showInterstitial(activity, requestId = requestId) { result ->
            emitAdResult(
                requestId = requestId,
                adFormat = result.optString("adFormat").trim().ifEmpty { "interstitial" },
                status = result.optString("status").trim(),
                message = result.optString("message").trim(),
                rewardType = result.optString("rewardType").trim(),
                rewardAmount = if (result.has("rewardAmount") && !result.isNull("rewardAmount")) {
                    result.optDouble("rewardAmount")
                } else {
                    Double.NaN
                }
            )
        }
    }

    private fun showRewardedIfAllowed(payload: JSONObject, preferRewardedInterstitial: Boolean) {
        val requestId = payload.optString("requestId").trim()
        if (isAuthGateVisible()) {
            Log.d(tag, "Rewarded skipped; auth gate visible.")
            emitAdResult(
                requestId = requestId,
                adFormat = if (preferRewardedInterstitial) "rewarded_interstitial" else "rewarded",
                status = "skipped:auth_gate",
                message = "Auth gate is visible."
            )
            return
        }
        adManager.showRewarded(
            activity = activity,
            requestId = requestId,
            preferRewardedInterstitial = preferRewardedInterstitial,
            callback = { result ->
                emitAdResult(
                    requestId = requestId,
                    adFormat = result.optString("adFormat").trim()
                        .ifEmpty { if (preferRewardedInterstitial) "rewarded_interstitial" else "rewarded" },
                    status = result.optString("status").trim(),
                    message = result.optString("message").trim(),
                    rewardType = result.optString("rewardType").trim(),
                    rewardAmount = if (result.has("rewardAmount") && !result.isNull("rewardAmount")) {
                        result.optDouble("rewardAmount")
                    } else {
                        Double.NaN
                    }
                )
            }
        )
    }

    private fun openNativeShare(url: String, title: String, text: String) {
        val urlTrimmed = url.trim()
        if (urlTrimmed.isEmpty()) return
        val titleTrimmed = (title.trim().ifBlank { "Quantura" })
        val textTrimmed = text.trim()
        val shareIntent = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_TITLE, titleTrimmed)
            putExtra(Intent.EXTRA_TEXT, if (textTrimmed.isNotEmpty()) "$textTrimmed $urlTrimmed" else urlTrimmed)
            putExtra(Intent.EXTRA_SUBJECT, titleTrimmed)
        }
        val chooser = Intent.createChooser(shareIntent, "Share via")
        if (chooser.resolveActivity(activity.packageManager) != null) {
            activity.startActivity(chooser)
        }
    }

    private fun handleNativeFeedAdRequest(payload: JSONObject) {
        val slotId = payload.optString("slotId").trim()
        val placement = payload.optString("placement").trim().ifEmpty { "feed" }
        val variant = payload.optString("variant").trim().ifEmpty { "nativeAdvanced" }
        if (slotId.isEmpty()) {
            onBridgeEvent(
                "quantura:native-feed-ad",
                JSONObject().put("ok", false).put("slotId", "").put("placement", placement).put("error", "slot_id_missing")
            )
            return
        }
        if (isAuthGateVisible()) {
            onBridgeEvent(
                "quantura:native-feed-ad",
                JSONObject().put("ok", false).put("slotId", slotId).put("placement", placement).put("error", "auth_gate_visible")
            )
            return
        }
        adManager.requestNativeFeedAd(
            activity = activity,
            slotId = slotId,
            placement = placement,
            variant = variant
        ) { result ->
            onBridgeEvent("quantura:native-feed-ad", result)
        }
    }

    private fun reportNativeFeedAdImpression(payload: JSONObject) {
        val slotId = payload.optString("slotId").trim()
        val placement = payload.optString("placement").trim().ifEmpty { "feed" }
        val adUnitId = payload.optString("adUnitId").trim()
        adManager.reportNativeFeedAdImpression(
            context = activity,
            slotId = slotId,
            placement = placement,
            adUnitId = adUnitId
        )
    }

    private fun reportNativeFeedAdClick(payload: JSONObject) {
        val slotId = payload.optString("slotId").trim()
        val placement = payload.optString("placement").trim().ifEmpty { "feed" }
        val adUnitId = payload.optString("adUnitId").trim()
        adManager.reportNativeFeedAdClick(
            context = activity,
            slotId = slotId,
            placement = placement,
            adUnitId = adUnitId
        )
    }

    private fun emitAdResult(
        requestId: String,
        adFormat: String,
        status: String,
        message: String = "",
        rewardType: String = "",
        rewardAmount: Double = Double.NaN,
    ) {
        val payload = JSONObject()
            .put("requestId", requestId)
            .put("adFormat", adFormat)
            .put("status", status)
            .put("message", message)
            .put("rewardType", rewardType)
            .put("rewardAmount", if (rewardAmount.isNaN()) JSONObject.NULL else rewardAmount)
        onBridgeEvent("quantura:native-ad-result", payload)
    }
}
