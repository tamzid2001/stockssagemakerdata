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
                "showInterstitialAd" -> showInterstitialIfAllowed()
                "showRewardedAd" -> showRewardedIfAllowed()
                "showRewardedInterstitial" -> showRewardedIfAllowed()
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
        showInterstitialIfAllowed()
        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(normalized))
        if (intent.resolveActivity(activity.packageManager) != null) {
            activity.startActivity(intent)
        }
    }

    private fun handleButtonClick(buttonId: String) {
        Log.d(tag, "Button trigger rewarded buttonId=${buttonId.trim()}")
        showRewardedIfAllowed()
        FirebaseAnalytics.getInstance(activity).logEvent(
            "native_bridge_button_click",
            bundleOf("button_id" to buttonId)
        )
    }

    private fun showInterstitialIfAllowed() {
        if (isAuthGateVisible()) {
            Log.d(tag, "Interstitial skipped; auth gate visible.")
            return
        }
        adManager.showInterstitial(activity)
    }

    private fun showRewardedIfAllowed() {
        if (isAuthGateVisible()) {
            Log.d(tag, "Rewarded skipped; auth gate visible.")
            return
        }
        adManager.showRewarded(activity)
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
}
