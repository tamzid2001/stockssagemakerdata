package com.example.quantura_android.web

import android.content.Intent
import android.net.Uri
import android.webkit.JavascriptInterface
import androidx.activity.ComponentActivity
import androidx.core.os.bundleOf
import com.example.quantura_android.ads.AdManager
import com.google.firebase.analytics.FirebaseAnalytics
import org.json.JSONObject

/**
 * Receives Quantura bridge messages from the web layer and dispatches to native actions.
 * All UI-affecting work is executed on the Activity UI thread.
 */
class QuanturaJavascriptBridge(
    private val activity: ComponentActivity,
    private val adManager: AdManager,
) {
    @JavascriptInterface
    fun postMessage(rawPayload: String?) {
        val payload = parsePayload(rawPayload)
        val action = payload.optString("action").trim()
        if (action.isEmpty()) return

        activity.runOnUiThread {
            when (action) {
                "showInterstitialAd" -> adManager.showInterstitial(activity)
                "showRewardedAd" -> adManager.showRewarded(activity)
                "openNewsLink" -> openNewsLink(payload.optString("url"))
                "handleButtonClick" -> handleButtonClick(payload.optString("buttonId"))
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
        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(normalized))
        if (intent.resolveActivity(activity.packageManager) != null) {
            activity.startActivity(intent)
        }
    }

    private fun handleButtonClick(buttonId: String) {
        FirebaseAnalytics.getInstance(activity).logEvent(
            "native_bridge_button_click",
            bundleOf("button_id" to buttonId)
        )
    }
}
