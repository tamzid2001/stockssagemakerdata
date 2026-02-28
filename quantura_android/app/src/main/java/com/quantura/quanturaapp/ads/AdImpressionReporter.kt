package com.quantura.quanturaapp.ads

import android.content.Context
import android.os.Bundle
import android.util.Log
import com.google.firebase.analytics.FirebaseAnalytics
import com.google.firebase.auth.FirebaseAuth
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.UUID

object AdImpressionReporter {
    private const val TAG = "AdImpressionReporter"
    private const val CALLBACK_ENDPOINT = "https://quantura.studio/api/analytics/ad-impression"

    fun report(
        context: Context,
        adFormat: String,
        adUnitId: String,
        placement: String = "",
        rewardType: String = "",
        rewardAmount: Double? = null,
    ) {
        val normalizedFormat = adFormat.trim().lowercase().ifEmpty { "unknown" }
        val normalizedUnit = adUnitId.trim()
        val normalizedPlacement = placement.trim()
        val normalizedRewardType = rewardType.trim()

        val analyticsBundle = Bundle().apply {
            putString(FirebaseAnalytics.Param.AD_PLATFORM, "admob")
            putString(FirebaseAnalytics.Param.AD_SOURCE, "admob")
            putString(FirebaseAnalytics.Param.AD_FORMAT, normalizedFormat)
            putString(FirebaseAnalytics.Param.AD_UNIT_NAME, normalizedUnit)
            if (normalizedRewardType.isNotEmpty()) putString("reward_type", normalizedRewardType)
            if (rewardAmount != null) putDouble("reward_amount", rewardAmount)
            if (normalizedPlacement.isNotEmpty()) putString("placement", normalizedPlacement)
            putString("platform", "android")
        }
        FirebaseAnalytics.getInstance(context).logEvent(FirebaseAnalytics.Event.AD_IMPRESSION, analyticsBundle)

        val payload = JSONObject().apply {
            put("impressionId", UUID.randomUUID().toString())
            put("platform", "android")
            put("adPlatform", "admob")
            put("adSource", "admob")
            put("adFormat", normalizedFormat)
            put("adUnitId", normalizedUnit)
            put("placement", normalizedPlacement)
            put("rewardType", normalizedRewardType)
            put("rewardAmount", rewardAmount ?: JSONObject.NULL)
        }

        val user = FirebaseAuth.getInstance().currentUser
        if (user == null) {
            postPayload(payload, bearerToken = "")
            return
        }

        user.getIdToken(false)
            .addOnSuccessListener { result ->
                postPayload(payload, bearerToken = result.token.orEmpty())
            }
            .addOnFailureListener { error ->
                Log.w(TAG, "Failed to obtain ID token for ad impression callback: ${error.message}")
                postPayload(payload, bearerToken = "")
            }
    }

    private fun postPayload(payload: JSONObject, bearerToken: String) {
        Thread {
            val connection = (URL(CALLBACK_ENDPOINT).openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 12_000
                readTimeout = 12_000
                doOutput = true
                setRequestProperty("Content-Type", "application/json")
                if (bearerToken.isNotBlank()) {
                    setRequestProperty("Authorization", "Bearer $bearerToken")
                }
            }
            try {
                connection.outputStream.use { output ->
                    output.write(payload.toString().toByteArray(Charsets.UTF_8))
                }
                val status = connection.responseCode
                if (status !in 200..299) {
                    Log.w(TAG, "Ad impression callback returned status=$status")
                }
            } catch (error: Exception) {
                Log.w(TAG, "Ad impression callback failed: ${error.message}")
            } finally {
                connection.disconnect()
            }
        }.start()
    }
}
