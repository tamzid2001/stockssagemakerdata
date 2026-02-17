package com.example.quantura_android.config

import com.google.firebase.remoteconfig.FirebaseRemoteConfig
import com.google.firebase.remoteconfig.FirebaseRemoteConfigSettings
import kotlinx.coroutines.tasks.await
import org.json.JSONObject

data class AdUnitIds(
    val interstitial: String,
    val rewarded: String,
)

class RemoteConfigManager(
    private val remoteConfig: FirebaseRemoteConfig = FirebaseRemoteConfig.getInstance(),
    isDebug: Boolean = false,
) {
    init {
        // 0s fetch interval in debug, 1h in production.
        val minIntervalSeconds = if (isDebug) 0L else 3600L
        val settings = FirebaseRemoteConfigSettings.Builder()
            .setMinimumFetchIntervalInSeconds(minIntervalSeconds)
            .build()
        remoteConfig.setConfigSettingsAsync(settings)
        remoteConfig.setDefaultsAsync(
            mapOf(
                "ad_unit_ids" to DEFAULT_AD_UNIT_IDS_JSON,
                "feature_flags" to DEFAULT_FEATURE_FLAGS_JSON,
            )
        )
    }

    suspend fun fetchAndActivate(): Boolean {
        return try {
            remoteConfig.fetchAndActivate().await()
        } catch (_: Exception) {
            false
        }
    }

    fun getAdUnitIds(): AdUnitIds {
        val raw = remoteConfig.getString("ad_unit_ids").ifBlank { DEFAULT_AD_UNIT_IDS_JSON }
        return try {
            val json = JSONObject(raw)
            AdUnitIds(
                interstitial = json.optString("interstitial", DEFAULT_INTERSTITIAL_ID),
                rewarded = json.optString("rewarded", DEFAULT_REWARDED_ID),
            )
        } catch (_: Exception) {
            AdUnitIds(DEFAULT_INTERSTITIAL_ID, DEFAULT_REWARDED_ID)
        }
    }

    fun isFeatureEnabled(key: String): Boolean {
        val raw = remoteConfig.getString("feature_flags").ifBlank { DEFAULT_FEATURE_FLAGS_JSON }
        return try {
            JSONObject(raw).optBoolean(key, false)
        } catch (_: Exception) {
            false
        }
    }

    companion object {
        private const val DEFAULT_INTERSTITIAL_ID = "ca-app-pub-3940256099942544/4411468910"
        private const val DEFAULT_REWARDED_ID = "ca-app-pub-3940256099942544/1712485313"
        private const val DEFAULT_AD_UNIT_IDS_JSON =
            """{"interstitial":"ca-app-pub-3940256099942544/4411468910","rewarded":"ca-app-pub-3940256099942544/1712485313"}"""
        private const val DEFAULT_FEATURE_FLAGS_JSON =
            """{"native_bridge_enabled":true,"ads_enabled":true}"""
    }
}
