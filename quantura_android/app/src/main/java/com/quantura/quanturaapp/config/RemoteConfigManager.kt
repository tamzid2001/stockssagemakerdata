package com.quantura.quanturaapp.config

import com.google.firebase.remoteconfig.FirebaseRemoteConfig
import com.google.firebase.remoteconfig.FirebaseRemoteConfigSettings
import kotlinx.coroutines.tasks.await
import org.json.JSONObject

data class AdUnitIds(
    val interstitial: String,
    val rewarded: String,
    val adaptiveBanner: String,
    val appOpen: String,
)

class RemoteConfigManager(
    private val remoteConfig: FirebaseRemoteConfig?,
    isDebug: Boolean = false,
) {
    init {
        remoteConfig?.let {
            // 0s fetch interval in debug, 1h in production.
            val minIntervalSeconds = if (isDebug) 0L else 3600L
            val settings = FirebaseRemoteConfigSettings.Builder()
                .setMinimumFetchIntervalInSeconds(minIntervalSeconds)
                .build()
            it.setConfigSettingsAsync(settings)
            it.setDefaultsAsync(
                mapOf(
                    "ad_unit_ids" to DEFAULT_AD_UNIT_IDS_JSON,
                    "feature_flags" to DEFAULT_FEATURE_FLAGS_JSON,
                )
            )
        }
    }

    suspend fun fetchAndActivate(): Boolean {
        val activeRemoteConfig = remoteConfig ?: return false
        return try {
            activeRemoteConfig.fetchAndActivate().await()
        } catch (_: Exception) {
            false
        }
    }

    fun getAdUnitIds(): AdUnitIds {
        val raw = remoteConfig?.getString("ad_unit_ids").orEmpty().ifBlank { DEFAULT_AD_UNIT_IDS_JSON }
        return try {
            val json = JSONObject(raw)
            AdUnitIds(
                interstitial = json.optString("interstitial", DEFAULT_INTERSTITIAL_ID),
                rewarded = json.optString("rewarded", DEFAULT_REWARDED_ID),
                adaptiveBanner = json.optString("adaptiveBanner", DEFAULT_ADAPTIVE_BANNER_ID),
                appOpen = json.optString("appOpen", DEFAULT_APP_OPEN_ID),
            )
        } catch (_: Exception) {
            AdUnitIds(DEFAULT_INTERSTITIAL_ID, DEFAULT_REWARDED_ID, DEFAULT_ADAPTIVE_BANNER_ID, DEFAULT_APP_OPEN_ID)
        }
    }

    fun isFeatureEnabled(key: String): Boolean {
        val raw = remoteConfig?.getString("feature_flags").orEmpty().ifBlank { DEFAULT_FEATURE_FLAGS_JSON }
        return try {
            JSONObject(raw).optBoolean(key, false)
        } catch (_: Exception) {
            false
        }
    }

    companion object {
        fun create(firebaseReady: Boolean, isDebug: Boolean = false): RemoteConfigManager {
            if (!firebaseReady) {
                return RemoteConfigManager(remoteConfig = null, isDebug = isDebug)
            }
            val remoteConfig = try {
                FirebaseRemoteConfig.getInstance()
            } catch (_: Exception) {
                null
            }
            return RemoteConfigManager(remoteConfig = remoteConfig, isDebug = isDebug)
        }

        private const val DEFAULT_INTERSTITIAL_ID = "ca-app-pub-3940256099942544/1033173712"
        private const val DEFAULT_REWARDED_ID = "ca-app-pub-3940256099942544/5224354917"
        private const val DEFAULT_ADAPTIVE_BANNER_ID = "ca-app-pub-3940256099942544/9214589741"
        private const val DEFAULT_APP_OPEN_ID = "ca-app-pub-3940256099942544/9257395921"
        private const val DEFAULT_AD_UNIT_IDS_JSON =
            """{"interstitial":"ca-app-pub-3940256099942544/1033173712","rewarded":"ca-app-pub-3940256099942544/5224354917","adaptiveBanner":"ca-app-pub-3940256099942544/9214589741","appOpen":"ca-app-pub-3940256099942544/9257395921"}"""
        private const val DEFAULT_FEATURE_FLAGS_JSON =
            """{"native_bridge_enabled":true,"ads_enabled":true}"""
    }
}
