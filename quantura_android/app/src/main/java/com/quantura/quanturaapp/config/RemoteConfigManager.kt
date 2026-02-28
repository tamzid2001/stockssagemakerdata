package com.quantura.quanturaapp.config

import com.google.firebase.remoteconfig.FirebaseRemoteConfig
import com.google.firebase.remoteconfig.FirebaseRemoteConfigSettings
import kotlinx.coroutines.tasks.await
import org.json.JSONObject

data class AdUnitIds(
    val appOpen: String,
    val adaptiveBanner: String,
    val fixedBanner: String,
    val interstitial: String,
    val rewarded: String,
    val rewardedInterstitial: String,
    val nativeAdvanced: String,
    val nativeVideo: String,
)

class RemoteConfigManager(
    private val remoteConfig: FirebaseRemoteConfig?,
    private val isDebug: Boolean = false,
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
                    "ads_use_real_android" to true,
                    "ads_use_real_ios" to true,
                    "play_integrity_enabled" to true,
                    "play_integrity_required" to false,
                    "play_integrity_cloud_project_number" to "",
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
        val useRealAndroidAds = if (isDebug) {
            false
        } else {
            remoteConfig?.getBoolean("ads_use_real_android") ?: true
        }
        val seed = if (useRealAndroidAds) LIVE_ANDROID_IDS else DEMO_AD_IDS

        val rawOverride = remoteConfig?.getString("ad_unit_ids").orEmpty()
        if (rawOverride.isBlank()) return seed

        return try {
            val json = JSONObject(rawOverride)
            AdUnitIds(
                appOpen = json.optString("appOpen", seed.appOpen),
                adaptiveBanner = json.optString("adaptiveBanner", seed.adaptiveBanner),
                fixedBanner = json.optString("fixedBanner", seed.fixedBanner),
                interstitial = json.optString("interstitial", seed.interstitial),
                rewarded = json.optString("rewarded", seed.rewarded),
                rewardedInterstitial = json.optString("rewardedInterstitial", seed.rewardedInterstitial),
                nativeAdvanced = json.optString("nativeAdvanced", seed.nativeAdvanced),
                nativeVideo = json.optString("nativeVideo", seed.nativeVideo),
            )
        } catch (_: Exception) {
            seed
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

    fun isPlayIntegrityEnabled(): Boolean {
        return remoteConfig?.getBoolean("play_integrity_enabled") ?: true
    }

    fun isPlayIntegrityRequired(): Boolean {
        return remoteConfig?.getBoolean("play_integrity_required") ?: false
    }

    fun playIntegrityCloudProjectNumber(): Long? {
        val raw = remoteConfig?.getString("play_integrity_cloud_project_number").orEmpty().trim()
        if (raw.isBlank()) return null
        return raw.toLongOrNull()
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

        private val DEMO_AD_IDS = AdUnitIds(
            appOpen = "ca-app-pub-3940256099942544/9257395921",
            adaptiveBanner = "ca-app-pub-3940256099942544/9214589741",
            fixedBanner = "ca-app-pub-3940256099942544/6300978111",
            interstitial = "ca-app-pub-3940256099942544/1033173712",
            rewarded = "ca-app-pub-3940256099942544/5224354917",
            rewardedInterstitial = "ca-app-pub-3940256099942544/5354046379",
            nativeAdvanced = "ca-app-pub-3940256099942544/2247696110",
            nativeVideo = "ca-app-pub-3940256099942544/1044960115",
        )

        private val LIVE_ANDROID_IDS = AdUnitIds(
            appOpen = "ca-app-pub-5322412772082850/1802977031",
            adaptiveBanner = "ca-app-pub-5322412772082850/3390017725",
            fixedBanner = "ca-app-pub-5322412772082850/3390017725",
            interstitial = "ca-app-pub-5322412772082850/7358556043",
            rewarded = "ca-app-pub-5322412772082850/1867749156",
            rewardedInterstitial = "ca-app-pub-5322412772082850/4780998745",
            nativeAdvanced = "ca-app-pub-5322412772082850/1144501483",
            nativeVideo = "ca-app-pub-5322412772082850/1144501483",
        )

        private const val DEFAULT_FEATURE_FLAGS_JSON =
            """{"native_bridge_enabled":true,"ads_enabled":true}"""
    }
}
