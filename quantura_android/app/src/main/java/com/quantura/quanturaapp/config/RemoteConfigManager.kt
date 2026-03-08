package com.quantura.quanturaapp.config

import android.os.Build
import android.util.Log
import com.google.firebase.remoteconfig.FirebaseRemoteConfig
import com.google.firebase.remoteconfig.FirebaseRemoteConfigSettings
import kotlinx.coroutines.tasks.await
import org.json.JSONObject

enum class AdPlatform {
    IOS,
    ANDROID,
}

enum class AdFormat {
    APP_OPEN,
    BANNER,
    INTERSTITIAL,
    REWARDED,
    REWARDED_INTERSTITIAL,
    NATIVE,
}

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

data class AdFeatureFlags(
    val nativeBridgeEnabled: Boolean,
    val adsEnabled: Boolean,
)

data class AdsRemoteConfigState(
    val adsEnabled: Boolean,
    val adsUseRealIos: Boolean,
    val adsUseRealAndroid: Boolean,
    val featureFlags: AdFeatureFlags,
    val ios: AdUnitIds,
    val android: AdUnitIds,
)

data class AdsEnvironment(
    val isDebugBuild: Boolean,
    val isSimulatorOrEmulator: Boolean,
    val isReleaseBuild: Boolean,
)

data class EffectiveAdsConfig(
    val adsEnabled: Boolean,
    val usingRealAds: Boolean,
    val usingTestAds: Boolean,
    val selectedUnits: AdUnitIds,
    val featureFlags: AdFeatureFlags,
    val adsEnabledTopLevel: Boolean,
    val adsUseRealAndroid: Boolean,
    val adsUseRealIos: Boolean,
    val environment: AdsEnvironment,
    val remoteConfigFetched: Boolean,
    val remoteConfigFetchedAtMs: Long?,
)

class RemoteConfigManager(
    private val remoteConfig: FirebaseRemoteConfig?,
    private val isDebug: Boolean = false,
    private val isEmulator: Boolean = false,
) {
    private val tag = "RemoteConfigManager"

    @Volatile
    private var lastFetchSucceeded: Boolean = false

    @Volatile
    private var lastFetchAtMs: Long? = null

    @Volatile
    private var lastEffectiveConfig: EffectiveAdsConfig? = null

    init {
        remoteConfig?.let {
            // 0s fetch interval in debug/emulator, 1h in production.
            val minIntervalSeconds = if (isDebug || isEmulator) 0L else 3600L
            val settings = FirebaseRemoteConfigSettings.Builder()
                .setMinimumFetchIntervalInSeconds(minIntervalSeconds)
                .build()
            it.setConfigSettingsAsync(settings)
            it.setDefaultsAsync(
                mapOf(
                    "ads_enabled" to true,
                    "ads_use_real_android" to true,
                    "ads_use_real_ios" to true,
                    "ad_unit_ids" to defaultAdUnitIdsPayload(),
                    "play_integrity_enabled" to true,
                    "play_integrity_required" to false,
                    "play_integrity_cloud_project_number" to "",
                    "native_feed_ad_start" to 4,
                    "native_feed_ad_interval" to 5,
                    "native_page_ad_midpoint" to 0.45,
                    "feature_flags" to DEFAULT_FEATURE_FLAGS_JSON,
                )
            )
        }
    }

    suspend fun fetchAndActivate(): Boolean {
        val activeRemoteConfig = remoteConfig ?: return false
        return try {
            val fetched = activeRemoteConfig.fetchAndActivate().await()
            // fetchAndActivate() returns false when no new values were activated.
            // Treat both true/false as a successful fetch operation.
            lastFetchSucceeded = true
            lastFetchAtMs = System.currentTimeMillis()
            Log.i(tag, "[Ads][Android] RC fetched success=$fetched")
            logEffectiveAdsConfig()
            fetched
        } catch (error: Exception) {
            lastFetchSucceeded = false
            lastFetchAtMs = System.currentTimeMillis()
            Log.w(tag, "[Ads][Android] RC fetch failed: ${error.message}")
            logEffectiveAdsConfig()
            false
        }
    }

    fun areAdsEnabled(): Boolean = getEffectiveAdsConfig().adsEnabled

    fun isUsingTestAds(): Boolean = getEffectiveAdsConfig().usingTestAds

    fun getAdsEnvironment(): AdsEnvironment =
        AdsEnvironment(
            isDebugBuild = isDebug,
            isSimulatorOrEmulator = isEmulator,
            isReleaseBuild = !isDebug && !isEmulator
        )

    fun getEffectiveAdsConfig(): EffectiveAdsConfig {
        val cached = lastEffectiveConfig
        val state = currentRemoteConfigState()
        val environment = getAdsEnvironment()
        val adsEnabled = state.adsEnabled && state.featureFlags.adsEnabled
        val usingRealAds = adsEnabled && state.adsUseRealAndroid
        val selectedUnits = if (usingRealAds) state.android else TEST_ANDROID_IDS
        val effective = EffectiveAdsConfig(
            adsEnabled = adsEnabled,
            usingRealAds = usingRealAds,
            usingTestAds = !usingRealAds,
            selectedUnits = selectedUnits,
            featureFlags = state.featureFlags,
            adsEnabledTopLevel = state.adsEnabled,
            adsUseRealAndroid = state.adsUseRealAndroid,
            adsUseRealIos = state.adsUseRealIos,
            environment = environment,
            remoteConfigFetched = lastFetchSucceeded,
            remoteConfigFetchedAtMs = lastFetchAtMs
        )
        if (cached != effective) {
            lastEffectiveConfig = effective
        }
        return effective
    }

    fun debugStatus(): EffectiveAdsConfig = getEffectiveAdsConfig()

    fun getAdUnitIds(): AdUnitIds {
        val state = currentRemoteConfigState()
        return AdUnitIds(
            appOpen = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.APP_OPEN,
                remoteConfigState = state
            ),
            adaptiveBanner = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.BANNER,
                remoteConfigState = state
            ),
            fixedBanner = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.BANNER,
                remoteConfigState = state
            ),
            interstitial = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.INTERSTITIAL,
                remoteConfigState = state
            ),
            rewarded = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.REWARDED,
                remoteConfigState = state
            ),
            rewardedInterstitial = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.REWARDED_INTERSTITIAL,
                remoteConfigState = state
            ),
            nativeAdvanced = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.NATIVE,
                remoteConfigState = state
            ),
            nativeVideo = resolveAdUnitId(
                platform = AdPlatform.ANDROID,
                format = AdFormat.NATIVE,
                remoteConfigState = state
            )
        )
    }

    fun resolveAdUnitId(
        platform: AdPlatform,
        format: AdFormat,
        environment: AdsEnvironment = getAdsEnvironment(),
        remoteConfigState: AdsRemoteConfigState = currentRemoteConfigState(),
    ): String {
        val adsEnabled = remoteConfigState.adsEnabled && remoteConfigState.featureFlags.adsEnabled
        val platformWantsRealAds = when (platform) {
            AdPlatform.IOS -> remoteConfigState.adsUseRealIos
            AdPlatform.ANDROID -> remoteConfigState.adsUseRealAndroid
        }
        val useRealAds = adsEnabled && platformWantsRealAds
        val selected = when (platform) {
            AdPlatform.IOS -> if (useRealAds) remoteConfigState.ios else TEST_IOS_IDS
            AdPlatform.ANDROID -> if (useRealAds) remoteConfigState.android else TEST_ANDROID_IDS
        }

        val resolved = when (format) {
            AdFormat.APP_OPEN -> selected.appOpen
            AdFormat.BANNER -> selected.adaptiveBanner
            AdFormat.INTERSTITIAL -> selected.interstitial
            AdFormat.REWARDED -> selected.rewarded
            AdFormat.REWARDED_INTERSTITIAL -> selected.rewardedInterstitial
            AdFormat.NATIVE -> selected.nativeAdvanced
        }
        if (platform == AdPlatform.ANDROID) {
            Log.i(
                tag,
                "[Ads][Android] Selected ad unit for ${format.name.lowercase()} = $resolved " +
                    "useReal=$useRealAds adsEnabled=$adsEnabled platformRealFlag=$platformWantsRealAds " +
                    "debug=${environment.isDebugBuild} emulator=${environment.isSimulatorOrEmulator}"
            )
        }
        return resolved
    }

    fun isFeatureEnabled(key: String): Boolean {
        val flags = parseFeatureFlags()
        return when (key) {
            "ads_enabled" -> flags.adsEnabled
            "native_bridge_enabled" -> flags.nativeBridgeEnabled
            else -> false
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

    fun nativeFeedAdStart(): Int {
        val value = remoteConfig?.getLong("native_feed_ad_start") ?: 4L
        return value.toInt().coerceIn(3, 20)
    }

    fun nativeFeedAdInterval(): Int {
        val value = remoteConfig?.getLong("native_feed_ad_interval") ?: 5L
        return value.toInt().coerceIn(3, 20)
    }

    fun nativePageAdMidpoint(): Double {
        val value = remoteConfig?.getDouble("native_page_ad_midpoint") ?: 0.45
        return value.coerceIn(0.2, 0.9)
    }

    private fun currentRemoteConfigState(): AdsRemoteConfigState {
        val adsEnabled = remoteConfig?.getBoolean("ads_enabled") ?: true
        val adsUseRealIos = remoteConfig?.getBoolean("ads_use_real_ios") ?: true
        val adsUseRealAndroid = remoteConfig?.getBoolean("ads_use_real_android") ?: true
        val featureFlags = parseFeatureFlags()
        val payload = parseAdUnitPayload(remoteConfig?.getString("ad_unit_ids").orEmpty())
        val ios = sanitizeLiveOverrides(
            parsed = parsePlatformUnitIds(
                payload = payload,
                platform = AdPlatform.IOS,
                seed = LIVE_IOS_IDS
            ),
            bundled = LIVE_IOS_IDS,
            platform = AdPlatform.IOS
        )
        val android = sanitizeLiveOverrides(
            parsed = parsePlatformUnitIds(
                payload = payload,
                platform = AdPlatform.ANDROID,
                seed = LIVE_ANDROID_IDS
            ),
            bundled = LIVE_ANDROID_IDS,
            platform = AdPlatform.ANDROID
        )
        return AdsRemoteConfigState(
            adsEnabled = adsEnabled,
            adsUseRealIos = adsUseRealIos,
            adsUseRealAndroid = adsUseRealAndroid,
            featureFlags = featureFlags,
            ios = ios,
            android = android
        )
    }

    private fun parseFeatureFlags(): AdFeatureFlags {
        val raw = remoteConfig?.getString("feature_flags").orEmpty().ifBlank { DEFAULT_FEATURE_FLAGS_JSON }
        return try {
            val json = JSONObject(raw)
            AdFeatureFlags(
                nativeBridgeEnabled = json.optBoolean("native_bridge_enabled", true),
                adsEnabled = json.optBoolean("ads_enabled", true)
            )
        } catch (_: Exception) {
            AdFeatureFlags(nativeBridgeEnabled = true, adsEnabled = true)
        }
    }

    private fun parseAdUnitPayload(raw: String): JSONObject? {
        val normalized = raw.trim()
        if (normalized.isEmpty()) return null
        return try {
            JSONObject(normalized)
        } catch (_: Exception) {
            null
        }
    }

    private fun parsePlatformUnitIds(
        payload: JSONObject?,
        platform: AdPlatform,
        seed: AdUnitIds,
    ): AdUnitIds {
        if (payload == null) return seed
        val platformKey = when (platform) {
            AdPlatform.IOS -> "ios"
            AdPlatform.ANDROID -> "android"
        }

        val platformJson = payload.optJSONObject(platformKey)
        if (platformJson != null) {
            return mergeUnitIds(platformJson, seed)
        }

        // Backward compatibility for flat JSON shape.
        return if (isFlatAdUnitPayload(payload)) mergeUnitIds(payload, seed) else seed
    }

    private fun isFlatAdUnitPayload(payload: JSONObject): Boolean {
        val keys = listOf(
            "appOpen",
            "banner",
            "adaptiveBanner",
            "fixedBanner",
            "interstitial",
            "rewarded",
            "rewardedInterstitial",
            "native",
            "nativeAdvanced",
            "nativeVideo",
        )
        return keys.any { payload.has(it) }
    }

    private fun mergeUnitIds(json: JSONObject, seed: AdUnitIds): AdUnitIds {
        val banner = json.optString("banner", json.optString("adaptiveBanner", seed.adaptiveBanner))
            .trim()
            .ifEmpty { seed.adaptiveBanner }
        val fixedBanner = json.optString("fixedBanner", banner)
            .trim()
            .ifEmpty { banner }
        val native = json.optString("native", json.optString("nativeAdvanced", seed.nativeAdvanced))
            .trim()
            .ifEmpty { seed.nativeAdvanced }
        val nativeVideo = json.optString("nativeVideo", native)
            .trim()
            .ifEmpty { native }

        return AdUnitIds(
            appOpen = json.optString("appOpen", seed.appOpen).trim().ifEmpty { seed.appOpen },
            adaptiveBanner = banner,
            fixedBanner = fixedBanner,
            interstitial = json.optString("interstitial", seed.interstitial).trim().ifEmpty { seed.interstitial },
            rewarded = json.optString("rewarded", seed.rewarded).trim().ifEmpty { seed.rewarded },
            rewardedInterstitial = json.optString("rewardedInterstitial", seed.rewardedInterstitial)
                .trim()
                .ifEmpty { seed.rewardedInterstitial },
            nativeAdvanced = native,
            nativeVideo = nativeVideo
        )
    }

    private fun sanitizeLiveOverrides(
        parsed: AdUnitIds,
        bundled: AdUnitIds,
        platform: AdPlatform,
    ): AdUnitIds {
        fun select(candidate: String, fallback: String, format: String): String {
            if (!candidate.isGoogleSampleAdUnit()) return candidate
            if (fallback.isGoogleSampleAdUnit()) return candidate
            Log.w(
                tag,
                "[Ads][Android] Ignoring sample ad unit override for ${platform.name.lowercase()}:$format and using bundled live unit instead."
            )
            return fallback
        }

        return AdUnitIds(
            appOpen = select(parsed.appOpen, bundled.appOpen, "app_open"),
            adaptiveBanner = select(parsed.adaptiveBanner, bundled.adaptiveBanner, "banner"),
            fixedBanner = select(parsed.fixedBanner, bundled.fixedBanner, "fixed_banner"),
            interstitial = select(parsed.interstitial, bundled.interstitial, "interstitial"),
            rewarded = select(parsed.rewarded, bundled.rewarded, "rewarded"),
            rewardedInterstitial = select(
                parsed.rewardedInterstitial,
                bundled.rewardedInterstitial,
                "rewarded_interstitial"
            ),
            nativeAdvanced = select(parsed.nativeAdvanced, bundled.nativeAdvanced, "native"),
            nativeVideo = select(parsed.nativeVideo, bundled.nativeVideo, "native_video")
        )
    }

    private fun String.isGoogleSampleAdUnit(): Boolean {
        return startsWith("ca-app-pub-3940256099942544/")
    }

    private fun defaultAdUnitIdsPayload(): String {
        return JSONObject()
            .put("ios", toPayload(LIVE_IOS_IDS))
            .put("android", toPayload(LIVE_ANDROID_IDS))
            .toString()
    }

    private fun toPayload(units: AdUnitIds): JSONObject {
        return JSONObject()
            .put("appOpen", units.appOpen)
            .put("banner", units.adaptiveBanner)
            .put("interstitial", units.interstitial)
            .put("rewarded", units.rewarded)
            .put("rewardedInterstitial", units.rewardedInterstitial)
            .put("native", units.nativeAdvanced)
    }

    private fun logEffectiveAdsConfig() {
        val effective = getEffectiveAdsConfig()
        Log.i(
            tag,
            "[Ads][Android] Final ad config adsEnabled=${effective.adsEnabled} " +
                "topLevel=${effective.adsEnabledTopLevel} featureFlag=${effective.featureFlags.adsEnabled} " +
                "useRealAndroid=${effective.adsUseRealAndroid} debug=${effective.environment.isDebugBuild} " +
                "emulator=${effective.environment.isSimulatorOrEmulator} usingTest=${effective.usingTestAds}"
        )
    }

    companion object {
        fun create(firebaseReady: Boolean, isDebug: Boolean = false): RemoteConfigManager {
            val isEmulator = detectEmulator()
            if (!firebaseReady) {
                return RemoteConfigManager(remoteConfig = null, isDebug = isDebug, isEmulator = isEmulator)
            }
            val remoteConfig = try {
                FirebaseRemoteConfig.getInstance()
            } catch (_: Exception) {
                null
            }
            return RemoteConfigManager(
                remoteConfig = remoteConfig,
                isDebug = isDebug,
                isEmulator = isEmulator
            )
        }

        private val TEST_ANDROID_IDS = AdUnitIds(
            appOpen = "ca-app-pub-3940256099942544/9257395921",
            adaptiveBanner = "ca-app-pub-3940256099942544/9214589741",
            fixedBanner = "ca-app-pub-3940256099942544/9214589741",
            interstitial = "ca-app-pub-3940256099942544/1033173712",
            rewarded = "ca-app-pub-3940256099942544/5224354917",
            rewardedInterstitial = "ca-app-pub-3940256099942544/5354046379",
            nativeAdvanced = "ca-app-pub-3940256099942544/2247696110",
            nativeVideo = "ca-app-pub-3940256099942544/2247696110",
        )

        private val TEST_IOS_IDS = AdUnitIds(
            appOpen = "ca-app-pub-3940256099942544/5575463023",
            adaptiveBanner = "ca-app-pub-3940256099942544/2435281174",
            fixedBanner = "ca-app-pub-3940256099942544/2435281174",
            interstitial = "ca-app-pub-3940256099942544/4411468910",
            rewarded = "ca-app-pub-3940256099942544/1712485313",
            rewardedInterstitial = "ca-app-pub-3940256099942544/6978759866",
            nativeAdvanced = "ca-app-pub-3940256099942544/3986624511",
            nativeVideo = "ca-app-pub-3940256099942544/3986624511",
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

        private val LIVE_IOS_IDS = AdUnitIds(
            appOpen = "ca-app-pub-5322412772082850/9489895363",
            adaptiveBanner = "ca-app-pub-5322412772082850/1256686703",
            fixedBanner = "ca-app-pub-5322412772082850/1256686703",
            interstitial = "ca-app-pub-5322412772082850/8775579497",
            rewarded = "ca-app-pub-5322412772082850/6928504142",
            rewardedInterstitial = "ca-app-pub-5322412772082850/1200846386",
            nativeAdvanced = "ca-app-pub-5322412772082850/5615422478",
            nativeVideo = "ca-app-pub-5322412772082850/5615422478",
        )

        private const val DEFAULT_FEATURE_FLAGS_JSON =
            """{"native_bridge_enabled":true,"ads_enabled":true}"""

        private fun detectEmulator(): Boolean {
            val fingerprint = Build.FINGERPRINT.lowercase()
            val model = Build.MODEL.lowercase()
            val brand = Build.BRAND.lowercase()
            val device = Build.DEVICE.lowercase()
            val product = Build.PRODUCT.lowercase()
            val hardware = Build.HARDWARE.lowercase()
            val manufacturer = Build.MANUFACTURER.lowercase()
            return fingerprint.contains("generic") ||
                fingerprint.contains("emulator") ||
                fingerprint.contains("vbox") ||
                model.contains("emulator") ||
                model.contains("sdk_gphone") ||
                model.contains("android sdk built for x86") ||
                manufacturer.contains("genymotion") ||
                hardware.contains("goldfish") ||
                hardware.contains("ranchu") ||
                (brand.startsWith("generic") && device.startsWith("generic")) ||
                product.contains("sdk") ||
                product.contains("simulator")
        }
    }
}
