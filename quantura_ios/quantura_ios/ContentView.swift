import FirebaseCore
#if canImport(FirebaseAuth)
import FirebaseAuth
#endif
#if canImport(FirebaseAnalytics)
import FirebaseAnalytics
#endif
import FirebaseRemoteConfig
#if canImport(GoogleSignIn)
import GoogleSignIn
#endif
#if canImport(AuthenticationServices)
import AuthenticationServices
#endif
#if canImport(CryptoKit)
import CryptoKit
#endif
#if canImport(Combine)
import Combine
#endif
import SwiftUI
import WebKit
#if canImport(StoreKit)
import StoreKit
#endif
#if canImport(UIKit)
import UIKit
#endif
#if canImport(GoogleMobileAds) && canImport(UIKit)
import GoogleMobileAds
#endif

private let quanturaURL = URL(string: "https://quantura.studio/")!

private enum NativeIapCatalog {
    static let defaultProductId = "pro"
    static let iosProductIds: [String] = [
        "goplan",
        "premium",
        "pro",
        "businessplan",
        "annualgoplan",
        "annualplusplan",
        "annualbusinessplan",
    ]
    private static let aliases: [String: String] = [
        "quanturapro": "pro",
        "quanturabusiness": "businessplan",
        "goplanyearly": "annualgoplan",
    ]

    static func normalize(_ rawValue: String) -> String {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return defaultProductId }
        if iosProductIds.contains(trimmed) { return trimmed }
        let lowered = trimmed.lowercased()
        if let mapped = aliases[lowered] { return mapped }
        return defaultProductId
    }
}

final class AdImpressionReporter {
    static let shared = AdImpressionReporter()

    private let callbackURL = URL(string: "https://quantura.studio/api/analytics/ad-impression")

    private init() {}

    func report(
        adFormat: String,
        adUnitId: String,
        placement: String = "",
        rewardType: String = "",
        rewardAmount: Double? = nil
    ) {
        let normalizedFormat = adFormat.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let normalizedUnitId = adUnitId.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedPlacement = placement.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedRewardType = rewardType.trimmingCharacters(in: .whitespacesAndNewlines)

#if canImport(FirebaseAnalytics)
        var params: [String: Any] = [
            AnalyticsParameterAdPlatform: "admob",
            AnalyticsParameterAdSource: "admob",
            AnalyticsParameterAdFormat: normalizedFormat,
            AnalyticsParameterAdUnitName: normalizedUnitId,
            "platform": "ios",
        ]
        if !normalizedPlacement.isEmpty {
            params["placement"] = normalizedPlacement
        }
        if !normalizedRewardType.isEmpty {
            params["reward_type"] = normalizedRewardType
        }
        if let rewardAmount {
            params["reward_amount"] = rewardAmount
        }
        Analytics.logEvent(AnalyticsEventAdImpression, parameters: params)
#endif

        guard callbackURL != nil else { return }
        Task {
            await postCallback(
                payload: [
                    "platform": "ios",
                    "adPlatform": "admob",
                    "adSource": "admob",
                    "adFormat": normalizedFormat,
                    "adUnitId": normalizedUnitId,
                    "placement": normalizedPlacement,
                    "rewardType": normalizedRewardType,
                    "rewardAmount": rewardAmount as Any,
                    "impressionId": UUID().uuidString,
                ]
            )
        }
    }

    private func postCallback(payload: [String: Any]) async {
        guard let callbackURL else { return }
        var request = URLRequest(url: callbackURL)
        request.httpMethod = "POST"
        request.timeoutInterval = 12
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let idToken = await currentIdToken(), !idToken.isEmpty {
            request.setValue("Bearer \(idToken)", forHTTPHeaderField: "Authorization")
        }
        request.httpBody = try? JSONSerialization.data(withJSONObject: payload)
        _ = try? await URLSession.shared.data(for: request)
    }

    private func currentIdToken() async -> String? {
#if canImport(FirebaseAuth)
        guard let user = Auth.auth().currentUser else { return nil }
        return await withCheckedContinuation { continuation in
            user.getIDTokenForcingRefresh(false) { token, _ in
                continuation.resume(returning: token)
            }
        }
#else
        return nil
#endif
    }
}

struct AdFormatStatusSnapshot: Equatable {
    let format: String
    let lastLoadStatus: String
    let lastShowStatus: String
}

final class AdDebugStatusStore {
    static let shared = AdDebugStatusStore()

    private var loadStatuses: [String: String] = [:]
    private var showStatuses: [String: String] = [:]
    private let queue = DispatchQueue(label: "com.quantura.ads.debug.status")

    private init() {}

    func updateLoad(format: String, status: String) {
        queue.async {
            self.loadStatuses[format.lowercased()] = status
        }
    }

    func updateShow(format: String, status: String) {
        queue.async {
            self.showStatuses[format.lowercased()] = status
        }
    }

    func snapshot(for formats: [String]) -> [AdFormatStatusSnapshot] {
        queue.sync {
            formats.map { format in
                let key = format.lowercased()
                return AdFormatStatusSnapshot(
                    format: key,
                    lastLoadStatus: loadStatuses[key] ?? "idle",
                    lastShowStatus: showStatuses[key] ?? "idle"
                )
            }
        }
    }
}

#if canImport(StoreKit)
@available(iOS 15.0, *)
final class StoreKitIapManager: ObservableObject {
    @Published var products: [Product] = []
    private var updatesTask: Task<Void, Never>?

    init() {
        startTransactionListener()
    }

    deinit {
        updatesTask?.cancel()
    }

    func fetchProducts(_ identifiers: [String]) async {
        guard !identifiers.isEmpty else {
            products = []
            return
        }
        do {
            products = try await Product.products(for: identifiers)
        } catch {
            products = []
        }
    }

    func purchase(_ product: Product) async -> Bool {
        do {
            let result = try await product.purchase()
            switch result {
            case .success(let verification):
                if case .verified(let transaction) = verification {
                    await transaction.finish()
                    return true
                }
                return false
            case .pending, .userCancelled:
                return false
            @unknown default:
                return false
            }
        } catch {
            return false
        }
    }

    private func startTransactionListener() {
        updatesTask = Task.detached {
            for await update in Transaction.updates {
                if case .verified(let transaction) = update {
                    await transaction.finish()
                }
            }
        }
    }
}
#endif

// Lightweight DI container to keep view wiring explicit and testable.
final class AppContainer {
    let remoteConfigManager = RemoteConfigManager()
    lazy var adManager = AdManager(remoteConfigManager: remoteConfigManager)
    lazy var appOpenAdManager = AppOpenAdManager(remoteConfigManager: remoteConfigManager, adManager: adManager)
}

enum AdPlatform {
    case ios
    case android
}

enum AdFormat: String {
    case appOpen = "app_open"
    case banner = "banner"
    case interstitial = "interstitial"
    case rewarded = "rewarded"
    case rewardedInterstitial = "rewarded_interstitial"
    case native = "native"
}

struct AdFeatureFlags: Equatable {
    let nativeBridgeEnabled: Bool
    let adsEnabled: Bool
}

struct AdFormatToggles: Equatable {
    let appOpen: Bool
    let banner: Bool
    let interstitial: Bool
    let rewarded: Bool
    let rewardedInterstitial: Bool
    let native: Bool

    func isEnabled(_ format: AdFormat) -> Bool {
        switch format {
        case .appOpen:
            return appOpen
        case .banner:
            return banner
        case .interstitial:
            return interstitial
        case .rewarded:
            return rewarded
        case .rewardedInterstitial:
            return rewardedInterstitial
        case .native:
            return native
        }
    }
}

struct AdsRemoteConfigState: Equatable {
    let adsEnabled: Bool
    let adsUseRealIos: Bool
    let adsUseRealAndroid: Bool
    let featureFlags: AdFeatureFlags
    let iosFormatToggles: AdFormatToggles
    let androidFormatToggles: AdFormatToggles
    let iosUnits: RemoteConfigManager.AdUnitIDs
    let androidUnits: RemoteConfigManager.AdUnitIDs
}

struct AdsEnvironment: Equatable {
    let isDebugBuild: Bool
    let isSimulatorOrEmulator: Bool
    let isReleaseBuild: Bool
}

struct EffectiveAdsConfig: Equatable {
    let adsEnabled: Bool
    let usingRealAds: Bool
    let usingTestAds: Bool
    let selectedUnits: RemoteConfigManager.AdUnitIDs
    let adsEnabledTopLevel: Bool
    let adsUseRealIos: Bool
    let adsUseRealAndroid: Bool
    let featureFlags: AdFeatureFlags
    let environment: AdsEnvironment
    let remoteConfigFetched: Bool
    let remoteConfigFetchedAt: Date?
}

final class RemoteConfigManager {
    private let remoteConfig: RemoteConfig?
    private let isDebugBuild: Bool
    private let isSimulator: Bool
    private let tag = "[Ads][iOS]"

    private var lastFetchSucceeded = false
    private var lastFetchAt: Date?
    private var cachedEffectiveConfig: EffectiveAdsConfig?

    struct AdUnitIDs: Equatable {
        let appOpen: String
        let adaptiveBanner: String
        let fixedBanner: String
        let interstitial: String
        let rewarded: String
        let rewardedInterstitial: String
        let nativeAdvanced: String
        let nativeVideo: String
    }

    private let testIOSIDs = AdUnitIDs(
        appOpen: "ca-app-pub-3940256099942544/5575463023",
        adaptiveBanner: "ca-app-pub-3940256099942544/2435281174",
        fixedBanner: "ca-app-pub-3940256099942544/2435281174",
        interstitial: "ca-app-pub-3940256099942544/4411468910",
        rewarded: "ca-app-pub-3940256099942544/1712485313",
        rewardedInterstitial: "ca-app-pub-3940256099942544/6978759866",
        nativeAdvanced: "ca-app-pub-3940256099942544/3986624511",
        nativeVideo: "ca-app-pub-3940256099942544/3986624511"
    )

    private let liveIOSIDs = AdUnitIDs(
        appOpen: "ca-app-pub-5322412772082850/9489895363",
        adaptiveBanner: "ca-app-pub-5322412772082850/1256686703",
        fixedBanner: "ca-app-pub-5322412772082850/1256686703",
        interstitial: "ca-app-pub-5322412772082850/8775579497",
        rewarded: "ca-app-pub-5322412772082850/6928504142",
        rewardedInterstitial: "ca-app-pub-5322412772082850/1200846386",
        nativeAdvanced: "ca-app-pub-5322412772082850/5615422478",
        nativeVideo: "ca-app-pub-5322412772082850/5615422478"
    )

    private let liveAndroidIDs = AdUnitIDs(
        appOpen: "ca-app-pub-5322412772082850/1802977031",
        adaptiveBanner: "ca-app-pub-5322412772082850/3390017725",
        fixedBanner: "ca-app-pub-5322412772082850/3390017725",
        interstitial: "ca-app-pub-5322412772082850/7358556043",
        rewarded: "ca-app-pub-5322412772082850/1867749156",
        rewardedInterstitial: "ca-app-pub-5322412772082850/4780998745",
        nativeAdvanced: "ca-app-pub-5322412772082850/1144501483",
        nativeVideo: "ca-app-pub-5322412772082850/1144501483"
    )

    private let testAndroidIDs = AdUnitIDs(
        appOpen: "ca-app-pub-3940256099942544/9257395921",
        adaptiveBanner: "ca-app-pub-3940256099942544/9214589741",
        fixedBanner: "ca-app-pub-3940256099942544/9214589741",
        interstitial: "ca-app-pub-3940256099942544/1033173712",
        rewarded: "ca-app-pub-3940256099942544/5224354917",
        rewardedInterstitial: "ca-app-pub-3940256099942544/5354046379",
        nativeAdvanced: "ca-app-pub-3940256099942544/2247696110",
        nativeVideo: "ca-app-pub-3940256099942544/2247696110"
    )

    init() {
        isDebugBuild = _isDebugAssertConfiguration()
#if targetEnvironment(simulator)
        isSimulator = true
#else
        isSimulator = false
#endif

        guard FirebaseApp.app() != nil else {
            remoteConfig = nil
            return
        }
        let rc = RemoteConfig.remoteConfig()
        let settings = RemoteConfigSettings()
        settings.minimumFetchInterval = (isDebugBuild || isSimulator) ? 0 : 3600
        rc.configSettings = settings
        rc.setDefaults([
            "ads_enabled": true as NSObject,
            "ads_use_real_ios": true as NSObject,
            "ads_use_real_android": true as NSObject,
            "ads_ad_inspector_enabled": false as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .android, format: .appOpen): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .android, format: .banner): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .android, format: .interstitial): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .android, format: .rewarded): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .android, format: .rewardedInterstitial): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .android, format: .native): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .ios, format: .appOpen): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .ios, format: .banner): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .ios, format: .interstitial): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .ios, format: .rewarded): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .ios, format: .rewardedInterstitial): true as NSObject,
            RemoteConfigManager.adFormatEnabledKey(platform: .ios, format: .native): true as NSObject,
            "ad_unit_ids": RemoteConfigManager.defaultAdUnitIdsSeedPayload as NSObject,
            "native_feed_ad_start": 4 as NSObject,
            "native_feed_ad_interval": 5 as NSObject,
            "native_page_ad_midpoint": 0.45 as NSObject,
            "feature_flags": """
            {"native_bridge_enabled":true,"ads_enabled":true}
            """ as NSObject,
        ])
        remoteConfig = rc
    }

    func fetchAndActivate(completion: ((Bool) -> Void)? = nil) {
        guard let remoteConfig else {
            lastFetchSucceeded = false
            lastFetchAt = Date()
            logEffectiveAdsConfig()
            completion?(false)
            return
        }
        remoteConfig.fetchAndActivate { status, _ in
            let ok = status == .successFetchedFromRemote || status == .successUsingPreFetchedData
            self.lastFetchSucceeded = ok
            self.lastFetchAt = Date()
            print("\(self.tag) RC fetched success=\(ok)")
            self.logEffectiveAdsConfig()
            completion?(ok)
        }
    }

    func adsEnvironment() -> AdsEnvironment {
        AdsEnvironment(
            isDebugBuild: isDebugBuild,
            isSimulatorOrEmulator: isSimulator,
            isReleaseBuild: !isDebugBuild && !isSimulator
        )
    }

    func areAdsEnabled() -> Bool {
        effectiveAdsConfig().adsEnabled
    }

    func isAdFormatEnabled(
        platform: AdPlatform = .ios,
        format: AdFormat,
        remoteConfigState: AdsRemoteConfigState? = nil
    ) -> Bool {
        let state = remoteConfigState ?? currentRemoteConfigState()
        let adsEnabled = state.adsEnabled && state.featureFlags.adsEnabled
        let formatEnabled = (platform == .ios)
            ? state.iosFormatToggles.isEnabled(format)
            : state.androidFormatToggles.isEnabled(format)
        return adsEnabled && formatEnabled
    }

    func isUsingTestAds() -> Bool {
        effectiveAdsConfig().usingTestAds
    }

    func isAdInspectorEnabled() -> Bool {
        remoteConfig?.configValue(forKey: "ads_ad_inspector_enabled").boolValue ?? false
    }

    func debugStatus() -> EffectiveAdsConfig {
        effectiveAdsConfig()
    }

    func effectiveAdsConfig() -> EffectiveAdsConfig {
        let state = currentRemoteConfigState()
        let environment = adsEnvironment()
        let adsEnabled = state.adsEnabled && state.featureFlags.adsEnabled
        let usingRealAds = adsEnabled && state.adsUseRealIos
        let selectedUnits = usingRealAds ? state.iosUnits : testIOSIDs
        let effective = EffectiveAdsConfig(
            adsEnabled: adsEnabled,
            usingRealAds: usingRealAds,
            usingTestAds: !usingRealAds,
            selectedUnits: selectedUnits,
            adsEnabledTopLevel: state.adsEnabled,
            adsUseRealIos: state.adsUseRealIos,
            adsUseRealAndroid: state.adsUseRealAndroid,
            featureFlags: state.featureFlags,
            environment: environment,
            remoteConfigFetched: lastFetchSucceeded,
            remoteConfigFetchedAt: lastFetchAt
        )
        if cachedEffectiveConfig != effective {
            cachedEffectiveConfig = effective
        }
        return effective
    }

    func adUnitIDs() -> AdUnitIDs {
        let state = currentRemoteConfigState()
        return AdUnitIDs(
            appOpen: resolveAdUnitId(platform: .ios, format: .appOpen, remoteConfigState: state),
            adaptiveBanner: resolveAdUnitId(platform: .ios, format: .banner, remoteConfigState: state),
            fixedBanner: resolveAdUnitId(platform: .ios, format: .banner, remoteConfigState: state),
            interstitial: resolveAdUnitId(platform: .ios, format: .interstitial, remoteConfigState: state),
            rewarded: resolveAdUnitId(platform: .ios, format: .rewarded, remoteConfigState: state),
            rewardedInterstitial: resolveAdUnitId(platform: .ios, format: .rewardedInterstitial, remoteConfigState: state),
            nativeAdvanced: resolveAdUnitId(platform: .ios, format: .native, remoteConfigState: state),
            nativeVideo: resolveAdUnitId(platform: .ios, format: .native, remoteConfigState: state)
        )
    }

    func resolveAdUnitId(
        platform: AdPlatform,
        format: AdFormat,
        environment: AdsEnvironment? = nil,
        remoteConfigState: AdsRemoteConfigState? = nil
    ) -> String {
        let resolvedEnvironment = environment ?? adsEnvironment()
        let state = remoteConfigState ?? currentRemoteConfigState()
        let adsEnabled = state.adsEnabled && state.featureFlags.adsEnabled
        let platformUseRealAds = (platform == .ios) ? state.adsUseRealIos : state.adsUseRealAndroid
        let formatEnabled = (platform == .ios)
            ? state.iosFormatToggles.isEnabled(format)
            : state.androidFormatToggles.isEnabled(format)
        let useRealAds = adsEnabled && platformUseRealAds
        let selected: AdUnitIDs
        switch platform {
        case .ios:
            selected = useRealAds ? state.iosUnits : testIOSIDs
        case .android:
            selected = useRealAds ? state.androidUnits : testAndroidIDs
        }

        let adUnitId: String
        switch format {
        case .appOpen:
            adUnitId = selected.appOpen
        case .banner:
            adUnitId = selected.adaptiveBanner
        case .interstitial:
            adUnitId = selected.interstitial
        case .rewarded:
            adUnitId = selected.rewarded
        case .rewardedInterstitial:
            adUnitId = selected.rewardedInterstitial
        case .native:
            adUnitId = selected.nativeAdvanced
        }
        if platform == .ios {
            print(
                "\(tag) Selected ad unit for \(format.rawValue) = \(adUnitId) " +
                    "useReal=\(useRealAds) adsEnabled=\(adsEnabled) formatEnabled=\(formatEnabled) platformRealFlag=\(platformUseRealAds) " +
                    "debug=\(resolvedEnvironment.isDebugBuild) simulator=\(resolvedEnvironment.isSimulatorOrEmulator)"
            )
        }
        return adUnitId
    }

    func featureFlag(_ key: String, default defaultValue: Bool = false) -> Bool {
        let flags = parseFeatureFlags()
        switch key {
        case "native_bridge_enabled":
            return flags.nativeBridgeEnabled
        case "ads_enabled":
            return flags.adsEnabled
        default:
            return defaultValue
        }
    }

    func nativeFeedAdStart() -> Int {
        let value = Int(remoteConfig?["native_feed_ad_start"].numberValue.intValue ?? 4)
        return max(3, min(20, value))
    }

    func nativeFeedAdInterval() -> Int {
        let value = Int(remoteConfig?["native_feed_ad_interval"].numberValue.intValue ?? 5)
        return max(3, min(20, value))
    }

    func nativePageAdMidpoint() -> Double {
        let value = remoteConfig?["native_page_ad_midpoint"].numberValue.doubleValue ?? 0.45
        return max(0.2, min(0.9, value))
    }

    private func currentRemoteConfigState() -> AdsRemoteConfigState {
        let adsEnabled = remoteConfig?.configValue(forKey: "ads_enabled").boolValue ?? true
        let adsUseRealIos = remoteConfig?.configValue(forKey: "ads_use_real_ios").boolValue ?? true
        let adsUseRealAndroid = remoteConfig?.configValue(forKey: "ads_use_real_android").boolValue ?? true
        let featureFlags = parseFeatureFlags()
        let iosFormatToggles = parseAdFormatToggles(platform: .ios)
        let androidFormatToggles = parseAdFormatToggles(platform: .android)
        let payload = parseAdUnitPayload(remoteConfig?["ad_unit_ids"].stringValue ?? "")
        let iosUnits = sanitizeLiveOverrides(
            parsePlatformUnitIDs(payload: payload, platform: .ios, seed: liveIOSIDs),
            bundled: liveIOSIDs,
            platform: .ios
        )
        let androidUnits = sanitizeLiveOverrides(
            parsePlatformUnitIDs(payload: payload, platform: .android, seed: liveAndroidIDs),
            bundled: liveAndroidIDs,
            platform: .android
        )
        return AdsRemoteConfigState(
            adsEnabled: adsEnabled,
            adsUseRealIos: adsUseRealIos,
            adsUseRealAndroid: adsUseRealAndroid,
            featureFlags: featureFlags,
            iosFormatToggles: iosFormatToggles,
            androidFormatToggles: androidFormatToggles,
            iosUnits: iosUnits,
            androidUnits: androidUnits
        )
    }

    private func parseAdFormatToggles(platform: AdPlatform) -> AdFormatToggles {
        AdFormatToggles(
            appOpen: remoteConfig?.configValue(forKey: Self.adFormatEnabledKey(platform: platform, format: .appOpen)).boolValue ?? true,
            banner: remoteConfig?.configValue(forKey: Self.adFormatEnabledKey(platform: platform, format: .banner)).boolValue ?? true,
            interstitial: remoteConfig?.configValue(forKey: Self.adFormatEnabledKey(platform: platform, format: .interstitial)).boolValue ?? true,
            rewarded: remoteConfig?.configValue(forKey: Self.adFormatEnabledKey(platform: platform, format: .rewarded)).boolValue ?? true,
            rewardedInterstitial: remoteConfig?.configValue(forKey: Self.adFormatEnabledKey(platform: platform, format: .rewardedInterstitial)).boolValue ?? true,
            native: remoteConfig?.configValue(forKey: Self.adFormatEnabledKey(platform: platform, format: .native)).boolValue ?? true
        )
    }

    private func parseFeatureFlags() -> AdFeatureFlags {
        guard
            let data = (remoteConfig?["feature_flags"].stringValue ?? "")
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .ifEmpty(replacement: #"{"native_bridge_enabled":true,"ads_enabled":true}"#)
                .data(using: .utf8),
            let json = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        else {
            return AdFeatureFlags(nativeBridgeEnabled: true, adsEnabled: true)
        }
        return AdFeatureFlags(
            nativeBridgeEnabled: (json["native_bridge_enabled"] as? Bool) ?? true,
            adsEnabled: (json["ads_enabled"] as? Bool) ?? true
        )
    }

    private func parseAdUnitPayload(_ raw: String) -> [String: Any]? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, let data = trimmed.data(using: .utf8) else {
            return nil
        }
        return (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
    }

    private func parsePlatformUnitIDs(
        payload: [String: Any]?,
        platform: AdPlatform,
        seed: AdUnitIDs
    ) -> AdUnitIDs {
        guard let payload else { return seed }
        let platformKey = platform == .ios ? "ios" : "android"
        if let nested = payload[platformKey] as? [String: Any] {
            return mergeUnits(nested: nested, seed: seed)
        }
        if payload.keys.contains(where: { ["appOpen", "banner", "interstitial", "rewarded", "rewardedInterstitial", "native", "adaptiveBanner", "nativeAdvanced"].contains($0) }) {
            return mergeUnits(nested: payload, seed: seed)
        }
        return seed
    }

    private func mergeUnits(nested: [String: Any], seed: AdUnitIDs) -> AdUnitIDs {
        let banner = ((nested["banner"] as? String) ?? (nested["adaptiveBanner"] as? String) ?? seed.adaptiveBanner)
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .ifEmpty(replacement: seed.adaptiveBanner)
        let fixedBanner = ((nested["fixedBanner"] as? String) ?? banner)
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .ifEmpty(replacement: banner)
        let native = ((nested["native"] as? String) ?? (nested["nativeAdvanced"] as? String) ?? seed.nativeAdvanced)
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .ifEmpty(replacement: seed.nativeAdvanced)
        let nativeVideo = ((nested["nativeVideo"] as? String) ?? native)
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .ifEmpty(replacement: native)
        return AdUnitIDs(
            appOpen: ((nested["appOpen"] as? String) ?? seed.appOpen).trimmingCharacters(in: .whitespacesAndNewlines).ifEmpty(replacement: seed.appOpen),
            adaptiveBanner: banner,
            fixedBanner: fixedBanner,
            interstitial: ((nested["interstitial"] as? String) ?? seed.interstitial).trimmingCharacters(in: .whitespacesAndNewlines).ifEmpty(replacement: seed.interstitial),
            rewarded: ((nested["rewarded"] as? String) ?? seed.rewarded).trimmingCharacters(in: .whitespacesAndNewlines).ifEmpty(replacement: seed.rewarded),
            rewardedInterstitial: ((nested["rewardedInterstitial"] as? String) ?? seed.rewardedInterstitial).trimmingCharacters(in: .whitespacesAndNewlines).ifEmpty(replacement: seed.rewardedInterstitial),
            nativeAdvanced: native,
            nativeVideo: nativeVideo
        )
    }

    static func adFormatEnabledKey(platform: AdPlatform, format: AdFormat) -> String {
        let platformKey = platform == .ios ? "ios" : "android"
        return "ads_\(platformKey)_\(format.rawValue)_enabled"
    }

    private func sanitizeLiveOverrides(
        _ parsed: AdUnitIDs,
        bundled: AdUnitIDs,
        platform: AdPlatform
    ) -> AdUnitIDs {
        func select(_ candidate: String, fallback: String, format: String) -> String {
            if !candidate.isGoogleSampleAdUnit { return candidate }
            if fallback.isGoogleSampleAdUnit { return candidate }
            let platformName = platform == .ios ? "ios" : "android"
            print("\(tag) Ignoring sample ad unit override for \(platformName):\(format) and using bundled live unit instead.")
            return fallback
        }

        return AdUnitIDs(
            appOpen: select(parsed.appOpen, fallback: bundled.appOpen, format: "app_open"),
            adaptiveBanner: select(parsed.adaptiveBanner, fallback: bundled.adaptiveBanner, format: "banner"),
            fixedBanner: select(parsed.fixedBanner, fallback: bundled.fixedBanner, format: "fixed_banner"),
            interstitial: select(parsed.interstitial, fallback: bundled.interstitial, format: "interstitial"),
            rewarded: select(parsed.rewarded, fallback: bundled.rewarded, format: "rewarded"),
            rewardedInterstitial: select(parsed.rewardedInterstitial, fallback: bundled.rewardedInterstitial, format: "rewarded_interstitial"),
            nativeAdvanced: select(parsed.nativeAdvanced, fallback: bundled.nativeAdvanced, format: "native"),
            nativeVideo: select(parsed.nativeVideo, fallback: bundled.nativeVideo, format: "native_video")
        )
    }

    private func logEffectiveAdsConfig() {
        let config = effectiveAdsConfig()
        print(
            "\(tag) Final ad config adsEnabled=\(config.adsEnabled) " +
                "topLevel=\(config.adsEnabledTopLevel) featureFlag=\(config.featureFlags.adsEnabled) " +
                "useRealIOS=\(config.adsUseRealIos) debug=\(config.environment.isDebugBuild) " +
                "simulator=\(config.environment.isSimulatorOrEmulator) usingTest=\(config.usingTestAds)"
        )
    }

    private static let defaultAdUnitIdsSeedPayload =
        #"{"ios":{"appOpen":"ca-app-pub-5322412772082850/9489895363","banner":"ca-app-pub-5322412772082850/1256686703","interstitial":"ca-app-pub-5322412772082850/8775579497","rewarded":"ca-app-pub-5322412772082850/6928504142","rewardedInterstitial":"ca-app-pub-5322412772082850/1200846386","native":"ca-app-pub-5322412772082850/5615422478"},"android":{"appOpen":"ca-app-pub-5322412772082850/1802977031","banner":"ca-app-pub-5322412772082850/3390017725","interstitial":"ca-app-pub-5322412772082850/7358556043","rewarded":"ca-app-pub-5322412772082850/1867749156","rewardedInterstitial":"ca-app-pub-5322412772082850/4780998745","native":"ca-app-pub-5322412772082850/1144501483"}}"#
}

#if canImport(GoogleMobileAds) && canImport(UIKit)
final class AdManager: NSObject, FullScreenContentDelegate {
    private let remoteConfigManager: RemoteConfigManager
    private var interstitialAd: InterstitialAd?
    private var rewardedAd: RewardedAd?
    private var rewardedInterstitialAd: RewardedInterstitialAd?
    private var nativeFeedLoaders: [String: NativeFeedLoader] = [:]
    private(set) var isShowingFullScreenAd = false
    private var interstitialLoadInFlight = false
    private var rewardedLoadInFlight = false
    private var rewardedInterstitialLoadInFlight = false
    private var pendingInterstitialRequest: PendingInterstitialRequest?
    private var pendingRewardedRequest: PendingRewardedRequest?
    private var interstitialCallbackContext: FullscreenAdCallbackContext?
    private var rewardedCallbackContext: FullscreenAdCallbackContext?
    private var rewardedInterstitialCallbackContext: FullscreenAdCallbackContext?

    private struct FullscreenAdCallbackContext {
        let requestId: String
        let adFormat: String
        let callback: ([String: Any]) -> Void
    }

    private struct PendingInterstitialRequest {
        let rootViewController: UIViewController
        let requestId: String
        let callback: ([String: Any]) -> Void
    }

    private struct PendingRewardedRequest {
        let rootViewController: UIViewController
        let requestId: String
        let preferRewardedInterstitial: Bool
        let callback: ([String: Any]) -> Void
    }

    private final class NativeFeedLoader: NSObject, AdLoaderDelegate, NativeAdLoaderDelegate {
        let key: String
        private(set) var adLoader: AdLoader?
        private let onLoaded: (NativeAd) -> Void
        private let onFailed: (Error) -> Void
        private let onFinished: (String) -> Void

        init(
            key: String,
            onLoaded: @escaping (NativeAd) -> Void,
            onFailed: @escaping (Error) -> Void,
            onFinished: @escaping (String) -> Void
        ) {
            self.key = key
            self.onLoaded = onLoaded
            self.onFailed = onFailed
            self.onFinished = onFinished
        }

        func attach(_ adLoader: AdLoader) {
            self.adLoader = adLoader
            adLoader.delegate = self
        }

        func adLoader(_ adLoader: AdLoader, didReceive nativeAd: NativeAd) {
            onLoaded(nativeAd)
            onFinished(key)
        }

        func adLoader(_ adLoader: AdLoader, didFailToReceiveAdWithError error: Error) {
            onFailed(error)
            onFinished(key)
        }
    }

    init(remoteConfigManager: RemoteConfigManager) {
        self.remoteConfigManager = remoteConfigManager
    }

    func primeAds() {
        guard remoteConfigManager.areAdsEnabled() else {
            print("[Ads][iOS] Prime skipped because ads are disabled.")
            return
        }
        print(
            "[Ads][iOS] Priming all formats usingTestAds=\(remoteConfigManager.isUsingTestAds()) " +
                "debug=\(remoteConfigManager.adsEnvironment().isDebugBuild) " +
                "simulator=\(remoteConfigManager.adsEnvironment().isSimulatorOrEmulator)"
        )
        loadInterstitial()
        loadRewarded()
        loadRewardedInterstitial()
    }

    func preloadAllFormatsForQa() {
        primeAds()
        requestNativeFeedAd(slotId: "qa-native-slot", placement: "qa_panel", variant: "nativeAdvanced") { detail in
            let ok = (detail["ok"] as? Bool) ?? false
            if ok {
                AdDebugStatusStore.shared.updateLoad(format: "native", status: "loaded")
            } else {
                let reason = (detail["error"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines)
                AdDebugStatusStore.shared.updateLoad(
                    format: "native",
                    status: "failed:\(reason?.isEmpty == false ? reason! : "native_request_failed")"
                )
            }
        }
    }

    func debugStatusSnapshots() -> [AdFormatStatusSnapshot] {
        AdDebugStatusStore.shared.snapshot(
            for: ["app_open", "banner", "interstitial", "rewarded", "rewarded_interstitial", "native"]
        )
    }

    func showInterstitial(
        from rootViewController: UIViewController?,
        requestId: String = "",
        callback: @escaping ([String: Any]) -> Void = { _ in }
    ) {
        DispatchQueue.main.async {
            guard self.remoteConfigManager.isAdFormatEnabled(format: .interstitial) else {
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: "interstitial",
                    status: "skipped:format_off",
                    message: "Interstitial ads are disabled."
                )
                return
            }
            guard self.remoteConfigManager.areAdsEnabled() else {
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: "interstitial",
                    status: "skipped:ads_disabled",
                    message: "Ads are disabled."
                )
                return
            }
            guard let rootViewController else {
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: "interstitial",
                    status: "failed",
                    message: "Unable to resolve a presenting view controller."
                )
                return
            }
            guard !self.isShowingFullScreenAd else {
                print("[Ads][iOS] Interstitial show skipped; another fullscreen ad is visible.")
                AdDebugStatusStore.shared.updateShow(format: "interstitial", status: "skipped:fullscreen_visible")
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: "interstitial",
                    status: "skipped:fullscreen_visible",
                    message: "Another fullscreen ad is currently visible."
                )
                return
            }
            guard let ad = self.interstitialAd else {
                print("[Ads][iOS] Interstitial unavailable; queueing until load completes.")
                AdDebugStatusStore.shared.updateShow(format: "interstitial", status: "queued:not_ready")
                self.pendingInterstitialRequest = PendingInterstitialRequest(
                    rootViewController: rootViewController,
                    requestId: requestId,
                    callback: callback
                )
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: "interstitial",
                    status: "queued",
                    message: "Interstitial ad is still loading."
                )
                self.loadInterstitial()
                return
            }
            ad.fullScreenContentDelegate = self
            self.interstitialCallbackContext = FullscreenAdCallbackContext(
                requestId: requestId,
                adFormat: "interstitial",
                callback: callback
            )
            print("[Ads][iOS] Presenting interstitial.")
            ad.present(from: rootViewController)
        }
    }

    func showRewarded(
        from rootViewController: UIViewController?,
        requestId: String = "",
        preferRewardedInterstitial: Bool = true,
        callback: @escaping ([String: Any]) -> Void = { _ in }
    ) {
        DispatchQueue.main.async {
            let rewardedEnabled = self.remoteConfigManager.isAdFormatEnabled(format: .rewarded)
            let rewardedInterstitialEnabled = self.remoteConfigManager.isAdFormatEnabled(format: .rewardedInterstitial)
            let preferredFormat = preferRewardedInterstitial ? "rewarded_interstitial" : "rewarded"
            guard rewardedEnabled || rewardedInterstitialEnabled else {
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: preferredFormat,
                    status: "skipped:format_off",
                    message: "Rewarded ads are disabled."
                )
                return
            }
            guard self.remoteConfigManager.areAdsEnabled() else {
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: preferredFormat,
                    status: "skipped:ads_disabled",
                    message: "Ads are disabled."
                )
                return
            }
            guard let rootViewController else {
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: preferredFormat,
                    status: "failed",
                    message: "Unable to resolve a presenting view controller."
                )
                return
            }
            guard !self.isShowingFullScreenAd else {
                print("[Ads][iOS] Rewarded show skipped; another fullscreen ad is visible.")
                AdDebugStatusStore.shared.updateShow(format: "rewarded", status: "skipped:fullscreen_visible")
                AdDebugStatusStore.shared.updateShow(format: "rewarded_interstitial", status: "skipped:fullscreen_visible")
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: preferredFormat,
                    status: "skipped:fullscreen_visible",
                    message: "Another fullscreen ad is currently visible."
                )
                return
            }
            if preferRewardedInterstitial, rewardedInterstitialEnabled, let rewardedInterstitial = self.rewardedInterstitialAd {
                rewardedInterstitial.fullScreenContentDelegate = self
                self.rewardedInterstitialCallbackContext = FullscreenAdCallbackContext(
                    requestId: requestId,
                    adFormat: "rewarded_interstitial",
                    callback: callback
                )
                print("[Ads][iOS] Presenting rewarded interstitial.")
                rewardedInterstitial.present(from: rootViewController) {
                    let reward = rewardedInterstitial.adReward
                    self.emitAdActionResult(
                        callback: callback,
                        requestId: requestId,
                        adFormat: "rewarded_interstitial",
                        status: "rewarded",
                        rewardType: reward.type,
                        rewardAmount: reward.amount
                    )
                }
                return
            }
            guard rewardedEnabled else {
                self.pendingRewardedRequest = PendingRewardedRequest(
                    rootViewController: rootViewController,
                    requestId: requestId,
                    preferRewardedInterstitial: preferRewardedInterstitial,
                    callback: callback
                )
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: preferredFormat,
                    status: "queued",
                    message: "Rewarded ad is still loading."
                )
                self.loadRewardedInterstitial()
                return
            }
            guard let ad = self.rewardedAd else {
                print("[Ads][iOS] Rewarded unavailable; queueing until load completes.")
                AdDebugStatusStore.shared.updateShow(format: "rewarded", status: "queued:not_ready")
                self.pendingRewardedRequest = PendingRewardedRequest(
                    rootViewController: rootViewController,
                    requestId: requestId,
                    preferRewardedInterstitial: preferRewardedInterstitial,
                    callback: callback
                )
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: preferredFormat,
                    status: "queued",
                    message: "Rewarded ad is still loading."
                )
                self.loadRewarded()
                self.loadRewardedInterstitial()
                return
            }
            ad.fullScreenContentDelegate = self
            self.rewardedCallbackContext = FullscreenAdCallbackContext(
                requestId: requestId,
                adFormat: "rewarded",
                callback: callback
            )
            print("[Ads][iOS] Presenting rewarded.")
            ad.present(from: rootViewController) {
                let reward = ad.adReward
                self.emitAdActionResult(
                    callback: callback,
                    requestId: requestId,
                    adFormat: "rewarded",
                    status: "rewarded",
                    rewardType: reward.type,
                    rewardAmount: reward.amount
                )
            }
        }
    }

    func requestNativeFeedAd(
        slotId: String,
        placement: String,
        variant: String = "nativeAdvanced",
        completion: @escaping ([String: Any]) -> Void
    ) {
        let normalizedSlotId = slotId.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedPlacement = placement.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? "feed"
            : placement.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedSlotId.isEmpty else {
            completion(buildNativeFeedErrorPayload(slotId: "", placement: normalizedPlacement, reason: "slot_id_missing"))
            return
        }
        guard remoteConfigManager.areAdsEnabled() else {
            completion(buildNativeFeedErrorPayload(slotId: normalizedSlotId, placement: normalizedPlacement, reason: "ads_disabled"))
            return
        }
        guard remoteConfigManager.isAdFormatEnabled(format: .native) else {
            completion(buildNativeFeedErrorPayload(slotId: normalizedSlotId, placement: normalizedPlacement, reason: "format_off"))
            return
        }

        let useVideo = variant.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "nativevideo"
        let adUnitId = useVideo
            ? remoteConfigManager.adUnitIDs().nativeVideo
            : remoteConfigManager.resolveAdUnitId(platform: .ios, format: .native)
        logNativeAdEvent(name: "ad_request", placement: normalizedPlacement, adUnitId: adUnitId, slotId: normalizedSlotId)
        AdDebugStatusStore.shared.updateLoad(format: "native", status: "loading")

        let loadKey = "\(normalizedSlotId)-\(UUID().uuidString)"
        let loader = NativeFeedLoader(
            key: loadKey,
            onLoaded: { [weak self] ad in
                guard let self else { return }
                self.logNativeAdEvent(name: "ad_loaded", placement: normalizedPlacement, adUnitId: adUnitId, slotId: normalizedSlotId)
                print("[Ads][iOS] Load success for native")
                AdDebugStatusStore.shared.updateLoad(format: "native", status: "loaded")
                completion([
                    "ok": true,
                    "slotId": normalizedSlotId,
                    "placement": normalizedPlacement,
                    "adUnitId": adUnitId,
                    "ad": self.serializeNativeAd(ad: ad, adUnitId: adUnitId),
                ])
            },
            onFailed: { [weak self] error in
                guard let self else { return }
                let reason = error.localizedDescription.isEmpty ? "native_load_failed" : error.localizedDescription
                self.logNativeAdEvent(
                    name: "ad_failed",
                    placement: normalizedPlacement,
                    adUnitId: adUnitId,
                    slotId: normalizedSlotId,
                    reason: reason
                )
                print("[Ads][iOS] Load fail for native: \(reason)")
                AdDebugStatusStore.shared.updateLoad(format: "native", status: "failed:\(reason)")
                completion(self.buildNativeFeedErrorPayload(slotId: normalizedSlotId, placement: normalizedPlacement, reason: reason))
            },
            onFinished: { [weak self] key in
                self?.nativeFeedLoaders.removeValue(forKey: key)
            }
        )
        let adLoader = AdLoader(
            adUnitID: adUnitId,
            rootViewController: nil,
            adTypes: [AdLoaderAdType.native],
            options: nil
        )
        loader.attach(adLoader)
        nativeFeedLoaders[loadKey] = loader
        adLoader.load(Request())
    }

    func reportNativeFeedAdImpression(slotId: String, placement: String, adUnitId: String) {
        let normalizedPlacement = placement.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? "feed"
            : placement.trimmingCharacters(in: .whitespacesAndNewlines)
        let resolvedAdUnit = adUnitId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? remoteConfigManager.resolveAdUnitId(platform: .ios, format: .native)
            : adUnitId.trimmingCharacters(in: .whitespacesAndNewlines)
        AdImpressionReporter.shared.report(
            adFormat: "native",
            adUnitId: resolvedAdUnit,
            placement: normalizedPlacement
        )
        AdDebugStatusStore.shared.updateShow(format: "native", status: "impression")
        logNativeAdEvent(name: "ad_impression", placement: normalizedPlacement, adUnitId: resolvedAdUnit, slotId: slotId)
    }

    func reportNativeFeedAdClick(slotId: String, placement: String, adUnitId: String) {
        let normalizedPlacement = placement.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? "feed"
            : placement.trimmingCharacters(in: .whitespacesAndNewlines)
        let resolvedAdUnit = adUnitId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? remoteConfigManager.resolveAdUnitId(platform: .ios, format: .native)
            : adUnitId.trimmingCharacters(in: .whitespacesAndNewlines)
        logNativeAdEvent(name: "ad_click", placement: normalizedPlacement, adUnitId: resolvedAdUnit, slotId: slotId)
    }

    private func loadInterstitial() {
        guard remoteConfigManager.isAdFormatEnabled(format: .interstitial) else {
            interstitialAd = nil
            interstitialLoadInFlight = false
            AdDebugStatusStore.shared.updateLoad(format: "interstitial", status: "disabled:format_off")
            return
        }
        guard !interstitialLoadInFlight else { return }
        interstitialLoadInFlight = true
        let adUnitID = remoteConfigManager.resolveAdUnitId(platform: .ios, format: .interstitial)
        print("[Ads][iOS] Loading interstitial unit=\(adUnitID)")
        AdDebugStatusStore.shared.updateLoad(format: "interstitial", status: "loading")
        InterstitialAd.load(with: adUnitID, request: Request()) { [weak self] ad, error in
            guard let self else { return }
            self.interstitialLoadInFlight = false
            self.interstitialAd = ad
            self.interstitialAd?.fullScreenContentDelegate = self
            if let error {
                print("[Ads][iOS] Interstitial load failed: \(error.localizedDescription)")
                print("[Ads][iOS] Load fail for interstitial: \(error.localizedDescription)")
                AdDebugStatusStore.shared.updateLoad(format: "interstitial", status: "failed:\(error.localizedDescription)")
                self.failPendingInterstitialRequest(message: error.localizedDescription)
            } else {
                print("[Ads][iOS] Interstitial load succeeded.")
                print("[Ads][iOS] Load success for interstitial")
                AdDebugStatusStore.shared.updateLoad(format: "interstitial", status: "loaded")
                self.drainPendingInterstitialRequest()
            }
        }
    }

    private func loadRewarded() {
        guard remoteConfigManager.isAdFormatEnabled(format: .rewarded) else {
            rewardedAd = nil
            rewardedLoadInFlight = false
            AdDebugStatusStore.shared.updateLoad(format: "rewarded", status: "disabled:format_off")
            return
        }
        guard !rewardedLoadInFlight else { return }
        rewardedLoadInFlight = true
        let adUnitID = remoteConfigManager.resolveAdUnitId(platform: .ios, format: .rewarded)
        print("[Ads][iOS] Loading rewarded unit=\(adUnitID)")
        AdDebugStatusStore.shared.updateLoad(format: "rewarded", status: "loading")
        RewardedAd.load(with: adUnitID, request: Request()) { [weak self] ad, error in
            guard let self else { return }
            self.rewardedLoadInFlight = false
            self.rewardedAd = ad
            self.rewardedAd?.fullScreenContentDelegate = self
            self.configureServerSideVerification(for: self.rewardedAd, adFormat: "rewarded")
            if let error {
                print("[Ads][iOS] Rewarded load failed: \(error.localizedDescription)")
                print("[Ads][iOS] Load fail for rewarded: \(error.localizedDescription)")
                AdDebugStatusStore.shared.updateLoad(format: "rewarded", status: "failed:\(error.localizedDescription)")
                self.maybeFailPendingRewardedRequest(message: error.localizedDescription)
            } else {
                print("[Ads][iOS] Rewarded load succeeded.")
                print("[Ads][iOS] Load success for rewarded")
                AdDebugStatusStore.shared.updateLoad(format: "rewarded", status: "loaded")
                self.drainPendingRewardedRequest()
            }
        }
    }

    private func loadRewardedInterstitial() {
        guard remoteConfigManager.isAdFormatEnabled(format: .rewardedInterstitial) else {
            rewardedInterstitialAd = nil
            rewardedInterstitialLoadInFlight = false
            AdDebugStatusStore.shared.updateLoad(format: "rewarded_interstitial", status: "disabled:format_off")
            return
        }
        guard !rewardedInterstitialLoadInFlight else { return }
        rewardedInterstitialLoadInFlight = true
        let adUnitID = remoteConfigManager.resolveAdUnitId(platform: .ios, format: .rewardedInterstitial)
        print("[Ads][iOS] Loading rewarded interstitial unit=\(adUnitID)")
        AdDebugStatusStore.shared.updateLoad(format: "rewarded_interstitial", status: "loading")
        RewardedInterstitialAd.load(with: adUnitID, request: Request()) { [weak self] ad, error in
            guard let self else { return }
            self.rewardedInterstitialLoadInFlight = false
            self.rewardedInterstitialAd = ad
            self.rewardedInterstitialAd?.fullScreenContentDelegate = self
            self.configureServerSideVerification(for: self.rewardedInterstitialAd, adFormat: "rewarded_interstitial")
            if let error {
                print("[Ads][iOS] Rewarded interstitial load failed: \(error.localizedDescription)")
                print("[Ads][iOS] Load fail for rewarded_interstitial: \(error.localizedDescription)")
                AdDebugStatusStore.shared.updateLoad(format: "rewarded_interstitial", status: "failed:\(error.localizedDescription)")
                self.maybeFailPendingRewardedRequest(message: error.localizedDescription)
            } else {
                print("[Ads][iOS] Rewarded interstitial load succeeded.")
                print("[Ads][iOS] Load success for rewarded_interstitial")
                AdDebugStatusStore.shared.updateLoad(format: "rewarded_interstitial", status: "loaded")
                self.drainPendingRewardedRequest()
            }
        }
    }

    private func configureServerSideVerification(for ad: RewardedAd?, adFormat: String) {
        guard let ad else { return }
        let options = ServerSideVerificationOptions()
#if canImport(FirebaseAuth)
        let uid = Auth.auth().currentUser?.uid.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
#else
        let uid = ""
#endif
        if !uid.isEmpty {
            options.userIdentifier = String(uid.prefix(120))
        }
        let payload = [
            "platform": "ios",
            "uid": uid,
            "adFormat": adFormat,
            "ts": Int(Date().timeIntervalSince1970 * 1000),
        ] as [String : Any]
        if let jsonData = try? JSONSerialization.data(withJSONObject: payload),
           let json = String(data: jsonData, encoding: .utf8) {
            options.customRewardText = String(json.prefix(450))
        }
        ad.serverSideVerificationOptions = options
    }

    private func configureServerSideVerification(for ad: RewardedInterstitialAd?, adFormat: String) {
        guard let ad else { return }
        let options = ServerSideVerificationOptions()
#if canImport(FirebaseAuth)
        let uid = Auth.auth().currentUser?.uid.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
#else
        let uid = ""
#endif
        if !uid.isEmpty {
            options.userIdentifier = String(uid.prefix(120))
        }
        let payload = [
            "platform": "ios",
            "uid": uid,
            "adFormat": adFormat,
            "ts": Int(Date().timeIntervalSince1970 * 1000),
        ] as [String : Any]
        if let jsonData = try? JSONSerialization.data(withJSONObject: payload),
           let json = String(data: jsonData, encoding: .utf8) {
            options.customRewardText = String(json.prefix(450))
        }
        ad.serverSideVerificationOptions = options
    }

    private func buildNativeFeedErrorPayload(slotId: String, placement: String, reason: String) -> [String: Any] {
        [
            "ok": false,
            "slotId": slotId,
            "placement": placement,
            "error": String(reason.prefix(220)),
        ]
    }

    private func serializeNativeAd(ad: NativeAd, adUnitId: String) -> [String: Any] {
        let iconDataUrl = imageDataUrl(from: ad.icon?.image)
        let mediaImage = ad.images?.first?.image
        let mediaDataUrl = imageDataUrl(from: mediaImage)
        return [
            "headline": ad.headline ?? "",
            "body": ad.body ?? "",
            "callToAction": ad.callToAction ?? "",
            "advertiser": ad.advertiser ?? "",
            "store": ad.store ?? "",
            "price": ad.price ?? "",
            "starRating": ad.starRating ?? 0,
            "iconDataUrl": iconDataUrl,
            "mediaDataUrl": mediaDataUrl,
            "hasVideoContent": ad.mediaContent.hasVideoContent,
            "adUnitId": adUnitId,
        ]
    }

    private func imageDataUrl(from image: UIImage?) -> String {
        guard let data = image?.pngData(), !data.isEmpty else { return "" }
        return "data:image/png;base64,\(data.base64EncodedString())"
    }

    private func logNativeAdEvent(
        name: String,
        placement: String,
        adUnitId: String,
        slotId: String,
        reason: String = ""
    ) {
#if canImport(FirebaseAnalytics)
        var params: [String: Any] = [
            AnalyticsParameterAdPlatform: "admob",
            AnalyticsParameterAdSource: "admob",
            AnalyticsParameterAdFormat: "native",
            AnalyticsParameterAdUnitName: adUnitId,
            "placement": String(placement.prefix(80)),
            "slot_id": String(slotId.prefix(80)),
        ]
        if !reason.isEmpty {
            params["reason"] = String(reason.prefix(120))
        }
        Analytics.logEvent(name, parameters: params)
#endif
    }

    func adDidRecordImpression(_ ad: FullScreenPresentingAd) {
        if ad === interstitialAd {
            AdImpressionReporter.shared.report(
                adFormat: "interstitial",
                adUnitId: remoteConfigManager.resolveAdUnitId(platform: .ios, format: .interstitial),
                placement: "navigation"
            )
            AdDebugStatusStore.shared.updateShow(format: "interstitial", status: "impression")
            return
        }
        if ad === rewardedAd {
            AdImpressionReporter.shared.report(
                adFormat: "rewarded",
                adUnitId: remoteConfigManager.resolveAdUnitId(platform: .ios, format: .rewarded),
                placement: "reward_action"
            )
            AdDebugStatusStore.shared.updateShow(format: "rewarded", status: "impression")
            return
        }
        if ad === rewardedInterstitialAd {
            AdImpressionReporter.shared.report(
                adFormat: "rewarded_interstitial",
                adUnitId: remoteConfigManager.resolveAdUnitId(platform: .ios, format: .rewardedInterstitial),
                placement: "reward_action"
            )
            AdDebugStatusStore.shared.updateShow(format: "rewarded_interstitial", status: "impression")
        }
    }

    func adWillPresentFullScreenContent(_ ad: FullScreenPresentingAd) {
        isShowingFullScreenAd = true
        if ad === interstitialAd {
            print("[Ads][iOS] Show success for interstitial")
            AdDebugStatusStore.shared.updateShow(format: "interstitial", status: "shown")
            if let context = interstitialCallbackContext {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "shown"
                )
            }
        } else if ad === rewardedAd {
            print("[Ads][iOS] Show success for rewarded")
            AdDebugStatusStore.shared.updateShow(format: "rewarded", status: "shown")
            if let context = rewardedCallbackContext {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "shown"
                )
            }
        } else if ad === rewardedInterstitialAd {
            print("[Ads][iOS] Show success for rewarded_interstitial")
            AdDebugStatusStore.shared.updateShow(format: "rewarded_interstitial", status: "shown")
            if let context = rewardedInterstitialCallbackContext {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "shown"
                )
            }
        }
    }

    func adDidDismissFullScreenContent(_ ad: FullScreenPresentingAd) {
        isShowingFullScreenAd = false
        if ad === interstitialAd {
            let context = interstitialCallbackContext
            interstitialCallbackContext = nil
            AdDebugStatusStore.shared.updateShow(format: "interstitial", status: "dismissed")
            if let context {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "dismissed"
                )
            }
            interstitialAd = nil
            loadInterstitial()
        } else if ad === rewardedAd {
            let context = rewardedCallbackContext
            rewardedCallbackContext = nil
            AdDebugStatusStore.shared.updateShow(format: "rewarded", status: "dismissed")
            if let context {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "dismissed"
                )
            }
            rewardedAd = nil
            loadRewarded()
        } else if ad === rewardedInterstitialAd {
            let context = rewardedInterstitialCallbackContext
            rewardedInterstitialCallbackContext = nil
            AdDebugStatusStore.shared.updateShow(format: "rewarded_interstitial", status: "dismissed")
            if let context {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "dismissed"
                )
            }
            rewardedInterstitialAd = nil
            loadRewardedInterstitial()
        }
    }

    func ad(
        _ ad: FullScreenPresentingAd,
        didFailToPresentFullScreenContentWithError error: Error
    ) {
        isShowingFullScreenAd = false
        print("[Ads][iOS] Fullscreen ad failed to present: \(error.localizedDescription)")
        if ad === interstitialAd {
            let context = interstitialCallbackContext
            interstitialCallbackContext = nil
            AdDebugStatusStore.shared.updateShow(format: "interstitial", status: "failed:\(error.localizedDescription)")
            if let context {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "failed",
                    message: error.localizedDescription
                )
            }
            interstitialAd = nil
            loadInterstitial()
        } else if ad === rewardedAd {
            let context = rewardedCallbackContext
            rewardedCallbackContext = nil
            AdDebugStatusStore.shared.updateShow(format: "rewarded", status: "failed:\(error.localizedDescription)")
            if let context {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "failed",
                    message: error.localizedDescription
                )
            }
            rewardedAd = nil
            loadRewarded()
        } else if ad === rewardedInterstitialAd {
            let context = rewardedInterstitialCallbackContext
            rewardedInterstitialCallbackContext = nil
            AdDebugStatusStore.shared.updateShow(format: "rewarded_interstitial", status: "failed:\(error.localizedDescription)")
            if let context {
                emitAdActionResult(
                    callback: context.callback,
                    requestId: context.requestId,
                    adFormat: context.adFormat,
                    status: "failed",
                    message: error.localizedDescription
                )
            }
            rewardedInterstitialAd = nil
            loadRewardedInterstitial()
        }
    }

    private func emitAdActionResult(
        callback: @escaping ([String: Any]) -> Void,
        requestId: String,
        adFormat: String,
        status: String,
        message: String = "",
        rewardType: String = "",
        rewardAmount: NSNumber? = nil
    ) {
        callback([
            "requestId": requestId,
            "adFormat": adFormat,
            "status": status,
            "message": message,
            "rewardType": rewardType,
            "rewardAmount": rewardAmount ?? NSNull(),
        ])
    }

    private func drainPendingInterstitialRequest() {
        guard let pending = pendingInterstitialRequest else { return }
        guard !isShowingFullScreenAd, interstitialAd != nil else { return }
        pendingInterstitialRequest = nil
        showInterstitial(
            from: pending.rootViewController,
            requestId: pending.requestId,
            callback: pending.callback
        )
    }

    private func failPendingInterstitialRequest(message: String) {
        guard let pending = pendingInterstitialRequest else { return }
        pendingInterstitialRequest = nil
        emitAdActionResult(
            callback: pending.callback,
            requestId: pending.requestId,
            adFormat: "interstitial",
            status: "failed",
            message: message
        )
    }

    private func drainPendingRewardedRequest() {
        guard let pending = pendingRewardedRequest else { return }
        guard !isShowingFullScreenAd else { return }
        let rewardedEnabled = remoteConfigManager.isAdFormatEnabled(format: .rewarded)
        let rewardedInterstitialEnabled = remoteConfigManager.isAdFormatEnabled(format: .rewardedInterstitial)
        let canShowRewardedInterstitial = pending.preferRewardedInterstitial &&
            rewardedInterstitialEnabled &&
            rewardedInterstitialAd != nil
        let canShowRewarded = rewardedEnabled && rewardedAd != nil
        let shouldFallbackToRewarded = canShowRewarded &&
            (!pending.preferRewardedInterstitial || !rewardedInterstitialEnabled || !rewardedInterstitialLoadInFlight)

        if canShowRewardedInterstitial {
            pendingRewardedRequest = nil
            showRewarded(
                from: pending.rootViewController,
                requestId: pending.requestId,
                preferRewardedInterstitial: true,
                callback: pending.callback
            )
        } else if shouldFallbackToRewarded {
            pendingRewardedRequest = nil
            showRewarded(
                from: pending.rootViewController,
                requestId: pending.requestId,
                preferRewardedInterstitial: false,
                callback: pending.callback
            )
        }
    }

    private func maybeFailPendingRewardedRequest(message: String) {
        guard let pending = pendingRewardedRequest else { return }
        if rewardedLoadInFlight || rewardedInterstitialLoadInFlight { return }
        if rewardedAd != nil || rewardedInterstitialAd != nil {
            drainPendingRewardedRequest()
            return
        }
        pendingRewardedRequest = nil
        emitAdActionResult(
            callback: pending.callback,
            requestId: pending.requestId,
            adFormat: pending.preferRewardedInterstitial ? "rewarded_interstitial" : "rewarded",
            status: "failed",
            message: message
        )
    }
}
#elseif canImport(UIKit)
final class AdManager {
    private let remoteConfigManager: RemoteConfigManager

    init(remoteConfigManager: RemoteConfigManager) {
        self.remoteConfigManager = remoteConfigManager
    }

    func primeAds() {
        _ = remoteConfigManager
    }

    func showInterstitial(
        from rootViewController: UIViewController?,
        requestId: String = "",
        callback: @escaping ([String: Any]) -> Void = { _ in }
    ) {
        _ = rootViewController
        _ = requestId
        _ = callback
    }

    func showRewarded(
        from rootViewController: UIViewController?,
        requestId: String = "",
        preferRewardedInterstitial: Bool = true,
        callback: @escaping ([String: Any]) -> Void = { _ in }
    ) {
        _ = rootViewController
        _ = requestId
        _ = preferRewardedInterstitial
        _ = callback
    }
}
#else
final class AdManager {
    private let remoteConfigManager: RemoteConfigManager

    init(remoteConfigManager: RemoteConfigManager) {
        self.remoteConfigManager = remoteConfigManager
    }

    func primeAds() {
        _ = remoteConfigManager
    }
}
#endif

final class WebViewLifecycleController: ObservableObject {
    weak var webView: WKWebView?

    func attach(_ webView: WKWebView) {
        self.webView = webView
    }

    // Pauses web media and emits a lifecycle event for JS listeners.
    func sceneDidEnterBackground() {
        webView?.evaluateJavaScript("""
            window.dispatchEvent(new Event('quantura:background'));
            document.querySelectorAll('video,audio').forEach(function(media) {
                try { media.pause(); } catch (_) {}
            });
        """)
    }

    func sceneWillEnterForeground() {
        webView?.evaluateJavaScript("window.dispatchEvent(new Event('quantura:foreground'));")
    }
}

#if canImport(UIKit)
struct QuanturaWebView: UIViewRepresentable {
    let url: URL
    let lifecycleController: WebViewLifecycleController
    let adManager: AdManager
    let remoteConfigManager: RemoteConfigManager
    @ObservedObject var authGateViewModel: AuthGateViewModel

    func makeCoordinator() -> Coordinator {
        Coordinator(
            lifecycleController: lifecycleController,
            adManager: adManager,
            remoteConfigManager: remoteConfigManager,
            authGateViewModel: authGateViewModel
        )
    }

    func makeUIView(context: Context) -> WKWebView {
        let userContentController = WKUserContentController()
        let nativeConsentScript = WKUserScript(
            source: """
            try {
              localStorage.setItem('quantura_cookie_consent', 'accepted');
              var banner = document.getElementById('cookie-banner');
              if (banner) { banner.classList.add('hidden'); }
            } catch (e) {}
            """,
            injectionTime: .atDocumentStart,
            forMainFrameOnly: false
        )
        userContentController.addUserScript(nativeConsentScript)
        userContentController.add(context.coordinator, name: "QuanturaBridge")
        userContentController.add(context.coordinator, name: "quanturaAuth")

        let configuration = WKWebViewConfiguration()
        configuration.userContentController = userContentController
        configuration.allowsInlineMediaPlayback = true
        configuration.defaultWebpagePreferences.allowsContentJavaScript = true
        configuration.applicationNameForUserAgent = "QuanturaiOSApp/1.0"

        let webView = WKWebView(frame: .zero, configuration: configuration)
        webView.navigationDelegate = context.coordinator
        webView.scrollView.keyboardDismissMode = .onDrag
        context.coordinator.webView = webView
        NativeAuthWebBridge.shared.attach(webView: webView)
        lifecycleController.attach(webView)
        context.coordinator.injectNativeRuntime()
        context.coordinator.injectPushTokenIfAvailable()
        webView.load(URLRequest(url: url))
        return webView
    }

    func updateUIView(_ uiView: WKWebView, context: Context) {
        if uiView.url != url {
            uiView.load(URLRequest(url: url))
        }
        context.coordinator.injectPushTokenIfAvailable()
    }

    static func dismantleUIView(_ uiView: WKWebView, coordinator: Coordinator) {
        uiView.configuration.userContentController.removeScriptMessageHandler(forName: "QuanturaBridge")
        uiView.configuration.userContentController.removeScriptMessageHandler(forName: "quanturaAuth")
        NativeAuthWebBridge.shared.detach(webView: uiView)
    }

    final class Coordinator: NSObject, WKScriptMessageHandler, WKNavigationDelegate {
        private let lifecycleController: WebViewLifecycleController
        private let adManager: AdManager
        private let remoteConfigManager: RemoteConfigManager
        private weak var authGateViewModel: AuthGateViewModel?
        private var tokenObserver: NSObjectProtocol?
        private var deepLinkObserver: NSObjectProtocol?
        private let trustedHosts: Set<String> = [
            "quantura.studio",
            "www.quantura.studio",
            "quantura-e2e3d.web.app",
            "quantura-e2e3d.firebaseapp.com",
            "localhost",
            "127.0.0.1",
        ]
#if canImport(AuthenticationServices)
        private struct AppleAuthContext {
            let requestId: String
            let rawNonce: String
        }
        private var pendingAppleAuthContext: AppleAuthContext?
#endif
        weak var webView: WKWebView?

        init(
            lifecycleController: WebViewLifecycleController,
            adManager: AdManager,
            remoteConfigManager: RemoteConfigManager,
            authGateViewModel: AuthGateViewModel
        ) {
            self.lifecycleController = lifecycleController
            self.adManager = adManager
            self.remoteConfigManager = remoteConfigManager
            self.authGateViewModel = authGateViewModel
            super.init()

            tokenObserver = NotificationCenter.default.addObserver(
                forName: .quanturaNativeFcmTokenUpdated,
                object: nil,
                queue: .main
            ) { [weak self] notification in
                guard let self, let token = notification.object as? String else { return }
                self.injectPushToken(token)
            }

            deepLinkObserver = NotificationCenter.default.addObserver(
                forName: .quanturaNativeDeepLinkUpdated,
                object: nil,
                queue: .main
            ) { [weak self] notification in
                guard let self, let target = notification.object as? String else { return }
                self.navigateToDeepLink(target)
            }
        }

        deinit {
            if let tokenObserver {
                NotificationCenter.default.removeObserver(tokenObserver)
            }
            if let deepLinkObserver {
                NotificationCenter.default.removeObserver(deepLinkObserver)
            }
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            NativeAuthWebBridge.shared.attach(webView: webView)
            injectNativeRuntime()
            injectPushTokenIfAvailable()
            NativeAuthWebBridge.shared.pushCurrentAuthState()
            NativeAuthWebBridge.shared.pushAuthGateState(
                visible: authGateViewModel?.isGateVisible == true,
                reason: "page_ready"
            )
#if canImport(FirebaseAuth)
            if let user = Auth.auth().currentUser, !user.isAnonymous {
                Task {
                    do {
                        try await NativeAuthWebBridge.shared.syncWebSessionFromCurrentUser(
                            forceRefresh: false,
                            source: "page_ready"
                        )
                    } catch {
                        print("[AuthBridge][iOS] Silent sync on page load failed: \(error.localizedDescription)")
                    }
                }
            }
#endif
            if !NativeBridgeState.shared.pendingDeepLink.isEmpty {
                navigateToDeepLink(NativeBridgeState.shared.pendingDeepLink)
            }
        }

        func webView(
            _ webView: WKWebView,
            decidePolicyFor navigationAction: WKNavigationAction,
            decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
        ) {
            if navigationAction.targetFrame?.isMainFrame == true,
               let destinationURL = navigationAction.request.url,
               isStripeCheckoutURL(destinationURL) {
                print("[Billing][iOS] Blocked Stripe checkout URL inside native app: \(destinationURL.absoluteString)")
                decisionHandler(.cancel)
                return
            }

            if navigationAction.targetFrame?.isMainFrame == true {
                let destinationURL = navigationAction.request.url
                if let destinationURL, !isTrustedURL(destinationURL) {
                    print("[WebView][iOS] Blocked untrusted main-frame navigation: \(destinationURL.absoluteString)")
                    DispatchQueue.main.async {
                        UIApplication.shared.open(destinationURL)
                    }
                    decisionHandler(.cancel)
                    return
                }
            }

            decisionHandler(.allow)
        }

        // Handles bridge messages from window.webkit.messageHandlers.QuanturaBridge.postMessage(...)
        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            if message.name == "quanturaAuth" {
                handleQuanturaAuthBridgeMessage(message.body)
                return
            }
            guard message.name == "QuanturaBridge" else { return }
            guard let sourceURL = webView?.url, isTrustedURL(sourceURL) else {
                print("[Bridge][iOS] Ignored QuanturaBridge message from untrusted page.")
                return
            }

            var payload: [String: Any] = [:]
            if let body = message.body as? [String: Any] {
                payload = body
            } else if let bodyText = message.body as? String,
                      let data = bodyText.data(using: .utf8),
                      let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                payload = json
            }

            guard let action = payload["action"] as? String else { return }
            DispatchQueue.main.async {
                switch action {
                case "showInterstitialAd":
                    let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    guard self.authGateViewModel?.isGateVisible != true else {
                        self.dispatchNativeAdResult([
                            "requestId": requestId,
                            "adFormat": "interstitial",
                            "status": "skipped:auth_gate",
                            "message": "Auth gate is visible.",
                        ])
                        return
                    }
                    self.adManager.showInterstitial(from: Self.topViewController(), requestId: requestId) { [weak self] detail in
                        self?.dispatchNativeAdResult(detail)
                    }
                case "showRewardedAd":
                    let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    guard self.authGateViewModel?.isGateVisible != true else {
                        self.dispatchNativeAdResult([
                            "requestId": requestId,
                            "adFormat": "rewarded",
                            "status": "skipped:auth_gate",
                            "message": "Auth gate is visible.",
                        ])
                        return
                    }
                    self.adManager.showRewarded(
                        from: Self.topViewController(),
                        requestId: requestId,
                        preferRewardedInterstitial: false
                    ) { [weak self] detail in
                        self?.dispatchNativeAdResult(detail)
                    }
                case "showRewardedInterstitial":
                    let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    guard self.authGateViewModel?.isGateVisible != true else {
                        self.dispatchNativeAdResult([
                            "requestId": requestId,
                            "adFormat": "rewarded_interstitial",
                            "status": "skipped:auth_gate",
                            "message": "Auth gate is visible.",
                        ])
                        return
                    }
                    self.adManager.showRewarded(
                        from: Self.topViewController(),
                        requestId: requestId,
                        preferRewardedInterstitial: true
                    ) { [weak self] detail in
                        self?.dispatchNativeAdResult(detail)
                    }
                case "openNewsLink":
                    guard let urlText = payload["url"] as? String, let url = URL(string: urlText) else { return }
                    let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    guard self.authGateViewModel?.isGateVisible != true else {
                        self.dispatchNativeAdResult([
                            "requestId": requestId,
                            "adFormat": "interstitial",
                            "status": "skipped:auth_gate",
                            "message": "Auth gate is visible.",
                        ])
                        return
                    }
                    var opened = false
                    self.adManager.showInterstitial(from: Self.topViewController(), requestId: requestId) { [weak self] detail in
                        self?.dispatchNativeAdResult(detail)
                        let status = String(describing: detail["status"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                        if status == "shown" || status == "queued" { return }
                        if !opened {
                            opened = true
                            UIApplication.shared.open(url)
                        }
                    }
                case "handleButtonClick":
                    let buttonID = String(describing: payload["buttonId"] ?? "")
                    print("[Ads][iOS] Button trigger rewarded buttonId=\(buttonID)")
                    let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    guard self.authGateViewModel?.isGateVisible != true else {
                        self.dispatchNativeAdResult([
                            "requestId": requestId,
                            "adFormat": "rewarded_interstitial",
                            "status": "skipped:auth_gate",
                            "message": "Auth gate is visible.",
                        ])
                        return
                    }
                    self.adManager.showRewarded(
                        from: Self.topViewController(),
                        requestId: requestId,
                        preferRewardedInterstitial: true
                    ) { [weak self] detail in
                        self?.dispatchNativeAdResult(detail)
                    }
                case "share":
                    self.openNativeShare(payload: payload)
                case "authSignIn":
                    self.authGateViewModel?.presentGate(trigger: "legacy_bridge_signin")
                case "authSignOut":
                    self.authGateViewModel?.signOutToAnonymous()
                    let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    if !requestId.isEmpty {
                        self.dispatchNativeAuthResult(requestId: requestId, provider: "native", ok: true)
                    }
                case "openAdInspector":
                    self.openAdInspectorIfAllowed()
                case "startNativePurchase":
                    self.handleNativeStoreKitPurchase(payload: payload)
                case "openNativeSubscriptionManager":
                    self.handleNativeSubscriptionManager(payload: payload)
                case "requestNativeFeedAd":
                    self.handleNativeFeedAdRequest(payload: payload)
                case "nativeFeedAdImpression":
                    self.handleNativeFeedAdImpression(payload: payload)
                case "nativeFeedAdClick":
                    self.handleNativeFeedAdClick(payload: payload)
                default:
                    break
                }
            }
        }

        private func handleQuanturaAuthBridgeMessage(_ body: Any) {
            guard let sourceURL = webView?.url, isTrustedURL(sourceURL) else {
                print("[AuthBridge][iOS] Ignored quanturaAuth message from untrusted page.")
                return
            }

            var payload: [String: Any] = [:]
            if let dictionary = body as? [String: Any] {
                payload = dictionary
            } else if let text = body as? String,
                      let data = text.data(using: .utf8),
                      let dictionary = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                payload = dictionary
            }

            let type = String(describing: payload["type"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
            guard !type.isEmpty else { return }

            DispatchQueue.main.async {
                switch type {
                case "REQUEST_SIGN_IN":
                    let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    let provider = String(describing: payload["provider"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                    print("[AuthBridge][iOS] Received REQUEST_SIGN_IN from web provider=\(provider) requestId=\(requestId)")
#if canImport(FirebaseAuth)
                    if let currentUser = Auth.auth().currentUser, !currentUser.isAnonymous {
                        Task {
                            do {
                                try await NativeAuthWebBridge.shared.syncWebSessionFromCurrentUser(
                                    forceRefresh: false,
                                    requestId: requestId,
                                    provider: provider,
                                    source: "web_request_restore"
                                )
                            } catch {
                                self.dispatchNativeAuthResult(
                                    requestId: requestId,
                                    provider: provider.isEmpty ? "native" : provider,
                                    ok: false,
                                    error: error.localizedDescription
                                )
                            }
                        }
                        NativeAuthWebBridge.shared.pushAuthGateState(visible: false, reason: "web_request_restore")
                        return
                    }
#endif
                    if !provider.isEmpty, !requestId.isEmpty {
                        self.handleNativeAuthSignIn(payload: payload)
                    } else {
                        self.authGateViewModel?.presentGate(trigger: "web_request")
                    }
                case "GET_AUTH_STATE":
                    print("[AuthBridge][iOS] Received GET_AUTH_STATE from web.")
                    NativeAuthWebBridge.shared.pushCurrentAuthState()
                    NativeAuthWebBridge.shared.pushAuthGateState(
                        visible: self.authGateViewModel?.isGateVisible == true,
                        reason: "get_auth_state"
                    )
                case "SIGN_OUT":
                    print("[AuthBridge][iOS] Received SIGN_OUT from web.")
                    self.handleNativeAuthSignOut(payload: payload)
                default:
                    print("[AuthBridge][iOS] Unknown bridge message type=\(type)")
                }
            }
        }

        private func openNativeShare(payload: [String: Any]) {
            guard let presenter = Self.topViewController() else { return }
            let url = String(describing: payload["url"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            guard !url.isEmpty else { return }
            let title = String(describing: payload["title"] ?? "Quantura")
            let text = String(describing: payload["text"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let finalText = text.isEmpty ? url : "\(text) \(url)"
            let activityVC = UIActivityViewController(activityItems: [title, finalText], applicationActivities: nil)
            presenter.present(activityVC, animated: true)
        }

        private func handleNativeFeedAdRequest(payload: [String: Any]) {
            let slotId = String(describing: payload["slotId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let placementRaw = String(describing: payload["placement"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let placement = placementRaw.isEmpty ? "feed" : placementRaw
            let variantRaw = String(describing: payload["variant"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let variant = variantRaw.isEmpty ? "nativeAdvanced" : variantRaw
            guard !slotId.isEmpty else {
                dispatchNativeFeedAdEvent([
                    "ok": false,
                    "slotId": "",
                    "placement": placement,
                    "error": "slot_id_missing",
                ])
                return
            }
            guard authGateViewModel?.isGateVisible != true else {
                dispatchNativeFeedAdEvent([
                    "ok": false,
                    "slotId": slotId,
                    "placement": placement,
                    "error": "auth_gate_visible",
                ])
                return
            }
            adManager.requestNativeFeedAd(slotId: slotId, placement: placement, variant: variant) { [weak self] detail in
                self?.dispatchNativeFeedAdEvent(detail)
            }
        }

        private func handleNativeFeedAdImpression(payload: [String: Any]) {
            let slotId = String(describing: payload["slotId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let placementRaw = String(describing: payload["placement"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let placement = placementRaw.isEmpty ? "feed" : placementRaw
            let adUnitId = String(describing: payload["adUnitId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            adManager.reportNativeFeedAdImpression(slotId: slotId, placement: placement, adUnitId: adUnitId)
        }

        private func handleNativeFeedAdClick(payload: [String: Any]) {
            let slotId = String(describing: payload["slotId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let placementRaw = String(describing: payload["placement"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let placement = placementRaw.isEmpty ? "feed" : placementRaw
            let adUnitId = String(describing: payload["adUnitId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            adManager.reportNativeFeedAdClick(slotId: slotId, placement: placement, adUnitId: adUnitId)
        }

        private func dispatchNativeFeedAdEvent(_ detailPayload: [String: Any]) {
            guard
                let data = try? JSONSerialization.data(withJSONObject: detailPayload, options: []),
                let detail = String(data: data, encoding: .utf8)
            else { return }
            webView?.evaluateJavaScript(
                "window.dispatchEvent(new CustomEvent('quantura:native-feed-ad',{detail:\(detail)}));"
            )
        }

        private func dispatchNativeAdResult(_ detailPayload: [String: Any]) {
            guard
                let data = try? JSONSerialization.data(withJSONObject: detailPayload, options: []),
                let detail = String(data: data, encoding: .utf8)
            else { return }
            webView?.evaluateJavaScript(
                "window.dispatchEvent(new CustomEvent('quantura:native-ad-result',{detail:\(detail)}));"
            )
        }

        private func openAdInspectorIfAllowed() {
#if canImport(GoogleMobileAds)
            guard remoteConfigManager.isAdInspectorEnabled() else {
                print("[Ads][iOS] Ad inspector launch skipped; RC flag disabled.")
                return
            }
            guard let presenter = Self.topViewController() else {
                print("[Ads][iOS] Ad inspector launch skipped; no presenter available.")
                return
            }
            MobileAds.shared.presentAdInspector(from: presenter) { error in
                if let error {
                    print("[Ads][iOS] Ad inspector closed with error: \(error.localizedDescription)")
                } else {
                    print("[Ads][iOS] Ad inspector closed.")
                }
            }
#endif
        }

        private func handleNativeStoreKitPurchase(payload: [String: Any]) {
            let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let orderId = String(describing: payload["orderId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let requestedProductId = String(describing: payload["productId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let productId = NativeIapCatalog.normalize(requestedProductId)
            guard !requestId.isEmpty else { return }

#if canImport(StoreKit)
            guard #available(iOS 15.0, *) else {
                dispatchNativePurchaseResult(
                    requestId: requestId,
                    orderId: orderId,
                    productId: productId,
                    ok: false,
                    status: "failed",
                    message: "StoreKit checkout requires iOS 15 or later."
                )
                return
            }

            Task { @MainActor in
                do {
                    let products = try await Product.products(for: [productId])
                    guard let product = products.first else {
                        dispatchNativePurchaseResult(
                            requestId: requestId,
                            orderId: orderId,
                            productId: productId,
                            ok: false,
                            status: "failed",
                            message: "App Store product was not found."
                        )
                        return
                    }

                    let result = try await product.purchase()
                    switch result {
                    case .success(let verification):
                        switch verification {
                        case .verified(let transaction):
                            await transaction.finish()
                            dispatchNativePurchaseResult(
                                requestId: requestId,
                                orderId: orderId,
                                productId: productId,
                                ok: true,
                                status: "purchased"
                            )
                        case .unverified(_, let verificationError):
                            dispatchNativePurchaseResult(
                                requestId: requestId,
                                orderId: orderId,
                                productId: productId,
                                ok: false,
                                status: "failed",
                                message: verificationError.localizedDescription
                            )
                        }
                    case .userCancelled:
                        dispatchNativePurchaseResult(
                            requestId: requestId,
                            orderId: orderId,
                            productId: productId,
                            ok: false,
                            status: "cancelled"
                        )
                    case .pending:
                        dispatchNativePurchaseResult(
                            requestId: requestId,
                            orderId: orderId,
                            productId: productId,
                            ok: false,
                            status: "pending"
                        )
                    @unknown default:
                        dispatchNativePurchaseResult(
                            requestId: requestId,
                            orderId: orderId,
                            productId: productId,
                            ok: false,
                            status: "failed",
                            message: "Unknown StoreKit purchase state."
                        )
                    }
                } catch {
                    dispatchNativePurchaseResult(
                        requestId: requestId,
                        orderId: orderId,
                        productId: productId,
                        ok: false,
                        status: "failed",
                        message: error.localizedDescription
                    )
                }
            }
#else
            dispatchNativePurchaseResult(
                requestId: requestId,
                orderId: orderId,
                productId: productId,
                ok: false,
                status: "failed",
                message: "StoreKit is unavailable in this build."
            )
#endif
        }

        private func handleNativeSubscriptionManager(payload: [String: Any]) {
            let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            guard !requestId.isEmpty else { return }

#if canImport(StoreKit)
            if #available(iOS 15.0, *) {
                Task { @MainActor in
                    do {
                        try await AppStore.sync()
                        dispatchNativePurchaseResult(
                            requestId: requestId,
                            orderId: "",
                            productId: "",
                            ok: true,
                            status: "restored",
                            message: "Purchases restored from the App Store."
                        )
                    } catch {
                        dispatchNativePurchaseResult(
                            requestId: requestId,
                            orderId: "",
                            productId: "",
                            ok: false,
                            status: "failed",
                            message: error.localizedDescription
                        )
                    }
                }
                return
            }
#endif

            guard let url = URL(string: "https://apps.apple.com/account/subscriptions") else {
                dispatchNativePurchaseResult(
                    requestId: requestId,
                    orderId: "",
                    productId: "",
                    ok: false,
                    status: "failed",
                    message: "Unable to open subscription settings."
                )
                return
            }
            UIApplication.shared.open(url) { opened in
                self.dispatchNativePurchaseResult(
                    requestId: requestId,
                    orderId: "",
                    productId: "",
                    ok: opened,
                    status: opened ? "subscriptions_opened" : "failed",
                    message: opened ? "" : "Unable to open subscription settings."
                )
            }
        }

        private func dispatchNativePurchaseResult(
            requestId: String,
            orderId: String,
            productId: String,
            ok: Bool,
            status: String,
            message: String = ""
        ) {
            guard
                let data = try? JSONSerialization.data(withJSONObject: [
                    "requestId": requestId,
                    "orderId": orderId,
                    "productId": productId,
                    "ok": ok,
                    "status": status,
                    "message": message,
                    "platform": "ios",
                ], options: []),
                let detail = String(data: data, encoding: .utf8)
            else { return }
            webView?.evaluateJavaScript(
                "window.dispatchEvent(new CustomEvent('quantura:native-purchase-result',{detail:\(detail)}));"
            )
        }

        private func handleNativeAuthSignIn(payload: [String: Any]) {
            let provider = String(describing: payload["provider"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            guard !provider.isEmpty, !requestId.isEmpty else { return }

            switch provider {
            case "google":
                handleNativeGoogleSignIn(requestId: requestId)
            case "apple":
                handleNativeAppleSignIn(requestId: requestId)
            case "email", "password":
                authGateViewModel?.presentGate(trigger: "web_request_email")
                authGateViewModel?.openEmailSheet()
            case "email_signup", "signup":
                authGateViewModel?.presentGate(trigger: "web_request_email_signup")
                authGateViewModel?.emailAuthMode = .signUp
                authGateViewModel?.emailAddress = ""
                authGateViewModel?.emailUsername = ""
                authGateViewModel?.emailPassword = ""
                authGateViewModel?.emailConfirmPassword = ""
                authGateViewModel?.isEmailSheetVisible = true
            case "github", "github.com":
                handleNativeOAuthSignIn(requestId: requestId, providerId: "github.com", providerLabel: "github")
            case "twitter", "x", "twitter.com":
                handleNativeOAuthSignIn(requestId: requestId, providerId: "twitter.com", providerLabel: "twitter")
            case "yahoo", "yahoo.com":
                handleNativeOAuthSignIn(requestId: requestId, providerId: "yahoo.com", providerLabel: "yahoo")
            case "microsoft", "microsoft.com":
                handleNativeOAuthSignIn(requestId: requestId, providerId: "microsoft.com", providerLabel: "microsoft")
            default:
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: provider,
                    ok: false,
                    error: "Native \(provider) sign-in is not configured in this build."
                )
            }
        }

#if canImport(FirebaseAuth) && canImport(GoogleSignIn)
        private func handleNativeGoogleSignIn(requestId: String) {
            guard FirebaseApp.app() != nil else {
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: "google",
                    ok: false,
                    error: "Firebase is not configured in this build."
                )
                return
            }

            guard let rootViewController = Self.topViewController() else {
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: "google",
                    ok: false,
                    error: "Unable to open native sign-in UI."
                )
                return
            }

            let clientID = FirebaseApp.app()?.options.clientID ?? ""
            guard !clientID.isEmpty else {
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: "google",
                    ok: false,
                    error: "Google client ID is missing in Firebase config."
                )
                return
            }

            print("[Auth][iOS] Starting Google Sign-In flow.")
            GIDSignIn.sharedInstance.configuration = GIDConfiguration(clientID: clientID)
            GIDSignIn.sharedInstance.signIn(withPresenting: rootViewController) { signInResult, signInError in
                if let signInError {
                    self.dispatchNativeAuthResult(
                        requestId: requestId,
                        provider: "google",
                        ok: false,
                        error: signInError.localizedDescription
                    )
                    return
                }

                guard
                    let user = signInResult?.user,
                    let idToken = user.idToken?.tokenString
                else {
                    self.dispatchNativeAuthResult(
                        requestId: requestId,
                        provider: "google",
                        ok: false,
                        error: "Google did not return an ID token."
                    )
                    return
                }

                let credential = GoogleAuthProvider.credential(
                    withIDToken: idToken,
                    accessToken: user.accessToken.tokenString
                )
                NativeAuthSessionManager.shared.signInOrLink(with: credential, provider: "google") { authResult, authError in
                    if let authError {
                        self.dispatchNativeAuthResult(
                            requestId: requestId,
                            provider: "google",
                            ok: false,
                            error: authError.localizedDescription
                        )
                        return
                    }
                    self.completeNativeAuthFromFirebase(
                        requestId: requestId,
                        provider: "google",
                        user: authResult?.user
                    )
                }
            }
        }
#else
        private func handleNativeGoogleSignIn(requestId: String) {
            dispatchNativeAuthResult(
                requestId: requestId,
                provider: "google",
                ok: false,
                error: "Google/Firebase Auth SDK is unavailable in this build."
            )
        }
#endif

#if canImport(FirebaseAuth)
        private func handleNativeOAuthSignIn(requestId: String, providerId: String, providerLabel: String) {
            guard FirebaseApp.app() != nil else {
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: providerLabel,
                    ok: false,
                    error: "Firebase is not configured in this build."
                )
                return
            }

            let provider = OAuthProvider(providerID: providerId)
            if providerId == "github.com" {
                provider.scopes = ["read:user", "user:email"]
            } else if providerId == "twitter.com" {
                provider.scopes = ["tweet.read", "users.read"]
            } else if providerId == "yahoo.com" {
                provider.scopes = ["profile", "email"]
            } else if providerId == "microsoft.com" {
                provider.scopes = ["user.read"]
            }

            if let currentUser = Auth.auth().currentUser, currentUser.isAnonymous {
                currentUser.link(with: provider, uiDelegate: nil) { authResult, authError in
                    if let authError {
                        self.dispatchNativeAuthResult(
                            requestId: requestId,
                            provider: providerLabel,
                            ok: false,
                            error: authError.localizedDescription
                        )
                        return
                    }
                    self.completeNativeAuthFromFirebase(
                        requestId: requestId,
                        provider: providerLabel,
                        user: authResult?.user
                    )
                }
                return
            }

            Auth.auth().signIn(with: provider, uiDelegate: nil) { authResult, authError in
                if let authError {
                    self.dispatchNativeAuthResult(
                        requestId: requestId,
                        provider: providerLabel,
                        ok: false,
                        error: authError.localizedDescription
                    )
                    return
                }
                self.completeNativeAuthFromFirebase(
                    requestId: requestId,
                    provider: providerLabel,
                    user: authResult?.user
                )
            }
        }
#else
        private func handleNativeOAuthSignIn(requestId: String, providerId: String, providerLabel: String) {
            _ = providerId
            dispatchNativeAuthResult(
                requestId: requestId,
                provider: providerLabel,
                ok: false,
                error: "OAuth provider sign-in is unavailable in this build."
            )
        }
#endif

#if canImport(FirebaseAuth) && canImport(AuthenticationServices) && canImport(CryptoKit)
        private func handleNativeAppleSignIn(requestId: String) {
            guard FirebaseApp.app() != nil else {
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: "apple",
                    ok: false,
                    error: "Firebase is not configured in this build."
                )
                return
            }

            let rawNonce = Self.randomNonceString()
            pendingAppleAuthContext = AppleAuthContext(requestId: requestId, rawNonce: rawNonce)

            let request = ASAuthorizationAppleIDProvider().createRequest()
            request.requestedScopes = [.fullName, .email]
            request.nonce = Self.sha256(rawNonce)
            let controller = ASAuthorizationController(authorizationRequests: [request])
            controller.delegate = self
            controller.presentationContextProvider = self
            print("[Auth][iOS] Starting Sign in with Apple flow.")
            controller.performRequests()
        }
#else
        private func handleNativeAppleSignIn(requestId: String) {
            dispatchNativeAuthResult(
                requestId: requestId,
                provider: "apple",
                ok: false,
                error: "Sign in with Apple is unavailable in this build."
            )
        }
#endif

        private func completeNativeAuthFromFirebase(
            requestId: String,
            provider: String,
            user: Any?
        ) {
#if canImport(FirebaseAuth)
            guard let firebaseUser = user as? FirebaseAuth.User else {
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: provider,
                    ok: false,
                    error: "Firebase user is unavailable after sign-in."
                )
                return
            }
            firebaseUser.getIDTokenForcingRefresh(true) { firebaseToken, firebaseTokenError in
                if let firebaseTokenError {
                    self.dispatchNativeAuthResult(
                        requestId: requestId,
                        provider: provider,
                        ok: false,
                        error: firebaseTokenError.localizedDescription
                    )
                    return
                }
                let token = firebaseToken?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                guard !token.isEmpty else {
                    self.dispatchNativeAuthResult(
                        requestId: requestId,
                        provider: provider,
                        ok: false,
                        error: "Native Firebase ID token is empty."
                    )
                    return
                }
                self.dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: provider,
                    ok: true,
                    idToken: token
                )
            }
#else
            _ = requestId
            _ = provider
            _ = user
#endif
        }

        private func handleNativeAuthSignOut(payload: [String: Any]) {
            let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
#if canImport(FirebaseAuth)
            do {
                try Auth.auth().signOut()
            } catch {
                // Ignore sign-out errors.
            }
#endif
#if canImport(GoogleSignIn)
            GIDSignIn.sharedInstance.signOut()
#endif
            guard !requestId.isEmpty else { return }
            dispatchNativeAuthResult(requestId: requestId, provider: "native", ok: true)
        }

#if canImport(FirebaseAuth) && canImport(AuthenticationServices) && canImport(CryptoKit)
        private static func randomNonceString(length: Int = 32) -> String {
            precondition(length > 0)
            let charset = Array("0123456789ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvwxyz-._")
            var result = ""
            var remaining = length

            while remaining > 0 {
                let random = Int.random(in: 0..<charset.count)
                result.append(charset[random])
                remaining -= 1
            }
            return result
        }

        private static func sha256(_ input: String) -> String {
            let digest = SHA256.hash(data: Data(input.utf8))
            return digest.map { String(format: "%02x", $0) }.joined()
        }
#endif

        private func dispatchNativeAuthResult(
            requestId: String,
            provider: String,
            ok: Bool,
            idToken: String = "",
            error: String = ""
        ) {
            guard
                let data = try? JSONSerialization.data(withJSONObject: [
                    "requestId": requestId,
                    "provider": provider,
                    "ok": ok,
                    "idToken": idToken,
                    "error": error,
                ], options: []),
                let detail = String(data: data, encoding: .utf8)
            else { return }
            webView?.evaluateJavaScript(
                "window.dispatchEvent(new CustomEvent('quantura:native-auth-result',{detail:\(detail)}));"
            )
        }

        func injectNativeRuntime() {
            let feedStart = remoteConfigManager.nativeFeedAdStart()
            let feedInterval = remoteConfigManager.nativeFeedAdInterval()
            let pageMidpoint = remoteConfigManager.nativePageAdMidpoint()
            webView?.evaluateJavaScript("""
                window.__QUANTURA_NATIVE_APP__ = true;
                window.__QUANTURA_NATIVE_PLATFORM__ = 'ios';
                window.__QUANTURA_NATIVE_AUTH_BRIDGE__ = true;
                window.__QUANTURA_NATIVE_AD_RULES__ = { feedStart: \(feedStart), feedInterval: \(feedInterval), pageMidpoint: \(pageMidpoint) };
                window.dispatchEvent(new CustomEvent('quantura:native-runtime-ready', { detail: { platform: 'ios', authBridge: true } }));
            """)
        }

        func injectPushTokenIfAvailable() {
            if !NativeBridgeState.shared.pushToken.isEmpty {
                injectPushToken(NativeBridgeState.shared.pushToken)
            }
        }

        private func injectPushToken(_ token: String) {
            let cleanToken = token.replacingOccurrences(of: "\\", with: "\\\\").replacingOccurrences(of: "'", with: "\\'")
            webView?.evaluateJavaScript(
                "window.__NATIVE_FCM_TOKEN__='\(cleanToken)';if(typeof window.__quanturaNativeTokenReady==='function')window.__quanturaNativeTokenReady('\(cleanToken)');"
            )
        }

        private func navigateToDeepLink(_ rawTarget: String) {
            let target = rawTarget.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !target.isEmpty else { return }
            let absolute: String
            if target.lowercased().hasPrefix("http://") || target.lowercased().hasPrefix("https://") {
                absolute = target
            } else {
                absolute = "\(quanturaURL.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/")))\(target.hasPrefix("/") ? target : "/\(target)")"
            }
            guard let url = URL(string: absolute) else { return }
            webView?.load(URLRequest(url: url))
            NativeBridgeState.shared.clearPendingDeepLink()
        }

        private func isTrustedURL(_ url: URL) -> Bool {
            guard let host = url.host?.lowercased() else { return false }
            if trustedHosts.contains(host) {
                return true
            }
            return host.hasSuffix(".quantura.studio")
        }

        private func isStripeCheckoutURL(_ url: URL) -> Bool {
            guard let host = url.host?.lowercased() else { return false }
            if host == "checkout.stripe.com" || host == "buy.stripe.com" || host.hasSuffix(".stripe.com") {
                return true
            }
            return false
        }

        static func topViewController(
            base: UIViewController? = UIApplication.shared.connectedScenes
                .compactMap { $0 as? UIWindowScene }
                .flatMap { $0.windows }
                .first { $0.isKeyWindow }?
                .rootViewController
        ) -> UIViewController? {
            if let nav = base as? UINavigationController {
                return topViewController(base: nav.visibleViewController)
            }
            if let tab = base as? UITabBarController, let selected = tab.selectedViewController {
                return topViewController(base: selected)
            }
            if let presented = base?.presentedViewController {
                return topViewController(base: presented)
            }
            return base
        }
    }
}

#if canImport(FirebaseAuth) && canImport(AuthenticationServices) && canImport(CryptoKit)
extension QuanturaWebView.Coordinator: ASAuthorizationControllerDelegate, ASAuthorizationControllerPresentationContextProviding {
    func authorizationController(
        controller: ASAuthorizationController,
        didCompleteWithAuthorization authorization: ASAuthorization
    ) {
        guard let context = pendingAppleAuthContext else { return }
        pendingAppleAuthContext = nil

        guard let credential = authorization.credential as? ASAuthorizationAppleIDCredential else {
            dispatchNativeAuthResult(
                requestId: context.requestId,
                provider: "apple",
                ok: false,
                error: "Apple credential was not returned."
            )
            return
        }

        guard let identityTokenData = credential.identityToken,
              let identityToken = String(data: identityTokenData, encoding: .utf8) else {
            dispatchNativeAuthResult(
                requestId: context.requestId,
                provider: "apple",
                ok: false,
                error: "Apple did not return an identity token."
            )
            return
        }

        let firebaseCredential = OAuthProvider.appleCredential(
            withIDToken: identityToken,
            rawNonce: context.rawNonce,
            fullName: credential.fullName
        )

        NativeAuthSessionManager.shared.signInOrLink(with: firebaseCredential, provider: "apple") { authResult, authError in
            if let authError {
                self.dispatchNativeAuthResult(
                    requestId: context.requestId,
                    provider: "apple",
                    ok: false,
                    error: authError.localizedDescription
                )
                return
            }
            self.completeNativeAuthFromFirebase(
                requestId: context.requestId,
                provider: "apple",
                user: authResult?.user
            )
        }
    }

    func authorizationController(controller: ASAuthorizationController, didCompleteWithError error: Error) {
        let requestId = pendingAppleAuthContext?.requestId ?? ""
        pendingAppleAuthContext = nil
        guard !requestId.isEmpty else { return }
        let nsError = error as NSError
        let message: String
        if nsError.domain == ASAuthorizationError.errorDomain, nsError.code == ASAuthorizationError.unknown.rawValue {
            message = "Sign in with Apple failed (AuthorizationError 1000). Verify Apple Sign-In capability, bundle ID configuration, and try again."
        } else {
            message = error.localizedDescription
        }
        dispatchNativeAuthResult(
            requestId: requestId,
            provider: "apple",
            ok: false,
            error: message
        )
    }

    func presentationAnchor(for controller: ASAuthorizationController) -> ASPresentationAnchor {
        if let top = QuanturaWebView.Coordinator.topViewController(), let window = top.view.window {
            return window
        }
        let fallbackWindow = UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .flatMap { $0.windows }
            .first { $0.isKeyWindow }
        return fallbackWindow ?? ASPresentationAnchor()
    }
}
#endif
#else
struct QuanturaWebView: View {
    let url: URL
    let lifecycleController: WebViewLifecycleController
    let adManager: AdManager
    @ObservedObject var authGateViewModel: AuthGateViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Quantura iOS shell")
                .font(.headline)
            Text("This WebView bridge target is available on UIKit platforms.")
                .font(.subheadline)
            Text(url.absoluteString)
                .font(.footnote)
                .foregroundStyle(.secondary)
        }
        .padding()
    }
}
#endif

private struct AdsQaFormatRow: Identifiable {
    let id = UUID()
    let format: String
    let adUnitId: String
    let loadStatus: String
    let showStatus: String
}

private struct AdsQaSnapshot {
    let remoteConfigFetched: Bool
    let remoteConfigFetchedAt: Date?
    let adsEnabled: Bool
    let adsEnabledTopLevel: Bool
    let featureFlagAdsEnabled: Bool
    let usingRealAds: Bool
    let usingTestAds: Bool
    let isDebugBuild: Bool
    let isSimulator: Bool
    let rows: [AdsQaFormatRow]

    static let empty = AdsQaSnapshot(
        remoteConfigFetched: false,
        remoteConfigFetchedAt: nil,
        adsEnabled: false,
        adsEnabledTopLevel: false,
        featureFlagAdsEnabled: false,
        usingRealAds: false,
        usingTestAds: true,
        isDebugBuild: false,
        isSimulator: false,
        rows: []
    )
}

struct ContentView: View {
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var lifecycleController = WebViewLifecycleController()
    @StateObject private var authGateViewModel = AuthGateViewModel()
    @State private var bannerAdsVisible: Bool = true
    @State private var adsQaPanelVisible: Bool = false
    @State private var adsQaSnapshot: AdsQaSnapshot = .empty
#if canImport(StoreKit)
    @StateObject private var storeKitManager = StoreKitIapManager()
#endif
    private let container = AppContainer()

    var body: some View {
        ZStack {
            QuanturaWebView(
                url: quanturaURL,
                lifecycleController: lifecycleController,
                adManager: container.adManager,
                remoteConfigManager: container.remoteConfigManager,
                authGateViewModel: authGateViewModel
            )
            .ignoresSafeArea(edges: [.top, .leading, .trailing])

            if authGateViewModel.isGateVisible {
                AuthGateView(viewModel: authGateViewModel)
                    .transition(.opacity)
                    .zIndex(999)
            }

            if container.remoteConfigManager.isAdInspectorEnabled() &&
                (
                    container.remoteConfigManager.adsEnvironment().isDebugBuild ||
                        container.remoteConfigManager.adsEnvironment().isSimulatorOrEmulator
                ) {
                VStack {
                    Spacer()
                    HStack {
                        Spacer()
                        Button("Ads QA") {
                            refreshAdsQaSnapshot()
                            adsQaPanelVisible = true
                        }
                        .buttonStyle(.bordered)
                        .padding(.trailing, 14)
                        .padding(.bottom, 90)
                    }
                }
                .zIndex(1000)
            }
        }
#if canImport(GoogleMobileAds) && canImport(UIKit)
        .safeAreaInset(edge: .bottom, spacing: 0) {
            if bannerAdsVisible {
                AdaptiveBannerContainer(
                    adUnitID: container.remoteConfigManager.resolveAdUnitId(platform: .ios, format: .banner)
                )
                    .frame(height: 60)
                    .background(.ultraThinMaterial)
            }
        }
#endif
        .sheet(isPresented: $adsQaPanelVisible) {
            AdsQaPanelView(
                snapshot: adsQaSnapshot,
                onRefresh: { refreshAdsQaSnapshot() },
                onLoadAll: { testAllAdFormatsFromQaPanel() }
            )
        }
        .onAppear {
            authGateViewModel.start()
            container.appOpenAdManager.setPresentationBlockedByAuthGate(authGateViewModel.isGateVisible)
            bannerAdsVisible = container.remoteConfigManager.isAdFormatEnabled(format: .banner)
            container.adManager.primeAds()
            container.appOpenAdManager.preloadAdIfNeeded()
            refreshAdsQaSnapshot()
            container.remoteConfigManager.fetchAndActivate { _ in
                let enabled = container.remoteConfigManager.isAdFormatEnabled(format: .banner)
                DispatchQueue.main.async {
                    bannerAdsVisible = enabled
                }
                container.adManager.primeAds()
                container.appOpenAdManager.preloadAdIfNeeded()
                refreshAdsQaSnapshot()
            }
#if canImport(StoreKit)
            if #available(iOS 15.0, *) {
                Task {
                    await storeKitManager.fetchProducts(NativeIapCatalog.iosProductIds)
                }
            }
#endif
        }
        .onChange(of: authGateViewModel.isGateVisible) { isVisible in
            container.appOpenAdManager.setPresentationBlockedByAuthGate(isVisible)
        }
        .onChange(of: scenePhase) { nextPhase in
            switch nextPhase {
            case .background:
                lifecycleController.sceneDidEnterBackground()
            case .active:
                lifecycleController.sceneWillEnterForeground()
                container.appOpenAdManager.sceneDidBecomeActive()
                refreshAdsQaSnapshot()
            default:
                break
            }
        }
    }

    private func refreshAdsQaSnapshot() {
        let config = container.remoteConfigManager.debugStatus()
        let statusByFormat = Dictionary(
            uniqueKeysWithValues: container.adManager.debugStatusSnapshots().map { ($0.format.lowercased(), $0) }
        )
        let rows: [AdsQaFormatRow] = [
            ("app_open", container.remoteConfigManager.resolveAdUnitId(platform: .ios, format: .appOpen)),
            ("banner", container.remoteConfigManager.resolveAdUnitId(platform: .ios, format: .banner)),
            ("interstitial", container.remoteConfigManager.resolveAdUnitId(platform: .ios, format: .interstitial)),
            ("rewarded", container.remoteConfigManager.resolveAdUnitId(platform: .ios, format: .rewarded)),
            ("rewarded_interstitial", container.remoteConfigManager.resolveAdUnitId(platform: .ios, format: .rewardedInterstitial)),
            ("native", container.remoteConfigManager.resolveAdUnitId(platform: .ios, format: .native)),
        ].map { key, adUnitId in
            let status = statusByFormat[key]
            return AdsQaFormatRow(
                format: key,
                adUnitId: adUnitId,
                loadStatus: status?.lastLoadStatus ?? "idle",
                showStatus: status?.lastShowStatus ?? "idle"
            )
        }

        adsQaSnapshot = AdsQaSnapshot(
            remoteConfigFetched: config.remoteConfigFetched,
            remoteConfigFetchedAt: config.remoteConfigFetchedAt,
            adsEnabled: config.adsEnabled,
            adsEnabledTopLevel: config.adsEnabledTopLevel,
            featureFlagAdsEnabled: config.featureFlags.adsEnabled,
            usingRealAds: config.usingRealAds,
            usingTestAds: config.usingTestAds,
            isDebugBuild: config.environment.isDebugBuild,
            isSimulator: config.environment.isSimulatorOrEmulator,
            rows: rows
        )
    }

    private func testAllAdFormatsFromQaPanel() {
        container.adManager.preloadAllFormatsForQa()
        container.appOpenAdManager.preloadAdIfNeeded()
        Task {
            try? await Task.sleep(nanoseconds: 700_000_000)
            await MainActor.run {
                refreshAdsQaSnapshot()
            }
        }
    }
}

private struct AdsQaPanelView: View {
    let snapshot: AdsQaSnapshot
    let onRefresh: () -> Void
    let onLoadAll: () -> Void

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(alignment: .leading, spacing: 10) {
                    Text(
                        "RC fetched=\(snapshot.remoteConfigFetched) " +
                            "adsEnabled=\(snapshot.adsEnabled) topLevel=\(snapshot.adsEnabledTopLevel) " +
                            "featureFlag=\(snapshot.featureFlagAdsEnabled)"
                    )
                    .font(.caption)

                    Text(
                        "usingRealAds=\(snapshot.usingRealAds) usingTestAds=\(snapshot.usingTestAds) " +
                            "debug=\(snapshot.isDebugBuild) simulator=\(snapshot.isSimulator)"
                    )
                    .font(.caption)

                    ForEach(snapshot.rows) { row in
                        VStack(alignment: .leading, spacing: 4) {
                            Text(row.format.replacingOccurrences(of: "_", with: " ").capitalized)
                                .font(.subheadline.weight(.semibold))
                            Text(row.adUnitId)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Text("load=\(row.loadStatus) · show=\(row.showStatus)")
                                .font(.caption2)
                        }
                        .padding(10)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
                    }
                }
                .padding(16)
            }
            .navigationTitle("Ads QA")
            .toolbar {
                ToolbarItemGroup(placement: .navigationBarTrailing) {
                    Button("Refresh", action: onRefresh)
                    Button("Load all ads", action: onLoadAll)
                }
            }
        }
    }
}

private extension String {
    var isGoogleSampleAdUnit: Bool {
        hasPrefix("ca-app-pub-3940256099942544/")
    }

    func ifEmpty(replacement: String) -> String {
        isEmpty ? replacement : self
    }
}

#Preview {
    ContentView()
}
