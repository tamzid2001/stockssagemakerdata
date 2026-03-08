import Foundation
#if canImport(GoogleMobileAds) && canImport(UIKit)
import GoogleMobileAds
import UIKit

final class AppOpenAdManager: NSObject, FullScreenContentDelegate {
    private let remoteConfigManager: RemoteConfigManager
    private let adManager: AdManager
    private var appOpenAd: AppOpenAd?
    private var isLoading = false
    private var isShowing = false
    private var presentationBlockedByAuthGate = false
    private var loadedAt: Date?
    private let maxCacheAgeSeconds: TimeInterval = 4 * 60 * 60

    init(remoteConfigManager: RemoteConfigManager, adManager: AdManager) {
        self.remoteConfigManager = remoteConfigManager
        self.adManager = adManager
    }

    func preloadAdIfNeeded() {
        guard remoteConfigManager.areAdsEnabled() else {
            print("[Ads][iOS][AppOpen] Ads disabled by feature flag.")
            AdDebugStatusStore.shared.updateLoad(format: "app_open", status: "disabled")
            return
        }
        guard remoteConfigManager.isAdFormatEnabled(format: .appOpen) else {
            print("[Ads][iOS][AppOpen] App open disabled by format flag.")
            AdDebugStatusStore.shared.updateLoad(format: "app_open", status: "disabled:format_off")
            appOpenAd = nil
            loadedAt = nil
            return
        }
        guard !isLoading else { return }
        guard !isAdFresh else { return }
        loadAd()
    }

    func sceneDidBecomeActive() {
        showIfAvailable()
    }

    func setPresentationBlockedByAuthGate(_ blocked: Bool) {
        presentationBlockedByAuthGate = blocked
        if blocked {
            print("[Ads][iOS][AppOpen] Presentation blocked by auth gate.")
        }
    }

    private var isAdFresh: Bool {
        guard appOpenAd != nil, let loadedAt else { return false }
        return Date().timeIntervalSince(loadedAt) < maxCacheAgeSeconds
    }

    private func loadAd() {
        let unitID = remoteConfigManager.resolveAdUnitId(platform: .ios, format: .appOpen)
        isLoading = true
        print("[Ads][iOS][AppOpen] Loading ad unit=\(unitID)")
        AdDebugStatusStore.shared.updateLoad(format: "app_open", status: "loading")
        AppOpenAd.load(with: unitID, request: Request()) { [weak self] ad, error in
            guard let self else { return }
            self.isLoading = false
            if let error {
                self.appOpenAd = nil
                self.loadedAt = nil
                print("[Ads][iOS][AppOpen] Load failed: \(error.localizedDescription)")
                print("[Ads][iOS] Load fail for app_open: \(error.localizedDescription)")
                AdDebugStatusStore.shared.updateLoad(format: "app_open", status: "failed:\(error.localizedDescription)")
                return
            }
            self.appOpenAd = ad
            self.loadedAt = Date()
            self.appOpenAd?.fullScreenContentDelegate = self
            print("[Ads][iOS][AppOpen] Load succeeded.")
            print("[Ads][iOS] Load success for app_open")
            AdDebugStatusStore.shared.updateLoad(format: "app_open", status: "loaded")
        }
    }

    private func showIfAvailable() {
        guard remoteConfigManager.areAdsEnabled() else { return }
        guard remoteConfigManager.isAdFormatEnabled(format: .appOpen) else {
            AdDebugStatusStore.shared.updateShow(format: "app_open", status: "skipped:format_off")
            return
        }
        guard !isShowing else { return }
        guard !presentationBlockedByAuthGate else {
            print("[Ads][iOS][AppOpen] Skipping show; auth gate is visible.")
            AdDebugStatusStore.shared.updateShow(format: "app_open", status: "skipped:auth_gate")
            return
        }
        guard !adManager.isShowingFullScreenAd else {
            print("[Ads][iOS][AppOpen] Skipping show; another fullscreen ad is already visible.")
            AdDebugStatusStore.shared.updateShow(format: "app_open", status: "skipped:fullscreen_visible")
            return
        }

        guard isAdFresh, let ad = appOpenAd else {
            print("[Ads][iOS][AppOpen] No fresh ad available; preloading.")
            AdDebugStatusStore.shared.updateShow(format: "app_open", status: "skipped:not_ready")
            preloadAdIfNeeded()
            return
        }

        guard let presenter = Self.topViewController() else {
            print("[Ads][iOS][AppOpen] No presenter view controller available.")
            return
        }
        guard !Self.hasPresentedModal() else {
            print("[Ads][iOS][AppOpen] Skipping show because another full-screen/modal UI is already presented.")
            return
        }

        isShowing = true
        ad.fullScreenContentDelegate = self
        print("[Ads][iOS][AppOpen] Presenting app open ad.")
        ad.present(from: presenter)
    }

    func adDidRecordImpression(_ ad: FullScreenPresentingAd) {
        print("[Ads][iOS][AppOpen] Impression recorded.")
        AdImpressionReporter.shared.report(
            adFormat: "app_open",
            adUnitId: remoteConfigManager.resolveAdUnitId(platform: .ios, format: .appOpen),
            placement: "app_open"
        )
        AdDebugStatusStore.shared.updateShow(format: "app_open", status: "impression")
    }

    func adWillPresentFullScreenContent(_ ad: FullScreenPresentingAd) {
        print("[Ads][iOS][AppOpen] Will present.")
        print("[Ads][iOS] Show success for app_open")
        AdDebugStatusStore.shared.updateShow(format: "app_open", status: "shown")
    }

    func adDidDismissFullScreenContent(_ ad: FullScreenPresentingAd) {
        print("[Ads][iOS][AppOpen] Dismissed.")
        isShowing = false
        appOpenAd = nil
        loadedAt = nil
        AdDebugStatusStore.shared.updateShow(format: "app_open", status: "dismissed")
        preloadAdIfNeeded()
    }

    func ad(
        _ ad: FullScreenPresentingAd,
        didFailToPresentFullScreenContentWithError error: Error
    ) {
        print("[Ads][iOS][AppOpen] Failed to show: \(error.localizedDescription)")
        isShowing = false
        appOpenAd = nil
        loadedAt = nil
        AdDebugStatusStore.shared.updateShow(format: "app_open", status: "failed:\(error.localizedDescription)")
        preloadAdIfNeeded()
    }

    private static func topViewController(
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

    private static func hasPresentedModal() -> Bool {
        let root = UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .flatMap { $0.windows }
            .first { $0.isKeyWindow }?
            .rootViewController
        return root?.presentedViewController != nil
    }
}
#elseif canImport(UIKit)
final class AppOpenAdManager {
    init(remoteConfigManager: RemoteConfigManager, adManager: AdManager) {
        _ = remoteConfigManager
        _ = adManager
    }

    func preloadAdIfNeeded() {}

    func sceneDidBecomeActive() {}

    func setPresentationBlockedByAuthGate(_ blocked: Bool) {
        _ = blocked
    }
}
#else
final class AppOpenAdManager {
    init(remoteConfigManager: RemoteConfigManager, adManager: AdManager) {
        _ = remoteConfigManager
        _ = adManager
    }

    func preloadAdIfNeeded() {}

    func sceneDidBecomeActive() {}

    func setPresentationBlockedByAuthGate(_ blocked: Bool) {
        _ = blocked
    }
}
#endif
