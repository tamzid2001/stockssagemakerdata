import FirebaseCore
#if canImport(FirebaseAuth)
import FirebaseAuth
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

final class RemoteConfigManager {
    private let remoteConfig: RemoteConfig?

    struct AdUnitIDs {
        let appOpen: String
        let adaptiveBanner: String
        let fixedBanner: String
        let interstitial: String
        let rewarded: String
        let rewardedInterstitial: String
        let nativeAdvanced: String
        let nativeVideo: String
    }

    private let demoIDs = AdUnitIDs(
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

    init() {
        guard FirebaseApp.app() != nil else {
            remoteConfig = nil
            return
        }
        let rc = RemoteConfig.remoteConfig()
        let settings = RemoteConfigSettings()
        settings.minimumFetchInterval = _isDebugAssertConfiguration() ? 0 : 3600
        rc.configSettings = settings
        rc.setDefaults([
            "ads_use_real_ios": true as NSObject,
            "ads_use_real_android": true as NSObject,
            "feature_flags": """
            {"native_bridge_enabled":true,"ads_enabled":true}
            """ as NSObject,
        ])
        remoteConfig = rc
    }

    func fetchAndActivate(completion: ((Bool) -> Void)? = nil) {
        guard let remoteConfig else {
            completion?(false)
            return
        }
        remoteConfig.fetchAndActivate { status, _ in
            completion?(status == .successFetchedFromRemote || status == .successUsingPreFetchedData)
        }
    }

    func adUnitIDs() -> AdUnitIDs {
        let isDebugBuild = _isDebugAssertConfiguration()
        let useRealIOSAds = !isDebugBuild && (remoteConfig?.configValue(forKey: "ads_use_real_ios").boolValue ?? true)
        let seed = useRealIOSAds ? liveIOSIDs : demoIDs

        guard
            let raw = remoteConfig?["ad_unit_ids"].stringValue,
            !raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
            let data = raw.data(using: .utf8),
            let json = (try? JSONSerialization.jsonObject(with: data)) as? [String: String]
        else {
            return seed
        }

        return AdUnitIDs(
            appOpen: json["appOpen"] ?? seed.appOpen,
            adaptiveBanner: json["adaptiveBanner"] ?? seed.adaptiveBanner,
            fixedBanner: json["fixedBanner"] ?? seed.fixedBanner,
            interstitial: json["interstitial"] ?? seed.interstitial,
            rewarded: json["rewarded"] ?? seed.rewarded,
            rewardedInterstitial: json["rewardedInterstitial"] ?? seed.rewardedInterstitial,
            nativeAdvanced: json["nativeAdvanced"] ?? seed.nativeAdvanced,
            nativeVideo: json["nativeVideo"] ?? seed.nativeVideo
        )
    }

    func featureFlag(_ key: String, default defaultValue: Bool = false) -> Bool {
        guard let remoteConfig else {
            return defaultValue
        }
        guard
            let data = remoteConfig["feature_flags"].stringValue.data(using: .utf8),
            let json = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        else {
            return defaultValue
        }
        return (json[key] as? Bool) ?? defaultValue
    }
}

#if canImport(GoogleMobileAds) && canImport(UIKit)
final class AdManager: NSObject, FullScreenContentDelegate {
    private let remoteConfigManager: RemoteConfigManager
    private var interstitialAd: InterstitialAd?
    private var rewardedAd: RewardedAd?
    private(set) var isShowingFullScreenAd = false

    init(remoteConfigManager: RemoteConfigManager) {
        self.remoteConfigManager = remoteConfigManager
    }

    func primeAds() {
        guard remoteConfigManager.featureFlag("ads_enabled", default: true) else { return }
        print("[Ads][iOS] Priming interstitial and rewarded ads.")
        loadInterstitial()
        loadRewarded()
    }

    func showInterstitial(from rootViewController: UIViewController?) {
        DispatchQueue.main.async {
            guard self.remoteConfigManager.featureFlag("ads_enabled", default: true) else { return }
            guard let rootViewController else { return }
            guard !self.isShowingFullScreenAd else {
                print("[Ads][iOS] Interstitial show skipped; another fullscreen ad is visible.")
                return
            }
            guard let ad = self.interstitialAd else {
                print("[Ads][iOS] Interstitial unavailable; reloading.")
                self.loadInterstitial()
                return
            }
            ad.fullScreenContentDelegate = self
            print("[Ads][iOS] Presenting interstitial.")
            ad.present(from: rootViewController)
        }
    }

    func showRewarded(from rootViewController: UIViewController?) {
        DispatchQueue.main.async {
            guard self.remoteConfigManager.featureFlag("ads_enabled", default: true) else { return }
            guard let rootViewController else { return }
            guard !self.isShowingFullScreenAd else {
                print("[Ads][iOS] Rewarded show skipped; another fullscreen ad is visible.")
                return
            }
            guard let ad = self.rewardedAd else {
                print("[Ads][iOS] Rewarded unavailable; reloading.")
                self.loadRewarded()
                return
            }
            ad.fullScreenContentDelegate = self
            print("[Ads][iOS] Presenting rewarded.")
            ad.present(from: rootViewController) {
                _ = ad.adReward
            }
        }
    }

    private func loadInterstitial() {
        let adUnitID = remoteConfigManager.adUnitIDs().interstitial
        print("[Ads][iOS] Loading interstitial unit=\(adUnitID)")
        InterstitialAd.load(with: adUnitID, request: Request()) { [weak self] ad, error in
            guard let self else { return }
            self.interstitialAd = ad
            self.interstitialAd?.fullScreenContentDelegate = self
            if let error {
                print("[Ads][iOS] Interstitial load failed: \(error.localizedDescription)")
            } else {
                print("[Ads][iOS] Interstitial load succeeded.")
            }
        }
    }

    private func loadRewarded() {
        let adUnitID = remoteConfigManager.adUnitIDs().rewarded
        print("[Ads][iOS] Loading rewarded unit=\(adUnitID)")
        RewardedAd.load(with: adUnitID, request: Request()) { [weak self] ad, error in
            guard let self else { return }
            self.rewardedAd = ad
            self.rewardedAd?.fullScreenContentDelegate = self
            if let error {
                print("[Ads][iOS] Rewarded load failed: \(error.localizedDescription)")
            } else {
                print("[Ads][iOS] Rewarded load succeeded.")
            }
        }
    }

    func adWillPresentFullScreenContent(_ ad: FullScreenPresentingAd) {
        isShowingFullScreenAd = true
    }

    func adDidDismissFullScreenContent(_ ad: FullScreenPresentingAd) {
        isShowingFullScreenAd = false
        if ad === interstitialAd {
            interstitialAd = nil
            loadInterstitial()
        } else if ad === rewardedAd {
            rewardedAd = nil
            loadRewarded()
        }
    }

    func ad(
        _ ad: FullScreenPresentingAd,
        didFailToPresentFullScreenContentWithError error: Error
    ) {
        isShowingFullScreenAd = false
        print("[Ads][iOS] Fullscreen ad failed to present: \(error.localizedDescription)")
        if ad === interstitialAd {
            interstitialAd = nil
            loadInterstitial()
        } else if ad === rewardedAd {
            rewardedAd = nil
            loadRewarded()
        }
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

    func showInterstitial(from rootViewController: UIViewController?) {
        _ = rootViewController
    }

    func showRewarded(from rootViewController: UIViewController?) {
        _ = rootViewController
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
    @ObservedObject var authGateViewModel: AuthGateViewModel

    func makeCoordinator() -> Coordinator {
        Coordinator(
            lifecycleController: lifecycleController,
            adManager: adManager,
            authGateViewModel: authGateViewModel
        )
    }

    func makeUIView(context: Context) -> WKWebView {
        let userContentController = WKUserContentController()
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
        private weak var authGateViewModel: AuthGateViewModel?
        private var tokenObserver: NSObjectProtocol?
        private var deepLinkObserver: NSObjectProtocol?
        private var lastNavigationInterstitialAt: Date = .distantPast
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
            authGateViewModel: AuthGateViewModel
        ) {
            self.lifecycleController = lifecycleController
            self.adManager = adManager
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
#if canImport(FirebaseAuth)
            if let user = Auth.auth().currentUser, !user.isAnonymous {
                Task {
                    do {
                        try await NativeAuthWebBridge.shared.syncWebSessionFromCurrentUser(forceRefresh: false)
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

            if navigationAction.targetFrame?.isMainFrame == true,
               let destination = navigationAction.request.url?.absoluteString,
               !destination.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                let now = Date()
                if now.timeIntervalSince(lastNavigationInterstitialAt) > 3 {
                    lastNavigationInterstitialAt = now
                    print("[Ads][iOS] Navigation trigger interstitial url=\(destination)")
                    adManager.showInterstitial(from: Self.topViewController())
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
                    self.adManager.showInterstitial(from: Self.topViewController())
                case "showRewardedAd":
                    self.adManager.showRewarded(from: Self.topViewController())
                case "openNewsLink":
                    guard let urlText = payload["url"] as? String, let url = URL(string: urlText) else { return }
                    self.adManager.showInterstitial(from: Self.topViewController())
                    UIApplication.shared.open(url)
                case "handleButtonClick":
                    let buttonID = String(describing: payload["buttonId"] ?? "")
                    print("[Ads][iOS] Button trigger rewarded buttonId=\(buttonID)")
                    self.adManager.showRewarded(from: Self.topViewController())
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
                case "startNativePurchase":
                    self.handleNativeStoreKitPurchase(payload: payload)
                case "openNativeSubscriptionManager":
                    self.handleNativeSubscriptionManager(payload: payload)
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
                    print("[AuthBridge][iOS] Received REQUEST_SIGN_IN from web.")
                    self.authGateViewModel?.presentGate(trigger: "web_request")
                case "GET_AUTH_STATE":
                    print("[AuthBridge][iOS] Received GET_AUTH_STATE from web.")
                    NativeAuthWebBridge.shared.pushCurrentAuthState()
                case "SIGN_OUT":
                    print("[AuthBridge][iOS] Received SIGN_OUT from web.")
                    self.authGateViewModel?.signOutToAnonymous()
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

        private func handleNativeStoreKitPurchase(payload: [String: Any]) {
            let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let orderId = String(describing: payload["orderId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let productId = String(describing: payload["productId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            guard !requestId.isEmpty else { return }
            guard !productId.isEmpty else {
                dispatchNativePurchaseResult(
                    requestId: requestId,
                    orderId: orderId,
                    productId: "",
                    ok: false,
                    status: "failed",
                    message: "Missing App Store product identifier."
                )
                return
            }

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
                        if let scene = UIApplication.shared.connectedScenes
                            .compactMap({ $0 as? UIWindowScene })
                            .first(where: { $0.activationState == .foregroundActive }) {
                            try await AppStore.showManageSubscriptions(in: scene)
                            dispatchNativePurchaseResult(
                                requestId: requestId,
                                orderId: "",
                                productId: "",
                                ok: true,
                                status: "subscriptions_opened"
                            )
                            return
                        }
                        throw NSError(
                            domain: "QuanturaStoreKit",
                            code: -1001,
                            userInfo: [NSLocalizedDescriptionKey: "No active scene available."]
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
            dispatchNativeAuthResult(requestId: requestId, provider: "google", ok: true)
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
            webView?.evaluateJavaScript("""
                window.__QUANTURA_NATIVE_APP__ = true;
                window.__QUANTURA_NATIVE_PLATFORM__ = 'ios';
                window.__QUANTURA_NATIVE_AUTH_BRIDGE__ = true;
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
        dispatchNativeAuthResult(
            requestId: requestId,
            provider: "apple",
            ok: false,
            error: error.localizedDescription
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

struct ContentView: View {
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var lifecycleController = WebViewLifecycleController()
    @StateObject private var authGateViewModel = AuthGateViewModel()
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
                authGateViewModel: authGateViewModel
            )
            .ignoresSafeArea(edges: [.top, .leading, .trailing])

            if authGateViewModel.isGateVisible {
                AuthGateView(viewModel: authGateViewModel)
                    .transition(.opacity)
                    .zIndex(999)
            }
        }
#if canImport(GoogleMobileAds) && canImport(UIKit)
        .safeAreaInset(edge: .bottom, spacing: 0) {
            AdaptiveBannerContainer(adUnitID: container.remoteConfigManager.adUnitIDs().adaptiveBanner)
                .frame(height: 60)
                .background(.ultraThinMaterial)
        }
#endif
        .onAppear {
            authGateViewModel.start()
            container.appOpenAdManager.setPresentationBlockedByAuthGate(authGateViewModel.isGateVisible)
            container.adManager.primeAds()
            container.appOpenAdManager.preloadAdIfNeeded()
            container.remoteConfigManager.fetchAndActivate { _ in
                container.adManager.primeAds()
                container.appOpenAdManager.preloadAdIfNeeded()
            }
#if canImport(StoreKit)
            if #available(iOS 15.0, *) {
                Task {
                    await storeKitManager.fetchProducts(["quantura_pro_monthly"])
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
            default:
                break
            }
        }
    }
}

#Preview {
    ContentView()
}
