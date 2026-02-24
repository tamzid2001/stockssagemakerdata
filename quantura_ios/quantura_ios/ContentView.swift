import FirebaseCore
#if canImport(FirebaseAuth)
import FirebaseAuth
#endif
import FirebaseRemoteConfig
#if canImport(GoogleSignIn)
import GoogleSignIn
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

private let quanturaURL = URL(string: "https://quantura-e2e3d.web.app/")!

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
        appOpen: "ca-app-pub-3940256099942544/9257395921",
        adaptiveBanner: "ca-app-pub-3940256099942544/9214589741",
        fixedBanner: "ca-app-pub-3940256099942544/6300978111",
        interstitial: "ca-app-pub-3940256099942544/1033173712",
        rewarded: "ca-app-pub-3940256099942544/5224354917",
        rewardedInterstitial: "ca-app-pub-3940256099942544/5354046379",
        nativeAdvanced: "ca-app-pub-3940256099942544/2247696110",
        nativeVideo: "ca-app-pub-3940256099942544/1044960115"
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
            "ads_use_real_ios": false as NSObject,
            "ads_use_real_android": false as NSObject,
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
        let useRealIOSAds = remoteConfig?.configValue(forKey: "ads_use_real_ios").boolValue ?? false
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

    init(remoteConfigManager: RemoteConfigManager) {
        self.remoteConfigManager = remoteConfigManager
    }

    func primeAds() {
        guard remoteConfigManager.featureFlag("ads_enabled", default: true) else { return }
        loadInterstitial()
        loadRewarded()
    }

    func showInterstitial(from rootViewController: UIViewController?) {
        DispatchQueue.main.async {
            guard self.remoteConfigManager.featureFlag("ads_enabled", default: true) else { return }
            guard let rootViewController else { return }
            guard let ad = self.interstitialAd else {
                self.loadInterstitial()
                return
            }
            ad.fullScreenContentDelegate = self
            ad.present(from: rootViewController)
        }
    }

    func showRewarded(from rootViewController: UIViewController?) {
        DispatchQueue.main.async {
            guard self.remoteConfigManager.featureFlag("ads_enabled", default: true) else { return }
            guard let rootViewController else { return }
            guard let ad = self.rewardedAd else {
                self.loadRewarded()
                return
            }
            ad.fullScreenContentDelegate = self
            ad.present(from: rootViewController) {
                _ = ad.adReward
            }
        }
    }

    private func loadInterstitial() {
        let adUnitID = remoteConfigManager.adUnitIDs().interstitial
        InterstitialAd.load(with: adUnitID, request: Request()) { [weak self] ad, _ in
            guard let self else { return }
            self.interstitialAd = ad
            self.interstitialAd?.fullScreenContentDelegate = self
        }
    }

    private func loadRewarded() {
        let adUnitID = remoteConfigManager.adUnitIDs().rewarded
        RewardedAd.load(with: adUnitID, request: Request()) { [weak self] ad, _ in
            guard let self else { return }
            self.rewardedAd = ad
            self.rewardedAd?.fullScreenContentDelegate = self
        }
    }

    func adDidDismissFullScreenContent(_ ad: FullScreenPresentingAd) {
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

    func makeCoordinator() -> Coordinator {
        Coordinator(lifecycleController: lifecycleController, adManager: adManager)
    }

    func makeUIView(context: Context) -> WKWebView {
        let userContentController = WKUserContentController()
        userContentController.add(context.coordinator, name: "QuanturaBridge")

        let configuration = WKWebViewConfiguration()
        configuration.userContentController = userContentController
        configuration.allowsInlineMediaPlayback = true
        configuration.defaultWebpagePreferences.allowsContentJavaScript = true
        configuration.applicationNameForUserAgent = "QuanturaiOSApp/1.0"

        let webView = WKWebView(frame: .zero, configuration: configuration)
        webView.navigationDelegate = context.coordinator
        webView.scrollView.keyboardDismissMode = .onDrag
        context.coordinator.webView = webView
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
    }

    final class Coordinator: NSObject, WKScriptMessageHandler, WKNavigationDelegate {
        private let lifecycleController: WebViewLifecycleController
        private let adManager: AdManager
        private var tokenObserver: NSObjectProtocol?
        private var deepLinkObserver: NSObjectProtocol?
        weak var webView: WKWebView?

        init(lifecycleController: WebViewLifecycleController, adManager: AdManager) {
            self.lifecycleController = lifecycleController
            self.adManager = adManager
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
            injectNativeRuntime()
            injectPushTokenIfAvailable()
            if !NativeBridgeState.shared.pendingDeepLink.isEmpty {
                navigateToDeepLink(NativeBridgeState.shared.pendingDeepLink)
            }
        }

        // Handles bridge messages from window.webkit.messageHandlers.QuanturaBridge.postMessage(...)
        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            guard message.name == "QuanturaBridge" else { return }

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
                    UIApplication.shared.open(url)
                case "handleButtonClick":
                    let buttonID = String(describing: payload["buttonId"] ?? "")
                    print("QuanturaBridge button click: \(buttonID)")
                case "share":
                    self.openNativeShare(payload: payload)
                case "authSignIn":
                    self.handleNativeAuthSignIn(payload: payload)
                case "authSignOut":
                    self.handleNativeAuthSignOut(payload: payload)
                default:
                    break
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

        private func handleNativeAuthSignIn(payload: [String: Any]) {
            let provider = String(describing: payload["provider"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            let requestId = String(describing: payload["requestId"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            guard !provider.isEmpty, !requestId.isEmpty else { return }

            guard provider == "google" else {
                dispatchNativeAuthResult(
                    requestId: requestId,
                    provider: provider,
                    ok: false,
                    error: "Native \(provider) sign-in is not configured in this build."
                )
                return
            }

#if canImport(FirebaseAuth) && canImport(GoogleSignIn)
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

                Auth.auth().signIn(with: credential) { authResult, authError in
                    if let authError {
                        self.dispatchNativeAuthResult(
                            requestId: requestId,
                            provider: "google",
                            ok: false,
                            error: authError.localizedDescription
                        )
                        return
                    }
                    authResult?.user.getIDTokenForcingRefresh(true) { firebaseToken, firebaseTokenError in
                        if let firebaseTokenError {
                            self.dispatchNativeAuthResult(
                                requestId: requestId,
                                provider: "google",
                                ok: false,
                                error: firebaseTokenError.localizedDescription
                            )
                            return
                        }
                        let token = firebaseToken?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                        guard !token.isEmpty else {
                            self.dispatchNativeAuthResult(
                                requestId: requestId,
                                provider: "google",
                                ok: false,
                                error: "Native Firebase ID token is empty."
                            )
                            return
                        }
                        self.dispatchNativeAuthResult(
                            requestId: requestId,
                            provider: "google",
                            ok: true,
                            idToken: token
                        )
                    }
                }
            }
#else
            dispatchNativeAuthResult(
                requestId: requestId,
                provider: "google",
                ok: false,
                error: "Google/Firebase Auth SDK is unavailable in this build."
            )
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
                window.dispatchEvent(new CustomEvent('quantura:native-runtime-ready', { detail: { platform: 'ios' } }));
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
    }
}
#else
struct QuanturaWebView: View {
    let url: URL
    let lifecycleController: WebViewLifecycleController
    let adManager: AdManager

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
#if canImport(StoreKit)
    @StateObject private var storeKitManager = StoreKitIapManager()
#endif
    private let container = AppContainer()

    var body: some View {
        QuanturaWebView(url: quanturaURL, lifecycleController: lifecycleController, adManager: container.adManager)
            .ignoresSafeArea()
            .onAppear {
                container.remoteConfigManager.fetchAndActivate { _ in
                    container.adManager.primeAds()
                }
#if canImport(StoreKit)
                if #available(iOS 15.0, *) {
                    Task {
                        await storeKitManager.fetchProducts(["quantura_pro_monthly"])
                    }
                }
#endif
            }
            .onChange(of: scenePhase) { nextPhase in
                switch nextPhase {
                case .background:
                    lifecycleController.sceneDidEnterBackground()
                case .active:
                    lifecycleController.sceneWillEnterForeground()
                default:
                    break
                }
            }
    }
}

#Preview {
    ContentView()
}
