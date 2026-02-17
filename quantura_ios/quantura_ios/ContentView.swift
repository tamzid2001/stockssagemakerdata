import FirebaseCore
import FirebaseRemoteConfig
import SwiftUI
import WebKit
import Combine
#if canImport(UIKit)
import UIKit
#endif
#if canImport(GoogleMobileAds) && canImport(UIKit)
import GoogleMobileAds
#endif

private let quanturaURL = URL(string: "https://quantura-e2e3d.web.app/")!

// Lightweight DI container to keep view wiring explicit and testable.
final class AppContainer {
    let remoteConfigManager = RemoteConfigManager()
    lazy var adManager = AdManager(remoteConfigManager: remoteConfigManager)
}

final class RemoteConfigManager {
    private let remoteConfig: RemoteConfig?

    struct AdUnitIDs {
        let interstitial: String
        let rewarded: String
    }

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
            "ad_unit_ids": """
            {"interstitial":"ca-app-pub-3940256099942544/4411468910","rewarded":"ca-app-pub-3940256099942544/1712485313"}
            """ as NSObject,
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
        guard let remoteConfig else {
            return AdUnitIDs(
                interstitial: "ca-app-pub-3940256099942544/4411468910",
                rewarded: "ca-app-pub-3940256099942544/1712485313"
            )
        }
        guard
            let data = remoteConfig["ad_unit_ids"].stringValue.data(using: .utf8),
            let json = (try? JSONSerialization.jsonObject(with: data)) as? [String: String]
        else {
            return AdUnitIDs(
                interstitial: "ca-app-pub-3940256099942544/4411468910",
                rewarded: "ca-app-pub-3940256099942544/1712485313"
            )
        }

        return AdUnitIDs(
            interstitial: json["interstitial"] ?? "ca-app-pub-3940256099942544/4411468910",
            rewarded: json["rewarded"] ?? "ca-app-pub-3940256099942544/1712485313"
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
final class AdManager: NSObject, GADFullScreenContentDelegate {
    private let remoteConfigManager: RemoteConfigManager
    private var interstitialAd: GADInterstitialAd?
    private var rewardedAd: GADRewardedAd?

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
            ad.present(fromRootViewController: rootViewController)
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
            ad.present(fromRootViewController: rootViewController) {
                _ = ad.adReward
            }
        }
    }

    private func loadInterstitial() {
        let adUnitID = remoteConfigManager.adUnitIDs().interstitial
        GADInterstitialAd.load(withAdUnitID: adUnitID, request: GADRequest()) { [weak self] ad, _ in
            guard let self else { return }
            self.interstitialAd = ad
            self.interstitialAd?.fullScreenContentDelegate = self
        }
    }

    private func loadRewarded() {
        let adUnitID = remoteConfigManager.adUnitIDs().rewarded
        GADRewardedAd.load(withAdUnitID: adUnitID, request: GADRequest()) { [weak self] ad, _ in
            guard let self else { return }
            self.rewardedAd = ad
            self.rewardedAd?.fullScreenContentDelegate = self
        }
    }

    func adDidDismissFullScreenContent(_ ad: GADFullScreenPresentingAd) {
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

        let webView = WKWebView(frame: .zero, configuration: configuration)
        webView.navigationDelegate = context.coordinator
        webView.scrollView.keyboardDismissMode = .onDrag
        context.coordinator.webView = webView
        lifecycleController.attach(webView)
        webView.load(URLRequest(url: url))
        return webView
    }

    func updateUIView(_ uiView: WKWebView, context: Context) {
        if uiView.url != url {
            uiView.load(URLRequest(url: url))
        }
    }

    static func dismantleUIView(_ uiView: WKWebView, coordinator: Coordinator) {
        uiView.configuration.userContentController.removeScriptMessageHandler(forName: "QuanturaBridge")
    }

    final class Coordinator: NSObject, WKScriptMessageHandler, WKNavigationDelegate {
        private let lifecycleController: WebViewLifecycleController
        private let adManager: AdManager
        weak var webView: WKWebView?

        init(lifecycleController: WebViewLifecycleController, adManager: AdManager) {
            self.lifecycleController = lifecycleController
            self.adManager = adManager
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
                default:
                    break
                }
            }
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
    private let container = AppContainer()

    var body: some View {
        QuanturaWebView(url: quanturaURL, lifecycleController: lifecycleController, adManager: container.adManager)
            .ignoresSafeArea()
            .onAppear {
                container.remoteConfigManager.fetchAndActivate { _ in
                    container.adManager.primeAds()
                }
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
