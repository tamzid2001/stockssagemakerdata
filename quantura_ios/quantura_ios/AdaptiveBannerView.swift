import SwiftUI
#if canImport(GoogleMobileAds) && canImport(UIKit)
import GoogleMobileAds
import UIKit

struct AdaptiveBannerContainer: View {
    let adUnitID: String
    @State private var bannerHeight: CGFloat = 0

    var body: some View {
        GeometryReader { proxy in
            let bannerWidth = max(proxy.size.width, 320)
            Color.clear
                .overlay {
                    BannerRepresentable(
                        adUnitID: adUnitID,
                        width: bannerWidth,
                        onHeightChanged: { nextHeight in
                            guard abs(bannerHeight - nextHeight) >= 0.5 else { return }
                            bannerHeight = nextHeight
                        }
                    )
                    .frame(height: max(bannerHeight, 1))
                }
        }
        .frame(height: max(bannerHeight, 1))
    }
}

private func topBannerAdSize(for width: CGFloat) -> AdSize {
    currentOrientationAnchoredAdaptiveBanner(width: max(width, 320))
}

private func topBannerHeight(for width: CGFloat) -> CGFloat {
    max(topBannerAdSize(for: width).size.height, 0)
}

private struct BannerRepresentable: UIViewRepresentable {
    let adUnitID: String
    let width: CGFloat
    let onHeightChanged: (CGFloat) -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(onHeightChanged: onHeightChanged)
    }

    func makeUIView(context: Context) -> BannerHostView {
        let hostView = BannerHostView()
        context.coordinator.attach(to: hostView)
        context.coordinator.update(adUnitID: adUnitID, width: width)
        return hostView
    }

    func updateUIView(_ uiView: BannerHostView, context: Context) {
        context.coordinator.attach(to: uiView)
        context.coordinator.update(adUnitID: adUnitID, width: width)
    }

    static func dismantleUIView(_ uiView: BannerHostView, coordinator: Coordinator) {
        coordinator.dismantle()
    }

    private static func topViewController(
        base: UIViewController? = UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .sorted { lhs, rhs in
                let lhsActive = lhs.activationState == .foregroundActive
                let rhsActive = rhs.activationState == .foregroundActive
                if lhsActive == rhsActive { return false }
                return lhsActive && !rhsActive
            }
            .flatMap { $0.windows }
            .first { $0.isKeyWindow || (!$0.isHidden && $0.windowLevel == .normal) }?
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

    final class BannerHostView: UIView {
        override init(frame: CGRect) {
            super.init(frame: frame)
            backgroundColor = .clear
        }

        required init?(coder: NSCoder) {
            super.init(coder: coder)
            backgroundColor = .clear
        }
    }

    final class Coordinator: NSObject, BannerViewDelegate {
        private weak var hostView: BannerHostView?
        private var bannerView: BannerView?
        private var currentAdUnitID: String = ""
        private var currentWidth: CGFloat = 0
        private var lastLoadSignature: String = ""
        private var isLoading = false
        private var retryWorkItem: DispatchWorkItem?
        private var waitingForSdkStart = false
        private let onHeightChanged: (CGFloat) -> Void

        init(onHeightChanged: @escaping (CGFloat) -> Void) {
            self.onHeightChanged = onHeightChanged
        }

        func attach(to hostView: BannerHostView) {
            self.hostView = hostView
            guard let bannerView else { return }
            if bannerView.superview !== hostView {
                hostView.subviews.forEach { $0.removeFromSuperview() }
                hostView.addSubview(bannerView)
                NSLayoutConstraint.activate([
                    bannerView.leadingAnchor.constraint(equalTo: hostView.leadingAnchor),
                    bannerView.trailingAnchor.constraint(equalTo: hostView.trailingAnchor),
                    bannerView.topAnchor.constraint(equalTo: hostView.topAnchor),
                    bannerView.bottomAnchor.constraint(equalTo: hostView.bottomAnchor),
                ])
            }
        }

        func update(adUnitID: String, width: CGFloat) {
            let resolvedWidth = max(width, 320)
            let rootViewController = BannerRepresentable.topViewController()
            let rootToken = rootViewController.map { ObjectIdentifier($0).hashValue } ?? 0
            let signature = "\(adUnitID)|\(Int(resolvedWidth.rounded()))|\(rootToken)"

            let bannerView = ensureBannerView(width: resolvedWidth)
            bannerView.rootViewController = rootViewController

            if currentAdUnitID != adUnitID {
                currentAdUnitID = adUnitID
                bannerView.adUnitID = adUnitID
                lastLoadSignature = ""
            }

            if rootViewController == nil {
                AdDebugStatusStore.shared.updateLoad(format: "banner", status: "waiting:presenter")
                scheduleRetry()
                return
            }

            if signature != lastLoadSignature {
                loadBanner(signature: signature)
            }
        }

        func dismantle() {
            retryWorkItem?.cancel()
            retryWorkItem = nil
            bannerView?.delegate = nil
            bannerView?.rootViewController = nil
            bannerView?.removeFromSuperview()
            bannerView = nil
            hostView = nil
            isLoading = false
            lastLoadSignature = ""
            currentAdUnitID = ""
            currentWidth = 0
            waitingForSdkStart = false
        }

        private func ensureBannerView(width: CGFloat) -> BannerView {
            if let bannerView {
                if abs(currentWidth - width) >= 1 {
                    currentWidth = width
                    bannerView.adSize = topBannerAdSize(for: width)
                    lastLoadSignature = ""
                }
                return bannerView
            }

            currentWidth = width
            let bannerView = BannerView(adSize: topBannerAdSize(for: width))
            bannerView.translatesAutoresizingMaskIntoConstraints = false
            bannerView.delegate = self
            self.bannerView = bannerView
            attach(to: hostView ?? BannerHostView())
            return bannerView
        }

        private func loadBanner(signature: String) {
            guard let bannerView else { return }
            retryWorkItem?.cancel()
            retryWorkItem = nil
            if isLoading { return }
            if !MobileAdsBootstrap.shared.isReady {
                AdDebugStatusStore.shared.updateLoad(format: "banner", status: "waiting:sdk_init")
                guard !waitingForSdkStart else { return }
                waitingForSdkStart = true
                MobileAdsBootstrap.shared.whenReady { [weak self] success in
                    guard let self else { return }
                    self.waitingForSdkStart = false
                    guard success else { return }
                    self.isLoading = false
                    self.lastLoadSignature = ""
                    self.update(adUnitID: self.currentAdUnitID, width: max(self.currentWidth, 320))
                }
                return
            }
            isLoading = true
            lastLoadSignature = signature
            print("[Ads][iOS][Banner] Loading ad unit=\(currentAdUnitID) width=\(Int(currentWidth.rounded()))")
            AdDebugStatusStore.shared.updateLoad(format: "banner", status: "loading")
            bannerView.load(Request())
        }

        private func scheduleRetry() {
            retryWorkItem?.cancel()
            let workItem = DispatchWorkItem { [weak self] in
                guard let self else { return }
                self.isLoading = false
                self.lastLoadSignature = ""
                self.update(adUnitID: self.currentAdUnitID, width: self.currentWidth)
            }
            retryWorkItem = workItem
            DispatchQueue.main.asyncAfter(deadline: .now() + 1.5, execute: workItem)
        }

        func bannerViewDidReceiveAd(_ bannerView: BannerView) {
            isLoading = false
            retryWorkItem?.cancel()
            retryWorkItem = nil
            print("[Ads][iOS][Banner] Load succeeded.")
            print("[Ads][iOS] Load success for banner")
            AdDebugStatusStore.shared.updateLoad(format: "banner", status: "loaded")
            reportHeight(max(CGFloat(cgSize(for: bannerView.adSize).height), 50))
        }

        func bannerView(_ bannerView: BannerView, didFailToReceiveAdWithError error: Error) {
            isLoading = false
            print("[Ads][iOS][Banner] Load failed: \(error.localizedDescription)")
            print("[Ads][iOS] Load fail for banner: \(error.localizedDescription)")
            AdDebugStatusStore.shared.updateLoad(format: "banner", status: "failed:\(error.localizedDescription)")
            reportHeight(0)
            scheduleRetry()
        }

        func bannerViewDidRecordImpression(_ bannerView: BannerView) {
            print("[Ads][iOS][Banner] Impression recorded.")
            AdDebugStatusStore.shared.updateShow(format: "banner", status: "impression")
            AdImpressionReporter.shared.report(
                adFormat: "banner",
                adUnitId: bannerView.adUnitID ?? "",
                placement: "top_banner"
            )
        }

        private func reportHeight(_ height: CGFloat) {
            DispatchQueue.main.async {
                self.onHeightChanged(max(height, 0))
            }
        }
    }
}
#else
struct AdaptiveBannerContainer: View {
    let adUnitID: String

    var body: some View {
        let _ = adUnitID
        EmptyView()
    }
}
#endif
