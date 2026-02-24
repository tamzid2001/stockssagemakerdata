import SwiftUI
#if canImport(GoogleMobileAds) && canImport(UIKit)
import GoogleMobileAds
import UIKit

struct AdaptiveBannerContainer: View {
    let adUnitID: String

    var body: some View {
        GeometryReader { proxy in
            BannerRepresentable(
                adUnitID: adUnitID,
                width: max(proxy.size.width, 320)
            )
        }
        .frame(height: 60)
    }
}

private struct BannerRepresentable: UIViewRepresentable {
    let adUnitID: String
    let width: CGFloat

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeUIView(context: Context) -> BannerView {
        let adSize = currentOrientationAnchoredAdaptiveBanner(width: width)
        let bannerView = BannerView(adSize: adSize)
        bannerView.adUnitID = adUnitID
        bannerView.rootViewController = Self.topViewController()
        bannerView.delegate = context.coordinator
        print("[Ads][iOS][Banner] Loading ad unit=\(adUnitID)")
        bannerView.load(Request())
        return bannerView
    }

    func updateUIView(_ uiView: BannerView, context: Context) {
        uiView.rootViewController = Self.topViewController()
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

    final class Coordinator: NSObject, BannerViewDelegate {
        func bannerViewDidReceiveAd(_ bannerView: BannerView) {
            print("[Ads][iOS][Banner] Load succeeded.")
        }

        func bannerView(_ bannerView: BannerView, didFailToReceiveAdWithError error: Error) {
            print("[Ads][iOS][Banner] Load failed: \(error.localizedDescription)")
        }

        func bannerViewDidRecordImpression(_ bannerView: BannerView) {
            print("[Ads][iOS][Banner] Impression recorded.")
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
