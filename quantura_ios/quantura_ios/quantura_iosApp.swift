import FirebaseCore
import SwiftUI
#if canImport(UIKit)
import UIKit
#endif
#if canImport(GoogleMobileAds)
import GoogleMobileAds
#endif

#if canImport(UIKit)
final class AppDelegate: NSObject, UIApplicationDelegate {
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        FirebaseApp.configure()
#if canImport(GoogleMobileAds)
        GADMobileAds.sharedInstance().start(completionHandler: nil)
#endif
        return true
    }
}
#endif

@main
struct quantura_iosApp: App {
#if canImport(UIKit)
    @UIApplicationDelegateAdaptor(AppDelegate.self) var delegate
#else
    init() {
        FirebaseApp.configure()
    }
#endif

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
