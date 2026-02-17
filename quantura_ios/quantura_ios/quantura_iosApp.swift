import FirebaseCore
import SwiftUI
#if canImport(UIKit)
import UIKit
#endif
#if canImport(GoogleMobileAds)
import GoogleMobileAds
#endif

enum FirebaseBootstrap {
    static func configureIfAvailable() -> Bool {
        if FirebaseApp.app() != nil {
            return true
        }
        guard let plistPath = Bundle.main.path(forResource: "GoogleService-Info", ofType: "plist") else {
            print("Firebase disabled: missing GoogleService-Info.plist (local-only file).")
            return false
        }
        guard let options = FirebaseOptions(contentsOfFile: plistPath) else {
            print("Firebase disabled: invalid GoogleService-Info.plist.")
            return false
        }
        FirebaseApp.configure(options: options)
        return FirebaseApp.app() != nil
    }
}

#if canImport(UIKit)
final class AppDelegate: NSObject, UIApplicationDelegate {
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        _ = FirebaseBootstrap.configureIfAvailable()
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
        _ = FirebaseBootstrap.configureIfAvailable()
    }
#endif

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
