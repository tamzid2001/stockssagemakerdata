import FirebaseCore
#if canImport(FirebaseMessaging)
import FirebaseMessaging
#endif
import SwiftUI
#if canImport(UserNotifications)
import UserNotifications
#endif
#if canImport(UIKit)
import UIKit
#endif
#if canImport(GoogleMobileAds)
import GoogleMobileAds
#endif
#if canImport(GoogleSignIn)
import GoogleSignIn
#endif

extension Notification.Name {
    static let quanturaNativeFcmTokenUpdated = Notification.Name("quanturaNativeFcmTokenUpdated")
    static let quanturaNativeDeepLinkUpdated = Notification.Name("quanturaNativeDeepLinkUpdated")
}

final class NativeBridgeState {
    static let shared = NativeBridgeState()

    private(set) var pushToken: String = ""
    private(set) var pendingDeepLink: String = ""

    private init() {}

    func updatePushToken(_ token: String) {
        let next = token.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !next.isEmpty else { return }
        pushToken = next
        NotificationCenter.default.post(name: .quanturaNativeFcmTokenUpdated, object: next)
    }

    func updateDeepLink(_ url: String) {
        let next = url.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !next.isEmpty else { return }
        pendingDeepLink = next
        NotificationCenter.default.post(name: .quanturaNativeDeepLinkUpdated, object: next)
    }

    func clearPendingDeepLink() {
        pendingDeepLink = ""
    }
}

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
        let firebaseReady = FirebaseBootstrap.configureIfAvailable()
#if canImport(GoogleMobileAds)
        MobileAds.shared.start(completionHandler: nil)
#endif
#if canImport(FirebaseMessaging) && canImport(UserNotifications)
        if firebaseReady {
            UNUserNotificationCenter.current().delegate = self
            Messaging.messaging().delegate = self
            UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .badge, .sound]) { granted, _ in
                if granted {
                    DispatchQueue.main.async {
                        application.registerForRemoteNotifications()
                    }
                }
            }
            if let remotePayload = launchOptions?[.remoteNotification] as? [AnyHashable: Any],
               let target = remotePayload["url"] as? String {
                NativeBridgeState.shared.updateDeepLink(target)
            }
        }
#endif
        return true
    }

#if canImport(GoogleSignIn)
    func application(
        _ app: UIApplication,
        open url: URL,
        options: [UIApplication.OpenURLOptionsKey : Any] = [:]
    ) -> Bool {
        return GIDSignIn.sharedInstance.handle(url)
    }
#endif

#if canImport(FirebaseMessaging)
    func application(
        _ application: UIApplication,
        didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
    ) {
        Messaging.messaging().apnsToken = deviceToken
    }
#endif
}
#endif

#if canImport(UIKit) && canImport(FirebaseMessaging) && canImport(UserNotifications)
extension AppDelegate: UNUserNotificationCenterDelegate, MessagingDelegate {
    func messaging(_ messaging: Messaging, didReceiveRegistrationToken fcmToken: String?) {
        guard let fcmToken else { return }
        NativeBridgeState.shared.updatePushToken(fcmToken)
    }

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        completionHandler([.banner, .list, .sound])
    }

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completionHandler: @escaping () -> Void
    ) {
        let info = response.notification.request.content.userInfo
        if let target = info["url"] as? String {
            NativeBridgeState.shared.updateDeepLink(target)
        }
        completionHandler()
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
