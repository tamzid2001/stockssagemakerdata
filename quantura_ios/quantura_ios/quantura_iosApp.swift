import FirebaseCore
#if canImport(FirebaseAuth)
import FirebaseAuth
#endif
#if canImport(FirebaseFirestore)
import FirebaseFirestore
#endif
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
#if canImport(AppTrackingTransparency)
import AppTrackingTransparency
#endif
#if canImport(AdSupport)
import AdSupport
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

#if canImport(FirebaseAuth) && canImport(FirebaseFirestore) && canImport(UserNotifications)
final class NativePersonalizedNotificationManager {
    static let shared = NativePersonalizedNotificationManager()

    private let db = Firestore.firestore()
    private var authHandle: AuthStateDidChangeListenerHandle?
    private var watchlistListener: ListenerRegistration?
    private var forecastListener: ListenerRegistration?
    private var aiLogListener: ListenerRegistration?
    private var activeUid: String = ""
    private var primedKeys: Set<String> = []
    private var lastSentAt: [String: Date] = [:]

    private init() {}

    func start() {
        guard FirebaseApp.app() != nil else { return }
        guard authHandle == nil else { return }
        authHandle = Auth.auth().addStateDidChangeListener { [weak self] _, user in
            self?.bind(user: user)
        }
        bind(user: Auth.auth().currentUser)
    }

    private func bind(user: FirebaseAuth.User?) {
        let uid = user?.uid.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if uid == activeUid { return }
        clearListeners()
        activeUid = uid
        guard !uid.isEmpty else { return }

        watchlistListener = db.collection("users").document(uid).collection("watchlist")
            .order(by: "updatedAt", descending: true)
            .limit(to: 1)
            .addSnapshotListener { [weak self] snapshot, _ in
                self?.handleSnapshot(
                    key: "watchlist_\(uid)",
                    snapshot: snapshot,
                    title: "Watchlist updated",
                    fallbackBody: "Your Quantura watchlist changed.",
                    url: "/watchlist"
                )
            }

        forecastListener = db.collection("forecast_requests")
            .whereField("userId", isEqualTo: uid)
            .limit(to: 1)
            .addSnapshotListener { [weak self] snapshot, _ in
                self?.handleSnapshot(
                    key: "forecast_\(uid)",
                    snapshot: snapshot,
                    title: "Forecast update",
                    fallbackBody: "A forecast was updated in your workspace.",
                    url: "/saved-forecasts"
                )
            }

        aiLogListener = db.collection("users").document(uid).collection("ai_logs")
            .order(by: "createdAt", descending: true)
            .limit(to: 1)
            .addSnapshotListener { [weak self] snapshot, _ in
                self?.handleSnapshot(
                    key: "ai_logs_\(uid)",
                    snapshot: snapshot,
                    title: "AI log update",
                    fallbackBody: "New AI activity is available.",
                    url: "/dashboard"
                )
            }
    }

    private func handleSnapshot(
        key: String,
        snapshot: QuerySnapshot?,
        title: String,
        fallbackBody: String,
        url: String
    ) {
        guard let snapshot else { return }
        if !primedKeys.contains(key) {
            primedKeys.insert(key)
            return
        }
        guard !snapshot.documentChanges.isEmpty else { return }
        guard let firstChange = snapshot.documentChanges.first else { return }
        guard firstChange.type == .added || firstChange.type == .modified else { return }

        if let last = lastSentAt[key], Date().timeIntervalSince(last) < 10 {
            return
        }
        lastSentAt[key] = Date()

        let data = firstChange.document.data()
        let explicitBody = String(
            data["notificationText"] as? String
                ?? data["summary"] as? String
                ?? data["notes"] as? String
                ?? data["title"] as? String
                ?? ""
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        let body = explicitBody.isEmpty ? fallbackBody : explicitBody
        scheduleLocalNotification(title: title, body: body, url: url)
    }

    private func scheduleLocalNotification(title: String, body: String, url: String) {
        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.sound = .default
        content.userInfo = ["url": url]

        let request = UNNotificationRequest(
            identifier: UUID().uuidString,
            content: content,
            trigger: UNTimeIntervalNotificationTrigger(timeInterval: 1, repeats: false)
        )
        UNUserNotificationCenter.current().add(request) { error in
            if let error {
                print("Native local notification scheduling failed: \(error.localizedDescription)")
            }
        }
    }

    private func clearListeners() {
        watchlistListener?.remove()
        watchlistListener = nil
        forecastListener?.remove()
        forecastListener = nil
        aiLogListener?.remove()
        aiLogListener = nil
    }
}
#endif

enum FirebaseBootstrap {
    static func configureIfAvailable() -> Bool {
        if FirebaseApp.app() != nil {
            print("[Firebase][iOS] Firebase already configured.")
            return true
        }

        let options: FirebaseOptions? = {
            let candidates = [
                ("GoogleService-Info", "plist"),
                ("GoogleService-Info.local", "plist"),
            ]
            for candidate in candidates {
                if let path = Bundle.main.path(forResource: candidate.0, ofType: candidate.1),
                   let parsed = FirebaseOptions(contentsOfFile: path) {
                    print("[Firebase][iOS] Using Firebase config \(candidate.0).\(candidate.1).")
                    return parsed
                }
            }
            return nil
        }()

        guard let options else {
            print("[Firebase][iOS] Firebase disabled: missing GoogleService-Info(.local).plist in app bundle.")
            return false
        }

        FirebaseApp.configure(options: options)
        print("[Firebase][iOS] Firebase configured successfully.")
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
        #if DEBUG
        MobileAds.shared.requestConfiguration.testDeviceIdentifiers = ["SIMULATOR"]
        #endif
        MobileAds.shared.start { status in
            print("[Ads][iOS] Mobile Ads initialized adapters=\(status.adapterStatusesByClassName.count)")
        }
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
#if canImport(FirebaseAuth)
        if firebaseReady {
            NativeAuthSessionManager.shared.startIfNeeded()
        }
#endif
#if canImport(FirebaseAuth) && canImport(FirebaseFirestore) && canImport(UserNotifications)
        if firebaseReady {
            NativePersonalizedNotificationManager.shared.start()
        }
#endif
        return true
    }

    func applicationDidBecomeActive(_ application: UIApplication) {
        _ = application
        requestTrackingPermissionIfNeeded()
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

    private func requestTrackingPermissionIfNeeded() {
#if canImport(AppTrackingTransparency)
        guard #available(iOS 14, *) else { return }
        guard ATTrackingManager.trackingAuthorizationStatus == .notDetermined else { return }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.8) {
            ATTrackingManager.requestTrackingAuthorization { status in
                switch status {
                case .authorized:
                    print("[Privacy][iOS] ATT authorized.")
                case .denied, .restricted, .notDetermined:
                    print("[Privacy][iOS] ATT denied/restricted/notDetermined.")
                @unknown default:
                    break
                }
            }
        }
#endif
    }
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
