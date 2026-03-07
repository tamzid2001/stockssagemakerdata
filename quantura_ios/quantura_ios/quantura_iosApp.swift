import Foundation
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
#if canImport(FBAudienceNetwork)
import FBAudienceNetwork
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

#if canImport(FirebaseAuth)
private extension FirebaseAuth.User {
    func idTokenAsync(forceRefresh: Bool) async throws -> String {
        try await withCheckedThrowingContinuation { continuation in
            getIDTokenForcingRefresh(forceRefresh) { token, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                let clean = token?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                if clean.isEmpty {
                    continuation.resume(
                        throwing: NSError(
                            domain: "NativeNotificationSyncService",
                            code: -1,
                            userInfo: [NSLocalizedDescriptionKey: "Firebase ID token is empty."]
                        )
                    )
                    return
                }
                continuation.resume(returning: clean)
            }
        }
    }
}
#endif

#if canImport(UserNotifications)
enum InactivityLocalNotificationScheduler {
    static let dailyIdentifier = "quantura.inactive.daily"
    static let weeklyIdentifier = "quantura.inactive.weekly"

    static func reschedule() {
        let center = UNUserNotificationCenter.current()
        center.getNotificationSettings { settings in
            let allowed = settings.authorizationStatus == .authorized
                || settings.authorizationStatus == .provisional
                || settings.authorizationStatus == .ephemeral
            guard allowed else { return }

            center.removePendingNotificationRequests(withIdentifiers: [dailyIdentifier, weeklyIdentifier])

            let daily = UNMutableNotificationContent()
            daily.title = "Come back to Quantura"
            daily.body = "Your watchlist and forecasts may have moved. Re-open Quantura for a quick check."
            daily.sound = .default
            daily.userInfo = ["url": "/forecasting?source=inactive_daily"]

            let weekly = UNMutableNotificationContent()
            weekly.title = "Weekly Quantura recap"
            weekly.body = "Review this week’s market updates and model outputs."
            weekly.sound = .default
            weekly.userInfo = ["url": "/explore?source=inactive_weekly"]

            let dailyRequest = UNNotificationRequest(
                identifier: dailyIdentifier,
                content: daily,
                trigger: UNTimeIntervalNotificationTrigger(timeInterval: 24 * 60 * 60, repeats: true)
            )
            let weeklyRequest = UNNotificationRequest(
                identifier: weeklyIdentifier,
                content: weekly,
                trigger: UNTimeIntervalNotificationTrigger(timeInterval: 7 * 24 * 60 * 60, repeats: true)
            )

            center.add(dailyRequest) { error in
                if let error {
                    print("[Notify][iOS] Daily inactivity schedule failed: \(error.localizedDescription)")
                }
            }
            center.add(weeklyRequest) { error in
                if let error {
                    print("[Notify][iOS] Weekly inactivity schedule failed: \(error.localizedDescription)")
                }
            }
        }
    }
}
#endif

#if canImport(FirebaseAuth)
@MainActor
final class NativeNotificationSyncService {
    static let shared = NativeNotificationSyncService()

    private let baseURL = URL(string: "https://quantura.studio")!
    private var authHandle: AuthStateDidChangeListenerHandle?
    private var currentFcmToken: String = ""
    private var lastSessionPingAt: Date = .distantPast
    private var lastRegisteredTokenKey: String = ""

    private init() {}

    func start() {
        guard FirebaseApp.app() != nil else { return }
        guard authHandle == nil else { return }
        authHandle = Auth.auth().addStateDidChangeListener { [weak self] _, user in
            guard let self else { return }
            Task { @MainActor in
                await self.syncSessionAndToken(user: user, forcePing: true, reason: "auth_state")
            }
        }
        Task { @MainActor in
            await syncSessionAndToken(user: Auth.auth().currentUser, forcePing: true, reason: "startup")
        }
    }

    func markSessionActive() {
        Task { @MainActor in
            await syncSessionAndToken(user: Auth.auth().currentUser, forcePing: true, reason: "app_active")
        }
    }

    func updateFcmToken(_ token: String) {
        let clean = token.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !clean.isEmpty else { return }
        currentFcmToken = clean
        Task { @MainActor in
            await syncSessionAndToken(user: Auth.auth().currentUser, forcePing: false, reason: "token_refresh")
        }
    }

    private func shouldPingSession() -> Bool {
        Date().timeIntervalSince(lastSessionPingAt) > 45
    }

    private func syncSessionAndToken(user: FirebaseAuth.User?, forcePing: Bool, reason: String) async {
        guard let user else { return }
        do {
            let idToken = try await user.idTokenAsync(forceRefresh: false)
            if forcePing || shouldPingSession() {
                _ = try await postJSON(
                    path: "/api/notifications/session/ping",
                    idToken: idToken,
                    payload: [
                        "isAnonymous": user.isAnonymous,
                    ]
                )
                lastSessionPingAt = Date()
            }

            let token = currentFcmToken.trimmingCharacters(in: .whitespacesAndNewlines)
            if !token.isEmpty {
                let key = "\(user.uid)|\(token)"
                if forcePing || key != lastRegisteredTokenKey {
                    _ = try await postJSON(
                        path: "/api/notifications/register-token",
                        idToken: idToken,
                        payload: [
                            "token": token,
                            "platform": "ios",
                        ]
                    )
                    lastRegisteredTokenKey = key
                }
            }
        } catch {
            print("[Notify][iOS] Session/token sync skipped reason=\(reason): \(error.localizedDescription)")
        }
    }

    private func postJSON(path: String, idToken: String, payload: [String: Any]) async throws -> [String: Any] {
        let endpoint = URL(string: path, relativeTo: baseURL) ?? baseURL.appendingPathComponent(path)
        var request = URLRequest(url: endpoint)
        request.httpMethod = "POST"
        request.timeoutInterval = 30
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(idToken)", forHTTPHeaderField: "Authorization")
        request.httpBody = try JSONSerialization.data(withJSONObject: payload, options: [])

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw NSError(
                domain: "NativeNotificationSyncService",
                code: -2,
                userInfo: [NSLocalizedDescriptionKey: "Invalid notification sync response."]
            )
        }
        let parsed = (try? JSONSerialization.jsonObject(with: data, options: [])) as? [String: Any] ?? [:]
        guard (200...299).contains(httpResponse.statusCode) else {
            let detail = String(describing: parsed["error"] ?? parsed["message"] ?? "request_failed")
            throw NSError(
                domain: "NativeNotificationSyncService",
                code: httpResponse.statusCode,
                userInfo: [NSLocalizedDescriptionKey: detail]
            )
        }
        return parsed
    }
}
#endif

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

        let options = optionsFromInfoPlist()

        guard let options else {
            print("[Firebase][iOS] Firebase disabled: missing valid Firebase keys in app Info.plist.")
            return false
        }

        guard validate(options, source: "Info.plist") else {
            print("[Firebase][iOS] Firebase disabled: invalid Firebase keys in app Info.plist.")
            return false
        }

        FirebaseApp.configure(options: options)
        print("[Firebase][iOS] Firebase configured successfully.")
        return FirebaseApp.app() != nil
    }

    private static func optionsFromInfoPlist() -> FirebaseOptions? {
        guard let info = Bundle.main.infoDictionary else {
            return nil
        }
        func value(_ key: String) -> String {
            (info[key] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        }

        let googleAppID = value("GOOGLE_APP_ID")
        let gcmSenderID = value("GCM_SENDER_ID")
        let apiKey = value("API_KEY")
        let projectID = value("PROJECT_ID")
        let clientID = value("CLIENT_ID")
        let storageBucket = value("STORAGE_BUCKET")

        guard !googleAppID.isEmpty, !gcmSenderID.isEmpty, !apiKey.isEmpty else {
            return nil
        }

        let options = FirebaseOptions(googleAppID: googleAppID, gcmSenderID: gcmSenderID)
        options.apiKey = apiKey
        options.projectID = projectID.isEmpty ? nil : projectID
        options.clientID = clientID.isEmpty ? nil : clientID
        options.bundleID = Bundle.main.bundleIdentifier ?? value("BUNDLE_ID")
        options.storageBucket = storageBucket.isEmpty ? nil : storageBucket
        return options
    }

    private static func validate(_ options: FirebaseOptions, source: String) -> Bool {
        let apiKey = (options.apiKey ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let apiKeyRegex = #"^A[0-9A-Za-z_-]{38}$"#
        guard apiKey.range(of: apiKeyRegex, options: .regularExpression) != nil else {
            print("[Firebase][iOS] Invalid API_KEY in \(source): expected 39 chars and prefix 'A'.")
            return false
        }

        let googleAppID = options.googleAppID.trimmingCharacters(in: .whitespacesAndNewlines)
        if googleAppID.isEmpty || googleAppID.contains("REPLACE_WITH_") {
            print("[Firebase][iOS] Invalid GOOGLE_APP_ID in \(source).")
            return false
        }

        let projectID = options.projectID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if projectID.isEmpty || projectID.contains("REPLACE_WITH_") {
            print("[Firebase][iOS] Invalid PROJECT_ID in \(source).")
            return false
        }

        let gcmSenderID = options.gcmSenderID.trimmingCharacters(in: .whitespacesAndNewlines)
        if gcmSenderID.isEmpty || gcmSenderID.contains("REPLACE_WITH_") {
            print("[Firebase][iOS] Invalid GCM_SENDER_ID in \(source).")
            return false
        }

        return true
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
        #if DEBUG || targetEnvironment(simulator)
        MobileAds.shared.requestConfiguration.testDeviceIdentifiers = ["SIMULATOR"]
        #endif
#if canImport(FBAudienceNetwork)
        if #available(iOS 14, *) {
            // Meta Audience Network guidance: explicitly opt into advertiser tracking when policy permits.
            FBAdSettings.setAdvertiserTrackingEnabled(true)
        }
#endif
        MobileAds.shared.start { status in
            print("[Ads][iOS] Mobile Ads initialized adapters=\(status.adapterStatusesByClassName.count)")
        }
#endif
#if canImport(FirebaseMessaging) && canImport(UserNotifications)
        if firebaseReady {
            UNUserNotificationCenter.current().delegate = self
            Messaging.messaging().delegate = self
            Messaging.messaging().token { token, _ in
                guard let token else { return }
                NativeBridgeState.shared.updatePushToken(token)
#if canImport(FirebaseAuth)
                NativeNotificationSyncService.shared.updateFcmToken(token)
#endif
            }
            UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .badge, .sound]) { granted, _ in
                if granted {
                    DispatchQueue.main.async {
                        application.registerForRemoteNotifications()
                    }
#if canImport(UserNotifications)
                    InactivityLocalNotificationScheduler.reschedule()
#endif
                }
            }
            if let remotePayload = launchOptions?[.remoteNotification] as? [AnyHashable: Any],
               let target = (remotePayload["url"] as? String) ?? (remotePayload["path"] as? String) {
                NativeBridgeState.shared.updateDeepLink(target)
            }
        }
#endif
#if canImport(FirebaseAuth)
        if firebaseReady {
            NativeAuthSessionManager.shared.startIfNeeded()
            NativeNotificationSyncService.shared.start()
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
#if canImport(FirebaseAuth)
        NativeNotificationSyncService.shared.markSessionActive()
#endif
#if canImport(UserNotifications)
        InactivityLocalNotificationScheduler.reschedule()
#endif
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
#if canImport(FBAudienceNetwork)
                    FBAdSettings.setAdvertiserTrackingEnabled(true)
#endif
                case .denied, .restricted, .notDetermined:
                    print("[Privacy][iOS] ATT denied/restricted/notDetermined.")
#if canImport(FBAudienceNetwork)
                    FBAdSettings.setAdvertiserTrackingEnabled(false)
#endif
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
#if canImport(FirebaseAuth)
        NativeNotificationSyncService.shared.updateFcmToken(fcmToken)
#endif
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
        if let target = (info["url"] as? String) ?? (info["path"] as? String) {
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
