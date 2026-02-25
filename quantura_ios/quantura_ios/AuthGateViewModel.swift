import FirebaseCore
#if canImport(FirebaseAuth)
import FirebaseAuth
#endif
#if canImport(GoogleSignIn)
import GoogleSignIn
#endif
#if canImport(UIKit)
import UIKit
#endif
#if canImport(AuthenticationServices)
import AuthenticationServices
#endif
#if canImport(CryptoKit)
import CryptoKit
#endif
import SwiftUI
#if canImport(Combine)
import Combine
#endif

@MainActor
final class AuthGateViewModel: ObservableObject {
    @Published var isGateVisible: Bool = true
    @Published var isBusy: Bool = false
    @Published var errorText: String = ""
    @Published var emailAddress: String = ""
    @Published var emailPassword: String = ""
    @Published var isEmailSheetVisible: Bool = false

#if canImport(FirebaseAuth)
    private var authStateHandle: AuthStateDidChangeListenerHandle?
    private var hasStarted = false
    private var bootstrapInFlight = false
    private var requiresBridgeSync = false
    private var skipForCurrentSession = false
    private var lastObservedUID: String = ""
#endif

    func start() {
#if canImport(FirebaseAuth)
        guard FirebaseApp.app() != nil else {
            print("[AuthGate][iOS] Firebase missing; gate stays visible.")
            isGateVisible = true
            return
        }
        guard !hasStarted else { return }
        hasStarted = true
        print("[AuthGate][iOS] Starting gate auth observer.")

        let auth = Auth.auth()
        if let currentUser = auth.currentUser {
            isGateVisible = currentUser.isAnonymous
            NativeAuthWebBridge.shared.pushAuthState(user: currentUser)
        } else {
            isGateVisible = true
        }

        authStateHandle = auth.addStateDidChangeListener { [weak self] _, user in
            Task { @MainActor [weak self] in
                self?.handleAuthStateChange(user: user)
            }
        }
        handleAuthStateChange(user: auth.currentUser)
#else
        isGateVisible = true
        errorText = "Firebase Auth SDK is unavailable in this build."
#endif
    }

    func presentGate(trigger: String) {
        print("[AuthGate][iOS] Present gate trigger=\(trigger)")
#if canImport(FirebaseAuth)
        skipForCurrentSession = false
#endif
        errorText = ""
        isGateVisible = true
    }

    func continueAnonymouslyForNow() {
        print("[AuthGate][iOS] Continue anonymously selected.")
#if canImport(FirebaseAuth)
        skipForCurrentSession = true
        requiresBridgeSync = false
#endif
        errorText = ""
        isGateVisible = false
    }

    func openEmailSheet() {
        emailAddress = ""
        emailPassword = ""
        errorText = ""
        isEmailSheetVisible = true
    }

    func signInWithEmail() {
#if canImport(FirebaseAuth)
        let email = emailAddress.trimmingCharacters(in: .whitespacesAndNewlines)
        let password = emailPassword
        guard !email.isEmpty, !password.isEmpty else {
            errorText = "Enter both email and password."
            return
        }
        beginInteractiveSignIn(provider: "email")
        let credential = EmailAuthProvider.credential(withEmail: email, password: password)
        signInOrLink(
            credential: credential,
            provider: "email",
            fallback: { completion in
                Auth.auth().signIn(with: credential, completion: completion)
            }
        )
#else
        errorText = "Email sign-in is unavailable in this build."
#endif
    }

    func signInWithGoogle(presenter: UIViewController) {
#if canImport(FirebaseAuth) && canImport(GoogleSignIn)
        guard FirebaseApp.app() != nil else {
            errorText = "Firebase is not configured."
            return
        }
        let clientID = FirebaseApp.app()?.options.clientID ?? ""
        guard !clientID.isEmpty else {
            errorText = "Google client ID is missing."
            return
        }
        beginInteractiveSignIn(provider: "google")
        GIDSignIn.sharedInstance.configuration = GIDConfiguration(clientID: clientID)
        print("[AuthGate][iOS] Starting Google Sign-In UI flow.")
        GIDSignIn.sharedInstance.signIn(withPresenting: presenter) { [weak self] result, error in
            Task { @MainActor [weak self] in
                guard let self else { return }
                if let error {
                    self.finishError(error.localizedDescription)
                    return
                }
                guard
                    let googleUser = result?.user,
                    let idToken = googleUser.idToken?.tokenString
                else {
                    self.finishError("Google did not return an ID token.")
                    return
                }
                let credential = GoogleAuthProvider.credential(
                    withIDToken: idToken,
                    accessToken: googleUser.accessToken.tokenString
                )
                self.signInOrLink(
                    credential: credential,
                    provider: "google",
                    fallback: { completion in
                        Auth.auth().signIn(with: credential, completion: completion)
                    }
                )
            }
        }
#else
        _ = presenter
        errorText = "Google Sign-In SDK is unavailable in this build."
#endif
    }

    func signInWithApple(presenterAnchor: ASPresentationAnchor) {
#if canImport(FirebaseAuth) && canImport(AuthenticationServices) && canImport(CryptoKit)
        beginInteractiveSignIn(provider: "apple")

        let rawNonce = Self.randomNonceString()
        let request = ASAuthorizationAppleIDProvider().createRequest()
        request.requestedScopes = [.fullName, .email]
        request.nonce = Self.sha256(rawNonce)

        let controller = ASAuthorizationController(authorizationRequests: [request])
        let delegate = AppleSignInDelegate(rawNonce: rawNonce) { [weak self] result in
            Task { @MainActor [weak self] in
                guard let self else { return }
                switch result {
                case .success(let credential):
                    self.signInOrLink(
                        credential: credential,
                        provider: "apple",
                        fallback: { completion in
                            Auth.auth().signIn(with: credential, completion: completion)
                        }
                    )
                case .failure(let error):
                    self.finishError(error.localizedDescription)
                }
            }
        }
        delegate.presentationAnchor = presenterAnchor
        controller.delegate = delegate
        controller.presentationContextProvider = delegate
        AppleSignInDelegate.retain(delegate)
        print("[AuthGate][iOS] Starting Sign in with Apple UI flow.")
        controller.performRequests()
#else
        _ = presenterAnchor
        errorText = "Sign in with Apple is unavailable in this build."
#endif
    }

    func signOutToAnonymous() {
#if canImport(FirebaseAuth)
        print("[AuthGate][iOS] Native sign-out requested.")
        do {
            try Auth.auth().signOut()
        } catch {
            print("[AuthGate][iOS] Sign-out warning: \(error.localizedDescription)")
        }
#if canImport(GoogleSignIn)
        GIDSignIn.sharedInstance.signOut()
#endif
        skipForCurrentSession = false
        requiresBridgeSync = false
        lastObservedUID = ""
        isGateVisible = true
        errorText = ""
        ensureAnonymousIfNeeded()
        NativeAuthWebBridge.shared.pushCurrentAuthState()
#endif
    }
}

#if canImport(FirebaseAuth)
@MainActor
private extension AuthGateViewModel {
    func handleAuthStateChange(user: FirebaseAuth.User?) {
        let uid = user?.uid ?? ""
        if uid != lastObservedUID {
            lastObservedUID = uid
        }

        if let user {
            print("[AuthGate][iOS] Auth state uid=\(user.uid) anonymous=\(user.isAnonymous)")
            NativeAuthWebBridge.shared.pushAuthState(user: user)
            if user.isAnonymous {
                isGateVisible = !skipForCurrentSession
                return
            }

            if requiresBridgeSync {
                print("[AuthGate][iOS] Waiting for native->web token sync before dismissing gate.")
                isGateVisible = true
                return
            }

            isGateVisible = false
            if !isBusy {
                Task {
                    do {
                        try await NativeAuthWebBridge.shared.syncWebSessionFromCurrentUser(forceRefresh: false)
                    } catch {
                        print("[AuthGate][iOS] Silent web sync skipped: \(error.localizedDescription)")
                    }
                }
            }
            return
        }

        print("[AuthGate][iOS] No authenticated user; ensuring anonymous bootstrap.")
        isGateVisible = !skipForCurrentSession
        ensureAnonymousIfNeeded()
    }

    func ensureAnonymousIfNeeded() {
        guard Auth.auth().currentUser == nil else { return }
        guard !bootstrapInFlight else { return }
        bootstrapInFlight = true
        Auth.auth().signInAnonymously { [weak self] _, error in
            Task { @MainActor [weak self] in
                guard let self else { return }
                self.bootstrapInFlight = false
                if let error {
                    print("[AuthGate][iOS] Anonymous sign-in failed: \(error.localizedDescription)")
                } else {
                    print("[AuthGate][iOS] Anonymous sign-in succeeded.")
                }
            }
        }
    }

    func beginInteractiveSignIn(provider: String) {
        print("[AuthGate][iOS] Presenting provider flow provider=\(provider)")
        skipForCurrentSession = false
        requiresBridgeSync = true
        isBusy = true
        errorText = ""
        isGateVisible = true
    }

    func signInOrLink(
        credential: AuthCredential,
        provider: String,
        fallback: @escaping (@escaping (AuthDataResult?, Error?) -> Void) -> Void
    ) {
        if let currentUser = Auth.auth().currentUser, currentUser.isAnonymous {
            print("[AuthGate][iOS] Attempting anonymous link provider=\(provider)")
            currentUser.link(with: credential) { [weak self] result, error in
                Task { @MainActor [weak self] in
                    guard let self else { return }
                    if let resolvedCredential = self.resolveCollisionCredential(error: error, fallback: credential) {
                        print("[AuthGate][iOS] Link collision provider=\(provider); fallback sign-in.")
                        Auth.auth().signIn(with: resolvedCredential) { authResult, signInError in
                            Task { @MainActor [weak self] in
                                guard let self else { return }
                                if let signInError {
                                    self.finishError(signInError.localizedDescription)
                                    return
                                }
                                let linkedUser = authResult?.user ?? result?.user
                                self.finishSuccess(user: linkedUser)
                            }
                        }
                        return
                    }
                    if let error {
                        self.finishError(error.localizedDescription)
                        return
                    }
                    self.finishSuccess(user: result?.user)
                }
            }
            return
        }

        fallback { [weak self] result, error in
            Task { @MainActor [weak self] in
                guard let self else { return }
                if let error {
                    self.finishError(error.localizedDescription)
                    return
                }
                self.finishSuccess(user: result?.user)
            }
        }
    }

    func finishSuccess(user: FirebaseAuth.User?) {
        guard let user else {
            finishError("Signed in, but Firebase user is unavailable.")
            return
        }
        print("[AuthGate][iOS] Native sign-in completed uid=\(user.uid); exchanging custom token.")
        Task {
            do {
                try await NativeAuthWebBridge.shared.syncWebSessionFromCurrentUser(forceRefresh: true)
                requiresBridgeSync = false
                isBusy = false
                errorText = ""
                isEmailSheetVisible = false
                isGateVisible = false
                print("[AuthGate][iOS] Web session sync succeeded uid=\(user.uid).")
            } catch {
                finishError("Signed in, but web sync failed: \(error.localizedDescription)")
            }
        }
    }

    func finishError(_ message: String) {
        print("[AuthGate][iOS] Sign-in flow failed: \(message)")
#if canImport(FirebaseAuth)
        requiresBridgeSync = (Auth.auth().currentUser?.isAnonymous == false)
#else
        requiresBridgeSync = false
#endif
        isBusy = false
        isGateVisible = true
        errorText = message
    }

    func resolveCollisionCredential(error: Error?, fallback: AuthCredential) -> AuthCredential? {
        guard let nsError = error as NSError? else { return nil }
        guard let code = AuthErrorCode(rawValue: nsError.code) else { return nil }
        let collisionCodes: Set<AuthErrorCode> = [
            .credentialAlreadyInUse,
            .emailAlreadyInUse,
            .accountExistsWithDifferentCredential,
            .providerAlreadyLinked,
        ]
        guard collisionCodes.contains(code) else { return nil }
        return (nsError.userInfo[AuthErrorUserInfoUpdatedCredentialKey] as? AuthCredential) ?? fallback
    }

    static func randomNonceString(length: Int = 32) -> String {
        let charset = Array("0123456789ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvwxyz-._")
        var result = ""
        result.reserveCapacity(length)
        for _ in 0..<length {
            result.append(charset.randomElement() ?? "0")
        }
        return result
    }

    static func sha256(_ input: String) -> String {
#if canImport(CryptoKit)
        let digest = SHA256.hash(data: Data(input.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
#else
        return input
#endif
    }
}
#endif

#if canImport(FirebaseAuth) && canImport(AuthenticationServices) && canImport(CryptoKit)
enum AppleSignInFlowError: LocalizedError {
    case message(String)

    var errorDescription: String? {
        switch self {
        case .message(let message):
            return message
        }
    }
}

final class AppleSignInDelegate: NSObject, ASAuthorizationControllerDelegate, ASAuthorizationControllerPresentationContextProviding {
    static var retainedDelegates: [AppleSignInDelegate] = []
    static func retain(_ delegate: AppleSignInDelegate) {
        retainedDelegates.append(delegate)
    }

    var presentationAnchor: ASPresentationAnchor = ASPresentationAnchor()

    private let rawNonce: String
    private let completion: (Result<AuthCredential, Error>) -> Void

    init(rawNonce: String, completion: @escaping (Result<AuthCredential, Error>) -> Void) {
        self.rawNonce = rawNonce
        self.completion = completion
    }

    func presentationAnchor(for controller: ASAuthorizationController) -> ASPresentationAnchor {
        presentationAnchor
    }

    func authorizationController(controller: ASAuthorizationController, didCompleteWithAuthorization authorization: ASAuthorization) {
        defer { Self.retainedDelegates.removeAll { $0 === self } }
        guard let credential = authorization.credential as? ASAuthorizationAppleIDCredential else {
            completion(.failure(AppleSignInFlowError.message("Apple credential was not returned.")))
            return
        }
        guard let tokenData = credential.identityToken, let idToken = String(data: tokenData, encoding: .utf8) else {
            completion(.failure(AppleSignInFlowError.message("Apple identity token is missing.")))
            return
        }
        let authCredential = OAuthProvider.appleCredential(
            withIDToken: idToken,
            rawNonce: rawNonce,
            fullName: credential.fullName
        )
        completion(.success(authCredential))
    }

    func authorizationController(controller: ASAuthorizationController, didCompleteWithError error: Error) {
        defer { Self.retainedDelegates.removeAll { $0 === self } }
        completion(.failure(error))
    }
}
#endif
