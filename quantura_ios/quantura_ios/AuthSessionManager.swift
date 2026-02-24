import FirebaseCore
#if canImport(FirebaseAuth)
import FirebaseAuth
#endif

#if canImport(FirebaseAuth)
final class NativeAuthSessionManager {
    static let shared = NativeAuthSessionManager()

    private var authHandle: AuthStateDidChangeListenerHandle?
    private var anonymousBootstrapInFlight = false

    private init() {}

    func startIfNeeded() {
        guard FirebaseApp.app() != nil else {
            print("[Auth][iOS] Firebase is unavailable; auth session manager not started.")
            return
        }
        guard authHandle == nil else { return }

        print("[Auth][iOS] Starting auth state listener.")
        authHandle = Auth.auth().addStateDidChangeListener { [weak self] _, user in
            self?.handleAuthState(user: user)
        }
        handleAuthState(user: Auth.auth().currentUser)
    }

    func signInOrLink(
        with credential: AuthCredential,
        provider: String,
        completion: @escaping (AuthDataResult?, Error?) -> Void
    ) {
        let providerLabel = provider.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let auth = Auth.auth()
        guard let currentUser = auth.currentUser else {
            print("[Auth][iOS] No current user; signing in with \(providerLabel).")
            auth.signIn(with: credential, completion: completion)
            return
        }

        if currentUser.isAnonymous {
            print("[Auth][iOS] Attempting link from anonymous user to provider=\(providerLabel).")
            currentUser.link(with: credential) { [weak self] authResult, error in
                if let resolved = self?.resolveLinkCollision(error: error, fallback: credential) {
                    print("[Auth][iOS] Link collision for provider=\(providerLabel); falling back to sign-in.")
                    auth.signIn(with: resolved, completion: completion)
                    return
                }
                if let error {
                    print("[Auth][iOS] Link failed provider=\(providerLabel): \(error.localizedDescription)")
                } else if let uid = authResult?.user.uid {
                    print("[Auth][iOS] Link success provider=\(providerLabel) uid=\(uid)")
                }
                completion(authResult, error)
            }
            return
        }

        print("[Auth][iOS] Existing non-anonymous session uid=\(currentUser.uid); signing in with \(providerLabel).")
        auth.signIn(with: credential, completion: completion)
    }

    private func handleAuthState(user: FirebaseAuth.User?) {
        guard let user else {
            signInAnonymouslyIfNeeded()
            return
        }
        print("[Auth][iOS] Auth state changed uid=\(user.uid) anonymous=\(user.isAnonymous)")
    }

    private func signInAnonymouslyIfNeeded() {
        guard !anonymousBootstrapInFlight else { return }
        anonymousBootstrapInFlight = true
        print("[Auth][iOS] No current user; bootstrapping anonymous session.")
        Auth.auth().signInAnonymously { [weak self] authResult, error in
            guard let self else { return }
            self.anonymousBootstrapInFlight = false
            if let error {
                print("[Auth][iOS] Anonymous sign-in failed: \(error.localizedDescription)")
                return
            }
            if let user = authResult?.user {
                print("[Auth][iOS] Anonymous sign-in succeeded uid=\(user.uid)")
            }
        }
    }

    private func resolveLinkCollision(error: Error?, fallback: AuthCredential) -> AuthCredential? {
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
}
#else
final class NativeAuthSessionManager {
    static let shared = NativeAuthSessionManager()

    private init() {}

    func startIfNeeded() {}

    func signInOrLink(
        with credential: Any,
        provider: String,
        completion: @escaping (Any?, Error?) -> Void
    ) {
        _ = credential
        _ = provider
        completion(nil, nil)
    }
}
#endif
