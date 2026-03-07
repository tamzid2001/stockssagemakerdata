import Foundation
import WebKit
#if canImport(FirebaseAuth)
import FirebaseAuth
#endif

private let fallbackWebOrigin = URL(string: "https://quantura.studio")!

@MainActor
final class NativeAuthWebBridge {
    static let shared = NativeAuthWebBridge()

    private weak var webView: WKWebView?
    private let trustedHosts: Set<String> = [
        "quantura.studio",
        "www.quantura.studio",
        "quantura-e2e3d.web.app",
        "quantura-e2e3d.firebaseapp.com",
        "localhost",
        "127.0.0.1",
    ]

    private init() {}

    func attach(webView: WKWebView) {
        self.webView = webView
        print("[AuthBridge][iOS] Attached WKWebView for auth sync.")
        pushCurrentAuthState()
    }

    func detach(webView: WKWebView) {
        guard self.webView === webView else { return }
        self.webView = nil
        print("[AuthBridge][iOS] Detached WKWebView.")
    }

    func pushCurrentAuthState(idTokenFresh: Bool = false) {
#if canImport(FirebaseAuth)
        pushAuthState(user: Auth.auth().currentUser, idTokenFresh: idTokenFresh)
#else
        pushAuthState(user: nil, idTokenFresh: idTokenFresh)
#endif
    }

#if canImport(FirebaseAuth)
    func pushAuthState(user: FirebaseAuth.User?, idTokenFresh: Bool = false) {
        var providers = user?.providerData.map { $0.providerID }.filter { !$0.isEmpty } ?? []
        if providers.isEmpty, user?.isAnonymous == true {
            providers = ["anonymous"]
        }

        let payload: [String: Any] = [
            "type": "AUTH_STATE",
            "uid": user?.uid ?? "",
            "isAnonymous": user?.isAnonymous ?? true,
            "providers": providers,
            "idTokenFresh": idTokenFresh,
        ]
        evaluateAuthJavaScript(for: payload)
    }

    func syncWebSessionFromCurrentUser(forceRefresh: Bool) async throws {
        guard let user = Auth.auth().currentUser else {
            throw NativeAuthBridgeError.userUnavailable
        }
        let nativeIdToken = try await user.nativeIDToken(forceRefresh: forceRefresh)
        let customToken = try await exchangeNativeIdTokenForCustomToken(nativeIdToken)
        pushAuthState(user: user, idTokenFresh: forceRefresh)
        injectCustomToken(customToken)
    }
#else
    func pushAuthState(user: Any?, idTokenFresh: Bool = false) {
        _ = user
        let payload: [String: Any] = [
            "type": "AUTH_STATE",
            "uid": "",
            "isAnonymous": true,
            "providers": [],
            "idTokenFresh": idTokenFresh,
        ]
        evaluateAuthJavaScript(for: payload)
    }

    func syncWebSessionFromCurrentUser(forceRefresh: Bool) async throws {
        _ = forceRefresh
        throw NativeAuthBridgeError.firebaseUnavailable
    }
#endif

    func injectCustomToken(_ customToken: String) {
        let token = customToken.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !token.isEmpty else { return }
        let escaped = token.jsSingleQuotedEscaped
        let script = """
            window.__QUANTURA_PENDING_CUSTOM_TOKEN__='\(escaped)';
            if (window.__quanturaAuthBridge?.receiveCustomToken) {
                window.__quanturaAuthBridge.receiveCustomToken('\(escaped)');
            }
            window.dispatchEvent(new CustomEvent('quantura:native-custom-token', { detail: { type: 'CUSTOM_TOKEN' } }));
        """
        evaluateJavaScript(script, reason: "custom-token")
    }

    private func evaluateAuthJavaScript(for payload: [String: Any]) {
        guard
            let data = try? JSONSerialization.data(withJSONObject: payload, options: []),
            let json = String(data: data, encoding: .utf8)
        else {
            return
        }
        let script = """
            window.__QUANTURA_PENDING_AUTH_STATE__=\(json);
            if (window.__quanturaAuthBridge?.onNativeAuthState) {
                window.__quanturaAuthBridge.onNativeAuthState(\(json));
            }
            window.dispatchEvent(new CustomEvent('quantura:native-auth-state', { detail: \(json) }));
        """
        evaluateJavaScript(script, reason: "auth-state")
    }

    private func evaluateJavaScript(_ script: String, reason: String) {
        guard let webView else {
            print("[AuthBridge][iOS] Skipping \(reason) injection; no WKWebView attached.")
            return
        }
        webView.evaluateJavaScript(script) { _, error in
            if let error {
                print("[AuthBridge][iOS] JS injection failed reason=\(reason): \(error.localizedDescription)")
            } else {
                print("[AuthBridge][iOS] JS injection succeeded reason=\(reason).")
            }
        }
    }

    private func exchangeNativeIdTokenForCustomToken(_ idToken: String) async throws -> String {
        let token = idToken.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !token.isEmpty else {
            throw NativeAuthBridgeError.invalidToken
        }

        let baseURL = preferredWebOrigin()
        let bundleId = Bundle.main.bundleIdentifier?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let mobilePayload: [String: Any] = [
            "platform": "ios",
            "bundleId": bundleId,
        ]

        do {
            return try await postAuthExchange(
                path: "/api/mobile/auth/exchange",
                baseURL: baseURL,
                idToken: token,
                payload: mobilePayload
            )
        } catch let error as NativeAuthExchangeHttpError {
            if ![400, 401, 403, 404, 405, 422, 501].contains(error.status) {
                throw NativeAuthBridgeError.exchangeFailed(detail: error.detail)
            }
            print("[AuthBridge][iOS] Mobile exchange unavailable; falling back to legacy endpoint.")
            return try await postAuthExchange(
                path: "/api/auth/exchange",
                baseURL: baseURL,
                idToken: token,
                payload: [:]
            )
        }
    }

    private func postAuthExchange(
        path: String,
        baseURL: URL,
        idToken: String,
        payload: [String: Any]
    ) async throws -> String {
        let endpoint = URL(string: path, relativeTo: baseURL) ?? baseURL.appendingPathComponent(path.trimmingCharacters(in: CharacterSet(charactersIn: "/")))
        var request = URLRequest(url: endpoint)
        request.httpMethod = "POST"
        request.timeoutInterval = 30
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(idToken)", forHTTPHeaderField: "Authorization")
        request.httpBody = try JSONSerialization.data(withJSONObject: payload, options: [])

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw NativeAuthBridgeError.httpFailed(status: -1)
        }

        let parsed = (try? JSONSerialization.jsonObject(with: data, options: [])) as? [String: Any]
        guard (200...299).contains(httpResponse.statusCode) else {
            let detail = String(describing: parsed?["error"] ?? parsed?["message"] ?? "auth exchange failed")
            throw NativeAuthExchangeHttpError(status: httpResponse.statusCode, detail: detail)
        }

        let customToken = String(describing: parsed?["customToken"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !customToken.isEmpty else {
            throw NativeAuthBridgeError.exchangeFailed(detail: "Server did not return customToken.")
        }
        return customToken
    }

    private func preferredWebOrigin() -> URL {
        if let url = webView?.url, isTrusted(url: url), let host = url.host {
            var components = URLComponents()
            components.scheme = (url.scheme?.isEmpty == false ? url.scheme! : "https")
            components.host = host
            components.port = url.port
            return components.url ?? fallbackWebOrigin
        }
        return fallbackWebOrigin
    }

    private func isTrusted(url: URL) -> Bool {
        guard let host = url.host?.lowercased() else { return false }
        if trustedHosts.contains(host) { return true }
        return host.hasSuffix(".quantura.studio")
    }
}

private struct NativeAuthExchangeHttpError: Error {
    let status: Int
    let detail: String
}

enum NativeAuthBridgeError: LocalizedError {
    case userUnavailable
    case invalidToken
    case httpFailed(status: Int)
    case exchangeFailed(detail: String)
    case firebaseUnavailable

    var errorDescription: String? {
        switch self {
        case .userUnavailable:
            return "No Firebase user is available."
        case .invalidToken:
            return "Native Firebase ID token is empty."
        case .httpFailed(let status):
            return "Auth exchange HTTP failed (\(status))."
        case .exchangeFailed(let detail):
            return "Auth exchange failed: \(detail)"
        case .firebaseUnavailable:
            return "Firebase Auth SDK is unavailable in this build."
        }
    }
}

private extension String {
    var jsSingleQuotedEscaped: String {
        replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }
}

#if canImport(FirebaseAuth)
private extension FirebaseAuth.User {
    func nativeIDToken(forceRefresh: Bool) async throws -> String {
        try await withCheckedThrowingContinuation { continuation in
            getIDTokenForcingRefresh(forceRefresh) { token, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                let cleanToken = token?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                if cleanToken.isEmpty {
                    continuation.resume(throwing: NativeAuthBridgeError.invalidToken)
                    return
                }
                continuation.resume(returning: cleanToken)
            }
        }
    }
}
#endif
