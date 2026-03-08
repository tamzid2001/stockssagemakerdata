import SwiftUI
#if canImport(AuthenticationServices)
import AuthenticationServices
#endif

struct AuthGateView: View {
    @ObservedObject var viewModel: AuthGateViewModel

    var body: some View {
        let quanturaInk = Color(red: 0.05, green: 0.11, blue: 0.24)
        let quanturaAqua = Color(red: 0.55, green: 0.84, blue: 0.86)
        let quanturaMist = Color(red: 0.91, green: 0.97, blue: 0.97)
        let quanturaSand = Color(red: 0.97, green: 0.95, blue: 0.90)
        let quanturaOrange = Color(red: 1.0, green: 0.48, blue: 0.10)
        ZStack {
            LinearGradient(
                colors: [
                    quanturaSand,
                    quanturaMist,
                    Color.white,
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            VStack(spacing: 22) {
                Spacer()

                VStack(spacing: 14) {
                    HStack(spacing: 14) {
                        Image("AppLogo")
                            .resizable()
                            .scaledToFit()
                            .frame(width: 68, height: 68)
                            .padding(6)
                            .background(Color.white.opacity(0.92), in: RoundedRectangle(cornerRadius: 18, style: .continuous))

                        VStack(alignment: .leading, spacing: 4) {
                            Text("QUANTURA")
                                .font(.system(size: 12, weight: .black, design: .rounded))
                                .tracking(1.8)
                                .foregroundStyle(quanturaInk)
                            Text("Sign in to Quantura")
                                .font(.system(size: 30, weight: .bold, design: .rounded))
                                .foregroundStyle(quanturaInk)
                            Text("Sync forecasts, alerts, and portfolio workflows in one native session.")
                                .font(.system(size: 15, weight: .medium))
                                .multilineTextAlignment(.leading)
                                .foregroundStyle(quanturaInk.opacity(0.74))
                        }
                    }
                    .padding(.horizontal, 18)
                    .padding(.vertical, 18)
                    .background(Color.white.opacity(0.88), in: RoundedRectangle(cornerRadius: 28, style: .continuous))
                }

                VStack(spacing: 12) {
                    Button {
                        guard let presenter = QuanturaWebView.Coordinator.topViewController() else {
                            viewModel.presentGate(trigger: "missing_presenter")
                            return
                        }
                        viewModel.signInWithGoogle(presenter: presenter)
                    } label: {
                        HStack(spacing: 10) {
                            AuthProviderMark(kind: .google)
                            Text("Continue with Google")
                                .fontWeight(.semibold)
                        }
                        .frame(maxWidth: .infinity, minHeight: 52)
                    }
                    .buttonStyle(.borderedProminent)
                    .tint(Color.white)
                    .foregroundStyle(quanturaInk)
                    .disabled(viewModel.isBusy)

                    HStack(spacing: 8) {
                        Button {
                            viewModel.signInWithGitHub()
                        } label: {
                            HStack(spacing: 6) {
                                AuthProviderMark(kind: .github)
                                Text("GitHub")
                                    .fontWeight(.semibold)
                            }
                            .frame(maxWidth: .infinity, minHeight: 44)
                        }
                        .buttonStyle(.bordered)
                        .tint(.white)
                        .foregroundStyle(.white)
                        .disabled(viewModel.isBusy)

                        Button {
                            viewModel.signInWithTwitter()
                        } label: {
                            HStack(spacing: 6) {
                                AuthProviderMark(kind: .x)
                                Text("Twitter/X")
                                    .fontWeight(.semibold)
                            }
                            .frame(maxWidth: .infinity, minHeight: 44)
                        }
                        .buttonStyle(.bordered)
                        .tint(.white)
                        .foregroundStyle(.white)
                        .disabled(viewModel.isBusy)
                    }

                    HStack(spacing: 8) {
                        Button {
                            viewModel.signInWithYahoo()
                        } label: {
                            HStack(spacing: 6) {
                                AuthProviderMark(kind: .yahoo)
                                Text("Yahoo")
                                    .fontWeight(.semibold)
                            }
                            .frame(maxWidth: .infinity, minHeight: 44)
                        }
                        .buttonStyle(.bordered)
                        .tint(.white)
                        .foregroundStyle(.white)
                        .disabled(viewModel.isBusy)

                        Button {
                            viewModel.signInWithMicrosoft()
                        } label: {
                            HStack(spacing: 6) {
                                AuthProviderMark(kind: .microsoft)
                                Text("Microsoft")
                                    .fontWeight(.semibold)
                            }
                            .frame(maxWidth: .infinity, minHeight: 44)
                        }
                        .buttonStyle(.bordered)
                        .tint(.white)
                        .foregroundStyle(.white)
                        .disabled(viewModel.isBusy)
                    }

#if canImport(AuthenticationServices)
                    SignInWithAppleButton(.signIn) { _ in
                        guard let presenter = QuanturaWebView.Coordinator.topViewController(),
                              let window = presenter.view.window else {
                            viewModel.presentGate(trigger: "missing_apple_anchor")
                            return
                        }
                        viewModel.signInWithApple(presenterAnchor: window)
                    } onCompletion: { _ in
                        // Completion is handled in the native delegate callback.
                    }
                    .frame(height: 52)
                    .clipShape(RoundedRectangle(cornerRadius: 12))
                    .disabled(viewModel.isBusy)
#endif

                    Button {
                        viewModel.openEmailSheet()
                    } label: {
                        HStack(spacing: 10) {
                            AuthProviderMark(kind: .email)
                            Text("Continue with Email")
                                .fontWeight(.semibold)
                        }
                        .frame(maxWidth: .infinity, minHeight: 52)
                    }
                    .buttonStyle(.bordered)
                    .tint(.white)
                    .foregroundStyle(.white)
                    .disabled(viewModel.isBusy)
                }
                .padding(.horizontal, 24)

                Text("Your data stays encrypted in transit. Quantura does not sell personal data.")
                    .font(.footnote)
                    .foregroundStyle(quanturaInk.opacity(0.72))
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 30)

                Button("Not now") {
                    viewModel.continueAnonymouslyForNow()
                }
                .buttonStyle(.plain)
                .foregroundStyle(quanturaInk.opacity(0.88))
                .font(.system(size: 15, weight: .medium))
                .disabled(viewModel.isBusy)

                if !viewModel.errorText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    Text(viewModel.errorText)
                        .font(.footnote)
                        .foregroundStyle(Color(red: 0.70, green: 0.15, blue: 0.12))
                        .multilineTextAlignment(.center)
                        .padding(.horizontal, 24)
                }

                Spacer()
            }
            .padding(.vertical, 26)

            if viewModel.isBusy {
                ZStack {
                    Color.black.opacity(0.24).ignoresSafeArea()
                    ProgressView("Signing in…")
                        .padding(18)
                        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 14))
                }
            }
        }
        .sheet(isPresented: $viewModel.isEmailSheetVisible) {
            EmailAuthSheet(viewModel: viewModel)
                .presentationDetents([.medium])
        }
    }
}

private enum AuthProviderMarkKind {
    case google
    case github
    case x
    case yahoo
    case microsoft
    case email
}

private struct AuthProviderMark: View {
    let kind: AuthProviderMarkKind

    var body: some View {
        switch kind {
        case .google:
            GoogleProviderMark()
        case .microsoft:
            MicrosoftProviderMark()
        case .github:
            SimpleProviderMark(label: "GH", background: Color(red: 0.07, green: 0.10, blue: 0.16), foreground: .white)
        case .x:
            SimpleProviderMark(label: "X", background: .black, foreground: .white)
        case .yahoo:
            SimpleProviderMark(label: "Y!", background: Color(red: 0.37, green: 0.00, blue: 0.82), foreground: .white)
        case .email:
            SimpleProviderMark(label: "@", background: Color(red: 1.0, green: 0.48, blue: 0.10), foreground: .white)
        }
    }
}

private struct GoogleProviderMark: View {
    var body: some View {
        ZStack {
            Circle()
                .fill(.white)
                .frame(width: 24, height: 24)

            ZStack {
                Circle()
                    .trim(from: 0.84, to: 1.0)
                    .stroke(Color(red: 0.26, green: 0.52, blue: 0.96), style: StrokeStyle(lineWidth: 3.0, lineCap: .round))
                    .rotationEffect(.degrees(16))
                Circle()
                    .trim(from: 0.03, to: 0.18)
                    .stroke(Color(red: 0.92, green: 0.26, blue: 0.21), style: StrokeStyle(lineWidth: 3.0, lineCap: .round))
                    .rotationEffect(.degrees(16))
                Circle()
                    .trim(from: 0.18, to: 0.41)
                    .stroke(Color(red: 0.98, green: 0.74, blue: 0.02), style: StrokeStyle(lineWidth: 3.0, lineCap: .round))
                    .rotationEffect(.degrees(16))
                Circle()
                    .trim(from: 0.41, to: 0.74)
                    .stroke(Color(red: 0.20, green: 0.66, blue: 0.33), style: StrokeStyle(lineWidth: 3.0, lineCap: .round))
                    .rotationEffect(.degrees(16))

                Rectangle()
                    .fill(Color(red: 0.26, green: 0.52, blue: 0.96))
                    .frame(width: 7, height: 3)
                    .offset(x: 3.5, y: 0.4)
            }
            .frame(width: 16, height: 16)
        }
    }
}

private struct MicrosoftProviderMark: View {
    var body: some View {
        ZStack {
            Circle()
                .fill(.white)
                .frame(width: 24, height: 24)

            VStack(spacing: 2) {
                HStack(spacing: 2) {
                    RoundedRectangle(cornerRadius: 1)
                        .fill(Color(red: 0.95, green: 0.31, blue: 0.13))
                        .frame(width: 6, height: 6)
                    RoundedRectangle(cornerRadius: 1)
                        .fill(Color(red: 0.50, green: 0.73, blue: 0.00))
                        .frame(width: 6, height: 6)
                }
                HStack(spacing: 2) {
                    RoundedRectangle(cornerRadius: 1)
                        .fill(Color(red: 0.00, green: 0.64, blue: 0.94))
                        .frame(width: 6, height: 6)
                    RoundedRectangle(cornerRadius: 1)
                        .fill(Color(red: 1.00, green: 0.73, blue: 0.00))
                        .frame(width: 6, height: 6)
                }
            }
        }
    }
}

private struct SimpleProviderMark: View {
    let label: String
    let background: Color
    let foreground: Color

    var body: some View {
        ZStack {
            Circle()
                .fill(background)
                .frame(width: 24, height: 24)
            Text(label)
                .font(.system(size: 10, weight: .bold, design: .rounded))
                .foregroundStyle(foreground)
        }
    }
}

private struct EmailAuthSheet: View {
    @ObservedObject var viewModel: AuthGateViewModel
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            VStack(spacing: 14) {
                Picker("Mode", selection: Binding(
                    get: { viewModel.emailAuthMode },
                    set: { viewModel.emailAuthMode = $0 }
                )) {
                    Text("Sign in").tag(EmailAuthMode.signIn)
                    Text("Create").tag(EmailAuthMode.signUp)
                }
                .pickerStyle(.segmented)

                TextField("Email", text: $viewModel.emailAddress)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled(true)
                    .keyboardType(.emailAddress)
                    .padding(12)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 10))
                if let error = viewModel.emailInlineError {
                    Text(error)
                        .font(.caption)
                        .foregroundStyle(.red)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }

                if viewModel.emailAuthMode == .signUp {
                    TextField("Username", text: $viewModel.emailUsername)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled(true)
                        .padding(12)
                        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 10))
                    if let error = viewModel.usernameInlineError {
                        Text(error)
                            .font(.caption)
                            .foregroundStyle(.red)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }

                SecureField("Password", text: $viewModel.emailPassword)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled(true)
                    .padding(12)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 10))
                if let error = viewModel.passwordInlineError {
                    Text(error)
                        .font(.caption)
                        .foregroundStyle(.red)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }

                if viewModel.emailAuthMode == .signUp {
                    SecureField("Confirm password", text: $viewModel.emailConfirmPassword)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled(true)
                        .padding(12)
                        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 10))
                    if let error = viewModel.confirmPasswordInlineError {
                        Text(error)
                            .font(.caption)
                            .foregroundStyle(.red)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }

                if !viewModel.errorText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    Text(viewModel.errorText)
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(.red)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }

                Button(viewModel.emailAuthMode == .signUp ? "Create account" : "Continue") {
                    viewModel.submitEmailAuth()
                }
                .buttonStyle(.borderedProminent)
                .frame(maxWidth: .infinity, minHeight: 48)
                .disabled(viewModel.isBusy)

                Text(viewModel.emailAuthMode == .signUp
                     ? "Create an account with email, username, password, and confirmation. Existing emails must use their current provider."
                     : "Sign in with your existing email and password. Switch to Create to open a new account.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.top, 4)

                Spacer()
            }
            .padding(16)
            .navigationTitle("Continue with Email")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button("Close") {
                        viewModel.dismissEmailSheet()
                        dismiss()
                    }
                }
            }
        }
    }
}
