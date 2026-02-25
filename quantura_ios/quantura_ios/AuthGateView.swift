import SwiftUI
#if canImport(AuthenticationServices)
import AuthenticationServices
#endif

struct AuthGateView: View {
    @ObservedObject var viewModel: AuthGateViewModel

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color(red: 0.04, green: 0.10, blue: 0.26),
                    Color(red: 0.06, green: 0.19, blue: 0.40),
                    Color(red: 0.02, green: 0.08, blue: 0.20),
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            VStack(spacing: 22) {
                Spacer()

                VStack(spacing: 12) {
                    Image(systemName: "chart.line.uptrend.xyaxis.circle.fill")
                        .resizable()
                        .scaledToFit()
                        .frame(width: 76, height: 76)
                        .foregroundStyle(.white, Color(red: 0.56, green: 0.83, blue: 0.98))

                    Text("Sign in to Quantura")
                        .font(.system(size: 30, weight: .bold, design: .rounded))
                        .foregroundStyle(.white)

                    Text("Sync forecasts, save screeners, and unlock personalized market alerts.")
                        .font(.system(size: 15, weight: .medium))
                        .multilineTextAlignment(.center)
                        .foregroundStyle(.white.opacity(0.84))
                        .padding(.horizontal, 28)
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
                            Image(systemName: "globe")
                            Text("Continue with Google")
                                .fontWeight(.semibold)
                        }
                        .frame(maxWidth: .infinity, minHeight: 52)
                    }
                    .buttonStyle(.borderedProminent)
                    .tint(Color.white)
                    .foregroundStyle(Color.black)
                    .disabled(viewModel.isBusy)

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
                            Image(systemName: "envelope.fill")
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
                    .foregroundStyle(.white.opacity(0.75))
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 30)

                Button("Not now") {
                    viewModel.continueAnonymouslyForNow()
                }
                .buttonStyle(.plain)
                .foregroundStyle(.white.opacity(0.88))
                .font(.system(size: 15, weight: .medium))
                .disabled(viewModel.isBusy)

                if !viewModel.errorText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    Text(viewModel.errorText)
                        .font(.footnote)
                        .foregroundStyle(Color(red: 1.0, green: 0.75, blue: 0.75))
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

private struct EmailAuthSheet: View {
    @ObservedObject var viewModel: AuthGateViewModel
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            VStack(spacing: 14) {
                TextField("Email", text: $viewModel.emailAddress)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled(true)
                    .keyboardType(.emailAddress)
                    .padding(12)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 10))

                SecureField("Password", text: $viewModel.emailPassword)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled(true)
                    .padding(12)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 10))

                Button("Continue") {
                    viewModel.signInWithEmail()
                }
                .buttonStyle(.borderedProminent)
                .frame(maxWidth: .infinity, minHeight: 48)
                .disabled(viewModel.isBusy)

                Text("If this email has no account yet, Quantura will create it and link it to your current app session.")
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
                        dismiss()
                    }
                }
            }
        }
    }
}
