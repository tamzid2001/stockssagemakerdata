import { getNativePlatform, isInstalledPwa, isNativeApp } from "../platform/runtime";

export type AuthProvider = "google" | "facebook" | "twitter" | "github" | "microsoft" | "yahoo";

export type AuthMode = "native" | "redirect" | "popup";

export const resolveAuthMode = (provider: AuthProvider): AuthMode => {
  if (isNativeApp() && provider === "google") return "native";
  if (isNativeApp() || isInstalledPwa()) return "redirect";
  return "popup";
};

export const exchangeNativeIdToken = async (idToken: string): Promise<string> => {
  const response = await fetch("/api/auth/exchange", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${idToken}`,
    },
    credentials: "same-origin",
    body: JSON.stringify({}),
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(String(payload?.error || payload?.message || "Native token exchange failed."));
  }

  const customToken = String(payload?.customToken || "").trim();
  if (!customToken) throw new Error("Server did not return a custom token.");
  return customToken;
};

export const describeRuntime = (): string => {
  if (isNativeApp()) return `native:${getNativePlatform() ?? "unknown"}`;
  if (isInstalledPwa()) return "pwa";
  return "web";
};
