export type NativePlatform = "ios" | "android" | null;

declare global {
  interface Window {
    __QUANTURA_NATIVE_PLATFORM__?: string;
    Capacitor?: {
      isNativePlatform?: () => boolean;
      getPlatform?: () => string;
    };
    QuanturaBridge?: { postMessage?: (payload: string) => void };
    webkit?: {
      messageHandlers?: {
        QuanturaBridge?: {
          postMessage?: (payload: unknown) => void;
        };
      };
    };
  }

  interface Navigator {
    standalone?: boolean;
  }
}

export const getNativePlatform = (): NativePlatform => {
  const explicit = String(window.__QUANTURA_NATIVE_PLATFORM__ || "").trim().toLowerCase();
  if (explicit === "ios" || explicit === "android") return explicit;

  try {
    if (window.Capacitor?.isNativePlatform?.() === true) {
      const platform = String(window.Capacitor.getPlatform?.() || "").trim().toLowerCase();
      if (platform === "ios" || platform === "android") return platform;
    }
  } catch {
    // no-op
  }

  const ua = String(navigator.userAgent || "").toLowerCase();
  if (ua.includes("quanturaandroidapp")) return "android";
  if (ua.includes("quanturaiosapp")) return "ios";

  try {
    if (window.QuanturaBridge?.postMessage) return "android";
  } catch {
    // no-op
  }
  try {
    if (window.webkit?.messageHandlers?.QuanturaBridge?.postMessage) return "ios";
  } catch {
    // no-op
  }

  return null;
};

export const isNativeApp = (): boolean => {
  try {
    if (window.Capacitor?.isNativePlatform?.() === true) return true;
  } catch {
    // no-op
  }
  return Boolean(getNativePlatform());
};

export const isInstalledPwa = (): boolean => {
  const standaloneMatch = Boolean(window.matchMedia?.("(display-mode: standalone)")?.matches);
  const iosStandalone = navigator.standalone === true;
  return standaloneMatch || iosStandalone;
};
