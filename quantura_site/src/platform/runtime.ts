export type NativePlatform = "ios" | "android" | null;

declare global {
  interface Window {
    __QUANTURA_NATIVE_PLATFORM__?: string;
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

export const isNativeApp = (): boolean => Boolean(getNativePlatform());

export const isInstalledPwa = (): boolean => {
  const standaloneMatch = Boolean(window.matchMedia?.("(display-mode: standalone)")?.matches);
  const iosStandalone = navigator.standalone === true;
  return standaloneMatch || iosStandalone;
};
