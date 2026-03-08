import type { NativeFeedAdBridgePayload, NativePlatform } from "./types";

declare global {
  interface Window {
    __QUANTURA_NATIVE_APP__?: boolean;
    __QUANTURA_NATIVE_PLATFORM__?: string;
    QuanturaBridge?: { postMessage?: (value: string) => void };
    quanturaAuth?: { postMessage?: (value: string) => void };
    ReactNativeWebView?: { postMessage?: (value: string) => void };
    webkit?: {
      messageHandlers?: {
        QuanturaBridge?: { postMessage?: (value: unknown) => void };
        quanturaAuth?: { postMessage?: (value: unknown) => void };
      };
    };
  }
}

const BRIDGE_EVENT_NAME = "quantura:native-feed-ad";

const safeJsonParse = <T>(raw: string, fallback: T): T => {
  try {
    return JSON.parse(raw) as T;
  } catch (_error) {
    return fallback;
  }
};

export const getNativePlatform = (): NativePlatform | null => {
  try {
    const explicit = String(window.__QUANTURA_NATIVE_PLATFORM__ || "").trim().toLowerCase();
    if (explicit === "ios" || explicit === "android") return explicit;
    if (window.quanturaAuth?.postMessage || window.QuanturaBridge?.postMessage) return "android";
    if (window.webkit?.messageHandlers?.quanturaAuth?.postMessage || window.webkit?.messageHandlers?.QuanturaBridge?.postMessage) return "ios";
  } catch (_error) {
    return null;
  }
  return null;
};

export const isNativeRuntime = (): boolean => Boolean(window.__QUANTURA_NATIVE_APP__ || getNativePlatform());

const postStringified = (message: Record<string, unknown>): string => JSON.stringify(message);

export const sendNativeBridgeMessage = (payload: Record<string, unknown>): boolean => {
  const message = postStringified(payload);
  try {
    if (window.QuanturaBridge?.postMessage) {
      window.QuanturaBridge.postMessage(message);
      return true;
    }
  } catch (_error) {
    // noop
  }

  try {
    const iosBridge = window.webkit?.messageHandlers?.QuanturaBridge?.postMessage;
    if (iosBridge) {
      iosBridge(payload);
      return true;
    }
  } catch (_error) {
    // noop
  }

  try {
    if (window.quanturaAuth?.postMessage) {
      window.quanturaAuth.postMessage(message);
      return true;
    }
  } catch (_error) {
    // noop
  }

  try {
    const iosAuth = window.webkit?.messageHandlers?.quanturaAuth?.postMessage;
    if (iosAuth) {
      iosAuth(payload);
      return true;
    }
  } catch (_error) {
    // noop
  }

  try {
    if (window.ReactNativeWebView?.postMessage) {
      window.ReactNativeWebView.postMessage(message);
      return true;
    }
  } catch (_error) {
    // noop
  }

  return false;
};

const normalizePayload = (raw: unknown): NativeFeedAdBridgePayload => {
  if (typeof raw === "string") {
    return safeJsonParse<NativeFeedAdBridgePayload>(raw, {});
  }
  if (raw && typeof raw === "object") {
    return raw as NativeFeedAdBridgePayload;
  }
  return {};
};

export const reportNativeFeedEvent = (
  eventName: "nativeFeedAdImpression" | "nativeFeedAdClick",
  payload: { slotId: string; placement: string; adUnitId?: string }
): void => {
  if (!isNativeRuntime()) return;
  sendNativeBridgeMessage({
    action: eventName,
    slotId: payload.slotId,
    placement: payload.placement,
    adUnitId: payload.adUnitId || "",
  });
};

export const requestNativeFeedAd = ({
  slotId,
  placement,
  variant = "nativeAdvanced",
  timeoutMs = 12000,
}: {
  slotId: string;
  placement: string;
  variant?: string;
  timeoutMs?: number;
}): Promise<NativeFeedAdBridgePayload> =>
  new Promise((resolve, reject) => {
    if (!isNativeRuntime()) {
      reject(new Error("native_runtime_unavailable"));
      return;
    }

    const cleanSlotId = String(slotId || "").trim();
    const cleanPlacement = String(placement || "inline").trim() || "inline";
    if (!cleanSlotId) {
      reject(new Error("slot_id_missing"));
      return;
    }

    const onEvent = (event: Event) => {
      const custom = event as CustomEvent;
      const detail = normalizePayload(custom.detail);
      const detailSlot = String(detail.slotId || "").trim();
      if (!detailSlot || detailSlot !== cleanSlotId) return;
      window.removeEventListener(BRIDGE_EVENT_NAME, onEvent as EventListener);
      window.clearTimeout(timeoutHandle);
      if (detail.ok === false) {
        reject(new Error(String(detail.error || "native_feed_ad_failed")));
        return;
      }
      resolve(detail);
    };

    window.addEventListener(BRIDGE_EVENT_NAME, onEvent as EventListener);

    const timeoutHandle = window.setTimeout(() => {
      window.removeEventListener(BRIDGE_EVENT_NAME, onEvent as EventListener);
      reject(new Error("native_feed_ad_timeout"));
    }, Math.max(2500, Number(timeoutMs) || 12000));

    const sent = sendNativeBridgeMessage({
      action: "requestNativeFeedAd",
      slotId: cleanSlotId,
      placement: cleanPlacement,
      variant,
    });

    if (!sent) {
      window.clearTimeout(timeoutHandle);
      window.removeEventListener(BRIDGE_EVENT_NAME, onEvent as EventListener);
      reject(new Error("native_bridge_unavailable"));
    }
  });
