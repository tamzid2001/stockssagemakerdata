const API_BASE = String(window.QUANTURA_EXPLORE_API_BASE || "").trim().replace(/\/$/, "");
const nativeFeedAdPending = new Map();
let nativeFeedListenerBound = false;
let nativeFeedSlotSequence = 0;

export function apiPath(path) {
  const normalized = path.startsWith("/") ? path : `/${path}`;
  if (API_BASE) {
    if (normalized.startsWith("/api/")) return `${API_BASE}${normalized}`;
    if (normalized === "/api") return `${API_BASE}${normalized}`;
    return `${API_BASE}/api${normalized}`;
  }
  if (normalized.startsWith("/api/")) return normalized;
  if (normalized === "/api") return normalized;
  return `/api${normalized}`;
}

export async function waitForFirebase(timeoutMs = 12000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (window.firebase?.apps?.length && window.firebase.auth) {
      return window.firebase;
    }
    await new Promise((resolve) => setTimeout(resolve, 40));
  }
  throw new Error("Firebase SDK not available on this page.");
}

export function track(eventName, params = {}) {
  try {
    if (typeof window.gtag === "function") {
      window.gtag("event", eventName, params);
    }
  } catch {
    // Ignore analytics errors.
  }
}

function detectNativePlatform() {
  try {
    const explicit = String(window.__QUANTURA_NATIVE_PLATFORM__ || "").trim().toLowerCase();
    if (explicit === "ios" || explicit === "android") return explicit;
    if (window.QuanturaBridge?.postMessage) return "android";
    if (window.webkit?.messageHandlers?.QuanturaBridge?.postMessage) return "ios";
  } catch {
    return "";
  }
  return "";
}

function postNativeBridgeMessage(payload = {}) {
  try {
    if (window.QuanturaBridge?.postMessage) {
      window.QuanturaBridge.postMessage(JSON.stringify(payload));
      return true;
    }
  } catch {
    // Fall through to iOS handler.
  }

  try {
    const iosHandler = window.webkit?.messageHandlers?.QuanturaBridge?.postMessage;
    if (iosHandler) {
      iosHandler(payload);
      return true;
    }
  } catch {
    return false;
  }

  return false;
}

function bindNativeFeedAdListener() {
  if (nativeFeedListenerBound) return;
  nativeFeedListenerBound = true;
  window.addEventListener("quantura:native-feed-ad", (event) => {
    const detail = event?.detail && typeof event.detail === "object" ? event.detail : {};
    const slotId = String(detail.slotId || "").trim();
    if (!slotId) return;
    const pending = nativeFeedAdPending.get(slotId);
    if (!pending) return;
    nativeFeedAdPending.delete(slotId);
    if (detail.ok === false) {
      const message = String(detail.error || "Native ad failed to load.").trim();
      pending.reject(new Error(message));
      return;
    }
    pending.resolve(detail);
  });
}

function nextNativeSlotId(prefix = "native-slot") {
  nativeFeedSlotSequence += 1;
  return `${prefix}-${Date.now()}-${nativeFeedSlotSequence}`;
}

export function isNativeRuntime() {
  return Boolean(window.__QUANTURA_NATIVE_APP__ || detectNativePlatform());
}

export function getNativeFeedAdRules() {
  const defaults = {
    feedStart: 6,
    feedInterval: 8,
    pageMidpoint: 0.55,
  };
  const raw = window.__QUANTURA_NATIVE_AD_RULES__ && typeof window.__QUANTURA_NATIVE_AD_RULES__ === "object"
    ? window.__QUANTURA_NATIVE_AD_RULES__
    : {};
  const feedStart = Number(raw.feedStart);
  const feedInterval = Number(raw.feedInterval);
  const pageMidpoint = Number(raw.pageMidpoint);
  return {
    feedStart: Number.isFinite(feedStart) ? Math.max(3, Math.min(20, Math.floor(feedStart))) : defaults.feedStart,
    feedInterval: Number.isFinite(feedInterval) ? Math.max(3, Math.min(20, Math.floor(feedInterval))) : defaults.feedInterval,
    pageMidpoint: Number.isFinite(pageMidpoint) ? Math.max(0.2, Math.min(0.9, pageMidpoint)) : defaults.pageMidpoint,
  };
}

export async function requestNativeFeedAd({
  slotId = "",
  placement = "feed",
  variant = "nativeAdvanced",
  timeoutMs = 12000,
} = {}) {
  if (!isNativeRuntime()) {
    throw new Error("Native runtime unavailable.");
  }
  bindNativeFeedAdListener();

  const resolvedSlotId = String(slotId || nextNativeSlotId("native-feed")).trim();
  if (!resolvedSlotId) {
    throw new Error("Slot id is required.");
  }

  return new Promise((resolve, reject) => {
    const timeoutHandle = window.setTimeout(() => {
      nativeFeedAdPending.delete(resolvedSlotId);
      reject(new Error("Native ad request timed out."));
    }, Math.max(2000, Number(timeoutMs) || 12000));

    nativeFeedAdPending.set(resolvedSlotId, {
      resolve: (detail) => {
        clearTimeout(timeoutHandle);
        resolve(detail);
      },
      reject: (error) => {
        clearTimeout(timeoutHandle);
        reject(error);
      },
    });

    const sent = postNativeBridgeMessage({
      action: "requestNativeFeedAd",
      slotId: resolvedSlotId,
      placement: String(placement || "feed").trim(),
      variant: String(variant || "nativeAdvanced").trim(),
    });

    if (!sent) {
      clearTimeout(timeoutHandle);
      nativeFeedAdPending.delete(resolvedSlotId);
      reject(new Error("Native bridge unavailable."));
    }
  });
}

function reportNativeFeedEvent(action, payload = {}) {
  if (!isNativeRuntime()) return false;
  return postNativeBridgeMessage({
    action,
    slotId: String(payload.slotId || "").trim(),
    placement: String(payload.placement || "").trim(),
    adUnitId: String(payload.adUnitId || "").trim(),
  });
}

export function reportNativeFeedAdImpression(payload = {}) {
  return reportNativeFeedEvent("nativeFeedAdImpression", payload);
}

export function reportNativeFeedAdClick(payload = {}) {
  return reportNativeFeedEvent("nativeFeedAdClick", payload);
}

export async function createApiClient(getAuthToken) {
  const request = async (method, path, body) => {
    const token = typeof getAuthToken === "function" ? await getAuthToken() : "";
    const headers = {
      "Content-Type": "application/json",
    };
    if (token) headers.Authorization = `Bearer ${token}`;

    const res = await fetch(apiPath(path), {
      method,
      headers,
      body: body == null ? undefined : JSON.stringify(body),
    });

    const text = await res.text();
    let payload = {};
    try {
      payload = text ? JSON.parse(text) : {};
    } catch {
      payload = {};
    }

    if (!res.ok) {
      const message = payload.error || payload.message || `Request failed (${res.status})`;
      const error = new Error(String(message));
      error.status = res.status;
      throw error;
    }

    return payload;
  };

  return {
    get: (path) => request("GET", path),
    post: (path, body = {}) => request("POST", path, body),
    patch: (path, body = {}) => request("PATCH", path, body),
    delete: (path, body = null) => request("DELETE", path, body),
  };
}

export function formatRelativeTime(isoOrMs) {
  const date = typeof isoOrMs === "number" ? new Date(isoOrMs) : new Date(isoOrMs || Date.now());
  const diffMs = Date.now() - date.getTime();
  const sec = Math.max(1, Math.floor(diffMs / 1000));
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  if (day < 7) return `${day}d ago`;
  return date.toLocaleDateString();
}

export function escapeHtml(value) {
  const raw = String(value ?? "");
  return raw
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function normalizeTicker(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9.\-]/g, "")
    .slice(0, 12);
}

export async function initAuth(onUserChanged) {
  const firebase = await waitForFirebase();
  const auth = firebase.auth();

  auth.onAuthStateChanged(async (user) => {
    if (!user) {
      try {
        await auth.signInAnonymously();
      } catch {
        // Ignore; UI can still run read-only.
      }
      onUserChanged?.(auth.currentUser || null);
      return;
    }
    onUserChanged?.(user);
  });

  return {
    auth,
    async signInWithGoogle() {
      const provider = new firebase.auth.GoogleAuthProvider();
      await auth.signInWithPopup(provider);
    },
    async signOut() {
      await auth.signOut();
      await auth.signInAnonymously().catch(() => undefined);
    },
    async getAuthToken() {
      const user = auth.currentUser;
      if (!user) return "";
      return user.getIdToken();
    },
  };
}
