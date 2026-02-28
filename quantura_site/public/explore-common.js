const API_BASE = String(window.QUANTURA_EXPLORE_API_BASE || "").trim().replace(/\/$/, "");

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
