import cors from "cors";
import express, { Request, Response } from "express";
import admin from "firebase-admin";
import { GoogleAuth } from "google-auth-library";

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();
const auth = admin.auth();
const messaging = admin.messaging();

const app = express();
app.disable("x-powered-by");
app.use(cors({ origin: true }));
app.use(express.json({ limit: "1mb" }));

type PostType = "forecast" | "backtest" | "agent" | "screener";

type Visibility = "public" | "unlisted" | "deleted";

type PostCounts = {
  likes: number;
  comments: number;
  reposts: number;
  shares: number;
  reports: number;
};

type PostDoc = {
  id: string;
  type: PostType;
  authorUid: string;
  authorHandle: string;
  authorPhotoURL: string;
  title: string;
  caption: string;
  tickers: string[];
  tags: string[];
  preview: {
    kind: "image" | "summary";
    imageUrl?: string;
    metrics?: Record<string, string | number>;
  };
  targetUrl: string;
  visibility: Visibility;
  createdAt: admin.firestore.Timestamp;
  updatedAt: admin.firestore.Timestamp;
  counts: PostCounts;
  score: number;
  lastEngagedAt: admin.firestore.Timestamp;
};

type ExploreCursor = {
  id: string;
  createdAtMs: number;
  score?: number;
};

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 40;
const PUBLIC_ORIGIN = asString(process.env.PUBLIC_ORIGIN, "https://quantura-e2e3d.web.app").replace(/\/$/, "");
const ADMIN_EMAIL = "tamzid257@gmail.com";
const MODEL_COUNCIL_RESPONSE_COLLECTION = "model_council_responses";
const OPENAI_API_KEY = asString(process.env.OPENAI_API_KEY).trim();
const GEMINI_API_KEY = asString(process.env.GEMINI_API_KEY).trim();
const MISTRAL_API_KEY = asString(process.env.MISTRAL_API_KEY).trim();
const PERPLEXITY_API_KEY = asString(process.env.PERPLEXITY_API_KEY).trim();
const MODEL_COUNCIL_OTHER_API_KEY = asString(process.env.MODEL_COUNCIL_OTHER_API_KEY).trim();
const MODEL_COUNCIL_OTHER_BASE_URL = asString(process.env.MODEL_COUNCIL_OTHER_BASE_URL).trim().replace(/\/$/, "");
const NOTIFICATION_REWRITE_MODEL = asString(process.env.NOTIFICATION_REWRITE_MODEL, "gpt-4o-mini").trim();
const FMP_API_KEY = asString(process.env.FMP_API_KEY).trim();
const PLAY_INTEGRITY_ANDROID_PACKAGE = asString(process.env.PLAY_INTEGRITY_ANDROID_PACKAGE).trim();
const REQUIRE_PLAY_INTEGRITY = asBoolean(process.env.REQUIRE_PLAY_INTEGRITY, false);
const IOS_IAP_WEBHOOK_SECRET = asString(process.env.IOS_IAP_WEBHOOK_SECRET).trim();
const APPLE_NOTIFICATIONS_WEBHOOK_SECRET = asString(process.env.APPLE_NOTIFICATIONS_WEBHOOK_SECRET).trim();
const ADMOB_SSV_WEBHOOK_SECRET = asString(process.env.ADMOB_SSV_WEBHOOK_SECRET).trim();
const DEFAULT_LLM_MODEL = asString(process.env.DEFAULT_LLM_MODEL, "gpt-5-mini").trim();
const LLM_TIMEOUT_MS = Math.max(5000, Math.min(120000, Math.floor(asFinite(process.env.LLM_TIMEOUT_MS, 30000))));
const PROMO_ID = asString(process.env.PROMO_ID, "quantura_generic_50_off").trim();
const PROMO_CODE = asString(process.env.PROMO_CODE, "QUANTURA50").trim().toUpperCase();
const PROMO_DISCOUNT_PERCENT = Math.max(1, Math.min(95, asFinite(process.env.PROMO_DISCOUNT_PERCENT, 50)));
const PROMO_ACTIVE = asBoolean(process.env.PROMO_ACTIVE, true);
const PROMO_DURATION_DAYS = Math.max(1, Math.min(120, Math.floor(asFinite(process.env.PROMO_DURATION_DAYS, 30))));
const GAMMA_API_BASE = "https://gamma-api.polymarket.com";
const POLYMARKET_CACHE_TTL_MS = 10 * 60 * 1000;
const POLYMARKET_CACHE_MAX_ENTRIES = 160;
const PROMO_START_MS = (() => {
  const raw = asString(process.env.PROMO_START_AT).trim();
  if (!raw) return Date.now() - 24 * 60 * 60 * 1000;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : Date.now() - 24 * 60 * 60 * 1000;
})();
const PROMO_END_MS = (() => {
  const raw = asString(process.env.PROMO_END_AT).trim();
  if (raw) {
    const parsed = Date.parse(raw);
    if (Number.isFinite(parsed)) return parsed;
  }
  return PROMO_START_MS + PROMO_DURATION_DAYS * 24 * 60 * 60 * 1000;
})();

type SavedItemType = "forecast" | "screener" | "model_council" | "post";
type MyRequestType = "forecast" | "screener" | "indicator" | "modelCouncil";
type MyRequestShareVisibility = "private" | "unlisted" | "public";

type SystemFolderConfig = {
  id: string;
  displayName: string;
  flag: "liked" | "reposted" | "saved" | "shared";
};

type NotificationCategory = "watchlist" | "explore" | "earnings" | "ipo" | "daily" | "weekly" | "inactive";

type NotificationPrefs = {
  global: boolean;
  following: boolean;
  tickers: boolean;
  watchlist: boolean;
  explore: boolean;
  earnings: boolean;
  ipos: boolean;
  daily: boolean;
  weekly: boolean;
  inactiveHidden: boolean;
};

type PolymarketTopOutcome = {
  label: string;
  prob: number;
};

type PolymarketMarketRecord = {
  id: string;
  question: string;
  slug?: string;
  endDate?: string;
  category?: string;
  image?: string;
  icon?: string;
  volumeUsd?: number;
  liquidityUsd?: number;
  outcomes: string[];
  outcomePrices: number[];
  isBinary: boolean;
  yesProb?: number;
  topOutcomes: PolymarketTopOutcome[];
  closed: boolean;
  active: boolean;
};

type PolymarketEventRecord = {
  id: string;
  title: string;
  slug?: string;
  ticker?: string;
  markets: PolymarketMarketRecord[];
};

type PolymarketSearchResponse = {
  query: string;
  fetchedAt: string;
  events: PolymarketEventRecord[];
};

type PolymarketCacheEntry = {
  expiresAtMs: number;
  value: PolymarketSearchResponse;
};

const SYSTEM_FOLDERS: SystemFolderConfig[] = [
  { id: "liked-posts", displayName: "Liked posts", flag: "liked" },
  { id: "reposted-posts", displayName: "Reposted posts", flag: "reposted" },
  { id: "saved-posts", displayName: "Saved posts", flag: "saved" },
  { id: "shared-posts", displayName: "Shared posts", flag: "shared" },
];

const DEFAULT_NOTIFICATION_PREFS: NotificationPrefs = {
  global: true,
  following: true,
  tickers: true,
  watchlist: true,
  explore: true,
  earnings: true,
  ipos: true,
  daily: true,
  weekly: true,
  inactiveHidden: true,
};

const MY_REQUEST_TYPE_SET = new Set<MyRequestType>(["forecast", "screener", "indicator", "modelCouncil"]);
const MY_REQUEST_SHARE_VISIBILITY_SET = new Set<MyRequestShareVisibility>(["private", "unlisted", "public"]);
const MY_REQUEST_TYPE_LABEL: Record<MyRequestType, string> = {
  forecast: "Forecast",
  screener: "Screener",
  indicator: "Indicator",
  modelCouncil: "Model Council",
};

const ROUTES = express.Router();
const polymarketCache = new Map<string, PolymarketCacheEntry>();
const PLAY_INTEGRITY_AUTH = new GoogleAuth({
  scopes: ["https://www.googleapis.com/auth/playintegrity"],
});

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asFinite(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function asBoolean(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return fallback;
}

function requestIpAddress(req: Request): string {
  const forwarded = asString(req.headers["x-forwarded-for"]);
  if (forwarded) {
    const first = forwarded.split(",")[0]?.trim() || "";
    if (first) return first.slice(0, 120);
  }
  const real = asString(req.headers["x-real-ip"]);
  if (real) return real.slice(0, 120);
  const socketIp = asString((req.socket as any)?.remoteAddress);
  return socketIp.slice(0, 120);
}

function normalizeTimezone(value: unknown): string {
  return sanitizeText(value, 80).replace(/[^A-Za-z0-9_./+\-]/g, "");
}

function normalizeCoarseLocation(
  value: unknown
): { lat: number | null; lon: number | null; countryCode: string; accuracyM: number | null; capturedAt: string } | null {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const latNum = asFinite(payload.lat, NaN);
  const lonNum = asFinite(payload.lon, NaN);
  const accNum = asFinite(payload.accuracyM, NaN);
  const countryRaw = asString(payload.countryCode).trim().toUpperCase();
  const countryCode = /^[A-Z]{2}$/.test(countryRaw) ? countryRaw : "";
  const captured = asString(payload.capturedAt);
  const capturedAt = Number.isFinite(Date.parse(captured)) ? new Date(captured).toISOString() : new Date().toISOString();
  return {
    lat: Number.isFinite(latNum) ? Number(latNum.toFixed(1)) : null,
    lon: Number.isFinite(lonNum) ? Number(lonNum.toFixed(1)) : null,
    countryCode,
    accuracyM: Number.isFinite(accNum) ? Math.max(0, Math.round(accNum)) : null,
    capturedAt,
  };
}

async function fetchIpDerivedRegion(ipAddress: string): Promise<{ region: string; countryCode: string }> {
  const ip = String(ipAddress || "").trim();
  if (!ip) return { region: "", countryCode: "" };
  const safeIp = ip.replace(/[^0-9a-fA-F:.]/g, "");
  if (!safeIp) return { region: "", countryCode: "" };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 2500);
  try {
    const response = await fetch(`https://ipapi.co/${encodeURIComponent(safeIp)}/json/`, {
      method: "GET",
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    if (!response.ok) return { region: "", countryCode: "" };
    const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const region = sanitizeText(payload.region || payload.region_code || payload.city || "", 80);
    const countryRaw = asString(payload.country_code || payload.country).trim().toUpperCase();
    const countryCode = /^[A-Z]{2}$/.test(countryRaw) ? countryRaw : "";
    return { region, countryCode };
  } catch {
    return { region: "", countryCode: "" };
  } finally {
    clearTimeout(timer);
  }
}

function parseJsonObject(text: string): Record<string, unknown> | null {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") return parsed as Record<string, unknown>;
  } catch {
    // fall through
  }
  const firstBrace = raw.indexOf("{");
  const lastBrace = raw.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const slice = raw.slice(firstBrace, lastBrace + 1);
    try {
      const parsed = JSON.parse(slice);
      if (parsed && typeof parsed === "object") return parsed as Record<string, unknown>;
    } catch {
      return null;
    }
  }
  return null;
}

type NotificationRewriteInput = {
  title: string;
  body: string;
  source: string;
  context?: Record<string, unknown>;
};

async function rewriteNotificationWithLlm(input: NotificationRewriteInput): Promise<{
  title: string;
  body: string;
  nextSteps: string[];
  personalized: boolean;
}> {
  const title = sanitizeText(input.title, 160) || "Quantura update";
  const body = sanitizeText(input.body, 500);
  const fallback = {
    title,
    body: body || "You have a new Quantura notification.",
    nextSteps: [] as string[],
    personalized: false,
  };
  if (!OPENAI_API_KEY) return fallback;

  const timezone = sanitizeText(input.context?.timezone, 80);
  const country = sanitizeText(input.context?.countryCode, 12).toUpperCase();
  const region = sanitizeText(input.context?.region, 80);
  const source = sanitizeText(input.source, 40);

  const prompt = {
    title,
    body,
    source,
    context: {
      timezone,
      countryCode: country,
      region,
    },
    style: "Keep it short and practical. Return JSON only.",
    outputSchema: {
      title: "string (<=90 chars)",
      body: "string (<=180 chars)",
      nextSteps: ["array of 0-3 short suggestions"],
    },
  };

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: NOTIFICATION_REWRITE_MODEL,
        temperature: 0.2,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              "You rewrite notification text for a finance app. Keep language plain and concise. Never mention policy text. Respond as valid JSON.",
          },
          {
            role: "user",
            content: JSON.stringify(prompt),
          },
        ],
      }),
    });
    if (!response.ok) return fallback;
    const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const content = asString((payload?.choices as any)?.[0]?.message?.content);
    const parsed = parseJsonObject(content);
    if (!parsed) return fallback;
    const nextTitle = sanitizeText(parsed.title, 90) || fallback.title;
    const nextBody = sanitizeText(parsed.body, 180) || fallback.body;
    const nextStepsRaw = Array.isArray(parsed.nextSteps) ? parsed.nextSteps : [];
    const nextSteps = nextStepsRaw.map((item) => sanitizeText(item, 90)).filter(Boolean).slice(0, 3);
    return {
      title: nextTitle,
      body: nextBody,
      nextSteps,
      personalized: true,
    };
  } catch {
    return fallback;
  }
}

function parseLimit(input: unknown): number {
  const parsed = Number(input);
  if (!Number.isFinite(parsed)) return DEFAULT_LIMIT;
  return Math.max(1, Math.min(MAX_LIMIT, Math.floor(parsed)));
}

function normalizeTicker(value: unknown): string {
  const raw = asString(value).trim().toUpperCase();
  if (!raw) return "";
  return raw.replace(/[^A-Z0-9.\-]/g, "").slice(0, 12);
}

function normalizeHandle(value: unknown): string {
  return asString(value).trim().toLowerCase().replace(/[^a-z0-9_.-]/g, "").slice(0, 40);
}

function sanitizeText(value: unknown, maxLen = 600): string {
  const raw = asString(value).replace(/\s+/g, " ").trim();
  if (!raw) return "";
  return raw.slice(0, maxLen);
}

function normalizeFolderId(value: unknown): string {
  const raw = asString(value).trim().toLowerCase();
  if (!raw) return "";
  return raw.replace(/[^a-z0-9_-]/g, "-").replace(/-+/g, "-").slice(0, 80);
}

function normalizeSavedItemType(value: unknown): SavedItemType | "" {
  const raw = asString(value).trim().toLowerCase();
  if (raw === "forecast" || raw === "screener" || raw === "model_council" || raw === "post") return raw;
  return "";
}

function normalizeSourceId(value: unknown): string {
  return sanitizeText(value, 220).replace(/[^A-Za-z0-9._:\-]/g, "");
}

function normalizeShareId(value: unknown): string {
  const raw = asString(value).trim();
  if (!raw) return "";
  return /^[A-Za-z0-9_-]{8,220}$/.test(raw) ? raw : "";
}

function normalizeNotificationCategory(value: unknown): NotificationCategory {
  const raw = sanitizeText(value, 40).toLowerCase();
  if (raw === "watchlist") return "watchlist";
  if (raw === "explore") return "explore";
  if (raw === "earnings") return "earnings";
  if (raw === "ipo" || raw === "ipos") return "ipo";
  if (raw === "daily") return "daily";
  if (raw === "weekly") return "weekly";
  if (raw === "inactive" || raw === "inactive_user") return "inactive";
  return "explore";
}

function normalizeNotificationDeepLink(value: unknown): string {
  const raw = sanitizeText(value, 500);
  if (!raw) return "/notifications";
  if (/^https?:\/\//i.test(raw)) return raw;
  const prefixed = raw.startsWith("/") ? raw : `/${raw}`;
  return prefixed.slice(0, 500);
}

function absoluteNotificationLink(deepLink: string): string {
  if (/^https?:\/\//i.test(deepLink)) return deepLink;
  const path = deepLink.startsWith("/") ? deepLink : `/${deepLink}`;
  return `${PUBLIC_ORIGIN}${path}`;
}

function normalizeNotificationPrefs(
  input: Record<string, unknown>,
  existing: Record<string, unknown> = {}
): NotificationPrefs {
  const merged = {
    ...DEFAULT_NOTIFICATION_PREFS,
    ...existing,
  } as Record<string, unknown>;
  const resolved = { ...merged, ...input } as Record<string, unknown>;
  return {
    global: asBoolean(resolved.global, DEFAULT_NOTIFICATION_PREFS.global),
    following: asBoolean(resolved.following, DEFAULT_NOTIFICATION_PREFS.following),
    tickers: asBoolean(resolved.tickers, DEFAULT_NOTIFICATION_PREFS.tickers),
    watchlist: asBoolean(resolved.watchlist, DEFAULT_NOTIFICATION_PREFS.watchlist),
    explore: asBoolean(resolved.explore, DEFAULT_NOTIFICATION_PREFS.explore),
    earnings: asBoolean(resolved.earnings, DEFAULT_NOTIFICATION_PREFS.earnings),
    ipos: asBoolean(resolved.ipos, DEFAULT_NOTIFICATION_PREFS.ipos),
    daily: asBoolean(resolved.daily, DEFAULT_NOTIFICATION_PREFS.daily),
    weekly: asBoolean(resolved.weekly, DEFAULT_NOTIFICATION_PREFS.weekly),
    inactiveHidden: asBoolean(resolved.inactiveHidden, DEFAULT_NOTIFICATION_PREFS.inactiveHidden),
  };
}

function isNotificationCategoryEnabled(prefs: NotificationPrefs, category: NotificationCategory): boolean {
  if (category === "watchlist") return prefs.watchlist && prefs.tickers;
  if (category === "explore") return prefs.explore && prefs.following;
  if (category === "earnings") return prefs.earnings;
  if (category === "ipo") return prefs.ipos;
  if (category === "daily") return prefs.daily;
  if (category === "weekly") return prefs.weekly;
  if (category === "inactive") return prefs.daily || prefs.weekly;
  return true;
}

function notificationCategoryLabel(category: NotificationCategory): string {
  if (category === "watchlist") return "Watchlist";
  if (category === "explore") return "Explore Feed";
  if (category === "earnings") return "Earnings";
  if (category === "ipo") return "IPO";
  if (category === "daily") return "Daily";
  if (category === "weekly") return "Weekly";
  if (category === "inactive") return "Inactive user";
  return "Notification";
}

function buildFolderItemDocId(itemType: SavedItemType, sourceId: string): string {
  const cleanSource = normalizeSourceId(sourceId).replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 180);
  return `${itemType}__${cleanSource || "item"}`;
}

function getTimestampMs(value: unknown): number {
  if (value instanceof admin.firestore.Timestamp) return value.toMillis();
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return Date.now();
}

function timestampFromMs(ms: number): admin.firestore.Timestamp {
  return admin.firestore.Timestamp.fromMillis(ms);
}

function normalizeMyRequestType(value: unknown): MyRequestType | "" {
  const raw = sanitizeText(value, 40).trim();
  if (!raw) return "";
  const lower = raw.toLowerCase();
  if (lower === "forecast" || lower === "screener" || lower === "indicator") return lower;
  if (lower === "modelcouncil" || lower === "model_council" || lower === "model-council") return "modelCouncil";
  return "";
}

function normalizeMyRequestVisibility(value: unknown, fallback: MyRequestShareVisibility = "private"): MyRequestShareVisibility {
  const raw = sanitizeText(value, 20).toLowerCase();
  if (raw === "public" || raw === "unlisted" || raw === "private") return raw;
  return fallback;
}

function normalizeMyRequestId(value: unknown): string {
  const raw = sanitizeText(value, 220);
  if (!raw) return "";
  return raw.replace(/[^A-Za-z0-9._:\-]/g, "_").slice(0, 220);
}

function buildMyRequestDocId(type: MyRequestType, sourceId: string): string {
  const cleanType = normalizeMyRequestType(type) || "forecast";
  const cleanSourceId = normalizeSourceId(sourceId).replace(/[^A-Za-z0-9._:\-]/g, "_").slice(0, 180);
  return `${cleanType}__${cleanSourceId || "item"}`;
}

function defaultMyRequestTitle(type: MyRequestType, payload: Record<string, unknown> = {}): string {
  const ticker = normalizeTicker(payload.ticker);
  if (type === "forecast") {
    return sanitizeText(payload.title, 160) || `${ticker || "Ticker"} forecast`;
  }
  if (type === "screener") {
    return sanitizeText(payload.title, 160) || "Screener run";
  }
  if (type === "indicator") {
    return sanitizeText(payload.title, 160) || `${ticker || "Ticker"} indicators`;
  }
  return sanitizeText(payload.title, 160) || `${ticker || "Ticker"} Model Council`;
}

function ensureMyRequestShareSlug(seed = ""): string {
  const existing = normalizeShareId(seed);
  if (existing) return existing;
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let out = "";
  for (let i = 0; i < 16; i += 1) {
    out += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return out;
}

async function ensureUniqueMyRequestShareSlug(ownerUid: string, requestId: string, seed = ""): Promise<string> {
  const preferred = ensureMyRequestShareSlug(seed);
  const preferredRef = db.collection("request_shares").doc(preferred);
  const preferredSnap = await preferredRef.get();
  if (!preferredSnap.exists) return preferred;
  const preferredData = (preferredSnap.data() || {}) as Record<string, unknown>;
  if (asString(preferredData.ownerUid) === ownerUid && asString(preferredData.requestId) === requestId) return preferred;

  for (let i = 0; i < 12; i += 1) {
    const candidate = ensureMyRequestShareSlug();
    const candidateRef = db.collection("request_shares").doc(candidate);
    const candidateSnap = await candidateRef.get();
    if (!candidateSnap.exists) return candidate;
    const candidateData = (candidateSnap.data() || {}) as Record<string, unknown>;
    if (asString(candidateData.ownerUid) === ownerUid && asString(candidateData.requestId) === requestId) return candidate;
  }
  return ensureMyRequestShareSlug(`${ownerUid.slice(0, 6)}${requestId.slice(0, 6)}${Date.now()}`);
}

function normalizeMyRequestShareObject(input: unknown): { visibility: MyRequestShareVisibility; slug: string; createdAt: unknown } {
  const raw = asPlainObject(input);
  return {
    visibility: normalizeMyRequestVisibility(raw.visibility, "private"),
    slug: normalizeShareId(raw.slug),
    createdAt: raw.createdAt || null,
  };
}

function trimOutputsMeta(input: unknown): Record<string, unknown> {
  const raw = asPlainObject(input);
  const out: Record<string, unknown> = {};
  Object.entries(raw)
    .slice(0, 18)
    .forEach(([key, value]) => {
      const cleanKey = sanitizeText(key, 60);
      if (!cleanKey) return;
      if (typeof value === "string") {
        out[cleanKey] = sanitizeText(value, 2400);
        return;
      }
      if (typeof value === "number" || typeof value === "boolean") {
        out[cleanKey] = value;
        return;
      }
      if (Array.isArray(value)) {
        out[cleanKey] = value
          .slice(0, 24)
          .map((item) => sanitizeText(item, 120))
          .filter(Boolean);
        return;
      }
      if (value && typeof value === "object") {
        const childOut: Record<string, unknown> = {};
        Object.entries(value as Record<string, unknown>)
          .slice(0, 12)
          .forEach(([childKey, childValue]) => {
            const cleanChildKey = sanitizeText(childKey, 40);
            if (!cleanChildKey) return;
            if (typeof childValue === "number" || typeof childValue === "boolean") {
              childOut[cleanChildKey] = childValue;
              return;
            }
            if (typeof childValue === "string") {
              childOut[cleanChildKey] = sanitizeText(childValue, 220);
            }
          });
        if (Object.keys(childOut).length) out[cleanKey] = childOut;
      }
    });
  return out;
}

function normalizeMyRequestInput(input: unknown): Record<string, unknown> {
  const raw = asPlainObject(input);
  const out: Record<string, unknown> = {};
  Object.entries(raw)
    .slice(0, 30)
    .forEach(([key, value]) => {
      const cleanKey = sanitizeText(key, 60);
      if (!cleanKey) return;
      if (typeof value === "string") {
        out[cleanKey] = sanitizeText(value, 4000);
        return;
      }
      if (typeof value === "number" || typeof value === "boolean") {
        out[cleanKey] = value;
        return;
      }
      if (Array.isArray(value)) {
        out[cleanKey] = value
          .slice(0, 40)
          .map((item) => (typeof item === "number" ? item : sanitizeText(item, 120)))
          .filter((item) => (typeof item === "number" ? Number.isFinite(item) : Boolean(item)));
        return;
      }
      if (value && typeof value === "object") {
        out[cleanKey] = trimOutputsMeta(value);
      }
    });
  return out;
}

function firstTickerFromRequest(input: Record<string, unknown>, sourceRef: Record<string, unknown>, outputsMeta: Record<string, unknown>): string {
  const direct = normalizeTicker(input.ticker || outputsMeta.ticker);
  if (direct) return direct;
  const topSymbols = Array.isArray(outputsMeta.topSymbols) ? outputsMeta.topSymbols : [];
  const first = normalizeTicker(topSymbols[0]);
  if (first) return first;
  return normalizeTicker(sourceRef.ticker);
}

function buildMyRequestSearchText(
  title: string,
  type: MyRequestType,
  ticker: string,
  input: Record<string, unknown>,
  outputsMeta: Record<string, unknown>
): string {
  const parts = [
    title,
    type,
    ticker,
    asString(input.question),
    asString(input.notes),
    asString(input.universe),
    asString(outputsMeta.summary),
  ]
    .join(" ")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  return parts.slice(0, 2400);
}

function myRequestShareUrl(slug: string): string {
  const cleanSlug = normalizeShareId(slug);
  if (!cleanSlug) return "";
  return `${PUBLIC_ORIGIN}/forecasting?requestShare=${encodeURIComponent(cleanSlug)}`;
}

function toMyRequestResponse(
  docId: string,
  data: Record<string, unknown>,
  opts: { includePayload?: boolean } = {}
): Record<string, unknown> {
  const includePayload = Boolean(opts.includePayload);
  const type = normalizeMyRequestType(data.type) || "forecast";
  const title = sanitizeText(data.title, 180) || defaultMyRequestTitle(type, data);
  const input = normalizeMyRequestInput(data.input);
  const outputsMeta = trimOutputsMeta(data.outputsMeta);
  const sourceRef = asPlainObject(data.sourceRef);
  const ticker = firstTickerFromRequest(input, sourceRef, outputsMeta);
  const createdAtMs = getTimestampMs(data.createdAt);
  const updatedAtMs = getTimestampMs(data.updatedAt || data.createdAt);
  const share = normalizeMyRequestShareObject(data.share);
  const visibility = normalizeMyRequestVisibility(data.visibility, share.visibility);
  const published = asBoolean(data.published, false);
  const deleted = asBoolean(data.deleted, false);
  const shareUrl =
    share.slug && share.visibility !== "private"
      ? myRequestShareUrl(share.slug)
      : "";

  const response: Record<string, unknown> = {
    id: docId,
    type,
    typeLabel: MY_REQUEST_TYPE_LABEL[type],
    title,
    ticker,
    ownerUid: asString(data.ownerUid),
    published,
    publishedAtMs: data.publishedAt ? getTimestampMs(data.publishedAt) : null,
    explorePostId: asString(data.explorePostId),
    deleted,
    createdAtMs,
    updatedAtMs,
    createdAt: new Date(createdAtMs).toISOString(),
    updatedAt: new Date(updatedAtMs).toISOString(),
    sourceRef: {
      collection: sanitizeText(sourceRef.collection, 80),
      id: sanitizeText(sourceRef.id, 220),
    },
    share: {
      visibility,
      slug: share.slug,
      shareUrl,
    },
  };

  if (includePayload) {
    response.input = input;
    response.outputsMeta = outputsMeta;
  } else {
    response.outputsMeta = trimOutputsMeta({
      summary: outputsMeta.summary,
      resultsCount: outputsMeta.resultsCount,
      forecastRowsCount: outputsMeta.forecastRowsCount,
      provider: outputsMeta.provider,
      model: outputsMeta.model,
      topSymbols: outputsMeta.topSymbols,
    });
  }

  return response;
}

function normalizeMyRequestPublishedFilter(input: unknown): "all" | "published" | "unpublished" {
  const raw = sanitizeText(input, 20).toLowerCase();
  if (raw === "published") return "published";
  if (raw === "unpublished" || raw === "draft") return "unpublished";
  return "all";
}

function extractScreenerTopSymbols(resultsRaw: unknown): string[] {
  const results = Array.isArray(resultsRaw) ? resultsRaw : [];
  const out: string[] = [];
  results.slice(0, 24).forEach((row) => {
    const symbol = normalizeTicker((row as Record<string, unknown>)?.symbol);
    if (!symbol || out.includes(symbol)) return;
    out.push(symbol);
  });
  return out.slice(0, 12);
}

function trimModelCouncilAnswer(answerRaw: unknown): string {
  const answer = sanitizeText(answerRaw, 2600);
  if (answer.length <= 700) return answer;
  return `${answer.slice(0, 697)}...`;
}

async function syncLegacyRequestsForUser(uid: string): Promise<void> {
  const cleanUid = sanitizeText(uid, 220);
  if (!cleanUid) return;

  const requestsRef = db.collection("users").doc(cleanUid).collection("requests");
  const [existingSnap, forecastSnap, screenerSnap, councilSnap] = await Promise.all([
    requestsRef.limit(240).get(),
    db.collection("forecast_requests").where("userId", "==", cleanUid).limit(120).get(),
    db.collection("screener_runs").where("userId", "==", cleanUid).limit(120).get(),
    db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).where("userId", "==", cleanUid).limit(120).get(),
  ]);

  const existingMap = new Map<string, Record<string, unknown>>();
  existingSnap.docs.forEach((doc) => {
    existingMap.set(doc.id, (doc.data() || {}) as Record<string, unknown>);
  });

  const postIds = [
    ...forecastSnap.docs.map((doc) => `forecast_${doc.id}`),
    ...screenerSnap.docs.map((doc) => `screener_${doc.id}`),
  ];
  const postMap = new Map<string, Record<string, unknown>>();
  if (postIds.length) {
    const postDocs = await db.getAll(...postIds.map((postId) => db.collection("posts").doc(postId)));
    postDocs.forEach((doc) => {
      if (!doc.exists) return;
      postMap.set(doc.id, (doc.data() || {}) as Record<string, unknown>);
    });
  }

  const writes: Array<Promise<unknown>> = [];
  const queueSync = (
    requestId: string,
    seed: {
      type: MyRequestType;
      title: string;
      input: Record<string, unknown>;
      outputsMeta: Record<string, unknown>;
      sourceRef: Record<string, unknown>;
      published: boolean;
      publishedAt: unknown;
      explorePostId: string;
      createdAt: unknown;
      updatedAt: unknown;
    }
  ) => {
    const existing = existingMap.get(requestId) || {};
    const existingShare = normalizeMyRequestShareObject(existing.share);
    const existingTitleEdited = asBoolean(existing.titleEdited, false);
    const existingTitle = sanitizeText(existing.title, 180);
    const nextTitle = existingTitleEdited && existingTitle ? existingTitle : sanitizeText(seed.title, 180) || defaultMyRequestTitle(seed.type, seed.input);
    const existingPublished = asBoolean(existing.published, false);
    const nextPublished = existingPublished || seed.published;
    const ticker = firstTickerFromRequest(seed.input, seed.sourceRef, seed.outputsMeta);
    const nextInput = normalizeMyRequestInput(seed.input);
    const nextOutputsMeta = trimOutputsMeta({
      ...seed.outputsMeta,
      ticker,
    });
    const nextShare = {
      visibility: normalizeMyRequestVisibility(existingShare.visibility, "private"),
      slug: normalizeShareId(existingShare.slug),
      createdAt: existingShare.createdAt || null,
    };
    const nextCreatedAt = existing.createdAt || seed.createdAt || admin.firestore.FieldValue.serverTimestamp();
    const seedUpdatedMs = getTimestampMs(seed.updatedAt || seed.createdAt);
    const existingUpdatedMs = getTimestampMs(existing.updatedAt || existing.createdAt);
    const nextUpdatedAt = existingUpdatedMs > seedUpdatedMs ? existing.updatedAt || existing.createdAt : timestampFromMs(seedUpdatedMs);

    const payload: Record<string, unknown> = {
      type: seed.type,
      ownerUid: cleanUid,
      title: nextTitle,
      titleEdited: existingTitleEdited,
      input: nextInput,
      outputsMeta: nextOutputsMeta,
      sourceRef: {
        collection: sanitizeText(seed.sourceRef.collection, 80),
        id: sanitizeText(seed.sourceRef.id, 220),
      },
      searchText: buildMyRequestSearchText(nextTitle, seed.type, ticker, nextInput, nextOutputsMeta),
      published: nextPublished,
      publishedAt:
        nextPublished
          ? existing.publishedAt || seed.publishedAt || seed.createdAt || admin.firestore.FieldValue.serverTimestamp()
          : null,
      explorePostId: nextPublished ? sanitizeText(existing.explorePostId || seed.explorePostId, 220) : "",
      deleted: asBoolean(existing.deleted, false),
      share: nextShare,
      createdAt: nextCreatedAt,
      updatedAt: nextUpdatedAt || admin.firestore.FieldValue.serverTimestamp(),
      visibility: normalizeMyRequestVisibility(existing.visibility, nextShare.visibility),
    };

    writes.push(requestsRef.doc(requestId).set(payload, { merge: true }));
  };

  forecastSnap.docs.forEach((doc) => {
    const data = (doc.data() || {}) as Record<string, unknown>;
    const ticker = normalizeTicker(data.ticker);
    const postId = `forecast_${doc.id}`;
    const postData = postMap.get(postId) || {};
    const postVisibility = sanitizeText(postData.visibility, 20).toLowerCase();
    const published = postVisibility === "public";
    const metricsRaw = asPlainObject(data.metrics);
    const metrics: Record<string, unknown> = {};
    ["lastClose", "medianEnd", "mae", "rmse", "mape", "coverage10_90", "historyPoints", "drift", "volatility"].forEach((key) => {
      const value = metricsRaw[key];
      if (value === null || value === undefined || value === "") return;
      if (typeof value === "number" && Number.isFinite(value)) {
        metrics[key] = value;
        return;
      }
      if (typeof value === "string") metrics[key] = sanitizeText(value, 80);
    });
    const requestId = buildMyRequestDocId("forecast", doc.id);
    queueSync(requestId, {
      type: "forecast",
      title: sanitizeText(data.title, 160) || `${ticker || "Ticker"} forecast`,
      input: {
        ticker,
        interval: sanitizeText(data.interval, 20),
        horizon: asFinite(data.horizon, 0) || null,
        service: sanitizeText(data.service, 40),
        quantiles: Array.isArray(data.quantiles) ? data.quantiles.slice(0, 12) : [],
      },
      outputsMeta: {
        summary: sanitizeText(data.serviceMessage || data.notes || "", 320),
        serviceMessage: sanitizeText(data.serviceMessage, 320),
        service: sanitizeText(data.service, 40),
        interval: sanitizeText(data.interval, 20),
        forecastRowsCount: Array.isArray(data.forecastRows) ? data.forecastRows.length : asFinite(data.forecastRowsCount, 0),
        metrics,
      },
      sourceRef: {
        collection: "forecast_requests",
        id: doc.id,
      },
      published,
      publishedAt: published ? postData.createdAt || data.createdAt : null,
      explorePostId: published ? postId : "",
      createdAt: data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: data.updatedAt || data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    });
  });

  screenerSnap.docs.forEach((doc) => {
    const data = (doc.data() || {}) as Record<string, unknown>;
    const topSymbols = extractScreenerTopSymbols(data.results);
    const postId = `screener_${doc.id}`;
    const postData = postMap.get(postId) || {};
    const postVisibility = sanitizeText(postData.visibility, 20).toLowerCase();
    const published = postVisibility === "public";
    const requestId = buildMyRequestDocId("screener", doc.id);
    queueSync(requestId, {
      type: "screener",
      title: sanitizeText(data.title, 160) || "Screener run",
      input: {
        universe: sanitizeText(data.universe, 40),
        market: sanitizeText(data.market, 20),
        maxNames: asFinite(data.maxNames, 0) || null,
        notes: sanitizeText(data.notes, 1200),
        model: sanitizeText(data.modelUsed || data.model, 80),
        filters: trimOutputsMeta(data.filters),
      },
      outputsMeta: {
        summary: sanitizeText(data.notes, 320),
        resultsCount: Array.isArray(data.results) ? data.results.length : asFinite(data.resultsFound, 0),
        topSymbols,
        modelUsed: sanitizeText(data.modelUsed || data.model, 80),
      },
      sourceRef: {
        collection: "screener_runs",
        id: doc.id,
      },
      published,
      publishedAt: published ? postData.createdAt || data.createdAt : null,
      explorePostId: published ? postId : "",
      createdAt: data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: data.updatedAt || data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    });
  });

  councilSnap.docs.forEach((doc) => {
    const data = (doc.data() || {}) as Record<string, unknown>;
    const ticker = normalizeTicker(data.ticker);
    const requestId = buildMyRequestDocId("modelCouncil", doc.id);
    queueSync(requestId, {
      type: "modelCouncil",
      title: sanitizeText(data.title, 160) || `${ticker || "Ticker"} Model Council`,
      input: {
        ticker,
        question: sanitizeText(data.question, 4000),
        provider: sanitizeText(data.provider, 80),
        model: sanitizeText(data.model, 120),
        language: sanitizeText(data.language, 20),
        modules: Array.isArray(data.selectedModules) ? data.selectedModules.slice(0, 24) : [],
      },
      outputsMeta: {
        summary: trimModelCouncilAnswer(data.answer),
        answer: trimModelCouncilAnswer(data.answer),
        provider: sanitizeText(data.provider, 80),
        model: sanitizeText(data.model, 120),
        citationsCount: Array.isArray(data.citations) ? data.citations.length : 0,
      },
      sourceRef: {
        collection: MODEL_COUNCIL_RESPONSE_COLLECTION,
        id: doc.id,
      },
      published: false,
      publishedAt: null,
      explorePostId: "",
      createdAt: data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: data.updatedAt || data.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    });
  });

  if (writes.length) {
    await Promise.all(writes);
  }
}

async function readMyRequestForOwner(uid: string, requestId: string): Promise<{ id: string; data: Record<string, unknown> } | null> {
  const cleanUid = sanitizeText(uid, 220);
  const cleanRequestId = normalizeMyRequestId(requestId);
  if (!cleanUid || !cleanRequestId) return null;
  const snap = await db.collection("users").doc(cleanUid).collection("requests").doc(cleanRequestId).get();
  if (!snap.exists) return null;
  const data = (snap.data() || {}) as Record<string, unknown>;
  if (asString(data.ownerUid) && asString(data.ownerUid) !== cleanUid) return null;
  return { id: snap.id, data };
}

function computeDecay(recencyHours: number): number {
  const safe = Math.max(0, recencyHours);
  return Math.max(0.05, Math.exp(-safe / 48));
}

function normalizeCounts(raw: unknown): PostCounts {
  const data = (raw || {}) as Partial<PostCounts>;
  return {
    likes: Math.max(0, Math.floor(asFinite(data.likes, 0))),
    comments: Math.max(0, Math.floor(asFinite(data.comments, 0))),
    reposts: Math.max(0, Math.floor(asFinite(data.reposts, 0))),
    shares: Math.max(0, Math.floor(asFinite(data.shares, 0))),
    reports: Math.max(0, Math.floor(asFinite(data.reports, 0))),
  };
}

function computeScore(counts: PostCounts, createdAtMs: number, nowMs = Date.now()): number {
  const base = counts.likes * 3 + counts.comments * 4 + counts.reposts * 5 + counts.shares * 2;
  if (base <= 0) return 0;
  const recencyHours = (nowMs - createdAtMs) / (1000 * 60 * 60);
  const score = base * computeDecay(recencyHours);
  return Number(score.toFixed(6));
}

function encodeCursor(cursor: ExploreCursor | null): string | null {
  if (!cursor) return null;
  const json = JSON.stringify(cursor);
  return Buffer.from(json, "utf8").toString("base64url");
}

function decodeCursor(raw: unknown): ExploreCursor | null {
  if (!raw || typeof raw !== "string") return null;
  try {
    const decoded = Buffer.from(raw, "base64url").toString("utf8");
    const parsed = JSON.parse(decoded) as Partial<ExploreCursor>;
    if (!parsed || typeof parsed !== "object") return null;
    if (!parsed.id || !Number.isFinite(parsed.createdAtMs)) return null;
    const cursor: ExploreCursor = {
      id: String(parsed.id),
      createdAtMs: Number(parsed.createdAtMs),
    };
    if (Number.isFinite(parsed.score)) cursor.score = Number(parsed.score);
    return cursor;
  } catch {
    return null;
  }
}

function decodeFirestoreValue(value: any): unknown {
  if (!value || typeof value !== "object") return null;
  if ("stringValue" in value) return String(value.stringValue);
  if ("booleanValue" in value) return Boolean(value.booleanValue);
  if ("integerValue" in value) return Number(value.integerValue);
  if ("doubleValue" in value) return Number(value.doubleValue);
  if ("timestampValue" in value) return String(value.timestampValue);
  if ("nullValue" in value) return null;
  if ("arrayValue" in value) {
    const values = Array.isArray(value.arrayValue?.values) ? value.arrayValue.values : [];
    return values.map((entry: any) => decodeFirestoreValue(entry));
  }
  if ("mapValue" in value) {
    const fields = value.mapValue?.fields || {};
    const out: Record<string, unknown> = {};
    Object.entries(fields).forEach(([key, child]) => {
      out[key] = decodeFirestoreValue(child);
    });
    return out;
  }
  return null;
}

function decodeFirestoreFields(fields: Record<string, unknown> | undefined): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (!fields || typeof fields !== "object") return out;
  Object.entries(fields).forEach(([key, value]) => {
    out[key] = decodeFirestoreValue(value);
  });
  return out;
}

function parseDocumentPath(cloudEvent: any): string {
  const valueName = asString(cloudEvent?.data?.value?.name);
  if (valueName.includes("/documents/")) {
    return valueName.split("/documents/")[1] || "";
  }
  const subject = asString(cloudEvent?.subject);
  if (subject.startsWith("documents/")) {
    return subject.replace(/^documents\//, "");
  }
  return "";
}

function buildMetaPayload(post: PostDoc): Record<string, string> {
  return {
    type: post.type,
    postId: post.id,
    ticker: post.tickers[0] || "",
    url: post.targetUrl,
  };
}

async function sendTopicNotification(topic: string, post: PostDoc, title: string, body: string): Promise<void> {
  try {
    await messaging.send({
      topic,
      notification: {
        title,
        body,
      },
      data: buildMetaPayload(post),
      webpush: {
        fcmOptions: {
          link: post.targetUrl,
        },
      },
      android: {
        notification: {
          channelId: "quantura-default",
          clickAction: post.targetUrl,
        },
      },
      apns: {
        payload: {
          aps: {
            sound: "default",
          },
        },
      },
    });
  } catch (error) {
    console.warn(`[Explore] Topic notification failed for ${topic}:`, error);
  }
}

function extractTickers(payload: Record<string, unknown>): string[] {
  const tickers = new Set<string>();
  const directFields = [payload.ticker, payload.symbol, payload.primaryTicker];
  directFields.forEach((value) => {
    const ticker = normalizeTicker(value);
    if (ticker) tickers.add(ticker);
  });

  const listFields = [payload.tickers, payload.symbols, payload.briefTickers];
  listFields.forEach((entry) => {
    if (!Array.isArray(entry)) return;
    entry.forEach((value) => {
      const ticker = normalizeTicker(value);
      if (ticker) tickers.add(ticker);
    });
  });

  const rows = Array.isArray(payload.results) ? payload.results : Array.isArray(payload.rows) ? payload.rows : [];
  rows.forEach((row) => {
    if (!row || typeof row !== "object") return;
    const ticker = normalizeTicker((row as any).ticker || (row as any).symbol);
    if (ticker) tickers.add(ticker);
  });

  return Array.from(tickers).slice(0, 8);
}

function extractPreview(payload: Record<string, unknown>, postType: PostType): PostDoc["preview"] {
  const imageUrl = sanitizeText(payload.imageUrl || payload.chartUrl || payload.previewImage || payload.thumbnailUrl, 1000);
  const metricsSource = payload.metrics && typeof payload.metrics === "object"
    ? (payload.metrics as Record<string, unknown>)
    : payload.summary && typeof payload.summary === "object"
    ? (payload.summary as Record<string, unknown>)
    : null;

  const metrics: Record<string, string | number> = {};
  if (metricsSource) {
    Object.entries(metricsSource)
      .slice(0, 6)
      .forEach(([key, value]) => {
        const cleanKey = sanitizeText(key, 40);
        if (!cleanKey) return;
        if (typeof value === "number" && Number.isFinite(value)) {
          metrics[cleanKey] = value;
        } else {
          const text = sanitizeText(value, 120);
          if (text) metrics[cleanKey] = text;
        }
      });
  }

  if (postType === "screener" && !Object.keys(metrics).length) {
    const count = Array.isArray(payload.results) ? payload.results.length : asFinite(payload.resultsFound, 0);
    if (count > 0) metrics.results = Math.floor(count);
  }

  if (imageUrl) {
    return {
      kind: "image",
      imageUrl,
      metrics: Object.keys(metrics).length ? metrics : undefined,
    };
  }

  return {
    kind: "summary",
    metrics: Object.keys(metrics).length ? metrics : undefined,
  };
}

function buildTargetUrl(postType: PostType, sourceDocId: string): string {
  switch (postType) {
    case "forecast":
      return `/forecasting?forecastId=${encodeURIComponent(sourceDocId)}`;
    case "backtest":
      return `/indicators?runId=${encodeURIComponent(sourceDocId)}`;
    case "screener":
      return `/screener?runId=${encodeURIComponent(sourceDocId)}`;
    case "agent":
      return `/ticker-query?agentRunId=${encodeURIComponent(sourceDocId)}`;
    default:
      return "/explore";
  }
}

function buildTitle(postType: PostType, payload: Record<string, unknown>, tickers: string[]): string {
  const ticker = tickers[0] || "Market";
  const candidate = sanitizeText(payload.title || payload.agentName || payload.universe || "", 120);
  if (candidate) return candidate;
  switch (postType) {
    case "forecast":
      return `${ticker} forecast update`;
    case "backtest": {
      const strategy = sanitizeText(payload.strategy || "strategy", 40);
      return `${ticker} strategy run (${strategy})`;
    }
    case "screener":
      return `${ticker} screener run`;
    case "agent":
      return `${ticker} AI agent run`;
    default:
      return "New Quantura insight";
  }
}

async function readAuthorProfile(authorUid: string): Promise<{ handle: string; photoURL: string }> {
  try {
    const snap = await db.collection("users").doc(authorUid).get();
    const data = (snap.data() || {}) as Record<string, unknown>;
    const profile = (data.profile || {}) as Record<string, unknown>;

    const handle =
      normalizeHandle(data.handle) ||
      normalizeHandle(profile.username) ||
      normalizeHandle(data.displayName) ||
      `user-${authorUid.slice(0, 8)}`;

    const photoURL = sanitizeText(data.photoURL || profile.photoURL || profile.avatarUrl || "", 1000);

    return { handle, photoURL };
  } catch {
    return {
      handle: `user-${authorUid.slice(0, 8)}`,
      photoURL: "",
    };
  }
}

async function publishAutoPost(post: PostDoc): Promise<void> {
  const title = post.title;
  const subtitle = `${post.authorHandle} • ${post.type.toUpperCase()}`;

  await sendTopicNotification("explore-global", post, title, subtitle);

  for (const ticker of post.tickers.slice(0, 5)) {
    await sendTopicNotification(`ticker-${ticker}`, post, `${ticker}: ${title}`, subtitle);
  }

  await sendTopicNotification(`author-${post.authorUid}`, post, title, subtitle);
}

async function createPostFromResult(postType: PostType, sourceDocId: string, payload: Record<string, unknown>): Promise<void> {
  const authorUid = sanitizeText(payload.authorUid || payload.userId || payload.uid || payload.ownerUid, 120);
  if (!authorUid) {
    console.warn(`[Explore] Skip ${postType}/${sourceDocId}: missing author uid`);
    return;
  }

  const postId = `${postType}_${sourceDocId}`;
  const postRef = db.collection("posts").doc(postId);
  const existing = await postRef.get();
  if (existing.exists) {
    return;
  }

  const { handle, photoURL } = await readAuthorProfile(authorUid);
  const tickers = extractTickers(payload);
  const tags = [postType, ...tickers.map((ticker) => ticker.toLowerCase())].slice(0, 12);
  const title = buildTitle(postType, payload, tickers);
  const caption = sanitizeText(
    payload.caption || payload.notes || payload.summary || payload.agentSummary || payload.description,
    400
  );
  const createdAtMs = getTimestampMs(payload.createdAt || payload.updatedAt);
  const createdAt = timestampFromMs(createdAtMs);

  const post: PostDoc = {
    id: postId,
    type: postType,
    authorUid,
    authorHandle: handle,
    authorPhotoURL: photoURL,
    title,
    caption,
    tickers,
    tags,
    preview: extractPreview(payload, postType),
    targetUrl: buildTargetUrl(postType, sourceDocId),
    visibility: "public",
    createdAt,
    updatedAt: createdAt,
    counts: {
      likes: 0,
      comments: 0,
      reposts: 0,
      shares: 0,
      reports: 0,
    },
    score: 0,
    lastEngagedAt: createdAt,
  };

  await postRef.set(post, { merge: false });
  await publishAutoPost(post);
}

async function verifyRequestUser(req: Request, required = false): Promise<admin.auth.DecodedIdToken | null> {
  const authHeader = asString(req.headers.authorization);
  if (!authHeader) {
    if (required) throw new Error("unauthenticated");
    return null;
  }
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    if (required) throw new Error("unauthenticated");
    return null;
  }
  const token = match[1];
  try {
    return await auth.verifyIdToken(token);
  } catch {
    throw new Error("invalid_token");
  }
}

function getBearerToken(req: Request): string {
  const authHeader = asString(req.headers["authorization"] || (req.headers as any)["Authorization"]).trim();
  if (!authHeader) return "";
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function normalizeAdFormat(value: unknown): string {
  const normalized = sanitizeText(value, 40)
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "");
  if (!normalized) return "unknown";
  return normalized;
}

function normalizeCurrency(value: unknown): string {
  const normalized = sanitizeText(value, 8).toUpperCase();
  if (/^[A-Z]{3}$/.test(normalized)) return normalized;
  return "USD";
}

function asPlainObject(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function normalizePolymarketSort(value: unknown): "relevance" | "volume" {
  return sanitizeText(value, 24).toLowerCase() === "volume" ? "volume" : "relevance";
}

function parseGammaArray(raw: unknown): unknown[] {
  if (Array.isArray(raw)) return raw;
  if (typeof raw !== "string") return [];
  const text = raw.trim();
  if (!text) return [];
  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function clampProbability(value: unknown): number | null {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (num < 0) return 0;
  if (num > 1) return 1;
  return num;
}

function parseUsdNumber(raw: unknown): number | undefined {
  const num = Number(raw);
  if (!Number.isFinite(num)) return undefined;
  return Math.max(0, num);
}

function parsePolymarketOutcomes(raw: unknown): string[] {
  return parseGammaArray(raw)
    .map((value) => sanitizeText(value, 120))
    .filter(Boolean)
    .slice(0, 16);
}

function parsePolymarketOutcomePrices(raw: unknown): number[] {
  return parseGammaArray(raw)
    .map((value) => clampProbability(value))
    .filter((value): value is number => typeof value === "number")
    .slice(0, 16);
}

function polymarketUrlFromSlug(slug: unknown): string {
  const clean = sanitizeText(slug, 240).replace(/^\/+|\/+$/g, "");
  if (!clean) return "";
  return `https://polymarket.com/event/${encodeURIComponent(clean)}`;
}

function normalizePolymarketMarket(raw: unknown, event: Record<string, unknown>): PolymarketMarketRecord | null {
  const market = asPlainObject(raw);
  const id = sanitizeText(market.id || market.marketId || market.conditionId || market.market_id, 120);
  const question = sanitizeText(market.question || market.title, 320);
  if (!id || !question) return null;

  const parsedOutcomes = parsePolymarketOutcomes(market.outcomes);
  const parsedPrices = parsePolymarketOutcomePrices(market.outcomePrices);
  const alignedLength = Math.min(parsedOutcomes.length, parsedPrices.length);
  const outcomes = alignedLength > 0 ? parsedOutcomes.slice(0, alignedLength) : [];
  const outcomePrices = alignedLength > 0 ? parsedPrices.slice(0, alignedLength) : [];

  const topOutcomes = outcomes
    .map((label, index) => ({ label, prob: outcomePrices[index] }))
    .filter((entry) => typeof entry.label === "string" && Number.isFinite(entry.prob))
    .sort((a, b) => b.prob - a.prob)
    .slice(0, 6);

  let closed = asBoolean(market.closed, false);
  const status = sanitizeText(market.status, 40).toLowerCase();
  if (status === "closed" || status === "resolved" || status === "ended") closed = true;
  const active = asBoolean(market.active, !closed);

  let yesProb: number | undefined = undefined;
  if (outcomes.length === 2) {
    const yesIndex = outcomes.findIndex((label) => /^yes$/i.test(label));
    if (yesIndex >= 0 && Number.isFinite(outcomePrices[yesIndex])) {
      yesProb = outcomePrices[yesIndex];
    }
  }

  return {
    id,
    question,
    slug: sanitizeText(market.slug, 220) || undefined,
    endDate: sanitizeText(market.endDate || market.end_date, 40) || undefined,
    category: sanitizeText(market.category || event.category, 80) || undefined,
    image: sanitizeText(market.image, 600) || undefined,
    icon: sanitizeText(market.icon, 600) || undefined,
    volumeUsd: parseUsdNumber(market.volume),
    liquidityUsd: parseUsdNumber(market.liquidity),
    outcomes,
    outcomePrices,
    isBinary: outcomes.length === 2,
    yesProb,
    topOutcomes,
    closed,
    active,
  };
}

function normalizePolymarketResponse(
  rawPayload: unknown,
  opts: { query: string; sort: "relevance" | "volume"; includeClosed: boolean }
): PolymarketSearchResponse {
  const root = asPlainObject(rawPayload);
  const rawEvents = Array.isArray(root.events) ? root.events : Array.isArray(rawPayload) ? (rawPayload as unknown[]) : [];
  const events: PolymarketEventRecord[] = [];

  rawEvents.forEach((rawEvent, index) => {
    const event = asPlainObject(rawEvent);
    const eventId = sanitizeText(event.id || event.eventId, 120) || `${opts.query || "event"}_${index + 1}`;
    const eventTitle = sanitizeText(event.title || event.name || event.question, 240) || "Prediction markets";
    const eventSlug = sanitizeText(event.slug, 220) || undefined;
    const eventTicker = sanitizeText(event.ticker, 24).toUpperCase() || undefined;
    const marketRows = Array.isArray(event.markets) ? event.markets : [];
    const markets = marketRows
      .map((rawMarket) => normalizePolymarketMarket(rawMarket, event))
      .filter((market): market is PolymarketMarketRecord => Boolean(market))
      .filter((market) => (opts.includeClosed ? true : !market.closed && market.active !== false));

    if (opts.sort === "volume") {
      markets.sort((a, b) => {
        const volumeDelta = (b.volumeUsd || 0) - (a.volumeUsd || 0);
        if (volumeDelta !== 0) return volumeDelta;
        return (b.liquidityUsd || 0) - (a.liquidityUsd || 0);
      });
    }

    if (!markets.length) return;
    events.push({
      id: eventId,
      title: eventTitle,
      slug: eventSlug,
      ticker: eventTicker,
      markets,
    });
  });

  return {
    query: sanitizeText(opts.query, 120),
    fetchedAt: new Date().toISOString(),
    events,
  };
}

function prunePolymarketCache(nowMs = Date.now()): void {
  for (const [key, entry] of polymarketCache.entries()) {
    if (entry.expiresAtMs <= nowMs) polymarketCache.delete(key);
  }
  while (polymarketCache.size > POLYMARKET_CACHE_MAX_ENTRIES) {
    const oldestKey = polymarketCache.keys().next().value as string | undefined;
    if (!oldestKey) break;
    polymarketCache.delete(oldestKey);
  }
}

function getPolymarketCache(cacheKey: string): PolymarketSearchResponse | null {
  const entry = polymarketCache.get(cacheKey);
  if (!entry) return null;
  if (entry.expiresAtMs <= Date.now()) {
    polymarketCache.delete(cacheKey);
    return null;
  }
  polymarketCache.delete(cacheKey);
  polymarketCache.set(cacheKey, entry);
  return entry.value;
}

function setPolymarketCache(cacheKey: string, value: PolymarketSearchResponse): void {
  prunePolymarketCache();
  polymarketCache.set(cacheKey, {
    expiresAtMs: Date.now() + POLYMARKET_CACHE_TTL_MS,
    value,
  });
  prunePolymarketCache();
}

async function fetchGammaJson(path: string, params: Record<string, string | number | boolean>): Promise<unknown> {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined) return;
    const serialized = String(value).trim();
    if (!serialized) return;
    query.set(key, serialized);
  });
  const url = `${GAMMA_API_BASE}${path}${query.toString() ? `?${query.toString()}` : ""}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": "QuanturaPolymarket/1.0",
      },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = sanitizeText((payload as any)?.error || (payload as any)?.message || "", 180);
      throw new Error(detail || `Gamma request failed (${response.status})`);
    }
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

function summarizeWebhookPayload(req: Request): Record<string, unknown> {
  const body = asPlainObject(req.body);
  const query = asPlainObject(req.query);
  return {
    path: req.path,
    method: req.method,
    ip: requestIpAddress(req),
    userAgent: sanitizeText(req.headers["user-agent"], 300),
    query,
    body,
    receivedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

function normalizeLlmMessages(raw: unknown): Array<{ role: "system" | "user" | "assistant"; content: string }> {
  const rows = Array.isArray(raw) ? raw : [];
  return rows
    .map((entry) => {
      const item = (entry || {}) as Record<string, unknown>;
      const roleRaw = asString(item.role).trim().toLowerCase();
      const role: "system" | "user" | "assistant" =
        roleRaw === "system" || roleRaw === "assistant" || roleRaw === "user" ? (roleRaw as any) : "user";
      const content = sanitizeText(item.content, 12000);
      return { role, content };
    })
    .filter((item) => item.content.length > 0)
    .slice(0, 40);
}

type LlmProviderId = "openai" | "gemini" | "mistral" | "perplexity" | "other";

function normalizeProvider(raw: unknown): LlmProviderId {
  const value = asString(raw).trim().toLowerCase();
  if (value === "gemini" || value === "mistral" || value === "perplexity" || value === "openai" || value === "other") {
    return value;
  }
  return "openai";
}

function parseWebhookSecret(req: Request): string {
  return sanitizeText(req.headers["x-quantura-webhook-secret"] || req.query.secret, 500);
}

function checkWebhookSecret(req: Request, expected: string): boolean {
  if (!expected) return true;
  const provided = parseWebhookSecret(req);
  return provided.length > 0 && provided === expected;
}

function llmTimeoutSignal(timeoutMs = LLM_TIMEOUT_MS): { signal: AbortSignal; clear: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
  };
}

async function invokeOpenAiLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
  allowWebSearch: boolean;
  stream: boolean;
  background: boolean;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY is not configured.");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: payload.model,
        input: payload.messages.map((item) => ({
          role: item.role,
          content: [{ type: "input_text", text: item.content }],
        })),
        max_output_tokens: payload.maxTokens,
        stream: Boolean(payload.stream),
        background: Boolean(payload.background),
        tools: payload.allowWebSearch ? [{ type: "web_search_preview" }] : [],
        metadata: {
          quantura_workflow: "model_council",
          quantura_prompt_caching: "enabled",
        },
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      const detail = sanitizeText((body as any)?.error?.message || "", 180);
      throw new Error(detail || `OpenAI request failed (${response.status}).`);
    }
    const text = sanitizeText(extractResponsesOutputText(body), 20000);
    if (!text) throw new Error("OpenAI returned an empty response.");
    return { text, usage: extractResponsesUsage(body) };
  } finally {
    clear();
  }
}

function extractResponsesOutputText(payload: Record<string, unknown>): string {
  const rawDirect = (payload as any)?.output_text;
  const directParts: string[] = [];
  if (typeof rawDirect === "string" || typeof rawDirect === "number" || typeof rawDirect === "boolean") {
    const text = sanitizeText(rawDirect, 24000);
    if (text) directParts.push(text);
  } else if (Array.isArray(rawDirect)) {
    rawDirect.forEach((part) => {
      const text = sanitizeText(
        (part as any)?.text ?? (part as any)?.value ?? (part as any)?.output_text ?? part,
        24000
      );
      if (text) directParts.push(text);
    });
  } else if (rawDirect && typeof rawDirect === "object") {
    const text = sanitizeText((rawDirect as any).text ?? (rawDirect as any).value ?? "", 24000);
    if (text) directParts.push(text);
  }
  const direct = sanitizeText(directParts.join("\n").trim(), 24000);
  if (direct) return direct;
  const output = Array.isArray((payload as any)?.output) ? ((payload as any).output as any[]) : [];
  const chunks: string[] = [];
  output.forEach((item) => {
    const content = Array.isArray(item?.content) ? item.content : [];
    content.forEach((part: any) => {
      const text = sanitizeText(
        part?.text?.value ??
          part?.text ??
          part?.output_text?.value ??
          part?.output_text ??
          part?.value?.text ??
          part?.value,
        24000
      );
      if (text) chunks.push(text);
    });
  });
  return sanitizeText(chunks.join("\n").trim(), 24000);
}

function extractResponsesUsage(payload: Record<string, unknown>): Record<string, unknown> {
  const usage = ((payload as any)?.usage || {}) as Record<string, unknown>;
  const inputTokens = asFinite((usage as any).input_tokens, 0);
  const outputTokens = asFinite((usage as any).output_tokens, 0);
  const totalTokens = asFinite((usage as any).total_tokens, inputTokens + outputTokens);
  const cachedTokens = asFinite(((usage as any).input_tokens_details || {}).cached_tokens, 0);
  return {
    prompt_tokens: Math.max(0, Math.floor(inputTokens)),
    completion_tokens: Math.max(0, Math.floor(outputTokens)),
    total_tokens: Math.max(0, Math.floor(totalTokens)),
    cached_tokens: Math.max(0, Math.floor(cachedTokens)),
  };
}

function extractResponsesCitations(payload: Record<string, unknown>): Array<Record<string, unknown>> {
  const output = Array.isArray((payload as any)?.output) ? ((payload as any).output as any[]) : [];
  const seen = new Set<string>();
  const citations: Array<Record<string, unknown>> = [];
  output.forEach((item) => {
    const content = Array.isArray(item?.content) ? item.content : [];
    content.forEach((part: any) => {
      const annotations = Array.isArray(part?.annotations) ? part.annotations : [];
      annotations.forEach((annotation: any) => {
        const url = sanitizeText(annotation?.url || annotation?.source || "", 500);
        const title = sanitizeText(annotation?.title || annotation?.text || url, 300);
        if (!url || seen.has(url)) return;
        seen.add(url);
        citations.push({
          url,
          title: title || url,
        });
      });
    });
  });
  return citations.slice(0, 16);
}

async function invokeGeminiLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  if (!GEMINI_API_KEY) throw new Error("GEMINI_API_KEY is not configured.");
  const prompt = payload.messages.map((item) => `${item.role.toUpperCase()}: ${item.content}`).join("\n\n");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(payload.model)}:generateContent?key=${encodeURIComponent(
        GEMINI_API_KEY
      )}`,
      {
        method: "POST",
        signal,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: payload.temperature,
            maxOutputTokens: payload.maxTokens,
          },
        }),
      }
    );
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) throw new Error(`Gemini request failed (${response.status}).`);
    const parts = ((((body.candidates as any)?.[0] || {}).content || {}).parts || []) as Array<Record<string, unknown>>;
    const text = sanitizeText(parts.map((part) => asString(part.text)).join("\n"), 20000);
    if (!text) throw new Error("Gemini returned an empty response.");
    return { text, usage: ((body.usageMetadata as any) || {}) as Record<string, unknown> };
  } finally {
    clear();
  }
}

async function invokeMistralLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown> }> {
  if (!MISTRAL_API_KEY) throw new Error("MISTRAL_API_KEY is not configured.");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch("https://api.mistral.ai/v1/chat/completions", {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${MISTRAL_API_KEY}`,
      },
      body: JSON.stringify({
        model: payload.model,
        messages: payload.messages,
        temperature: payload.temperature,
        max_tokens: payload.maxTokens,
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) throw new Error(`Mistral request failed (${response.status}).`);
    const text = sanitizeText((((body.choices as any)?.[0] || {}).message || {}).content, 20000);
    if (!text) throw new Error("Mistral returned an empty response.");
    return { text, usage: ((body.usage as any) || {}) as Record<string, unknown> };
  } finally {
    clear();
  }
}

async function invokePerplexityLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
}): Promise<{ text: string; usage: Record<string, unknown>; citations: unknown[] }> {
  if (!PERPLEXITY_API_KEY) throw new Error("PERPLEXITY_API_KEY is not configured.");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch("https://api.perplexity.ai/chat/completions", {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${PERPLEXITY_API_KEY}`,
      },
      body: JSON.stringify({
        model: payload.model,
        messages: payload.messages,
        temperature: payload.temperature,
        max_tokens: payload.maxTokens,
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) throw new Error(`Perplexity request failed (${response.status}).`);
    const text = sanitizeText((((body.choices as any)?.[0] || {}).message || {}).content, 20000);
    if (!text) throw new Error("Perplexity returned an empty response.");
    return {
      text,
      usage: ((body.usage as any) || {}) as Record<string, unknown>,
      citations: Array.isArray(body.citations) ? body.citations : [],
    };
  } finally {
    clear();
  }
}

async function invokeOtherLlm(payload: {
  model: string;
  messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
  temperature: number;
  maxTokens: number;
  allowWebSearch: boolean;
  stream: boolean;
  background: boolean;
}): Promise<{ text: string; usage: Record<string, unknown>; citations: unknown[] }> {
  if (!MODEL_COUNCIL_OTHER_API_KEY) throw new Error("MODEL_COUNCIL_OTHER_API_KEY is not configured.");
  if (!MODEL_COUNCIL_OTHER_BASE_URL) throw new Error("MODEL_COUNCIL_OTHER_BASE_URL is not configured.");
  const { signal, clear } = llmTimeoutSignal();
  try {
    const response = await fetch(`${MODEL_COUNCIL_OTHER_BASE_URL}/v1/responses`, {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${MODEL_COUNCIL_OTHER_API_KEY}`,
      },
      body: JSON.stringify({
        model: payload.model,
        input: payload.messages.map((item) => ({
          role: item.role,
          content: [{ type: "input_text", text: item.content }],
        })),
        max_output_tokens: payload.maxTokens,
        stream: Boolean(payload.stream),
        background: Boolean(payload.background),
        tools: payload.allowWebSearch ? [{ type: "web_search_preview" }] : [],
      }),
    });
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      const detail = sanitizeText((body as any)?.error?.message || "", 180);
      throw new Error(detail || `Other provider request failed (${response.status}).`);
    }
    const text = sanitizeText(extractResponsesOutputText(body), 20000);
    if (!text) throw new Error("Other provider returned an empty response.");
    return {
      text,
      usage: extractResponsesUsage(body),
      citations: extractResponsesCitations(body),
    };
  } finally {
    clear();
  }
}

async function invokeLlmWithFallback(rawPayload: Record<string, unknown>): Promise<{
  provider: string;
  model: string;
  text: string;
  latencyMs: number;
  usage: Record<string, unknown>;
  citations?: unknown[];
  attempted: string[];
}> {
  const provider = normalizeProvider(rawPayload.provider);
  const fallbackProviders = Array.isArray(rawPayload.fallbackProviders)
    ? rawPayload.fallbackProviders.map((item) => normalizeProvider(item))
    : [];
  const providers = Array.from(new Set([provider, ...fallbackProviders]));
  const messages = normalizeLlmMessages(rawPayload.messages);
  if (!messages.length) throw new Error("messages are required.");
  const model = sanitizeText(rawPayload.model, 120) || DEFAULT_LLM_MODEL;
  const params = (rawPayload.params || {}) as Record<string, unknown>;
  const temperature = Math.max(0, Math.min(2, asFinite(params.temperature, 0.2)));
  const maxTokens = Math.max(64, Math.min(4000, Math.floor(asFinite(params.maxTokens, 600))));
  const allowWebSearch = asBoolean(params.webSearch ?? params.allowWebSearch ?? rawPayload.webSearch, true);
  const stream = asBoolean(params.stream ?? rawPayload.stream, false);
  const background = asBoolean(params.background ?? rawPayload.background, true);

  const errors: string[] = [];
  const startedAt = Date.now();
  for (const currentProvider of providers) {
    try {
      if (currentProvider === "openai") {
        const result = await invokeOpenAiLlm({
          model,
          messages,
          temperature,
          maxTokens,
          allowWebSearch,
          stream,
          background,
        });
        return {
          provider: currentProvider,
          model,
          text: result.text,
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
        };
      }
      if (currentProvider === "gemini") {
        const result = await invokeGeminiLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: result.text,
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
        };
      }
      if (currentProvider === "mistral") {
        const result = await invokeMistralLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: result.text,
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          attempted: [...errors],
        };
      }
      if (currentProvider === "perplexity") {
        const result = await invokePerplexityLlm({ model, messages, temperature, maxTokens });
        return {
          provider: currentProvider,
          model,
          text: result.text,
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          citations: result.citations,
          attempted: [...errors],
        };
      }
      if (currentProvider === "other") {
        const result = await invokeOtherLlm({
          model,
          messages,
          temperature,
          maxTokens,
          allowWebSearch,
          stream,
          background,
        });
        return {
          provider: currentProvider,
          model,
          text: result.text,
          latencyMs: Date.now() - startedAt,
          usage: result.usage,
          citations: result.citations,
          attempted: [...errors],
        };
      }
    } catch (error: any) {
      errors.push(`${currentProvider}:${sanitizeText(error?.message || error, 180) || "failed"}`);
    }
  }

  throw new Error(errors.join(" | ") || "No provider succeeded.");
}

async function decodePlayIntegrityToken(input: {
  integrityToken: string;
  packageName: string;
}): Promise<{
  ok: boolean;
  packageName: string;
  appRecognitionVerdict: string;
  deviceRecognitionVerdicts: string[];
  licensingVerdict: string;
  nonce: string;
  timestampMillis: string;
  raw: Record<string, unknown>;
}> {
  const client = await PLAY_INTEGRITY_AUTH.getClient();
  const endpoint = `https://playintegrity.googleapis.com/v1/${encodeURIComponent(input.packageName)}:decodeIntegrityToken`;
  const response = await client.request({
    url: endpoint,
    method: "POST",
    data: {
      integrity_token: input.integrityToken,
    },
  });
  const payload = ((response.data as any)?.tokenPayloadExternal || {}) as Record<string, unknown>;
  const appIntegrity = (payload.appIntegrity || {}) as Record<string, unknown>;
  const deviceIntegrity = (payload.deviceIntegrity || {}) as Record<string, unknown>;
  const accountDetails = (payload.accountDetails || {}) as Record<string, unknown>;
  const requestDetails = (payload.requestDetails || {}) as Record<string, unknown>;

  const appRecognitionVerdict = sanitizeText(appIntegrity.appRecognitionVerdict, 120);
  const deviceRecognitionVerdicts = Array.isArray(deviceIntegrity.deviceRecognitionVerdict)
    ? deviceIntegrity.deviceRecognitionVerdict.map((item: unknown) => sanitizeText(item, 120)).filter(Boolean)
    : [];
  const licensingVerdict = sanitizeText(accountDetails.appLicensingVerdict, 120);
  const nonce = sanitizeText(requestDetails.nonce, 320);
  const timestampMillis = sanitizeText(requestDetails.timestampMillis, 40);

  const ok =
    appRecognitionVerdict === "PLAY_RECOGNIZED" &&
    deviceRecognitionVerdicts.length > 0 &&
    licensingVerdict !== "UNLICENSED";

  return {
    ok,
    packageName: input.packageName,
    appRecognitionVerdict,
    deviceRecognitionVerdicts,
    licensingVerdict,
    nonce,
    timestampMillis,
    raw: payload,
  };
}

function isPostVisibleToViewer(post: Record<string, unknown>, viewerUid: string | null): boolean {
  const visibility = asString(post.visibility, "public");
  if (visibility === "public") return true;
  if (!viewerUid) return false;
  return viewerUid === asString(post.authorUid);
}

function toPostResponse(
  snap: admin.firestore.QueryDocumentSnapshot | admin.firestore.DocumentSnapshot,
  viewerState: { liked: boolean; reposted: boolean; saved: boolean } = { liked: false, reposted: false, saved: false }
): Record<string, unknown> {
  const data = (snap.data() || {}) as Record<string, unknown>;
  const createdAtMs = getTimestampMs(data.createdAt);
  const updatedAtMs = getTimestampMs(data.updatedAt || data.createdAt);
  const counts = normalizeCounts(data.counts);

  return {
    id: snap.id,
    type: asString(data.type, "forecast"),
    authorUid: asString(data.authorUid),
    authorHandle: asString(data.authorHandle),
    authorPhotoURL: asString(data.authorPhotoURL),
    title: asString(data.title),
    caption: asString(data.caption),
    tickers: Array.isArray(data.tickers) ? data.tickers : [],
    tags: Array.isArray(data.tags) ? data.tags : [],
    preview: data.preview || { kind: "summary" },
    targetUrl: asString(data.targetUrl, "/explore"),
    visibility: asString(data.visibility, "public"),
    createdAt: new Date(createdAtMs).toISOString(),
    createdAtMs,
    updatedAt: new Date(updatedAtMs).toISOString(),
    counts,
    score: asFinite(data.score, 0),
    lastEngagedAtMs: getTimestampMs(data.lastEngagedAt || data.createdAt),
    viewer: {
      liked: viewerState.liked,
      reposted: viewerState.reposted,
      saved: viewerState.saved,
    },
  };
}

async function fetchViewerEngagement(
  postIds: string[],
  viewerUid: string | null
): Promise<Map<string, { liked: boolean; reposted: boolean; saved: boolean }>> {
  const engagement = new Map<string, { liked: boolean; reposted: boolean; saved: boolean }>();
  postIds.forEach((id) => engagement.set(id, { liked: false, reposted: false, saved: false }));

  if (!viewerUid || postIds.length === 0) return engagement;

  const likeRefs = postIds.map((postId) => db.collection("postLikes").doc(postId).collection("users").doc(viewerUid));
  const repostRefs = postIds.map((postId) => db.collection("postReposts").doc(postId).collection("users").doc(viewerUid));
  const savedRefs = postIds.map((postId) => db.collection("users").doc(viewerUid).collection("saved_post_state").doc(postId));

  const [likeDocs, repostDocs, savedDocs] = await Promise.all([
    db.getAll(...likeRefs),
    db.getAll(...repostRefs),
    db.getAll(...savedRefs),
  ]);

  likeDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false, saved: false };
    if (doc.exists) current.liked = true;
    engagement.set(postId, current);
  });

  repostDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false, saved: false };
    if (doc.exists) current.reposted = true;
    engagement.set(postId, current);
  });

  savedDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false, saved: false };
    const data = (doc.data() || {}) as Record<string, unknown>;
    current.saved = asBoolean(data.saved, false);
    engagement.set(postId, current);
  });

  return engagement;
}

function buildNextCursor(item: Record<string, unknown>, includeScore: boolean): ExploreCursor {
  const cursor: ExploreCursor = {
    id: asString(item.id),
    createdAtMs: asFinite(item.createdAtMs, Date.now()),
  };
  if (includeScore) cursor.score = asFinite(item.score, 0);
  return cursor;
}

async function listFollowingPosts(
  viewerUid: string,
  limit: number,
  cursor: ExploreCursor | null,
  tickerFilter: string,
  queryText: string
): Promise<{ posts: admin.firestore.QueryDocumentSnapshot[]; nextCursor: string | null }> {
  const followsSnap = await db.collection("users").doc(viewerUid).collection("follows").limit(250).get();
  const followedAuthorUids = followsSnap.docs.map((doc) => doc.id).filter(Boolean);

  if (followedAuthorUids.length === 0) {
    return { posts: [], nextCursor: null };
  }

  const chunks: string[][] = [];
  for (let i = 0; i < followedAuthorUids.length; i += 10) {
    chunks.push(followedAuthorUids.slice(i, i + 10));
  }

  const perChunkLimit = Math.max(limit, 20);
  const snapshots = await Promise.all(
    chunks.map((authorChunk) =>
      db
        .collection("posts")
        .where("visibility", "==", "public")
        .where("authorUid", "in", authorChunk)
        .orderBy("createdAt", "desc")
        .orderBy(admin.firestore.FieldPath.documentId(), "desc")
        .limit(perChunkLimit)
        .get()
    )
  );

  const dedup = new Map<string, admin.firestore.QueryDocumentSnapshot>();
  snapshots.forEach((snap) => {
    snap.docs.forEach((doc) => {
      if (!dedup.has(doc.id)) dedup.set(doc.id, doc);
    });
  });

  let docs = Array.from(dedup.values());

  if (tickerFilter) {
    docs = docs.filter((doc) => {
      const tickers = (doc.data().tickers || []) as string[];
      return tickers.includes(tickerFilter);
    });
  }

  if (queryText.startsWith("@")) {
    const handle = normalizeHandle(queryText.slice(1));
    docs = docs.filter((doc) => normalizeHandle(doc.data().authorHandle) === handle);
  }

  docs.sort((a, b) => {
    const aMs = getTimestampMs(a.data().createdAt);
    const bMs = getTimestampMs(b.data().createdAt);
    if (aMs !== bMs) return bMs - aMs;
    return b.id.localeCompare(a.id);
  });

  if (cursor) {
    docs = docs.filter((doc) => {
      const ms = getTimestampMs(doc.data().createdAt);
      if (ms < cursor.createdAtMs) return true;
      if (ms > cursor.createdAtMs) return false;
      return doc.id < cursor.id;
    });
  }

  const sliced = docs.slice(0, limit + 1);
  const hasMore = sliced.length > limit;
  const page = hasMore ? sliced.slice(0, limit) : sliced;
  const lastDoc = page[page.length - 1];

  const nextCursor = hasMore && lastDoc
    ? encodeCursor({
        id: lastDoc.id,
        createdAtMs: getTimestampMs(lastDoc.data().createdAt),
      })
    : null;

  return {
    posts: page,
    nextCursor,
  };
}

async function updatePostEngagement(
  postId: string,
  updateFn: (tx: admin.firestore.Transaction, postRef: admin.firestore.DocumentReference, data: Record<string, unknown>) => Promise<Record<string, unknown>>
): Promise<Record<string, unknown>> {
  const postRef = db.collection("posts").doc(postId);

  return db.runTransaction(async (tx) => {
    const postSnap = await tx.get(postRef);
    if (!postSnap.exists) {
      throw new Error("not_found");
    }

    const postData = (postSnap.data() || {}) as Record<string, unknown>;
    const visibility = asString(postData.visibility, "public");
    if (visibility === "deleted") {
      throw new Error("gone");
    }

    const next = await updateFn(tx, postRef, postData);
    return next;
  });
}

async function syncTopicsForUser(uid: string): Promise<void> {
  const userRef = db.collection("users").doc(uid);
  const [userSnap, tokenSnap, followsSnap, watchSnap] = await Promise.all([
    userRef.get(),
    userRef.collection("fcmTokens").limit(100).get(),
    userRef.collection("follows").limit(200).get(),
    userRef.collection("watchTickers").limit(200).get(),
  ]);

  if (tokenSnap.empty) return;

  const prefs = normalizeNotificationPrefs({}, (((userSnap.data() || {}) as any).notificationPrefs || {}) as Record<string, unknown>);
  const enableGlobal = prefs.global;
  const enableFollowing = prefs.following;
  const enableTickers = prefs.tickers;

  const desiredTopics = new Set<string>();
  if (enableGlobal) desiredTopics.add("explore-global");
  if (enableFollowing) {
    followsSnap.docs.forEach((doc) => {
      desiredTopics.add(`author-${doc.id}`);
    });
  }
  if (enableTickers) {
    watchSnap.docs.forEach((doc) => {
      const ticker = normalizeTicker(doc.id || doc.data().ticker);
      if (ticker) desiredTopics.add(`ticker-${ticker}`);
    });
  }

  for (const tokenDoc of tokenSnap.docs) {
    const token = tokenDoc.id;
    if (!token) continue;

    const currentTopics = new Set<string>(Array.isArray(tokenDoc.data().topics) ? tokenDoc.data().topics : []);
    const toSubscribe = Array.from(desiredTopics).filter((topic) => !currentTopics.has(topic));
    const toUnsubscribe = Array.from(currentTopics).filter((topic) => !desiredTopics.has(topic));

    for (const topic of toSubscribe) {
      try {
        await messaging.subscribeToTopic([token], topic);
      } catch (error) {
        console.warn(`[Explore] subscribeToTopic failed (${topic})`, error);
      }
    }

    for (const topic of toUnsubscribe) {
      try {
        await messaging.unsubscribeFromTopic([token], topic);
      } catch (error) {
        console.warn(`[Explore] unsubscribeFromTopic failed (${topic})`, error);
      }
    }

    await tokenDoc.ref.set(
      {
        topics: Array.from(desiredTopics),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  }
}

async function writeNotificationHistoryItem(params: {
  uid: string;
  category: NotificationCategory;
  title: string;
  body: string;
  deepLink: string;
  hidden?: boolean;
  nextSteps?: string[];
  metadata?: Record<string, unknown>;
}): Promise<string> {
  const ref = db.collection("notifications").doc(params.uid).collection("items").doc();
  await ref.set(
    {
      category: params.category,
      title: sanitizeText(params.title, 160) || `${notificationCategoryLabel(params.category)} update`,
      body: sanitizeText(params.body, 400),
      deepLink: normalizeNotificationDeepLink(params.deepLink),
      hidden: Boolean(params.hidden),
      read: false,
      nextSteps: Array.isArray(params.nextSteps)
        ? params.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean).slice(0, 4)
        : [],
      metadata: params.metadata && typeof params.metadata === "object" ? params.metadata : {},
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  return ref.id;
}

async function sendPushToUserTokens(params: {
  uid: string;
  category: NotificationCategory;
  title: string;
  body: string;
  deepLink: string;
  hidden?: boolean;
  nextSteps?: string[];
  metadata?: Record<string, unknown>;
  force?: boolean;
}): Promise<{ uid: string; delivered: number; attempted: number; skipped: boolean; reason: string; historyId: string }> {
  const uid = sanitizeText(params.uid, 220);
  if (!uid) {
    return { uid: "", delivered: 0, attempted: 0, skipped: true, reason: "invalid_uid", historyId: "" };
  }

  const userRef = db.collection("users").doc(uid);
  const [userSnap, tokenSnap] = await Promise.all([userRef.get(), userRef.collection("fcmTokens").limit(250).get()]);
  const userData = (userSnap.data() || {}) as Record<string, unknown>;
  const prefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
  const category = normalizeNotificationCategory(params.category);
  if (!params.force && !isNotificationCategoryEnabled(prefs, category)) {
    return { uid, delivered: 0, attempted: 0, skipped: true, reason: "pref_disabled", historyId: "" };
  }

  const deepLink = normalizeNotificationDeepLink(params.deepLink);
  const absoluteLink = absoluteNotificationLink(deepLink);
  const path = deepLink.startsWith("/") ? deepLink : (() => {
    try {
      const parsed = new URL(deepLink);
      return `${parsed.pathname || "/"}${parsed.search || ""}${parsed.hash || ""}`;
    } catch {
      return "/notifications";
    }
  })();

  const historyId = await writeNotificationHistoryItem({
    uid,
    category,
    title: params.title,
    body: params.body,
    deepLink,
    hidden: params.hidden ?? (category === "inactive"),
    nextSteps: params.nextSteps,
    metadata: params.metadata,
  });

  const tokens = tokenSnap.docs.map((doc) => sanitizeText(doc.id, 4096)).filter(Boolean);
  if (!tokens.length) {
    return { uid, delivered: 0, attempted: 0, skipped: true, reason: "no_tokens", historyId };
  }

  const message = {
    tokens,
    notification: {
      title: sanitizeText(params.title, 160) || `${notificationCategoryLabel(category)} update`,
      body: sanitizeText(params.body, 300) || "You have a new Quantura update.",
    },
    data: {
      category,
      path,
      deepLink,
      url: absoluteLink,
      historyId,
    },
    webpush: {
      fcmOptions: {
        link: absoluteLink,
      },
    },
    android: {
      notification: {
        channelId: "quantura_push",
      },
    },
    apns: {
      payload: {
        aps: {
          sound: "default",
        },
      },
    },
  };

  const result = await messaging.sendEachForMulticast(message);
  const invalidCodes = new Set([
    "messaging/registration-token-not-registered",
    "messaging/invalid-registration-token",
    "messaging/invalid-argument",
  ]);

  await Promise.all(
    result.responses.map((response, index) => {
      if (response.success) return Promise.resolve();
      const code = sanitizeText((response.error as any)?.code, 80);
      if (!invalidCodes.has(code)) return Promise.resolve();
      const token = tokens[index];
      if (!token) return Promise.resolve();
      return userRef.collection("fcmTokens").doc(token).delete().catch(() => undefined);
    })
  );

  return {
    uid,
    delivered: result.successCount,
    attempted: tokens.length,
    skipped: false,
    reason: "",
    historyId,
  };
}

async function collectUsersByFollowedAuthor(authorUid: string): Promise<Set<string>> {
  const clean = sanitizeText(authorUid, 220);
  const out = new Set<string>();
  if (!clean) return out;
  const followsSnap = await db
    .collectionGroup("follows")
    .where(admin.firestore.FieldPath.documentId(), "==", clean)
    .limit(600)
    .get();
  followsSnap.docs.forEach((doc) => {
    const uid = doc.ref.parent.parent?.id || "";
    if (uid) out.add(uid);
  });
  return out;
}

async function collectUsersByWatchTickers(tickers: string[]): Promise<Set<string>> {
  const cleanTickers = Array.from(new Set((Array.isArray(tickers) ? tickers : []).map((item) => normalizeTicker(item)).filter(Boolean)))
    .slice(0, 6);
  const out = new Set<string>();
  for (const ticker of cleanTickers) {
    const watchSnap = await db
      .collectionGroup("watchTickers")
      .where(admin.firestore.FieldPath.documentId(), "==", ticker)
      .limit(800)
      .get();
    watchSnap.docs.forEach((doc) => {
      const uid = doc.ref.parent.parent?.id || "";
      if (uid) out.add(uid);
    });
  }
  return out;
}

async function collectUsersForIpoNotifications(limit = 1200): Promise<Set<string>> {
  const out = new Set<string>();
  const snap = await db.collection("users").limit(Math.max(1, Math.min(limit, 2000))).get();
  snap.docs.forEach((doc) => out.add(doc.id));
  return out;
}

async function deleteCollectionDocs(query: admin.firestore.Query, batchSize = 200): Promise<void> {
  while (true) {
    const snap = await query.limit(batchSize).get();
    if (snap.empty) return;
    const batch = db.batch();
    snap.docs.forEach((doc) => batch.delete(doc.ref));
    await batch.commit();
    if (snap.size < batchSize) return;
  }
}

function isAdminEmail(email: unknown): boolean {
  return asString(email).trim().toLowerCase() === ADMIN_EMAIL;
}

function systemFolderById(folderId: string): SystemFolderConfig | null {
  return SYSTEM_FOLDERS.find((folder) => folder.id === folderId) || null;
}

async function inferPremiumUser(uid: string): Promise<boolean> {
  try {
    const orders = await db
      .collection("orders")
      .where("userId", "==", uid)
      .orderBy("createdAt", "desc")
      .limit(30)
      .get();
    return orders.docs.some((doc) => {
      const data = doc.data() || {};
      const paymentStatus = asString(data.paymentStatus).trim().toLowerCase();
      const stripePaymentStatus = asString(data.stripePaymentStatus).trim().toLowerCase();
      const status = asString(data.status).trim().toLowerCase();
      return ["paid", "succeeded", "complete", "completed", "active"].includes(paymentStatus)
        || ["paid", "succeeded", "complete", "completed", "active"].includes(stripePaymentStatus)
        || ["paid", "completed", "active"].includes(status);
    });
  } catch {
    return false;
  }
}

async function buildProfilePayload(
  userDocId: string,
  userData: Record<string, unknown>,
  viewerUid: string | null
): Promise<Record<string, unknown>> {
  const profile = (userData.profile || {}) as Record<string, unknown>;
  const handle = normalizeHandle(userData.handle || profile.username || userDocId.slice(0, 12)) || `user-${userDocId.slice(0, 8)}`;
  const isOwner = viewerUid === userDocId;
  const email = asString(userData.email).trim().toLowerCase();
  const isAdmin = isAdminEmail(email);
  const explicitPremium =
    asBoolean(profile.premium, false)
    || asBoolean(userData.premium, false)
    || asBoolean(profile.verified, false)
    || ["go", "plus", "pro", "business", "desk", "premium"].includes(
      asString(profile.plan || userData.plan || userData.subscriptionTier).trim().toLowerCase()
    );
  const premium = explicitPremium ? true : await inferPremiumUser(userDocId);
  const verified = isAdmin || premium || asBoolean(profile.verified, false);
  const publicEmailOptIn = asBoolean(profile.publicEmailOptIn, false);
  const publicProfile = asBoolean(profile.publicProfile, false);
  const photoURL = asString(userData.photoURL || profile.photoURL || profile.avatarUrl || "");
  const name = sanitizeText(userData.name || userData.displayName || profile.name || "", 120);
  const username = normalizeHandle(profile.username || userData.handle || "") || handle;
  const profileUrl = `${PUBLIC_ORIGIN}/u/${encodeURIComponent(handle)}`;

  return {
    uid: userDocId,
    handle,
    username,
    name,
    photoURL,
    bio: sanitizeText(profile.bio || "", 400),
    publicProfile,
    publicEmailOptIn,
    email: isOwner || publicEmailOptIn ? email : "",
    emailVisible: Boolean(isOwner || publicEmailOptIn),
    verified,
    premium,
    isAdmin,
    profileUrl,
    canEdit: isOwner,
  };
}

async function upsertSavedPostState(
  uid: string,
  postId: string,
  patch: Partial<{ liked: boolean; reposted: boolean; saved: boolean; shared: boolean }>
): Promise<void> {
  if (!uid || !postId) return;
  const ref = db.collection("users").doc(uid).collection("saved_post_state").doc(postId);
  const payload: Record<string, unknown> = {
    postId,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  if (typeof patch.liked === "boolean") payload.liked = patch.liked;
  if (typeof patch.reposted === "boolean") payload.reposted = patch.reposted;
  if (typeof patch.saved === "boolean") payload.saved = patch.saved;
  if (typeof patch.shared === "boolean") payload.shared = patch.shared;
  await ref.set(payload, { merge: true });
}

async function listSystemFolderCounts(uid: string): Promise<Record<string, number>> {
  const counts: Record<string, number> = {};
  const base = db.collection("users").doc(uid).collection("saved_post_state");
  await Promise.all(
    SYSTEM_FOLDERS.map(async (folder) => {
      const snap = await base.where(folder.flag, "==", true).limit(400).get();
      counts[folder.id] = snap.size;
    })
  );
  return counts;
}

function buildPostSavedItem(postId: string, data: Record<string, unknown>): Record<string, unknown> {
  const createdAtMs = getTimestampMs(data.createdAt);
  const updatedAtMs = getTimestampMs(data.updatedAt || data.createdAt);
  const tickers = Array.isArray(data.tickers) ? data.tickers : [];
  return {
    itemType: "post",
    sourceId: postId,
    itemId: `post__${postId}`,
    title: asString(data.title, "Explore post"),
    subtitle: asString(data.caption, ""),
    ticker: asString(tickers[0] || ""),
    targetUrl: `/explore?post=${encodeURIComponent(postId)}`,
    createdAtMs,
    updatedAtMs,
    visibility: asString(data.visibility, "public"),
  };
}

async function resolveSavedItem(
  uid: string,
  itemType: SavedItemType,
  sourceId: string
): Promise<Record<string, unknown> | null> {
  const cleanSourceId = normalizeSourceId(sourceId);
  if (!uid || !cleanSourceId) return null;

  if (itemType === "forecast") {
    const snap = await db.collection("forecast_requests").doc(cleanSourceId).get();
    if (!snap.exists) return null;
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (asString(data.userId) !== uid) return null;
    return {
      itemType,
      sourceId: snap.id,
      itemId: buildFolderItemDocId(itemType, snap.id),
      title: asString(data.title, `${normalizeTicker(data.ticker)} forecast`),
      subtitle: asString(data.serviceMessage || data.notes || ""),
      ticker: normalizeTicker(data.ticker),
      targetUrl: `/forecasting?forecastId=${encodeURIComponent(snap.id)}`,
      createdAtMs: getTimestampMs(data.createdAt),
      updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
    };
  }

  if (itemType === "screener") {
    const snap = await db.collection("screener_runs").doc(cleanSourceId).get();
    if (!snap.exists) return null;
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (asString(data.userId) !== uid) return null;
    return {
      itemType,
      sourceId: snap.id,
      itemId: buildFolderItemDocId(itemType, snap.id),
      title: asString(data.title, "Screener run"),
      subtitle: asString(data.notes || ""),
      ticker: normalizeTicker(((data.results as Array<Record<string, unknown>> | undefined) || [])[0]?.symbol),
      targetUrl: `/screener?runId=${encodeURIComponent(snap.id)}`,
      createdAtMs: getTimestampMs(data.createdAt),
      updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
    };
  }

  if (itemType === "model_council") {
    const snap = await db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).doc(cleanSourceId).get();
    if (!snap.exists) return null;
    const data = (snap.data() || {}) as Record<string, unknown>;
    if (asString(data.userId) !== uid) return null;
    return {
      itemType,
      sourceId: snap.id,
      itemId: buildFolderItemDocId(itemType, snap.id),
      title: `${normalizeTicker(data.ticker) || "Ticker"} Model Council`,
      subtitle: asString(data.question || ""),
      ticker: normalizeTicker(data.ticker),
      targetUrl: `/model-council`,
      createdAtMs: getTimestampMs(data.createdAt),
      updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
    };
  }

  const snap = await db.collection("posts").doc(cleanSourceId).get();
  if (!snap.exists) return null;
  const data = (snap.data() || {}) as Record<string, unknown>;
  if (!isPostVisibleToViewer(data, uid)) return null;
  return buildPostSavedItem(snap.id, data);
}

async function listSystemFolderItems(uid: string, folderId: string, limit: number): Promise<Record<string, unknown>[]> {
  const folder = systemFolderById(folderId);
  if (!folder) return [];
  const stateSnap = await db
    .collection("users")
    .doc(uid)
    .collection("saved_post_state")
    .where(folder.flag, "==", true)
    .limit(Math.max(limit, 80))
    .get();
  if (stateSnap.empty) return [];

  const postRefs = stateSnap.docs.map((doc) => db.collection("posts").doc(doc.id));
  const postDocs = await db.getAll(...postRefs);

  const out = postDocs
    .filter((snap) => snap.exists)
    .map((snap) => {
      const data = (snap.data() || {}) as Record<string, unknown>;
      return buildPostSavedItem(snap.id, data);
    })
    .sort((a, b) => asFinite(b.updatedAtMs, 0) - asFinite(a.updatedAtMs, 0));

  return out.slice(0, limit);
}

function matchesSearchQuery(item: Record<string, unknown>, query: string): boolean {
  const normalized = sanitizeText(query, 140).toLowerCase();
  if (!normalized) return true;
  const haystack = [
    asString(item.title),
    asString(item.subtitle),
    asString(item.ticker),
    asString(item.itemType),
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(normalized);
}

ROUTES.get("/health", (_req, res) => {
  res.status(200).json({ ok: true, service: "quantura-explore-api", ts: new Date().toISOString() });
});

ROUTES.post("/llm/run", async (req, res) => {
  const startedAt = Date.now();
  const requestPayload = asPlainObject(req.body);
  const requestedProvider = normalizeProvider(requestPayload.provider || "openai");
  const fallbackProviders = Array.isArray(requestPayload.fallbackProviders)
    ? requestPayload.fallbackProviders.map((item) => normalizeProvider(item))
    : [];
  const providerChain = Array.from(new Set([requestedProvider, ...fallbackProviders]));
  const retryProvider = providerChain.find((item) => item !== requestedProvider) || "openai";
  const requestedModel = sanitizeText(requestPayload.model, 120) || DEFAULT_LLM_MODEL;
  try {
    const result = await invokeLlmWithFallback(requestPayload);
    res.status(200).json({
      text: result.text,
      model: result.model,
      provider: result.provider,
      latencyMs: Number.isFinite(result.latencyMs) ? result.latencyMs : Date.now() - startedAt,
      usage: result.usage,
      citations: result.citations || [],
      attempted: result.attempted || [],
      disclaimer: "LLMs can sometimes make mistakes.",
    });
  } catch (error: any) {
    const message = sanitizeText(error?.message || error, 220) || "llm_run_failed";
    res.status(502).json({
      text: "",
      model: requestedModel,
      provider: requestedProvider,
      latencyMs: Date.now() - startedAt,
      usage: {},
      citations: [],
      error: message,
      retryProvider,
      retryModel: requestedModel,
    });
  }
});

ROUTES.post("/mobile/play-integrity/verify", async (req, res) => {
  try {
    let user: admin.auth.DecodedIdToken | null = null;
    try {
      user = await verifyRequestUser(req, false);
    } catch (error: any) {
      if (String(error?.message) === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      throw error;
    }

    const body = asPlainObject(req.body);
    const integrityToken = sanitizeText(body.integrityToken || body.token, 12000);
    const providedPackage = sanitizeText(body.packageName, 220);
    const packageName = providedPackage || PLAY_INTEGRITY_ANDROID_PACKAGE;
    const expectedNonce = sanitizeText(body.nonce || body.integrityNonce, 320);

    if (!integrityToken) {
      res.status(400).json({ error: "missing_integrity_token" });
      return;
    }
    if (!packageName) {
      res.status(400).json({ error: "missing_package_name" });
      return;
    }

    const verdict = await decodePlayIntegrityToken({
      integrityToken,
      packageName,
    });
    const packageMatches = !PLAY_INTEGRITY_ANDROID_PACKAGE || packageName === PLAY_INTEGRITY_ANDROID_PACKAGE;
    const nonceMatches = !expectedNonce || verdict.nonce === expectedNonce;
    const ok = verdict.ok && packageMatches && nonceMatches;

    await db.collection("mobile_play_integrity_events").add({
      uid: user?.uid || "",
      packageName,
      expectedNonce,
      nonceMatches,
      packageMatches,
      ok,
      verdict: {
        appRecognitionVerdict: verdict.appRecognitionVerdict,
        deviceRecognitionVerdicts: verdict.deviceRecognitionVerdicts,
        licensingVerdict: verdict.licensingVerdict,
        nonce: verdict.nonce,
        timestampMillis: verdict.timestampMillis,
      },
      ipAddress: requestIpAddress(req),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    if (REQUIRE_PLAY_INTEGRITY && !ok) {
      res.status(403).json({
        error: "play_integrity_failed",
        ok: false,
        packageMatches,
        nonceMatches,
      });
      return;
    }

    res.status(200).json({
      ok,
      packageMatches,
      nonceMatches,
      verdict: {
        appRecognitionVerdict: verdict.appRecognitionVerdict,
        deviceRecognitionVerdicts: verdict.deviceRecognitionVerdicts,
        licensingVerdict: verdict.licensingVerdict,
        nonce: verdict.nonce,
        timestampMillis: verdict.timestampMillis,
      },
    });
  } catch (error: any) {
    console.error("[Mobile] play integrity verify failed", error);
    const detail = sanitizeText(error?.message || error, 220);
    res.status(500).json({ error: "play_integrity_verify_failed", detail });
  }
});

ROUTES.post("/mobile/auth/exchange", async (req, res) => {
  try {
    const nativeIdToken = getBearerToken(req);
    if (!nativeIdToken) {
      res.status(400).json({ error: "missing_bearer_token" });
      return;
    }

    let decoded: admin.auth.DecodedIdToken;
    try {
      decoded = await auth.verifyIdToken(nativeIdToken);
    } catch {
      res.status(401).json({ error: "invalid_token" });
      return;
    }

    const body = asPlainObject(req.body);
    const integrityToken = sanitizeText(body.integrityToken, 12000);
    const expectedNonce = sanitizeText(body.integrityNonce || body.nonce, 320);
    const providedPackage = sanitizeText(body.packageName, 220);
    const packageName = providedPackage || PLAY_INTEGRITY_ANDROID_PACKAGE;

    let integrityChecked = false;
    let integrityOk = true;
    let integrityReason = "not_required";
    let packageMatches = true;
    let nonceMatches = true;
    let integritySummary: Record<string, unknown> = {};

    if (integrityToken) {
      if (!packageName) {
        res.status(400).json({ error: "missing_package_name" });
        return;
      }

      integrityChecked = true;
      const verdict = await decodePlayIntegrityToken({
        integrityToken,
        packageName,
      });
      packageMatches = !PLAY_INTEGRITY_ANDROID_PACKAGE || packageName === PLAY_INTEGRITY_ANDROID_PACKAGE;
      nonceMatches = !expectedNonce || verdict.nonce === expectedNonce;
      integrityOk = verdict.ok && packageMatches && nonceMatches;
      integrityReason = integrityOk ? "passed" : "failed";
      integritySummary = {
        appRecognitionVerdict: verdict.appRecognitionVerdict,
        deviceRecognitionVerdicts: verdict.deviceRecognitionVerdicts,
        licensingVerdict: verdict.licensingVerdict,
        nonce: verdict.nonce,
        timestampMillis: verdict.timestampMillis,
      };
    } else if (REQUIRE_PLAY_INTEGRITY) {
      integrityChecked = true;
      integrityOk = false;
      integrityReason = "missing_integrity_token";
    }

    if (REQUIRE_PLAY_INTEGRITY && !integrityOk) {
      await db.collection("mobile_auth_exchange_events").add({
        uid: decoded.uid,
        status: "blocked",
        reason: integrityReason,
        packageName,
        expectedNonce,
        packageMatches,
        nonceMatches,
        ipAddress: requestIpAddress(req),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      res.status(403).json({
        error: "play_integrity_failed",
        reason: integrityReason,
        packageMatches,
        nonceMatches,
      });
      return;
    }

    const customTokenRaw = await auth.createCustomToken(decoded.uid);
    const customToken = typeof customTokenRaw === "string" ? customTokenRaw : String(customTokenRaw);

    await db.collection("mobile_auth_exchange_events").add({
      uid: decoded.uid,
      status: "issued",
      provider: sanitizeText(decoded.firebase?.sign_in_provider, 80),
      packageName,
      expectedNonce,
      integrityChecked,
      integrityOk,
      integrityReason,
      packageMatches,
      nonceMatches,
      ipAddress: requestIpAddress(req),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    res.status(200).json({
      customToken,
      uid: decoded.uid,
      integrity: {
        checked: integrityChecked,
        ok: integrityOk,
        reason: integrityReason,
        packageMatches,
        nonceMatches,
        summary: integritySummary,
      },
    });
  } catch (error: any) {
    console.error("[Mobile] auth exchange failed", error);
    const detail = sanitizeText(error?.message || error, 220);
    res.status(500).json({ error: "auth_exchange_failed", detail });
  }
});

ROUTES.post("/analytics/ad-impression", async (req, res) => {
  try {
    let user: admin.auth.DecodedIdToken | null = null;
    try {
      user = await verifyRequestUser(req, false);
    } catch (error: any) {
      if (String(error?.message) === "invalid_token") {
        res.status(401).json({ error: "invalid_token" });
        return;
      }
      throw error;
    }

    const body = asPlainObject(req.body);
    const adFormat = normalizeAdFormat(body.adFormat);
    const adUnitId = sanitizeText(body.adUnitId || body.adUnitName, 220);
    const adPlatform = sanitizeText(body.adPlatform || "admob", 60).toLowerCase();
    const adSource = sanitizeText(body.adSource || "admob", 120);
    const placement = sanitizeText(body.placement || body.source || "", 120);
    const impressionId = sanitizeText(body.impressionId, 220);
    const rewardType = sanitizeText(body.rewardType, 120);
    const rewardAmount = Number.isFinite(Number(body.rewardAmount)) ? Number(body.rewardAmount) : null;
    const currency = normalizeCurrency(body.currency);
    const value = Number.isFinite(Number(body.value)) ? Number(body.value) : null;
    const platform = sanitizeText(body.platform, 30).toLowerCase() || "unknown";

    if (!adFormat || adFormat === "unknown") {
      res.status(400).json({ error: "invalid_ad_format" });
      return;
    }

    const docId = impressionId ? sanitizeText(impressionId, 180) : "";
    const collection = db.collection("ad_impressions");
    const docRef = docId ? collection.doc(docId) : collection.doc();
    await docRef.set(
      {
        uid: user?.uid || sanitizeText(body.uid, 220),
        adFormat,
        adUnitId,
        adPlatform,
        adSource,
        placement,
        platform,
        rewardType,
        rewardAmount,
        currency,
        value,
        impressionId: docId || docRef.id,
        deviceId: sanitizeText(body.deviceId, 220),
        appVersion: sanitizeText(body.appVersion, 80),
        ipAddress: requestIpAddress(req),
        userAgent: sanitizeText(req.headers["user-agent"], 300),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.status(200).json({ ok: true, id: docRef.id });
  } catch (error) {
    console.error("[Ads] ad impression callback failed", error);
    res.status(500).json({ error: "ad_impression_failed" });
  }
});

ROUTES.post("/notify/sendTest", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const body = asPlainObject(req.body);
    const explicitTokens = Array.isArray(body.tokens)
      ? body.tokens.map((item) => sanitizeText(item, 4096)).filter(Boolean)
      : [];
    const singleToken = sanitizeText(body.token, 4096);
    if (singleToken) explicitTokens.push(singleToken);

    let tokens = Array.from(new Set(explicitTokens));
    if (!tokens.length) {
      const tokenSnap = await db.collection("users").doc(user.uid).collection("fcmTokens").limit(100).get();
      tokens = tokenSnap.docs.map((doc) => sanitizeText(doc.id, 4096)).filter(Boolean);
    }

    if (!tokens.length) {
      res.status(404).json({ error: "no_tokens_available" });
      return;
    }

    const title = sanitizeText(body.title, 120) || "Quantura test notification";
    const message = sanitizeText(body.message, 240) || "Push delivery check from Quantura.";
    const targetPath = sanitizeText(body.path || "/notifications", 280);

    const result = await messaging.sendEachForMulticast({
      tokens,
      notification: {
        title,
        body: message,
      },
      data: {
        type: "test",
        path: targetPath,
      },
    });

    res.status(200).json({
      ok: true,
      requested: tokens.length,
      successCount: result.successCount,
      failureCount: result.failureCount,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] sendTest failed", error);
    res.status(500).json({ error: "notify_send_test_failed" });
  }
});

ROUTES.post("/earnings/refresh", async (req, res) => {
  try {
    if (!FMP_API_KEY) {
      res.status(503).json({ error: "missing_fmp_api_key" });
      return;
    }

    const body = asPlainObject(req.body);
    const symbol = normalizeTicker(body.symbol || body.ticker);
    if (!symbol) {
      res.status(400).json({ error: "symbol_required" });
      return;
    }
    const start = sanitizeText(body.start || body.from, 20) || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const end = sanitizeText(body.end || body.to, 20) || new Date(Date.now() + 120 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

    const docRef = db.collection("earningsCalendar").doc(symbol);
    const existingSnap = await docRef.get();
    const existing = (existingSnap.data() || {}) as Record<string, unknown>;
    const lastFetchedAtMs = getTimestampMs(existing.lastFetchedAt);
    const hasRecentCache = Array.isArray(existing.items) && Date.now() - lastFetchedAtMs < 7 * 24 * 60 * 60 * 1000;
    if (hasRecentCache) {
      res.status(200).json({
        ok: true,
        symbol,
        cached: true,
        lastFetchedAtMs,
        items: existing.items,
        lastUpdated: sanitizeText(existing.lastUpdated, 30),
      });
      return;
    }

    const candidateUrls = [
      `https://financialmodelingprep.com/stable/earnings-calendar?symbol=${encodeURIComponent(symbol)}&from=${encodeURIComponent(start)}&to=${encodeURIComponent(end)}&apikey=${encodeURIComponent(FMP_API_KEY)}`,
      `https://financialmodelingprep.com/api/v3/earning_calendar?from=${encodeURIComponent(start)}&to=${encodeURIComponent(end)}&apikey=${encodeURIComponent(FMP_API_KEY)}`,
    ];

    let records: Array<Record<string, unknown>> = [];
    let fetchedFrom = "";
    for (const endpoint of candidateUrls) {
      const response = await fetch(endpoint, { method: "GET" });
      if (!response.ok) continue;
      const rows = (await response.json().catch(() => [])) as Array<Record<string, unknown>>;
      const filtered = (Array.isArray(rows) ? rows : []).filter((row) => normalizeTicker(row.symbol || row.ticker) === symbol);
      if (filtered.length) {
        records = filtered;
        fetchedFrom = endpoint.includes("/stable/") ? "stable" : "v3";
        break;
      }
    }

    const items = records
      .map((row) => ({
        symbol,
        date: sanitizeText(row.date, 20),
        epsActual: Number.isFinite(Number((row as any).epsActual)) ? Number((row as any).epsActual) : null,
        epsEstimated: Number.isFinite(Number((row as any).epsEstimated)) ? Number((row as any).epsEstimated) : null,
        revenueActual: Number.isFinite(Number((row as any).revenueActual)) ? Number((row as any).revenueActual) : null,
        revenueEstimated: Number.isFinite(Number((row as any).revenueEstimated)) ? Number((row as any).revenueEstimated) : null,
        lastUpdated: sanitizeText((row as any).lastUpdated, 20),
      }))
      .filter((row) => row.date)
      .sort((a, b) => a.date.localeCompare(b.date))
      .slice(0, 160);

    const lastUpdated = items
      .map((item) => item.lastUpdated)
      .filter(Boolean)
      .sort()
      .pop() || "";

    await docRef.set(
      {
        symbol,
        start,
        end,
        source: "fmp",
        sourceVariant: fetchedFrom || "none",
        lastUpdated,
        lastFetchedAt: admin.firestore.FieldValue.serverTimestamp(),
        items,
      },
      { merge: true }
    );

    res.status(200).json({
      ok: true,
      symbol,
      start,
      end,
      cached: false,
      fetchedCount: items.length,
      lastUpdated,
      lastFetchedAtMs: Date.now(),
      items,
    });
  } catch (error: any) {
    console.error("[Earnings] refresh failed", error);
    res.status(500).json({ error: "earnings_refresh_failed" });
  }
});

ROUTES.post("/polymarket/search", async (req, res) => {
  try {
    const body = asPlainObject(req.body);
    const query = sanitizeText(body.q, 120);
    if (!query) {
      res.status(400).json({ error: "query_required" });
      return;
    }
    const limitPerType = Math.max(1, Math.min(60, Math.floor(asFinite(body.limitPerType, 20))));
    const includeClosed = asBoolean(body.includeClosed, false);
    const sort = normalizePolymarketSort(body.sort);
    const cacheKey = `search::${query.toLowerCase()}::${limitPerType}::${includeClosed ? 1 : 0}::${sort}`;
    const cached = getPolymarketCache(cacheKey);
    if (cached) {
      res.status(200).json(cached);
      return;
    }

    const payload = await fetchGammaJson("/public-search", {
      q: query,
      limit_per_type: limitPerType,
      keep_closed_markets: includeClosed ? 1 : 0,
    });
    const normalized = normalizePolymarketResponse(payload, {
      query,
      sort,
      includeClosed,
    });
    setPolymarketCache(cacheKey, normalized);
    res.status(200).json(normalized);
  } catch (error: any) {
    console.error("[Polymarket] search failed", error);
    res.status(502).json({
      error: "polymarket_search_failed",
      detail: sanitizeText(error?.message, 180) || "Unable to fetch Polymarket markets.",
    });
  }
});

ROUTES.post("/polymarket/active", async (req, res) => {
  try {
    const body = asPlainObject(req.body);
    const limit = Math.max(1, Math.min(60, Math.floor(asFinite(body.limit, 24))));
    const offset = Math.max(0, Math.min(2000, Math.floor(asFinite(body.offset, 0))));
    const sort = normalizePolymarketSort(body.sort || "volume");
    const cacheKey = `active::${limit}::${offset}::${sort}`;
    const cached = getPolymarketCache(cacheKey);
    if (cached) {
      res.status(200).json(cached);
      return;
    }

    const payload = await fetchGammaJson("/events", {
      active: true,
      closed: false,
      limit,
      offset,
    });
    const normalized = normalizePolymarketResponse(payload, {
      query: "top-active",
      sort,
      includeClosed: false,
    });
    setPolymarketCache(cacheKey, normalized);
    res.status(200).json(normalized);
  } catch (error: any) {
    console.error("[Polymarket] active failed", error);
    res.status(502).json({
      error: "polymarket_active_failed",
      detail: sanitizeText(error?.message, 180) || "Unable to fetch active markets.",
    });
  }
});

ROUTES.post("/webhooks/inapppurchasesios", async (req, res) => {
  try {
    if (!checkWebhookSecret(req, IOS_IAP_WEBHOOK_SECRET)) {
      res.status(401).json({ error: "invalid_webhook_secret" });
      return;
    }
    await db.collection("webhook_ios_iap").add(summarizeWebhookPayload(req));
    res.status(200).json({ ok: true });
  } catch (error) {
    console.error("[Webhook] inapppurchasesios failed", error);
    res.status(500).json({ error: "webhook_store_failed" });
  }
});

ROUTES.post("/webhooks/applenotifications", async (req, res) => {
  try {
    if (!checkWebhookSecret(req, APPLE_NOTIFICATIONS_WEBHOOK_SECRET)) {
      res.status(401).json({ error: "invalid_webhook_secret" });
      return;
    }
    await db.collection("webhook_apple_notifications").add(summarizeWebhookPayload(req));
    res.status(200).json({ ok: true });
  } catch (error) {
    console.error("[Webhook] applenotifications failed", error);
    res.status(500).json({ error: "webhook_store_failed" });
  }
});

ROUTES.post("/webhooks/admob/reward", async (req, res) => {
  try {
    if (!checkWebhookSecret(req, ADMOB_SSV_WEBHOOK_SECRET)) {
      res.status(401).json({ error: "invalid_webhook_secret" });
      return;
    }
    const query = asPlainObject(req.query);
    const body = asPlainObject(req.body);
    const rewardAmount = asFinite(query.reward_amount || body.reward_amount, NaN);
    const rewardType = sanitizeText(query.reward_type || body.reward_type, 120);
    const adUnit = sanitizeText(query.ad_unit || body.ad_unit, 220);
    const userId = sanitizeText(query.user_id || body.user_id, 220);
    const customData = sanitizeText(query.custom_data || body.custom_data, 1200);

    await db.collection("webhook_admob_ssv").add({
      ...summarizeWebhookPayload(req),
      rewardAmount: Number.isFinite(rewardAmount) ? rewardAmount : null,
      rewardType,
      adUnit,
      userId,
      customData,
      transactionId: sanitizeText(query.transaction_id || body.transaction_id, 220),
      adNetwork: sanitizeText(query.ad_network || body.ad_network, 120),
      timestamp: sanitizeText(query.timestamp || body.timestamp, 40),
      signature: sanitizeText(query.signature || body.signature, 600),
      keyId: sanitizeText(query.key_id || body.key_id, 120),
    });

    res.status(200).send("ok");
  } catch (error) {
    console.error("[Webhook] admob reward failed", error);
    res.status(500).json({ error: "webhook_store_failed" });
  }
});

ROUTES.get("/explore/suggestions", async (req, res) => {
  try {
    const query = sanitizeText(req.query.query, 32).toUpperCase();
    const viewer = await verifyRequestUser(req, false).catch(() => null);

    const popSnap = await db
      .collection("posts")
      .where("visibility", "==", "public")
      .orderBy("createdAt", "desc")
      .limit(120)
      .get();

    const counts = new Map<string, number>();
    popSnap.docs.forEach((doc) => {
      const tickers = Array.isArray(doc.data().tickers) ? (doc.data().tickers as string[]) : [];
      tickers.forEach((ticker) => {
        const clean = normalizeTicker(ticker);
        if (!clean) return;
        counts.set(clean, (counts.get(clean) || 0) + 1);
      });
    });

    if (viewer?.uid) {
      const watchSnap = await db.collection("users").doc(viewer.uid).collection("watchTickers").limit(100).get();
      watchSnap.docs.forEach((doc) => {
        const clean = normalizeTicker(doc.id || doc.data().ticker);
        if (!clean) return;
        counts.set(clean, (counts.get(clean) || 0) + 20);
      });
    }

    const suggestions = Array.from(counts.entries())
      .filter(([ticker]) => (query ? ticker.startsWith(query) : true))
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 15)
      .map(([ticker]) => ticker);

    res.status(200).json({ suggestions });
  } catch (error) {
    console.error("[Explore] suggestions failed", error);
    res.status(500).json({ error: "suggestions_failed" });
  }
});

ROUTES.get("/explore", async (req, res) => {
  try {
    const modeRaw = sanitizeText(req.query.mode, 24).toLowerCase();
    const mode: "trending" | "latest" | "following" | "tickers" =
      modeRaw === "latest" || modeRaw === "following" || modeRaw === "tickers" ? (modeRaw as any) : "trending";

    const limit = parseLimit(req.query.limit);
    const cursor = decodeCursor(req.query.cursor);
    const tickerFilter = normalizeTicker(req.query.ticker);
    const queryText = sanitizeText(req.query.q, 80);

    const viewer = await verifyRequestUser(req, false).catch((err) => {
      if (String(err?.message) === "invalid_token") {
        throw new Error("invalid_token");
      }
      return null;
    });

    if (mode === "following" && !viewer?.uid) {
      res.status(401).json({ error: "auth_required_for_following" });
      return;
    }

    let docs: admin.firestore.QueryDocumentSnapshot[] = [];
    let nextCursor: string | null = null;

    if (mode === "following" && viewer?.uid) {
      const followingPage = await listFollowingPosts(viewer.uid, limit, cursor, tickerFilter, queryText);
      docs = followingPage.posts;
      nextCursor = followingPage.nextCursor;
    } else {
      const searchMode = queryText.startsWith("#")
        ? "tag"
        : queryText.startsWith("@")
        ? "author"
        : /^[A-Za-z.\-]{1,12}$/.test(queryText)
        ? "ticker"
        : "none";

      let queryRef: admin.firestore.Query = db.collection("posts").where("visibility", "==", "public");

      if (mode === "tickers") {
        if (tickerFilter) {
          queryRef = queryRef.where("tickers", "array-contains", tickerFilter);
        }
      } else if (searchMode === "ticker") {
        queryRef = queryRef.where("tickers", "array-contains", normalizeTicker(queryText));
      } else if (searchMode === "tag") {
        const tag = normalizeHandle(queryText.slice(1));
        if (tag) queryRef = queryRef.where("tags", "array-contains", tag);
      } else if (searchMode === "author") {
        const handle = normalizeHandle(queryText.slice(1));
        if (handle) queryRef = queryRef.where("authorHandle", "==", handle);
      } else if (tickerFilter) {
        queryRef = queryRef.where("tickers", "array-contains", tickerFilter);
      }

      const usingTrending = mode === "trending";
      if (usingTrending) {
        queryRef = queryRef
          .orderBy("score", "desc")
          .orderBy("createdAt", "desc")
          .orderBy(admin.firestore.FieldPath.documentId(), "desc");

        if (cursor) {
          queryRef = queryRef.startAfter(
            asFinite(cursor.score, 0),
            timestampFromMs(cursor.createdAtMs),
            cursor.id
          );
        }
      } else {
        queryRef = queryRef
          .orderBy("createdAt", "desc")
          .orderBy(admin.firestore.FieldPath.documentId(), "desc");

        if (cursor) {
          queryRef = queryRef.startAfter(timestampFromMs(cursor.createdAtMs), cursor.id);
        }
      }

      const snap = await queryRef.limit(limit + 1).get();
      const pageDocs = snap.docs.slice(0, limit);
      const hasMore = snap.docs.length > limit;
      docs = pageDocs;

      if (hasMore && pageDocs.length) {
        const last = pageDocs[pageDocs.length - 1];
        const postData = toPostResponse(last);
        nextCursor = encodeCursor(buildNextCursor(postData, usingTrending));
      }
    }

    const visibleDocs = docs.filter((doc) => isPostVisibleToViewer(doc.data() as Record<string, unknown>, viewer?.uid || null));
    const postIds = visibleDocs.map((doc) => doc.id);
    const engagement = await fetchViewerEngagement(postIds, viewer?.uid || null);

    const posts = visibleDocs.map((doc) => {
      const viewerState = engagement.get(doc.id) || { liked: false, reposted: false, saved: false };
      return toPostResponse(doc, viewerState);
    });

    res.status(200).json({
      mode,
      count: posts.length,
      cursor: nextCursor,
      posts,
    });
  } catch (error: any) {
    if (String(error?.message) === "invalid_token") {
      res.status(401).json({ error: "invalid_token" });
      return;
    }
    console.error("[Explore] list failed", error);
    res.status(500).json({ error: "explore_fetch_failed" });
  }
});

ROUTES.get("/posts/:postId", async (req, res) => {
  try {
    const postId = sanitizeText(req.params.postId, 180);
    if (!postId) {
      res.status(400).json({ error: "invalid_post_id" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const postSnap = await db.collection("posts").doc(postId).get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const data = (postSnap.data() || {}) as Record<string, unknown>;
    if (!isPostVisibleToViewer(data, viewer?.uid || null)) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    const [commentsSnap, engagement] = await Promise.all([
      db.collection("posts").doc(postId).collection("comments").orderBy("createdAt", "desc").limit(100).get(),
      fetchViewerEngagement([postId], viewer?.uid || null),
    ]);

    const comments = commentsSnap.docs.map((commentDoc) => {
      const comment = commentDoc.data() || {};
      const createdAtMs = getTimestampMs(comment.createdAt);
      return {
        id: commentDoc.id,
        authorUid: asString(comment.authorUid),
        authorHandle: asString(comment.authorHandle),
        text: asString(comment.text),
        createdAt: new Date(createdAtMs).toISOString(),
        createdAtMs,
      };
    });

    const viewerState = engagement.get(postId) || { liked: false, reposted: false, saved: false };

    res.status(200).json({
      post: toPostResponse(postSnap, viewerState),
      comments,
    });
  } catch (error) {
    console.error("[Explore] detail failed", error);
    res.status(500).json({ error: "post_detail_failed" });
  }
});

ROUTES.post("/posts/:postId/like", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    if (!user || !postId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const likeRef = db.collection("postLikes").doc(postId).collection("users").doc(user.uid);

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const likeSnap = await tx.get(likeRef);
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);

      let liked = false;
      if (likeSnap.exists) {
        tx.delete(likeRef);
        counts.likes = Math.max(0, counts.likes - 1);
      } else {
        liked = true;
        tx.set(likeRef, {
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        counts.likes += 1;
      }

      const score = computeScore(counts, createdAtMs);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        liked,
        counts,
        score,
      };
    });

    await upsertSavedPostState(user.uid, postId, { liked: asBoolean((result as any).liked, false) });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] like failed", error);
    res.status(500).json({ error: "like_failed" });
  }
});

ROUTES.post("/posts/:postId/repost", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    if (!user || !postId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const repostRef = db.collection("postReposts").doc(postId).collection("users").doc(user.uid);

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const repostSnap = await tx.get(repostRef);
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);

      let reposted = false;
      if (repostSnap.exists) {
        tx.delete(repostRef);
        counts.reposts = Math.max(0, counts.reposts - 1);
      } else {
        reposted = true;
        tx.set(repostRef, {
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        counts.reposts += 1;
      }

      const score = computeScore(counts, createdAtMs);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        reposted,
        counts,
        score,
      };
    });

    await upsertSavedPostState(user.uid, postId, { reposted: asBoolean((result as any).reposted, false) });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] repost failed", error);
    res.status(500).json({ error: "repost_failed" });
  }
});

ROUTES.post("/posts/:postId/share", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, false);
    const postId = sanitizeText(req.params.postId, 180);
    if (!postId) {
      res.status(400).json({ error: "invalid_post_id" });
      return;
    }

    const shareEventRef = db.collection("postShareEvents").doc();

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);
      counts.shares += 1;
      const score = computeScore(counts, createdAtMs);

      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      tx.set(shareEventRef, {
        postId,
        uid: user?.uid || null,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        source: sanitizeText((req.body || {}).source, 80) || "web",
      });

      return {
        shared: true,
        counts,
        score,
      };
    });

    if (user?.uid) {
      await upsertSavedPostState(user.uid, postId, { shared: true });
    }

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] share failed", error);
    res.status(500).json({ error: "share_failed" });
  }
});

ROUTES.post("/posts/:postId/save", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    if (!user || !postId) {
      res.status(400).json({ error: "invalid_post_id" });
      return;
    }

    const postSnap = await db.collection("posts").doc(postId).get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }
    const postData = (postSnap.data() || {}) as Record<string, unknown>;
    if (!isPostVisibleToViewer(postData, user.uid)) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    const stateRef = db.collection("users").doc(user.uid).collection("saved_post_state").doc(postId);
    const stateSnap = await stateRef.get();
    const currentSaved = asBoolean((stateSnap.data() || {}).saved, false);
    const explicit = (req.body || {}).save;
    const nextSaved = typeof explicit === "boolean" ? explicit : !currentSaved;

    await upsertSavedPostState(user.uid, postId, { saved: nextSaved });
    res.status(200).json({ ok: true, saved: nextSaved });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] save post failed", error);
    res.status(500).json({ error: "save_post_failed" });
  }
});

ROUTES.post("/posts/:postId/comment", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const text = sanitizeText((req.body || {}).text, 500);

    if (!user || !postId || !text) {
      res.status(400).json({ error: "invalid_comment" });
      return;
    }

    const profile = await readAuthorProfile(user.uid);
    const commentRef = db.collection("posts").doc(postId).collection("comments").doc();

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);

      tx.set(commentRef, {
        authorUid: user.uid,
        authorHandle: profile.handle,
        text,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      counts.comments += 1;
      const score = computeScore(counts, createdAtMs);

      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        commentId: commentRef.id,
        counts,
        score,
      };
    });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] comment failed", error);
    res.status(500).json({ error: "comment_failed" });
  }
});

ROUTES.delete("/posts/:postId/comment/:commentId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const commentId = sanitizeText(req.params.commentId, 180);

    if (!user || !postId || !commentId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const postRef = db.collection("posts").doc(postId);
    const commentRef = postRef.collection("comments").doc(commentId);

    const result = await db.runTransaction(async (tx) => {
      const [postSnap, commentSnap] = await Promise.all([tx.get(postRef), tx.get(commentRef)]);

      if (!postSnap.exists) throw new Error("not_found");
      if (!commentSnap.exists) throw new Error("comment_not_found");

      const postData = (postSnap.data() || {}) as Record<string, unknown>;
      const commentData = (commentSnap.data() || {}) as Record<string, unknown>;

      if (asString(commentData.authorUid) !== user.uid) {
        throw new Error("forbidden");
      }

      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);
      counts.comments = Math.max(0, counts.comments - 1);
      const score = computeScore(counts, createdAtMs);

      tx.delete(commentRef);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastEngagedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return { deleted: true, counts, score };
    });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "forbidden") {
      res.status(403).json({ error: code });
      return;
    }
    if (code === "not_found" || code === "comment_not_found") {
      res.status(404).json({ error: code });
      return;
    }
    console.error("[Explore] delete comment failed", error);
    res.status(500).json({ error: "comment_delete_failed" });
  }
});

ROUTES.post("/posts/:postId/report", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const reason = sanitizeText((req.body || {}).reason, 120);
    const details = sanitizeText((req.body || {}).details, 1000);

    if (!user || !postId || !reason) {
      res.status(400).json({ error: "invalid_report" });
      return;
    }

    const reportRef = db.collection("reports").doc(`${postId}_${user.uid}`);

    const result = await updatePostEngagement(postId, async (tx, postRef, postData) => {
      const counts = normalizeCounts(postData.counts);
      const createdAtMs = getTimestampMs(postData.createdAt);
      const reportSnap = await tx.get(reportRef);

      if (!reportSnap.exists) {
        tx.set(reportRef, {
          postId,
          reporterUid: user.uid,
          reason,
          details,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          status: "open",
        });
        counts.reports += 1;
      }

      const score = computeScore(counts, createdAtMs);
      tx.set(
        postRef,
        {
          counts,
          score,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return {
        reported: true,
        counts,
        score,
      };
    });

    res.status(200).json(result);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    if (code === "not_found") {
      res.status(404).json({ error: code });
      return;
    }
    if (code === "gone") {
      res.status(410).json({ error: code });
      return;
    }
    console.error("[Explore] report failed", error);
    res.status(500).json({ error: "report_failed" });
  }
});

ROUTES.patch("/posts/:postId/visibility", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);
    const visibility = sanitizeText((req.body || {}).visibility, 20) as Visibility;

    if (!user || !postId || !["public", "unlisted"].includes(visibility)) {
      res.status(400).json({ error: "invalid_visibility" });
      return;
    }

    const postRef = db.collection("posts").doc(postId);
    const postSnap = await postRef.get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const post = (postSnap.data() || {}) as Record<string, unknown>;
    if (asString(post.authorUid) !== user.uid) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    await postRef.set(
      {
        visibility,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.status(200).json({ ok: true, visibility });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] visibility update failed", error);
    res.status(500).json({ error: "visibility_update_failed" });
  }
});

ROUTES.delete("/posts/:postId", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    const postId = sanitizeText(req.params.postId, 180);

    if (!user || !postId) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const postRef = db.collection("posts").doc(postId);
    const postSnap = await postRef.get();
    if (!postSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const postData = (postSnap.data() || {}) as Record<string, unknown>;
    if (asString(postData.authorUid) !== user.uid) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    await postRef.set(
      {
        visibility: "deleted",
        deletedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    await Promise.all([
      deleteCollectionDocs(postRef.collection("comments")),
      deleteCollectionDocs(db.collection("postLikes").doc(postId).collection("users")),
      deleteCollectionDocs(db.collection("postReposts").doc(postId).collection("users")),
      deleteCollectionDocs(db.collection("postShareEvents").where("postId", "==", postId)),
      deleteCollectionDocs(db.collection("reports").where("postId", "==", postId)),
    ]);

    await Promise.all([
      db.collection("postLikes").doc(postId).delete().catch(() => undefined),
      db.collection("postReposts").doc(postId).delete().catch(() => undefined),
    ]);

    await postRef.delete();

    res.status(200).json({ ok: true, deleted: true });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] delete failed", error);
    res.status(500).json({ error: "post_delete_failed" });
  }
});

ROUTES.get("/profile/handle/:handle", async (req, res) => {
  try {
    const handle = normalizeHandle(req.params.handle);
    if (!handle) {
      res.status(400).json({ error: "invalid_handle" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);

    let snap = await db.collection("users").where("handle", "==", handle).limit(1).get();
    if (snap.empty) {
      snap = await db.collection("users").where("profile.username", "==", handle).limit(1).get();
    }

    if (snap.empty) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    const userDoc = snap.docs[0];
    const userData = userDoc.data() || {};
    const payload = await buildProfilePayload(userDoc.id, userData, viewer?.uid || null);

    if (!asBoolean(payload.publicProfile, false) && viewer?.uid !== userDoc.id) {
      res.status(404).json({ error: "not_found" });
      return;
    }

    res.status(200).json(payload);
  } catch (error) {
    console.error("[Explore] profile by handle failed", error);
    res.status(500).json({ error: "profile_lookup_failed" });
  }
});

ROUTES.get("/profile/:uid/posts", async (req, res) => {
  try {
    const uid = sanitizeText(req.params.uid, 140);
    const limit = parseLimit(req.query.limit);
    const cursor = decodeCursor(req.query.cursor);
    if (!uid) {
      res.status(400).json({ error: "invalid_uid" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const isOwner = viewer?.uid === uid;

    let queryRef: admin.firestore.Query;
    if (isOwner) {
      queryRef = db
        .collection("posts")
        .where("authorUid", "==", uid)
        .orderBy("createdAt", "desc")
        .orderBy(admin.firestore.FieldPath.documentId(), "desc");
    } else {
      queryRef = db
        .collection("posts")
        .where("authorUid", "==", uid)
        .where("visibility", "==", "public")
        .orderBy("createdAt", "desc")
        .orderBy(admin.firestore.FieldPath.documentId(), "desc");
    }

    if (cursor) {
      queryRef = queryRef.startAfter(timestampFromMs(cursor.createdAtMs), cursor.id);
    }

    const snap = await queryRef.limit(limit + 1).get();
    let docs = snap.docs;
    if (isOwner) {
      docs = docs.filter((doc) => asString(doc.data().visibility) !== "deleted");
    }

    const page = docs.slice(0, limit);
    const hasMore = docs.length > limit;
    const postIds = page.map((doc) => doc.id);
    const engagement = await fetchViewerEngagement(postIds, viewer?.uid || null);

    const posts = page.map((doc) => toPostResponse(doc, engagement.get(doc.id) || { liked: false, reposted: false, saved: false }));
    const next = hasMore && posts.length
      ? encodeCursor(buildNextCursor(posts[posts.length - 1] as Record<string, unknown>, false))
      : null;

    res.status(200).json({ posts, cursor: next, owner: isOwner });
  } catch (error) {
    console.error("[Explore] profile posts failed", error);
    res.status(500).json({ error: "profile_posts_failed" });
  }
});

ROUTES.get("/me/profile", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const userSnap = await db.collection("users").doc(viewer.uid).get();
    if (!userSnap.exists) {
      res.status(404).json({ error: "not_found" });
      return;
    }
    const payload = await buildProfilePayload(viewer.uid, (userSnap.data() || {}) as Record<string, unknown>, viewer.uid);
    res.status(200).json(payload);
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] me profile failed", error);
    res.status(500).json({ error: "me_profile_failed" });
  }
});

ROUTES.patch("/me/profile", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const body = (req.body || {}) as Record<string, unknown>;
    const profilePatch: Record<string, unknown> = {};
    if (typeof body.publicEmailOptIn === "boolean") {
      profilePatch.publicEmailOptIn = body.publicEmailOptIn;
    }
    if (typeof body.publicProfile === "boolean") {
      profilePatch.publicProfile = body.publicProfile;
    }
    if (!Object.keys(profilePatch).length) {
      res.status(400).json({ error: "no_supported_fields" });
      return;
    }

    await db.collection("users").doc(viewer.uid).set(
      {
        profile: profilePatch,
        profileUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const userSnap = await db.collection("users").doc(viewer.uid).get();
    const payload = await buildProfilePayload(viewer.uid, (userSnap.data() || {}) as Record<string, unknown>, viewer.uid);
    res.status(200).json({ ok: true, profile: payload });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] me profile update failed", error);
    res.status(500).json({ error: "me_profile_update_failed" });
  }
});

ROUTES.get("/me/notification-settings", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const userRef = db.collection("users").doc(user.uid);
    const [userSnap, followsSnap, watchSnap, tokenSnap] = await Promise.all([
      userRef.get(),
      userRef.collection("follows").limit(200).get(),
      userRef.collection("watchTickers").limit(200).get(),
      userRef.collection("fcmTokens").limit(100).get(),
    ]);

    const userData = (userSnap.data() || {}) as Record<string, unknown>;
    const prefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
    const privacyRaw = (userData.notificationPrivacy || {}) as Record<string, unknown>;
    const coarseLocation = normalizeCoarseLocation(privacyRaw.coarseLocation);

    res.status(200).json({
      notificationPrefs: {
        global: prefs.global,
        following: prefs.following,
        tickers: prefs.tickers,
        watchlist: prefs.watchlist,
        explore: prefs.explore,
        earnings: prefs.earnings,
        ipos: prefs.ipos,
        daily: prefs.daily,
        weekly: prefs.weekly,
        inactiveHidden: prefs.inactiveHidden,
      },
      notificationPrivacy: {
        locationConsent: asBoolean(privacyRaw.locationConsent, false),
        ipRegionConsent: asBoolean(privacyRaw.ipRegionConsent, false),
        timezone: normalizeTimezone(privacyRaw.timezone),
        ipRegion: sanitizeText(privacyRaw.ipRegion, 80),
        coarseLocation,
        updatedAtMs: getTimestampMs(privacyRaw.updatedAt || Date.now()),
      },
      follows: followsSnap.docs.map((doc) => doc.id),
      watchTickers: watchSnap.docs
        .map((doc) => normalizeTicker(doc.id || doc.data().ticker))
        .filter(Boolean),
      tokenCount: tokenSnap.size,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] me notification settings failed", error);
    res.status(500).json({ error: "notification_settings_failed" });
  }
});

ROUTES.post("/notifications/register-token", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const token = sanitizeText((req.body || {}).token, 4096);
    const platform = sanitizeText((req.body || {}).platform, 32) || "web";
    if (!token || token.length < 20) {
      res.status(400).json({ error: "invalid_token" });
      return;
    }

    const tokenRef = db.collection("users").doc(user.uid).collection("fcmTokens").doc(token);
    const privacyInput = ((req.body || {}) as Record<string, unknown>).notificationPrivacy;
    const locationConsent = asBoolean((privacyInput as Record<string, unknown>)?.locationConsent, false);
    const ipRegionConsent = asBoolean((privacyInput as Record<string, unknown>)?.ipRegionConsent, false);
    const tokenMeta: Record<string, unknown> = {};
    if (locationConsent) {
      const timezone = normalizeTimezone((privacyInput as Record<string, unknown>)?.timezone);
      const coarseLocation = normalizeCoarseLocation((privacyInput as Record<string, unknown>)?.coarseLocation);
      tokenMeta.timezone = timezone;
      tokenMeta.coarseLocation = coarseLocation;
      tokenMeta.locationConsent = true;
      tokenMeta.ipRegionConsent = ipRegionConsent;
      let ipRegion = sanitizeText((privacyInput as Record<string, unknown>)?.ipRegion, 80);
      if (ipRegionConsent && !ipRegion) {
        const derived = await fetchIpDerivedRegion(requestIpAddress(req));
        ipRegion = derived.region;
      }
      tokenMeta.ipRegion = ipRegion;
    }
    await tokenRef.set(
      {
        token,
        platform,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
        ...tokenMeta,
      },
      { merge: true }
    );

    await syncTopicsForUser(user.uid);

    res.status(200).json({ ok: true, tokenSuffix: token.slice(-10) });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] register token failed", error);
    res.status(500).json({ error: "register_token_failed" });
  }
});

ROUTES.post("/notifications/preferences", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const input = (req.body || {}) as Record<string, unknown>;
    const userRef = db.collection("users").doc(user.uid);
    const userSnap = await userRef.get();
    const userData = (userSnap.data() || {}) as Record<string, unknown>;
    const existingPrefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
    const notificationPrefs = normalizeNotificationPrefs(input, existingPrefs as unknown as Record<string, unknown>);

    const existingPrivacy = (userData.notificationPrivacy || {}) as Record<string, unknown>;
    const locationConsent =
      typeof input.locationConsent === "boolean"
        ? asBoolean(input.locationConsent, false)
        : asBoolean(existingPrivacy.locationConsent, false);
    const ipRegionConsent =
      locationConsent &&
      (typeof input.ipRegionConsent === "boolean"
        ? asBoolean(input.ipRegionConsent, false)
        : asBoolean(existingPrivacy.ipRegionConsent, false));
    const timezone = locationConsent
      ? normalizeTimezone(input.timezone || existingPrivacy.timezone || "")
      : "";
    const coarseLocation = locationConsent
      ? normalizeCoarseLocation(input.coarseLocation || existingPrivacy.coarseLocation)
      : null;
    let ipRegion = locationConsent && ipRegionConsent ? sanitizeText(input.ipRegion || existingPrivacy.ipRegion, 80) : "";
    if (locationConsent && ipRegionConsent && !ipRegion) {
      const derived = await fetchIpDerivedRegion(requestIpAddress(req));
      ipRegion = derived.region;
      if (!coarseLocation?.countryCode && derived.countryCode) {
        if (coarseLocation) coarseLocation.countryCode = derived.countryCode;
      }
    }
    const notificationPrivacy = {
      locationConsent,
      ipRegionConsent,
      timezone,
      ipRegion,
      coarseLocation: locationConsent ? coarseLocation : null,
      ipAddress: locationConsent && ipRegionConsent ? sanitizeText(requestIpAddress(req), 120) : "",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await db
      .collection("users")
      .doc(user.uid)
      .set(
        {
          notificationPrefs,
          notificationPrivacy,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

    await syncTopicsForUser(user.uid);

    res.status(200).json({
      ok: true,
      notificationPrefs,
      notificationPrivacy: {
        locationConsent,
        ipRegionConsent,
        timezone,
        ipRegion,
        coarseLocation: locationConsent ? coarseLocation : null,
        updatedAtMs: Date.now(),
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] preferences failed", error);
    res.status(500).json({ error: "preferences_update_failed" });
  }
});

ROUTES.post("/notifications/personalize", async (req, res) => {
  try {
    const input = (req.body || {}) as Record<string, unknown>;
    const title = sanitizeText(input.title, 160) || "Quantura update";
    const body = sanitizeText(input.body, 500);
    const source = sanitizeText(input.source, 40) || "notification";
    const context = input.context && typeof input.context === "object" ? (input.context as Record<string, unknown>) : {};

    const rewritten = await rewriteNotificationWithLlm({
      title,
      body,
      source,
      context,
    });

    res.status(200).json({
      notification: {
        title: rewritten.title,
        body: rewritten.body,
        nextSteps: rewritten.nextSteps,
        personalized: rewritten.personalized,
        disclaimer: "LLMs can sometimes make mistakes.",
      },
    });
  } catch (error) {
    console.error("[Explore] notification personalize failed", error);
    res.status(500).json({ error: "notification_personalize_failed" });
  }
});

ROUTES.post("/notifications/sync-topics", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    await syncTopicsForUser(user.uid);
    res.status(200).json({ ok: true });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] sync topics failed", error);
    res.status(500).json({ error: "sync_topics_failed" });
  }
});

ROUTES.get("/notifications/config", (_req, res) => {
  const vapidPublicKey = sanitizeText(process.env.FCM_WEB_VAPID_KEY || "", 4096);
  res.status(200).json({ vapidPublicKey });
});

ROUTES.post("/notifications/session/ping", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const input = asPlainObject(req.body);
    const isAnonymous =
      typeof input.isAnonymous === "boolean"
        ? asBoolean(input.isAnonymous, false)
        : sanitizeText(user.firebase?.sign_in_provider, 40) === "anonymous";
    const userRef = db.collection("users").doc(user.uid);
    const userSnap = await userRef.get();
    const userData = (userSnap.data() || {}) as Record<string, unknown>;
    const existingPrefs = normalizeNotificationPrefs({}, (userData.notificationPrefs || {}) as Record<string, unknown>);
    const rawPrefs =
      input.notificationPrefs && typeof input.notificationPrefs === "object"
        ? (input.notificationPrefs as Record<string, unknown>)
        : {};
    const notificationPrefs = normalizeNotificationPrefs(rawPrefs, existingPrefs as unknown as Record<string, unknown>);

    await userRef.set(
      {
        isAnonymous,
        lastActiveAt: admin.firestore.FieldValue.serverTimestamp(),
        notificationPrefs,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    res.status(200).json({
      ok: true,
      uid: user.uid,
      isAnonymous,
      notificationPrefs,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] session ping failed", error);
    res.status(500).json({ error: "notification_session_ping_failed" });
  }
});

ROUTES.get("/notifications/items", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestedCategory = sanitizeText((req.query as Record<string, unknown>).category, 40).toLowerCase();
    const categoryFilter = requestedCategory && requestedCategory !== "all" ? normalizeNotificationCategory(requestedCategory) : null;
    const unreadOnly = asBoolean((req.query as Record<string, unknown>).unread, false);
    const includeHidden = asBoolean((req.query as Record<string, unknown>).includeHidden, false);
    const limitValue = Math.max(1, Math.min(120, Math.floor(asFinite((req.query as Record<string, unknown>).limit, 40))));
    const fetchLimit = Math.max(limitValue * 3, 120);

    let query: admin.firestore.Query = db
      .collection("notifications")
      .doc(user.uid)
      .collection("items")
      .orderBy("createdAt", "desc")
      .limit(fetchLimit);
    const snap = await query.get();
    const items = snap.docs
      .map((doc) => {
        const data = (doc.data() || {}) as Record<string, unknown>;
        const category = normalizeNotificationCategory(data.category);
        const hidden = asBoolean(data.hidden, false);
        return {
          id: doc.id,
          category,
          title: sanitizeText(data.title, 160) || `${notificationCategoryLabel(category)} update`,
          body: sanitizeText(data.body, 400),
          deepLink: normalizeNotificationDeepLink(data.deepLink),
          hidden,
          read: asBoolean(data.read, false),
          nextSteps: Array.isArray(data.nextSteps)
            ? data.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean).slice(0, 4)
            : [],
          createdAtMs: getTimestampMs(data.createdAt),
        };
      })
      .filter((item) => (unreadOnly ? !item.read : true))
      .filter((item) => (includeHidden ? true : !item.hidden))
      .filter((item) => (categoryFilter ? item.category === categoryFilter : true));

    const unreadCount = items.filter((item) => !item.read).length;
    res.status(200).json({
      ok: true,
      unreadCount,
      count: items.length,
      items: items.slice(0, limitValue),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] list items failed", error);
    res.status(500).json({ error: "notification_items_failed" });
  }
});

ROUTES.post("/notifications/items/:itemId/read", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const itemId = sanitizeText(req.params.itemId, 220).replace(/[^A-Za-z0-9._-]/g, "");
    if (!itemId) {
      res.status(400).json({ error: "invalid_item_id" });
      return;
    }
    const readValue = typeof (req.body || {}).read === "boolean" ? asBoolean((req.body || {}).read, true) : true;
    const itemRef = db.collection("notifications").doc(user.uid).collection("items").doc(itemId);
    const itemSnap = await itemRef.get();
    if (!itemSnap.exists) {
      res.status(404).json({ error: "notification_item_not_found" });
      return;
    }
    await itemRef.set(
      {
        read: readValue,
        readAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    res.status(200).json({ ok: true, id: itemId, read: readValue });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] mark item read failed", error);
    res.status(500).json({ error: "notification_item_update_failed" });
  }
});

ROUTES.post("/notifications/items/read-all", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const includeHidden = asBoolean((req.body || {}).includeHidden, false);
    let updated = 0;
    while (true) {
      const query: admin.firestore.Query = db
        .collection("notifications")
        .doc(user.uid)
        .collection("items")
        .where("read", "==", false)
        .limit(200);
      const snap = await query.get();
      if (snap.empty) break;
      const batch = db.batch();
      snap.docs.forEach((doc) => {
        const hidden = asBoolean((doc.data() || {}).hidden, false);
        if (!includeHidden && hidden) return;
        batch.set(
          doc.ref,
          {
            read: true,
            readAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
        updated += 1;
      });
      await batch.commit();
      if (snap.size < 200) break;
    }
    res.status(200).json({ ok: true, updated });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] mark all read failed", error);
    res.status(500).json({ error: "notification_read_all_failed" });
  }
});

ROUTES.post("/notify/event", async (req, res) => {
  try {
    const actor = await verifyRequestUser(req, true);
    if (!actor) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const body = asPlainObject(req.body);
    const category = normalizeNotificationCategory(body.category);
    const title = sanitizeText(body.title, 160) || `${notificationCategoryLabel(category)} update`;
    const message = sanitizeText(body.body || body.message, 320) || "You have a new Quantura update.";
    const deepLink = normalizeNotificationDeepLink(body.deepLink || body.path || "/notifications");
    const nextSteps = Array.isArray(body.nextSteps) ? body.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean) : [];
    const metadata = body.metadata && typeof body.metadata === "object" ? (body.metadata as Record<string, unknown>) : {};
    const force = asBoolean(body.force, false);
    const hidden = typeof body.hidden === "boolean" ? asBoolean(body.hidden, false) : category === "inactive";

    const explicitUserIds = Array.isArray(body.userIds)
      ? body.userIds.map((item) => sanitizeText(item, 220)).filter(Boolean)
      : [];
    const targetUsers = new Set<string>(explicitUserIds);

    const inputTickers = Array.isArray(body.tickers)
      ? body.tickers.map((item) => normalizeTicker(item)).filter(Boolean)
      : [normalizeTicker(body.ticker)].filter(Boolean);
    if (!targetUsers.size) {
      if (category === "watchlist" || category === "earnings") {
        const byTicker = await collectUsersByWatchTickers(inputTickers);
        byTicker.forEach((uid) => targetUsers.add(uid));
      } else if (category === "explore") {
        const authorUid = sanitizeText(body.authorUid || body.userId, 220);
        const byFollow = await collectUsersByFollowedAuthor(authorUid);
        byFollow.forEach((uid) => targetUsers.add(uid));
      } else if (category === "ipo") {
        const allUsers = await collectUsersForIpoNotifications();
        allUsers.forEach((uid) => targetUsers.add(uid));
      } else {
        targetUsers.add(actor.uid);
      }
    }

    const excludeUid = sanitizeText(body.excludeUid, 220);
    if (excludeUid) targetUsers.delete(excludeUid);

    if (!targetUsers.size) {
      res.status(404).json({ error: "no_target_users" });
      return;
    }

    const deliverResults = await Promise.all(
      Array.from(targetUsers)
        .slice(0, 2000)
        .map((uid) =>
          sendPushToUserTokens({
            uid,
            category,
            title,
            body: message,
            deepLink,
            hidden,
            nextSteps,
            metadata: {
              ...metadata,
              actorUid: actor.uid,
              tickers: inputTickers,
            },
            force,
          })
        )
    );

    const deliveredUsers = deliverResults.filter((row) => row.delivered > 0).length;
    const attemptedTokens = deliverResults.reduce((sum, row) => sum + row.attempted, 0);
    const deliveredTokens = deliverResults.reduce((sum, row) => sum + row.delivered, 0);
    res.status(200).json({
      ok: true,
      category,
      targetUsers: targetUsers.size,
      deliveredUsers,
      attemptedTokens,
      deliveredTokens,
      skippedUsers: deliverResults.filter((row) => row.skipped).length,
      results: deliverResults.slice(0, 60),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] event send failed", error);
    res.status(500).json({ error: "notify_event_failed" });
  }
});

ROUTES.post("/notify/watchlist", async (req, res) => {
  try {
    const actor = await verifyRequestUser(req, true);
    if (!actor) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const body = asPlainObject(req.body);
    const title = sanitizeText(body.title, 160) || "Watchlist update";
    const message = sanitizeText(body.body || body.message, 320) || "A watchlist update is available.";
    const deepLink = normalizeNotificationDeepLink(body.deepLink || body.path || "/watchlist");
    const nextSteps = Array.isArray(body.nextSteps) ? body.nextSteps.map((item) => sanitizeText(item, 120)).filter(Boolean) : [];
    const metadata = body.metadata && typeof body.metadata === "object" ? (body.metadata as Record<string, unknown>) : {};
    const force = asBoolean(body.force, false);
    const hidden = asBoolean(body.hidden, false);

    const explicitUserIds = Array.isArray(body.userIds)
      ? body.userIds.map((item) => sanitizeText(item, 220)).filter(Boolean)
      : [];
    const targetUsers = new Set<string>(explicitUserIds);

    const inputTickers = Array.isArray(body.tickers)
      ? body.tickers.map((item) => normalizeTicker(item)).filter(Boolean)
      : [normalizeTicker(body.ticker)].filter(Boolean);

    if (!targetUsers.size) {
      const byTicker = await collectUsersByWatchTickers(inputTickers);
      byTicker.forEach((uid) => targetUsers.add(uid));
    }

    const excludeUid = sanitizeText(body.excludeUid, 220);
    if (excludeUid) targetUsers.delete(excludeUid);

    if (!targetUsers.size) {
      res.status(404).json({ error: "no_target_users" });
      return;
    }

    const deliverResults = await Promise.all(
      Array.from(targetUsers)
        .slice(0, 2000)
        .map((uid) =>
          sendPushToUserTokens({
            uid,
            category: "watchlist",
            title,
            body: message,
            deepLink,
            hidden,
            nextSteps,
            metadata: {
              ...metadata,
              actorUid: actor.uid,
              tickers: inputTickers,
            },
            force,
          })
        )
    );

    const deliveredUsers = deliverResults.filter((row) => row.delivered > 0).length;
    const attemptedTokens = deliverResults.reduce((sum, row) => sum + row.attempted, 0);
    const deliveredTokens = deliverResults.reduce((sum, row) => sum + row.delivered, 0);

    res.status(200).json({
      ok: true,
      category: "watchlist",
      targetUsers: targetUsers.size,
      deliveredUsers,
      attemptedTokens,
      deliveredTokens,
      skippedUsers: deliverResults.filter((row) => row.skipped).length,
      results: deliverResults.slice(0, 60),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Notify] watchlist send failed", error);
    res.status(500).json({ error: "notify_watchlist_failed" });
  }
});

ROUTES.get(["/promo/status", "/explore/promo/status"], (_req, res) => {
  const serverTimeMs = Date.now();
  const startsAtMs = PROMO_START_MS;
  const endsAtMs = PROMO_END_MS;
  const active = PROMO_ACTIVE && serverTimeMs >= startsAtMs && serverTimeMs < endsAtMs;
  res.status(200).json({
    serverTimeMs,
    promo: {
      id: PROMO_ID,
      active,
      code: PROMO_CODE,
      discountPercent: PROMO_DISCOUNT_PERCENT,
      headline: "Upgrade your research workflow with a limited-time offer",
      body: `Apply code ${PROMO_CODE} for ${PROMO_DISCOUNT_PERCENT}% off your first cycle.`,
      startsAtMs,
      endsAtMs,
      serverTimeMs,
    },
  });
});

ROUTES.post("/follows/:authorUid", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const authorUid = sanitizeText(req.params.authorUid, 140);
    if (!authorUid || authorUid === user.uid) {
      res.status(400).json({ error: "invalid_author_uid" });
      return;
    }

    const ref = db.collection("users").doc(user.uid).collection("follows").doc(authorUid);
    const snap = await ref.get();
    const explicitFollow = (req.body || {}).follow;

    const shouldFollow = typeof explicitFollow === "boolean" ? explicitFollow : !snap.exists;
    if (shouldFollow) {
      await ref.set(
        {
          authorUid,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    } else {
      await ref.delete();
    }

    await syncTopicsForUser(user.uid);

    res.status(200).json({ ok: true, following: shouldFollow });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] follow toggle failed", error);
    res.status(500).json({ error: "follow_toggle_failed" });
  }
});

ROUTES.post("/watch-tickers/:ticker", async (req, res) => {
  try {
    const user = await verifyRequestUser(req, true);
    if (!user) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const ticker = normalizeTicker(req.params.ticker);
    if (!ticker) {
      res.status(400).json({ error: "invalid_ticker" });
      return;
    }

    const ref = db.collection("users").doc(user.uid).collection("watchTickers").doc(ticker);
    const snap = await ref.get();
    const explicitWatch = (req.body || {}).watch;

    const shouldWatch = typeof explicitWatch === "boolean" ? explicitWatch : !snap.exists;

    if (shouldWatch) {
      await ref.set(
        {
          ticker,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    } else {
      await ref.delete();
    }

    await syncTopicsForUser(user.uid);

    res.status(200).json({ ok: true, watching: shouldWatch, ticker });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] watch ticker failed", error);
    res.status(500).json({ error: "watch_ticker_failed" });
  }
});

ROUTES.get("/my-requests/shared/:slug", async (req, res) => {
  try {
    const slug = normalizeShareId(req.params.slug);
    if (!slug) {
      res.status(400).json({ error: "invalid_share_slug" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const shareSnap = await db.collection("request_shares").doc(slug).get();
    if (!shareSnap.exists) {
      res.status(404).json({ error: "share_not_found" });
      return;
    }
    const shareData = (shareSnap.data() || {}) as Record<string, unknown>;
    const visibility = normalizeMyRequestVisibility(shareData.visibility, "private");
    const ownerUid = sanitizeText(shareData.ownerUid, 220);
    const requestId = normalizeMyRequestId(shareData.requestId);
    if (!ownerUid || !requestId) {
      res.status(404).json({ error: "share_invalid" });
      return;
    }
    const canRead =
      visibility === "public" ||
      visibility === "unlisted" ||
      (viewer?.uid && viewer.uid === ownerUid);
    if (!canRead) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    const requestSnap = await db.collection("users").doc(ownerUid).collection("requests").doc(requestId).get();
    if (!requestSnap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const data = (requestSnap.data() || {}) as Record<string, unknown>;
    if (asBoolean(data.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const responseItem = toMyRequestResponse(requestSnap.id, data, { includePayload: true });
    res.status(200).json({
      request: responseItem,
      readOnly: !(viewer?.uid && viewer.uid === ownerUid),
      share: {
        slug,
        visibility,
        shareUrl: myRequestShareUrl(slug),
      },
    });
  } catch (error) {
    console.error("[Explore] read shared request failed", error);
    res.status(500).json({ error: "request_share_lookup_failed" });
  }
});

ROUTES.get("/my-requests", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    await syncLegacyRequestsForUser(viewer.uid);

    const typeFilter = normalizeMyRequestType(req.query.type);
    const publishedFilter = normalizeMyRequestPublishedFilter(req.query.published);
    const queryText = sanitizeText(req.query.q, 140).toLowerCase();
    const limit = parseLimit(req.query.limit);

    const snap = await db
      .collection("users")
      .doc(viewer.uid)
      .collection("requests")
      .orderBy("updatedAt", "desc")
      .limit(Math.max(limit * 4, 140))
      .get();

    const rows = snap.docs
      .map((doc) => toMyRequestResponse(doc.id, (doc.data() || {}) as Record<string, unknown>, { includePayload: true }))
      .filter((item) => !asBoolean(item.deleted, false))
      .filter((item) => {
        const itemType = normalizeMyRequestType(item.type);
        if (typeFilter && itemType !== typeFilter) return false;
        if (publishedFilter === "published" && !asBoolean(item.published, false)) return false;
        if (publishedFilter === "unpublished" && asBoolean(item.published, false)) return false;
        if (!queryText) return true;
        const haystack = [
          asString(item.title),
          asString(item.ticker),
          asString(item.typeLabel),
          asString((item.outputsMeta as Record<string, unknown>)?.summary || ""),
          asString((item.input as Record<string, unknown>)?.question || ""),
          asString((item.input as Record<string, unknown>)?.notes || ""),
          asString(item.createdAt),
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(queryText);
      })
      .slice(0, limit);

    res.status(200).json({
      items: rows,
      count: rows.length,
      type: typeFilter || "all",
      published: publishedFilter,
      q: queryText,
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] list my requests failed", error);
    res.status(500).json({ error: "my_requests_list_failed" });
  }
});

ROUTES.post("/my-requests", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const body = asPlainObject(req.body);
    const type = normalizeMyRequestType(body.type);
    if (!type || !MY_REQUEST_TYPE_SET.has(type)) {
      res.status(400).json({ error: "invalid_request_type" });
      return;
    }

    const sourceRefRaw = asPlainObject(body.sourceRef);
    const sourceCollection = sanitizeText(sourceRefRaw.collection, 80);
    const sourceId = sanitizeText(sourceRefRaw.id, 220);
    const requestIdFromBody = normalizeMyRequestId(body.requestId);
    const requestId = requestIdFromBody || (sourceCollection && sourceId ? buildMyRequestDocId(type, sourceId) : `req_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`);
    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const existingSnap = await requestRef.get();
    const existing = (existingSnap.data() || {}) as Record<string, unknown>;

    const input = normalizeMyRequestInput(body.input);
    const outputsMeta = trimOutputsMeta(body.outputsMeta);
    const titleEdited = asBoolean(existing.titleEdited, false);
    const title = titleEdited
      ? sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, input)
      : sanitizeText(body.title, 180) || sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, input);
    const shareExisting = normalizeMyRequestShareObject(existing.share);
    const shareRequested = normalizeMyRequestShareObject(body.share);
    const share: Record<string, unknown> = {
      visibility: normalizeMyRequestVisibility(shareRequested.visibility, shareExisting.visibility),
      slug: normalizeShareId(shareRequested.slug || shareExisting.slug),
      createdAt: shareExisting.createdAt || shareRequested.createdAt || null,
    };
    const ticker = firstTickerFromRequest(input, { collection: sourceCollection, id: sourceId }, outputsMeta);

    const payload: Record<string, unknown> = {
      type,
      ownerUid: viewer.uid,
      title,
      titleEdited,
      input,
      outputsMeta,
      sourceRef: {
        collection: sourceCollection,
        id: sourceId,
      },
      searchText: buildMyRequestSearchText(title, type, ticker, input, outputsMeta),
      published: asBoolean(body.published, asBoolean(existing.published, false)),
      publishedAt: existing.publishedAt || null,
      explorePostId: sanitizeText(body.explorePostId || existing.explorePostId, 220),
      deleted: false,
      share,
      visibility: normalizeMyRequestVisibility(body.visibility, normalizeMyRequestVisibility(existing.visibility, share.visibility as MyRequestShareVisibility)),
      createdAt: existing.createdAt || admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await requestRef.set(payload, { merge: true });
    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] upsert my request failed", error);
    res.status(500).json({ error: "my_request_upsert_failed" });
  }
});

ROUTES.get("/my-requests/:requestId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const item = await readMyRequestForOwner(viewer.uid, requestId);
    if (!item || asBoolean(item.data.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    res.status(200).json({
      request: toMyRequestResponse(item.id, item.data, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] read my request failed", error);
    res.status(500).json({ error: "my_request_read_failed" });
  }
});

ROUTES.patch("/my-requests/:requestId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const body = asPlainObject(req.body);
    const title = sanitizeText(body.title, 180);
    if (!title) {
      res.status(400).json({ error: "invalid_title" });
      return;
    }

    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    if (asBoolean(existing.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }

    await requestRef.set(
      {
        title,
        titleEdited: true,
        searchText: buildMyRequestSearchText(
          title,
          normalizeMyRequestType(existing.type) || "forecast",
          normalizeTicker((existing.input as Record<string, unknown>)?.ticker),
          normalizeMyRequestInput(existing.input),
          trimOutputsMeta(existing.outputsMeta)
        ),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const share = normalizeMyRequestShareObject(existing.share);
    if (share.slug && share.visibility !== "private") {
      await db
        .collection("request_shares")
        .doc(share.slug)
        .set(
          {
            title,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
    }

    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] rename my request failed", error);
    res.status(500).json({ error: "my_request_rename_failed" });
  }
});

ROUTES.post("/my-requests/:requestId/share", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }

    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    if (asBoolean(existing.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }

    const body = asPlainObject(req.body);
    const visibility = normalizeMyRequestVisibility(body.visibility, "unlisted");
    const shareExisting = normalizeMyRequestShareObject(existing.share);

    if (visibility === "private") {
      await requestRef.set(
        {
          share: {
            visibility: "private",
            slug: shareExisting.slug || "",
            createdAt: shareExisting.createdAt || null,
          },
          visibility: "private",
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      if (shareExisting.slug) {
        await db.collection("request_shares").doc(shareExisting.slug).delete().catch(() => undefined);
      }
      const refreshed = await requestRef.get();
      res.status(200).json({
        ok: true,
        share: {
          visibility: "private",
          slug: shareExisting.slug || "",
          shareUrl: "",
        },
        request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
      });
      return;
    }

    const slug = await ensureUniqueMyRequestShareSlug(viewer.uid, requestId, shareExisting.slug);
    const sharePayload = {
      visibility,
      slug,
      createdAt: shareExisting.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    };
    const type = normalizeMyRequestType(existing.type) || "forecast";
    const title = sanitizeText(existing.title, 180) || defaultMyRequestTitle(type, normalizeMyRequestInput(existing.input));
    const ticker = firstTickerFromRequest(
      normalizeMyRequestInput(existing.input),
      asPlainObject(existing.sourceRef),
      trimOutputsMeta(existing.outputsMeta)
    );

    await Promise.all([
      requestRef.set(
        {
          share: sharePayload,
          visibility,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      ),
      db
        .collection("request_shares")
        .doc(slug)
        .set(
          {
            slug,
            ownerUid: viewer.uid,
            requestId,
            type,
            title,
            ticker,
            visibility,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
    ]);

    const shareUrl = myRequestShareUrl(slug);
    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      share: {
        visibility,
        slug,
        shareUrl,
      },
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] share my request failed", error);
    res.status(500).json({ error: "my_request_share_failed" });
  }
});

ROUTES.post("/my-requests/:requestId/unpublish", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    if (asBoolean(existing.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }

    const sourceRef = asPlainObject(existing.sourceRef);
    const sourceCollection = sanitizeText(sourceRef.collection, 80);
    const sourceId = sanitizeText(sourceRef.id, 220);
    const type = normalizeMyRequestType(existing.type) || "forecast";
    const candidatePostIds = [
      sanitizeText(existing.explorePostId, 220),
      type === "forecast" && sourceCollection === "forecast_requests" ? `forecast_${sourceId}` : "",
      type === "screener" && sourceCollection === "screener_runs" ? `screener_${sourceId}` : "",
    ]
      .map((item) => sanitizeText(item, 220))
      .filter(Boolean);

    await Promise.all(
      candidatePostIds.map((postId) =>
        db
          .collection("posts")
          .doc(postId)
          .set(
            {
              visibility: "hidden",
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          )
          .catch(() => undefined)
      )
    );

    await requestRef.set(
      {
        published: false,
        publishedAt: null,
        explorePostId: "",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const refreshed = await requestRef.get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(refreshed.id, (refreshed.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] unpublish my request failed", error);
    res.status(500).json({ error: "my_request_unpublish_failed" });
  }
});

ROUTES.post("/my-requests/:requestId/duplicate", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const source = await readMyRequestForOwner(viewer.uid, requestId);
    if (!source || asBoolean(source.data.deleted, false)) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const type = normalizeMyRequestType(source.data.type) || "forecast";
    const cloneId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    const cloneTitle = sanitizeText(source.data.title, 160) || defaultMyRequestTitle(type, normalizeMyRequestInput(source.data.input));

    await db
      .collection("users")
      .doc(viewer.uid)
      .collection("requests")
      .doc(cloneId)
      .set(
        {
          ...source.data,
          ownerUid: viewer.uid,
          title: `${cloneTitle} (Copy)`,
          titleEdited: true,
          published: false,
          publishedAt: null,
          explorePostId: "",
          share: {
            visibility: "private",
            slug: "",
            createdAt: null,
          },
          visibility: "private",
          deleted: false,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: false }
      );

    const cloneSnap = await db.collection("users").doc(viewer.uid).collection("requests").doc(cloneId).get();
    res.status(200).json({
      ok: true,
      request: toMyRequestResponse(cloneSnap.id, (cloneSnap.data() || {}) as Record<string, unknown>, { includePayload: true }),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] duplicate my request failed", error);
    res.status(500).json({ error: "my_request_duplicate_failed" });
  }
});

ROUTES.delete("/my-requests/:requestId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const requestId = normalizeMyRequestId(req.params.requestId);
    if (!requestId) {
      res.status(400).json({ error: "invalid_request_id" });
      return;
    }
    const requestRef = db.collection("users").doc(viewer.uid).collection("requests").doc(requestId);
    const snap = await requestRef.get();
    if (!snap.exists) {
      res.status(404).json({ error: "request_not_found" });
      return;
    }
    const existing = (snap.data() || {}) as Record<string, unknown>;
    const share = normalizeMyRequestShareObject(existing.share);

    await requestRef.set(
      {
        deleted: true,
        published: false,
        publishedAt: null,
        explorePostId: "",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    if (share.slug) {
      await db.collection("request_shares").doc(share.slug).delete().catch(() => undefined);
    }
    res.status(200).json({ ok: true, deleted: true, requestId });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] delete my request failed", error);
    res.status(500).json({ error: "my_request_delete_failed" });
  }
});

ROUTES.get("/saved/folders", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const userRef = db.collection("users").doc(viewer.uid);
    const [customSnap, systemCounts] = await Promise.all([
      userRef.collection("saved_folders").orderBy("updatedAt", "desc").limit(80).get(),
      listSystemFolderCounts(viewer.uid),
    ]);

    const folders = [
      ...SYSTEM_FOLDERS.map((folder) => ({
        id: folder.id,
        name: folder.displayName,
        isSystem: true,
        itemCount: asFinite(systemCounts[folder.id], 0),
      })),
      ...customSnap.docs
        .map((doc) => ({ id: doc.id, ...(doc.data() || {}) }))
        .filter((row) => !asBoolean((row as Record<string, unknown>).isSystem, false))
        .map((row) => ({
          id: asString((row as Record<string, unknown>).id),
          name: asString((row as Record<string, unknown>).name, "Untitled folder"),
          isSystem: false,
          itemCount: asFinite((row as Record<string, unknown>).itemCount, 0),
          createdAtMs: getTimestampMs((row as Record<string, unknown>).createdAt),
          updatedAtMs: getTimestampMs((row as Record<string, unknown>).updatedAt),
        })),
    ];

    res.status(200).json({ folders });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] list saved folders failed", error);
    res.status(500).json({ error: "saved_folders_failed" });
  }
});

ROUTES.post("/saved/folders", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const requestedName = sanitizeText((req.body || {}).name, 80);
    const requestedId = normalizeFolderId((req.body || {}).id);
    if (!requestedName || requestedName.length < 2) {
      res.status(400).json({ error: "invalid_folder_name" });
      return;
    }

    let folderId = requestedId || normalizeFolderId(requestedName);
    if (!folderId || systemFolderById(folderId)) {
      folderId = `folder-${Date.now()}`;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const existing = await folderRef.get();
    if (existing.exists) {
      folderId = `${folderId}-${Math.floor(Date.now() / 1000)}`;
    }

    await db
      .collection("users")
      .doc(viewer.uid)
      .collection("saved_folders")
      .doc(folderId)
      .set(
        {
          name: requestedName,
          isSystem: false,
          itemCount: 0,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

    res.status(200).json({
      ok: true,
      folder: {
        id: folderId,
        name: requestedName,
        isSystem: false,
        itemCount: 0,
      },
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] create saved folder failed", error);
    res.status(500).json({ error: "saved_folder_create_failed" });
  }
});

ROUTES.get("/saved/folders/:folderId/items", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const folderId = normalizeFolderId(req.params.folderId);
    const limit = parseLimit(req.query.limit);
    if (!folderId) {
      res.status(400).json({ error: "invalid_folder_id" });
      return;
    }

    if (systemFolderById(folderId)) {
      const items = await listSystemFolderItems(viewer.uid, folderId, limit);
      res.status(200).json({ folderId, isSystem: true, items });
      return;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const folderSnap = await folderRef.get();
    if (!folderSnap.exists) {
      res.status(404).json({ error: "folder_not_found" });
      return;
    }

    const itemsSnap = await folderRef.collection("items").orderBy("updatedAt", "desc").limit(limit).get();
    const items = itemsSnap.docs.map((doc) => ({ itemId: doc.id, ...(doc.data() || {}) }));
    res.status(200).json({ folderId, isSystem: false, items });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] list folder items failed", error);
    res.status(500).json({ error: "saved_folder_items_failed" });
  }
});

ROUTES.post("/saved/folders/:folderId/items", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const folderId = normalizeFolderId(req.params.folderId);
    if (!folderId || systemFolderById(folderId)) {
      res.status(400).json({ error: "invalid_folder_id" });
      return;
    }
    const itemType = normalizeSavedItemType((req.body || {}).itemType);
    const sourceId = normalizeSourceId((req.body || {}).sourceId);
    if (!itemType || !sourceId) {
      res.status(400).json({ error: "invalid_item_payload" });
      return;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const folderSnap = await folderRef.get();
    if (!folderSnap.exists) {
      res.status(404).json({ error: "folder_not_found" });
      return;
    }

    const resolved = await resolveSavedItem(viewer.uid, itemType, sourceId);
    if (!resolved) {
      res.status(404).json({ error: "source_not_found" });
      return;
    }

    const itemId = buildFolderItemDocId(itemType, sourceId);
    const itemRef = folderRef.collection("items").doc(itemId);

    await db.runTransaction(async (tx) => {
      const existing = await tx.get(itemRef);
      tx.set(
        itemRef,
        {
          ...resolved,
          itemType,
          sourceId,
          itemId,
          createdAt: existing.exists ? existing.get("createdAt") || admin.firestore.FieldValue.serverTimestamp() : admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      if (!existing.exists) {
        tx.set(
          folderRef,
          {
            itemCount: admin.firestore.FieldValue.increment(1),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
      }
    });

    res.status(200).json({ ok: true, folderId, itemId, item: resolved });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] save folder item failed", error);
    res.status(500).json({ error: "saved_folder_item_create_failed" });
  }
});

ROUTES.delete("/saved/folders/:folderId/items/:itemType/:sourceId", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }

    const folderId = normalizeFolderId(req.params.folderId);
    const itemType = normalizeSavedItemType(req.params.itemType);
    const sourceId = normalizeSourceId(req.params.sourceId);
    if (!folderId || !itemType || !sourceId || systemFolderById(folderId)) {
      res.status(400).json({ error: "invalid_request" });
      return;
    }

    const folderRef = db.collection("users").doc(viewer.uid).collection("saved_folders").doc(folderId);
    const itemId = buildFolderItemDocId(itemType, sourceId);
    const itemRef = folderRef.collection("items").doc(itemId);

    await db.runTransaction(async (tx) => {
      const existing = await tx.get(itemRef);
      if (!existing.exists) return;
      tx.delete(itemRef);
      tx.set(
        folderRef,
        {
          itemCount: admin.firestore.FieldValue.increment(-1),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    });

    res.status(200).json({ ok: true, folderId, itemId });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] delete folder item failed", error);
    res.status(500).json({ error: "saved_folder_item_delete_failed" });
  }
});

ROUTES.get("/saved/search", async (req, res) => {
  try {
    const viewer = await verifyRequestUser(req, true);
    if (!viewer) {
      res.status(401).json({ error: "unauthenticated" });
      return;
    }
    const queryText = sanitizeText(req.query.q, 140);
    const limit = parseLimit(req.query.limit);

    const [forecastSnap, screenerSnap, councilSnap, ownPostsSnap, savedStateSnap] = await Promise.all([
      db.collection("forecast_requests").where("userId", "==", viewer.uid).orderBy("createdAt", "desc").limit(60).get(),
      db.collection("screener_runs").where("userId", "==", viewer.uid).orderBy("createdAt", "desc").limit(60).get(),
      db.collection(MODEL_COUNCIL_RESPONSE_COLLECTION).where("userId", "==", viewer.uid).limit(60).get(),
      db.collection("posts").where("authorUid", "==", viewer.uid).orderBy("createdAt", "desc").limit(60).get(),
      db.collection("users").doc(viewer.uid).collection("saved_post_state").limit(140).get(),
    ]);

    const items: Record<string, unknown>[] = [];
    forecastSnap.docs.forEach((doc) => {
      const data = doc.data() || {};
      items.push({
        itemType: "forecast",
        sourceId: doc.id,
        itemId: buildFolderItemDocId("forecast", doc.id),
        title: asString(data.title, `${normalizeTicker(data.ticker)} forecast`),
        subtitle: asString(data.serviceMessage || ""),
        ticker: normalizeTicker(data.ticker),
        targetUrl: `/forecasting?forecastId=${encodeURIComponent(doc.id)}`,
        createdAtMs: getTimestampMs(data.createdAt),
        updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
      });
    });

    screenerSnap.docs.forEach((doc) => {
      const data = doc.data() || {};
      items.push({
        itemType: "screener",
        sourceId: doc.id,
        itemId: buildFolderItemDocId("screener", doc.id),
        title: asString(data.title, "Screener run"),
        subtitle: asString(data.notes || ""),
        ticker: normalizeTicker(((data.results as Array<Record<string, unknown>> | undefined) || [])[0]?.symbol),
        targetUrl: `/screener?runId=${encodeURIComponent(doc.id)}`,
        createdAtMs: getTimestampMs(data.createdAt),
        updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
      });
    });

    councilSnap.docs.forEach((doc) => {
      const data = doc.data() || {};
      items.push({
        itemType: "model_council",
        sourceId: doc.id,
        itemId: buildFolderItemDocId("model_council", doc.id),
        title: `${normalizeTicker(data.ticker) || "Ticker"} Model Council`,
        subtitle: asString(data.question || ""),
        ticker: normalizeTicker(data.ticker),
        targetUrl: "/model-council",
        createdAtMs: getTimestampMs(data.createdAt),
        updatedAtMs: getTimestampMs(data.updatedAt || data.createdAt),
      });
    });

    ownPostsSnap.docs.forEach((doc) => {
      const data = (doc.data() || {}) as Record<string, unknown>;
      if (asString(data.visibility) === "deleted") return;
      items.push(buildPostSavedItem(doc.id, data));
    });

    if (!savedStateSnap.empty) {
      const refs = savedStateSnap.docs.map((doc) => db.collection("posts").doc(doc.id));
      const postDocs = await db.getAll(...refs);
      postDocs.forEach((doc) => {
        if (!doc.exists) return;
        const data = (doc.data() || {}) as Record<string, unknown>;
        if (!isPostVisibleToViewer(data, viewer.uid)) return;
        items.push(buildPostSavedItem(doc.id, data));
      });
    }

    const dedup = new Map<string, Record<string, unknown>>();
    items.forEach((item) => {
      const key = `${asString(item.itemType)}:${asString(item.sourceId)}`;
      if (!dedup.has(key)) dedup.set(key, item);
    });

    const filtered = Array.from(dedup.values())
      .filter((item) => matchesSearchQuery(item, queryText))
      .sort((a, b) => asFinite(b.updatedAtMs, 0) - asFinite(a.updatedAtMs, 0));

    res.status(200).json({
      q: queryText,
      count: filtered.length,
      items: filtered.slice(0, limit),
    });
  } catch (error: any) {
    const code = String(error?.message || "");
    if (code === "unauthenticated" || code === "invalid_token") {
      res.status(401).json({ error: code });
      return;
    }
    console.error("[Explore] saved search failed", error);
    res.status(500).json({ error: "saved_search_failed" });
  }
});

ROUTES.get("/shares/:shareId", async (req, res) => {
  try {
    const shareId = normalizeShareId(req.params.shareId);
    if (!shareId) {
      res.status(400).json({ error: "invalid_share_id" });
      return;
    }

    const viewer = await verifyRequestUser(req, false).catch(() => null);
    const shareSnap = await db.collection("shares").doc(shareId).get();
    if (!shareSnap.exists) {
      res.status(404).json({ error: "share_not_found" });
      return;
    }
    const shareDoc = (shareSnap.data() || {}) as Record<string, unknown>;
    const kind = asString(shareDoc.kind).trim().toLowerCase();
    const sourceCollection = asString(shareDoc.sourceCollection);
    const sourceId = asString(shareDoc.sourceId);
    if (!kind || !sourceCollection || !sourceId) {
      res.status(404).json({ error: "share_invalid" });
      return;
    }

    const sourceSnap = await db.collection(sourceCollection).doc(sourceId).get();
    if (!sourceSnap.exists) {
      res.status(404).json({ error: "source_not_found" });
      return;
    }

    if (kind !== "screener") {
      res.status(200).json({
        shareId,
        kind,
        sourceId,
        sourceCollection,
        readOnly: true,
        unsupported: true,
      });
      return;
    }

    const source = (sourceSnap.data() || {}) as Record<string, unknown>;
    const ownerUid = asString(source.userId);
    const readOnly = !(viewer?.uid && ownerUid && viewer.uid === ownerUid);
    const results = Array.isArray(source.results) ? source.results.slice(0, 300) : [];
    const ownerProfile = ownerUid ? await readAuthorProfile(ownerUid) : { handle: "", photoURL: "" };

    res.status(200).json({
      shareId,
      kind: "screener",
      sourceId: sourceSnap.id,
      readOnly,
      canImport: Boolean(viewer?.uid && readOnly),
      screener: {
        id: sourceSnap.id,
        title: asString(source.title, "Screener run"),
        notes: asString(source.notes, ""),
        market: asString(source.market, ""),
        universe: asString(source.universe, ""),
        userId: ownerUid,
        ownerUsername: asString(source.ownerUsername || ownerProfile.handle),
        ownerAvatar: asString(source.ownerAvatar || "bull"),
        isPublic: asBoolean(source.isPublic, false),
        results,
        createdAt: source.createdAt || null,
        updatedAt: source.updatedAt || null,
      },
    });
  } catch (error) {
    console.error("[Explore] share lookup failed", error);
    res.status(500).json({ error: "share_lookup_failed" });
  }
});

app.use("/api", ROUTES);
app.use("/", ROUTES);

app.use((_req, res) => {
  res.status(404).json({ error: "not_found" });
});

app.use((error: any, _req: Request, res: Response, _next: any) => {
  console.error("[Explore] unhandled route error", error);
  res.status(500).json({ error: "internal_error" });
});

export const quanturaExploreApi = app;

async function handleCreateTrigger(postType: PostType, cloudEvent: any): Promise<void> {
  try {
    const docPath = parseDocumentPath(cloudEvent);
    if (!docPath) {
      console.warn(`[Explore] ${postType} trigger missing doc path`);
      return;
    }
    const sourceDocId = docPath.split("/").pop() || "";
    if (!sourceDocId) return;

    const fields = cloudEvent?.data?.value?.fields;
    const payload = decodeFirestoreFields(fields || {});

    if (!Object.keys(payload).length) {
      console.warn(`[Explore] ${postType} trigger had empty payload: ${docPath}`);
      return;
    }

    if (!payload.createdAt && cloudEvent?.data?.value?.createTime) {
      payload.createdAt = cloudEvent.data.value.createTime;
    }

    await createPostFromResult(postType, sourceDocId, payload);
  } catch (error) {
    console.error(`[Explore] ${postType} trigger failed`, error);
  }
}

export async function onForecastCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("forecast", cloudEvent);
}

export async function onBacktestCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("backtest", cloudEvent);
}

export async function onAgentRunCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("agent", cloudEvent);
}

export async function onScreenerRunCreated(cloudEvent: any): Promise<void> {
  await handleCreateTrigger("screener", cloudEvent);
}
