import cors from "cors";
import express, { Request, Response } from "express";
import admin from "firebase-admin";

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

const ROUTES = express.Router();

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
      return `/backtesting?backtestId=${encodeURIComponent(sourceDocId)}`;
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
      return `${ticker} backtest (${strategy})`;
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

function isPostVisibleToViewer(post: Record<string, unknown>, viewerUid: string | null): boolean {
  const visibility = asString(post.visibility, "public");
  if (visibility === "public") return true;
  if (!viewerUid) return false;
  return viewerUid === asString(post.authorUid);
}

function toPostResponse(
  snap: admin.firestore.QueryDocumentSnapshot | admin.firestore.DocumentSnapshot,
  viewerState: { liked: boolean; reposted: boolean } = { liked: false, reposted: false }
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
    },
  };
}

async function fetchViewerEngagement(
  postIds: string[],
  viewerUid: string | null
): Promise<Map<string, { liked: boolean; reposted: boolean }>> {
  const engagement = new Map<string, { liked: boolean; reposted: boolean }>();
  postIds.forEach((id) => engagement.set(id, { liked: false, reposted: false }));

  if (!viewerUid || postIds.length === 0) return engagement;

  const likeRefs = postIds.map((postId) => db.collection("postLikes").doc(postId).collection("users").doc(viewerUid));
  const repostRefs = postIds.map((postId) => db.collection("postReposts").doc(postId).collection("users").doc(viewerUid));

  const [likeDocs, repostDocs] = await Promise.all([
    db.getAll(...likeRefs),
    db.getAll(...repostRefs),
  ]);

  likeDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false };
    if (doc.exists) current.liked = true;
    engagement.set(postId, current);
  });

  repostDocs.forEach((doc, index) => {
    const postId = postIds[index];
    const current = engagement.get(postId) || { liked: false, reposted: false };
    if (doc.exists) current.reposted = true;
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

  const prefs = ((userSnap.data() || {}) as any).notificationPrefs || {};
  const enableGlobal = asBoolean(prefs.global, true);
  const enableFollowing = asBoolean(prefs.following, true);
  const enableTickers = asBoolean(prefs.tickers, true);

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

ROUTES.get("/health", (_req, res) => {
  res.status(200).json({ ok: true, service: "quantura-explore-api", ts: new Date().toISOString() });
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
      const viewerState = engagement.get(doc.id) || { liked: false, reposted: false };
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

    const viewerState = engagement.get(postId) || { liked: false, reposted: false };

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
    const profile = (userData.profile || {}) as Record<string, unknown>;

    res.status(200).json({
      uid: userDoc.id,
      handle: normalizeHandle(userData.handle || profile.username || handle),
      photoURL: asString(userData.photoURL || profile.photoURL || ""),
      bio: asString(profile.bio || ""),
      publicProfile: asBoolean(profile.publicProfile, false),
    });
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

    const posts = page.map((doc) => toPostResponse(doc, engagement.get(doc.id) || { liked: false, reposted: false }));
    const next = hasMore && posts.length
      ? encodeCursor(buildNextCursor(posts[posts.length - 1] as Record<string, unknown>, false))
      : null;

    res.status(200).json({ posts, cursor: next, owner: isOwner });
  } catch (error) {
    console.error("[Explore] profile posts failed", error);
    res.status(500).json({ error: "profile_posts_failed" });
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
    const prefs = (userData.notificationPrefs || {}) as Record<string, unknown>;

    res.status(200).json({
      notificationPrefs: {
        global: asBoolean(prefs.global, true),
        following: asBoolean(prefs.following, true),
        tickers: asBoolean(prefs.tickers, true),
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
    await tokenRef.set(
      {
        token,
        platform,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
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
    const notificationPrefs = {
      global: asBoolean(input.global, true),
      following: asBoolean(input.following, true),
      tickers: asBoolean(input.tickers, true),
    };

    await db
      .collection("users")
      .doc(user.uid)
      .set(
        {
          notificationPrefs,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

    await syncTopicsForUser(user.uid);

    res.status(200).json({ ok: true, notificationPrefs });
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
