const crypto = require("crypto");
const fs = require("fs/promises");
const path = require("path");
const express = require("express");

const { onRequest } = require("firebase-functions/v2/https");
const { setGlobalOptions } = require("firebase-functions/v2/options");

const { cert, initializeApp } = require("firebase-admin/app");
const { getRemoteConfig, RemoteConfigFetchResponse } = require("firebase-admin/remote-config");

setGlobalOptions({ region: "us-central1", maxInstances: 10, memory: "512MiB" });

let adminApp = null;

const getAdminApp = () => {
  if (adminApp) return adminApp;
  const rawServiceAccount = String(process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "").trim();
  adminApp = rawServiceAccount
    ? initializeApp({ credential: cert(JSON.parse(rawServiceAccount)) })
    : initializeApp();
  return adminApp;
};

const TEMPLATE_TTL_MS = 5 * 60 * 1000;
let cachedTemplate = null;
let cachedTemplateLoadedAt = 0;
let lastRemoteConfigErrorLogAt = 0;

const templatesRoot = path.join(__dirname, "templates");

const parseCookies = (header) => {
  const out = {};
  if (!header) return out;
  const parts = String(header).split(";");
  for (const part of parts) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (!key) continue;
    out[key] = decodeURIComponent(value);
  }
  return out;
};

const ensureRandomizationId = (req, res) => {
  const cookies = parseCookies(req.headers.cookie || "");
  const existing = String(cookies.qs_rcid || "").trim();
  if (existing) return existing;

  const id = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex");
  // Stable assignment cookie for A/B targeting. This is a functional cookie (not analytics).
  res.setHeader("Set-Cookie", `qs_rcid=${encodeURIComponent(id)}; Path=/; Max-Age=${60 * 60 * 24 * 400}; SameSite=Lax; Secure`);
  return id;
};

const safeJsonForHtml = (value) =>
  JSON.stringify(value)
    // Guard against closing the script tag.
    .replace(/<\//g, "<\\/")
    // Avoid HTML parser confusion.
    .replace(/</g, "\\u003c");

const injectRemoteConfig = (html, { initialFetchResponse }) => {
  if (!initialFetchResponse) return html;
  const marker = "</head>";
  const payload = `
    <script>
      window.__QUANTURA_RC_TEMPLATE_ID__ = "firebase-server";
      window.__QUANTURA_RC_INITIAL_FETCH_RESPONSE__ = ${safeJsonForHtml(initialFetchResponse)};
    </script>
  `;
  if (html.includes(marker)) {
    return html.replace(marker, `${payload}\n${marker}`);
  }
  return `${payload}\n${html}`;
};

const normalizePath = (rawPath) => {
  const pathname = String(rawPath || "/").split("?")[0] || "/";
  if (pathname.length > 1 && pathname.endsWith("/")) return pathname.slice(0, -1);
  return pathname;
};

const templateFromRoute = (route) => {
  const clean = String(route || "").trim();
  if (!clean || clean === "/") return "index.html";
  const segments = clean
    .replace(/^\//, "")
    .split("/")
    .filter(Boolean);
  if (!segments.length) return "index.html";
  const segmentPattern = /^[a-zA-Z0-9._-]+$/;
  if (segments.some((segment) => segment === "." || segment === ".." || !segmentPattern.test(segment))) {
    return null;
  }
  const rel = path.join(...segments);
  return rel.endsWith(".html") ? rel : `${rel}.html`;
};

const resolveTemplate = (pathname) => {
  const route = normalizePath(pathname);
  if (route === "/" || route === "") return "index.html";

  const forecastingAliases = new Set([
    "/forecasting",
    "/autopilot",
    "/uploads",
    "/sports-forecasting",
    "/terminal/fx",
    "/tools/fx",
    "/predictions",
    "/indicators",
    "/news",
    "/market-headlines",
    "/options",
    "/saved-forecasts",
    "/studio",
    "/forecast",
    "/terminal",
    "/gpt-5",
    "/gpt5",
  ]);
  if (forecastingAliases.has(route)) return "forecasting.html";
  if (route === "/events-calendar") return "events.html";

  const dashboardAliases = new Set([
    "/dashboard",
    "/account",
    "/watchlist",
    "/productivity",
    "/collaboration",
    "/notifications",
  ]);
  if (dashboardAliases.has(route)) return "dashboard.html";

  if (route === "/screener") return "screener.html";
  if (route === "/research") return "research.html";
  if (route === "/pricing") return "pricing.html";
  if (route === "/purchase") return "pricing.html";
  if (route === "/contact") return "contact.html";
  if (route === "/shop") return "shop.html";
  if (route === "/admin") return "admin.html";
  if (route === "/terms") return "terms.html";
  if (route === "/privacy") return "privacy.html";
  if (route === "/disclaimer") return "disclaimer.html";

  if (route === "/blog") return path.join("blog", "index.html");
  if (route === "/blogs") return path.join("blog", "index.html");
  if (route.startsWith("/blog/posts/")) {
    const slug = route.slice("/blog/posts/".length);
    if (!slug) return path.join("blog", "index.html");
    if (slug.includes("..") || slug.includes("/") || slug.includes("\\")) return null;
    const withExt = slug.endsWith(".html") ? slug : `${slug}.html`;
    return path.join("blog", "posts", withExt);
  }
  if (route.startsWith("/blog/topics/")) {
    const topic = route.slice("/blog/topics/".length);
    if (!topic) return path.join("blog", "index.html");
    if (topic.includes("..") || topic.includes("/") || topic.includes("\\")) return null;
    const withExt = topic.endsWith(".html") ? topic : `${topic}.html`;
    return path.join("blog", "topics", withExt);
  }

  if (route === "/ticker" || route.startsWith("/ticker/")) return "ticker.html";

  return templateFromRoute(route);
};

const REMOVED_MODEL_COUNCIL_ROUTES = new Set([
  "/model-council",
  "/ticker-query",
  "/ask-gpt-5",
  "/ask-gpt5",
  "/ask-gpt",
]);

const loadTemplateHtml = async (relPath) => {
  const fullPath = path.join(templatesRoot, relPath);
  const data = await fs.readFile(fullPath, "utf8");
  return data;
};

const getServerTemplate = async () => {
  const now = Date.now();
  if (cachedTemplate && now - cachedTemplateLoadedAt < TEMPLATE_TTL_MS) return cachedTemplate;
  const rc = getRemoteConfig(getAdminApp());
  const template = rc.initServerTemplate({
    // Keep UI stable even if the server template has not been published yet.
    defaultConfig: {
      welcome_message: "Welcome to Quantura",
      watchlist_enabled: "true",
      forecast_prophet_enabled: "true",
      forecast_canvas_enabled: "true",
      enable_social_leaderboard: "true",
      forecast_model_primary: "Quantura Horizon",
      promo_banner_text: "",
      maintenance_mode: "false",
      volatility_threshold: "0.05",
      llm_allowed_models: "gpt-5-nano,gpt-5-mini,gpt-5,gpt-5.1,gpt-5.2",
      ai_usage_tiers: JSON.stringify({
        free: {
          allowed_models: ["gpt-5-nano", "gpt-5-mini"],
          weekly_limit: 3,
          daily_limit: 3,
          volatility_alerts: false,
          workspace_limit: 0,
          ad_free: false,
        },
        go: {
          allowed_models: ["gpt-5-nano", "gpt-5-mini"],
          weekly_limit: 10,
          daily_limit: 10,
          volatility_alerts: true,
          workspace_limit: 1,
          ad_free: true,
        },
        plus: {
          allowed_models: ["gpt-5-mini", "gpt-5"],
          weekly_limit: 25,
          daily_limit: 25,
          volatility_alerts: true,
          workspace_limit: 3,
          ad_free: true,
        },
        pro: {
          allowed_models: ["gpt-5-mini", "gpt-5", "gpt-5.1"],
          weekly_limit: 60,
          daily_limit: 60,
          volatility_alerts: true,
          workspace_limit: 8,
          ad_free: true,
        },
        business: {
          allowed_models: ["gpt-5-nano", "gpt-5-mini", "gpt-5", "gpt-5.1", "gpt-5.2", "amazon.nova-lite-v1:0", "amazon.nova-pro-v1:0"],
          weekly_limit: 150,
          daily_limit: 150,
          volatility_alerts: true,
          workspace_limit: 30,
          ad_free: true,
        },
      }),
      push_notifications_enabled: "true",
      webpush_vapid_key: "",
      stripe_checkout_enabled: "true",
      stripe_public_key: "",
      native_ios_storekit_checkout_only: "true",
      native_android_play_billing_enabled: "true",
      native_iap_product_ids: JSON.stringify({
        ios: {
          go: "goplan",
          plus: "premium",
          pro: "pro",
          business: "businessplan",
          desk: "businessplan",
          forecast: "goplan",
          annual_go: "annualgoplan",
          annual_plus: "annualplusplan",
          annual_pro: "pro",
          annual_business: "annualbusinessplan",
          default: "pro",
        },
        android: {
          go: "goplan",
          plus: "premium",
          pro: "quanturapro",
          business: "quanturabusiness",
          desk: "quanturabusiness",
          forecast: "goplan",
          annual_go: "goplanyearly",
          annual_plus: "annualplusplan",
          annual_pro: "quanturapro",
          annual_business: "annualbusinessplan",
          default: "quanturapro",
        },
      }),
      ads_use_real_ios: "true",
      ads_use_real_android: "true",
      holiday_promo: "false",
    },
  });
  try {
    await template.load();
  } catch (error) {
    const nowMs = Date.now();
    if (nowMs - lastRemoteConfigErrorLogAt > 60 * 1000) {
      lastRemoteConfigErrorLogAt = nowMs;
      // eslint-disable-next-line no-console
      console.error("Remote Config SSR: unable to load server template, using defaults.", error?.message || error);
    }
    // If the server template doesn't exist yet, seed an empty template so evaluate() can still run.
    // This keeps initialFetchResponse working with the defaultConfig values.
    try {
      template.set({ etag: "qs_default", parameters: {}, conditions: [] });
    } catch (seedError) {
      // eslint-disable-next-line no-console
      console.error("Remote Config SSR: failed to seed default template cache.", seedError?.message || seedError);
    }
  }
  cachedTemplate = template;
  cachedTemplateLoadedAt = now;
  return template;
};

const ssrHandler = async (req, res) => {
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.status(405).set("Allow", "GET, HEAD").send("Method not allowed.");
    return;
  }

  if (REMOVED_MODEL_COUNCIL_ROUTES.has(normalizePath(req.path || "/"))) {
    res.redirect(308, "/forecasting");
    return;
  }

  const relTemplatePath = resolveTemplate(req.path || "/");
  if (!relTemplatePath) {
    res.status(404).send("Not found.");
    return;
  }

  let html = "";
  try {
    html = await loadTemplateHtml(relTemplatePath);
  } catch (error) {
    res.status(404).send("Not found.");
    return;
  }

  let initialFetchResponse = null;
  try {
    const template = await getServerTemplate();
    const randomizationId = ensureRandomizationId(req, res);
    const config = template.evaluate({
      randomizationId,
      path: normalizePath(req.path || "/"),
    });
    const fetchResponse = new RemoteConfigFetchResponse(getAdminApp(), config);
    initialFetchResponse = typeof fetchResponse.toJSON === "function" ? fetchResponse.toJSON() : fetchResponse;
  } catch (error) {
    const now = Date.now();
    if (now - lastRemoteConfigErrorLogAt > 60 * 1000) {
      lastRemoteConfigErrorLogAt = now;
      // eslint-disable-next-line no-console
      console.error("Remote Config SSR hydration failed:", error?.message || error);
    }
    initialFetchResponse = null;
  }

  const rendered = injectRemoteConfig(html, { initialFetchResponse });
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  // HTML varies by functional cookie (qs_rcid). Avoid long-lived CDN caching across users.
  res.setHeader("Cache-Control", "private, no-cache, max-age=0, must-revalidate");
  res.status(200).send(req.method === "HEAD" ? "" : rendered);
};

const vercelApp = express();
vercelApp.use(ssrHandler);

module.exports = vercelApp;
module.exports.ssr = onRequest(ssrHandler);
module.exports.ssrHandler = ssrHandler;
