const crypto = require("crypto");
const fs = require("fs/promises");
const path = require("path");

const { onRequest } = require("firebase-functions/v2/https");
const { setGlobalOptions } = require("firebase-functions/v2/options");

const { initializeApp } = require("firebase-admin/app");
const { getRemoteConfig, RemoteConfigFetchResponse } = require("firebase-admin/remote-config");

setGlobalOptions({ region: "us-central1", maxInstances: 10 });

const adminApp = initializeApp();

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
    .replace(/<\\//g, "<\\/")
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

const resolveTemplate = (pathname) => {
  const route = normalizePath(pathname);
  if (route === "/" || route === "") return "index.html";

  const forecastingAliases = new Set([
    "/forecasting",
    "/indicators",
    "/trending",
    "/news",
    "/options",
    "/saved-forecasts",
    "/studio",
  ]);
  if (forecastingAliases.has(route)) return "forecasting.html";

  const dashboardAliases = new Set([
    "/dashboard",
    "/account",
    "/watchlist",
    "/productivity",
    "/collaboration",
    "/uploads",
    "/autopilot",
    "/notifications",
    "/purchase",
  ]);
  if (dashboardAliases.has(route)) return "dashboard.html";

  if (route === "/screener") return "screener.html";
  if (route === "/pricing") return "pricing.html";
  if (route === "/contact") return "contact.html";
  if (route === "/admin") return "admin.html";

  if (route === "/blog") return path.join("blog", "index.html");
  if (route.startsWith("/blog/posts/")) {
    const slug = route.slice("/blog/posts/".length);
    if (!slug) return path.join("blog", "index.html");
    if (slug.includes("..") || slug.includes("/") || slug.includes("\\")) return null;
    const withExt = slug.endsWith(".html") ? slug : `${slug}.html`;
    return path.join("blog", "posts", withExt);
  }

  return null;
};

const loadTemplateHtml = async (relPath) => {
  const fullPath = path.join(templatesRoot, relPath);
  const data = await fs.readFile(fullPath, "utf8");
  return data;
};

const getServerTemplate = async () => {
  const now = Date.now();
  if (cachedTemplate && now - cachedTemplateLoadedAt < TEMPLATE_TTL_MS) return cachedTemplate;
  const rc = getRemoteConfig(adminApp);
  const template = await rc.getServerTemplate();
  cachedTemplate = template;
  cachedTemplateLoadedAt = now;
  return template;
};

exports.ssr = onRequest(async (req, res) => {
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.status(405).set("Allow", "GET, HEAD").send("Method not allowed.");
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
    const fetchResponse = new RemoteConfigFetchResponse(adminApp, config);
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
});
