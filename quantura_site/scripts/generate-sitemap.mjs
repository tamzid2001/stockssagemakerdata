#!/usr/bin/env node
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const siteRoot = path.resolve(__dirname, "..");
const pagesRoot = path.join(siteRoot, "pages");
const publicRoot = path.join(siteRoot, "public");
const outputPath = path.join(publicRoot, "sitemap.xml");

const baseUrl = String(process.env.SITE_URL || "https://quantura.studio").replace(/\/$/, "");

const EXCLUDE_FILES = new Set([
  "admin.html",
  "forecast-admin.html",
  "forecast-detail.html",
]);

const FORECAST_HUB_ROUTES = [
  "/forecasts/markets",
  "/forecasts/earnings",
  "/forecasts/corporate",
  "/forecasts/technology",
  "/forecasts/politics",
  "/forecasts/economics",
  "/forecasts/sports",
];

async function walkHtmlFiles(dir, out = []) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      await walkHtmlFiles(fullPath, out);
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".html")) {
      out.push(fullPath);
    }
  }
  return out;
}

function normalizeRouteFromPage(relativePath) {
  const normalized = relativePath.replace(/\\/g, "/");
  if (normalized === "index.html") return "/";
  if (normalized === "blog/index.html") return "/blog";
  let route = `/${normalized.replace(/\.html$/, "")}`;
  if (route.endsWith("/index")) {
    route = route.slice(0, -"/index".length) || "/";
  }
  return route;
}

function hasNoIndex(html) {
  const normalized = String(html || "").toLowerCase();
  if (!normalized.includes("name=\"robots\"")) return false;
  return normalized.includes("noindex");
}

async function collectRoutes() {
  const files = await walkHtmlFiles(pagesRoot);
  const routes = [];
  for (const filePath of files) {
    const rel = path.relative(pagesRoot, filePath);
    if (EXCLUDE_FILES.has(path.basename(rel))) continue;
    if (/\s\d+\.html$/i.test(rel)) continue;

    const html = await fs.readFile(filePath, "utf8");
    if (hasNoIndex(html)) continue;

    const route = normalizeRouteFromPage(rel);
    if (!route) continue;

    const stat = await fs.stat(filePath);
    routes.push({
      route,
      lastmod: stat.mtime.toISOString().slice(0, 10),
    });
  }

  const rssPath = path.join(publicRoot, "blog", "rss.xml");
  try {
    const rssStat = await fs.stat(rssPath);
    routes.push({ route: "/blog/rss.xml", lastmod: rssStat.mtime.toISOString().slice(0, 10) });
  } catch {
    // RSS feed is optional.
  }

  const forecastPagePath = path.join(pagesRoot, "forecasts.html");
  try {
    const forecastStat = await fs.stat(forecastPagePath);
    const lastmod = forecastStat.mtime.toISOString().slice(0, 10);
    for (const route of FORECAST_HUB_ROUTES) routes.push({ route, lastmod });
  } catch {
    // Forecast hub is optional during staged rollouts.
  }

  const dedup = new Map();
  for (const item of routes) {
    const key = item.route;
    const previous = dedup.get(key);
    if (!previous || item.lastmod > previous.lastmod) {
      dedup.set(key, item);
    }
  }

  return Array.from(dedup.values()).sort((a, b) => a.route.localeCompare(b.route));
}

function renderSitemapXml(items) {
  const lines = [];
  lines.push('<?xml version="1.0" encoding="UTF-8"?>');
  lines.push('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">');
  for (const item of items) {
    lines.push("  <url>");
    lines.push(`    <loc>${baseUrl}${item.route}</loc>`);
    lines.push(`    <lastmod>${item.lastmod}</lastmod>`);
    lines.push("  </url>");
  }
  lines.push("</urlset>");
  lines.push("");
  return lines.join("\n");
}

async function main() {
  const routes = await collectRoutes();
  const xml = renderSitemapXml(routes);
  await fs.writeFile(outputPath, xml, "utf8");
  console.log(`Sitemap generated with ${routes.length} routes -> ${outputPath}`);
}

main().catch((error) => {
  console.error("Failed to generate sitemap:", error);
  process.exitCode = 1;
});
