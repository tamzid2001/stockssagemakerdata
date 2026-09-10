// Editorial tool, never called on page loads/builds. Review staged photos before --publish.
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { imageUrl, photoFigure, escapeHtml, referralUrl } from "./blog-photo.mjs";

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registryPath = path.join(site, "brand/blog-photos.json");
const manifestPath = path.join(site, "pages/blog/posts.manifest.json");
const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
const queries = {
  "macro-signals": "financial district city skyline",
  "technical-risk": "stock market analysis computer",
  "forecasting-workflows": "data analysis laptop desk",
  "market-narratives": "business newspaper",
  "forecast-alerts": "smartphone desk business",
  "ai-forecast-analysis": "computer data technology",
  "build-notes": "software developer code",
  "data-validation": "data server network",
  "portfolio-playbooks": "finance research desk",
  "sagemaker-canvas": "computer workstation",
};
const accessKey = process.env.UNSPLASH_ACCESS_KEY;
async function api(url) {
  if (!accessKey) throw new Error("UNSPLASH_ACCESS_KEY must be provided server-side");
  const parsed = new URL(url);
  if (parsed.protocol !== "https:" || parsed.hostname !== "api.unsplash.com") throw new Error("Unapproved API host");
  parsed.searchParams.delete("client_id");
  const response = await fetch(parsed, { headers: { Authorization: `Client-ID ${accessKey}`, "Accept-Version": "v1" }, redirect: "error", signal: AbortSignal.timeout(20000) });
  if (!response.ok) throw new Error(`Unsplash returned HTTP ${response.status}; no files published`);
  if (Number(response.headers.get("x-ratelimit-remaining")) < 2) throw new Error("Unsplash request budget exhausted; retry after the quota resets");
  return response.json();
}
async function saveRegistry(registry) { await fs.writeFile(registryPath, JSON.stringify(registry, null, 2) + "\n"); }
if (process.argv.includes("--stage")) {
  try { await fs.access(registryPath); throw new Error("An editorial registry already exists; preserve existing selections"); } catch (error) { if (error.code !== "ENOENT") throw error; }
  const registry = { version: 1, source: "Unsplash API v1", fetched_at: new Date().toISOString(), photos: {}, posts: {} };
  const used = new Set();
  for (const [topic, query] of Object.entries(queries)) {
    const posts = manifest.posts.filter(post => post.topic === topic);
    const url = new URL("https://api.unsplash.com/search/photos");
    for (const [key, value] of Object.entries({ query, orientation: "landscape", content_filter: "high", per_page: "30" })) url.searchParams.set(key, value);
    const photos = [];
    for (let page = 1; page <= 3 && photos.length < posts.length; page++) {
      url.searchParams.set("page", String(page));
      const data = await api(url);
      photos.push(...(data.results || []).filter(photo => !used.has(photo.id) && !photos.some(item => item.id === photo.id) && photo.width >= 1280 && photo.user?.name && photo.links?.download_location && photo.urls?.regular));
    }
    if (photos.length < posts.length) throw new Error(`Not enough suitable photos for ${topic}; review the editorial query`);
    for (const [index, post] of posts.entries()) {
      const photo = photos[index]; used.add(photo.id);
      const item = { id: photo.id, query, image_url: photo.urls.regular, width: photo.width, height: photo.height, alt: photo.alt_description || "Editorial photograph", photographer: photo.user.name, photographer_url: referralUrl(photo.user.links.html), photo_url: referralUrl(photo.links.html), download_location: photo.links.download_location, download_tracked_at: null };
      imageUrl(item); // Validate every source before it can reach a page.
      registry.photos[item.id] = item;
      registry.posts[post.slug] = item.id;
    }
  }
  await saveRegistry(registry);
  console.log(`Staged ${Object.keys(registry.posts).length} editorial selections for visual review; nothing published.`);
} else if (process.argv.includes("--publish") || process.argv.includes("--render")) {
  const registry = JSON.parse(await fs.readFile(registryPath, "utf8"));
  // Checkpoint each tracked selection so retrying never re-counts completed work.
  if (process.argv.includes("--publish")) for (const photo of Object.values(registry.photos)) {
    if (photo.download_tracked_at) continue;
    const url = new URL(photo.download_location);
    if (url.pathname !== `/photos/${photo.id}/download`) throw new Error("Invalid download tracking endpoint");
    await api(url);
    photo.download_tracked_at = new Date().toISOString();
    await saveRegistry(registry);
  }
  const pending = [];
  for (const post of manifest.posts) {
    const photo = registry.photos[registry.posts[post.slug]];
    if (!photo || !/^[a-z0-9-]+$/.test(post.slug)) throw new Error("Unreviewed post or invalid slug");
    const figure = photoFigure(photo);
    for (const root of ["pages", "public"]) {
      const file = path.join(site, root, "blog/posts", `${post.slug}.html`);
      let html = await fs.readFile(file, "utf8");
      const pattern = /<figure\b[^>]*>\s*<img\b[^>]*(?:hero-illustration\.svg|images\.unsplash\.com)[\s\S]*?<\/figure>/;
      if (!pattern.test(html)) throw new Error(`Expected one editorial hero in ${post.slug}`);
      html = html.replace(pattern, figure).replace(/(<meta (?:property="og:image"|name="twitter:image") content=")[^"]*("\s*\/?>)/g, `$1${escapeHtml(imageUrl(photo))}$2`);
      html = html.replace(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g, (full, body) => {
        const value = JSON.parse(body);
        if (value["@type"] !== "BlogPosting" && value["@type"] !== "Article") return full;
        value.image = { "@type": "ImageObject", url: imageUrl(photo), creditText: `Photo by ${photo.photographer} on Unsplash`, creator: { "@type": "Person", name: photo.photographer, url: photo.photographer_url } };
        return `<script type="application/ld+json">${JSON.stringify(value, null, 2).replace(/</g, "\\u003c")}</script>`;
      });
      pending.push([file, html]);
    }
    post.heroImage = imageUrl(photo);
    post.heroPhoto = { provider: "unsplash", id: photo.id, alt: photo.alt, photographer: photo.photographer, photographerUrl: photo.photographer_url, sourceUrl: photo.photo_url, selectedAt: photo.download_tracked_at };
  }
  // All source documents validate before any page is overwritten. No directory deletion.
  for (const [file, html] of pending) await fs.writeFile(file, html);
  manifest.imageryUpdatedAt = new Date().toISOString();
  for (const root of ["pages", "public"]) await fs.writeFile(path.join(site, root, "blog/posts.manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
  console.log(`Published ${manifest.posts.length} credited Unsplash blog photos across canonical and static pages.`);
} else throw new Error("Use --stage, then visually review, then --publish; --render makes no API requests.");
