// Idempotent source/SSR shell migration. Original archived surfaces remain in
// Git history; private resources and subscription billing records are untouched.
import fs from "node:fs/promises";
import path from "node:path";
import { JSDOM } from "jsdom";
const root = path.resolve(new URL("..", import.meta.url).pathname);
async function walk(dir) {
  const out = [];
  for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
    const name = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...await walk(name)); else if (name.endsWith(".html")) out.push(name);
  }
  return out;
}
for (const filename of await walk(path.join(root, "pages"))) {
  let html = await fs.readFile(filename, "utf8");
  const dom = new JSDOM(html, { includeNodeLocations: true });
  const doc = dom.window.document;
  const edits = [];
  const replace = (node, text = "") => {
    const loc = dom.nodeLocation(node); if (loc) edits.push({ start: loc.startOffset, end: loc.endOffset, text });
  };
  const sidebar = Boolean(doc.querySelector(".app-sidebar"));
  for (const nav of doc.querySelectorAll(".header .nav-links")) replace(nav, `<nav class="nav-links" aria-label="Main navigation"><a href="/forecasting">Terminal</a>${sidebar ? "" : '<a href="/screener">Screener</a>'}<a href="/shop">Shop</a><a href="/blog">Blog</a><a href="https://quantura.mintlify.app/">API Docs</a></nav>`);
  for (const node of doc.querySelectorAll('.aws-integration-card, .forecast-alert-settings-card, #profile .security-summary')) replace(node);
  for (const title of doc.querySelectorAll('#profile .section-title h2')) replace(title, '<h2>Profile</h2>');
  for (const note of doc.querySelectorAll('#profile .section-title p')) replace(note, '<p class="small">Manage your private account details and saved research.</p>');
  for (const node of doc.querySelectorAll('[data-panel="autopilot"], [data-panel="notifications"], a[href="/pricing"], a[href="/purchase"], a[href="/notifications"], a[href="/autopilot"], [data-panel-target="autopilot"], [data-panel-target="notifications"], #header-notifications')) replace(node);
  for (const grid of doc.querySelectorAll(".pricing-grid")) {
    const section = grid.closest("section");
    if (section) replace(section, '<section class="section free-access-note"><div class="container"><h2>Free to explore. Yours to save.</h2><p>Forecast markets, explore the Screener and download supported data without a paid plan. Create an account to save your requests and organize private workspaces. Fair-use limits and provider availability apply.</p><a class="cta" href="/forecasting">Run a forecast</a><p class="small muted">Bring-your-own AWS infrastructure is billed separately by AWS. Third-party data licensing restrictions still apply.</p></div></section>');
  }
  // Prefer outer replacements and avoid applying nested offsets twice.
  const nonoverlapping = edits.sort((a,b) => a.start-b.start || b.end-a.end).filter((edit, i, all) => !all.slice(0,i).some(other => other.start <= edit.start && other.end >= edit.end));
  for (const edit of nonoverlapping.reverse()) html = html.slice(0,edit.start) + edit.text + html.slice(edit.end);
  if (filename.endsWith("/forecasting.html")) {
    html = html.replace(/    <script defer src="\/forecast-controls[^\n]+\n/g, "").replace(/(    <script defer src="\/app\.js)/, '    <script defer src="/forecast-controls.js?v=20260915a"></script>\n$1');
  }
  if (filename.endsWith("/pricing.html")) html = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="robots" content="noindex"><meta http-equiv="refresh" content="0;url=/forecasting"><title>Quantura is free</title></head><body><p>Paid plans are archived. <a href="/forecasting">Use Quantura for free</a>. Existing subscribers can manage billing in their account.</p><footer><a href="/terms">Terms</a> · <a href="/privacy">Privacy</a> · <a href="/disclaimer">Disclaimer</a></footer></body></html>\n';
  html = html.replace(/[\t ]+$/gm, "");
  await fs.writeFile(filename, html);
  const relative = path.relative(path.join(root, "pages"), filename);
  const template = path.join(root, "functions_ssr/templates", relative);
  await fs.mkdir(path.dirname(template), { recursive: true });
  await fs.writeFile(template, html);
  // Some routes are also committed static copies (not all SSR pages are).
  const published = path.join(root, "public", relative);
  if (await fs.stat(published).catch(() => null)) await fs.writeFile(published, html);
  dom.window.close();
}
console.info("Free-product source pages, SSR templates and existing static copies synchronized.");
