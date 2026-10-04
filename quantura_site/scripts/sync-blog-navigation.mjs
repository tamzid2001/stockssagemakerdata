import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(await fs.readFile(path.join(root, "public/blog/posts.manifest.json"), "utf8"));
const posts = manifest.posts.filter(post => /^[a-z0-9-]+$/.test(post.slug)).sort((a, b) => a.dateIso.localeCompare(b.dateIso) || a.slug.localeCompare(b.slug));
const escape = value => String(value).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const link = (post, direction) => post
  ? `<a class="cta secondary" rel="${direction === "Previous" ? "prev" : "next"}" href="/blog/posts/${post.slug}" aria-label="${direction} blog: ${escape(post.title)}">${direction} blog</a>`
  : `<button class="cta secondary" type="button" disabled>${direction} blog</button>`;
for (let i = 0; i < posts.length; i++) {
  const file = path.join(root, "pages/blog/posts", posts[i].slug + ".html");
  let html = await fs.readFile(file, "utf8");
  html = html.replace(/\s*<!-- BLOG_NAVIGATION_START -->[\s\S]*?<!-- BLOG_NAVIGATION_END -->/g, "");
  const nav = `\n<!-- BLOG_NAVIGATION_START -->\n<nav class="blog-post-navigation" aria-label="More blog posts">${link(posts[i - 1], "Previous")}${link(posts[i + 1], "Next")}<button class="cta secondary" type="button" data-copy-blog-link>Copy shareable link</button><span class="small" role="status" data-blog-share-status></span></nav>\n<!-- BLOG_NAVIGATION_END -->\n`;
  html = html.replace(/<\/article>/, nav + "</article>");
  if (!html.includes('/blog-navigation.js')) html = html.replace("</head>", '    <script defer src="/blog-navigation.js?v=20261003"></script>\n  </head>');
  await fs.writeFile(file, html);
  await fs.copyFile(file, path.join(root, "public/blog/posts", posts[i].slug + ".html"));
}
console.info(`Updated navigation on ${posts.length} blog posts.`);
