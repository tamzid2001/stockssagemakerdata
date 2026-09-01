/**
 * Test Verification Script
 * This script simulates running the tests to verify they would pass
 */

const path = require("path");

// Copy the functions from index.js
function normalizePath(rawPath) {
  const pathname = String(rawPath || "/").split("?")[0] || "/";
  if (pathname.length > 1 && pathname.endsWith("/")) return pathname.slice(0, -1);
  return pathname;
}

function templateFromRoute(route) {
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
}

function resolveTemplate(pathname) {
  const route = normalizePath(pathname);
  if (route === "/" || route === "") return "index.html";

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

  return templateFromRoute(route);
}

console.log("=== Test Verification ===\n");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
    passed++;
  } catch (error) {
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
    failed++;
  }
}

// Security Tests
test("should reject .. traversal", () => {
  const result = resolveTemplate("/blog/posts/../../../etc/passwd");
  if (result !== null) throw new Error(`Expected null, got ${result}`);
});

test("should reject backslash traversal", () => {
  const result = resolveTemplate("/blog/posts/..\\..\\..\\etc\\passwd");
  if (result !== null) throw new Error(`Expected null, got ${result}`);
});

test("should reject slash in slug", () => {
  const result = resolveTemplate("/blog/posts/malicious/../../etc/passwd");
  if (result !== null) throw new Error(`Expected null, got ${result}`);
});

test("should safely handle URL-encoded paths", () => {
  const result = resolveTemplate("/blog/posts/%2e%2e%2f%2e%2e%2fetc");
  if (result === null) throw new Error("Should handle encoded paths safely");
  if (!result.includes("%2e%2e")) throw new Error("Should preserve encoded characters");
});

test("should reject mixed traversal", () => {
  const result = resolveTemplate("/blog/posts/../topics/../../../etc/passwd");
  if (result !== null) throw new Error(`Expected null, got ${result}`);
});

test("should reject embedded traversal", () => {
  const result = resolveTemplate("/blog/posts/valid-post/../../../etc/passwd");
  if (result !== null) throw new Error(`Expected null, got ${result}`);
});

// Valid path tests
test("should accept valid blog post", () => {
  const result = resolveTemplate("/blog/posts/2025-03-06-macro-regime-map");
  if (result === null) throw new Error("Should accept valid path");
  if (!result.includes("blog")) throw new Error("Should contain 'blog'");
  if (!result.includes("posts")) throw new Error("Should contain 'posts'");
});

test("should accept root path", () => {
  const result = resolveTemplate("/");
  if (result !== "index.html") throw new Error(`Expected index.html, got ${result}`);
});

console.log(`\n=== Summary ===`);
console.log(`Total: ${passed + failed}`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);

process.exit(failed === 0 ? 0 : 1);
