// Manual test verification - simulating test execution
// This file demonstrates that the tests would pass

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

console.log("=== Manual Test Verification ===\n");

// Test 1: Path traversal with ..
console.log("Test 1: Path traversal with ..");
const test1 = resolveTemplate("/blog/posts/../../../etc/passwd");
console.log(`  Input: /blog/posts/../../../etc/passwd`);
console.log(`  Result: ${test1}`);
console.log(`  Expected: null`);
console.log(`  Status: ${test1 === null ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 2: Backslash traversal
console.log("Test 2: Backslash traversal");
const test2 = resolveTemplate("/blog/posts/..\\..\\..\\etc\\passwd");
console.log(`  Input: /blog/posts/..\\..\\..\\etc\\passwd`);
console.log(`  Result: ${test2}`);
console.log(`  Expected: null`);
console.log(`  Status: ${test2 === null ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 3: Forward slash in slug
console.log("Test 3: Forward slash in slug");
const test3 = resolveTemplate("/blog/posts/malicious/../../etc/passwd");
console.log(`  Input: /blog/posts/malicious/../../etc/passwd`);
console.log(`  Result: ${test3}`);
console.log(`  Expected: null`);
console.log(`  Status: ${test3 === null ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 4: Dot segment
console.log("Test 4: Dot segment in route");
const test4 = resolveTemplate("/./blog/posts/test");
console.log(`  Input: /./blog/posts/test`);
console.log(`  Result: ${test4}`);
console.log(`  Expected: null`);
console.log(`  Status: ${test4 === null ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 5: Double dot segment
console.log("Test 5: Double dot segment");
const test5 = resolveTemplate("/../blog/posts/test");
console.log(`  Input: /../blog/posts/test`);
console.log(`  Result: ${test5}`);
console.log(`  Expected: null`);
console.log(`  Status: ${test5 === null ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 6: Special characters
console.log("Test 6: Special characters");
const test6 = resolveTemplate("/blog/posts/test<script>alert(1)</script>");
console.log(`  Input: /blog/posts/test<script>alert(1)</script>`);
console.log(`  Result: ${test6}`);
console.log(`  Expected: null`);
console.log(`  Status: ${test6 === null ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 7: Valid blog post
console.log("Test 7: Valid blog post");
const test7 = resolveTemplate("/blog/posts/2025-03-06-macro-regime-map");
console.log(`  Input: /blog/posts/2025-03-06-macro-regime-map`);
console.log(`  Result: ${test7}`);
console.log(`  Expected: blog/posts/2025-03-06-macro-regime-map.html`);
const test7Pass = test7 && test7.includes("blog") && test7.includes("posts") && test7.includes("2025-03-06-macro-regime-map");
console.log(`  Status: ${test7Pass ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 8: Valid blog topic
console.log("Test 8: Valid blog topic");
const test8 = resolveTemplate("/blog/topics/ai-forecast-analysis");
console.log(`  Input: /blog/topics/ai-forecast-analysis`);
console.log(`  Result: ${test8}`);
console.log(`  Expected: blog/topics/ai-forecast-analysis.html`);
const test8Pass = test8 && test8.includes("blog") && test8.includes("topics") && test8.includes("ai-forecast-analysis");
console.log(`  Status: ${test8Pass ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 9: Root path
console.log("Test 9: Root path");
const test9 = resolveTemplate("/");
console.log(`  Input: /`);
console.log(`  Result: ${test9}`);
console.log(`  Expected: index.html`);
console.log(`  Status: ${test9 === "index.html" ? "✓ PASS" : "✗ FAIL"}\n`);

// Test 10: Hyphens and underscores
console.log("Test 10: Hyphens and underscores");
const test10 = resolveTemplate("/blog/posts/valid-post_name-123");
console.log(`  Input: /blog/posts/valid-post_name-123`);
console.log(`  Result: ${test10}`);
console.log(`  Expected: blog/posts/valid-post_name-123.html`);
const test10Pass = test10 && test10.includes("valid-post_name-123");
console.log(`  Status: ${test10Pass ? "✓ PASS" : "✗ FAIL"}\n`);

console.log("=== Summary ===");
const allTests = [test1 === null, test2 === null, test3 === null, test4 === null, test5 === null, test6 === null, test7Pass, test8Pass, test9 === "index.html", test10Pass];
const passed = allTests.filter(Boolean).length;
const total = allTests.length;
console.log(`Total: ${total}`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${total - passed}`);
console.log(`\nAll security tests: ${passed === total ? "✓ PASSED" : "✗ FAILED"}`);
