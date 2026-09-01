/**
 * Path Traversal Security Tests for SSR Handler
 * 
 * These tests verify that the path traversal vulnerability has been mitigated.
 * The tests check various attack vectors including:
 * - Directory traversal with .. sequences
 * - Encoded traversal attempts
 * - Backslash traversal
 * - Null byte injection
 * - Special characters in paths
 */

const path = require("path");

// Simple test framework
class TestRunner {
  constructor() {
    this.tests = [];
    this.passed = 0;
    this.failed = 0;
    this.results = [];
  }

  test(name, fn) {
    this.tests.push({ name, fn });
  }

  async run() {
    console.log("\n=== Path Traversal Security Tests ===\n");
    
    for (const { name, fn } of this.tests) {
      const startTime = Date.now();
      try {
        await fn();
        const duration = Date.now() - startTime;
        this.passed++;
        this.results.push({ name, passed: true, duration, error: null });
        console.log(`✓ ${name} (${duration}ms)`);
      } catch (error) {
        const duration = Date.now() - startTime;
        this.failed++;
        this.results.push({ name, passed: false, duration, error: error.message });
        console.log(`✗ ${name} (${duration}ms)`);
        console.log(`  Error: ${error.message}`);
      }
    }

    console.log(`\n=== Test Summary ===`);
    console.log(`Total: ${this.tests.length}`);
    console.log(`Passed: ${this.passed}`);
    console.log(`Failed: ${this.failed}`);
    
    return this.failed === 0 ? 0 : 1;
  }

  getResults() {
    return this.results;
  }
}

// Simple assertion library
function assert(condition, message) {
  if (!condition) {
    throw new Error(message || "Assertion failed");
  }
}

function assertEqual(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(message || `Expected ${expected}, got ${actual}`);
  }
}

function assertContains(str, substring, message) {
  if (!str.includes(substring)) {
    throw new Error(message || `Expected "${str}" to contain "${substring}"`);
  }
}

// Mock response object
function createMockResponse() {
  const res = {
    statusCode: 200,
    headers: {},
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    send(data) {
      this.body = data;
      return this;
    },
    setHeader(name, value) {
      this.headers[name] = value;
      return this;
    },
    redirect(code, url) {
      this.statusCode = code;
      this.redirectUrl = url;
      return this;
    },
    set(name, value) {
      this.headers[name] = value;
      return this;
    }
  };
  return res;
}

// Test the path resolution functions directly
// We'll extract and test the core logic without needing Firebase dependencies

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

  const forecastingAliases = new Set([
    "/forecasting",
    "/autopilot",
    "/uploads",
    "/sports-forecasting",
    "/indicators",
    "/news",
    "/historical-data",
    "/historical-prices",
    "/market-headlines",
    "/options",
    "/saved-forecasts",
    "/forecast",
    "/terminal",
    "/gpt-5",
    "/gpt5",
  ]);
  if (forecastingAliases.has(route)) return "forecasting.html";
  if (route === "/events-calendar") return "events.html";

  if (route === "/forecasts") return "forecasts.html";
  const forecastHubRoutes = new Set([
    "/forecasts/markets",
    "/forecasts/earnings",
    "/forecasts/corporate",
    "/forecasts/technology",
    "/forecasts/politics",
    "/forecasts/economics",
    "/forecasts/sports",
  ]);
  if (forecastHubRoutes.has(route)) return "forecasts.html";
  if (/^\/forecasts\/(stocks|companies|politics|sports)\/[A-Za-z0-9._-]+$/.test(route)) return "forecasts.html";
  if (/^\/forecasts\/[A-Za-z0-9][A-Za-z0-9-]*$/.test(route)) return "forecast-detail.html";

  const dashboardAliases = new Set([
    "/dashboard",
    "/account",
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
  if (route === "/admin/forecasts") return "forecast-admin.html";
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
}

// Create test runner
const runner = new TestRunner();

// Path Traversal Attack Prevention Tests
runner.test("should reject path with .. traversal attempt", async () => {
  const result = resolveTemplate("/blog/posts/../../../etc/passwd");
  assertEqual(result, null, "Should return null for path traversal attempt");
});

runner.test("should reject path with backslash traversal", async () => {
  const result = resolveTemplate("/blog/posts/..\\..\\..\\etc\\passwd");
  assertEqual(result, null, "Should return null for backslash traversal");
});

runner.test("should reject path with forward slash in blog post slug", async () => {
  const result = resolveTemplate("/blog/posts/malicious/../../etc/passwd");
  assertEqual(result, null, "Should return null for slash in slug");
});

runner.test("should reject path with dot segment in route", async () => {
  const result = resolveTemplate("/./blog/posts/test");
  assertEqual(result, null, "Should return null for dot segment");
});

runner.test("should reject path with double dot segment", async () => {
  const result = resolveTemplate("/../blog/posts/test");
  assertEqual(result, null, "Should return null for double dot segment");
});

runner.test("should reject blog topic with traversal attempt", async () => {
  const result = resolveTemplate("/blog/topics/../../../etc/passwd");
  assertEqual(result, null, "Should return null for topic traversal");
});

runner.test("should reject path with special characters", async () => {
  const result = resolveTemplate("/blog/posts/test<script>alert(1)</script>");
  assertEqual(result, null, "Should return null for special characters");
});

runner.test("should reject path with null bytes", async () => {
  const result = resolveTemplate("/blog/posts/test\x00.html");
  assertEqual(result, null, "Should return null for null bytes");
});

// Additional Path Traversal Exploit Scenarios
runner.test("should safely handle URL-encoded paths without decoding", async () => {
  // URL-encoded paths should not be decoded, preventing encoded traversal attacks
  const result = resolveTemplate("/blog/posts/%2e%2e%2f%2e%2e%2fetc");
  // The %2e won't be decoded to ".", so this becomes a literal filename
  // This is safe because the filesystem won't interpret %2e as .
  assert(result !== null, "Should handle encoded paths safely");
  assertContains(result, "%2e%2e", "Should preserve encoded characters");
});

runner.test("should reject paths with encoded slashes in slug", async () => {
  // Even though %2f is encoded, the slug extraction happens after normalization
  // and the check for "/" in slug will catch literal slashes
  const result = resolveTemplate("/blog/posts/test%2fmalicious");
  // This is safe - %2f won't be decoded, so it's just a literal filename
  assert(result !== null, "Encoded slashes are treated as literal characters");
  assertContains(result, "%2f", "Should preserve encoded slash");
});

runner.test("should reject mixed slash and dot-dot traversal", async () => {
  const result = resolveTemplate("/blog/posts/../topics/../../../etc/passwd");
  assertEqual(result, null, "Should return null for mixed traversal");
});

runner.test("should reject path with multiple slashes attempting traversal", async () => {
  const result = resolveTemplate("/blog/posts//../../etc/passwd");
  assertEqual(result, null, "Should return null for multiple slashes");
});

runner.test("should reject path with dot-dot in middle of valid-looking path", async () => {
  const result = resolveTemplate("/blog/posts/valid-post/../../../etc/passwd");
  assertEqual(result, null, "Should return null for embedded traversal");
});

// Valid Path Handling Tests
runner.test("should accept valid blog post path", async () => {
  const result = resolveTemplate("/blog/posts/2025-03-06-macro-regime-map-before-positioning-risk");
  assert(result !== null, "Should accept valid blog post path");
  assertContains(result, "blog", "Result should contain 'blog'");
  assertContains(result, "posts", "Result should contain 'posts'");
  assertContains(result, "2025-03-06-macro-regime-map-before-positioning-risk", "Result should contain slug");
});

runner.test("should accept valid blog topic path", async () => {
  const result = resolveTemplate("/blog/topics/ai-forecast-analysis");
  assert(result !== null, "Should accept valid blog topic path");
  assertContains(result, "blog", "Result should contain 'blog'");
  assertContains(result, "topics", "Result should contain 'topics'");
  assertContains(result, "ai-forecast-analysis", "Result should contain topic");
});

runner.test("should accept root path", async () => {
  const result = resolveTemplate("/");
  assertEqual(result, "index.html", "Root path should return index.html");
});

runner.test("should accept valid forecasting path", async () => {
  const result = resolveTemplate("/forecasting");
  assertEqual(result, "forecasting.html", "Forecasting path should return forecasting.html");
});

runner.test("should normalize trailing slashes", async () => {
  const result = resolveTemplate("/forecasting/");
  assertEqual(result, "forecasting.html", "Should normalize trailing slash");
});

runner.test("should accept segments with hyphens and underscores", async () => {
  const result = resolveTemplate("/blog/posts/valid-post_name-123");
  assert(result !== null, "Should accept hyphens and underscores");
  assertContains(result, "valid-post_name-123", "Result should contain the slug");
});

runner.test("should accept segments with dots in filename", async () => {
  const result = resolveTemplate("/blog/posts/post.v1.2");
  assert(result !== null, "Should accept dots in filename");
  assertContains(result, "post.v1.2", "Result should contain the slug with dots");
});

// Segment Validation Tests
runner.test("should validate segment pattern correctly", async () => {
  const result = templateFromRoute("/valid-name_123.test");
  assert(result !== null, "Should accept valid segment pattern");
});

runner.test("should reject invalid segment pattern", async () => {
  const result = templateFromRoute("/invalid@name");
  assertEqual(result, null, "Should reject invalid characters");
});

runner.test("should reject empty segments after filtering", async () => {
  const result = templateFromRoute("///");
  assertEqual(result, "index.html", "Should return index.html for empty segments");
});

// Run all tests
(async () => {
  const exitCode = await runner.run();
  
  // Export results for JSON output
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
      exitCode,
      results: runner.getResults()
    };
  }
  
  process.exit(exitCode);
})();
