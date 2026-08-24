import { build } from "esbuild";
import { resolve } from "node:path";

const rootDir = resolve(new URL("..", import.meta.url).pathname);
const entryFile = resolve(rootDir, "src/vercel-observability.ts");
const outFile = resolve(rootDir, "public/vercel-observability.js");

await build({
  entryPoints: [entryFile],
  outfile: outFile,
  bundle: true,
  format: "iife",
  platform: "browser",
  target: ["es2020"],
  sourcemap: false,
  minify: true,
  logLevel: "info",
  define: {
    "process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV || "production"),
    __VERCEL_OBSERVABILITY_CLIENT_CONFIG__: JSON.stringify(
      process.env.VERCEL_OBSERVABILITY_CLIENT_CONFIG || ""
    ),
  },
});

console.info("[Vercel observability][build] wrote", outFile);
