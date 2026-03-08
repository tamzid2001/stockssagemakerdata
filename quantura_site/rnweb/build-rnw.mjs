import { build } from "esbuild";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const rootDir = resolve(new URL("..", import.meta.url).pathname);
const entryFile = resolve(rootDir, "rnweb/index.web.tsx");
const outFile = resolve(rootDir, "public/assets/rnweb/quantura-rnw.js");

await mkdir(dirname(outFile), { recursive: true });

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
  jsx: "automatic",
  define: {
    "process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV || "production"),
    __DEV__: "false",
  },
  alias: {
    "react-native": "react-native-web",
  },
});

console.info("[RNW][build] wrote", outFile);
