import { build } from "esbuild";
import { readFile, stat } from "node:fs/promises";
import { resolve } from "node:path";

const root = resolve(new URL("..", import.meta.url).pathname);
await build({ entryPoints: [resolve(root, "public/app.js")], outfile: resolve(root, "public/app.min.js"),
  minify: true, target: ["es2020"], sourcemap: false, legalComments: "none" });
// Resolve the local design-system import at build time instead of a second
// render-blocking CSS request. External font URLs remain external.
const css = (await readFile(resolve(root, "public/styles.css"), "utf8"))
  .replace(/@import url\('\/professional.css[^']*'\);/, '@import "./professional.css";');
await build({ stdin: { contents: css, loader: "css", resolveDir: resolve(root, "public") },
  outfile: resolve(root, "public/styles.min.css"), bundle: true, minify: true,
  external: ["https://*", "/assets/*"], target: ["chrome109", "safari16"], legalComments: "none" });
for (const file of ["app.js", "app.min.js", "styles.css", "professional.css", "styles.min.css"]) {
  console.info(`${file}: ${(await stat(resolve(root, "public", file))).size} bytes`);
}
