import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { transform } from "esbuild";
import { resolve } from "node:path";

const root = resolve(new URL("../..", import.meta.url).pathname);
// Tracked application files only: local editor duplicates do not define CI.
const files = execFileSync("git", ["ls-files", "quantura_site/public/*.js", "quantura_site/public/*.css", "quantura_site/functions_ssr/*.js"], { cwd: root, encoding: "utf8" }).trim().split("\n");
for (const file of ["quantura_site/public/theme-init.js", "quantura_site/public/ui-runtime.js", "quantura_site/public/support-chat.js", "quantura_site/public/contentsquare.js"]) if (!files.includes(file)) files.push(file);
let count = 0;
for (const file of files) {
  if (/assets\/|\.min\./.test(file)) continue;
  if (file.endsWith(".js")) execFileSync(process.execPath, ["--check", resolve(root, file)], { stdio: "pipe" });
  if (file.endsWith(".css")) {
    const result = await transform(await readFile(resolve(root, file), "utf8"), { loader: "css", logLevel: "silent" });
    if (result.warnings.length) throw new Error(`${file}: ${result.warnings.map(w => w.text).join(", ")}`);
  }
  count++;
}
console.info(`JavaScript syntax and CSS parser checks passed: ${count} files.`);
