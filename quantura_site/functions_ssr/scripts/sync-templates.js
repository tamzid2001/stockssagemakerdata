const fs = require("fs/promises");
const path = require("path");

const sourceRoot = path.join(__dirname, "..", "..", "pages");
const destRoot = path.join(__dirname, "..", "templates");

const walk = async (dir) => {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const out = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...(await walk(full)));
    } else {
      out.push(full);
    }
  }
  return out;
};

const main = async () => {
  await fs.rm(destRoot, { recursive: true, force: true });
  await fs.mkdir(destRoot, { recursive: true });

  const files = await walk(sourceRoot);
  let copied = 0;
  for (const file of files) {
    if (!file.endsWith(".html")) continue;
    const rel = path.relative(sourceRoot, file);
    const dest = path.join(destRoot, rel);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    await fs.copyFile(file, dest);
    copied += 1;
  }

  // eslint-disable-next-line no-console
  console.log(`Quantura SSR: synced ${copied} HTML templates into ${destRoot}`);
};

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exitCode = 1;
});
