// Deterministic derivatives of the supplied Quantura artwork; no generated/redesigned logo.
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import sharp from "sharp";

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const source = path.join(site, "brand/quantura-source.png");
const transparent = { r: 0, g: 0, b: 0, alpha: 0 };
const png = size => sharp(source).resize(size, size, { fit: "contain", background: transparent }).png({ compressionLevel: 9 }).toBuffer();
const write = async (relative, bytes) => {
  const dest = path.join(site, relative);
  await fs.mkdir(path.dirname(dest), { recursive: true });
  await fs.writeFile(dest, bytes);
};
const icon = await png(192);
const data = `data:image/png;base64,${icon.toString("base64")}`;
const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="192" height="192" viewBox="0 0 192 192" role="img" aria-label="Quantura"><image width="192" height="192" href="${data}"/></svg>\n`;
for (const relative of ["public/favicon.svg", "public/assets/quantura-icon.svg", "../favicon/favicon.svg"]) await write(relative, svg);
for (const [name, size] of [["favicon-96x96.png", 96], ["apple-touch-icon.png", 180], ["web-app-manifest-192x192.png", 192], ["web-app-manifest-512x512.png", 512]]) {
  const bytes = await png(size);
  await write(`public/${name}`, bytes);
  await write(`../favicon/${name}`, bytes);
}
// Separate maskable icon keeps the full mark inside the platform's safe circle.
const inset = await png(300);
await write("public/maskable-icon-512.png", await sharp({ create: { width: 512, height: 512, channels: 4, background: "#0b1526" } }).composite([{ input: inset, gravity: "centre" }]).png().toBuffer());
for (const name of ["quantura-brand-icon.png", "logo.png"]) await write(`public/assets/${name}`, await png(256));
for (const [name, color] of [["quantura-logo.svg", "#102033"], ["quantura-logo-dark.svg", "#edf3fb"]]) {
  await write(`public/assets/${name}`, `<svg xmlns="http://www.w3.org/2000/svg" width="600" height="160" viewBox="0 0 600 160" role="img" aria-label="Quantura"><image width="160" height="160" href="${data}"/><text x="170" y="108" fill="${color}" font-family="Arial,Helvetica,sans-serif" font-size="86" font-weight="700">Quantura</text></svg>\n`);
  await write(`../favicon/${name}`, await fs.readFile(path.join(site, `public/assets/${name}`)));
}
// Multi-resolution ICO: PNG payloads are supported by current browser/OS icon readers.
const icons = await Promise.all([16, 32, 48, 256].map(png));
const header = Buffer.alloc(6 + icons.length * 16);
header.writeUInt16LE(1, 2); header.writeUInt16LE(icons.length, 4);
let offset = header.length;
icons.forEach((bytes, index) => {
  const size = [16, 32, 48, 256][index]; const base = 6 + index * 16;
  header[base] = size % 256; header[base + 1] = size % 256;
  header.writeUInt16LE(1, base + 4); header.writeUInt16LE(32, base + 6);
  header.writeUInt32LE(bytes.length, base + 8); header.writeUInt32LE(offset, base + 12);
  offset += bytes.length;
});
for (const relative of ["public/favicon.ico", "../favicon/favicon.ico"]) await write(relative, Buffer.concat([header, ...icons]));
await write("public/assets/quantura-social.png", await sharp(source).resize(520, 520).extend({ top: 55, bottom: 55, left: 340, right: 340, background: "#0b1526" }).flatten({ background: "#0b1526" }).png().toBuffer());
console.log("Quantura brand assets generated from the supplied source artwork.");
