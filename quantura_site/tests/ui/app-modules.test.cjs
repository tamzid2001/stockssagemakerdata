const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');

test('optional legacy filters and non-English copy load as separate modules', async () => {
  const app = fs.readFileSync(path.join(root, 'public/app.js'), 'utf8');
  assert.match(app, /import\("\/screener-legacy-filters\.js\?v=/);
  assert.match(app, /if \(!groups\.length\) return;[\s\S]*?import\("\/screener-legacy-filters/);
  assert.match(app, /if \(resolved !== "en" && !UI_I18N_TEXT\[resolved\]\) void loadUiI18nCatalog\(\)/);
  assert.match(app, /if \(!document\.getElementById\("unsplash-grid"\)\) return;[\s\S]*?import\("\/unsplash-gallery/);
  assert.match(app, /seedDefaultAIAgents[\s\S]*?import\("\/admin-seed-data/);
  const url = new URL(`file://${path.join(root, 'public/ui-i18n-catalog.js')}`);
  const { packs } = await import(url.href);
  assert.deepEqual(Object.keys(packs).sort(), ['ar', 'bn', 'de', 'es', 'fr']);
  assert.ok(packs.es.sign_in && packs.ar.sign_in && packs.bn.sign_in);
});
