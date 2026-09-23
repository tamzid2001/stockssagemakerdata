const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const { buildTerminalProfilePanel, extractDashboardPanel } = require('../../functions_ssr/scripts/sync-templates.js');

const root = path.resolve(__dirname, '../..');
const read = (name) => fs.readFileSync(path.join(root, name), 'utf8');

test('archived dashboard account controls render once inside Terminal Profile', () => {
  const source = read('pages/forecasting.html');
  const output = read('functions_ssr/templates/forecasting.html');
  const dashboard = read('pages/dashboard.html');
  assert.match(source, /<!-- TERMINAL_PROFILE_PANEL -->/);
  assert.equal(output.includes('<!-- TERMINAL_PROFILE_PANEL -->'), false);
  assert.equal(buildTerminalProfilePanel(dashboard).includes('Kanban board'), false);
  assert.match(extractDashboardPanel(dashboard, 'orders'), /id="user-forecasts"/);

  const { document } = new JSDOM(output).window;
  const profile = document.querySelector('[data-panel-router="terminal"] [data-panel="profile"]');
  assert.ok(profile);
  for (const id of ['email-auth-form', 'user-email', 'user-forecasts', 'workspace-select', 'api-key-form']) {
    assert.ok(profile.querySelector(`#${id}`), id);
  }
  const ids = [...document.querySelectorAll('[id]')].map((el) => el.id);
  assert.equal(new Set(ids).size, ids.length, 'source and imported controls must not duplicate IDs');
  assert.equal(profile.querySelector('[id="productivity"]'), null);
  assert.equal(profile.querySelector('[id="productivity-board"]'), null);
  assert.equal(profile.querySelector('[id="tasks-calendar"]'), null);
});

test('Terminal navigation owns Profile and sign-in/out; mobile navigation mirrors them', () => {
  const { document } = new JSDOM(read('pages/forecasting.html')).window;
  const labels = [...document.querySelectorAll('.sidebar-nav .sidebar-link')].map((link) => link.textContent.trim());
  assert.deepEqual(labels, ['Forecast', 'Download', 'Screener', 'Profile', 'Sign in']);
  assert.equal(document.querySelector('#dashboard-auth-link')?.textContent.trim(), 'Sign in');
  assert.equal(document.querySelector('#header-auth'), null);
  const app = read('public/app.js');
  assert.match(app, /terminal: \["forecast", "download", "screener", "profile"\]/);
  assert.match(app, /data-auth-nav="true"/);
  assert.match(app, /section\.hidden = accountAuthed/);
  assert.match(app, /section\.hidden = !accountAuthed/);
  assert.match(app, /if \(ui\.productivityBoard \|\| ui\.tasksCalendar\) startWorkspaceTasks/);
});

test('legacy dashboards and productivity redirect to Profile without removing their backend data', () => {
  const vercel = JSON.parse(read('vercel.json'));
  const firebase = JSON.parse(read('firebase.json'));
  for (const route of ['/dashboard', '/account', '/productivity', '/collaboration', '/admin']) {
    assert.equal(vercel.redirects.find((entry) => entry.source === route)?.destination, '/forecasting?panel=profile', route);
    assert.equal(firebase.hosting.redirects.find((entry) => entry.source === route)?.destination, '/forecasting?panel=profile', route);
  }
  assert.doesNotMatch(read('pages/index.html'), /Kanban board|Open productivity/);
  assert.match(read('functions_ssr/index.js'), /res\.redirect\(302, "\/forecasting\?panel=profile"\)/);
});
