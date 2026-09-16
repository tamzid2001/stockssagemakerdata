const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../../public/app.js'), 'utf8');
const start = source.indexOf('  const createSessionCoordinator =');
const end = source.indexOf('  const ensureSessionUser =', start);
const create = vm.runInNewContext(source.slice(start, end) + '\ncreateSessionCoordinator;', { Promise, queueMicrotask });
function fixture() {
  let restore, calls = 0;
  const auth = { currentUser: null, onAuthStateChanged(fn) { restore = fn; return () => {}; },
    async signInAnonymously() { calls++; auth.currentUser = { uid: 'guest', isAnonymous: true }; return { user: auth.currentUser }; } };
  return { auth, restored(user) { auth.currentUser = user; restore(user); }, calls: () => calls };
}
test('Terminal waits for persisted full account; never starts anonymous auth during restoration', async () => {
  const f=fixture(), ensure=create(f.auth, Promise.resolve()), waiting=ensure();
  await Promise.resolve(); assert.equal(f.calls(), 0);
  f.restored({ uid:'owner', isAnonymous:false });
  assert.equal((await waiting).uid, 'owner'); assert.equal(f.calls(), 0);
});
test('Concurrent guest panels create only one anonymous identity', async () => {
  const f=fixture(), ensure=create(f.auth, Promise.resolve());
  const a=ensure(), b=ensure(); await Promise.resolve(); f.restored(null);
  assert.equal((await a).uid, 'guest'); assert.equal((await b).uid, 'guest'); assert.equal(f.calls(), 1);
});
test('A full login supersedes the earlier guest without cached-user regression', async () => {
  const f=fixture(), ensure=create(f.auth, Promise.resolve()), a=ensure();
  await Promise.resolve(); f.restored(null); await a;
  f.auth.currentUser={ uid:'owner', isAnonymous:false };
  assert.equal((await ensure()).uid, 'owner'); assert.equal(f.calls(), 1);
});
test('Persistence failure fails closed instead of overwriting an existing account', async () => {
  const f=fixture(), ensure=create(f.auth, Promise.reject(new Error('storage blocked')));
  await assert.rejects(ensure(), /storage blocked/); assert.equal(f.calls(), 0);
});
test('Profile removes archived cloud/notification controls; navigation keeps Terminal and Shop', () => {
  const {JSDOM}=require('jsdom');
  const html=fs.readFileSync(require('node:path').join(__dirname,'../../pages/dashboard.html'),'utf8');
  const doc=new JSDOM(html).window.document;
  assert.equal(doc.querySelector('#profile .aws-integration-card, #profile .forecast-alert-settings-card, #profile .security-summary'),null);
  assert.ok(doc.querySelector('.header .nav-links a[href="/shop"]'));
  assert.equal(doc.querySelector('.header .nav-links a[href="/forecasting"]').textContent,'Terminal');
  assert.match(source.slice(source.indexOf('  const normalizeTopNavigation'),source.indexOf('  const normalizeFooterSocialLinks')),/nav_shop/);
});
