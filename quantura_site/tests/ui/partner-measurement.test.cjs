const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const source = fs.readFileSync(path.join(__dirname, '../../public/partner-measurement.js'), 'utf8');
function page(url = 'https://quantura.studio/contact', consent = '', gpc = false) {
  const dom = new JSDOM('<!doctype html><head></head><body></body>', { url, runScripts: 'outside-only' });
  if (consent) dom.window.localStorage.setItem('quantura_cookie_consent', consent);
  Object.defineProperty(dom.window.navigator, 'globalPrivacyControl', { value: gpc });
  dom.window.eval(source);
  dom.window.document.dispatchEvent(new dom.window.Event('DOMContentLoaded'));
  return dom;
}
test('No pixel, Zift, or attribution capture before consent, under GPC, or on private pages', () => {
  for (const args of [[], [undefined, 'declined'], [undefined, 'accepted', true],
    ['https://quantura.studio/forecasting?gameForecastId=private', 'accepted']]) {
    const dom = page(...args); const w = dom.window;
    assert.equal(w.document.querySelectorAll('script').length, 0);
    assert.equal(w.QuanturaAds.capture(), null);
    w.QuanturaAds.leadCreated('lead_test'); assert.equal(w.oaiq, undefined);
    dom.window.close();
  }
});
test('One initialization and deduped confirmed lead; raw attribution and sanitized URL', () => {
  const dom = page('https://quantura.studio/contact?oppref=opaque#form', 'accepted'); const w = dom.window;
  w.document.cookie = '__oppref=raw%2F+value'; w.document.cookie = '__obref=browser-reference';
  assert.deepEqual(JSON.parse(JSON.stringify(w.QuanturaAds.capture())), {
    consent: 'granted', sourceUrl: 'https://quantura.studio/contact', oppref: 'raw%2F+value', obref: 'browser-reference',
  });
  w.eval(source);
  w.document.dispatchEvent(new w.Event('quantura:consent-change'));
  w.QuanturaAds.leadCreated('lead_contact123'); w.QuanturaAds.leadCreated('lead_contact123');
  const events = w.oaiq.q.map(a => Array.from(a));
  assert.equal(events.filter(a => a[0] === 'init').length, 1);
  assert.equal(events.filter(a => a[1] === 'page_viewed').length, 1);
  const lead = events.filter(a => a[1] === 'lead_created');
  assert.equal(lead.length, 1); assert.equal(lead[0][3].event_id, 'lead_contact123');
  assert.equal(lead[0][3].opt_out, true);
  assert.equal(w.document.querySelectorAll('#quantura-openai-pixel').length, 1);
  assert.equal(w.document.querySelectorAll('#quantura-aws-marketplace-analytics').length, 0);
  w.localStorage.setItem('quantura_cookie_consent', 'declined');
  w.document.dispatchEvent(new w.Event('quantura:consent-change'));
  assert.deepEqual(Array.from(w.oaiq.q.at(-1)), ['consent', false]);
  assert.equal(w.QuanturaAds.capture(), null);
  w.QuanturaAds.leadCreated('lead_after_revoke'); assert.equal(events.filter(a => a[1] === 'lead_created').length, 1);
  dom.window.close();
});
test('Zift loads asynchronously only after consent; transport errors stay isolated', () => {
  const dom = page('https://quantura.studio/', 'accepted'); const w = dom.window;
  const zift = w.document.querySelector('#quantura-aws-marketplace-analytics');
  assert.ok(zift); assert.equal(zift.async, true);
  w.oaiq = () => { throw new Error('measurement unavailable'); };
  assert.doesNotThrow(() => w.QuanturaAds.leadCreated('lead_test'));
  dom.window.close();
});
