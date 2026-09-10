const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const root = path.resolve(__dirname, '../..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');
const tick = () => new Promise(resolve => setTimeout(resolve, 0));

test('homepage uses supplied responsive brand assets, removes dock and labels mobile navigation', () => {
  const html = read('pages/index.html'), app = read('public/app.js');
  assert.doesNotMatch(html, /class="home-bottom-nav|id="home-bottom-nav|data-unsplash-gallery/);
  assert.match(html, /quantura-product-concept-640\.webp/);
  assert.match(html, /example values, not live market data/);
  assert.match(app, /<span>Q Forecast<\/span>/);
  assert.match(app, /headerAuth\.innerHTML = accountAuthed[\s\S]{0,160}<span>Dashboard<\/span>[\s\S]{0,100}<span>Sign in<\/span>/);
  assert.match(app, /DOMContentLoaded.*init/);
  assert.doesNotMatch(read('public/styles.css'), /a\[href="\/forecasting"\][^{]*\{\s*display: none/);
});

test('brand install icons have distinct maskable artwork and correct dimensions', async () => {
  const sharp = require('sharp');
  for (const [file, size] of [['favicon-96x96.png',96],['apple-touch-icon.png',180],['web-app-manifest-192x192.png',192],['web-app-manifest-512x512.png',512],['maskable-icon-512.png',512]]) {
    const meta = await sharp(path.join(root,'public',file)).metadata();
    assert.equal(meta.width,size); assert.equal(meta.height,size);
  }
  const icons=JSON.parse(read('public/site.webmanifest')).icons;
  assert.ok(icons.some(i=>i.purpose==='maskable' && i.src.includes('maskable-icon')));
  assert.notDeepEqual(fs.readFileSync(path.join(root,'public/maskable-icon-512.png')),fs.readFileSync(path.join(root,'public/web-app-manifest-512x512.png')));
});

test('every current blog post has tracked, hotlinked photography and visible attribution', async () => {
  const { photoFigure, imageUrl } = await import('../../scripts/blog-photo.mjs');
  const registry=JSON.parse(read('brand/blog-photos.json'));
  const manifest=JSON.parse(read('pages/blog/posts.manifest.json'));
  assert.equal(manifest.posts.length,78);
  for(const post of manifest.posts) {
    const photo=registry.photos[registry.posts[post.slug]];
    assert.ok(photo.download_tracked_at);
    const html=read(`pages/blog/posts/${post.slug}.html`);
    assert.match(html,/data-unsplash-photo=/); assert.match(html,/utm_source=quantura/);
    assert.match(html,/Illustrative photography/); assert.doesNotMatch(html,/hero-illustration\.svg/);
    assert.equal(html,read(`public/blog/posts/${post.slug}.html`));
    assert.ok(new URL(imageUrl(photo)).searchParams.has('ixid'));
  }
  assert.throws(()=>imageUrl({image_url:'https://untrusted.example/x'}));
  assert.throws(()=>photoFigure({download_tracked_at:null}));
});

test('Contentsquare requires consent, excludes private pages and obeys global privacy control', () => {
  for(const [route,consent,gpc,expected] of [['/',false,false,0],['/',true,false,1],['/forecasting',true,false,0],['/account',true,false,0],['/?private=value',true,false,0],['/',true,true,0]]) {
    const d=new JSDOM('',{url:`https://quantura.studio${route}`,runScripts:'outside-only'});
    if(consent)d.window.localStorage.setItem('quantura_cookie_consent','accepted');
    Object.defineProperty(d.window.navigator,'globalPrivacyControl',{value:gpc});
    d.window.eval(read('public/contentsquare.js'));
    assert.equal(d.window.document.querySelectorAll('script[data-quantura-contentsquare]').length,expected);
    if(expected) {
      d.window.document.dispatchEvent(new d.window.Event('quantura:consent-change'));
      assert.equal(d.window.document.scripts.length,1);
      d.window.localStorage.removeItem('quantura_cookie_consent');
      d.window.document.dispatchEvent(new d.window.Event('quantura:consent-change'));
      assert.equal(d.window._uxa.at(-1)[0],'optout');
    }
    d.window.close();
  }
});

test('support dialog requires sign in, safely renders text, filters URLs and clears conversation', async () => {
  const d=new JSDOM('<button id="help">Help</button>',{url:'https://quantura.studio/',runScripts:'outside-only'});
  const w=d.window; w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};
  w.HTMLDialogElement.prototype.close=function(){this.open=false;this.onclose?.();};
  let currentUser=null,calls=0;
  w.firebase={auth:()=>({currentUser,onAuthStateChanged:()=>{}})};
  w.AbortController=AbortController;
  w.fetch=async()=>{calls++;return {ok:true,json:async()=>({data:{answer:'<script>not executable</script>',references:[{url:'https://evil.example',title:'Bad'},{url:'https://quantura.studio/contact',title:'Contact'}]}})};};
  w.eval(read('public/support-chat.js')); w.QuanturaSupport.open(w.document.getElementById('help'));
  const form=w.document.querySelector('form'),input=w.document.querySelector('textarea');
  input.value='Where are CSVs?'; form.dispatchEvent(new w.Event('submit',{cancelable:true})); await tick();
  assert.equal(calls,0); assert.match(w.document.querySelector('[role=status]').textContent,/Sign in/);
  currentUser={isAnonymous:false,getIdToken:async()=> 'session-fixture'};
  form.dispatchEvent(new w.Event('submit',{cancelable:true})); await tick();
  assert.equal(calls,1); assert.equal(w.document.scripts.length,0);
  assert.equal(w.document.querySelectorAll('a[href="https://evil.example"]').length,0);
  assert.equal(w.localStorage.length,0);
  [...w.document.querySelectorAll('button')].find(b=>b.textContent==='Clear chat').click();
  assert.equal(w.document.querySelector('[role=log]').children.length,1);
  assert.equal(input.value,''); d.window.close();
});
