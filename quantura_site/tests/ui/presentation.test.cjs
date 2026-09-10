const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const root = path.resolve(__dirname, '../..');
const source = file => fs.readFileSync(path.join(root, 'public', file), 'utf8');
const page = file => fs.readFileSync(path.join(root, 'pages', file), 'utf8');
const tick = () => new Promise(resolve => setTimeout(resolve, 0));

function dom(html = '<div id="chart"></div>') {
  const d = new JSDOM(html, { url:'https://quantura.studio/forecasting', runScripts:'outside-only', pretendToBeVisual:true });
  d.window.matchMedia = () => ({matches:false});
  return d;
}

test('theme initializes before layout, honors stored preference and handles blocked storage', () => {
  for (const theme of ['light','dark']) {
    const d=dom(); d.window.localStorage.setItem('quantura_theme',theme); d.window.eval(source('theme-init.js'));
    assert.equal(d.window.document.documentElement.dataset.theme,theme); d.window.close();
  }
  const d=dom(); Object.defineProperty(d.window,'localStorage',{get(){throw new Error('blocked');}});
  d.window.matchMedia=()=>({matches:true}); d.window.eval(source('theme-init.js'));
  assert.equal(d.window.document.documentElement.dataset.theme,'dark'); d.window.close();
});

function contrast(a,b) {
  const light = hex => { const c=hex.slice(1).match(/../g).map(x=>parseInt(x,16)/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4); return c[0]*.2126+c[1]*.7152+c[2]*.0722; };
  const x=light(a),y=light(b); return (Math.max(x,y)+.05)/(Math.min(x,y)+.05);
}
test('shared light/dark surface and feedback tokens meet WCAG text contrast', () => {
  const css=source('professional.css');
  const blocks=[css.match(/:root \{([\s\S]*?)\n\}/)[1],css.match(/:root\[data-theme="dark"\] \{([\s\S]*?)\n\}/)[1]];
  const base={};
  for (const block of blocks) {
    for(const [,k,v] of block.matchAll(/--([\w-]+): (#[0-9a-f]{6});/g)) base[k]=v;
    for (const [fg,bg] of [['foreground','background'],['foreground','card'],['muted-foreground','card'],['muted-foreground','secondary'],['primary-foreground','primary'],['warning','warning-surface'],['success','success-surface'],['destructive','destructive-surface']]) assert.ok(contrast(base[fg],base[bg])>=4.5,`${fg}/${bg}`);
  }
  assert.match(css,/\.toast[^{}]*\{[^}]*background: var\(--popover\)[^}]*color: var\(--popover-foreground\)/);
  assert.match(css,/select option, select optgroup \{ background: var\(--popover\)/);
  assert.match(source('app.js'),/quantura:theme-change/);
});

test('public pages load early theme, defer heavy libraries, and retain native mounts', () => {
  for(const file of ['index.html','forecasting.html','screener.html','dashboard.html','forecasts.html']) {
    const html=page(file);
    assert.ok(html.indexOf('/theme-init.js')<html.indexOf('/styles.css'));
    assert.ok(html.indexOf('/ui-runtime.js')<html.indexOf('/app.js'));
    assert.doesNotMatch(html,/<script[^>]+(?:cdn.plot.ly|quantura-rnw.js)/);
    assert.doesNotMatch(html,/brier/i);
  }
  assert.match(page('forecasting.html'),/data-quantura-rn-root/);
  assert.match(source('app.js'),/if \(isNativeApp\(\)\) \{\s*window.QuanturaUI\?\.loadNative/);
  assert.doesNotMatch(source('forecasts.js'),/brier/i);
  assert.match(page('index.html'),/data-lazy-video=/);
  assert.doesNotMatch(page('index.html'),/\sautoplay\s/);
});

test('Plotly loads once on demand, themes charts, and never loads for idle UI', async () => {
  const d=dom(); const w=d.window; w.eval(source('ui-runtime.js'));
  assert.equal(w.document.scripts.length,0);
  const a=w.QuanturaUI.loadPlotly(),b=w.QuanturaUI.loadPlotly();
  assert.equal(w.document.scripts.length,1);
  const calls=[];
  w.Plotly={react:async()=>calls.push('react'),newPlot:async()=>calls.push('newPlot'),relayout:async(_el,theme)=>calls.push(theme)};
  w.document.scripts[0].dispatchEvent(new w.Event('load'));
  const [pa,pb]=await Promise.all([a,b]); assert.equal(pa,pb);
  await pa.react(w.document.getElementById('chart'),[],{},{});
  assert.equal(calls[0],'react'); assert.ok('font.color' in calls[1]);
  d.window.close();
});

test('offscreen charts defer, replace stale renders, and show recoverable failures', async () => {
  const d=dom(); const w=d.window; let intersect;
  w.IntersectionObserver=class {constructor(cb){intersect=cb;} observe(){} unobserve(){}};
  w.eval(source('ui-runtime.js')); const host=w.document.getElementById('chart'); let value=0;
  assert.equal(w.QuanturaUI.deferChart(host,()=>value=1),true);
  w.QuanturaUI.deferChart(host,()=>value=2); assert.equal(value,0);
  intersect([{target:host,isIntersecting:true}]); await tick(); assert.equal(value,2);
  w.QuanturaUI.whenVisible(host,()=>{throw new Error('fixture');}); intersect([{target:host,isIntersecting:true}]); await tick();
  assert.equal(host.querySelector('button').textContent,'Retry chart'); d.window.close();
});

test('Screener uses semantic tokens and contained responsive layouts', () => {
  const css=source('screener.css');
  assert.match(css,/--qs-panel: var\(--card\)/); assert.match(css,/--qs-muted: var\(--muted-foreground\)/);
  assert.match(css,/minmax\(min\(100%, 145px\), 1fr\)/);
  assert.match(css,/\.qs-table-wrap \{[^}]*overflow: auto/);
  assert.match(css,/grid-template-columns: minmax\(0, .8fr\) minmax\(0, 1.2fr\)/);
  const html=page('screener.html'); assert.match(html,/<details[^>]+open/);
  assert.match(source('screener-panel-lazy.js'),/if \(!requested\(\)\) return Promise.resolve/);
  assert.match(source('screener-workspace-loader.js'),/copy.querySelector\(".qs-main"\)/);
  assert.doesNotMatch(page('forecasting.html'),/id="qs-filters"/);
});

test('market selector debounces, preserves source metadata, and supports arrow/Escape navigation', async () => {
  const d=dom(page('forecasting.html')); const w=d.window; let requests=0;
  w.fetch=async()=>{requests++; return {ok:true,json:async()=>({count:1,groups:{yahoo:[{resource_id:'fixture',symbol:'TEST',name:'Test instrument with a long display name',asset_class:'equity',source:'yahoo',exchange:'TEST EXCHANGE',forecast_available:true}]}})};};
  w.eval(source('market-search.js'));
  const query=w.document.getElementById('market-search-query');
  for(const value of ['T','TE','TEST']) {query.value=value;query.dispatchEvent(new w.Event('input'));}
  await new Promise(r=>setTimeout(r,350)); assert.equal(requests,1);
  const result=w.document.getElementById('market-search-results'); assert.match(result.textContent,/Yahoo Finance/); assert.match(result.textContent,/TEST EXCHANGE/);
  query.dispatchEvent(new w.KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true})); assert.equal(w.document.activeElement.dataset.marketAction,'forecast');
  w.document.activeElement.dispatchEvent(new w.KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true})); assert.equal(w.document.activeElement.dataset.marketAction,'history');
  w.document.activeElement.dispatchEvent(new w.KeyboardEvent('keydown',{key:'Escape',bubbles:true})); assert.equal(w.document.activeElement,query); assert.equal(result.hidden,true);
  w.document.getElementById('market-search-form').dispatchEvent(new w.Event('submit',{cancelable:true})); await tick(); assert.equal(requests,1,'repeat search uses bounded short-lived cache');
  d.window.close();
});

test('market selector does not render late responses over a newer query', async () => {
  const d=dom(page('forecasting.html'));const w=d.window;const pending=[];
  w.fetch=()=>new Promise(resolve=>pending.push(resolve));w.eval(source('market-search.js'));
  const q=w.document.getElementById('market-search-query'),form=w.document.getElementById('market-search-form');
  q.value='OLD';form.dispatchEvent(new w.Event('submit'));q.value='NEW';form.dispatchEvent(new w.Event('submit'));
  pending[1]({ok:true,json:async()=>({count:0,groups:{}})});await tick();
  pending[0]({ok:true,json:async()=>({count:999,groups:{}})});await tick();
  assert.match(w.document.getElementById('market-search-status').textContent,/^0 results/);d.window.close();
});
