const test=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const {JSDOM}=require('jsdom');
const root=path.resolve(__dirname,'../..');const tick=()=>new Promise(r=>setTimeout(r,0));
function setup(){const d=new JSDOM(fs.readFileSync(path.join(root,'pages/screener.html'),'utf8'),{url:'https://quantura.studio/screener',runScripts:'outside-only'});d.window.fetch=async()=>({ok:true,json:async()=>({items:[],total:0,page:1,pageCount:1})});const calls=[];d.window.QuanturaScreenerAccount={request:async(p,o)=>{calls.push([p,o]);return {data:[],items:[]};}};d.window.eval(fs.readFileSync(path.join(root,'public/screener.js'),'utf8'));return {d,w:d.window,calls};}
test('saved filters preserve quantile AND rules and email is opt-in',async()=>{
  const {d,w,calls}=setup();await tick();w.document.getElementById('qs-add-rule').click();await tick();
  assert.equal(w.document.getElementById('qs-alert-email').checked,false);
  w.document.getElementById('qs-alert-name').value='Average opportunity';w.document.getElementById('qs-save-alert').click();await tick();await tick();
  const body=calls.find(c=>c[1]?.method==='POST')[1].body;assert.equal(body.email,false);assert.equal(body.filters.quantileRules[0].quantile,'p50');assert.equal(body.filters.quantileRules[0].percent,10);
  assert.match(w.document.getElementById('qs-alert-status').textContent,/Saved/);d.window.close();
});
test('guest errors are visible, saved markup escaped and remove uses own account API',async()=>{
  const {d,w,calls}=setup();await tick();w.QuanturaScreenerAccount.request=async()=>{throw new Error('Sign in to save filters');};
  w.document.getElementById('qs-load-alerts').click();await tick();assert.match(w.document.getElementById('qs-alert-status').textContent,/Sign in/);
  w.QuanturaScreenerAccount.request=async(p,o)=>{calls.push([p,o]);return p.includes('notifications')?{items:[]}:{data:[{id:'a'.repeat(24),name:'<img src=x>',email:false,filters:{positions:[],search:'TEST',bias:'all',earnings:'all',specialP10:false}}]};};
  w.document.getElementById('qs-load-alerts').click();await tick();await tick();assert.equal(w.document.querySelector('#qs-saved-list img'),null);
  w.document.querySelector('[data-apply-alert]').click();await tick();assert.equal(w.document.getElementById('qs-search').value,'TEST');
  w.document.getElementById('qs-alert-name').value='Resaved filter';w.document.getElementById('qs-save-alert').click();await tick();await tick();
  const resaved=calls.find(c=>c[1]?.method==='POST')[1].body.filters;assert.equal(resaved.search,'TEST');assert.equal('bias' in resaved,false);assert.equal('earnings' in resaved,false);assert.equal('specialP10' in resaved,false);
  w.document.querySelector('[data-remove-alert]').click();await tick();assert.ok(calls.some(c=>c[1]?.method==='DELETE'));d.window.close();
});
test('shared screener shows seven quantiles, no earnings or obsolete special-signal UI',()=>{
  const {d,w}=setup();const html=w.document.getElementById('qs-filters').textContent;assert.doesNotMatch(html,/Earnings|Special P10|Model bias/);
  assert.ok(w.document.getElementById('qs-statistic'));assert.ok(w.document.getElementById('qs-signal'));d.window.close();
});
