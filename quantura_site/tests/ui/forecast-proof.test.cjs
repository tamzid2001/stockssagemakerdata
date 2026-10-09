const {JSDOM}=require('jsdom');
const {readFileSync}=require('node:fs');
const test=require('node:test');
const assert=require('node:assert/strict');
const source=readFileSync('public/forecast-proof.js','utf8');
const setup=()=>{const dom=new JSDOM('<section id="host"></section>',{runScripts:'outside-only'});dom.window.eval(source);return {w:dom.window,host:dom.window.document.getElementById('host')};};
const tick=()=>new Promise(r=>setImmediate(r));
test('receipts make no automatic requests and preserve exact proof download bytes',async()=>{
  const {w,host}=setup();const calls=[],bytes='{"nonce":"private","output":[100]}';let blob;
  w.URL.createObjectURL=value=>{blob=value;return 'blob:proof';};w.URL.revokeObjectURL=()=>{};w.HTMLAnchorElement.prototype.click=function(){};
  w.QuanturaForecastProof.attach(host,{job:{forecast_id:'f',provenance:{status:'stamped',receipt:{timestamp:'2026-10-09T15:00:00Z'}}},request:async(path)=>{calls.push(path);return {data:{content_matches:true,stamp_found:true,manifest_utf8:bytes}};}});
  assert.equal(calls.length,0);assert.match(host.textContent,/Confirms content and time/);
  host.querySelector('button').click();await tick();assert.match(host.textContent,/Content matches/);
  host.querySelectorAll('button')[1].click();await tick();
  assert.match(calls[1],/format=envelope/);assert.ok(blob instanceof w.Blob);
  const reader=new w.FileReader();const read=new Promise(r=>reader.onload=()=>r(reader.result));reader.readAsText(blob);assert.equal(await read,bytes);
});
test('late receipt responses do not change another forecast; public screeners have no stamping action',async()=>{
  const {w,host}=setup();let resolve;const pending=new Promise(r=>resolve=r);
  w.QuanturaForecastProof.attach(host,{job:{forecast_id:'old'},canStamp:true,request:()=>pending});
  host.querySelector('button').click();
  w.QuanturaForecastProof.attach(host,{job:{forecast_id:'new'},canStamp:true,request:()=>pending});
  resolve({data:{status:'stamped',receipt:{timestamp:'2026-10-09T15:00Z'}}});await tick();
  assert.equal(host.querySelector('button').textContent,'Create receipt');
  w.QuanturaForecastProof.attach(host,{job:{forecast_id:'public',published_screener:{}},canStamp:true,request:()=>pending});assert.equal(host.hidden,true);
});
