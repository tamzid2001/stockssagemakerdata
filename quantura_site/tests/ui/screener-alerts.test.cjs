const test=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const {JSDOM}=require('jsdom');
const root=path.resolve(__dirname,'../..');const tick=()=>new Promise(r=>setTimeout(r,0));
function setup(payload={items:[],total:0,page:1,pageCount:1}){const d=new JSDOM(fs.readFileSync(path.join(root,'pages/screener.html'),'utf8'),{url:'https://quantura.studio/screener',runScripts:'outside-only'});d.window.fetch=async()=>({ok:true,json:async()=>payload});const calls=[];d.window.QuanturaScreenerAccount={request:async(p,o)=>{calls.push([p,o]);return {data:[],items:[]};}};d.window.eval(fs.readFileSync(path.join(root,'public/screener.js'),'utf8'));return {d,w:d.window,calls};}
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
test('saved alerts and the screener-only inbox expose icons and evaluation status',async()=>{
  const {d,w,calls}=setup();
  w.QuanturaScreenerAccount.request=async(p)=>{calls.push([p]);return p.includes('notifications')?{items:[]}:{data:[{id:'b'.repeat(24),name:'Close signal',email:true,filters:{positions:[]}}],meta:{maximum:10,email_configured:true,last_evaluation:{date:'2026-09-18',matched_rows:0,email_status:'no_matches'}}};};
  w.document.getElementById('qs-load-alerts').click();await tick();await tick();
  assert.ok(w.document.querySelector('#saved-alerts summary .iconoir-bell-notification'));
  assert.ok(w.document.querySelector('#qs-saved-list .qs-alert-icon'));
  assert.match(w.document.getElementById('qs-alert-summary').textContent,/1 of 10 active/);
  assert.match(w.document.getElementById('qs-alert-inbox').textContent,/No matches on/);
  assert.ok(calls.some(([path])=>path==='/api/notifications/items?category=screener&limit=20'));
  d.window.close();
});
test('shared screener shows seven quantiles, no earnings or obsolete special-signal UI',()=>{
  const {d,w}=setup();const html=w.document.getElementById('qs-filters').textContent;assert.doesNotMatch(html,/Earnings|Special P10|Model bias/);
  assert.ok(w.document.getElementById('qs-statistic'));assert.ok(w.document.getElementById('qs-signal'));d.window.close();
});
test('daily P99 Buy and final target are visible; no Sell or Neutral signal options',async()=>{
  const row={ticker:'PLTR',actual_price:100,p99:130,cutoff_p99_signal:{value:'buy',price:131,p99:130,quote_timestamp:'2026-09-17',forecast_date:'2026-09-21'},current_signal:{value:'buy',price:131,p99:130,price_target:136,target_date:'2026-09-29',forecast_date:'2026-09-21'}};
  const {d,w,calls}=setup({items:[row],total:1,page:1,pageCount:1});await tick();
  assert.match(w.document.getElementById('qs-table-body').textContent,/Buy · close above P99/);
  assert.match(w.document.getElementById('qs-table-body').textContent,/Target 136/);
  assert.deepEqual([...w.document.getElementById('qs-signal').options].map(o=>o.value),['all','buy']);
  w.document.getElementById('qs-signal').value='buy';w.document.getElementById('qs-signal').dispatchEvent(new w.Event('change',{bubbles:true}));
  w.document.getElementById('qs-alert-name').value='P99 cutoff buy';w.document.getElementById('qs-save-alert').click();await tick();await tick();
  assert.equal(calls.find(c=>c[1]?.method==='POST')[1].body.filters.signal,'buy');
  for(const value of ['above-p99','below-p99','above-p01','below-p01'])assert.ok(w.document.querySelector(`[name="position"][value="${value}"]`));
  d.window.close();
});
test('mobile result cards keep core ticker metrics visible and expand one row at a time',async()=>{
  const row={ticker:'GOLD',company_name:'Gold',actual_price:4381.3,actual_price_timestamp:'2026-09-20T12:00:00Z',p01:70,p10:80,p25:90,p50:100,p75:110,p90:120,p99:130,current_signal:{value:'buy',forecast_date:'2026-09-21'},quantile_position:'below_p10',forecast_view_url:'/forecasting?ticker=GOLD',forecast_action:'view'};
  const {d,w}=setup({items:[row],total:1,universeCount:1,page:1,pageCount:1,generatedAt:'2026-09-20T12:00:00Z',manifest:{successfully_processed:1,failed:0,coverage_percentage:100}});await tick();
  const result=w.document.querySelector('#qs-table-body tr'),toggle=result.querySelector('[data-row-toggle]');
  assert.equal(result.querySelectorAll('.qs-mobile-core').length,5);assert.equal(result.querySelectorAll('.qs-mobile-detail').length,9);
  assert.equal(toggle.getAttribute('aria-expanded'),'false');toggle.click();assert.equal(result.classList.contains('is-expanded'),true);assert.equal(toggle.getAttribute('aria-expanded'),'true');assert.match(toggle.textContent,/Fewer metrics/);
  toggle.click();assert.equal(result.classList.contains('is-expanded'),false);assert.match(toggle.textContent,/More metrics/);d.window.close();
});
test('saved scan day navigation retains filters and CSV date; missing days are not requested',async()=>{
  const d=new JSDOM(fs.readFileSync(path.join(root,'pages/screener.html'),'utf8'),{url:'https://quantura.studio/screener',runScripts:'outside-only'});
  const w=d.window,requests=[];
  w.fetch=async url=>{requests.push(String(url));const selected=new URL(String(url),'https://quantura.studio').searchParams.get('date')||'2026-09-23';return {ok:true,json:async()=>({items:[{ticker:'PLTR'}],total:1,universeCount:1,page:1,pageCount:1,selectedDate:selected,scanDate:selected,generatedAt:`${selected}T22:00:00Z`,availableDates:['2026-09-23','2026-09-22','2026-09-19']})};};
  w.eval(fs.readFileSync(path.join(root,'public/screener.js'),'utf8'));await tick();
  w.document.getElementById('qs-search').value='PLTR';
  w.document.getElementById('qs-date-previous').click();await tick();
  assert.equal(w.document.getElementById('qs-date').value,'2026-09-22');
  assert.equal(w.document.getElementById('qs-last-buy-heading').textContent,'Buy in scan');
  assert.match(requests.at(-1),/date=2026-09-22/);assert.match(requests.at(-1),/search=PLTR/);
  assert.match(w.document.getElementById('qs-export').href,/date=2026-09-22/);
  w.document.getElementById('qs-date').value='2026-09-21';w.document.getElementById('qs-date').dispatchEvent(new w.Event('change'));
  assert.equal(w.document.getElementById('qs-date').value,'2026-09-22');
  assert.match(w.document.getElementById('qs-date-availability').textContent,/No validated scan/);
  assert.equal(requests.length,2);
  w.document.getElementById('qs-date-next').click();await tick();
  assert.equal(w.document.getElementById('qs-date').value,'2026-09-23');
  assert.equal(w.document.getElementById('qs-last-buy-heading').textContent,'Last Buy');
  d.window.close();
});
