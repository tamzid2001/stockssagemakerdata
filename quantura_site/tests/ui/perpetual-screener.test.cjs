const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM}=require('jsdom');
const root=path.resolve(__dirname,'../..');
test('perpetual screener displays six ensemble quantiles and enables real filter controls',async()=>{
  const dom=new JSDOM(fs.readFileSync(path.join(root,'pages/screener.html'),'utf8'),{url:'https://quantura.studio/screener?source=kalshi_perps',runScripts:'outside-only'});
  const w=dom.window,doc=w.document;
  const row={ticker:'KXBTCPERP',company_name:'Bitcoin perpetual',actual_price:100,p01:80,p25:90,p50:100,p75:110,p90:120,p99:130,forecast_view_url:'/forecasting?panel=forecast&screenerTicker=KXBTCPERP&screenerScan=perps-real',forecast_action:'view'};
  w.fetch=async()=>({ok:true,json:async()=>({items:[row],dataSource:'kalshi_perps',total:1,universeCount:1,manifest:{forecasts_published:1}})});
  w.eval(fs.readFileSync(path.join(root,'public/screener.js'),'utf8'));
  await new Promise(r=>setTimeout(r,20));
  const body=doc.querySelector('#qs-table-body');
  for(const q of ['P01','P25','P50','P75','P90','P99'])assert.equal(body.querySelector(`[data-label="${q}"]`).hidden,false);
  assert.equal(body.querySelector('[data-label="P10"]').hidden,true);
  assert.equal(doc.getElementById('qs-statistic').disabled,false);
  assert.equal(doc.getElementById('qs-add-rule').disabled,false);
  assert.match(body.querySelector('.qs-view-forecast').href,/screenerScan=perps-real/);
  doc.getElementById('qs-add-rule').click();
  assert.deepEqual(Array.from(doc.querySelectorAll('[data-rule="quantile"] option'),e=>e.value),['p01','p25','p50','p75','p90','p99']);
  // Reproduce the original CSS regression: desktop cells may not be collapsed.
  const css=fs.readFileSync(path.join(root,'public/screener.css'),'utf8');
  assert.doesNotMatch(css,/\[data-source="kalshi_perps"\][^{]*nth-child\(n\+3\)[^{]*\{\s*display:\s*none/);
  dom.window.close();
});
