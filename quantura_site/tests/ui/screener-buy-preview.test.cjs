const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const root=path.resolve(__dirname,'../..'),tick=()=>new Promise(r=>setTimeout(r,5));
function setup(items){
  const d=new JSDOM(fs.readFileSync(path.join(root,'pages/index.html'),'utf8'),{url:'https://quantura.studio/',runScripts:'outside-only'});
  d.window.fetch=async()=>({ok:true,json:async()=>({items,total:items.length,scanId:'scan-1'})});
  d.window.eval(fs.readFileSync(path.join(root,'public/screener-buy-preview.js'),'utf8'));return d;
}
test('homepage never relabels a legacy Buy as an approved daily P99 signal',async()=>{
  const d=setup([{ticker:'OLD',signal:'buy',current_signal:{value:'buy'},buy_price_target:120}]);await tick();
  assert.equal(d.window.document.querySelectorAll('[data-buy-cards] article').length,0);
  assert.match(d.window.document.querySelector('[data-buy-status]').textContent,/No Buy signals/);d.window.close();
});
test('homepage links exact published forecast and safely renders final P99 target',async()=>{
  const d=setup([{ticker:'TEST',company_name:'<img src=x>',actual_price:131,signal:'buy',buy_price_target:136,current_signal:{rule:'daily_close_above_first_p99_v2',input_date:'2026-09-18',p99:130}}]);await tick();
  const card=d.window.document.querySelector('[data-buy-cards] article');assert.ok(card);assert.equal(card.querySelector('img'),null);
  assert.match(card.querySelector('a').href,/screenerTicker=TEST&screenerScan=scan-1/);assert.match(card.textContent,/136/);d.window.close();
});
