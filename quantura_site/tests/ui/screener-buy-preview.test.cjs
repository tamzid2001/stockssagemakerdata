const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const root=path.resolve(__dirname,'../..'),tick=()=>new Promise(r=>setTimeout(r,5));
function setup(items){
  const d=new JSDOM(fs.readFileSync(path.join(root,'pages/index.html'),'utf8'),{url:'https://quantura.studio/',runScripts:'outside-only'});
  d.window.fetch=async url=>{d.window.requestedUrl=String(url);return {ok:true,json:async()=>({items,total:items.length,scanId:'scan-1',scanDate:'2026-09-18'})};};
  d.window.eval(fs.readFileSync(path.join(root,'public/screener-buy-preview.js'),'utf8'));return d;
}
test('homepage shows no recommendation when a validated quantile row is unavailable',async()=>{
  const d=setup([{ticker:'OLD',signal:'buy',current_signal:{value:'buy'},buy_price_target:120}]);await tick();
  assert.equal(d.window.document.querySelectorAll('[data-buy-cards] article').length,0);
  assert.match(d.window.document.querySelector('[data-buy-status]').textContent,/No validated mega-cap forecasts/);
  assert.doesNotMatch(d.window.requestedUrl,/signal=buy/);d.window.close();
});
test('homepage links exact published forecast and displays neutral quantiles',async()=>{
  const d=setup([{ticker:'TEST',company_name:'<img src=x>',actual_price:131,p50:132,p99:136,market_cap:800_000_000_000,signal:'buy'}]);await tick();
  const card=d.window.document.querySelector('[data-buy-cards] article');assert.ok(card);assert.equal(card.querySelector('img'),null);
  assert.match(card.querySelector('a').href,/screenerTicker=TEST&screenerScan=scan-1/);assert.match(card.textContent,/First P50/);assert.match(card.textContent,/136/);
  assert.doesNotMatch(d.window.document.getElementById('home-buy-signals').textContent,/Buy signal|Sell signal|approved/i);d.window.close();
});

test('homepage only displays validated mega caps in descending market-cap order',async()=>{
  const mega=Array.from({length:8},(_,i)=>({ticker:`MEGA${i}`,market_cap:200_000_000_000+i*100_000_000_000,actual_price:120,p50:121,p99:130}));
  const d=setup([{ticker:'SMALL',market_cap:2_000_000_000,actual_price:120,p50:121},...mega,{ticker:'UNKNOWN',actual_price:120,p50:121},{ticker:'UNVALIDATED',market_cap:10_000_000_000_000,actual_price:120}]);
  await tick();
  const tickers=Array.from(d.window.document.querySelectorAll('[data-buy-cards] article a')).map(a=>a.textContent);
  assert.deepEqual(tickers,['MEGA7','MEGA6','MEGA5','MEGA4','MEGA3','MEGA2']);
  const query=new URL(d.window.requestedUrl,'https://quantura.studio').searchParams;
  assert.equal(query.get('marketCap'),'mega');
  assert.equal(query.get('sort'),'marketCap');
  assert.equal(query.get('direction'),'desc');
  assert.match(d.window.document.querySelector('[data-buy-status]').textContent,/6 mega-cap forecasts/);
  d.window.close();
});
