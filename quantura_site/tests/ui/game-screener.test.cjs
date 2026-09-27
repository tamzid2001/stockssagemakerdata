const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM}=require('jsdom');
const root=path.resolve(__dirname,'../..');
const script=fs.readFileSync(path.join(root,'public/game-forecasts.js'),'utf8');
const tick=()=>new Promise(resolve=>setTimeout(resolve,0));
const id=n=>n.toString(16).padStart(32,'0');
function row(n,provider,symbol,side,outcome,patch={}){
  return {id:id(n),provider,symbol,side,outcome,contract_id:provider==='kalshi'?symbol+':'+side:String(n),event_id:symbol.split('-').slice(0,2).join('-'),
    event_title:provider==='kalshi'?'Chicago C vs Boston':'Chicago Cubs vs. Boston Red Sox',game_start:'2026-09-27T19:05:00Z',forecast_end:'2026-09-27T23:05:00Z',
    generated_at:'2026-09-27T18:24:00Z',models:['prophet','toto','granite','chronos','timesfm'],status:'final_pregame',
    endpoint:{'0.01':.1,'0.25':.3,'0.5':side==='no'||side==='short'?.4:.6,'0.75':.7,'0.9':.8,'0.99':.9},...patch};
}
const kalshi='KXMLBGAME-26SEP261915CHCBOS-CHC',poly='aec-mlb-chc-bos-2026-09-26';
const fixtures=()=>[row(1,'kalshi',kalshi,'yes','Chicago C'),row(2,'kalshi',kalshi,'no','No'),row(3,'polymarket_us',poly,'long','Cubs'),row(4,'polymarket_us',poly,'short','Red Sox')];
function setup(html='<div id="game-forecast-page" hidden></div>'){
  const d=new JSDOM(html,{url:'https://quantura.studio/forecasting?panel=screener&source=today_games',runScripts:'outside-only'});
  d.window.eval(script);return d;
}
test('late-mounted Terminal screener renders shared games and actual provider/side forecast links',()=>{
  const d=setup(),w=d.window;
  // The shared game script loads before Terminal fetches the screener fragment.
  const cards=w.document.createElement('div');cards.id='qs-games';w.document.body.append(cards);
  w.QuanturaGames.render(w.QuanturaGames.group(fixtures()));
  assert.equal(cards.querySelectorAll('.game-card').length,1);
  assert.equal(cards.querySelector('a').search,'?panel=forecast&gameForecastId='+id(1));
  assert.equal(cards.querySelectorAll('dl dd').length,6);
  cards.querySelector('input[value="no"]').click();
  assert.equal(cards.querySelector('a').search,'?panel=forecast&gameForecastId='+id(2));
  assert.match(cards.querySelector('.game-probability').textContent,/40.0%/);
  cards.querySelector('input[value="polymarket_us"]').click();
  assert.equal(cards.querySelector('a').search,'?panel=forecast&gameForecastId='+id(4));
  assert.equal(cards.querySelector('.game-selected-outcome').textContent,'No · Chicago Cubs to win');
  assert.equal(w.document.activeElement.value,'polymarket_us');
  cards.querySelector('input[value="yes"]').click();
  assert.equal(cards.querySelector('a').search,'?panel=forecast&gameForecastId='+id(3));
  assert.match(cards.querySelector('.game-selected-outcome').textContent,/Cubs/);
  d.window.close();
});
test('game platform, link search and strict quantile filters use actual selected-side prices',()=>{
  const d=setup('<div id="qs-games"></div>'),w=d.window,rows=fixtures();
  rows[0].latest_price=.2;rows[1].latest_price=.7;rows[2].latest_price=.8;rows[3].latest_price=null;
  assert.equal(w.QuanturaGames.filter(rows,{search:'BOSTON   cubs'}).length,1);
  const linked=w.QuanturaGames.filter(rows,{search:'https://polymarket.us/sports/mlb/mlb-chc-bos-2026-09-26'});
  assert.equal(linked.length,1);assert.deepEqual(Array.from(linked[0].providers),['polymarket_us']);
  assert.equal(w.QuanturaGames.filter(rows,{search:'https://polymarket.us/sports/mlb/mlb-chc-bos-2026-09-12'}).length,0);
  const below=w.QuanturaGames.filter(rows,{comparison:'below',quantile:'0.5'});
  assert.deepEqual(Object.keys(below[0].markets[0].variants),['kalshi']);assert.equal(below[0].markets[0].variants.kalshi.yes.id,id(1));
  w.QuanturaGames.render(w.QuanturaGames.filter(rows,{provider:'kalshi'}));
  w.QuanturaGames.render(w.QuanturaGames.filter(rows,{provider:'polymarket_us',comparison:'above',quantile:'0.5'}));
  assert.equal(w.document.querySelector('input[value="polymarket_us"]').checked,true);
  assert.equal(w.document.querySelector('input[value="no"]').disabled,true);
  assert.match(w.document.querySelector('.game-price-comparison').textContent,/80.0¢.*60.0¢.*20.0¢ above/);
  d.window.close();
});
test('game cards expose both original market links and reject foreign URLs',()=>{
  const d=setup('<div id="qs-games"></div>'),w=d.window,rows=fixtures();
  rows.forEach(row=>row.market_url=row.provider==='kalshi'?'https://kalshi.com/markets/kxmlbgame/event/kxmlbgame-26sep261915chcbos':'https://polymarket.us/sports/mlb/mlb-chc-bos-2026-09-26');
  w.QuanturaGames.render(w.QuanturaGames.group(rows));
  const links=w.document.querySelectorAll('.game-exchange-link');assert.equal(links.length,2);assert.equal(links[0].target,'_blank');assert.match(links[0].rel,/noopener/);
  rows[0].market_url='https://kalshi.com.evil.test/market';rows[1].market_url='javascript:alert(1)';
  w.QuanturaGames.render(w.QuanturaGames.group(rows));assert.equal(w.document.querySelectorAll('.game-exchange-link').length,1);
  d.window.close();
});
test('selecting a new forecasting market leaves a saved game, restores settings and ignores late history',async()=>{
  const d=new JSDOM('<style>body.game-forecast-view #ensemble-forecast-settings{display:none}</style><div id="ensemble-forecast-settings">Models and horizon</div><div id="game-forecast-page" hidden></div>',{url:'https://quantura.studio/forecasting?panel=forecast&gameForecastId='+id(1),runScripts:'outside-only'}),w=d.window;
  let finishHistory;const item={...fixtures()[0],input_cutoff:'2026-09-27T18:00:00Z',history_count:2,predictions:[{timestamp:'2026-09-27T23:05:00Z',quantiles:fixtures()[0].endpoint}]};
  w.fetch=async url=>String(url).endsWith('/history')?new Promise(resolve=>{finishHistory=resolve;}):{ok:true,json:async()=>({item})};w.eval(script);for(let i=0;i<4;i++)await tick();
  assert.equal(w.getComputedStyle(w.document.getElementById('ensemble-forecast-settings')).display,'none');
  w.dispatchEvent(new w.CustomEvent('quantura:market-selected',{detail:{resource:{symbol:'AAPL'},intent:'forecast'}}));
  assert.equal(w.document.body.classList.contains('game-forecast-view'),false);assert.equal(w.document.getElementById('game-forecast-page').hidden,true);
  assert.notEqual(w.getComputedStyle(w.document.getElementById('ensemble-forecast-settings')).display,'none');assert.equal(new URL(w.location.href).searchParams.has('gameForecastId'),false);
  finishHistory({ok:true,json:async()=>({observations:[{timestamp:'2026-09-27T18:00:00Z',price:.4}]})});await tick();
  assert.equal(w.document.querySelector('[data-observed-history]'),null);d.window.close();
});
test('genuine history is drawn with a cutoff boundary and can be hidden without hiding forecast',async()=>{
  const d=new JSDOM('<div id="game-forecast-page" hidden></div>',{url:'https://quantura.studio/forecasting?panel=forecast&gameForecastId='+id(1),runScripts:'outside-only'}),w=d.window;
  const item={...fixtures()[0],input_cutoff:'2026-09-27T18:00:00Z',history_count:2,observations:[{timestamp:'2026-09-27T15:00:00Z',price:.3},{timestamp:'2026-09-27T18:00:00Z',price:.4}],predictions:[{timestamp:'2026-09-27T23:05:00Z',quantiles:fixtures()[0].endpoint}]};
  w.Date.now=()=>Date.parse('2026-09-27T20:30:00Z');
  w.fetch=async url=>({ok:true,json:async()=>String(url).endsWith('/history')?{observations:[...item.observations,{timestamp:'2026-09-27T19:00:00Z',price:.45},{timestamp:'2026-09-28T01:00:00Z',price:.9}],history_source:'saved_model_input_and_provider_outcomes'}:{item}});w.eval(script);for(let i=0;i<4;i++)await tick();
  assert.equal(w.document.querySelectorAll('circle[data-observed-history]').length,3);assert.ok(w.document.querySelector('[data-forecast-cutoff]'));
  assert.equal((w.document.querySelector('path[data-observed-history]').getAttribute('d').match(/M/g)||[]).length,2);
  const toggle=w.document.querySelector('input[name="showGameHistory"]');toggle.click();assert.equal(w.document.querySelector('[data-observed-history]'),null);assert.ok(w.document.querySelector('svg polyline'));
  w.document.querySelectorAll('.game-forecast-actions button')[0].click();assert.equal(w.document.body.classList.contains('game-forecast-view'),false);d.window.close();
});
test('game matching keeps kickoff changes and unrelated propositions separate; unavailable sides stay disabled',()=>{
  const d=setup('<div id="qs-games"></div>'),w=d.window;
  const rows=[...fixtures(),row(5,'kalshi',kalshi,'yes','Chicago C',{game_start:'2026-09-27T22:05:00Z'}),
    row(6,'kalshi','KXMLBSPREAD-26SEP261915CHCBOS-CHC15','yes','Chicago C -1.5',{market_title:'Chicago C wins by over 1.5 runs'})];
  const games=w.QuanturaGames.group(rows);assert.equal(games.length,2);
  const game=games.find(g=>g.game_start==='2026-09-27T19:05:00Z');assert.equal(game.markets.length,3);
  const spread=game.markets.find(m=>m.title.includes('1.5'));assert.deepEqual(Object.keys(spread.variants),['kalshi']);
  w.QuanturaGames.render([game]);
  const select=w.document.querySelector('.game-card select');select.value=spread.id;select.dispatchEvent(new w.Event('change'));
  assert.equal(w.document.querySelector('input[value="no"]').disabled,true);
  assert.equal(w.document.querySelector('a').search,'?panel=forecast&gameForecastId='+id(6));
  assert.match(w.document.querySelector('.game-card').textContent,/No forecast is not available/);
  d.window.close();
});
test('screener follows pagination before grouping, counting games and building outcome CSV',async()=>{
  const d=setup(),w=d.window,requests=[];
  const copy=new w.DOMParser().parseFromString(fs.readFileSync(path.join(root,'pages/screener.html'),'utf8'),'text/html');
  w.document.body.append(copy.querySelector('.qs-main'));
  w.URL.createObjectURL=()=> 'blob:games';w.URL.revokeObjectURL=()=>{};
  const rows=fixtures();w.fetch=async url=>{requests.push(String(url));if(String(url).endsWith('/prices'))return {ok:true,json:async()=>({date:'2026-09-27',items:rows.map(row=>({id:row.id,latest_price:.5,price_kind:'market_quote',price_checked_at:row.generated_at})),missing:0})};return {ok:true,json:async()=>({date:'2026-09-27',items:requests.length===1?rows.slice(0,1):rows.slice(1),next_cursor:requests.length===1?id(1):null,coverage:[],bounded:requests.length===1})};};
  w.eval(fs.readFileSync(path.join(root,'public/screener.js'),'utf8'));for(let i=0;i<5;i++)await tick();
  assert.equal(requests.length,3);assert.match(requests[1],/cursor=/);assert.match(requests[2],/prices$/);
  assert.equal(w.document.getElementById('qs-games').hidden,false);
  assert.equal(w.document.querySelectorAll('.game-card').length,1);
  assert.equal(w.document.getElementById('qs-metric-unit').textContent,'games');
  assert.equal(w.document.getElementById('qs-metric-matches').textContent,'1');
  assert.match(w.document.getElementById('qs-metric-total').textContent,/4 outcome forecasts/);
  assert.equal(w.document.getElementById('qs-export').getAttribute('aria-disabled'),null);
  assert.doesNotMatch(w.document.getElementById('qs-freshness').textContent,/unavailable/);
  d.window.close();
});
