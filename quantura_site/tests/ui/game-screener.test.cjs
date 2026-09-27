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
  assert.equal(w.document.activeElement.value,'polymarket_us');
  cards.querySelector('input[value="yes"]').click();
  assert.equal(cards.querySelector('a').search,'?panel=forecast&gameForecastId='+id(3));
  assert.match(cards.querySelector('.game-selected-outcome').textContent,/Cubs/);
  d.window.close();
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
  const rows=fixtures();w.fetch=async url=>{requests.push(String(url));return {ok:true,json:async()=>({date:'2026-09-27',items:requests.length===1?rows.slice(0,1):rows.slice(1),next_cursor:requests.length===1?id(1):null,coverage:[],bounded:requests.length===1})};};
  w.eval(fs.readFileSync(path.join(root,'public/screener.js'),'utf8'));for(let i=0;i<5;i++)await tick();
  assert.equal(requests.length,2);assert.match(requests[1],/cursor=/);
  assert.equal(w.document.getElementById('qs-games').hidden,false);
  assert.equal(w.document.querySelectorAll('.game-card').length,1);
  assert.equal(w.document.getElementById('qs-metric-unit').textContent,'games');
  assert.equal(w.document.getElementById('qs-metric-matches').textContent,'1');
  assert.match(w.document.getElementById('qs-metric-total').textContent,/4 outcome forecasts/);
  assert.equal(w.document.getElementById('qs-export').getAttribute('aria-disabled'),null);
  assert.doesNotMatch(w.document.getElementById('qs-freshness').textContent,/unavailable/);
  d.window.close();
});
