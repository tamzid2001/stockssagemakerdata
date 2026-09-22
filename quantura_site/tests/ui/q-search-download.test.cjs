const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const root=path.join(__dirname,'../..'),source=f=>fs.readFileSync(path.join(root,'public',f),'utf8'),page=fs.readFileSync(path.join(root,'pages/forecasting.html'),'utf8');
const api=require('../../public/q-market.js'),tick=()=>new Promise(r=>setTimeout(r,5));
const stock={resource_id:'yahoo:AAPL',resource_type:'instrument',symbol:'AAPL',name:'Apple',asset_class:'equity',source:'yahoo',forecast_available:true};
const settings={kind:'history',range:'latest',frequency:'1Day',limit:500,end:'',session:'regular',adjustment:'split',phase:'auto',layout:'normalized',target:'price',missing:'leave'};
const dom=()=>new JSDOM(page,{url:'https://quantura.studio/forecasting',runScripts:'outside-only'});
const row=i=>({resource_id:`polymarket_us:${i}`,resource_type:'prediction_market_contract',symbol:`match-${i}`,name:`Player ${i}`,source:'polymarket_us',event_slug:'match',outcome:`Yes ${i}`,market_group:i%2?'Player props':'Totals',market_title:`Player ${i} total`,forecast_available:true,contract_id:`${i}`,contract:{source:'polymarket_us',contractId:`${i}`,providerSymbol:`match-${i}`,eventTitle:'A vs B',side:'long'}});

test('one Q Search, one Q Download, hidden compatibility fields and upload remain',()=>{
  const d=dom(),doc=d.window.document;
  assert.equal(doc.querySelectorAll('#market-search-form').length,1);assert.equal(doc.querySelectorAll('[data-panel="download"]').length,1);
  assert.equal(doc.querySelectorAll('#pm-search,#alpaca-symbol,#alpaca-options-underlying').length,0);
  assert.equal(doc.getElementById('market-search-source').type,'hidden');assert.ok(doc.getElementById('q-upload-csv'));assert.ok(doc.getElementById('ensemble-csv-file'));
  d.window.close();
});
test('Auto routes every instrument with immutable symbol and provider-aware request shape',()=>{
  assert.equal(api.requestFor(stock,settings).body.source,'auto');assert.equal(api.requestFor(stock,settings).body.adjustment,'split');
  assert.equal(api.requestFor({...stock,source:'kalshi_perps'},settings).url,'/api/market-data/perps/history');
  const r=row(1),p=api.requestFor(r,{...settings,frequency:'1Min'},[r.contract]);assert.equal(p.body.contracts[0].contractId,'1');assert.equal(p.body.frequency,'1m');assert.equal(p.body.history_phase,'auto');
  assert.throws(()=>api.requestFor(r,settings,[{...r.contract,source:'kalshi'}]),/one provider/);
  assert.throws(()=>api.requestFor(stock,{...settings,kind:'options'}),/specific call or put/);
});
test('exports are the preview snapshot, latest N is applied independently per contract, CSV text is safe',()=>{
  const rows=Array.from({length:502},(_,i)=>({timestamp:new Date(i*60000).toISOString(),contract_id:'yes',price:i/1000}));rows.push({timestamp:'2026-01-01T00:00:00Z',contract_id:'no',price:.2,note:'=SUM(A1)'});
  const snapshot=api.snapshot({rows,metadata:{source:'kalshi'}},stock,settings,{body:{source:'auto'}});
  assert.equal(snapshot.rows.length,501);assert.equal(snapshot.rows[0].price,.002);assert.equal(snapshot.metadata.row_count,501);
  assert.match(api.csv(snapshot),/'=SUM\(A1\)/);assert.match(api.csv(snapshot),/resource_id,provider/);
  assert.equal(JSON.parse(JSON.stringify(snapshot)).rows.length,501);
});
test('Q Search paginates and filters hundreds of props, preserves exact IDs and outside dismissal',async()=>{
  const d=dom(),w=d.window;w.__quanturaSetPanel=()=>{};let calls=[];
  w.fetch=async url=>{calls.push(url);return {ok:true,json:async()=>url.includes('/event?')?{title:'A vs B',groups:{polymarket_us:Array.from({length:100},(_,i)=>row((url.includes('cursor=')?100:0)+i))},next_cursor:url.includes('cursor=')?null:'next',related_events:[]}:{count:1,groups:{polymarket_us:[row(0)]}}};};
  w.eval(source('q-market.js'));w.eval(source('market-search.js'));w.document.getElementById('market-search-query').value='A vs B';w.document.getElementById('market-search-form').dispatchEvent(new w.Event('submit',{cancelable:true}));await tick();
  assert.match(calls[0],/source=auto/);assert.match(calls[0],/rank=true/);
  w.document.querySelector('[data-market-action="event"]').click();await tick();assert.equal(w.document.querySelectorAll('[data-market-resource]').length,100);
  w.document.querySelector('[data-market-action="more"]').click();await tick();assert.equal(w.document.querySelectorAll('[data-market-resource]').length,200);
  const input=w.document.getElementById('q-event-filter');input.value='Player 199';input.dispatchEvent(new w.Event('input',{bubbles:true}));
  assert.equal(w.document.querySelectorAll('[data-market-resource]:not([hidden])').length,1);
  w.document.querySelector('[data-market-resource]:not([hidden]) [data-market-action="prediction-forecast"]').click();
  assert.equal(w.QuanturaMarketSelection.contract_id,'199');assert.equal(w.document.getElementById('market-search-results').hidden,true);
  d.window.close();
});
test('download snapshot invalidates on selection/date changes and stale requests cannot replace it',async()=>{
  const d=dom(),w=d.window,pending=[];w.fetch=()=>new Promise(r=>pending.push(r));w.eval(source('q-market.js'));w.eval(source('q-download.js'));
  const select=r=>w.dispatchEvent(new w.CustomEvent('quantura:market-selected',{detail:{resource:r,intent:'download'}}));
  select(stock);const form=w.document.getElementById('q-download-form');form.dispatchEvent(new w.Event('submit',{cancelable:true}));
  select({...stock,symbol:'MSFT',resource_id:'yahoo:MSFT'});pending[0]({ok:true,json:async()=>({rows:[{timestamp:'2026-01-01',close:100}],provider:'yahoo'})});await tick();
  assert.equal(w.document.getElementById('qd-csv').disabled,true);assert.equal(w.document.getElementById('qd-preview-table').textContent,'');
  form.dispatchEvent(new w.Event('submit',{cancelable:true}));pending[1]({ok:true,json:async()=>({rows:[{timestamp:'2026-01-02',close:101}],provider:'yahoo'})});await tick();
  assert.equal(w.document.getElementById('qd-csv').disabled,false);assert.match(w.document.getElementById('qd-preview-table').textContent,/101/);
  w.document.getElementById('qd-end').dispatchEvent(new w.Event('input',{bubbles:true}));assert.equal(w.document.getElementById('qd-csv').disabled,true);d.window.close();
});
test('multi-outcome basket preserves six soccer sides and never mixes providers',()=>{
  const d=dom(),w=d.window;w.eval(source('q-market.js'));w.eval(source('q-download.js'));
  for(let i=0;i<6;i++)w.dispatchEvent(new w.CustomEvent('quantura:market-selected',{detail:{resource:row(i),intent:'add-download'}}));
  assert.equal(w.document.querySelectorAll('[data-remove]').length,6);
  const kalshi={...row(8),resource_id:'kalshi:8',source:'kalshi',contract:{...row(8).contract,source:'kalshi'}};
  w.dispatchEvent(new w.CustomEvent('quantura:market-selected',{detail:{resource:kalshi,intent:'download'}}));assert.equal(w.document.querySelectorAll('[data-remove]').length,1);d.window.close();
});
test('old routes map to Q Download; no pricing/model/forecast API behavior is replaced',()=>{
  assert.match(source('app.js'),/\["sports-autopilot", "news", "options", "download"\].*return "download"/);
  assert.match(source('app.js'),/provider.value = "auto"/);
  assert.match(source('q-terminal.css'),/z-index:120/);
});
