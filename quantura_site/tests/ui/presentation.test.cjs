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

test('live moneylines browse without a search term and select the exact side for forecasts or downloads', async () => {
  const d=dom(page('forecasting.html')); const w=d.window; let request; const selections=[];
  w.__quanturaSetPanel=()=>{};w.HTMLElement.prototype.scrollIntoView=()=>{};
  w.addEventListener('quantura:market-selected',event=>selections.push(event.detail));
  const contract={source:'polymarket_us',contractId:'side-b',side:'short',eventTitle:'A vs B'};
  w.fetch=async url=>{request=url;return {ok:true,json:async()=>({count:1,groups:{polymarket_us:[{resource_type:'prediction_market_contract',resource_id:'polymarket_us:side-b',source:'polymarket_us',symbol:'game',contract_id:'side-b',outcome:'B',name:'B · A vs B',status:'open',timing:'live',contract}]}})};};
  w.eval(source('market-search.js'));w.document.querySelector('[data-market-mode="live"]').click();await tick();
  assert.match(request,/mode=live/);assert.equal(w.document.getElementById('market-search-query').required,false);
  w.document.querySelector('[data-market-action="prediction-forecast"]').click();
  assert.equal(selections[0].intent,'forecast');assert.equal(selections[0].resource.contract.side,'short');
  w.document.getElementById('market-search-query').value='https://polymarket.us/event/game';
  w.document.getElementById('market-search-form').dispatchEvent(new w.Event('submit'));await tick();
  assert.match(request,/market-search\/resolve\?url=/);
  w.document.querySelector('[data-market-action="prediction-download"]').click();
  assert.equal(selections[1].intent,'download'); assert.equal(w.QuanturaMarketSelection.contract_id,'side-b');
  d.window.close();
});

test('primary form builds prediction-market minute and single-model requests without treating contracts as tickers', () => {
  const d=dom(page('forecasting.html'));const w=d.window;
  w.document.getElementById('ensemble-model-list').innerHTML='<article data-ensemble-model="prophet"><input name="ensemble_model_enabled" type="checkbox" checked><input data-model-weight="prophet" value="1"></article>';
  w.QuanturaMarketSelection={source:'kalshi',symbol:'KXGAME-TEAM',contract_id:'KXGAME-TEAM:yes'};
  w.document.getElementById('ensemble-source-type').value='prediction_market';
  w.eval(`const ui={ensembleForecastForm:document.getElementById('ensemble-forecast-form'),ensembleModelList:document.getElementById('ensemble-model-list')};
    const ensembleUiState={capabilities:{models:[{id:'prophet',available:true}]}};
    const state={activeWorkspaceId:'workspace-fixture',user:{uid:'user-fixture'},tickerContext:{}};
    const ensembleQuantileKey=v=>String(Number(v));const normalizeTicker=v=>String(v).trim().toUpperCase();
    const ensembleDurationMinutes = ${source('app.js').split('  const ensembleDurationMinutes =')[1].split('  const ensembleTimeZone =')[0]}
    ${source('app.js').split('  const getEnsembleSelections =')[1].split('  const renderEnsembleProgress =')[0].replace(/^/, 'const getEnsembleSelections =')}
    window.build=buildEnsembleRequest;`);
  const request=w.build();assert.equal(request.source.type,'prediction_market');assert.equal(request.source.contract_id,'KXGAME-TEAM:yes');
  assert.equal(request.frequency,'1min');assert.equal(request.horizon_mode,'frequency_periods');assert.equal(request.calendar,'NONE');assert.equal(request.transform,'logit');
  assert.deepEqual(JSON.parse(JSON.stringify(request.models)),{prophet:{enabled:true,weight:1}});
  w.document.getElementById('ensemble-source-type').value='ticker';w.document.getElementById('ensemble-ticker').value='MSFT';
  const stock=w.build();assert.equal(stock.source.symbol,'MSFT');assert.equal(stock.source.limit,500);assert.equal(stock.source.start,undefined);
  w.document.getElementById('ensemble-history-lag').value='30';
  w.document.getElementById('ensemble-history-lag-unit').value='minutes';
  assert.equal(w.build().history_lag_minutes,30);
  w.document.getElementById('ensemble-history-lag').value='2';
  w.document.getElementById('ensemble-history-lag-unit').value='days';
  assert.equal(w.build().history_lag_minutes,2880);
  w.document.getElementById('ensemble-ticker-frequency').value='1Hour';
  assert.equal(w.build().frequency,'1h');assert.equal(w.build().horizon_mode,'frequency_periods');
  w.document.getElementById('ensemble-source-type').value='prediction_market';
  w.document.getElementById('ensemble-history-phase').value='in_game';
  w.document.getElementById('ensemble-history-lookback').value='60';
  assert.equal(w.build().source.history_phase,'in_game');assert.equal(w.build().source.history_lookback_minutes,60);
  w.document.getElementById('ensemble-history-lookback').value='-1';assert.throws(()=>w.build(),/duration/);
  d.window.close();
});

test('forecast chart focuses recent hour plus future and includes explicitly requested P10/P90 lines', async () => {
  const d=dom(page('forecasting.html')); const w=d.window;
  w.eval('const ensembleChartDefaultRange =' + source('app.js').split('  const ensembleChartDefaultRange =')[1].split('  const ensembleDatasetFrequency =')[0] + '\nwindow.range=ensembleChartDefaultRange;');
  const job={forecast_id:'fixture',source:{type:'prediction_market'},frequency:'1min',created_at:'2026-09-12T16:30:00Z',
    history:[{timestamp:'2026-09-12T16:00:00Z',target:.4}],quantiles:[.1,.5,.9],predictions:[{timestamp:'2026-09-12T17:00:00Z',quantiles:{'0.1':.3,'0.5':.5,'0.9':.7}}]};
  assert.deepEqual(Array.from(w.range(job)),[Date.parse('2026-09-12T15:30:00Z'),Date.parse('2026-09-12T17:00:00Z')]);
  assert.equal(w.range({...job,source:{type:'ticker'},frequency:'1D'}),null);
  w.record=[];w.eval(`const ui={ensembleForecastChart:document.getElementById('ensemble-forecast-chart')};
    const ensembleUiState={chartWindowId:'',chartWindow:null}; const ensembleChartDefaultRange=window.range; const getPlotly=async()=>({react:async(...a)=>window.record.push(a)});
    const isDarkMode=()=>false, ensembleQuantileKey=String, ensembleQuantileLabel=q=>'P'+Math.round(q*100);
    const escapeHtml=String,ensembleMarketIdentity=()=>({title:'Selected side · Fixture game'});
    const ensembleTimeZone=()=> 'America/New_York',ensembleChartTime=v=>v,ensembleLocalTime=v=>String(v);
    const renderEnsembleChart =${source('app.js').split('  const renderEnsembleChart =')[1].split('  const startEnsembleObservations =')[0]}
    window.renderChart=renderEnsembleChart;`);
  await w.renderChart(job);
  assert.ok(w.record[0][1].some(t=>t.name==='P10 ensemble'));
  assert.ok(w.record[0][1].some(t=>t.name==='P90 ensemble'));
  assert.equal(w.record[0][2].uirevision,'fixture');
  assert.equal(w.record[0][2].title.text,'Selected side · Fixture game');
  assert.ok(w.record[0][2].xaxis.ticktext.every(t=>/AM|PM/.test(t)));
  await w.renderChart({...job,quantiles:[.5]});
  assert.ok(!w.record[1][1].some(t=>/^P(10|90) ensemble$/.test(t.name)));
  d.window.close();
});

test('advanced help is an accessible modal and dataset frequency only belongs to uploaded data', () => {
  const d=dom(page('forecasting.html')); const w=d.window;
  const dialog=w.document.getElementById('ensemble-settings-help');
  dialog.showModal=function(){this.open=true;};dialog.close=function(){this.open=false;};
  w.eval('const help ='+source('app.js').split('    const help = document.getElementById("ensemble-settings-help");')[1].split('    window.addEventListener("quantura:market-selected"')[0].replace(/^/, 'document.getElementById("ensemble-settings-help");'));
  w.document.getElementById('ensemble-settings-help-open').click(); assert.equal(dialog.open,true);
  w.document.getElementById('ensemble-settings-help-close').click(); assert.equal(dialog.open,false);
  assert.equal(dialog.getAttribute('aria-labelledby'),'ensemble-settings-help-title');
  const frequency=w.document.getElementById('ensemble-frequency');
  assert.ok(w.document.getElementById('ensemble-model-list').closest('details.ensemble-advanced-settings'));
  assert.equal(frequency.tagName,'SELECT'); assert.equal(frequency.closest('[data-ensemble-source]').dataset.ensembleSource,'workspace_dataset');
  frequency.value='custom';frequency.dispatchEvent(new w.Event('change'));
  assert.equal(w.document.getElementById('ensemble-frequency-custom').hidden,false);
  d.window.close();
});

test('latest quote uses the saved side, labels staleness and does not invent updated trading probabilities', () => {
  const d=dom(page('forecasting.html')),w=d.window;
  w.eval(`const escapeHtml=s=>String(s).replaceAll('<','&lt;'),ensembleQuantileKey=String;
    const ensembleTimeZone=${source('app.js').split('  const ensembleTimeZone =')[1].split('  const describeEnsembleCapability =')[0]}
    window.live=renderEnsembleLiveQuote;`);
  const now=Date.parse('2026-09-12T20:00:00Z');
  const job={source:{type:'prediction_market',symbol:'game',outcome:'Michigan Wolverines',side:'short'},history:[{timestamp:'2026-09-12T19:30:00Z',target:.1}],observations:[{timestamp:'2026-09-12T19:59:00Z',target:.4}],quantiles:[.1,.5,.9],predictions:[{timestamp:'2026-09-12T20:30:00Z',quantiles:{'0.1':.2,'0.5':.5,'0.9':.8}}]};
  w.live(job,now);const text=w.document.getElementById('ensemble-live-quote').textContent;
  assert.match(text,/Michigan Wolverines \(short\)/);assert.match(text,/quote 0.4/);assert.match(text,/1 min old/);assert.match(text,/not an updated conditional forecast/);assert.doesNotMatch(text,/Stale/);
  w.live(job,now+3600_000);assert.match(w.document.getElementById('ensemble-live-quote').textContent,/Stale.*horizon has ended/s);
  d.window.close();
});

test('forecast polling retries transient failures without submitting duplicate compute', async () => {
  const d=dom(page('forecasting.html')),w=d.window;
  w.scheduled=[];w.setTimeout=fn=>{w.scheduled.push(fn);return w.scheduled.length;};w.clearTimeout=()=>{};
  w.calls=0;w.rendered=false;w.messages=[];
  w.eval(`const ensembleUiState={pollGeneration:0,pollTimer:0};const ui={};
    const setEnsembleBusy=()=>{},setEnsembleStatus=s=>window.messages.push(s),renderEnsembleProgress=()=>{};
    const renderCompletedEnsemble=async()=>{window.rendered=true;};
    const apiRequestJson=async(path)=>{if(!path.startsWith('/api/v1/ensemble-forecasts/'))throw Error('Unexpected route');if(++window.calls===1)throw Error('Timeout');return {data:{status:'completed'}};};
    const stopEnsemblePolling=${source('app.js').split('  const stopEnsemblePolling =')[1].split('  const loadEnsembleCapabilities =')[0]}
    window.poll=pollEnsembleForecast;`);
  await w.poll('fixture');assert.equal(w.scheduled.length,1);assert.match(w.messages[0],/not restarted/);
  await w.scheduled[0]();assert.equal(w.calls,2);assert.equal(w.rendered,true);
  d.window.close();
});

test('request navigation and opposite-side forecast are explicit actions; Foundry hides market search', () => {
  const d=dom(page('forecasting.html')),document=d.window.document;
  assert.equal(document.querySelector('#ensemble-request-previous').type,'button');
  assert.equal(document.querySelector('#ensemble-request-next').type,'button');
  assert.equal(document.querySelector('#ensemble-other-side').hidden,true);
  const app=source('app.js');
  assert.match(app,/marketSelector.hidden = \["autopilot", "foundry"\].includes\(next\)/);
  assert.match(app,/candidates.length!==1/);
  assert.match(app,/row.contract\?\.marketId===s.market_id&&row.contract_id!==s.contract_id/);
  assert.match(app,/if\(!ids.length\|\|ids.length>50\)return/);
  assert.match(app,/method:'DELETE'/);
  assert.match(app,/lock-open/);
  d.window.close();
});

test('Quantura Forecast is the expanded primary form with exactly one submission action', () => {
  const d=dom(page('forecasting.html')); const document=d.window.document;
  const panel=document.querySelector('[data-panel="forecast"]');
  assert.equal(panel.querySelector('h2').textContent,'Quantura Forecast');
  assert.equal(panel.querySelector('#ensemble-forecast-settings').tagName,'SECTION');
  assert.equal(panel.querySelectorAll('button[type="submit"]').length,1);
  assert.equal(panel.querySelector('button[type="submit"]').textContent.trim(),'Run forecast');
  assert.equal(panel.querySelector('#forecast-form'),null);
  assert.equal(panel.querySelector('#legacy-forecast-detail').hidden,true);
  assert.doesNotMatch(panel.textContent,/Advanced asynchronous forecast|Five-model probabilistic ensemble|Open this section/);
  assert.match(source('app.js'),/if \(next === "forecast"\) refreshPrimaryForecast\(\)/);
  assert.match(source('app.js'),/ensembleUiState.accessKey === accessKey/);
  d.window.close();
});

test('ensemble refresh retains configuration, fetches latest history, and formats local 12-hour times', () => {
  const d=dom(page('forecasting.html'));const w=d.window;
  w.eval(source('app.js').split('  const ensembleTimeZone =')[1].split('  const ensembleQuantileLabel =')[0].replace(/^/, 'const ensembleTimeZone =') + '\nwindow.helpers={ensembleLocalTime,ensembleChartTime,ensembleHorizonLabel,refreshedEnsembleRequest,setEnsembleBusy};');
  const h=w.helpers;
  assert.match(h.ensembleLocalTime('2026-09-11T22:00:00Z','America/New_York'),/6:00 PM/);
  assert.match(h.ensembleLocalTime('2026-01-11T22:00:00Z','America/New_York'),/5:00 PM/);
  assert.notEqual(h.ensembleLocalTime('2026-11-01T05:30:00Z','America/New_York'),h.ensembleLocalTime('2026-11-01T06:30:00Z','America/New_York'));
  assert.equal(h.ensembleChartTime('2026-09-11','America/New_York'),'2026-09-11');
  const job={workspace_id:'ws',source:{type:'prediction_market',provider:'polymarket_us',symbol:'game',contract_id:'side',end:'stale'},prediction_length:30,frequency:'1min',horizon_mode:'frequency_periods',models:{prophet:{enabled:true,weight:1}},quantiles:[.1,.5],transform:'logit'};
  const refreshed=h.refreshedEnsembleRequest(job);
  assert.equal(refreshed.source.end,undefined);assert.equal(refreshed.source.contract_id,'side');assert.equal(refreshed.models,job.models);
  assert.equal(h.ensembleHorizonLabel(job),'30 minutes');assert.equal(h.ensembleHorizonLabel({...job,frequency:'1h'}),'30 hours');
  assert.equal(w.document.querySelectorAll('#ensemble-refresh-latest').length,1);
  assert.equal(w.document.getElementById('ensemble-progress').hidden,true);
  assert.match(source('app.js'),/sourceRef.collection === "ensemble_forecast_jobs"/);
  assert.doesNotMatch(source('app.js').split('const renderCompletedEnsemble =')[1].split('const stopEnsemblePolling =')[0],/<strong>Transform:/);
  d.window.close();
});

test('forecast summary averages columns and qualifies terminal quantile probabilities', () => {
  const d=dom(page('forecasting.html'));const w=d.window;
  w.eval('const ensembleQuantileKey=v=>String(Number(v)); const ensembleDistributionSummary =' + source('app.js').split('  const ensembleDistributionSummary =')[1].split('  const ensembleQuantileLabel =')[0] + '\nwindow.summary=ensembleDistributionSummary;');
  const result=w.summary({history:[{timestamp:'2026-09-11T12:00:00Z',target:.4}],quantiles:[.25,.5,.75],predictions:[{quantiles:{'0.25':.3,'0.5':.5,'0.75':.7}},{quantiles:{'0.25':.4,'0.5':.6,'0.75':.8}}]});
  assert.equal(result.nearest,.25);assert.equal(result.probabilityHigher,.75);assert.ok(Math.abs(result.averages['0.25']-.35)<1e-10);
  assert.match(source('app.js'),/model-implied, not a validated win rate/);
  assert.match(source('app.js'),/Recipients must sign in and have access/);
  assert.match(source('app.js'),/Downloaded input history/);
  assert.match(source('app.js'),/Observed after forecast/);
  assert.doesNotMatch(source('app.js').split('const renderEnsembleChart =')[1].split('const startEnsembleObservations =')[0],/apiFetchTickerHistory/);
  d.window.close();
});

test('queued forecast clears the preceding distribution and shows downloaded input count', () => {
  const d=dom(page('forecasting.html')); const w=d.window;
  w.ui={}; for(const [key,id] of Object.entries({ensembleForecastResults:'ensemble-forecast-results',ensembleResultState:'ensemble-result-state',ensembleResultMeta:'ensemble-result-meta',ensembleSummary:'ensemble-forecast-summary',ensembleObservedMetrics:'ensemble-observed-metrics',ensembleObservationStatus:'ensemble-observation-status',ensembleForecastChart:'ensemble-forecast-chart',ensembleResultTable:'ensemble-result-table'})) w.ui[key]=w.document.getElementById(id);
  w.ensembleUiState={}; w.setEnsembleBusy=()=>{};w.setEnsembleStatus=()=>{};w.titleCaseLabel=s=>s;w.escapeHtml=s=>String(s);
  w.ui.ensembleSummary.innerHTML='<p>Previous forecast</p>';
  w.ui.ensembleObservedMetrics.textContent='Old metrics';
  w.eval('const renderEnsembleProgress ='+source('app.js').split('  const renderEnsembleProgress =')[1].split('  const renderEnsembleChart =')[0]+'\nwindow.renderProgress=renderEnsembleProgress;');
  w.renderProgress({forecast_id:'new-job',status:'queued',input_row_count:500,progress:{total_models:5}});
  assert.equal(w.ui.ensembleSummary.hidden,true); assert.equal(w.ui.ensembleSummary.textContent,'');
  assert.equal(w.ui.ensembleObservedMetrics.textContent,''); assert.match(w.ui.ensembleResultMeta.textContent,/500 observed bars/);
  d.window.close();
});

test('market selection configures the primary ensemble, including dataset-to-ticker switching', async () => {
  const d=dom(page('forecasting.html')); const w=d.window; const document=w.document;
  w.HTMLElement.prototype.scrollIntoView=()=>{};
  let panel=''; w.__quanturaSetPanel=value=>{panel=value;};
  w.fetch=async()=>({ok:true,json:async()=>({count:1,groups:{yahoo:[{symbol:'TEST',name:'Test equity',asset_class:'equity',source:'yahoo',forecast_available:true}]}})});
  document.getElementById('ensemble-source-type').value='workspace_dataset';
  let changes=0;document.getElementById('ensemble-source-type').addEventListener('change',()=>changes++);
  w.eval(source('market-search.js'));
  document.getElementById('market-search-query').value='TEST';
  document.getElementById('market-search-form').dispatchEvent(new w.Event('submit',{cancelable:true}));await tick();
  document.querySelector('[data-market-action="forecast"]').click();
  assert.equal(document.getElementById('ensemble-ticker').value,'TEST');
  assert.equal(document.getElementById('ensemble-provider').value,'yahoo');
  assert.equal(document.getElementById('ensemble-source-type').value,'ticker');
  assert.equal(changes,1);assert.equal(panel,'forecast');d.window.close();
});

test('primary forecast loads capabilities on authenticated activation without automatically running compute', async () => {
  const d=dom(page('forecasting.html')); const w=d.window;const document=w.document;
  const client=source('app.js');
  const implementation=client.slice(client.indexOf('  const refreshPrimaryForecast ='),client.indexOf('  const loadEnsemblePresets ='));
  let signedIn=false,capabilities=0,presets=0;
  w.hasFullAccount=()=>signedIn;w.state={tickerContext:{ticker:'TEST'}};
  w.ui={ensembleForecastSettings:document.getElementById('ensemble-forecast-settings'),ensembleModelList:document.getElementById('ensemble-model-list'),ensembleTicker:document.getElementById('ensemble-ticker')};
  w.setEnsembleStatus=()=>{};w.normalizeTicker=s=>s;w.getQueryParam=()=>'';
  w.loadEnsembleCapabilities=async()=>{capabilities++;};w.loadEnsemblePresets=async()=>{presets++;};
  w.eval(implementation+'\nwindow.refreshTestForecast=refreshPrimaryForecast;');
  await w.refreshTestForecast();assert.equal(capabilities,0);assert.match(w.ui.ensembleModelList.textContent,/Sign in/);
  signedIn=true;await w.refreshTestForecast();assert.equal(capabilities,1);assert.equal(presets,1);assert.equal(w.ui.ensembleTicker.value,'TEST');
  document.querySelector('[data-panel="forecast"]').classList.add('hidden');await w.refreshTestForecast();assert.equal(capabilities,1);
  assert.doesNotMatch(implementation,/method: "POST"|pollEnsembleForecast/);d.window.close();
});
