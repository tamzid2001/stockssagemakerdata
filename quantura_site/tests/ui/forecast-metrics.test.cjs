const test=require('node:test');
const assert=require('node:assert/strict');
const {JSDOM}=require('jsdom');
const {compute,render}=require('../../public/forecast-metrics.js');
const time=n=>new Date(Date.UTC(2026,8,18,14,n)).toISOString();
const job=()=>({source:{type:'prediction_market'},frequency:'1min',completed_at:time(0),
  quantiles:[.1,.5,.9],history:[{timestamp:time(-1),target:8},{timestamp:time(0),target:10}],
  predictions:[1,2,3].map(n=>({timestamp:time(n),quantiles:{'0.1':n+8,'0.5':n+10,'0.9':n+12}})),
  observations:[{timestamp:time(1),target:10,interval:'1min'},{timestamp:time(2),target:14,interval:'1min'},{timestamp:time(3),target:12,interval:'1min'}]});
test('MAE, RMSE, sMAPE and wQL use the final ensemble on matching actuals',()=>{
  const r=compute(job()).prospective;
  assert.equal(r.count,3);assert.equal(r.mae,4/3);assert.equal(r.rmse,Math.sqrt(2));
  assert.ok(Math.abs(r.smape-(2/21+4/26+2/25)/3)<1e-12);
  assert.ok(Math.abs(r.average_wql-((1.2/36)+(4/36)+(1.2/36))/3)<1e-12);
});
test('deduplicate outcomes and ignore unfinished, fabricated, wrong-frequency, unmatched and future bars',()=>{
  const j=job();j.observations.push(j.observations[0],{timestamp:time(1),target:999,interval:'1h'},
    {timestamp:time(1),target:999,is_forward_filled:true},{timestamp:time(1),target:999,is_complete:false},
    {timestamp:time(1),target:999,observed:false},{timestamp:time(5),target:999});
  assert.equal(compute(j,Date.parse(time(2))).prospective.count,2);
});
test('retrospective outcomes and unknown publication times never inflate prospective metrics',()=>{
  const j=job();j.completed_at=time(2);
  assert.equal(compute(j).prospective.count,1);assert.equal(compute(j).retrospective.count,2);
  j.source.analysis_mode='historical_replay';assert.equal(compute(j).prospective.count,0);
  delete j.source.analysis_mode;delete j.completed_at;assert.equal(compute(j).prospective.count,0);
});
test('daily comparisons use final session closes, not provisional minute quotes or mismatched target fields',()=>{
  const j=job();j.frequency='1D';j.source={type:'ticker',field:'close'};j.completed_at='2026-09-17T20:01:00Z';
  j.predictions=[{timestamp:'2026-09-18T00:00:00Z',quantiles:{'0.1':9,'0.5':10,'0.9':11}}];
  j.observations=[{timestamp:'2026-09-18T13:30:00Z',interval:'1D',target:10},{timestamp:'2026-09-18T15:00:00Z',interval:'1min',target:100}];
  assert.equal(compute(j).prospective.count,1);assert.equal(compute(j).prospective.mae,0);
  j.source.field='volume';assert.equal(compute(j).prospective.count,0);assert.equal(compute(j).target_supported,false);
});
test('same-day daily forecasts use exchange close time, including early closes, rather than the midnight label',()=>{
  const j=job();j.source={type:'ticker'};j.frequency='1D';j.chart_calendar=require('../../public/market-calendars/nyse.json');
  j.completed_at='2026-11-27T17:00:00Z'; // Black Friday, 1pm NY / 18:00 UTC early close.
  j.predictions=[{timestamp:'2026-11-27T00:00:00Z',quantiles:{'0.1':9,'0.5':10,'0.9':11}}];
  j.observations=[{timestamp:'2026-11-27T14:30:00Z',interval:'1D',target:10}];
  assert.equal(compute(j,Date.parse('2026-11-27T17:30:00Z')).prospective.count,0);
  assert.equal(compute(j,Date.parse('2026-11-27T19:00:00Z')).prospective.count,1);
  j.completed_at='2026-11-27T18:01:00Z';
  assert.equal(compute(j,Date.parse('2026-11-27T19:00:00Z')).retrospective.count,1);
});
test('no P50 means no fabricated point forecast; custom quantiles still receive their own metrics',()=>{
  const j=job();j.quantiles=[.123,.876];j.predictions.forEach(r=>r.quantiles={'.123':11,'.876':14});
  // The public contract uses canonical numeric-string keys.
  j.predictions.forEach(r=>r.quantiles={'0.123':11,'0.876':14});
  const r=compute(j).prospective;assert.equal(r.count,3);assert.equal(r.point_count,0);assert.equal(r.mae,null);
  assert.equal(r.quantiles.length,2);assert.equal(r.quantiles[0].count,3);
});
test('zero denominators and constant outcomes remain unavailable, never NaN/Infinity',()=>{
  const j=job();j.history.forEach(r=>r.target=0);j.observations.forEach(r=>r.target=0);j.predictions.forEach(r=>r.quantiles={'0.1':0,'0.5':0,'0.9':0});
  const r=compute(j).prospective;
  assert.equal(r.average_wql,null);
  assert.equal(r.smape,0);assert.equal(r.mae,0);assert.doesNotMatch(JSON.stringify(r),/NaN|Infinity/);
});
test('missing intermediate observations never become invented outcomes',()=>{
  const j=job();j.observations.splice(1,1);assert.equal(compute(j).prospective.count,2);assert.equal(compute(j).prospective.mae,1);
});
test('legacy forecast explains how to calculate historical metrics, with live outcomes kept separate',()=>{
  const dom=new JSDOM('<section id="metrics"></section>'),host=dom.window.document.getElementById('metrics'),j=job();
  j.observations=[];render(host,j);assert.match(host.textContent,/Forecast quality/);assert.match(host.textContent,/Reproduce saved configuration/);
  assert.doesNotMatch(host.textContent,/Not available/);assert.equal(host.querySelectorAll('dl > div').length,4);
  render(host,job());assert.match(host.textContent,/3 \/ 3 timestamp-matched/);assert.match(host.textContent,/Small validation sample/);
  const help=host.querySelector('button');help.click();assert.equal(help.getAttribute('aria-expanded'),'true');
  assert.equal(help.nextElementSibling.hidden,false);assert.match(host.textContent,/Weighted quantile loss/);
  help.focus();render(host,job());
  assert.equal(host.querySelector('button').getAttribute('aria-expanded'),'true');
  assert.equal(dom.window.document.activeElement,host.querySelector('button'));
  assert.doesNotMatch(host.textContent,/Brier|ensemble accuracy|Median absolute error|Forecast bias|WAPE|Directional accuracy|R²|MASE|coverage/i);dom.window.close();
});
test('historical validation shows all four numeric cards with no future observations',()=>{
  const dom=new JSDOM('<section></section>'),host=dom.window.document.querySelector('section'),j=job();
  j.observations=[];
  j.historical_validation={status:'completed',holdout_rows:3,training_rows:40,metrics:{count:3,point_count:3,mae:1.25,rmse:2,smape:.05,average_wql:.2}};
  render(host,j);
  assert.deepEqual([...host.querySelectorAll('dd')].map(el=>el.textContent),['1.25','2','5.00%','0.2']);
  assert.equal(host.querySelectorAll('dl > div').length,4);
  assert.match(host.textContent,/Historical validation/);assert.match(host.textContent,/40 earlier training values/);
  assert.doesNotMatch(host.textContent,/Waiting for|Not available|Choose Reproduce/);
  dom.window.close();
});
test('validation failures, insufficient history and undefined metrics have specific explanations',()=>{
  const dom=new JSDOM('<section></section>'),host=dom.window.document.querySelector('section'),j=job();j.observations=[];
  for(const [status,reason] of [['failed',/could not complete/],['insufficient_history',/Not enough history/],['no_matching_outcomes',/no observed timestamps matched/]]) {
    j.historical_validation={status,minimum_training_rows:32};render(host,j);assert.match(host.textContent,reason);
    assert.equal(host.querySelectorAll('dd').length,4);assert.ok([...host.querySelectorAll('dd')].every(el=>el.textContent==='—'));
  }
  j.historical_validation={status:'completed',holdout_rows:3,training_rows:40,metrics:{count:3,point_count:0,mae:null,rmse:null,smape:null,average_wql:null}};
  render(host,j);assert.match(host.textContent,/P50 was not requested/);assert.match(host.textContent,/all zero/);
  dom.window.close();
});
