const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const controls = require('../../public/forecast-controls.js');
const app = fs.readFileSync(path.resolve(__dirname,'../../public/app.js'),'utf8');

test('input-close BUY is strict, independent of observed quote/averages, no P01 sell',()=>{
  const job={history:[{timestamp:'2026-09-18',target:131}],observations:[{timestamp:'2026-09-21',target:1}],predictions:[{timestamp:'2026-09-21',quantiles:{'0.01':20,'0.99':130}},{timestamp:'2026-09-22',quantiles:{'0.99':500}}]};
  assert.equal(controls.cutoffP99Signal(job).signal,'buy');
  for(const price of [130,129,10]) {job.history[0].target=price;assert.equal(controls.cutoffP99Signal(job).signal,'none');}
  delete job.predictions[0].quantiles['0.99'];assert.equal(controls.cutoffP99Signal(job).status,'unavailable');
  job.predictions[0].quantiles['0.99']=null;assert.equal(controls.cutoffP99Signal(job).status,'unavailable');
});
test('both actions disable but only the active action shows progress',()=>{
  const dom=new JSDOM('<button id="run">Run forecast</button><button id="find"><i></i><span>Find recent high / low</span></button>',{runScripts:'outside-only'});
  const w=dom.window;
  w.ui={ensembleRunButton:w.document.getElementById('run'),ensembleFindSignal:w.document.getElementById('find')};w.ensembleUiState={};
  w.eval(app.slice(app.indexOf('  const setEnsembleBusy ='),app.indexOf('  const ensembleDistributionSummary ='))+'\nwindow.busy=setEnsembleBusy;');
  w.busy(true);assert.equal(w.document.body.textContent.match(/Forecast in progress/g).length,1);
  assert.equal(w.ui.ensembleFindSignal.textContent,'Find recent high / low');assert.equal(w.ui.ensembleFindSignal.disabled,true);
  w.busy(false);w.ensembleUiState.busyAction='search';w.busy(true);
  assert.equal(w.ui.ensembleRunButton.textContent,'Run forecast');assert.match(w.ui.ensembleFindSignal.textContent,/Searching/);
  w.busy(false);assert.equal(w.ui.ensembleRunButton.disabled,false);assert.equal(w.ui.ensembleFindSignal.disabled,false);assert.ok(w.ui.ensembleFindSignal.querySelector('i'));
  dom.window.close();
});
test('new searches request P99 and explicitly version the cutoff rule',()=>{
  const begin=app.indexOf('    ui.ensembleFindSignal?.addEventListener');
  const handler=app.slice(begin,app.indexOf('    ui.ensembleForecastForm?.addEventListener',begin));
  assert.match(handler,/search_signal_rule:"cutoff_above_p99"/);assert.match(handler,/configured.quantiles,.99/);
});
test('recent-search result labels the input close instead of a withheld future quote',()=>{
  const dom=new JSDOM('<section id="ensemble-cutoff-p99-signal"></section><section id="ensemble-first-row-signal"></section>',{runScripts:'outside-only'});
  const w=dom.window;w.QuanturaForecastControls=controls;w.escapeHtml=String;w.ensembleLocalTime=String;
  const begin=app.indexOf('  const renderEnsembleSignals =');
  w.eval(app.slice(begin,app.indexOf('  const describeEnsembleCapability =',begin))+'\nwindow.render=renderEnsembleSignals;');
  w.render({recent_signal_search:{signal_rule:'cutoff_above_p99',status:'found',signal:'buy',cutoff_observation:{timestamp:'2026-09-18',price:131},thresholds:{p99:130},cutoffs_examined:1,max_cutoffs:20}});
  assert.match(w.document.getElementById('ensemble-first-row-signal').textContent,/BUY · input close above first P99/);
  assert.doesNotMatch(w.document.getElementById('ensemble-first-row-signal').textContent,/withheld|P10\/P90 breach/);
  assert.equal(w.document.getElementById('ensemble-cutoff-p99-signal').hidden,true);dom.window.close();
});
