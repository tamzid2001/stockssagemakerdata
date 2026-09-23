const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const controls = require('../../public/forecast-controls.js');
const app = fs.readFileSync(path.resolve(__dirname,'../../public/app.js'),'utf8');

test('forecast controls expose observations and quantiles without trade classifications',()=>{
  assert.equal(controls.cutoffP99Signal,undefined);
  assert.equal(controls.firstRowSignal,undefined);
  assert.equal(typeof controls.firstRowObservation,'function');
});
test('forecast action shows a single progress label without the recent high/low action',()=>{
  const page=fs.readFileSync(path.resolve(__dirname,'../../pages/forecasting.html'),'utf8');
  assert.doesNotMatch(page,/ensemble-find-signal|Find recent high \/ low/);
  const dom=new JSDOM('<button id="run">Run forecast</button>',{runScripts:'outside-only'});
  const w=dom.window;
  w.ui={ensembleRunButton:w.document.getElementById('run')};w.ensembleUiState={};
  w.eval(app.slice(app.indexOf('  const setEnsembleBusy ='),app.indexOf('  const ensembleDistributionSummary ='))+'\nwindow.busy=setEnsembleBusy;');
  w.busy(true);assert.equal(w.document.body.textContent.match(/Forecast in progress/g).length,1);
  assert.equal(w.ui.ensembleRunButton.disabled,true);
  w.busy(false);assert.equal(w.ui.ensembleRunButton.disabled,false);assert.equal(w.ui.ensembleRunButton.textContent,'Run forecast');
  dom.window.close();
});
test('forecast form no longer starts a recent high/low search',()=>{
  assert.doesNotMatch(app,/ensembleFindSignal|ensemble_recent_signal_search_created/);
});
