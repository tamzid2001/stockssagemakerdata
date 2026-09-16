const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const source = fs.readFileSync(path.resolve(__dirname,'../../public/app.js'),'utf8');
const start = source.slice(source.indexOf('  const startEnsembleObservations ='),source.indexOf('  const renderCompletedEnsemble ='));

function setup() {
  const dom = new JSDOM('<button id="ensemble-chart-refresh"><i aria-hidden="true"></i><span>Refresh quotes</span></button>',{runScripts:'outside-only',pretendToBeVisual:true});
  const w=dom.window, timers=new Map();let sequence=0,calls=0;
  w.QuanturaForecastControls=require('../../public/forecast-controls.js');
  w.setTimeout=(fn,ms)=>{timers.set(++sequence,{fn,ms});return sequence;};
  w.clearTimeout=id=>timers.delete(id);
  w.ensembleUiState={forecastId:'f',observationGeneration:0,chartWindow:[0,100],busy:false};
  w.ui={ensembleForecastResults:{getClientRects:()=>[{}]},ensembleObservationStatus:{},ensembleObservedMetrics:{},ensembleForecastChart:{}};
  w.response={data:{rows:[{timestamp:'2026-09-13T12:02:00Z',target:.4}],observed_at:'2026-09-13T12:02:00Z',availability:'available'}};
  w.apiRequestJson=async()=>{calls++;return w.response;};
  w.renderEnsembleLiveQuote=()=>{};w.ensembleLocalTime=t=>t;
  w.ensembleChartDefaultRange=()=>[0,200];w.renderEnsembleChart=async()=>{};
  w.getPlotly=async()=>({relayout:async()=>{}});
  w.eval(start+'\nwindow.start=startEnsembleObservations;');
  const resume = source.slice(source.indexOf('    const resumeQuoteOverlay ='),source.indexOf('    window.addEventListener("quantura:market-selected"',source.indexOf('    const resumeQuoteOverlay =')));
  w.eval(resume);
  const job={forecast_id:'f',source:{type:'prediction_market'},completed_at:'2026-09-13T12:00:00Z',predictions:[]};
  w.start(job);
  return {w,dom,timers,job,calls:()=>calls};
}

test('tab return immediately refreshes overlay without a page reload',async()=>{
  const s=setup();
  Object.defineProperty(s.w.document,'hidden',{configurable:true,value:true});
  await s.w.ensembleUiState.refreshObservations();assert.equal(s.calls(),0);
  assert.equal([...s.timers.values()].at(-1).ms,60000);
  Object.defineProperty(s.w.document,'hidden',{configurable:true,value:false});
  s.w.document.dispatchEvent(new s.w.Event('visibilitychange'));
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(s.calls(),1);assert.equal(s.job.observations.length,1);
  assert.equal(s.w.document.getElementById('ensemble-chart-refresh').disabled,false);
  assert.ok(s.w.document.querySelector('#ensemble-chart-refresh i'));
  s.dom.window.close();
});

test('busy skip and transient errors retain automatic retry; manual refresh moves range',async()=>{
  const s=setup();s.w.ensembleUiState.busy=true;
  await s.w.ensembleUiState.refreshObservations();assert.equal(s.calls(),0);
  assert.equal(s.timers.size,1);
  s.w.ensembleUiState.busy=false;s.w.apiRequestJson=async()=>{throw new Error('offline');};
  await s.w.ensembleUiState.refreshObservations();assert.match(s.w.ui.ensembleObservationStatus.textContent,/Retrying automatically/);
  assert.equal(s.timers.size,1);
  s.w.apiRequestJson=async()=>s.w.response;
  await s.w.ensembleUiState.refreshObservations({focusLatest:true});
  assert.deepEqual(s.w.ensembleUiState.chartWindow,[0,200]);s.dom.window.close();
});

test('concurrent refresh deduplicates and old response cannot overwrite next job',async()=>{
  const s=setup();let finish,calls=0;
  s.w.apiRequestJson=()=>{calls++;return new Promise(resolve=>{finish=resolve;});};
  const pending=s.w.ensembleUiState.refreshObservations();
  await s.w.ensembleUiState.refreshObservations();assert.equal(calls,1);
  s.w.ensembleUiState.forecastId='new';
  s.w.start({...s.job,forecast_id:'new'});
  finish(s.w.response);await pending;
  assert.equal(s.job.observations,undefined);
  assert.equal(s.w.document.getElementById('ensemble-chart-refresh').disabled,false);
  s.dom.window.close();
});

test('chart refresh is distinct from compute and source/SSR controls are synchronized',()=>{
  for(const directory of ['pages','functions_ssr/templates']){
    const html=fs.readFileSync(path.resolve(__dirname,'../..',directory,'forecasting.html'),'utf8');
    assert.match(html,/id="ensemble-chart-refresh"[^>]+><i[^>]+><\/i><span>Refresh quotes/);
  }
  assert.match(source,/ensemble-chart-refresh[^\n]*addEventListener\("click"[^\n]*refreshObservations/);
});
