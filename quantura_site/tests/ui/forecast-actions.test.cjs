const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const root = path.resolve(__dirname, '../..');
const source = fs.readFileSync(path.join(root,'public/app.js'),'utf8');

test('every forecast action has one icon-labelled control above the chart, in both templates',()=>{
  for (const folder of ['pages','functions_ssr/templates']) {
    const d=new JSDOM(fs.readFileSync(path.join(root,folder,'forecasting.html'),'utf8'));
    const doc=d.window.document, chart=doc.getElementById('ensemble-forecast-chart');
    for(const suffix of ['share-link','refresh-latest','download-csv','download-json','copy-config','run-again','check-status','request-previous','request-next','other-side','chart-refresh','chart-focus']) {
      const nodes=doc.querySelectorAll('#ensemble-'+suffix);assert.equal(nodes.length,1);
      const button=nodes[0];assert.ok(button.closest('.ensemble-chart-toolbar'));
      assert.ok(button.querySelector('i[aria-hidden="true"]'));assert.ok(button.querySelector('span').textContent.trim());
      assert.ok(button.compareDocumentPosition(chart)&d.window.Node.DOCUMENT_POSITION_FOLLOWING);
    }
    assert.ok(doc.querySelector('details.ensemble-toolbar-menu > summary'));
    d.window.close();
  }
});

const minute = '2026-09-13T12:';
const row=(m,bid,target=bid)=>({timestamp:minute+String(m).padStart(2,'0')+':00Z',bid,target});
function setup(){
  const d=new JSDOM('<section id="ensemble-first-row-signal"></section><table><tbody><tr><td>First row</td></tr></tbody></table>',{runScripts:'outside-only'});
  const helper = require(path.join(root,'public/forecast-controls.js'));
  d.window.QuanturaForecastControls={firstRowSignal:job=>helper.firstRowSignal(job,Date.parse(minute+'05:00Z'))};
  d.window.eval('const escapeHtml=String,ensembleLocalTime=String,ui={ensembleResultTable:document.querySelector("table")};'+source.slice(source.indexOf('  const renderEnsembleSignals ='),source.indexOf('  const renderEnsembleLiveQuote ='))+'\nwindow.signals=renderEnsembleSignals;');
  const job={source:{type:'prediction_market'},frequency:'1min',completed_at:minute+'01:30Z',
    history:[row(1,.3)],predictions:[2,3,4,5].map(m=>({...row(m,0),quantiles:{'0.1':.2,'0.9':.8}})),
    observations:[row(2,.5),row(3,.85),row(4,.15)]};
  return {d,job,run:j=>{d.window.signals(j);return d.window.document.getElementById('ensemble-first-row-signal').textContent;}};
}

test('only the first completed quote displays Buy below P10, Sell above P90, otherwise Neutral',()=>{
  const {d,job,run}=setup();
  for(const [price,label] of [[.1,'BUY — below P10'],[.85,'SELL — above P90'],[.5,'NEUTRAL'],[.2,'NEUTRAL'],[.8,'NEUTRAL']]) {
    job.observations[0]=row(2,price);
    const text=run(job);assert.ok(text.includes(label));
    assert.doesNotMatch(text,/Cross upward|Cross downward|P90 · Buy|P10 · Sell/);
    assert.equal(d.window.document.querySelectorAll('.first-row-signal-badge').length,1);
  }
  d.window.close();
});

test('later crossings, ticks, missing first quote and filled bars do not manufacture first-quote signals',()=>{
  const {d,job,run}=setup();
  assert.match(run(job),/NEUTRAL/); // Later P90/P10 crossings cannot change the first quote.
  for(const changed of [
    {...job,observations:[row(3,.9)]},
    {...job,observations:[{...row(2,.9),timestamp:minute+'02:15Z'}]},
    {...job,observations:[{...row(2,.9),is_forward_filled:true}]},
  ]) assert.doesNotMatch(run(changed),/BUY —|SELL —|NEUTRAL/);
  for(const completed_at of [minute+'04:30Z',null]) assert.match(run({...job,completed_at}),/NEUTRAL[\s\S]*retrospective/);
  assert.doesNotMatch(source,/ensembleMinuteSignals|Cross upward|Cross downward/);
  for(const folder of ['pages','functions_ssr/templates']) assert.doesNotMatch(fs.readFileSync(path.join(root,folder,'forecasting.html'),'utf8'),/ensemble-crossing-signals/);
  d.window.close();
});
