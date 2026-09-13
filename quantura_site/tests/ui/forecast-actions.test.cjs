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
  const d=new JSDOM('',{runScripts:'outside-only'});
  d.window.eval(source.slice(source.indexOf('  const ensembleMinuteSignals ='),source.indexOf('  const renderEnsembleSignals ='))+'\nwindow.signals=ensembleMinuteSignals;');
  const job={source:{type:'prediction_market'},frequency:'1min',completed_at:minute+'01:30Z',
    history:[row(1,.3)],predictions:[2,3,4,5].map(m=>({...row(m,0),quantiles:{'0.1':.2,'0.9':.8}})),
    observations:[row(2,.5),row(3,.85),row(4,.15)]};
  return {d,job,run:j=>d.window.signals(j,Date.parse(minute+'05:00Z'))};
}

test('one-minute bid crossings produce P90 buy and P10 sell events',()=>{
  const {d,job,run}=setup(), r=run(job);
  assert.deepEqual(Array.from(r.events,e=>e.kind),['buy','sell']);
  assert.equal(r.basis,'selected-side closing bid');
  assert.equal(Date.parse(r.events[0].timestamp),Date.parse(minute+'03:00Z'));d.window.close();
});

test('ticks, future quotes, gaps, absent bid, unpublished curves and unchanged-above do not fabricate signals',()=>{
  const {d,job,run}=setup();
  assert.equal(run({...job,observations:[row(2,.5),row(4,.85)]}).events.length,0);
  assert.equal(run({...job,observations:[row(2,.5),{...row(3,.9),timestamp:minute+'03:15Z'}]}).events.length,0);
  assert.equal(run({...job,observations:[row(2,.85),row(3,.95)]}).events.length,0);
  assert.equal(run({...job,observations:[row(2,.5),row(3,.5,.9)]}).events.length,0); // ask is not bid
  assert.equal(run({...job,observations:job.observations.map(({bid,...r})=>r)}).events.length,0);
  assert.equal(run({...job,completed_at:minute+'04:30Z'}).events.length,0);
  assert.equal(run({...job,frequency:'1h'}).available,false);
  assert.equal(run({...job,observations:[row(2,.5),{...row(3,.9),is_forward_filled:true}]}).events.length,0);
  d.window.close();
});
