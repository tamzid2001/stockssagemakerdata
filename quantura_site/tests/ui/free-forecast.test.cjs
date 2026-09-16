const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {execFileSync} = require('node:child_process');
const {JSDOM} = require('jsdom');
const root = path.resolve(__dirname,'../..');
const helper = require('../../public/forecast-controls.js');
test('first-row thresholds compare with the first minute AFTER completion, not a backdated quote',()=>{
  const job={frequency:'1min',source:{type:'prediction_market'},completed_at:'2026-09-16T10:04:12Z',predictions:[{timestamp:'2026-09-16T10:01:00Z',quantiles:{'0.1':.2,'0.9':.8}}],observations:[{timestamp:'2026-09-16T10:01:00Z',target:.1},{timestamp:'2026-09-16T10:05:00Z',target:.15}]};
  const now=Date.parse('2026-09-16T10:06:00Z');
  const result=helper.firstRowSignal(job,now);
  assert.equal(result.signal,'buy');assert.equal(result.prospective,true);
  assert.equal(result.quoteTimestamp,Date.parse('2026-09-16T10:05:00Z'));
  for(const [price,signal] of [[.85,'sell'],[.2,'none'],[.8,'none'],[.5,'none']]){job.observations[1].target=price;assert.equal(helper.firstRowSignal(job,now).signal,signal);}
  job.observations[1].is_forward_filled=true;assert.equal(helper.firstRowSignal(job,now).status,'waiting');
  job.observations[1]={timestamp:'2026-09-16T10:06:00Z',target:.1};assert.equal(helper.firstRowSignal(job,now).status,'waiting');
});
test('calendar input uses the device timezone and rejects nonexistent DST times',()=>{
  const script = `const h=require(${JSON.stringify(path.join(root,'public/forecast-controls.js'))});console.log(JSON.stringify([h.localValue('2026-09-15T19:30:00Z'),h.localInstant('2026-09-15T15:30')]));try{h.localInstant('2026-03-08T02:30');process.exit(2)}catch{}`;
  const values = JSON.parse(execFileSync(process.execPath,['-e',script],{env:{...process.env,TZ:'America/New_York'},encoding:'utf8'}));
  assert.deepEqual(values,['2026-09-15T15:30','2026-09-15T19:30:00.000Z']);
});
test('CSV parser validates structure, bound, headers, finite values and column choices',()=>{
  const text='timestamp,value\n'+Array.from({length:40},(_,i)=>`2026-09-${String(i%28+1).padStart(2,'0')},${i+1}`).join('\n');
  const table=helper.parseCsv(text); assert.equal(helper.csvSeries(table,'timestamp','value').length,40);
  for(const bad of ['x,x\n1,2', 'date,value\n"unclosed,1',text+'\n2026-09-01,3,4']) assert.throws(()=>helper.parseCsv(bad));
  assert.throws(()=>helper.parseCsv(text,10));
  assert.throws(()=>helper.csvSeries(table,'timestamp','timestamp'));
  table.rows[0][1]=''; assert.throws(()=>helper.csvSeries(table,'timestamp','value'));
});
test('forecast source and SSR have calendar controls and real CSV upload, without Foundry navigation',()=>{
  for(const dir of ['pages','functions_ssr/templates']){
    const d=new JSDOM(fs.readFileSync(path.join(root,dir,'forecasting.html'),'utf8'));
    const doc=d.window.document;
    for(const id of ['ensemble-history-cutoff','ensemble-prediction-end']) assert.equal(doc.getElementById(id).type,'datetime-local');
    assert.equal(doc.getElementById('ensemble-csv-file').type,'file');
    assert.ok(doc.getElementById('ensemble-csv-help-open').getAttribute('aria-haspopup'));
    assert.equal(doc.querySelector('[data-panel="autopilot"]'),null);
    assert.equal(doc.querySelector('a[href="/pricing"]'),null);
    assert.ok(doc.querySelector('#ensemble-save-profile'));
    d.window.close();
  }
});
test('search and export panels are out of document flow with semantic colors',()=>{
  const styles=fs.readFileSync(path.join(root,'public/styles.css'),'utf8');
  const system=fs.readFileSync(path.join(root,'public/professional.css'),'utf8');
  assert.match(styles,/\.ensemble-toolbar-menu-items \{ position: absolute; z-index: 80/);
  assert.doesNotMatch(styles,/\.ensemble-toolbar-menu-items \{ position: static/);
  assert.match(system,/\.market-search-results \{ position: absolute; z-index: 100/);
  assert.match(system,/font-size: 16px/); // no automatic iOS input zoom
});
