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

test('forecast pages and chart do not present trade-signal badges',()=>{
  for(const folder of ['pages','functions_ssr/templates']) {
    const html=fs.readFileSync(path.join(root,folder,'forecasting.html'),'utf8');
    assert.doesNotMatch(html,/ensemble-first-row-signal|ensemble-cutoff-p99-signal|ensemble-crossing-signals/);
  }
  assert.doesNotMatch(source,/renderEnsembleSignals|first-row-signal-badge|BUY — below P10|SELL — above P90/);
  assert.match(source,/First observed quote/);
});
