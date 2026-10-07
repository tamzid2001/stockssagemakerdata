const {JSDOM}=require('jsdom');
const {readFileSync}=require('node:fs');
const test=require('node:test');
const assert=require('node:assert/strict');
const source=readFileSync('public/forecast-questions.js','utf8');
const job=()=>({forecast_id:'test-forecast',source:{type:'ticker',symbol:'TEST'},completed_at:'2026-10-06T00:00:00Z',history:[{timestamp:'2026-10-01T00:00:00Z',target:100},{timestamp:'2026-10-02T00:00:00Z',target:90}],predictions:[{timestamp:'2026-10-03T00:00:00Z',quantiles:{'0.01':80,'0.5':110,'0.99':140}}]});
const setup=()=>{const dom=new JSDOM('<section id="host"></section>',{runScripts:'outside-only',url:'https://quantura.studio/forecasting'});dom.window.eval(source);return {dom,w:dom.window,host:dom.window.document.getElementById('host')};};
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const response=()=>({data:{conversation_id:'5c71cfa6-57f8-4a69-941d-73cadfd6d263',question:'Median?',response:{heading:'Median outlook',answer:'Computed from saved values.',facts:[{label:'Final P50',value:110,kind:'forecast',timestamp:'2026-10-03T00:00:00Z'}]}}});
test('three AI cards, labeled question form and structured safe facts appear beneath a forecast',async()=>{
 const {w,host}=setup();const calls=[];const request=async(path,options)=>{calls.push({path,options});return response();};
 w.QuanturaForecastQA.attach(host,{job:job(),reference:{kind:'ensemble',id:'test'},request});
 assert.equal(host.querySelectorAll('.jev-suggestion').length,3);assert.equal(host.querySelectorAll('.jev-suggestion svg').length,3);assert.match(host.textContent,/Forecast.*History.*Evidence/s);
 assert.equal(host.querySelector('label').htmlFor,host.querySelector('textarea').id);
 host.querySelector('.jev-suggestion').click();await tick();
 assert.equal(calls[0].path,'/api/v1/jev/forecast-questions');assert.equal(calls[0].options.body.context.id,'test');assert.equal(calls[0].options.body.predictions,undefined);
 assert.match(host.querySelector('[role=log]').textContent,/Final P50.*110/s);assert.match(host.textContent,/Saved in Requests/);assert.equal(host.querySelector('textarea').value,'');
});
test('failed questions keep their text and idempotency key for retry, with accessible error state',async()=>{
 const {w,host}=setup();let attempts=0;const turns=[];
 w.QuanturaForecastQA.attach(host,{job:job(),reference:{kind:'ensemble',id:'test'},request:async(_path,options)=>{turns.push(options.body.turn_id);if(++attempts===1)throw Error('Jev unavailable');return response();}});
 const input=host.querySelector('textarea'),form=host.querySelector('form');input.value='What is P50?';form.requestSubmit();await tick();
 assert.equal(input.value,'What is P50?');assert.equal(input.getAttribute('aria-invalid'),'true');assert.equal(host.querySelector('[role=alert]').textContent,'Jev unavailable');assert.equal(host.querySelector('.jev-conversation').children.length,0);
 form.requestSubmit();await tick();assert.equal(turns[0],turns[1]);assert.equal(input.hasAttribute('aria-invalid'),false);assert.equal(host.querySelector('.jev-conversation').children.length,1);
});
test('changing markets aborts the old context and ignores a delayed response',async()=>{
 const {w,host}=setup();let resolve,signal;const promise=new Promise(r=>resolve=r);
 w.QuanturaForecastQA.attach(host,{job:job(),reference:{kind:'ensemble',id:'old'},request:(_path,options)=>{signal=options.signal;return promise;}});
 host.querySelector('textarea').value='Median?';host.querySelector('form').requestSubmit();
 w.dispatchEvent(new w.CustomEvent('quantura:market-selected',{detail:{intent:'forecast'}}));assert.equal(signal.aborted,true);
 w.QuanturaForecastQA.attach(host,{job:{...job(),forecast_id:'new'},reference:{kind:'ensemble',id:'new'},request:async()=>response()});resolve(response());await tick();
 assert.equal(host.querySelector('.jev-conversation').children.length,0);assert.equal(host.__jevState.reference.id,'new');assert.equal(host.hidden,false);
});
test('notes anchor to real points, escape chart HTML, and write only on explicit Save',async()=>{
 const {w,host}=setup();const calls=[],chart={data:[]},annotationChanges=[];w.Plotly={relayout:async(_chart,layout)=>annotationChanges.push(layout.annotations)};
 const j={...job(),frequency:'1D'};w.QuanturaForecastQA.attach(host,{job:j,reference:{kind:'ensemble',id:'test'},chart,request:async(path,options)=>{calls.push({path,options});return {data:{notes:[]}};}});await tick();
 const note=host.querySelector('.jev-note-form input'),form=host.querySelector('.jev-note-form');note.value='<img src=x onerror=alert(1)>';form.requestSubmit();
 assert.equal(calls.filter(c=>c.path.includes('/save')).length,0);assert.equal(host.querySelectorAll('img').length,0);
 assert.match(host.querySelector('.jev-notes-list').textContent,/Oct 1, 2026/);assert.doesNotMatch(host.querySelector('.jev-notes-list').textContent,/Sep 30|PM/);
 const annotations=w.QuanturaForecastQA.plotAnnotations(j);assert.match(annotations[0].text,/&lt;img/);assert.equal(annotations[0].y,100);
 host.querySelector('.forecast-note-controls>button').click();await tick();const save=calls.find(c=>c.path.endsWith('/save'));assert.equal(save.options.body.notes[0].value,undefined);assert.equal(save.options.body.notes[0].series,'history');
 host.querySelector('.jev-high-low input').click();assert.equal(w.QuanturaForecastQA.plotAnnotations(j).length,5);
});
test('saved conversations render safely and link to the exact published snapshot',async()=>{
 const {w}=setup();w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};
 await w.QuanturaForecastQA.restore('id',async()=>({data:{title:'Saved',context_reference:{kind:'screener',symbol:'AAPL',scan_id:'2026-10-06-scan'},messages:[{question:'<script>bad()</script>',response:{heading:'Answer',answer:'Safe saved text',facts:[]}}]}}));
 assert.equal(w.document.querySelectorAll('script').length,0);assert.match(w.document.querySelector('dialog').textContent,/<script>bad\(\)<\/script>/);
 assert.equal(new URL(w.document.querySelector('dialog a').href).searchParams.get('screenerScan'),'2026-10-06-scan');
});
