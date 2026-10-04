const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const code=fs.readFileSync(path.join(__dirname,'../../public/economic-data.js'),'utf8');
const tick=()=>new Promise(r=>setTimeout(r,10));
test('BigQuery selection sends authenticated exact columns/filters, previews returned timestamps, and follows the Download panel',async()=>{
 const dom=new JSDOM('<body><select id="ensemble-source-type"></select><form id="forecast"><div id="economic-forecast-controls"><div data-economic-fields></div><p data-economic-status></p><div data-economic-table></div><button data-economic-preview></button><button data-economic-download></button><a data-economic-link></a></div></form><section id="download"><div data-economic-download-settings></div></section></body>',{url:'https://quantura.studio/forecasting',runScripts:'outside-only'}),w=dom.window,calls=[];
 w.QuanturaAuth={signedIn:true,getToken:async()=> 'test-session'};w.QuanturaQMarket={snapshot:d=>d,csv:()=>''};
 w.fetch=async(url,options)=>{calls.push({url,options});const source=JSON.parse(options.body);return {ok:true,json:async()=>url.endsWith('describe')?{name:'Daily observations',forecastable:true,time_fields:[{field:'date',label:'Date'}],values:[{field:'value',label:'Value'}],filter_fields:[{field:'station',type:'STRING'}],dimensions:[],frequency:'1D',url:'https://console.cloud.google.com/bigquery'}:{source,frequency:'1D',rows:[{timestamp:'2025-01-01T00:00:00Z',target:12}],metadata:{units:'provider units'},warnings:[]}};};
 w.eval(code);
 const resource={resource_type:'economic_series',source:'bigquery',symbol:'weather.daily',economic_source:{type:'economic_series',provider:'bigquery',dataset_id:'weather',table_id:'daily'}};
 w.dispatchEvent(new w.CustomEvent('quantura:market-selected',{detail:{resource,intent:'download'}}));await tick();
 const host=w.document.getElementById('economic-forecast-controls');assert.equal(host.parentElement.dataset.economicDownloadSettings,'');assert.equal(host.hidden,false);
 const filter=host.querySelector('[data-bigquery-filter]');filter.querySelector('select').value='station';filter.querySelector('input').value='725030';
 host.querySelector('[data-economic-preview]').click();await tick();
 const request=JSON.parse(calls.at(-1).options.body);assert.equal(request.time_field,'date');assert.equal(request.value_field,'value');assert.deepEqual(request.dimensions,{station:'725030'});assert.equal(request.aggregation,'none');assert.equal(calls.at(-1).options.headers.Authorization,'Bearer test-session');
 assert.match(host.querySelector('[data-economic-table]').textContent,/2025-01-01/);
 w.dispatchEvent(new w.CustomEvent('quantura:panel-changed',{detail:{panel:'forecast'}}));assert.equal(host.parentElement.id,'forecast');
 w.dispatchEvent(new w.CustomEvent('quantura:market-selected',{detail:{resource:{source:'alpaca',resource_type:'equity'},intent:'forecast'}}));
 assert.equal(host.hidden,true);assert.throws(()=>w.QuanturaEconomics.source(),/Choose an economic/);dom.window.close();
});
