const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const root = path.resolve(__dirname, '../..');
const app = fs.readFileSync(path.join(root, 'public/app.js'), 'utf8');
const tick = () => new Promise(resolve => setTimeout(resolve, 0));
function history() {
  const d = new JSDOM('<section data-my-requests-panel><input data-my-requests-search><button data-action="my-requests-refresh">Refresh</button><p data-my-requests-status></p><div data-my-requests-list></div></section>', {url:'https://quantura.studio/forecasting',runScripts:'outside-only'});
  const w=d.window;
  const rows=Array.from({length:120},(_,i)=>({id:`request_${i}`,title:`Saved ${i}`,ticker:i===119?'OLDEST':'PLTR',type:'forecast',updatedAtMs:120-i}));
  const calls=[];
  w.fetch=async url=>{calls.push(url);const start=Number(new URL(url,w.location).searchParams.get('cursor')||0);return {ok:true,json:async()=>({items:rows.slice(start,start+40),has_more:start+40<rows.length,next_cursor:start+40<rows.length?String(start+40):null})};};
  w.eval(`
    const state={user:{uid:'owner'},myRequests:[],myRequestsById:{},myRequestsLoadedAt:0,myRequestsPanelState:{}};
    const ui={myRequestsPanels:[document.querySelector('[data-my-requests-panel]')]};
    const hasSessionUser=()=>Boolean(state.user);
    const normalizeMyRequestType=value=>String(value||'');
    const normalizeMyRequestPublishedFilter=value=>value||'all';
    const buildApiAuthHeaders=async()=>({});const showToast=()=>{};
    const escapeHtml=value=>String(value).replace(/</g,'&lt;');const formatTimestamp=value=>String(value||'');const icon=()=>'';
    const MY_REQUEST_TYPE_LABELS={forecast:'Forecast'};const skeletonHtml=()=>'<span>Loading</span>';
    ${app.slice(app.indexOf('  const getMyRequestPanelStateKey ='),app.indexOf('  const upsertMyRequest ='))}
    window.historyTest={state,load:fetchMyRequestsList,render:renderMyRequestsPanels,remove:removeMyRequestFromState};
    bindMyRequestsPanels();
  `);
  const next=async()=>{w.document.querySelector('[data-requests-next]').click();await tick();await tick();};
  return {d,w,calls,next,api:w.historyTest};
}
test('My Requests reaches all 120 entries with lazy server cursors and cached Previous',async()=>{
  const {d,w,api,calls,next}=history();await api.load();
  const cards=()=>[...w.document.querySelectorAll('[data-my-requests-list] [data-request-id].order-card')].map(e=>e.dataset.requestId);
  const seen=[...cards()];assert.equal(seen.length,20);assert.equal(calls.length,1);
  w.document.querySelector('[data-request-select-all]').click();assert.equal(w.document.querySelectorAll('[data-request-select]:checked').length,20);
  for(let page=1;page<6;page++){await next();seen.push(...cards());assert.equal(w.document.querySelectorAll('[data-request-select]:checked').length,0);assert.equal(w.document.querySelector('[data-request-delete-selected]').disabled,true);}
  assert.equal(new Set(seen).size,120);assert.equal(seen.at(-1),'request_119');assert.equal(calls.length,3);
  assert.match(calls[1],/cursor=40/);assert.match(calls[2],/cursor=80/);
  assert.equal(w.document.querySelector('[data-requests-next]').disabled,true);
  w.document.querySelector('[data-requests-previous]').click();assert.equal(cards()[0],'request_80');assert.equal(calls.length,3);
  const search=w.document.querySelector('[data-my-requests-search]');search.value='OLDEST';search.dispatchEvent(new w.Event('input'));
  assert.deepEqual(cards(),['request_119']);assert.equal(w.document.querySelector('[data-requests-previous]').disabled,true);
  api.remove('request_119');api.render();assert.equal(cards().length,0);assert.match(w.document.querySelector('[data-my-requests-list]').textContent,/No requests/);
  d.window.close();
});
test('search can continue beyond an empty loaded page, refresh resets paging, sign-out clears it',async()=>{
  const {d,w,api,next,calls}=history();await api.load();
  const search=w.document.querySelector('[data-my-requests-search]');search.value='OLDEST';search.dispatchEvent(new w.Event('input'));
  assert.match(w.document.querySelector('[data-my-requests-status]').textContent,/Next searches older/);
  await next();await next();assert.match(w.document.querySelector('[data-my-requests-list]').textContent,/Saved 119/);assert.equal(calls.length,3);
  await api.load({force:true});assert.equal(api.state.myRequests.length,40);assert.equal(api.state.myRequestsNextCursor,'40');
  api.state.user=null;await api.load();api.render();assert.equal(api.state.myRequests.length,0);assert.equal(api.state.myRequestsNextCursor,null);assert.equal(w.document.querySelector('[data-my-requests-pagination]').hidden,true);
  d.window.close();
});
test('My Requests defaults to all types and offers the current request categories',()=>{
  for(const dir of ['pages','functions_ssr/templates']){
    const html=fs.readFileSync(path.join(root,dir,'forecasting.html'),'utf8');
    assert.doesNotMatch(html,/data-my-requests-panel data-default-type/);
    if(dir.includes("templates")){
      const document=new JSDOM(html).window.document;
      const values=[...document.querySelectorAll("#profile-requests [data-my-requests-type] option")].map(option=>option.value);
      assert.deepEqual(values,["","forecast","download","csv","jev"]);
    }
  }
});

test('legacy response records use Scout and retired Indicator requests stay out of the list',async()=>{
  const {d,w,api}=history();await api.load();
  api.state.myRequests=[{id:'retired',type:'indicator',title:'Old indicator'},{id:'reply',type:'jev',title:'AAPL Model Council',outputsMeta:{answer:'Historical range analysis'}}];
  api.render();assert.doesNotMatch(w.document.querySelector('[data-my-requests-list]').textContent,/Old indicator|Model Council/);
  assert.match(w.document.querySelector('[data-my-requests-list]').textContent,/AAPL Scout/);
  const normalize=app.slice(app.indexOf('  const normalizeMyRequestType ='),app.indexOf('  const normalizeMyRequestVisibility ='));
  w.eval(`${normalize}window.normalizeType=normalizeMyRequestType;`);
  for(const type of ['modelCouncil','model_council','model-council','jev'])assert.equal(w.normalizeType(type),'jev');
  assert.equal(w.normalizeType('indicator'),'');d.window.close();
});

test('CSV preview saves a named private resource once and retains columns for reopening',async()=>{
  const markup=fs.readFileSync(path.join(root,'pages/forecasting.html'),'utf8');
  const d=new JSDOM(markup,{url:'https://quantura.studio/forecasting',runScripts:'outside-only'}),w=d.window,calls=[],records=[];
  w.QuanturaForecastControls=require(path.join(root,'public/forecast-controls.js'));
  w.apiRequestJson=async(path,options)=>{calls.push({path,...options});return {data:{id:'csv_test'}};};
  w.upsertMyRequest=async record=>records.push(record);
  const helper=app.slice(app.indexOf('  const applyEnsembleCsvPreview ='),app.indexOf('  const bindEnsembleForecastUi ='));
  const start=app.indexOf('    document.getElementById("ensemble-csv-save")?.addEventListener');
  const save=app.slice(start,app.indexOf('    const csvHelp =',start));
  w.eval(`const ensembleUiState={};const requireFullAccount=()=>true;${helper}${save}window.csvTest={state:ensembleUiState,preview:applyEnsembleCsvPreview};`);
  const text='time,close,other\n2026-01-01,100,1\n2026-01-02,101,2\n';
  w.csvTest.preview(text,'Strategy input.csv');
  assert.equal(calls.length,0,'preview must not upload private content without Save');
  assert.match(w.document.getElementById('ensemble-csv-preview-table').textContent,/101/);
  w.document.getElementById('ensemble-csv-target').value='other';
  w.document.getElementById('ensemble-csv-save').click();await tick();await tick();
  assert.equal(calls[0].path,'/api/v1/uploads/csv');assert.equal(calls[0].body.csv_text,text);
  assert.equal(records[0].title,'Strategy input');assert.equal(records[0].input.target_column,'other');
  assert.equal(records[0].sourceRef.id,'csv_test');assert.equal('csv_text' in records[0].input,false);
  w.document.getElementById('ensemble-csv-save').click();await tick();await tick();assert.equal(calls.length,1,'repeated Save reuses the existing stored file');
  d.window.close();
});

test('reopening a CSV request uses authenticated storage and restores its selected columns',async()=>{
  const d=new JSDOM(fs.readFileSync(path.join(root,'pages/forecasting.html'),'utf8'),{url:'https://quantura.studio/forecasting?panel=profile',runScripts:'outside-only'}),w=d.window,calls=[];
  w.QuanturaForecastControls=require(path.join(root,'public/forecast-controls.js'));
  w.fetch=async(url,options)=>{calls.push({url,options});return {ok:true,text:async()=>'date,close,volume\n2026-01-01,100,3\n2026-01-02,101,5\n'};};
  const helper=app.slice(app.indexOf('  const applyEnsembleCsvPreview ='),app.indexOf('  const bindEnsembleForecastUi ='));
  const load=app.slice(app.indexOf('  const loadMyRequestIntoUi ='),app.indexOf('  const loadSharedMyRequestFromUrl ='));
  w.__quanturaSetPanel=(panel,options)=>calls.push({panel,options});
  w.eval(`const ensembleUiState={},ui={};const hasSessionUser=()=>true;const normalizeMyRequestType=value=>value;const mapMyRequestTypeToPanel=()=>"forecast";const buildApiAuthHeaders=async()=>({Authorization:"Bearer synthetic-test"});${helper}${load}window.loadCsv=loadMyRequestIntoUi;`);
  const request={id:'csv__csv_test',type:'csv',title:'Research series',sourceRef:{collection:'uploaded_csvs',id:'csv_test'},input:{date_column:'date',target_column:'volume',frequency:'1h'}};
  await w.loadCsv({request});
  assert.equal(calls[0].panel,'forecast');assert.equal(calls[0].options.pushPath,true);
  assert.equal(calls[1].url,'/api/v1/uploads/csv/csv_test/download');assert.equal(calls[1].options.headers.Authorization,'Bearer synthetic-test');
  assert.equal(w.document.getElementById('ensemble-csv-name').value,'Research series');
  assert.equal(w.document.getElementById('ensemble-csv-target').value,'volume');
  assert.match(w.document.getElementById('ensemble-csv-preview-table').textContent,/101/);
  w.fetch=async()=>({ok:false});await assert.rejects(w.loadCsv({request}),/unavailable|access/);d.window.close();
});
