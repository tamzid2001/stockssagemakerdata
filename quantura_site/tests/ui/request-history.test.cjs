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
test('My Requests does not hide types behind an implicit forecast filter or Type dropdown',()=>{
  for(const dir of ['pages','functions_ssr/templates']){
    const html=fs.readFileSync(path.join(root,dir,'forecasting.html'),'utf8');
    assert.doesNotMatch(html,/data-my-requests-type|data-my-requests-panel data-default-type/);
  }
});
