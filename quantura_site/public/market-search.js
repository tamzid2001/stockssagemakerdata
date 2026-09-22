(() => {
  "use strict";
  const form=document.getElementById("market-search-form"),queryInput=document.getElementById("market-search-query"),status=document.getElementById("market-search-status"),results=document.getElementById("market-search-results");
  if(!form||!queryInput||!status||!results)return;
  const workspace=form.closest(".market-search-workspace")||form.parentElement;
  const resources=new Map(),cache=new Map();let timer,controller,sequence=0,mode="open",eventView=null,lastGroups={},lastErrors={};
  const escapeHtml=v=>String(v??"").replace(/[&<>'"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"})[c]);
  const providerLabel=s=>({alpaca:"Alpaca",yahoo:"Yahoo Finance",polymarket_us:"Polymarket US",kalshi:"Kalshi",kalshi_perps:"Kalshi Perpetuals"})[s]||s;
  queryInput.setAttribute("aria-controls","market-search-results");queryInput.setAttribute("aria-describedby","market-search-status");queryInput.setAttribute("aria-expanded","false");queryInput.maxLength=2048;
  function closeResults(){clearTimeout(timer);controller?.abort();controller=null;++sequence;results.hidden=true;results.removeAttribute("aria-busy");queryInput.setAttribute("aria-expanded","false");}
  const outside=e=>{if(!workspace.contains(e.target))closeResults();};
  document.addEventListener("pointerdown",outside,true);document.addEventListener("click",outside,true);document.addEventListener("focusin",outside);
  window.addEventListener("quantura:panel-changed",closeResults);
  workspace.addEventListener("keydown",event=>{if(event.key==="Escape"){event.preventDefault();closeResults();queryInput.focus();}});
  function setPanel(panel){if(window.__quanturaSetPanel)window.__quanturaSetPanel(panel);else window.location.href=`/forecasting?panel=${encodeURIComponent(panel)}`;}
  function eventId(row){return row.source==="polymarket_us"?row.event_slug:row.event_id;}
  function card(row){
    resources.set(row.resource_id,row);const prediction=row.resource_type==="prediction_market_contract";
    const forecast=row.forecast_available&&!['closed','settled'].includes(row.status);
    return `<article class="market-search-result" data-market-resource="${escapeHtml(row.resource_id)}"><div class="market-search-result-main"><div class="market-search-result-symbol">${escapeHtml(prediction?row.outcome||row.side:row.symbol)}</div><div><strong>${escapeHtml(row.market_title||row.name||row.symbol)}</strong><div class="small muted">${escapeHtml([prediction?row.contract?.eventTitle:null,row.symbol,providerLabel(row.source),row.exchange,row.market_group,row.side,row.timing||row.status,row.unit].filter(Boolean).join(" · "))}</div></div></div><div class="market-search-result-actions">
      ${forecast?`<button class="cta small" type="button" data-market-action="${prediction?'prediction-forecast':'forecast'}">Forecast</button>`:""}
      <button class="cta secondary small" type="button" data-market-action="${prediction?'prediction-download':'history'}">Download history</button>
      ${prediction?`<button class="cta secondary small" type="button" data-market-action="add-download">+ Export outcome</button>${eventId(row)?'<button class="cta secondary small" type="button" data-market-action="event">All event markets</button>':''}`:["equity","etf"].includes(row.asset_class)?'<button class="cta secondary small" type="button" data-market-action="options">Options</button>':''}
      </div></article>`;
  }
  function show(){results.hidden=false;queryInput.setAttribute("aria-expanded","true");}
  function render(groups,errors={}){
    resources.clear();show();
    results.innerHTML=Object.entries(groups).filter(([,rows])=>rows.length).map(([source,rows])=>`<section class="market-search-group"><h3>${escapeHtml(providerLabel(source))}</h3>${rows.map(card).join("")}</section>`).join("")+Object.keys(errors).map(s=>`<p class="notice small">${escapeHtml(providerLabel(s))}: temporarily unavailable.</p>`).join("");
    if(!results.innerHTML)results.innerHTML='<div class="empty-state">No supported market matched this search. Try a ticker or an exact event link.</div>';
  }
  async function get(url,signal){
    const saved=cache.get(url);if(saved&&Date.now()-saved.at<60000)return saved.payload;
    const response=await fetch(url,{headers:{Accept:"application/json"},signal}),payload=await response.json();
    if(!response.ok)throw Error(payload.message||"Search is temporarily unavailable.");
    cache.set(url,{at:Date.now(),payload});if(cache.size>20)cache.delete(cache.keys().next().value);return payload;
  }
  async function search(rank=false){
    clearTimeout(timer);controller?.abort();const run=++sequence,query=queryInput.value.trim();eventView=null;
    if(query.length<2&&mode!=="live"){closeResults();status.textContent="Enter at least two characters to search markets.";return;}
    controller=new AbortController();results.setAttribute("aria-busy","true");status.textContent="Searching configured providers…";
    try{
      const link=/^https?:\/\//i.test(query);
      const params=new URLSearchParams(link?{url:query}:{q:query,source:"auto",limit:"20",mode,...(rank&&/\s/.test(query)?{rank:"true"}:{})});
      const payload=await get(`${link?'/api/market-search/resolve':'/api/market-search'}?${params}`,controller.signal);if(run!==sequence)return;
      lastGroups=payload.groups||{};lastErrors=payload.errors||{};render(lastGroups,lastErrors);
      status.textContent=`${Number(payload.count||0).toLocaleString()} results · Choose the exact outcome. ${payload.coverage||"Arrow keys to explore; Escape to close."}`;
      if(link){const first=Object.values(lastGroups).flat().find(r=>eventId(r));if(first)void openEvent(first.source,eventId(first));}
    }catch(error){if(error.name!=="AbortError"&&run===sequence){show();results.innerHTML='<div class="empty-state">Unable to search. Retry or paste an exact provider event link.</div>';status.textContent=error.message;}}
    finally{if(run===sequence)results.removeAttribute("aria-busy");}
  }
  function renderEvent(){
    const v=eventView;if(!v)return;resources.clear();show();
    const rows=[...v.rows.values()],groups=[...new Set(rows.map(r=>r.market_group||"Other markets"))];
    results.innerHTML=`<div class="q-event-toolbar"><button type="button" class="cta secondary small" data-market-action="back">← Results</button><strong>${escapeHtml(v.title)}</strong><span class="small">${rows.length} outcomes loaded</span><input type="search" id="q-event-filter" aria-label="Filter loaded event markets" placeholder="Filter loaded props, players, lines" value="${escapeHtml(v.filter||'')}" /></div>
      ${v.related.length?`<details class="q-event-group"><summary>Related market groups (${v.related.length})</summary><div class="q-event-groups">${v.related.map(r=>`<button class="task-chip" type="button" data-market-action="related" data-event-id="${escapeHtml(r.event_id)}">${escapeHtml(r.group)} · ${escapeHtml(r.label)}</button>`).join('')}</div></details>`:''}
      <p class="small muted">${escapeHtml(v.coverage||'')} ${v.relatedComplete===false?'Related-event discovery is partial; retry for more groups.':''}</p>
      ${groups.map(group=>`<details class="q-event-group" open><summary>${escapeHtml(group)} (${rows.filter(r=>(r.market_group||'Other markets')===group).length})</summary>${rows.filter(r=>(r.market_group||'Other markets')===group).map(card).join('')}</details>`).join('')}
      ${v.next?'<button class="cta secondary small" type="button" data-market-action="more">Load more event outcomes</button>':'<p class="small muted">All pages for this branch loaded. Other related branches open separately.</p>'}`;
    filterEvent(v.filter||'');
  }
  function filterEvent(text){if(eventView)eventView.filter=text;for(const el of results.querySelectorAll('[data-market-resource]'))el.hidden=!el.textContent.toLowerCase().includes(text.toLowerCase());}
  async function openEvent(source,id,more=false){
    controller?.abort();controller=new AbortController();const run=++sequence;
    if(!more)eventView={source,id,title:id,rows:new Map(),related:[],next:null,filter:""};
    const v=eventView;status.textContent="Loading provider-verified event markets…";results.setAttribute("aria-busy","true");
    try{
      const params=new URLSearchParams({source,event_id:id,...(more&&v.next?{cursor:v.next}:{})});
      const payload=await get(`/api/market-search/event?${params}`,controller.signal);if(run!==sequence)return;
      for(const row of Object.values(payload.groups||{}).flat())v.rows.set(row.resource_id,row);
      v.title=payload.title||id;v.related=payload.related_events||[];v.relatedComplete=payload.related_complete;v.next=payload.next_cursor;v.coverage=payload.coverage;renderEvent();
      status.textContent=`${v.rows.size} outcomes loaded · ${providerLabel(source)}. ${v.next?'More pages are available.':'Branch loaded.'} Choose a specific line and side.`;
    }catch(error){if(error.name!=="AbortError"&&run===sequence)status.textContent=error.message;}
    finally{if(run===sequence)results.removeAttribute("aria-busy");}
  }
  form.addEventListener("submit",event=>{event.preventDefault();void search(true);});
  queryInput.addEventListener("input",()=>{controller?.abort();++sequence;clearTimeout(timer);timer=setTimeout(()=>search(),300);});
  document.querySelectorAll("[data-market-mode]").forEach(button=>button.addEventListener("click",()=>{mode=button.dataset.marketMode;document.querySelectorAll("[data-market-mode]").forEach(b=>b.setAttribute("aria-pressed",String(b===button)));queryInput.required=mode!=="live";void search();}));
  queryInput.addEventListener("keydown",event=>{if(event.key==="ArrowDown"&&!results.hidden){event.preventDefault();results.querySelector('[data-market-action]')?.focus();}});
  results.addEventListener("input",event=>{if(event.target.id==="q-event-filter")filterEvent(event.target.value);});
  results.addEventListener("keydown",event=>{
    if(!["ArrowDown","ArrowUp","Home","End"].includes(event.key)||event.target.tagName==="INPUT")return;
    const buttons=[...results.querySelectorAll('[data-market-action]')].filter(b=>!b.closest('[hidden]')),index=buttons.indexOf(document.activeElement);if(index<0)return;
    event.preventDefault();if(event.key==="ArrowUp"&&index===0){queryInput.focus();return;}buttons[event.key==="Home"?0:event.key==="End"?buttons.length-1:Math.max(0,Math.min(buttons.length-1,index+(event.key==="ArrowDown"?1:-1)))]?.focus();
  });
  results.addEventListener("click",event=>{
    const button=event.target.closest('[data-market-action]');if(!button)return;
    const action=button.dataset.marketAction,row=resources.get(button.closest('[data-market-resource]')?.dataset.marketResource);
    if(action==="back"){closeResults();eventView=null;render(lastGroups,lastErrors);return;}
    if(action==="more"&&eventView){void openEvent(eventView.source,eventView.id,true);return;}
    if(action==="related"&&eventView){void openEvent(eventView.source,button.dataset.eventId);return;}
    if(action==="event"&&row){void openEvent(row.source,eventId(row));return;}
    if(!row)return;
    const intent=action.includes("forecast")?"forecast":action==="options"?"options":action==="add-download"?"add-download":"download";
    if(intent!=="add-download")closeResults();
    window.QuanturaMarketSelection=row;
    if(intent==="forecast"){
      const source=document.getElementById("ensemble-source-type"),ticker=document.getElementById("ensemble-ticker"),provider=document.getElementById("ensemble-provider");
      if(ticker)ticker.value=row.symbol;if(provider)provider.value="auto";
      if(source){source.value=row.contract_id?"prediction_market":row.source==="kalshi_perps"?"kalshi_perp":"ticker";source.dispatchEvent(new Event("change",{bubbles:true}));}
    }
    if(intent!=="add-download")setPanel(intent==="forecast"?"forecast":"download");
    window.dispatchEvent(new CustomEvent("quantura:market-selected",{detail:{resource:row,intent}}));
    status.textContent=`Selected ${row.outcome||row.symbol} · ${row.market_title||row.name} · ${providerLabel(row.source)}${intent==='add-download'?' · Added to Q Download.':''}.`;
  });
})();
