(() => {
  "use strict";
  const byId=id=>document.getElementById(id), form=byId("q-download-form"), api=window.QuanturaQMarket;
  if(!form||!api)return;
  const html=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
  let selected=null,saved=null,controller=null,sequence=0,optionSequence=0,optionController=null,optionSymbol="";
  const basket=new Map();
  const value=id=>byId(`qd-${id}`).value;
  const status=text=>{byId("qd-status").textContent=text;};
  const local=d=>new Date(d.getTime()-d.getTimezoneOffset()*60000).toISOString().slice(0,16);
  byId("qd-end").placeholder="Now";
  byId("qd-start").value=local(new Date(Date.now()-7*86400000));
  byId("qd-timezone").textContent=Intl.DateTimeFormat().resolvedOptions().timeZone;
  function invalidate(message="Settings changed. Preview again before downloading."){
    controller?.abort();controller=null;++sequence;saved=null;
    byId("qd-csv").disabled=byId("qd-json").disabled=true;
    byId("qd-preview").disabled=false;byId("qd-preview").removeAttribute("aria-busy");byId("qd-cancel").hidden=true;
    byId("qd-preview-table").replaceChildren();status(message);
  }
  function settings(){return Object.fromEntries(["kind","range","frequency","limit","start","end","session","adjustment","phase","layout","target","missing"].map(k=>[k,value(k)]).concat([["optionSymbol",optionSymbol]]));}
  async function json(url,options={}){
    const response=await fetch(url,{...options,headers:{Accept:"application/json",...(options.body?{"Content-Type":"application/json"}:{})}});
    const data=await response.json();if(!response.ok||data.ok===false)throw Error(data.message||data.error?.message||"The provider could not return these observations.");return data;
  }
  function renderBasket(){
    byId("qd-basket").innerHTML=[...basket.values()].map(row=>`<button type="button" class="task-chip" data-remove="${html(row.resource_id)}" aria-label="Remove ${html(row.outcome)}">${html(row.outcome)} · ${html(row.market_title||row.symbol)} ×</button>`).join("");
  }
  function configure(){
    const prediction=selected?.resource_type==="prediction_market_contract",stock=selected&&["equity","etf"].includes(selected.asset_class);
    const largeLimit=[...byId("qd-limit").options].find(o=>o.value==="50000");
    if(largeLimit)largeLimit.disabled=selected?.source==="kalshi_perps";
    if(largeLimit?.disabled && value("limit")==="50000")byId("qd-limit").value="2000";
    byId("qd-kind").options[1].disabled=!stock;
    if(!stock)byId("qd-kind").value="history";
    byId("qd-options").hidden=value("kind")!=="options";
    document.querySelectorAll("[data-qd-stock]").forEach(el=>el.hidden=!stock||value("kind")==="options");
    document.querySelectorAll("[data-qd-prediction]").forEach(el=>el.hidden=!prediction);
    document.querySelectorAll("[data-qd-dates]").forEach(el=>el.hidden=value("range")!=="dates");
    document.querySelectorAll("[data-qd-latest]").forEach(el=>el.hidden=value("range")!=="latest");
    // These aggregations already exist in the prediction-market export service.
    const interval=byId("qd-frequency"),old=interval.value;
    interval.innerHTML=[...[['1Min','Minute'],['1Hour','Hourly'],['1Day','Daily']],...(prediction?[['raw','Raw observations / trades'],['5m','5 minutes'],['15m','15 minutes'],['30m','30 minutes'],['final','Final observation']]:[])].map(([v,label])=>`<option value="${v}">${label}</option>`).join("");
    interval.value=[...interval.options].some(o=>o.value===old)?old:"1Day";
  }
  async function loadChain(){
    optionController?.abort();optionController=new AbortController();const run=++optionSequence;
    optionSymbol="";byId("qd-option-chain").replaceChildren();invalidate();
    const expiration=value("expiration");if(!selected||!expiration)return;
    byId("qd-option-status").textContent="Loading verified option contracts…";
    try{
      const data=await json(`/api/market-data/options/chain?${new URLSearchParams({underlying:selected.symbol,source:"auto",expiration,type:value("option-type")})}`,{signal:optionController.signal});
      if(run!==optionSequence)return;
      byId("qd-option-status").textContent=`${data.contracts?.length||0} contracts · ${data.provider}. Select a contract; history availability may differ.`;
      byId("qd-option-chain").innerHTML=`<table><thead><tr><th>Contract</th><th>Type</th><th>Strike</th><th>Bid</th><th>Ask</th></tr></thead><tbody>${(data.contracts||[]).map(c=>`<tr><td><button type="button" class="cta secondary small" data-option="${html(c.symbol||c.contractSymbol)}">${html(c.symbol||c.contractSymbol)}</button></td><td>${html(c.type)}</td><td>${html(c.strikePrice??c.strike)}</td><td>${html(c.bid??"—")}</td><td>${html(c.ask??"—")}</td></tr>`).join("")}</tbody></table>`;
    }catch(error){if(run===optionSequence&&error.name!=="AbortError")byId("qd-option-status").textContent=error.message;}
  }
  async function loadExpirations(){
    optionController?.abort();optionController=new AbortController();const run=++optionSequence;
    optionSymbol="";byId("qd-option-chain").replaceChildren();byId("qd-expiration").replaceChildren();
    if(!selected||value("kind")!=="options")return;
    byId("qd-option-status").textContent="Loading available expirations…";
    try{
      const data=await json(`/api/market-data/options/expirations?${new URLSearchParams({underlying:selected.symbol,source:"auto"})}`,{signal:optionController.signal});
      if(run!==optionSequence)return;
      byId("qd-expiration").innerHTML=(data.expirations||[]).map(date=>`<option>${html(date)}</option>`).join("");
      if(data.expirations?.length)await loadChain();else byId("qd-option-status").textContent="No supported expirations were returned for this underlying.";
    }catch(error){if(run===optionSequence&&error.name!=="AbortError")byId("qd-option-status").textContent=error.message;}
  }
  window.addEventListener("quantura:market-selected",event=>{
    const row=event.detail?.resource;if(!api.validResource(row))return;
    const prediction=row.resource_type==="prediction_market_contract";
    const adding=event.detail.intent==="add-download";
    if(!adding||!prediction||selected?.source!==row.source)basket.clear();
    if(prediction){if(basket.size>=25&&!basket.has(row.resource_id)){status("Export at most 25 outcomes at once. Remove one before adding another.");return;}basket.set(row.resource_id,row);}
    const wasPrediction=selected?.resource_type==="prediction_market_contract";
    selected=row;optionSymbol="";++optionSequence;optionController?.abort();
    if(prediction&&!wasPrediction)byId("qd-frequency").value="1Min";
    byId("qd-selection").textContent=api.label(row);
    invalidate("Market selected. Preview to retrieve historical observations.");renderBasket();configure();
    if(event.detail.intent==="options"){byId("qd-kind").value="options";configure();}
    if(value("kind")==="options")void loadExpirations();
  });
  byId("qd-basket").addEventListener("click",event=>{
    const button=event.target.closest("[data-remove]");if(!button)return;
    basket.delete(button.dataset.remove);invalidate();renderBasket();
  });
  form.addEventListener("input",()=>invalidate());
  form.addEventListener("change",event=>{
    invalidate();configure();
    if(event.target.id==="qd-kind"){++optionSequence;optionController?.abort();if(value("kind")==="options")void loadExpirations();}
    if(["qd-expiration","qd-option-type"].includes(event.target.id))void loadChain();
  });
  byId("qd-option-chain").addEventListener("click",event=>{
    const button=event.target.closest("[data-option]");if(!button)return;
    optionSymbol=button.dataset.option;invalidate();
    byId("qd-option-chain").querySelectorAll("tr").forEach(tr=>tr.setAttribute("aria-selected",String(tr===button.closest("tr"))));
    byId("qd-option-status").textContent=`Selected ${optionSymbol}. Preview its history below.`;
  });
  form.addEventListener("submit",async event=>{
    event.preventDefault();invalidate();const run=sequence;
    try{
      const config=settings(),request=api.requestFor(selected,config,[...basket.values()].map(r=>r.contract));
      controller=new AbortController();byId("qd-preview").disabled=true;byId("qd-preview").setAttribute("aria-busy","true");byId("qd-cancel").hidden=false;status("Downloading real provider observations…");
      const payload=await json(request.url,{method:"POST",body:JSON.stringify(request.body),signal:controller.signal});
      if(run!==sequence)return;
      saved=api.snapshot(payload,selected,config,request);
      const {columns,rows,metadata}=saved;
      byId("qd-preview-table").innerHTML=rows.length?`<table><thead><tr>${columns.map(c=>`<th scope="col">${html(c)}</th>`).join("")}</tr></thead><tbody>${rows.slice(0,100).map(r=>`<tr>${columns.map(c=>`<td>${html(typeof r[c]==="object"?JSON.stringify(r[c]):r[c])}</td>`).join("")}</tr>`).join("")}</tbody></table>`:"";
      const times=rows.map(r=>r.timestamp).filter(Boolean).sort();
      const warnings=payload.validation?.messages||payload.metadata?.validation?.messages||payload.warnings||[];
      status(`${rows.length.toLocaleString()} rows · ${metadata.provider} · ${times[0]||"no start"} → ${times.at(-1)||"no end"} UTC. Preview shows up to 100 rows; exports include all ${rows.length}. ${warnings.join(" ")}`);
      byId("qd-csv").disabled=byId("qd-json").disabled=!rows.length;
    }catch(error){if(run===sequence&&error.name!=="AbortError")status(error.message||"History could not be downloaded.");}
    finally{if(run===sequence){byId("qd-preview").disabled=false;byId("qd-preview").removeAttribute("aria-busy");byId("qd-cancel").hidden=true;}}
  });
  byId("qd-cancel").addEventListener("click",()=>invalidate("Preview canceled. No download was saved."));
  for(const format of ["csv","json"])byId(`qd-${format}`).addEventListener("click",()=>{
    if(!saved)return;
    const body=format==="csv"?api.csv(saved):JSON.stringify(saved,null,2),url=URL.createObjectURL(new Blob([body],{type:format==="csv"?"text/csv;charset=utf-8":"application/json"}));
    const a=document.createElement("a");a.href=url;a.download=`quantura-${(selected?.symbol||"history").replace(/[^a-z0-9_-]/gi,"-")}.${format}`;document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
  });
  configure();
})();
