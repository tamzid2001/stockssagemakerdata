(function (root, factory) {
  const api=factory(); if(typeof module==="object" && module.exports) module.exports=api;
  if(!root?.document) return;
  root.QuanturaQMarket=api;
  // One public selected identity. No account data or API credentials in storage.
  root.addEventListener("quantura:market-selected", event=>{
    const row=event.detail?.resource; if(!api.validResource(row)) return;
    root.QuanturaMarketSelection=row;
    const source=root.document.getElementById("ensemble-source-type"),ticker=root.document.getElementById("ensemble-ticker"),provider=root.document.getElementById("ensemble-provider");
    if(ticker)ticker.value=row.symbol;
    if(provider)provider.value="auto";
    if(source){source.value=row.contract_id?"prediction_market":row.source==="kalshi_perps"?"kalshi_perp":"ticker";source.dispatchEvent(new root.Event("change",{bubbles:true}));}
    const chip=root.document.getElementById("q-selected-market");
    if(chip) { chip.replaceChildren(); const label=root.document.createElement("strong"); label.textContent=api.label(row); chip.append(label);
      const change=root.document.createElement("button");change.type="button";change.className="cta secondary small";change.textContent="Change";
      change.onclick=()=>root.document.getElementById("market-search-query")?.focus();chip.append(change);
      const upload=root.document.createElement("button");upload.type="button";upload.className="cta secondary small";upload.textContent="Upload CSV";upload.dataset.qUpload="true";chip.append(upload);
    }
  });
  root.document.addEventListener("click",event=>{
    if(!event.target.closest("#q-upload-csv,[data-q-upload]"))return;
    root.__quanturaSetPanel?.("forecast");
    const source=root.document.getElementById("ensemble-source-type");if(source){source.value="series";source.dispatchEvent(new root.Event("change",{bubbles:true}));}
    const chip=root.document.getElementById("q-selected-market");
    if(chip)chip.textContent="Your CSV · choose the file and columns below. Use Q Search to return to a market.";
  });
  // Old ticker/download links still hydrate through verified provider discovery.
  const params=new URLSearchParams(root.location.search),symbol=params.get("ticker");
  if(symbol && !params.has("ensembleForecastId") && !params.has("screenerTicker")) {
    const query=new URLSearchParams({q:symbol,source:params.get("marketSource")==="kalshi_perps"?"kalshi_perps":"auto"});
    root.fetch("/api/market-search?"+query,{signal:AbortSignal.timeout(15000)})
      .then(r=>r.ok?r.json():null).then(payload=>{
        if(root.QuanturaMarketSelection)return;
        const row=Object.values(payload?.groups||{}).flat().find(r=>!r.contract_id && api.validResource(r) && r.symbol.toUpperCase()===symbol.toUpperCase());
        if(row)root.dispatchEvent(new root.CustomEvent("quantura:market-selected",{detail:{resource:row,intent:root.location.pathname==="/options"?"options":"restore"}}));
      }).catch(()=>{}); // No fabricated fallback selection when discovery fails.
  }
})(typeof window!=="undefined"?window:null,function(){
  "use strict";
  const sources=["alpaca","yahoo","kalshi","polymarket_us","kalshi_perps"];
  const validResource=row=>!!row && sources.includes(row.source) && typeof row.resource_id==="string" && row.resource_id.length<=500 && typeof row.symbol==="string" && row.symbol.length<=220;
  const label=row=>[row.outcome || row.symbol,row.market_title || row.name,row.source,row.exchange].filter(Boolean).join(" · ");
  function isoLocal(value){
    if(!value)return null;
    const d=new Date(value);if(!Number.isFinite(d.getTime()))throw Error("Choose a valid local date and time.");
    const expected=value.slice(0,16),actual=new Date(d.getTime()-d.getTimezoneOffset()*60000).toISOString().slice(0,16);
    if(expected!==actual)throw Error("That local time does not exist because of a timezone clock change.");
    return d.toISOString();
  }
  function requestFor(row,settings,contracts=[]){
    if(!validResource(row))throw Error("Select a market with Q Search first.");
    const end=isoLocal(settings.end)||new Date().toISOString();
    if(Date.parse(end)>Date.now()+60000)throw Error("The end time must not be in the future.");
    const prediction=row.resource_type==="prediction_market_contract";
    const frequency=settings.frequency;
    if(!["1Min","1Hour","1Day","raw","5m","15m","30m","final"].includes(frequency) || (!prediction && !["1Min","1Hour","1Day"].includes(frequency)))throw Error("Choose a supported interval.");
    const limit=Number(settings.limit);if(![500,1000,2000,50000].includes(limit))throw Error("Choose a supported row limit.");
    const interval=frequency==="1Min"?60000:frequency==="1Hour"?3600000:86400000;
    let start=settings.range==="dates"?isoLocal(settings.start):null;
    if(settings.range==="dates"&&!start)throw Error("Choose the start of the date range.");
    if(start && Date.parse(start)>=Date.parse(end))throw Error("Start must be before the end time.");
    if(prediction){
      if(!contracts.length || contracts.length>25 || contracts.some(c=>c.source!==row.source || !c.contractId || !c.providerSymbol))throw Error("Select up to 25 outcomes from one provider.");
      // The provider supports at most 90 days per export. Latest N is a bounded
      // lookback, not a promise that missing minutes exist.
      start ||= new Date(Date.parse(end)-Math.min(90*86400000,Math.max(7*86400000,limit*interval*2))).toISOString();
      return {url:"/api/sports/prediction-markets/export",body:{source:row.source,contracts,start,end,frequency:({"1Min":"1m","1Hour":"1h","1Day":"1d"})[frequency]||frequency,history_phase:settings.phase,mode:settings.layout,target:settings.target,missing:settings.missing,format:"json"}};
    }
    if(row.source==="kalshi_perps" && limit>5000)throw Error("Perpetual history supports at most 5,000 rows per request. Choose 2,000 or fewer here.");
    const option=settings.kind==="options";
    if(option&&!settings.optionSymbol)throw Error("Choose a specific call or put from the chain.");
    if(option || row.source==="kalshi_perps") start ||= new Date(Date.parse(end)-Math.max(7*86400000,limit*interval*3)).toISOString();
    return {url:row.source==="kalshi_perps"?"/api/market-data/perps/history":option?"/api/market-data/options/history":"/api/market-data/stocks/history",body:{source:"auto",symbol:row.symbol,contractSymbol:option?settings.optionSymbol:undefined,start:start||undefined,end,timeframe:frequency,frequency:({"1Min":"1min","1Hour":"1h","1Day":"1D"})[frequency],limit,session:settings.session,adjustment:settings.adjustment,format:"json"}};
  }
  function snapshot(payload,row,settings,request){
    if(!Array.isArray(payload.rows))throw Error("The provider did not return an exportable dataset.");
    let rows=payload.rows;
    if(rows.length>100000)throw Error("This export exceeds the preview limit. Use a smaller window.");
    if(settings.range==="latest"){
      const grouped=new Map();for(const r of rows){const key=r.contract_id||r.item_id||row.resource_id;if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(r);}
      rows=[...grouped.values()].flatMap(group=>group.sort((a,b)=>String(a.timestamp).localeCompare(String(b.timestamp))).slice(-Number(settings.limit)));
    }
    const {rows:unused,previewRows:ignored,...providerMetadata}=payload;
    const metadata={...providerMetadata,resource_id:row.resource_id,selection:{symbol:row.symbol,source:row.source,name:row.name,contract_id:row.contract_id||null},query:request.body,exported_at:new Date().toISOString(),row_count:rows.length,timezone:"UTC",provider:payload.provider||payload.metadata?.source||row.source};
    const columns=[...new Set(rows.flatMap(r=>Object.keys(r)))];
    return {metadata,columns,rows};
  }
  function csv(data){
    // Formula-like provider text must not execute when opening the CSV. Numbers,
    // including negatives, remain numeric. Identity/provenance repeated per row.
    const cell=v=>{if(v==null)return "";let s=typeof v==="object"?JSON.stringify(v):String(v);if(typeof v==="string"&&/^[\s]*[=+\-@\t\r]/.test(s))s="'"+s;return /[",\r\n]/.test(s)?'"'+s.replaceAll('"','""')+'"':s;};
    const columns=[...new Set(["resource_id","provider",...data.columns])];
    return [columns.map(cell).join(","),...data.rows.map(r=>columns.map(k=>cell(({resource_id:data.metadata.resource_id,provider:data.metadata.provider,...r})[k])).join(","))].join("\r\n");
  }
  return {validResource,label,isoLocal,requestFor,snapshot,csv};
});
