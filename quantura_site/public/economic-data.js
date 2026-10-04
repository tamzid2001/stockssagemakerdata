(() => {
  "use strict";
  const forecast=document.getElementById("economic-forecast-controls"),explorer=document.getElementById("economic-explorer");
  if(!forecast&&!explorer)return;
  const host=forecast||explorer,fields=host.querySelector("[data-economic-fields]"),status=host.querySelector("[data-economic-status]"),table=host.querySelector("[data-economic-table]");
  let selected=null,description=null,controller=null,sequence=0,snapshot=null;
  const esc=value=>String(value??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
  async function request(path,body,signal) {
    const response=await fetch(path,{method:body?"POST":"GET",headers:{Accept:"application/json",...(body?{"Content-Type":"application/json"}:{})},...(body?{body:JSON.stringify(body)}:{}),signal});
    const data=await response.json();if(!response.ok||data.ok===false)throw Error(data.message||"The data source could not return this selection.");return data;
  }
  function source() {
    if(!selected||!description)throw Error("Choose an economic indicator and wait for its settings to load.");
    const dimensions={};let ref_area;
    for(const input of fields.querySelectorAll("[data-economic-dimension]")) {
      if(!input.value)throw Error("Choose "+input.dataset.economicDimension+".");
      if(input.dataset.economicDimension==="REF_AREA")ref_area=input.value;else dimensions[input.dataset.economicDimension]=input.value;
    }
    const value=fields.querySelector("[data-economic-value]")?.value;
    return {...selected.economic_source,dimensions,...(ref_area?{ref_area}:{}),...(selected.source==="fiscaldata"?{value_field:value}:{})};
  }
  function frequency() {
    const freq=fields.querySelector('[data-economic-dimension="FREQ"]')?.value;
    return ({A:"1YE-DEC",Q:"1QE-DEC",M:"1ME",D:"1D"})[freq]||description?.frequency||"1D";
  }
  function updateSelection() {
    snapshot=null;table?.replaceChildren();
    host.querySelector("[data-economic-download]").disabled=true;
    if(!selected||!description)return;
    try{selected.economic_source=source();selected.frequency=frequency();}catch{}
    document.getElementById("ensemble-source-type")?.dispatchEvent(new Event("change",{bubbles:true}));
  }
  async function choose(resource) {
    controller?.abort();controller=new AbortController();const run=++sequence;
    selected=resource;description=null;snapshot=null;fields.replaceChildren();table?.replaceChildren();
    status.textContent="Loading series dimensions…";host.setAttribute("aria-busy","true");
    host.querySelector("[data-economic-download]").disabled=true;
    try{
      const data=await request("/api/economic-data/describe",resource.economic_source,controller.signal);if(run!==sequence)return;
      description=data;
      fields.innerHTML=`<strong>${esc(data.name)}</strong><div class="form-grid">${data.values.length>1?`<div class="field"><label for="economic-value">Value</label><select id="economic-value" name="economic_value" data-economic-value>${data.values.map(v=>`<option value="${esc(v.field)}">${esc(v.label)}</option>`).join("")}</select></div>`:`<input type="hidden" data-economic-value value="${esc(data.values[0]?.field||'OBS_VALUE')}" />`}${data.dimensions.map(d=>{
        const previous=d.field==="REF_AREA"?resource.economic_source.ref_area:resource.economic_source.dimensions?.[d.field];
        const chosen=previous|| (d.values.length===1?d.values[0]:d.field==="REF_AREA"&&d.values.includes("USA")?"USA":"");
        return `<div class="field"><label for="economic-${esc(d.field)}">${esc(d.field==="REF_AREA"?"Country / economy":d.label)}</label><select id="economic-${esc(d.field)}" name="economic_${esc(d.field)}" data-economic-dimension="${esc(d.field)}">${chosen?'':'<option value="">Choose a value</option>'}${d.values.map(v=>`<option value="${esc(v)}"${v===chosen?' selected':''}>${esc(v)}</option>`).join("")}</select></div>`;
      }).join("")}</div>`;
      if(resource.economic_source.value_field&&fields.querySelector("[data-economic-value]"))fields.querySelector("[data-economic-value]").value=resource.economic_source.value_field;
      updateSelection();status.textContent=`${data.frequency} · ${data.units||'Units supplied with observations'}. Select one value for every dimension.`;
      const link=host.querySelector("[data-economic-link]");link.href=data.url;link.hidden=false;
    }catch(error){if(run===sequence&&error.name!=="AbortError")status.textContent=error.message;}
    finally{if(run===sequence)host.removeAttribute("aria-busy");}
  }
  async function preview() {
    controller?.abort();controller=new AbortController();const run=++sequence;const button=host.querySelector("[data-economic-preview]");button.disabled=true;
    try{
      const config={...source(),limit:50000};status.textContent="Downloading observed series history…";
      const data=await request("/api/economic-data/history",config,controller.signal);if(run!==sequence)return;
      selected.economic_source=data.source;selected.frequency=data.frequency;
      snapshot=window.QuanturaQMarket.snapshot(data,selected,{range:"dates"},{body:config});
      table.innerHTML=`<table><thead><tr><th>Reporting period</th><th>Value</th><th>Unit</th></tr></thead><tbody>${data.rows.slice(-30).map(r=>`<tr><td>${esc(r.TIME_PERIOD||r.record_date)}</td><td>${esc(r.target)}</td><td>${esc(data.metadata.units)}</td></tr>`).join("")}</tbody></table>`;
      status.textContent=`${data.rows.length.toLocaleString()} observed values · ${data.frequency} · ${data.metadata.units}. ${(data.warnings||[]).join(" ")}`;
      host.querySelector("[data-economic-download]").disabled=!data.rows.length;
      document.getElementById("ensemble-source-type")?.dispatchEvent(new Event("change",{bubbles:true}));
    }catch(error){if(run===sequence&&error.name!=="AbortError")status.textContent=error.message;}
    finally{if(run===sequence)button.disabled=false;}
  }
  fields.addEventListener("change",updateSelection);
  host.querySelector("[data-economic-preview]").addEventListener("click",preview);
  host.querySelector("[data-economic-download]").addEventListener("click",()=>{
    if(!snapshot)return;
    const url=URL.createObjectURL(new Blob([window.QuanturaQMarket.csv(snapshot)],{type:"text/csv;charset=utf-8"}));
    const link=document.createElement("a");link.href=url;link.download=`quantura-${selected.symbol}.csv`;link.click();URL.revokeObjectURL(url);
  });
  window.QuanturaEconomics={source,frequency};
  window.addEventListener("quantura:market-selected",event=>{
    const row=event.detail?.resource;
    if(row?.resource_type==="economic_series")void choose(row);
    else{controller?.abort();++sequence;selected=null;description=null;snapshot=null;}
  });
  if(explorer) {
    const form=explorer.querySelector("form"),results=explorer.querySelector("[data-economic-results]");let searchRows=[];
    form.addEventListener("submit",async event=>{
      event.preventDefault();controller?.abort();controller=new AbortController();const run=++sequence,button=form.querySelector('button[type="submit"]');button.disabled=true;status.textContent="Searching economic indicators…";
      try{
        const params=new URLSearchParams({provider:form.elements.provider.value,q:form.elements.q.value,limit:"20"});
        const data=await request("/api/economic-data/search?"+params,null,controller.signal);if(run!==sequence)return;
        searchRows=data.results;results.innerHTML=searchRows.map((r,i)=>`<button class="cta secondary small" type="button" data-economic-result="${i}">${esc(r.name)} · ${esc(r.dataset_id||r.id)}</button>`).join("");
        status.textContent=`${data.results.length} results shown${data.count>data.results.length?' of '+data.count:''}. Choose an indicator.`;
      }catch(error){if(run===sequence&&error.name!=="AbortError")status.textContent=error.message;}
      finally{if(run===sequence)button.disabled=false;}
    });
    results.addEventListener("click",event=>{
      const button=event.target.closest("[data-economic-result]");if(!button)return;const row=searchRows[Number(button.dataset.economicResult)];
      const resource={resource_type:"economic_series",resource_id:row.provider+":"+row.id,symbol:row.id,name:row.name,source:row.provider,frequency:row.frequency,economic_source:{type:"economic_series",provider:row.provider,...(row.provider==="fiscaldata"?{series_id:row.id}:{dataset_id:row.dataset_id,indicator_id:row.indicator_id})}};
      void choose(resource);
    });
    explorer.querySelector("[data-economic-forecast]").addEventListener("click",()=>{
      try{selected.economic_source=source();selected.frequency=frequency();sessionStorage.setItem("quantura_pending_market_selection",JSON.stringify({resource:selected,intent:"forecast",at:Date.now()}));window.location.href="/forecasting";}catch(error){status.textContent=error.message;}
    });
  }
})();
