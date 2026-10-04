(() => {
  "use strict";
  const forecast=document.getElementById("economic-forecast-controls"),explorer=document.getElementById("economic-explorer");
  if(!forecast&&!explorer)return;
  const host=forecast||explorer,fields=host.querySelector("[data-economic-fields]"),status=host.querySelector("[data-economic-status]"),table=host.querySelector("[data-economic-table]");
  let selected=null,description=null,controller=null,sequence=0,snapshot=null;
  const esc=value=>String(value??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
  async function request(path,body,signal) {
    const token=window.QuanturaAuth?.signedIn?await window.QuanturaAuth.getToken():"";
    const response=await fetch(path,{method:body?"POST":"GET",headers:{Accept:"application/json",...(token?{Authorization:`Bearer ${token}`}:{ }),...(body?{"Content-Type":"application/json"}:{})},...(body?{body:JSON.stringify(body)}:{}),signal});
    const data=await response.json();if(!response.ok||data.ok===false)throw Error(data.message||"The data source could not return this selection.");return data;
  }
  function source() {
    if(!selected||!description)throw Error("Choose an economic indicator and wait for its settings to load.");
    if(selected.source==="bigquery") {
      if(!description.forecastable)throw Error("This table has no supported time-series columns. Choose another table.");
      const dimensions={};
      for(const row of fields.querySelectorAll("[data-bigquery-filter]")) {
        const field=row.querySelector("select").value,value=row.querySelector("input").value.trim();
        if(!field&&!value)continue;
        if(!field||!value)throw Error("Choose a column and an exact filter value.");
        if(Object.hasOwn(dimensions,field))throw Error("Use each filter column once.");
        dimensions[field]=value;
      }
      return {...selected.economic_source,time_field:fields.querySelector("[data-economic-time]").value,value_field:fields.querySelector("[data-economic-value]").value,aggregation:fields.querySelector("[data-economic-aggregation]").value,frequency:fields.querySelector("[data-economic-frequency]").value,dimensions};
    }
    const dimensions={};let ref_area;
    for(const input of fields.querySelectorAll("[data-economic-dimension]")) {
      if(!input.value)throw Error("Choose "+input.dataset.economicDimension+".");
      if(input.dataset.economicDimension==="REF_AREA")ref_area=input.value;else dimensions[input.dataset.economicDimension]=input.value;
    }
    const value=fields.querySelector("[data-economic-value]")?.value;
    return {...selected.economic_source,dimensions,...(ref_area?{ref_area}:{}),...(selected.source==="fiscaldata"?{value_field:value}:{})};
  }
  function frequency() {
    if(selected?.source==="bigquery")return fields.querySelector("[data-economic-frequency]")?.value||"1D";
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
      if(resource.source==="bigquery") {
        const options=(rows,current)=>rows.map(v=>`<option value="${esc(v.field)}"${v.field===current?' selected':''}>${esc(v.label.slice(0,90))}</option>`).join("");
        fields.innerHTML=`<strong>${esc(data.name)}</strong><div class="form-grid"><div class="field"><label for="economic-time">Time column · UTC</label><select id="economic-time" data-economic-time>${options(data.time_fields,resource.economic_source.time_field)}</select></div><div class="field"><label for="economic-value">Numeric value</label><select id="economic-value" data-economic-value>${options(data.values,resource.economic_source.value_field)}</select></div><div class="field"><label for="economic-frequency">Forecast interval</label><select id="economic-frequency" data-economic-frequency><option value="1h">Hourly</option><option value="1D" selected>Daily</option><option value="1W-MON">Weekly · Monday</option><option value="1MS">Monthly</option></select></div><div class="field"><label for="economic-aggregation">Aggregation</label><select id="economic-aggregation" data-economic-aggregation><option value="none">None · keep observed timestamps</option><option value="avg">Average</option><option value="sum">Sum</option><option value="min">Minimum</option><option value="max">Maximum</option></select></div></div><div data-bigquery-filters></div><button class="cta secondary small" type="button" data-bigquery-add-filter>Add exact filter</button>`;
        fields.querySelector('[data-economic-frequency]').value=resource.economic_source.frequency||"1D";
        fields.querySelector('[data-economic-aggregation]').value=resource.economic_source.aggregation||"none";
        const add=(field="",value="")=>{
          const list=fields.querySelector('[data-bigquery-filters]');if(list.children.length>=8)return;
          const row=document.createElement('div');row.className='form-grid';row.dataset.bigqueryFilter='';
          row.innerHTML=`<div class="field"><label>Filter column<select aria-label="Filter column"><option value="">No filter</option>${data.filter_fields.map(f=>`<option value="${esc(f.field)}"${field===f.field?' selected':''}>${esc(f.field)}</option>`).join('')}</select></label></div><div class="field"><label>Exact value<input aria-label="Exact filter value" maxlength="180" value="${esc(value)}" placeholder="e.g. station or country code" /></label></div>`;list.append(row);
        };
        const filters=Object.entries(resource.economic_source.dimensions||{});if(filters.length)filters.forEach(([k,v])=>add(k,v));else add();
        fields.querySelector('[data-bigquery-add-filter]').onclick=()=>add();
        host.querySelector('[data-economic-preview]').disabled=!data.forecastable;
        updateSelection();status.textContent=data.forecastable?"Preview the selected columns. Exact filters keep distinct series separate; aggregation uses completed UTC intervals.":"This table has no supported date and numeric columns. Choose a different public table.";
        const link=host.querySelector('[data-economic-link]');link.href=data.url;link.hidden=false;return;
      }
      host.querySelector('[data-economic-preview]').disabled=false;
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
      table.innerHTML=`<table><thead><tr><th>Reporting period</th><th>Value</th><th>Unit</th></tr></thead><tbody>${data.rows.slice(-30).map(r=>`<tr><td>${esc(r.TIME_PERIOD||r.record_date||r.timestamp)}</td><td>${esc(r.target)}</td><td>${esc(data.metadata.units)}</td></tr>`).join("")}</tbody></table>`;
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
  // The same database settings travel with the selected series in Forecast and Download.
  const marker=forecast?document.createComment('database settings'):null;
  if(marker)forecast.before(marker);
  function panelChanged(panel){
    if(!forecast)return;
    const slot=document.querySelector('[data-economic-download-settings]');
    if(panel==='download'&&selected){slot?.append(forecast);forecast.hidden=false;}
    else{marker.after(forecast);forecast.hidden=!selected;}
  }
  window.addEventListener('quantura:panel-changed',event=>panelChanged(event.detail?.panel));
  window.addEventListener("quantura:market-selected",event=>{
    const row=event.detail?.resource;
    if(row?.resource_type==="economic_series"){void choose(row);panelChanged(event.detail?.intent==='add-download'||event.detail?.intent==='download'?'download':'forecast');}
    else{controller?.abort();++sequence;selected=null;description=null;snapshot=null;fields.replaceChildren();table?.replaceChildren();host.hidden=true;if(marker)marker.after(host);}
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
      const resource={resource_type:"economic_series",resource_id:row.provider+":"+row.id,symbol:row.id,name:row.name,source:row.provider,frequency:row.frequency,economic_source:row.bigquery_source||{type:"economic_series",provider:row.provider,...(row.provider==="fiscaldata"?{series_id:row.id}:{dataset_id:row.dataset_id,indicator_id:row.indicator_id})}};
      void choose(resource);
    });
    explorer.querySelector("[data-economic-forecast]").addEventListener("click",()=>{
      try{selected.economic_source=source();selected.frequency=frequency();sessionStorage.setItem("quantura_pending_market_selection",JSON.stringify({resource:selected,intent:"forecast",at:Date.now()}));window.location.href="/forecasting";}catch(error){status.textContent=error.message;}
    });
  }
})();
