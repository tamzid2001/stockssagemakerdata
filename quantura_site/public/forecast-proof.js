(() => {
  'use strict';
  const node=(tag,text,className)=>{const n=document.createElement(tag);if(text)n.textContent=text;if(className)n.className=className;return n;};
  function attach(host,{job,request,canStamp=false}) {
    if(!host)return;
    host.replaceChildren();host.dataset.forecastId=job.forecast_id;
    host.hidden=Boolean(job.published_screener||job.sagemaker_item||(!canStamp&&!job.provenance));
    if(host.hidden)return;
    const path=`/api/v1/ensemble-forecasts/${encodeURIComponent(job.forecast_id)}/proof`;
    const title=node('strong','Timestamp receipt');
    const status=node('span','','small muted');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
    const controls=node('div','','ensemble-toolbar-group');
    host.append(title,status,controls);
    let proof=job.provenance;
    const current=()=>host.dataset.forecastId===job.forecast_id;
    function button(label,action) {
      const b=node('button',label,'task-chip');b.type='button';b.addEventListener('click',async()=>{
        b.disabled=true;
        try {await action();}catch(error){if(current())status.textContent=error.message||'Receipt unavailable. Try again.';}
        finally {if(current())b.disabled=false;}
      });controls.append(b);
    }
    function render() {
      if(!current())return;
      controls.replaceChildren();
      if(proof?.status==='stamped') {
        const time=new Intl.DateTimeFormat(undefined,{dateStyle:'medium',timeStyle:'short'}).format(new Date(proof.receipt.timestamp));
        status.textContent=`vBase · Stamped ${time}. Confirms content and time.`;
        button('Verify receipt',async()=>{
          const value=await request(`${path}?verify=true`);
          if(current())status.textContent=value.data?.content_matches&&value.data?.stamp_found?'Content matches its vBase timestamp receipt.':'No matching timestamp receipt was found.';
        });
        button('Download proof',async()=>{
          const value=await request(`${path}?format=envelope`);
          if(!current())return;
          const bytes=value.data?.manifest_utf8;if(typeof bytes!=='string')throw new Error('Proof download unavailable.');
          const url=URL.createObjectURL(new Blob([bytes],{type:'application/json'}));
          const a=node('a');a.href=url;a.download=`quantura-proof-${job.forecast_id}.json`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
        });
      }else {
        status.textContent=proof?.status==='stamping'?'Timestamp receipt is being prepared.':'Create a receipt for this saved forecast. Its timestamp will be now.';
        if(canStamp)button(proof?.status==='stamping'?'Check receipt':'Create receipt',async()=>{
          const value=await request(path,{method:'POST',body:{}});
          if(current()){proof=value.data;render();}
        });
      }
    }
    render();
  }
  window.QuanturaForecastProof={attach};
})();
