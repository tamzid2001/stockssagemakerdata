(() => {
  'use strict';
  const list=document.getElementById('canvas-items'),form=document.getElementById('canvas-upload-form');if(!list&&!form)return;
  const status=document.getElementById('canvas-status'),escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const table=(headers,rows)=>`<table class="data-table"><thead><tr>${headers.map(h=>`<th>${escape(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>`<tr>${r.map(v=>`<td>${escape(v)}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
  const rawUrl=item=>'https://raw.githubusercontent.com/tamzid2001/stockssagemakerdata/main/'+item.path.split('/').map(encodeURIComponent).join('/');
  const viewUrl=id=>'/forecasting?panel=forecast&sagemakerForecastId='+id;
  const admin=async()=>{await window.QuanturaAuth?.ready;if(window.QuanturaAuth?.verifiedEmail!=='tamzid257@gmail.com')return false;try{const token=await window.QuanturaAuth.getToken();const r=await fetch('/api/sagemaker/admin/access',{headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(15000)});return r.ok;}catch{return false;}};
  if(list){
    let items=[];const input=document.getElementById('canvas-search');
    const draw=()=>{const q=input.value.toLowerCase(),rows=items.filter(i=>`${i.ticker} ${i.name} ${i.path}`.toLowerCase().includes(q));status.textContent=`${rows.length} files`;list.innerHTML=rows.map(i=>`<article class="card"><h2>${escape(i.ticker || 'Dataset')} · ${escape(i.name)}</h2><p class="small muted">${i.quantiles.map(q=>'P'+q*100).join(' / ') || escape(i.kind)} · ${i.row_count} rows</p><p class="small muted">${escape(i.start_at?.slice(0,10) || '')} – ${escape(i.end_at?.slice(0,10) || '')}</p><div class="canvas-actions">${i.kind==='forecast'?`<a class="cta small" href="${viewUrl(i.id)}">View forecast</a>`:''}<button class="cta secondary small" type="button" data-canvas-preview="${i.id}">Preview</button><a class="cta secondary small" href="${rawUrl(i)}" target="_blank" rel="noopener">CSV</a></div>${i.warnings.length?`<details><summary>Source notes</summary><p class="small">${i.warnings.map(escape).join(' ')}</p></details>`:''}</article>`).join('');};
    const dialog=document.getElementById('canvas-preview');document.getElementById('canvas-close').onclick=()=>dialog.close();dialog.onclick=e=>{if(e.target===dialog){const b=dialog.getBoundingClientRect();if(e.clientX<b.left||e.clientX>b.right||e.clientY<b.top||e.clientY>b.bottom)dialog.close();}};
    const preview=async id=>{status.textContent='Loading preview…';try{const r=await fetch('/api/sagemaker/'+id),d=await r.json();if(!r.ok)throw Error('Unable to preview this export. Download its source CSV to inspect it.');document.getElementById('canvas-preview-title').textContent=d.item.name;document.getElementById('canvas-preview-table').innerHTML=table(d.headers,d.preview);document.getElementById('canvas-preview-actions').innerHTML=d.item.kind==='forecast'?`<a class="cta" href="${viewUrl(id)}">Open forecast & Scout</a>`:'';dialog.showModal();status.textContent='';}catch(e){status.textContent=e.message;}};
    list.onclick=e=>{const b=e.target.closest('[data-canvas-preview]');if(b)void preview(b.dataset.canvasPreview);};input.oninput=draw;
    fetch('/api/sagemaker').then(r=>{if(!r.ok)throw Error('The library is unavailable. Please retry.');return r.json();}).then(d=>{items=d.items;draw();const id=new URLSearchParams(location.search).get('file');if(id)void preview(id);}).catch(e=>status.textContent=e.message);
    void admin().then(ok=>{document.querySelector('[data-canvas-admin-link]').hidden=!ok;});
  }
  if(form){
    const file=document.getElementById('canvas-upload-file'),publish=document.getElementById('canvas-publish');let csv='',isAdmin=false;
    const refreshAdmin=()=>void admin().then(ok=>{isAdmin=ok;if(csv)publish.disabled=!ok;status.textContent=ok?'Admin access verified. Choose a CSV to preview.':'Sign in with your admin account to publish.';});refreshAdmin();document.addEventListener('quantura:organization',refreshAdmin);
    let previewGeneration=0;
    file.onchange=async()=>{
      const generation=++previewGeneration;publish.disabled=true;csv='';const f=file.files?.[0];if(!f)return;
      try{
        if(f.size>3000000)throw Error('Choose a CSV smaller than 3 MB.');
        const text=await f.text(),token=await window.QuanturaAuth.getToken();
        const response=await fetch('/api/sagemaker/preview',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({csv:text}),signal:AbortSignal.timeout(30000)}),parsed=await response.json();
        if(generation!==previewGeneration)return;
        if(!response.ok)throw Error(parsed.error==='admin_required'?'Sign in with your admin account.':`CSV preview failed: ${parsed.error}`);
        if(!parsed.quantiles.length)throw Error('No quantile columns found. Use P01 / P10 / P50 / P90 or similar Canvas headers.');
        csv=text;document.getElementById('canvas-name').value=f.name.replace(/\.csv$/i,'');
        const ticker=f.name.toUpperCase().match(/GOOGL|NVDA|NFLX|PLTR|AAPL|AMZN|MSFT|META|TSLA|SPY|QQQ|WMT|XAUUSD|BTC/);if(ticker)document.getElementById('canvas-ticker').value=ticker[0];
        document.getElementById('canvas-detected').textContent=`${parsed.row_count} rows · ${parsed.quantiles.map(q=>'P'+q*100).join(' / ')} · ${parsed.frequency} · UTC dates`;
        document.getElementById('canvas-upload-preview').innerHTML=table(parsed.headers,parsed.preview);publish.disabled=!isAdmin;status.textContent='Preview ready. Confirm the ticker and model metrics.';
      }catch(e){if(generation===previewGeneration){status.textContent=e.message;csv='';}}
    };
    form.onsubmit=async e=>{e.preventDefault();if(!csv||!isAdmin)return;publish.disabled=true;status.textContent='Publishing to the repository…';try{const metrics={};for(const input of form.querySelectorAll('[data-canvas-metric]'))if(input.value!==''){const key=input.dataset.canvasMetric,value=Number(input.value);metrics[key]=['smape','mape','wape'].includes(key)?value/100:value;}const token=await window.QuanturaAuth.getToken();const r=await fetch('/api/sagemaker',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({csv,name:document.getElementById('canvas-name').value,ticker:document.getElementById('canvas-ticker').value,metrics,metrics_basis:document.getElementById('canvas-metrics-basis').value}),signal:AbortSignal.timeout(90_000)}),d=await r.json();if(!r.ok)throw Error(d.error==='admin_required'?'Admin sign-in is required.':d.error==='canvas_github_not_configured'?'Repository publishing is not configured.':`Publish failed: ${d.error}`);status.replaceChildren(document.createTextNode('Published. '));const link=document.createElement('a');link.href=d.url;link.textContent='View forecast';status.append(link);csv='';form.reset();}catch(e){status.textContent=e.message;publish.disabled=false;}};
  }
})();
