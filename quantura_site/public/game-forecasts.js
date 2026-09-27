/* Public saved snapshots; viewing never queues inference or trading. */
(() => {
  'use strict';
  const cards=document.getElementById('qs-games'),page=document.getElementById('game-forecast-page');if(!cards&&!page)return;
  const bands=['0.01','0.25','0.5','0.75','0.9','0.99'];let sequence=0,viewed=null,saveBusy=false;
  const savedFor=new Set();
  async function saveViewed(){
    const user=window.firebase?.auth?.().currentUser,entry=viewed;
    if(!entry||entry.saved||!user||user.isAnonymous||saveBusy)return;
    const key=user.uid+':'+entry.item.id+':'+entry.item.generated_at;if(savedFor.has(key))return;
    saveBusy=true;
    try{
      const token=await user.getIdToken();
      const response=await fetch(`/api/screener/games/${entry.item.id}/save`,{method:'POST',headers:{Authorization:`Bearer ${token}`},credentials:'same-origin',signal:AbortSignal.timeout(15000)});
      if(!response.ok)throw Error();const result=await response.json();savedFor.add(key);
      if(viewed===entry && window.firebase?.auth?.().currentUser?.uid===user.uid){const status=el('p','Saved to your profile’s Requests.','small');status.setAttribute('role','status');page.append(status);}
      document.dispatchEvent(new CustomEvent('quantura:request-saved',{detail:{requestId:result.request_id}}));
    }catch{if(viewed===entry){const status=el('p','Forecast loaded. Saving to Requests failed.','small'),retry=el('button','Retry save','cta secondary');retry.type='button';retry.addEventListener('click',saveViewed);status.append(retry);page.append(status);}}finally{saveBusy=false;if(viewed!==entry)saveViewed();}
  }
  const time=v=>new Intl.DateTimeFormat(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',timeZone:'America/New_York',timeZoneName:'short'}).format(new Date(v));
  const el=(tag,text='',cls)=>{const n=document.createElement(tag);n.textContent=text;if(cls)n.className=cls;return n;};
  const percent=v=>Number.isFinite(v)?`${(100*v).toFixed(1)}%`:'—',label=q=>'P'+String(Math.round(Number(q)*100)).padStart(2,'0');
  const href=id=>`/forecasting?panel=forecast&gameForecastId=${encodeURIComponent(id)}`;
  function provider(game){const n=el('div',game.provider==='kalshi'?'Kalshi':'Polymarket US','game-provider small');if(window.QuanturaLogos)n.insertAdjacentHTML('afterbegin',window.QuanturaLogos.markup(game));return n;}
  async function show(id,saved=false){
    if(!page){window.location.href=href(id);return;}
    const run=++sequence;viewed=null;page.hidden=false;page.setAttribute('aria-busy','true');page.replaceChildren(el('p','Loading saved game forecast…'));
    try{
      const headers={};if(saved){const user=window.firebase?.auth?.().currentUser;if(!user||user.isAnonymous)throw Error();headers.Authorization=`Bearer ${await user.getIdToken()}`;}
      const response=await fetch(`/api/screener/games/${saved?'saved/':''}${encodeURIComponent(id)}`,{headers,signal:AbortSignal.timeout(20000)});if(!response.ok)throw Error();const {item}=await response.json();if(run!==sequence)return;
      const rows=item.predictions;if(!Array.isArray(rows)||!rows.length)throw Error();const available=bands.filter(q=>rows.every(r=>Number.isFinite(r.quantiles?.[q])));if(!available.includes('0.5'))throw Error();
      page.replaceChildren(provider(item),el('h2',item.event_title),el('p',item.outcome),el('p',`Game starts ${time(item.game_start)}. Forecast ends ${time(item.forecast_end)}.`),el('p',`${item.recomputed_at?'Recomputed':'Updated'} ${time(item.recomputed_at||item.generated_at)} · ${item.history_count} genuine hourly observations · ${item.models.join(' + ')}`,'small'),el('p',`Pregame data through ${time(item.input_cutoff)} · ${item.recomputed_at?'Retrospective recalculation from the original pregame cutoff.':item.status==='final_pregame'?'Final pregame forecast.':'Updates hourly until the start hour.'}`,'small muted'));
      if(available.length!==6)page.append(el('p','This older snapshot has fewer quantiles. The next successful ensemble refresh will replace it.','notice small'));
      const chart=document.createElementNS('http://www.w3.org/2000/svg','svg');chart.setAttribute('viewBox','0 0 720 280');chart.setAttribute('role','img');chart.setAttribute('aria-label','Outcome probability forecast: outer P01–P99 band, inner P25–P75 band and P50 median. Values follow in the table.');
      const first=Date.parse(rows[0].timestamp),last=Date.parse(rows.at(-1).timestamp),x=r=>52+640*(Date.parse(r.timestamp)-first)/Math.max(1,last-first),y=q=>232-208*q;
      const svg=(tag,attrs,text)=>{const n=document.createElementNS(chart.namespaceURI,tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);if(text)n.textContent=text;chart.append(n);};
      for(const p of[0,.25,.5,.75,1]){svg('line',{x1:52,x2:692,y1:y(p),y2:y(p),stroke:'var(--border)'});svg('text',{x:43,y:y(p)+4,'text-anchor':'end',fill:'var(--muted-foreground)','font-size':12},`${p*100}%`);}
      const band=(lo,hi,opacity)=>{if(available.includes(lo)&&available.includes(hi))svg('polygon',{points:[...rows.map(r=>`${x(r)},${y(r.quantiles[hi])}`),...rows.slice().reverse().map(r=>`${x(r)},${y(r.quantiles[lo])}`)].join(' '),fill:'var(--primary)',opacity});};band('0.01','0.99',.12);band('0.25','0.75',.23);
      svg('polyline',{points:rows.map(r=>`${x(r)},${y(r.quantiles['0.5'])}`).join(' '),fill:'none',stroke:'var(--primary)','stroke-width':3});svg('text',{x:52,y:258,fill:'var(--muted-foreground)','font-size':12},time(rows[0].timestamp));svg('text',{x:692,y:258,'text-anchor':'end',fill:'var(--muted-foreground)','font-size':12},time(rows.at(-1).timestamp));page.append(chart);
      const actions=el('div','','game-forecast-actions'),back=el('a','Back to Screener','cta secondary');back.href='/forecasting?panel=screener';actions.append(back);
      const refresh=el('button',saved?'Reload saved forecast':'Refresh forecast','cta secondary');refresh.type='button';refresh.addEventListener('click',()=>show(id,saved));actions.append(refresh);
      const download=el('button','Download CSV','cta secondary');download.type='button';download.addEventListener('click',()=>{const csv=['timestamp,'+available.map(label).join(','),...rows.map(r=>[r.timestamp,...available.map(q=>r.quantiles[q])].join(','))].join('\r\n');const url=URL.createObjectURL(new Blob([csv],{type:'text/csv'})),a=el('a');a.href=url;a.download=`${item.provider}-${id}-pregame.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});actions.append(download);page.append(actions);
      const table=el('table','','game-probabilities');table.append(el('caption','Selected outcome probability · forecast quantiles'));const head=el('thead'),tr=el('tr');for(const title of['Time',...available.map(label)]){const th=el('th',title);th.scope='col';tr.append(th);}head.append(tr);table.append(head);
      const body=el('tbody');for(const row of rows){const tr=el('tr');tr.append(el('td',time(row.timestamp)));for(const q of available)tr.append(el('td',percent(row.quantiles[q])));body.append(tr);}table.append(body);const scroll=el('div','','game-table-scroll');scroll.append(table);page.append(scroll);
      const details=el('details');details.append(el('summary','Data and methodology'),el('p',item.method));if(item.schedule_verified_at)details.append(el('p',`Schedule verified ${time(item.schedule_verified_at)} · ${item.schedule_source}`,'small'));for(const warning of item.warnings||[])details.append(el('p',warning,'small'));page.append(details);
      viewed={item,saved};if(!saved)saveViewed();
    }catch{if(run===sequence){page.replaceChildren(el('p',saved?'Sign in to open your saved forecast.':'This forecast could not be loaded.'));const retry=el('button','Try again','cta secondary');retry.type='button';retry.addEventListener('click',()=>show(id,saved));page.append(retry);}}finally{if(run===sequence)page.removeAttribute('aria-busy');}
  }
  function render(items){if(!cards)return;cards.replaceChildren();for(const game of items){const card=el('article','','game-card');card.append(provider(game),el('h3',game.event_title),el('p',game.outcome),el('p',`Starts ${time(game.game_start)}`,'small'));if(Number.isFinite(game.endpoint?.['0.5']))card.append(el('p',`End-of-horizon P50: ${percent(game.endpoint['0.5'])}`));card.append(el('p',`${game.status==='final_pregame'?'Final pregame forecast':'Updates hourly before start hour'} · ${game.recomputed_at?'Recomputed':'Updated'} ${time(game.recomputed_at||game.generated_at)}`,'small'));const link=el('a','View forecast','cta secondary');link.href=href(game.id);card.append(link);cards.append(card);}}
  window.QuanturaGames=Object.freeze({render,show});
  const params=new URLSearchParams(window.location.search),id=params.get('gameForecastId'),savedId=params.get('userGameForecastId');
  if(page&&(/^[a-f0-9]{32}$/.test(id||'')||/^[a-f0-9]{40}$/.test(savedId||''))){
    document.body.classList.add('game-forecast-view');
    if(id)show(id);
    try{window.firebase?.auth?.().onAuthStateChanged(user=>{if(savedId&&user&&!user.isAnonymous)show(savedId,true);else saveViewed();});}catch{}
    if(savedId&&!window.firebase?.auth?.().currentUser){page.hidden=false;page.replaceChildren(el('p','Sign in to open your saved forecast.'));}
  }
})();
