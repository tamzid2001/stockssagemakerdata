/* Public pregame snapshots; no account, order or trading capability. */
(() => {
  'use strict';
  const section=document.getElementById('today-games');if(!section)return;
  const cards=section.querySelector('[data-game-cards]'),status=section.querySelector('[data-game-status]');
  const dialog=document.getElementById('game-forecast-dialog');
  const time=value=>new Intl.DateTimeFormat(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',timeZoneName:'short'}).format(new Date(value));
  const element=(tag,value,className)=>{const node=document.createElement(tag);node.textContent=value;if(className)node.className=className;return node;};
  const percent=value=>`${(100*value).toFixed(1)}%`;
  async function show(id) {
    dialog.showModal();const body=dialog.querySelector('[data-game-detail]');body.replaceChildren(element('p','Loading forecast…'));
    try {
      const response=await fetch(`/api/screener/games/${id}`);if(!response.ok)throw new Error('unavailable');const {item}=await response.json();
      if(!dialog.open)return;
      const title=dialog.querySelector('h2');title.textContent=`${item.outcome} · ${item.event_title}`;
      body.replaceChildren(element('p',`Game starts ${time(item.game_start)}. Forecast ends ${time(item.forecast_end)}.`),element('p',`Updated ${time(item.generated_at)} · ${item.history_count} genuine hourly observations · ${item.models.join(' + ')}`, 'small'));
      const chart=document.createElementNS('http://www.w3.org/2000/svg','svg');chart.setAttribute('viewBox','0 0 600 220');chart.setAttribute('role','img');chart.setAttribute('aria-label','Probability forecast: P10 to P90 range and median. The table below provides values.');
      const rows=item.predictions,first=Date.parse(rows[0].timestamp),last=Date.parse(rows.at(-1).timestamp);
      const x=r=>30+540*(Date.parse(r.timestamp)-first)/Math.max(1,last-first),y=q=>190-160*q;
      const path=(points,kind,stroke)=>{const node=document.createElementNS(chart.namespaceURI,kind);node.setAttribute('points',points);node.setAttribute('fill',kind==='polygon'?'var(--game-band)':'none');node.setAttribute('stroke',stroke);node.setAttribute('stroke-width','3');chart.append(node);};
      path([...rows.map(r=>`${x(r)},${y(r.quantiles['0.9'])}`),...rows.slice().reverse().map(r=>`${x(r)},${y(r.quantiles['0.1'])}`)].join(' '),'polygon','none');
      path(rows.map(r=>`${x(r)},${y(r.quantiles['0.5'])}`).join(' '),'polyline','var(--primary)');
      body.append(chart);
      const table=document.createElement('table');table.className='game-probabilities';
      const caption=element('caption','Forecast probabilities for the selected outcome');table.append(caption);
      const head=document.createElement('thead'),tr=document.createElement('tr');for(const label of ['Time','P10','P50','P90']){const th=element('th',label);th.scope='col';tr.append(th);}head.append(tr);table.append(head);
      const tbody=document.createElement('tbody');for(const row of rows){const tr=document.createElement('tr');tr.append(element('td',time(row.timestamp)));for(const q of ['0.1','0.5','0.9'])tr.append(element('td',percent(row.quantiles[q])));tbody.append(tr);}table.append(tbody);
      const scroller=element('div','', 'game-table-scroll');scroller.append(table);body.append(scroller);
      const details=document.createElement('details');details.append(element('summary','Data and methodology'),element('p',item.method));
      for(const warning of item.warnings||[])details.append(element('p',warning,'small'));body.append(details);
    } catch {body.replaceChildren(element('p','This forecast could not be loaded. Close and try again.'));}
  }
  async function load(){
    status.textContent='Loading today’s game forecasts…';
    try {
      const response=await fetch('/api/screener/games');if(!response.ok)throw new Error('unavailable');const data=await response.json();cards.replaceChildren();
      status.textContent=data.items.length?`${data.items.length} outcome forecasts · Today in New York (${data.date})`:'No pregame forecasts are available for today yet.';
      if(data.coverage?.some(p=>p.partial || p.failed) || data.bounded)status.textContent+=' Some markets are unavailable in this scan.';
      for(const game of data.items){
        const card=element('article','','game-card');card.append(element('div',game.provider==='kalshi'?'Kalshi':'Polymarket US','small'),element('h3',game.event_title),element('p',game.outcome),element('p',`Starts ${time(game.game_start)}`,'small'),element('p',`${game.status==='final_pregame'?'Final pregame forecast':'Updates hourly before start hour'} · Updated ${time(game.generated_at)}`,'small'));
        const button=element('button','View forecast','cta secondary');button.type='button';button.addEventListener('click',()=>show(game.id));card.append(button);cards.append(card);
      }
    } catch{status.textContent='Game forecasts are temporarily unavailable. Try refreshing.';}
  }
  dialog.querySelector('[data-game-close]').addEventListener('click',()=>dialog.close());
  section.querySelector('[data-game-refresh]').addEventListener('click',load);load();
})();
