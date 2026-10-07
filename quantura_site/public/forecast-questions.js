(() => {
  'use strict';
  const states=new Map();
  const el=(tag,text='',className='')=>{const node=document.createElement(tag);node.textContent=text;if(className)node.className=className;return node;};
  const escape=v=>String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
  const uuid=()=>crypto.randomUUID();
  const finite=v=>typeof v==='number'&&Number.isFinite(v);
  const format=v=>finite(v)?v.toLocaleString(undefined,{maximumFractionDigits:6}):String(v);
  const time=(v,calendar=false)=>new Intl.DateTimeFormat(undefined,calendar?{dateStyle:'medium',timeZone:'UTC'}:{dateStyle:'medium',timeStyle:'short'}).format(new Date(v));
  const icon=()=>{const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('aria-hidden','true');svg.setAttribute('fill','none');svg.setAttribute('stroke','currentColor');svg.setAttribute('stroke-width','1.7');const path=document.createElementNS(svg.namespaceURI,'path');path.setAttribute('d','m12 3 2.8 6.2L21 12l-6.2 2.8L12 21l-2.8-6.2L3 12l6.2-2.8L12 3Z M20 2v4 M18 4h4');svg.append(path);return svg;};
  async function transport(path,{body,signal,method='POST'}={}) {
    const user=window.firebase?.auth?.().currentUser;
    if(!user||user.isAnonymous)throw new Error('Sign in to ask Jev and save your research.');
    const token=await (window.QuanturaAuth?.getToken(user)??user.getIdToken());
    const response=await fetch(path,{method,credentials:'same-origin',headers:{Authorization:`Bearer ${token}`,...(body?{'Content-Type':'application/json'}:{})},body:body?JSON.stringify(body):undefined,signal:signal?AbortSignal.any([signal,AbortSignal.timeout(45000)]):AbortSignal.timeout(45000)});
    const value=await response.json().catch(()=>({}));
    if(!response.ok)throw new Error(value.error?.message||'Unable to load this research. Please retry.');
    return value;
  }
  function suggestions(job) {
    const median=finite(job.predictions?.[0]?.quantiles?.['0.5']);
    return [
      {category:job.predictions?.length?'Forecast':'Data',question:median?'How does P50 change from the last observed value to the end of this forecast?':'Summarize the observed values in this series.'},
      {category:'History',question:'What were the observed high and low, and when did they occur?'},
      {category:'Evidence',question:job.predictions?.length?'Where did these observations come from, and what are their units?':'What data checks should I make before forecasting this uploaded series?'},
    ];
  }
  function messageNode(message) {
    const article=el('article','','jev-answer');
    const question=el('h4',message.question,'jev-question');question.prepend(icon());article.append(question);
    const response=message.response || {};
    article.append(el('h5',response.heading || 'Saved response'),el('p',response.answer || 'No saved answer is available.'));
    if(response.facts?.length){const list=el('dl','','jev-facts');for(const fact of response.facts){const row=el('div');row.append(el('dt',fact.label),el('dd',format(fact.value)));if(fact.timestamp)row.append(el('span',`${fact.kind==='forecast'?'Forecast':'Observed'} · ${time(fact.timestamp,/^(1D|1W-MON|1MS)$/.test(response.frequency))}`,'small muted'));list.append(row);}article.append(list);}
    const details=el('details'),summary=el('summary','Sources and context');details.append(summary);
    for(const ref of response.references || [])details.append(el('p',[ref.provider,ref.frequency,ref.input_cutoff?`Input cutoff ${time(ref.input_cutoff,/^(1D|1W-MON|1MS)$/.test(ref.frequency))}`:null,ref.generated_at?`Generated ${time(ref.generated_at)}`:null].filter(Boolean).join(' · '),'small'));
    for(const warning of response.warnings || [])details.append(el('p',warning,'small'));
    details.append(el('p','Answers use saved observations and forecast values. Live quotes and strategy returns require separate evidence.','small muted'));article.append(details);return article;
  }
  function points(job) {
    const calendar=/^(1D|1W-MON|1MS)$/.test(job.frequency);
    const observed=(job.history || []).filter(r=>finite(r.target)).map(r=>({series:'history',timestamp:r.timestamp,value:r.target,label:`Observed · ${time(r.timestamp,calendar)} · ${format(r.target)}`}));
    const forecast=(job.predictions || []).flatMap(r=>Object.entries(r.quantiles || {}).filter(([q,v])=>q==='0.5'&&finite(v)).map(([q,value])=>({series:q,timestamp:r.timestamp,value,label:`P50 · ${time(r.timestamp,calendar)} · ${format(value)}`})));
    return [...observed,...forecast];
  }
  function annotationRows(state) {
    const rows=state.notes.map(n=>({...n,label:n.text}));
    if(state.highLow){for(const series of ['history','0.5']){const data=state.points.filter(p=>p.series===series);if(!data.length)continue;const prefix=series==='history'?'Observed':'P50 forecast';const high=data.reduce((a,b)=>a.value>=b.value?a:b),low=data.reduce((a,b)=>a.value<=b.value?a:b);rows.push({...high,label:`${prefix} high`},{...low,label:`${prefix} low`});}}
    return rows;
  }
  function plotAnnotations(job) {
    const state=states.get(job.forecast_id);if(!state)return [];
    const dark=document.documentElement.dataset.theme==='dark';
    return annotationRows(state).map(n=>({x:n.series==='history'&&window.QuanturaForecastControls?window.QuanturaForecastControls.stockChartTimestamp(n,job):n.timestamp,y:n.value,xref:'x',yref:'y',text:escape(n.label),showarrow:true,arrowhead:2,ax:20,ay:-35,bgcolor:dark?'#101d31':'#ffffff',bordercolor:'#64748b',arrowcolor:'#64748b',font:{size:11,color:dark?'#edf3fb':'#102033'},captureevents:false}));
  }
  function refreshAnnotations(state) {
    if(state.chart && window.Plotly && state.chart.data)void window.Plotly.relayout(state.chart,{annotations:plotAnnotations(state.job)});
    state.onAnnotations?.(annotationRows(state));
  }
  function renderNotes(state,host) {
    const details=el('details','','forecast-note-controls'),summary=el('summary','Chart annotations & notes');details.append(summary);
    const toggleLabel=el('label','','jev-high-low'),toggle=el('input');toggle.type='checkbox';toggle.checked=state.highLow;toggle.addEventListener('change',()=>{state.highLow=toggle.checked;refreshAnnotations(state);});toggleLabel.append(toggle,el('span','Mark observed and P50 high / low'));details.append(toggleLabel);
    const form=el('form','','jev-note-form'),label=el('label','Point'),select=el('select');select.id=`jev-point-${state.id}`;label.htmlFor=select.id;
    state.points.forEach((p,i)=>{const option=el('option',p.label);option.value=String(i);select.append(option);});
    const textLabel=el('label','Note'),input=el('input');input.id=`jev-note-${state.id}`;input.maxLength=240;input.required=true;textLabel.htmlFor=input.id;
    const add=el('button','Add note','cta secondary small');add.type='submit';add.disabled=true;form.append(label,select,textLabel,input,add);details.append(form);
    const list=el('ul','','jev-notes-list'),status=el('p','','small');status.setAttribute('role','status');details.append(list);
    const save=el('button','Save notes','cta secondary small');save.type='button';save.disabled=true;details.append(save,status);
    const draw=()=>{list.replaceChildren();for(const n of state.notes){const row=el('li'),remove=el('button','Remove','task-chip');remove.type='button';remove.setAttribute('aria-label',`Remove note: ${n.text}`);remove.addEventListener('click',()=>{state.notes=state.notes.filter(note=>note.id!==n.id);save.disabled=false;draw();refreshAnnotations(state);});row.append(el('span',`${n.text} · ${n.series==='history'?'Observed':'P50'} · ${time(n.timestamp,/^(1D|1W-MON|1MS)$/.test(state.job.frequency))} · ${format(n.value)}`),remove);list.append(row);}};
    form.addEventListener('submit',event=>{event.preventDefault();const point=state.points[Number(select.value)];if(add.disabled||!point||!input.value.trim())return;if(state.notes.length>=20){status.textContent='Use up to 20 notes per forecast.';return;}state.notes.push({id:uuid(),text:input.value.trim(),timestamp:point.timestamp,series:point.series,value:point.value});input.value='';save.disabled=false;draw();refreshAnnotations(state);});
    save.addEventListener('click',async()=>{save.disabled=true;status.textContent='Saving notes…';try{await state.request('/api/v1/jev/annotations/save',{body:{context:state.reference,notes:state.notes.map(({id,text,timestamp,series})=>({id,text,timestamp,series}))},signal:state.controller.signal});if(state.disposed)return;status.textContent='Notes saved to your account.';}catch(error){if(state.disposed)return;save.disabled=false;status.textContent=error.message;}});
    // One read when a context opens. Chart redraws and typing never write to Firestore.
    void state.request('/api/v1/jev/annotations/read',{body:{context:state.reference},signal:state.controller.signal}).then(result=>{if(state.disposed)return;state.notes=result.data?.notes || [];draw();refreshAnnotations(state);}).catch(()=>{}).finally(()=>{if(!state.disposed)add.disabled=false;});
    host.append(details);
  }
  function attach(host,{job,reference,request=transport,chart=null,onAnnotations=null}={}) {
    if(!host||!job||!reference)return;
    const signature=JSON.stringify(reference)+String(job.completed_at||job.generated_at||'');
    if(host.__jevState?.signature===signature && !host.__jevState.disposed)return host.__jevState;
    dispose(host);host.hidden=false;
    const id=job.forecast_id || JSON.stringify(reference),state={id,signature,reference,request,chart,onAnnotations,job,controller:new AbortController(),disposed:false,conversationId:uuid(),messages:[],notes:[],points:points(job),highLow:false,pending:null};
    states.set(id,state);host.__jevState=state;host.className='forecast-jev';
    const heading=el('div','','jev-heading'),title=el('h3',job.predictions?.length?'Ask Jev about this forecast':'Ask Jev about your data');title.prepend(icon());heading.append(title,el('p',job.source?.name||job.title||job.source?.symbol||'Your time series','small muted'));
    const cards=el('div','','jev-suggestions');for(const suggestion of suggestions(job)){const button=el('button','','jev-suggestion');button.type='button';button.append(icon(),el('span',suggestion.category,'jev-suggestion-category'),el('span',suggestion.question));button.addEventListener('click',()=>{input.value=suggestion.question;form.requestSubmit();});cards.append(button);}
    const topics=el('details','','jev-topics');topics.append(el('summary','Explore more questions'));const topicList=el('div','','jev-topic-list');
    const qs=Object.keys(job.predictions?.[0]?.quantiles || {}).sort((a,b)=>Number(a)-Number(b));
    const prompts=[...qs.map(q=>[`P${Number(q)*100}`,`What is the maximum P${Number(q)*100} across this forecast?`]),['Scenario range','How does the forecast range change over the horizon?'],['Historical variability','What is the historical variability of the observed values?'],['Data quality','What are the input cutoff, observation count and frequency?'],['Validation','Is there a saved historical validation report?'],['Strategy research','What evidence would I need to backtest an entry below P01?']];
    for(const [name,prompt]of prompts){const button=el('button',name,'task-chip');button.type='button';button.addEventListener('click',()=>{input.value=prompt;input.focus();});topicList.append(button);}topics.append(topicList);
    const log=el('div','','jev-conversation');log.setAttribute('role','log');log.setAttribute('aria-live','polite');log.setAttribute('aria-relevant','additions');log.setAttribute('aria-label','Jev forecast conversation');
    const form=el('form','','jev-question-form'),label=el('label','Your question'),input=el('textarea'),submit=el('button','Ask Jev','cta'),error=el('p','','jev-error small'),status=el('p','','small muted');
    input.id=`jev-question-${uuid()}`;input.name='question';input.required=true;input.maxLength=1500;input.rows=2;input.placeholder='Ask about a quantile, past prices, sources or models…';label.htmlFor=input.id;
    error.id=`jev-error-${uuid()}`;error.setAttribute('role','alert');status.setAttribute('role','status');input.setAttribute('aria-describedby',error.id);submit.type='submit';form.append(label,input,submit,error,status);
    const reset=el('button','New conversation','task-chip');reset.type='button';reset.addEventListener('click',()=>{if(state.pending?.busy)return;state.conversationId=uuid();state.messages=[];state.pending=null;log.replaceChildren();status.textContent='New conversation. Earlier saved conversations remain in Requests.';});
    form.addEventListener('submit',async event=>{
      event.preventDefault();if(state.pending?.busy || !input.value.trim())return;
      const question=input.value.trim();state.pending=state.pending?.question===question?state.pending:{question,turnId:uuid()};state.pending.busy=true;
      error.textContent='';input.removeAttribute('aria-invalid');submit.disabled=true;form.setAttribute('aria-busy','true');cards.querySelectorAll('button').forEach(button=>button.disabled=true);status.textContent='Reading the saved forecast…';
      try{const result=await request('/api/v1/jev/forecast-questions',{body:{context:reference,question,conversation_id:state.conversationId,turn_id:state.pending.turnId},signal:state.controller.signal});if(state.disposed)return;
        const message=result.data;state.conversationId=message.conversation_id;state.messages.push(message);log.append(messageNode(message));input.value='';status.textContent='Saved in Requests.';state.pending=null;document.dispatchEvent(new CustomEvent('quantura:request-saved'));
      }catch(cause){if(state.disposed)return;error.textContent=cause.message;input.setAttribute('aria-invalid','true');status.textContent='Your question is ready to retry.';state.pending.busy=false;
      }finally{if(!state.disposed){submit.disabled=false;form.removeAttribute('aria-busy');cards.querySelectorAll('button').forEach(button=>button.disabled=false);}}
    });
    input.addEventListener('input',()=>{input.removeAttribute('aria-invalid');error.textContent='';});
    host.replaceChildren(heading,cards,topics,log,form,reset,el('p','AI can sometimes make mistakes. Please check important info.','small muted'));
    if(state.points.length && (chart||onAnnotations))renderNotes(state,host);
    return state;
  }
  function dispose(host) {const state=host?.__jevState;if(!state)return;state.disposed=true;state.controller.abort();if(states.get(state.id)===state)states.delete(state.id);host.__jevState=null;}
  function clear(){for(const state of states.values()){state.disposed=true;state.controller.abort();}states.clear();document.querySelectorAll('.forecast-jev').forEach(host=>{host.hidden=true;host.__jevState=null;});}
  async function restore(conversationId,request=transport) {
    const result=await request(`/api/v1/jev/conversations/${encodeURIComponent(conversationId)}`,{method:'GET'}),saved=result.data;
    const dialog=el('dialog','','jev-saved-dialog'),close=el('button','Close','cta secondary small');close.type='button';close.addEventListener('click',()=>dialog.close());
    dialog.append(close,el('h2',`${saved.title} · Jev`));for(const message of saved.messages || [])dialog.append(messageNode(message));
    const ref=saved.context_reference;let url;
    if(ref.kind==='ensemble')url=`/forecasting?panel=forecast&ensembleForecastId=${encodeURIComponent(ref.id)}`;
    if(ref.kind==='game'||ref.kind==='saved_game')url=`/forecasting?panel=forecast&${ref.kind==='game'?'gameForecastId':'userGameForecastId'}=${encodeURIComponent(ref.id)}`;
    if(ref.kind==='screener')url=`/forecasting?panel=forecast&screenerTicker=${encodeURIComponent(ref.symbol)}&screenerScan=${encodeURIComponent(ref.scan_id)}`;
    if(url){const link=el('a','Open forecast','cta');link.href=url;dialog.append(link);}
    dialog.append(el('p','AI can sometimes make mistakes. Please check important info.','small muted'));document.body.append(dialog);dialog.addEventListener('close',()=>dialog.remove(),{once:true});dialog.addEventListener('click',event=>{const b=dialog.getBoundingClientRect();if(event.target===dialog&&(event.clientX<b.left||event.clientX>b.right||event.clientY<b.top||event.clientY>b.bottom))dialog.close();});dialog.showModal();
  }
  window.addEventListener('quantura:market-selected',event=>{if(event.detail?.intent==='forecast')clear();});
  window.QuanturaForecastQA=Object.freeze({attach,dispose,clear,restore,plotAnnotations,annotationRows});
})();
