/* On-demand support. History is scoped to the current identity on this device. */
(() => {
  "use strict";
  const MAX_CHATS=20, MAX_MESSAGES=40, RETENTION=30*86400000;
  let dialog,list,input,status,submit,historyPanel,historyList,historyButton;
  let controller,generation=0,identity,storageKey,activeId,messages=[],chats=[];
  const element=(tag,text,className)=>{const n=document.createElement(tag);if(text)n.textContent=text;if(className)n.className=className;return n;};
  function safeReferences(references) {
    return (Array.isArray(references)?references:[]).slice(0,8).flatMap(reference=>{
      try {const url=new URL(reference.url);
        if(url.protocol!=="https:"||!["quantura.studio","quantura.mintlify.app"].includes(url.hostname)||url.username||url.password)return [];
        return [{title:String(reference.title||"Documentation").slice(0,200),url:url.href}];
      }catch{return [];}
    });
  }
  function addMessage(role,text,references=[]) {
    const row=element("section","","support-message support-message-"+role);
    row.append(element("strong",role==="user"?"You":"Support"),element("p",text));
    const links=element("div","","support-references");
    for(const ref of safeReferences(references)){const a=element("a",ref.title);a.href=ref.url;a.target="_blank";a.rel="noopener noreferrer";links.append(a);}
    if(links.childElementCount)row.append(links);
    list.append(row);list.scrollTop=list.scrollHeight;
  }
  function renderMessages() {
    list.replaceChildren();
    if(!messages.length)addMessage("assistant","How can I help? Ask about forecasts, workspaces, downloads, or API access.");
    for(const m of messages)addMessage(m.role,m.content,m.references);
  }
  function persist() {
    if(!storageKey)return;
    try {chats=chats.filter(c=>c.updated>Date.now()-RETENTION).slice(0,MAX_CHATS);
      if(chats.length)localStorage.setItem(storageKey,JSON.stringify(chats));else localStorage.removeItem(storageKey);
    }catch{status.textContent="History is available for this visit. This browser cannot save it.";}
  }
  function cancelRequest(){generation++;controller?.abort();controller=null;if(submit)submit.disabled=false;}
  function toggleHistory(show=historyPanel.hidden){historyPanel.hidden=!show;list.hidden=show;historyButton.setAttribute("aria-expanded",String(show));if(show)renderHistory();}
  function newChat(closeHistory=true){cancelRequest();activeId=null;messages=[];input.value="";status.textContent="";renderMessages();if(closeHistory)toggleHistory(false);renderHistory();}
  function renderHistory() {
    historyButton.textContent="History"+(chats.length?" ("+chats.length+")":"");historyList.replaceChildren();
    if(!chats.length)historyList.append(element("p","Your conversations will appear here.","small"));
    for(const chat of chats) {
      const row=element("div","","support-history-row"),open=element("button","","support-history-open");open.type="button";
      open.append(element("strong",chat.title),element("small",new Date(chat.updated).toLocaleDateString(undefined,{month:"short",day:"numeric"})));
      open.setAttribute("aria-current",String(chat.id===activeId));
      open.addEventListener("click",()=>{cancelRequest();activeId=chat.id;messages=chat.messages.map(m=>({...m}));input.value="";status.textContent="";renderMessages();toggleHistory(false);input.focus();});
      const remove=element("button","Delete","support-history-delete");remove.type="button";remove.setAttribute("aria-label","Delete conversation: "+chat.title);
      remove.addEventListener("click",()=>{chats=chats.filter(c=>c.id!==chat.id);if(activeId===chat.id)newChat(false);persist();renderHistory();});
      row.append(open,remove);historyList.append(row);
    }
  }
  function changeIdentity(user) {
    const next=user?.uid||null;if(identity===next)return;
    identity=next;storageKey=next?"quantura_support_v1:"+next:null;chats=[];
    if(storageKey)try {
      const stored=JSON.parse(localStorage.getItem(storageKey)||"[]");
      if(Array.isArray(stored))chats=stored.filter(c=>c&&typeof c.id==="string"&&typeof c.title==="string"&&Number.isFinite(c.updated)&&c.updated>Date.now()-RETENTION&&Array.isArray(c.messages)&&c.messages.length&&c.messages.length<=MAX_MESSAGES&&c.messages.length%2===0&&c.messages.every((m,i)=>m&&m.role===(i%2?"assistant":"user")&&typeof m.content==="string"&&m.content.length<=3000)).slice(0,MAX_CHATS).map(c=>({...c,title:c.title.slice(0,60),messages:c.messages.map(m=>({role:m.role,content:m.content,references:safeReferences(m.references)}))}));
    }catch{/* Unavailable storage never prevents help. */}
    newChat();persist();
  }
  function saveConversation() {
    activeId ||= window.crypto?.randomUUID?.()||"chat-"+Date.now()+"-"+Math.random().toString(36).slice(2);
    const chat={id:activeId,title:messages.find(m=>m.role==="user")?.content.slice(0,60)||"Conversation",updated:Date.now(),messages:messages.slice(-MAX_MESSAGES)};
    chats=[chat,...chats.filter(c=>c.id!==activeId)].slice(0,MAX_CHATS);persist();renderHistory();
  }
  async function send(event) {
    event.preventDefault();const question=input.value.trim();if(!question||controller)return;
    const user=window.firebase?.auth?.()?.currentUser;
    if(!user){status.textContent="Initializing a secure guest session. Retry shortly.";return;}
    changeIdentity(user);
    if(/(?:qnt_live_|apikey_|xkeysib-|sk-|hf_|mint_)[A-Za-z0-9_\-]{12,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|Bearer\s+\S{20,}/i.test(question)){status.textContent="Please remove credentials before sending.";return;}
    const turn=generation;controller=new AbortController();const request=controller,timeout=setTimeout(()=>request.abort(),65000);
    toggleHistory(false);submit.disabled=true;status.textContent="Finding an answer…";
    const outgoing=[...messages.slice(-8).map(({role,content})=>({role,content})),{role:"user",content:question}],userRowIndex=list.childElementCount;
    addMessage("user",question);input.value="";
    try {
      const token=await user.getIdToken();
      const response=await fetch("/api/support/chat",{method:"POST",credentials:"same-origin",signal:request.signal,headers:{"Content-Type":"application/json",Authorization:"Bearer "+token},body:JSON.stringify({messages:outgoing})});
      const payload=await response.json().catch(()=>({}));if(turn!==generation)return;
      if(!response.ok)throw new Error([401,422,429,503].includes(response.status)?payload.error?.message||"Support is unavailable. Please retry.":"Support is unavailable. Please retry or contact us.");
      if(typeof payload.data?.answer!=="string"||!Array.isArray(payload.data?.references))throw new Error("The reply could not be displayed. Please retry.");
      messages=[...messages,{role:"user",content:question},{role:"assistant",content:payload.data.answer,references:safeReferences(payload.data.references)}].slice(-MAX_MESSAGES);
      addMessage("assistant",payload.data.answer,payload.data.references);status.textContent="";saveConversation();
    }catch(error){if(turn!==generation)return;status.textContent=error.name==="AbortError"?"The request timed out. Please retry or contact support.":error.message;list.children[userRowIndex]?.remove();input.value=question;}
    finally{clearTimeout(timeout);if(turn===generation){controller=null;submit.disabled=false;if(dialog.open)input.focus();}}
  }
  function createDialog() {
    dialog=element("dialog","","support-dialog");dialog.id="quantura-support";dialog.setAttribute("aria-labelledby","support-title");dialog.setAttribute("data-cs-mask","");dialog.setAttribute("data-cs-exclude","");
    const header=element("header","","support-header"),heading=element("div"),title=element("h2","Support");title.id="support-title";heading.append(title,element("p","Quantura product help","small"));
    const close=element("button","×","support-close");close.type="button";close.setAttribute("aria-label","Close support");close.addEventListener("click",()=>dialog.close());header.append(heading,close);
    const toolbar=element("div","","support-toolbar"),start=element("button","New conversation","cta secondary small");start.type="button";start.addEventListener("click",()=>{newChat();input.focus();});
    historyButton=element("button","History","cta secondary small");historyButton.type="button";historyButton.setAttribute("aria-controls","support-history");historyButton.setAttribute("aria-expanded","false");historyButton.addEventListener("click",()=>toggleHistory());toolbar.append(start,historyButton);
    historyPanel=element("section","","support-history");historyPanel.id="support-history";historyPanel.hidden=true;historyPanel.append(element("h3","Recent conversations"),element("p","Saved on this device for 30 days.","small"));historyList=element("div","","support-history-list");historyPanel.append(historyList);
    list=element("div","","support-messages");list.setAttribute("role","log");list.setAttribute("aria-live","polite");list.setAttribute("aria-label","Support conversation");
    const form=element("form","","support-form"),label=element("label","Message");label.htmlFor="support-question";
    input=element("textarea");input.id="support-question";input.rows=2;input.maxLength=1000;input.placeholder="Ask about Quantura…";input.setAttribute("autocomplete","off");input.setAttribute("aria-describedby","support-privacy");
    input.addEventListener("keydown",e=>{if(e.key==="Enter"&&!e.shiftKey&&!e.isComposing){e.preventDefault();form.requestSubmit();}});
    submit=element("button","Send","cta support-send");submit.type="submit";form.append(label,input,submit);form.addEventListener("submit",send);
    status=element("p","","support-status small");status.setAttribute("role","status");
    const privacy=element("p","Don’t share passwords or private data. History is saved on this device.","support-privacy small");privacy.id="support-privacy";
    const note=element("p","AI can sometimes make mistakes. Please check important info.","support-accuracy small"),footer=element("footer","","support-footer");
    for(const [text,href]of[["Documentation","https://quantura.mintlify.app/"],["Contact support","/contact"],["Privacy","/privacy"]]){const a=element("a",text);a.href=href;footer.append(a);}
    dialog.append(header,toolbar,list,historyPanel,form,status,privacy,note,footer);document.body.append(dialog);
    changeIdentity(window.firebase?.auth?.()?.currentUser);window.firebase?.auth?.()?.onAuthStateChanged?.(changeIdentity);
  }
  window.QuanturaSupport=Object.freeze({open(launcher){if(!dialog)createDialog();else changeIdentity(window.firebase?.auth?.()?.currentUser);if(!dialog.open)dialog.showModal();dialog.onclose=()=>launcher?.focus();input.focus();}});
})();
