(() => {
  "use strict";
  const offer=document.querySelector('[data-promo-banner]');
  const offerActive=offer && Date.now()<Date.parse(offer.dataset.promoExpires);
  if(offer && !offerActive)offer.hidden=true;
  const promoCopy=document.querySelector('[data-promo-copy]'),promoStatus=document.querySelector('[data-promo-copy-status]');
  promoCopy?.addEventListener('click',async()=>{
    try{await navigator.clipboard.writeText('WELCOME50');if(promoStatus)promoStatus.textContent='WELCOME50 copied.';}
    catch{if(promoStatus)promoStatus.textContent='Promo code: WELCOME50. It applies automatically at eligible checkout.';}
  });
  document.querySelector('[data-promo-regular-checkout]')?.addEventListener('click',()=>window.QuanturaAuth?.showPricing({plan:"pro",withPromotion:false}).catch(error=>{const note=document.querySelector('.purchase-note');if(note)note.textContent=error.message;}));
  const dialog=document.getElementById("enterprise-dialog"),open=document.getElementById("enterprise-open"),close=document.getElementById("enterprise-close");
  if(!dialog || !open || !close)return;
  open.addEventListener("click",()=>dialog.showModal());
  close.addEventListener("click",()=>dialog.close());
  // Only a press and release on the backdrop closes the form. Dragging out of
  // an input must not discard the visitor's work.
  let backdropPress=false;
  const outside=(event)=>{
    const rect=dialog.getBoundingClientRect();
    return event.target===dialog && (event.clientX<rect.left || event.clientX>rect.right || event.clientY<rect.top || event.clientY>rect.bottom);
  };
  dialog.addEventListener("pointerdown",event=>{backdropPress=outside(event);});
  dialog.addEventListener("pointerup",event=>{if(backdropPress && outside(event))dialog.close();backdropPress=false;});
  dialog.addEventListener("pointercancel",()=>{backdropPress=false;});
  dialog.addEventListener("close",()=>open.focus());
  const panel=document.querySelector('[data-metered-plan]'),button=panel?.querySelector('[data-action="purchase"]'),billingCopy=document.querySelector('[data-trial-billing-copy]');
  const accountHost=document.querySelector('[data-metered-account]'),budgetForm=document.querySelector('[data-metered-budget-form]');
  let authSequence=0,currentUser=null;
  const request=async(path,body)=>{
    const token=await window.QuanturaAuth.getToken(currentUser);
    const response=await fetch(path,{method:body?'POST':'GET',headers:{Authorization:`Bearer ${token}`,Accept:'application/json',...(body?{'Content-Type':'application/json'}:{})},cache:'no-store',...(body?{body:JSON.stringify(body)}:{})});
    const result=await response.json();if(!response.ok)throw Error(result.message||'Billing information could not load. Please retry.');return result;
  };
  const money=cents=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(cents/100);
  const renderUsage=data=>{
    if(!data){if(accountHost)accountHost.hidden=true;return;}
    accountHost.hidden=false;
    document.querySelector('[data-metered-usage]').textContent=data.trial_free?'Your trial usage is free. Paid usage begins after the 14-day trial.':`${data.completed_forecasts} completed forecasts · ${money(data.usage_cents)} before discounts and tax · ${data.pending_forecasts} pending`;
    budgetForm.elements.budget.value=data.monthly_budget_cents/100;
  };
  budgetForm?.addEventListener('submit',async event=>{
    event.preventDefault();const status=document.querySelector('[data-metered-budget-status]'),submit=budgetForm.querySelector('button');submit.disabled=true;
    try{const cents=Math.round(Number(budgetForm.elements.budget.value)*100);await request('/api/shop/metered-budget',{monthly_budget_cents:cents});status.textContent=`Monthly usage budget saved: ${money(cents)}. Completed usage remains payable.`;}
    catch(error){status.textContent=error.message;}finally{submit.disabled=false;}
  });
  if(window.firebase?.auth)firebase.auth().onAuthStateChanged(async user=>{
    const run=++authSequence;currentUser=user;if(!panel||!button)return;
    panel.dataset.trialDays='14';panel.dataset.subscriptionActive='false';button.disabled=false;button.textContent=button.dataset.labelAuth=button.dataset.labelGuest='Start 14-day free trial';
    const meterNote=panel.querySelector('.purchase-note');if(meterNote)meterNote.textContent='Sign in to start your 14-day free trial.';
    if(billingCopy)billingCopy.textContent='14 days free, then pay for completed forecasts. Cancel anytime.';
    const proPanel=document.querySelector('[data-pro-plan]'),proButton=proPanel?.querySelector('[data-action="purchase"]');
    if(proPanel){proPanel.dataset.trialDays='14';proPanel.dataset.subscriptionActive='false';if(proButton){proButton.disabled=false;proButton.textContent=proButton.dataset.labelAuth=proButton.dataset.labelGuest='Try Pro for 14 days';delete proButton.dataset.labelActive;}proPanel.querySelector('.purchase-note').textContent='Sign in before checkout.';}
    if(accountHost)accountHost.hidden=true;
    if(!user||user.isAnonymous)return;
    try{
      const {data}=await request('/api/shop/subscription-access');if(run!==authSequence)return;
      if(data.docs_available){
        panel.dataset.subscriptionActive='true';button.disabled=true;const proPanel=document.querySelector('[data-pro-plan]');if(proPanel){proPanel.dataset.subscriptionActive='true';proPanel.querySelector('[data-action="purchase"]').disabled=true;proPanel.querySelector('[data-action="purchase"]').textContent=proPanel.querySelector('[data-action="purchase"]').dataset.labelActive=data.plan==='metered'?'Your plan is active':'Pro is active';proPanel.querySelector('.purchase-note').textContent='Your current plan is active. Manage billing before changing plans.';}button.textContent=data.plan==='metered'?'Pay as you go is active':'Your access is active';
        const note=panel.querySelector('.purchase-note');if(note)note.textContent=data.plan==='metered'?'View your usage below or open Manage billing.':data.billing_provider==='stripe'?'Your existing plan stays unchanged. Open Manage billing to review it.':'Your existing plan stays unchanged. Manage it in Clerk Account settings.';
        if(billingCopy)billingCopy.textContent=data.subscription_status==='trialing'?'Your 14-day trial includes API and MCP access. Trial forecast usage is free.':data.plan==='metered'?'Completed custom forecasts cost $0.50 each, billed monthly.':'Your existing subscription retains its agreed pricing.';
      }else if(!data.can_trial){const proPanel=document.querySelector('[data-pro-plan]');if(proPanel){proPanel.dataset.trialDays='0';proPanel.querySelector('[data-action="purchase"]').dataset.labelAuth='Get Pro';}panel.dataset.trialDays='0';button.dataset.labelAuth='Start pay as you go';button.textContent='Start pay as you go';if(billingCopy)billingCopy.textContent='Your trial has been used. New completed forecasts cost $0.50 each, billed monthly.';}
      const usage=await request('/api/shop/metered-usage');if(run===authSequence)renderUsage(usage.data);
    }catch(error){const note=panel.querySelector('.purchase-note');if(note)note.textContent=error.message;}
  });
})();
