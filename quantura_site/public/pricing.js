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
  document.querySelector('[data-promo-regular-checkout]')?.addEventListener('click',()=>window.QuanturaAuth?.showPricing({withPromotion:false}).catch(error=>{const note=document.querySelector('.purchase-note');if(note)note.textContent=error.message;}));
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
  const panel=document.querySelector("[data-trial-days]"),button=panel?.querySelector('[data-action="purchase"]'),billingCopy=document.querySelector("[data-trial-billing-copy]");
  let authSequence=0;
  if(window.firebase?.auth)firebase.auth().onAuthStateChanged(async user=>{
    const run=++authSequence;if(!panel || !button)return;
    panel.dataset.trialDays="14";panel.dataset.subscriptionActive="false";button.dataset.labelAuth=button.dataset.labelGuest="Start 14-day free trial";
    if(billingCopy)billingCopy.textContent=offerActive?"14 days free, then 50% off your first paid month or year with WELCOME50 for eligible new subscribers. Renews at $199.99/month or $1,999.92/year. Cancel before the trial ends to avoid a charge.":"14 days free, then $199.99/month or $1,999.92/year. Cancel before the trial ends to avoid a charge.";
    if(!user || user.isAnonymous)return;
    try{
      const token=await (window.QuanturaAuth?.getToken(user) ?? user.getIdToken());
      const response=await fetch("/api/shop/subscription-access",{headers:{Authorization:`Bearer ${token}`,Accept:"application/json"},cache:"no-store"});
      if(!response.ok)return;
      const {data}=await response.json();if(run!==authSequence)return;
      const billingLink=document.getElementById('billing-portal-link');if(billingLink){billingLink.href='/forecasting?panel=profile';billingLink.textContent='Manage subscription';}
      if(!data.can_trial || data.pro_available){
        panel.dataset.subscriptionActive=String(data.pro_available);panel.dataset.trialDays="0";button.dataset.labelAuth="Get Pro";
        if(!button.disabled)button.textContent=data.pro_available?"Pro is active":"Get Pro";
        button.disabled=data.pro_available;
        const note=panel.querySelector('.purchase-note');if(data.pro_available && note)note.textContent='Manage your subscription in Clerk Account settings.';
        if(billingCopy)billingCopy.textContent=data.pro_available?"Your Pro access is active. Manage your trial or subscription below.":"$199.99/month or $1,999.92/year. Billed at checkout. Cancel renewal in Manage subscription.";
      }
    }catch{} // Checkout independently rechecks eligibility server-side.
  });
})();
