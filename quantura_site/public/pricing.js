(() => {
  "use strict";
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
    if(billingCopy)billingCopy.textContent="14 days free, then $199.99/month or $1,999.92/year. Cancel before the trial ends to avoid a charge.";
    if(!user || user.isAnonymous)return;
    try{
      const token=await (window.QuanturaAuth?.getToken(user) ?? user.getIdToken());
      const response=await fetch("/api/shop/subscription-access",{headers:{Authorization:`Bearer ${token}`,Accept:"application/json"},cache:"no-store"});
      if(!response.ok)return;
      const {data}=await response.json();if(run!==authSequence)return;
      if(!data.can_trial || data.pro_available){
        panel.dataset.subscriptionActive=String(data.pro_available);panel.dataset.trialDays="0";button.dataset.labelAuth="Get Pro";
        if(!button.disabled)button.textContent=data.pro_available?"Pro is active":"Get Pro";
        button.disabled=data.pro_available;
        if(billingCopy)billingCopy.textContent=data.pro_available?"Your Pro access is active. Manage your trial or subscription below.":"$199.99/month or $1,999.92/year. Billed at checkout. Cancel renewal in Manage subscription.";
      }
    }catch{} // Checkout independently rechecks eligibility server-side.
  });
})();
