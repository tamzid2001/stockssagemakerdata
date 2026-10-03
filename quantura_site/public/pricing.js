(() => {
  "use strict";
  const dialog=document.getElementById("enterprise-dialog"),open=document.getElementById("enterprise-open"),close=document.getElementById("enterprise-close");
  if(!dialog || !open || !close)return;
  open.addEventListener("click",()=>dialog.showModal());
  close.addEventListener("click",()=>dialog.close());
  dialog.addEventListener("close",()=>open.focus());
})();
