(() => {
  "use strict";
  const host=document.getElementById("api-docs-content"),status=document.getElementById("api-docs-status"),gate=document.getElementById("api-docs-gate");
  if(!host || !window.firebase?.auth)return;
  let sequence=0;
  firebase.auth().onAuthStateChanged(async user=>{
    const run=++sequence;host.replaceChildren();host.hidden=true;gate.hidden=false;
    if(!user || user.isAnonymous){status.textContent="Sign in with paid Pro, enterprise or administrator access to open your API documentation.";return;}
    status.textContent="Checking your API access…";
    try{
      const token=await (window.QuanturaAuth?.getToken(user) ?? user.getIdToken());
      const response=await fetch("/api/shop/api-docs",{headers:{Authorization:`Bearer ${token}`,Accept:"application/json"},cache:"no-store"});
      const payload=await response.json();if(run!==sequence)return;
      if(!response.ok)throw Error(payload.message || "API documentation requires paid Pro or enterprise access.");
      // This markup is the server-owned documentation, never provider input.
      host.innerHTML=payload.data.html;host.hidden=false;gate.hidden=true;status.textContent="";
      const script=document.createElement("script");script.src="https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.37.0";
      const specResponse=await fetch("/api/openapi.json",{headers:{Authorization:`Bearer ${token}`,Accept:"application/json"},cache:"no-store"});
      if(!specResponse.ok)throw Error("The API definition could not load.");
      const specification=await specResponse.json();
      if(run!==sequence)return;
      script.onload=()=>{if(run===sequence)window.Scalar?.createApiReference(document.getElementById("api-reference"),{content:specification,theme:"saturn",hideModels:false});};
      script.onerror=()=>{if(run===sequence)status.textContent="The interactive viewer is unavailable. The guides and OpenAPI download remain available.";};
      document.head.append(script);
    }catch(error){if(run===sequence)status.textContent=error.message;}
  });
})();
