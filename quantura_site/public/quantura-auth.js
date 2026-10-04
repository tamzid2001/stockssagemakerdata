/* Clerk owns web sign-in and API sessions. The Firebase credential is only for
 * the existing Firestore/Storage SDK rules during the data-access migration. */
(() => {
  "use strict";
  const native=Boolean(window.__QUANTURA_NATIVE_APP__ || window.Capacitor?.isNativePlatform?.());
  const publishableKey="pk_live_Y2xlcmsucXVhbnR1cmEuc3R1ZGlvJA";
  const domain="https://clerk.quantura.studio";
  let clerk,bridgeKey=null,bridgePromise=Promise.resolve(),workspaceId=null,accessKey="",apiKeysAvailable=false;
  const appearance={variables:{colorPrimary:"#18b6a4",borderRadius:"0.65rem",fontFamily:"Inter, system-ui, sans-serif"},elements:{rootBox:{width:"100%"},cardBox:{boxShadow:"none"}}};
  const api={enabled:!native,ready:null,get organizationId(){return clerk?.organization?.id||null;},get workspaceId(){return workspaceId;},get signedIn(){return Boolean(clerk?.user);},
    get verifiedEmail(){const email=clerk?.user?.primaryEmailAddress;return email?.verification?.status==="verified"?email.emailAddress:null;},
    async getToken(fallbackUser){if(native || (fallbackUser?.isAnonymous && !clerk?.session))return fallbackUser?.getIdToken() || "";await api.ready;if(clerk?.session)return clerk.session.getToken();return fallbackUser?.isAnonymous ? fallbackUser.getIdToken() : "";},
    async signIn(redirectUrl="/forecasting?panel=profile"){await api.ready;clerk.openSignIn({afterSignInUrl:redirectUrl,afterSignUpUrl:redirectUrl});},
    async signOut(){await api.ready;await clerk.signOut();},
    async showPricing(){await api.ready;if(!clerk?.user){await api.signIn("/pricing");return;}const host=document.querySelector("[data-clerk-pricing]");if(host){host.hidden=false;component("[data-clerk-pricing]","PricingTable",{for:"user",newSubscriptionRedirectUrl:"/forecasting?panel=profile",checkoutProps:{appearance}});host.scrollIntoView({behavior:"smooth",block:"center"});}else location.assign("/forecasting?panel=profile");},
  };
  window.QuanturaAuth=api;
  function script(path) {return new Promise((resolve,reject)=>{const node=document.createElement("script");node.src=domain+path;node.crossOrigin="anonymous";node.async=true;const timeout=setTimeout(()=>reject(Error("Account sign-in could not load. Please retry.")),15000);node.onload=()=>{clearTimeout(timeout);resolve();};node.onerror=()=>{clearTimeout(timeout);reject(Error("Account sign-in could not load. Please retry."));};document.head.append(node);});}
  async function sync(resources) {
    const key=resources.session?.id||"";
    const auth=window.firebase?.auth?.();
    if(auth && bridgeKey!==key) {
      if(key){
        const restored=await auth.currentUser?.getIdTokenResult?.().catch(()=>null);
        if(restored?.claims?.clerk_session_id===key && restored.claims.clerk_user_id===resources.user?.id)bridgeKey=key;
        else {
        const token=await resources.session.getToken();
        const response=await fetch("/api/auth/clerk/session",{method:"POST",headers:{Authorization:`Bearer ${token}`,"Content-Type":"application/json"},credentials:"same-origin",body:"{}"});
        if(!response.ok)throw Error("Your account could not be restored. Please sign in again.");
        const {data}=await response.json();
        await auth.signInWithCustomToken(data.custom_token);bridgeKey=key;
        }
      }else {if(auth.currentUser && !auth.currentUser.isAnonymous)await auth.signOut();bridgeKey="";}
    }
    workspaceId=resources.organization?.id||null;
    const accessIdentity=resources.session?.id||"";
    if(accessKey!==accessIdentity){
      apiKeysAvailable=false;accessKey=accessIdentity;
      if(resources.session)try{const token=await resources.session.getToken();const response=await fetch("/api/shop/subscription-access",{headers:{Authorization:`Bearer ${token}`},credentials:"same-origin"});if(response.ok){const result=await response.json();apiKeysAvailable=result.data?.api_keys_available===true;}}catch{}
    }
    document.body.dataset.clerkState=resources.user?"signed-in":"signed-out";
    document.dispatchEvent(new CustomEvent("quantura:organization",{detail:{id:workspaceId}}));
    mount(resources);
  }
  const mounted=new WeakMap();
  function component(selector,type,options={}) {
    for(const host of document.querySelectorAll(selector))if(!mounted.has(host)) {
      clerk[`mount${type}`](host,{appearance,...options});mounted.set(host,type);
    }
  }
  function mount(resources) {
    document.body.classList.add("clerk-web");
    const authSection=document.getElementById("auth");
    if(authSection && !authSection.querySelector("[data-clerk-sign-in]")) {
      const host=document.createElement("div");host.dataset.clerkSignIn="";host.className="clerk-account-card";authSection.querySelector(".container")?.append(host);
    }
    if(!resources.user) {
      if(new URLSearchParams(location.search).get("auth")==="sign-up")component("[data-clerk-sign-in]","SignUp",{routing:"hash",signInUrl:"/forecasting?panel=profile",forceRedirectUrl:"/forecasting?panel=profile"});
      else component("[data-clerk-sign-in]","SignIn",{routing:"hash",signUpUrl:"/forecasting?panel=profile&auth=sign-up",forceRedirectUrl:"/forecasting?panel=profile"});
    }
    const header=document.querySelector(".nav-actions");
    // Clerk's account menu is the web identity display; old session badges
    // must not reappear when the data SDK restores its anonymous credential.
    document.querySelectorAll("#header-user-status, #header-user-email, #header-signout").forEach(host=>host.remove());
    document.querySelectorAll('#dashboard-auth-link, [data-auth-nav="true"]').forEach(host=>host.remove());
    if(header && !header.querySelector("#header-auth")) {
      const signin=document.createElement("button");signin.id="header-auth";signin.type="button";signin.className="cta secondary";signin.textContent="Sign in";header.append(signin);
    }
    const signin=header?.querySelector("#header-auth");
    if(signin)signin.hidden=Boolean(resources.user);
    if(header && !header.querySelector("[data-clerk-user-button]")) {
      const host=document.createElement("div");host.dataset.clerkUserButton="";header.append(host);
    }
    if(resources.user) {
      component("[data-clerk-user-button]","UserButton",{userProfileUrl:"/forecasting?panel=profile",userProfileMode:"navigation",userProfileProps:{apiKeysProps:{hide:!apiKeysAvailable}}});
      component("[data-clerk-organization-switcher]","OrganizationSwitcher",{hidePersonal:false,afterSelectOrganizationUrl:location.pathname+location.search,afterSelectPersonalUrl:location.pathname+location.search});
      component("[data-clerk-user-profile]","UserProfile",{routing:"hash",apiKeysProps:{hide:!apiKeysAvailable}});
      if(resources.organization)component("[data-clerk-organization-profile]","OrganizationProfile",{routing:"hash",apiKeysProps:{hide:true}});
    }
  }
  // Capture old web auth actions before the legacy/native handlers run.
  document.addEventListener("click",event=>{
    if(native)return;
    const signin=event.target.closest("#header-auth, #google-signin, #email-create, #auth-forgot-password");
    const signout=event.target.closest("#header-signout");
    const purchase=event.target.closest('[data-trial-days] [data-action="purchase"]');
    if(purchase) {
      event.preventDefault();event.stopImmediatePropagation();
      api.showPricing().catch(showError);return;
    }
    if(signin){event.preventDefault();event.stopImmediatePropagation();if(clerk?.user)location.assign("/forecasting?panel=profile");else api.signIn().catch(showError);}
    else if(signout){event.preventDefault();event.stopImmediatePropagation();api.signOut().catch(showError);}
  },true);
  function showError(error) {const host=document.getElementById("auth-email-message")||document.getElementById("toast");if(host){host.textContent=error.message;host.classList.add("show");}}
  api.ready=(async()=>{
    if(native)return;
    document.body.classList.add("clerk-web");
    await script("/npm/@clerk/ui@1.38.1/dist/ui.browser.js");
    const tag=document.createElement("script");tag.dataset.clerkPublishableKey=publishableKey;tag.src=domain+"/npm/@clerk/clerk-js@6.37.0/dist/clerk.browser.js";tag.async=true;tag.crossOrigin="anonymous";
    await new Promise((resolve,reject)=>{const timeout=setTimeout(()=>reject(Error("Account sign-in could not load. Please retry.")),15000);tag.onload=()=>{clearTimeout(timeout);resolve();};tag.onerror=()=>{clearTimeout(timeout);reject(Error("Account sign-in could not load. Please retry."));};document.head.append(tag);});
    clerk=window.Clerk;
    await clerk.load({ui:{ClerkUI:window.__internal_ClerkUICtor},appearance});
    await sync({user:clerk.user,session:clerk.session,organization:clerk.organization});
    clerk.addListener(resources=>{bridgePromise=bridgePromise.then(()=>sync(resources)).catch(showError);});
  })();
  api.ready.catch(showError);
})();
