const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const script=fs.readFileSync(path.join(__dirname,'../../public/quantura-auth.js'),'utf8');
const tick=()=>new Promise(resolve=>setTimeout(resolve,5));
function setup({signedIn=true,restored=false,native=false,apiKeys=false,accessFails=false,promo=false,promoFails=false,planMissing=false,expired=false,startFails=false}={}) {
 const d=new JSDOM('<body><div class="nav-actions"><span id="header-user-status">Guest Session</span><button id="header-auth">Sign in</button></div><section id="auth"><div class="container"><div class="auth-grid">Old auth</div></div></section><div data-clerk-user-profile></div><div data-trial-days="14"><button data-action="purchase">Start trial</button></div><div data-clerk-pricing hidden></div><div id="toast"></div></body>',{url:'https://quantura.studio/',runScripts:'outside-only'}),w=d.window;
 if(promo){const panel=w.document.querySelector("[data-trial-days]");panel.dataset.purchasePanel="";panel.dataset.promoCode="WELCOME50";panel.dataset.promoExpires=expired?"2020-01-01T00:00:00Z":"2027-10-07T02:42:00Z";panel.insertAdjacentHTML("beforeend",'<p class="purchase-note" role="status"></p>');w.document.body.insertAdjacentHTML("beforeend",'<button data-billing-cycle="monthly" aria-pressed="true"></button><button data-billing-cycle="yearly" aria-pressed="false"></button><button data-promo-regular-checkout hidden></button>');}
 const calls={bridge:0,signOut:0,legacyClick:0,signIn:0,tokens:[],pricing:[],signInOptions:[],profiles:[],userButtons:[],access:0,starts:0,updates:[],drawers:[],flowOptions:[],confirms:0};
 w.HTMLElement.prototype.scrollIntoView=()=>{};
 const session={id:'sess_Unit',getToken:async()=> 'clerk-session-unit'};
 const user={id:'user_Unit',primaryEmailAddress:{emailAddress:'tamzid257@gmail.com',verification:{status:'verified'}}};
 const auth={currentUser:{uid:'old_uid',isAnonymous:false,getIdToken:async()=> 'firebase-sdk-unit',getIdTokenResult:async()=>({claims:restored?{clerk_user_id:'user_Unit',clerk_session_id:'sess_Unit'}:{}})},signOut:async()=>{calls.signOut++;auth.currentUser=null;},signInWithCustomToken:async token=>{calls.tokens.push(token);auth.currentUser={uid:'legacy_uid',isAnonymous:false};}};
 w.firebase={auth:()=>auth};w.__QUANTURA_NATIVE_APP__=native;
 w.Clerk={user:signedIn?user:null,session:signedIn?session:null,organization:null,load:async()=>{},addListener:fn=>{w.updateClerk=fn;},mountSignIn:()=>{},mountUserButton:(_h,p)=>calls.userButtons.push(p),mountUserProfile:(_h,p)=>calls.profiles.push(p),mountPricingTable:(_host,options)=>calls.pricing.push(options),openSignIn:options=>{calls.signIn++;calls.signInOptions.push(options);},signOut:async()=>{w.updateClerk({user:null,session:null,organization:null});}};
 const flow={status:"needs_initialization",totals:{},start:async()=>{calls.starts++;if(startFails)return {error:Error("provider unavailable")};flow.status="needs_confirmation";return {error:null};},update:async params=>{calls.updates.push(params);if(promoFails&&params.promoCode)return {error:Error("new subscribers only")};flow.totals={discounts:{discount:params.promoCode?{promoCode:"WELCOME50",percentOff:50,durationInCycles:1}:undefined}};return {error:null};},confirm:()=>{calls.confirms++;throw Error("must not purchase automatically");}};
 w.Clerk.billing={getPlans:async()=>({data:planMissing?[]:[{id:"plan_pro",slug:"pro"}]})};w.Clerk.__experimental_checkout=options=>{calls.flowOptions.push(options);return {checkout:flow};};w.Clerk.__internal_openCheckout=options=>calls.drawers.push(options);
 const append=w.document.head.append.bind(w.document.head);
 w.document.head.append=(node)=>{append(node);if(node.tagName==='SCRIPT')queueMicrotask(()=>node.onload());};
 w.fetch=async url=>{if(url.includes("subscription-access")){calls.access++;return {ok:!accessFails,json:async()=>({data:{api_keys_available:apiKeys}})};}calls.bridge++;return {ok:true,json:async()=>({data:{uid:'legacy_uid',custom_token:'firebase-custom-unit'}})};};
 w.document.getElementById('header-auth').addEventListener('click',()=>{calls.legacyClick++;});
 w.eval(script);return {d,w,calls,auth};
}
test('Clerk identity restores the imported UID but every API token stays a Clerk session',async()=>{
 const {d,w,calls,auth}=setup();await w.QuanturaAuth.ready;
 assert.equal(calls.bridge,1);assert.deepEqual(calls.tokens,['firebase-custom-unit']);assert.equal(auth.currentUser.uid,'legacy_uid');
 assert.equal(await w.QuanturaAuth.getToken(auth.currentUser),'clerk-session-unit');
 assert.equal(w.QuanturaAuth.verifiedEmail,'tamzid257@gmail.com');
 assert.equal(w.document.getElementById('header-user-status'),null);
 assert.equal(w.document.getElementById('header-auth').hidden,true);
 w.updateClerk({user:w.Clerk.user,session:w.Clerk.session,organization:{id:'org_Unit'}});await tick();
 assert.equal(calls.bridge,1);assert.equal(w.QuanturaAuth.workspaceId,'org_Unit');d.window.close();
});
test('restored matching SDK credential skips additional session-bridge database requests',async()=>{
 const {d,w,calls}=setup({restored:true});await w.QuanturaAuth.ready;assert.equal(calls.bridge,0);d.window.close();
});
test('signed-out Clerk clears a full Firebase login and captures old sign-in controls',async()=>{
 const {d,w,calls}=setup({signedIn:false});await w.QuanturaAuth.ready;assert.equal(calls.signOut,1);
 w.document.getElementById('header-auth').click();await tick();assert.equal(calls.signIn,1);assert.equal(calls.legacyClick,0);d.window.close();
});
test('native clients keep the existing native session without loading Clerk or minting a bridge',async()=>{
 const {d,w,calls,auth}=setup({native:true});await w.QuanturaAuth.ready;assert.equal(w.QuanturaAuth.enabled,false);assert.equal(calls.bridge,0);assert.equal(w.document.querySelectorAll('script').length,0);
 assert.equal(await w.QuanturaAuth.getToken(auth.currentUser),'firebase-sdk-unit');d.window.close();
});
test('monthly and annual Pro purchases require Clerk sign-in before displaying checkout',async()=>{
 for(const cycle of ['monthly','yearly']){
  const {d,w,calls}=setup({signedIn:false});await w.QuanturaAuth.ready;
  w.localStorage.setItem('quantura_pricing_cycle',cycle);
  const purchase=w.document.querySelector('[data-action="purchase"]');
  purchase.addEventListener('click',()=>calls.legacyClick++);purchase.click();await tick();
  assert.equal(calls.signIn,1);assert.equal(calls.legacyClick,0);assert.equal(calls.pricing.length,0);
  assert.equal(calls.signInOptions[0].afterSignInUrl,'/pricing');
  assert.equal(w.document.querySelector('[data-clerk-pricing]').hidden,true);d.window.close();
 }
});
test('both Pro cycles use one Clerk pricing component for the signed-in account',async()=>{
 const {d,w,calls}=setup();await w.QuanturaAuth.ready;
 for(const cycle of ['monthly','yearly']){w.localStorage.setItem('quantura_pricing_cycle',cycle);w.document.querySelector('[data-action="purchase"]').click();await tick();}
 assert.equal(calls.signIn,0);assert.equal(calls.pricing.length,1);assert.equal(calls.pricing[0].for,'user');
 assert.equal(calls.pricing[0].newSubscriptionRedirectUrl,'/forecasting?panel=profile');
 assert.equal(w.document.querySelector('[data-clerk-pricing]').hidden,false);d.window.close();
});

test('API keys use server eligibility, stay hidden for free/trial and on access errors, and appear for paid/admin accounts',async()=>{
 for(const options of [{apiKeys:false},{apiKeys:true},{apiKeys:true,accessFails:true}]){
  const {d,w,calls}=setup(options);await w.QuanturaAuth.ready;
  assert.equal(calls.profiles[0].apiKeysProps.hide,!options.apiKeys||options.accessFails===true);
  assert.equal(calls.userButtons[0].userProfileProps.apiKeysProps.hide,calls.profiles[0].apiKeysProps.hide);
  assert.equal(calls.access,1);d.window.close();
 }
});

test('new-member checkout applies WELCOME50 to the shared Clerk flow before opening either billing cycle',async()=>{
 for(const cycle of ['monthly','yearly']){
  const {d,w,calls}=setup({promo:true});await w.QuanturaAuth.ready;
  for(const button of w.document.querySelectorAll('[data-billing-cycle]'))button.setAttribute('aria-pressed',String(button.dataset.billingCycle===cycle));
  await w.QuanturaAuth.showPricing();
  assert.equal(calls.starts,1);assert.equal(calls.updates[0].promoCode,'WELCOME50');assert.equal(calls.drawers.length,1);
  assert.equal(calls.drawers[0].planPeriod,cycle==='yearly'?'annual':'month');assert.equal(calls.drawers[0].for,'user');
  assert.equal(calls.flowOptions[0].planId,'plan_pro');assert.equal(calls.pricing.length,0);assert.equal(calls.confirms,0);
  assert.match(w.document.querySelector('.purchase-note').textContent,/WELCOME50 applied/);d.window.close();
 }
});
test('a rejected promotion never silently opens a full-price checkout; regular pricing needs an explicit retry',async()=>{
 const {d,w,calls}=setup({promo:true,promoFails:true});await w.QuanturaAuth.ready;
 await assert.rejects(w.QuanturaAuth.showPricing(),/new subscribers only/);
 assert.equal(calls.drawers.length,0);assert.equal(w.document.querySelector('[data-promo-regular-checkout]').hidden,false);
 await w.QuanturaAuth.showPricing({withPromotion:false});assert.equal(calls.updates[1].promoCode,'');assert.equal(calls.drawers.length,1);
 assert.equal(calls.starts,1);assert.equal(calls.confirms,0);d.window.close();
});
test('promotion checkout is serialized and fails safely if the plan or checkout is unavailable',async()=>{
 const {d,w,calls}=setup({promo:true});await w.QuanturaAuth.ready;await Promise.all([w.QuanturaAuth.showPricing(),w.QuanturaAuth.showPricing()]);
 assert.equal(calls.starts,1);assert.equal(calls.drawers.length,1);d.window.close();
 for(const option of [{planMissing:true},{startFails:true}]){const {d,w,calls}=setup({promo:true,...option});await w.QuanturaAuth.ready;await assert.rejects(w.QuanturaAuth.showPricing());assert.equal(calls.drawers.length,0);assert.equal(calls.confirms,0);d.window.close();}
});
test('expired offers do not apply a promo code or advertise a discounted checkout',async()=>{
 const {d,w,calls}=setup({promo:true,expired:true});await w.QuanturaAuth.ready;await w.QuanturaAuth.showPricing();
 assert.equal(calls.updates[0].promoCode,'');assert.match(w.document.querySelector('.purchase-note').textContent,/Regular pricing/);assert.equal(calls.confirms,0);d.window.close();
});
test('an advertised promo cannot be shown as applied without verified Clerk discount totals',async()=>{
 const {d,w,calls}=setup({promo:true});await w.QuanturaAuth.ready;
 const flow=w.Clerk.__experimental_checkout({}).checkout;flow.update=async()=>({error:null});
 await assert.rejects(w.QuanturaAuth.showPricing(),/could not be verified/);assert.equal(calls.drawers.length,0);assert.equal(calls.confirms,0);d.window.close();
});
