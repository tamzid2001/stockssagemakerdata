const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const script=fs.readFileSync(path.join(__dirname,'../../public/quantura-auth.js'),'utf8');
const tick=()=>new Promise(resolve=>setTimeout(resolve,5));
function setup({signedIn=true,restored=false,native=false}={}) {
 const d=new JSDOM('<body><div class="nav-actions"><span id="header-user-status">Guest Session</span><button id="header-auth">Sign in</button></div><section id="auth"><div class="container"><div class="auth-grid">Old auth</div></div></section><div data-clerk-user-profile></div><div data-trial-days="14"><button data-action="purchase">Start trial</button></div><div data-clerk-pricing hidden></div><div id="toast"></div></body>',{url:'https://quantura.studio/',runScripts:'outside-only'}),w=d.window;
 const calls={bridge:0,signOut:0,legacyClick:0,signIn:0,tokens:[],pricing:[],signInOptions:[]};
 w.HTMLElement.prototype.scrollIntoView=()=>{};
 const session={id:'sess_Unit',getToken:async()=> 'clerk-session-unit'};
 const user={id:'user_Unit',primaryEmailAddress:{emailAddress:'tamzid257@gmail.com',verification:{status:'verified'}}};
 const auth={currentUser:{uid:'old_uid',isAnonymous:false,getIdToken:async()=> 'firebase-sdk-unit',getIdTokenResult:async()=>({claims:restored?{clerk_user_id:'user_Unit',clerk_session_id:'sess_Unit'}:{}})},signOut:async()=>{calls.signOut++;auth.currentUser=null;},signInWithCustomToken:async token=>{calls.tokens.push(token);auth.currentUser={uid:'legacy_uid',isAnonymous:false};}};
 w.firebase={auth:()=>auth};w.__QUANTURA_NATIVE_APP__=native;
 w.Clerk={user:signedIn?user:null,session:signedIn?session:null,organization:null,load:async()=>{},addListener:fn=>{w.updateClerk=fn;},mountSignIn:()=>{},mountUserButton:()=>{},mountUserProfile:()=>{},mountPricingTable:(_host,options)=>calls.pricing.push(options),openSignIn:options=>{calls.signIn++;calls.signInOptions.push(options);},signOut:async()=>{w.updateClerk({user:null,session:null,organization:null});}};
 const append=w.document.head.append.bind(w.document.head);
 w.document.head.append=(node)=>{append(node);if(node.tagName==='SCRIPT')queueMicrotask(()=>node.onload());};
 w.fetch=async()=>{calls.bridge++;return {ok:true,json:async()=>({data:{uid:'legacy_uid',custom_token:'firebase-custom-unit'}})};};
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
