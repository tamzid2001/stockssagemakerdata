/* Existing GA4 web identifiers only. No API secret, queries, uploaded rows or forecast values. */
(() => {
  'use strict';
  const measurement='G-R9Y1C8WBKS';
  const accepted=()=>{try{return localStorage.getItem('quantura_cookie_consent')==='accepted' && navigator.globalPrivacyControl!==true;}catch{return false;}};
  function sanitizedLocation(){return location.origin+location.pathname;}
  function configureTag(){
    window['ga-disable-'+measurement]=!accepted();
    if(typeof window.gtag==='function'){
      window.gtag('consent','update',{analytics_storage:accepted()?'granted':'denied',ad_storage:'denied',ad_user_data:'denied',ad_personalization:'denied'});
      window.gtag('set',{page_location:sanitizedLocation()});
    }
    // Editorial pages do not load Firebase Analytics. Use the same web stream there.
    if(!accepted() || window.firebase?.analytics || document.querySelector('script[data-quantura-ga4]'))return;
    window.dataLayer=window.dataLayer||[];window.gtag=window.gtag||function(){window.dataLayer.push(arguments);};
    window.gtag('consent','default',{analytics_storage:'granted',ad_storage:'denied',ad_user_data:'denied',ad_personalization:'denied'});
    window.gtag('js',new Date());
    let referrer='';try{const r=new URL(document.referrer);referrer=r.origin+r.pathname;}catch{}
    window.gtag('config',measurement,{page_location:sanitizedLocation(),page_referrer:referrer});
    const script=document.createElement('script');script.async=true;script.dataset.quanturaGa4='true';script.src='https://www.googletagmanager.com/gtag/js?id='+measurement;document.head.append(script);
  }
  async function syncConsent(){
    try {
      const user=window.firebase?.auth?.().currentUser;if(!user)return false;
      await window.firebase.firestore().collection('users').doc(user.uid).set({analyticsConsent:accepted()?'accepted':'denied',analyticsConsentUpdatedAt:new Date().toISOString()},{merge:true});
      if(typeof window.gtag==='function')window.gtag('consent','update',{analytics_storage:accepted()?'granted':'denied',ad_storage:'denied',ad_user_data:'denied',ad_personalization:'denied'});
      return true;
    }catch{return false;}
  }
  const get=key=>new Promise(resolve=>{
    if(typeof window.gtag!=='function'){resolve(null);return;}
    let done=false;const finish=v=>{if(done)return;done=true;clearTimeout(timer);resolve(v);};
    const timer=setTimeout(()=>finish(null),750);
    try{window.gtag('get',measurement,key,finish);}catch{finish(null);}
  });
  async function capture(){
    if(!accepted() || !await syncConsent())return null;
    const [client,session]=await Promise.all([get('client_id'),get('session_id')]);
    if(!accepted() || !/^\d{1,20}\.\d{1,20}$/.test(String(client)) || !Number.isSafeInteger(Number(session)))return null;
    return {analytics_consent:'granted',client_id:String(client),session_id:Number(session),consent_at:new Date().toISOString()};
  }
  function event(name){
    if(!accepted())return;
    const params={video_id:'quantura_product_tour',page_location:sanitizedLocation()};
    try{if(window.firebase?.analytics)window.firebase.analytics().logEvent(name,params);else window.gtag?.('event',name,params);}catch{}
  }
  function init(){
    configureTag();
    try{window.firebase?.auth?.().onAuthStateChanged(()=>syncConsent());}catch{}
    document.querySelectorAll('video[data-lazy-video]').forEach(video=>{video.addEventListener('play',()=>event('product_tour_started'));video.addEventListener('ended',()=>event('product_tour_completed'));});
  }
  document.addEventListener('quantura:consent-change',()=>{configureTag();syncConsent();});
  window.QuanturaGa4=Object.freeze({capture,sanitizedLocation});
  window['ga-disable-'+measurement]=!accepted();
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init,{once:true});else init();
})();
