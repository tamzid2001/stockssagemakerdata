/* Company domains are exact ticker matches; only the fixed logo CDN is used. */
(() => {
  'use strict';
  const domains=new Map(Object.entries({"AAPL":"apple.com","MSFT":"microsoft.com","GOOGL":"abc.xyz","GOOG":"abc.xyz","AMZN":"aboutamazon.com","NVDA":"nvidia.com","META":"meta.com","TSLA":"tesla.com","NFLX":"netflix.com","AVGO":"broadcom.com","WMT":"stock.walmart.com","BRK-B":"berkshirehathaway.com","JPM":"jpmorganchase.com","V":"usa.visa.com","MA":"mastercard.com","ORCL":"oracle.com","AMD":"amd.com","PLTR":"palantir.com","SPY":"spdrs.com","QQQ":"invesco.com"})),queued=new WeakSet(),queue=[];let running=false,lookupAt=0,cooldown=0;
  const escape=value=>String(value||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const providers={kalshi:'kalshi.com',polymarket_us:'polymarket.com',kalshi_perps:'kalshi.com'};
  const valid=value=>typeof value==='string' && /^([a-z0-9][a-z0-9-]*\.)+[a-z]{2,24}$/.test(value);
  function markup(resource={}) {
    const provider=providers[resource.provider||resource.source];
    let symbol=String(resource.symbol||resource.ticker||'').toUpperCase();
    if(resource.source==='dukascopy')symbol=/\.US-USD$/.test(symbol)?symbol.replace(/\.US-USD$/,''):'';
    if(!provider && !/^[A-Z0-9][A-Z0-9.^=-]{0,23}$/.test(symbol))return '';
    return `<span class="market-logo" aria-hidden="true" ${provider?`data-logo-domain="${provider}"`:`data-logo-symbol="${escape(symbol)}"`}>${escape((provider?'':symbol).slice(0,2))}</span>`;
  }
  async function domainFor(symbol) {
    if(domains.has(symbol))return domains.get(symbol);
    try {const saved=JSON.parse(sessionStorage.getItem(`quantura-logo:${symbol}`)||'null');if(saved&&Date.now()-saved.at<86400000&&valid(saved.domain)){domains.set(symbol,saved.domain);return saved.domain;}}catch{}
    if(Date.now()<cooldown)return null;
    await new Promise(resolve=>setTimeout(resolve,Math.max(0,lookupAt+1250-Date.now())));lookupAt=Date.now();
    const response=await fetch(`/api/market-data/company-logo/${encodeURIComponent(symbol)}`,{signal:AbortSignal.timeout(10000)});
    if(response.status===429){cooldown=Date.now()+Math.max(60000,(Number(response.headers.get('Retry-After'))||60)*1000);return null;}
    if(!response.ok)return null;
    const data=await response.json(),domain=data.symbol===symbol&&valid(data.domain)?data.domain:null;
    domains.set(symbol,domain);if(domain)try{sessionStorage.setItem(`quantura-logo:${symbol}`,JSON.stringify({at:Date.now(),domain}));}catch{}
    return domain;
  }
  async function drain() {
    if(running)return;running=true;
    try {while(queue.length){const node=queue.shift();if(!node.isConnected)continue;
      try {const domain=node.dataset.logoDomain||await domainFor(node.dataset.logoSymbol);if(!valid(domain))continue;
        const img=document.createElement('img');img.alt='';img.width=28;img.height=28;img.decoding='async';img.referrerPolicy='no-referrer';
        img.addEventListener('error',()=>{img.remove();node.textContent=(node.dataset.logoSymbol||'').slice(0,2);},{once:true});
        img.src=domain==='kalshi.com'?'/assets/logo-kalshi.png':domain==='polymarket.com'?'/assets/logo-polymarket.png':`https://cdn.tickerlogos.com/${domain}`;node.replaceChildren(img);
      }catch{}
      // Below the CDN's 30 requests / 10 seconds burst allowance.
      await new Promise(resolve=>setTimeout(resolve,450));
    }}finally{running=false;}
  }
  const observer=typeof IntersectionObserver==='function'?new IntersectionObserver(entries=>{for(const entry of entries)if(entry.isIntersecting){observer.unobserve(entry.target);queue.push(entry.target);}drain();},{rootMargin:'120px'}):null;
  function hydrate(root=document) {for(const node of root.querySelectorAll('.market-logo[data-logo-symbol],.market-logo[data-logo-domain]'))if(!queued.has(node)){queued.add(node);observer?observer.observe(node):queue.push(node);}if(!observer)drain();}
  window.QuanturaLogos=Object.freeze({markup,hydrate});
  new MutationObserver(records=>{for(const record of records)for(const node of record.addedNodes)if(node.nodeType===1){if(node.matches?.('.market-logo'))hydrate(node.parentElement);else hydrate(node);}}).observe(document.body,{childList:true,subtree:true});
  hydrate();
})();
