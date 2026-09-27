import type { Router } from "express";

const cache=new Map<string,{at:number;domain:string|null}>();
const pending=new Map<string,Promise<string|null>>();
let requests:number[]=[],blockedUntil=0;
export function companyDomain(website:unknown):string|null {
  if(typeof website!=="string" || website.length>300)return null;
  try {
    const parsed=new URL(/^https?:\/\//i.test(website)?website:`https://${website}`);
    const domain=parsed.hostname.toLowerCase().replace(/^www\./,"");
    return /^([a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,24}$/.test(domain) && !parsed.username && !parsed.password?domain:null;
  }catch{return null;}
}
export async function companyLogo(symbol:string,request:typeof fetch=fetch):Promise<string|null> {
  const saved=cache.get(symbol);if(saved && Date.now()-saved.at<86400000)return saved.domain;
  if(pending.has(symbol))return pending.get(symbol)!;
  const now=Date.now();requests=requests.filter(t=>now-t<60000);
  if(now<blockedUntil || requests.length>=50)throw Error("LOGO_RATE_LIMIT");
  requests.push(now);
  const promise=(async()=>{
    const response=await request(`https://www.allinvestview.com/api/logo-search/?q=${encodeURIComponent(symbol)}`,{signal:AbortSignal.timeout(8000),headers:{Accept:"application/json"}});
    if(response.status===429){const retry=Number(response.headers.get("Retry-After"));blockedUntil=Date.now()+Math.max(60000,Number.isFinite(retry)?retry*1000:60000);throw Error("LOGO_RATE_LIMIT");}
    if(!response.ok)throw Error("LOGO_UNAVAILABLE");
    const data=await response.json() as any;
    // A fuzzy company search must never attach another company's logo.
    const exact=Array.isArray(data.results)?data.results.filter((r:any)=>typeof r.symbol==="string" && r.symbol.toUpperCase()===symbol):[];
    const domains=[...new Set(exact.map((r:any)=>companyDomain(r.website)).filter(Boolean))];
    const domain=domains.length===1?domains[0] as string:null;
    cache.set(symbol,{at:Date.now(),domain});while(cache.size>2000)cache.delete(cache.keys().next().value!);
    return domain;
  })().finally(()=>pending.delete(symbol));
  pending.set(symbol,promise);return promise;
}
export function registerCompanyLogoRoutes(router:Router):void {
  router.get("/market-data/company-logo/:symbol",async(req,res)=>{
    const symbol=String(req.params.symbol).toUpperCase();
    if(!/^[A-Z0-9][A-Z0-9.^=-]{0,23}$/.test(symbol)){res.status(422).json({error:"invalid_symbol"});return;}
    try {const domain=await companyLogo(symbol);res.setHeader("Cache-Control","public, max-age=86400, s-maxage=86400");res.json({symbol,domain});}
    catch(error){const limited=(error as Error).message==="LOGO_RATE_LIMIT";if(limited)res.setHeader("Retry-After","60");res.status(limited?429:503).json({error:limited?"logo_rate_limit":"logo_unavailable"});}
  });
}
