import { createHash, randomUUID } from "node:crypto";
import type { Router, Request, Response } from "express";
import type admin from "firebase-admin";
import { authenticatePlatformRequest, requireScope, type ApiPrincipal } from "./apiAccess";
import { BrevoNotificationMailer, FirestoreEmailDeliveryLedger, type NotificationEmail } from "./brevoEmail";
import { parseQuantScreenerQuery, rowMatchesQuery, type QuantScreenerDataset, type QuantScreenerQuery, type QuantScreenerRow } from "./quantScreener";
import { forecastRows, newYorkDate, type SavedScreenerSignal } from "./screenerSignals";

type Options = { db: FirebaseFirestore.Firestore; auth: admin.auth.Auth; publicOrigin: string };
export type SavedScreenerAlert = { id:string; name:string; filters:QuantScreenerQuery; email:boolean; created_at:string };
const COLLECTION = "screener_saved_alerts";
const hash = (s:string) => createHash("sha256").update(s).digest("hex");
const escape = (s:unknown) => String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]!));

export function requireAlertAccount(principal: ApiPrincipal, write:boolean): void {
  if (principal.guest) throw new Error("ACCOUNT_REQUIRED");
  requireScope(principal,write ? "alerts:write" : "alerts:read");
}
export function parseSavedAlert(body:unknown, now=new Date().toISOString()):SavedScreenerAlert {
  if (!body || typeof body!=="object" || Array.isArray(body)) throw new Error("INVALID_ALERT");
  const value=body as Record<string,unknown>;
  if(Object.keys(value).some(k=>!["name","filters","email"].includes(k)) ||
    typeof value.name!=="string" || !value.name.trim() || value.name.length>80 || /[\x00-\x1f]/.test(value.name) ||
    typeof value.email!=="boolean" || !value.filters || typeof value.filters!=="object" || Array.isArray(value.filters)) throw new Error("INVALID_ALERT");
  const raw=value.filters as Record<string,unknown>;
  if (Object.keys(raw).some(k=>!["search","universe","marketCap","minMarketCap","maxMarketCap","signal","signalChanged","quantileRules","positions","sort","direction","page","pageSize","statistic"].includes(k))) throw new Error("INVALID_FILTERS");
  // Null numeric bounds are absent in persisted normalized filters.
  const parsed=parseQuantScreenerQuery({...raw,position:raw.positions,minMarketCap:raw.minMarketCap??"",maxMarketCap:raw.maxMarketCap??"",page:1,pageSize:100});
  if(parsed.errors.length)throw new Error("INVALID_FILTERS");
  const filters=parsed.query;
  return {id:hash(JSON.stringify(filters)).slice(0,24),name:value.name.trim(),filters,email:value.email,created_at:now};
}

/** Digests use finalized exchange closes only, never after-hours provisional quotes. */
export function closingRows(items:QuantScreenerRow[], date:string):QuantScreenerRow[] {
  return items.flatMap(row=>{
    const state=row as QuantScreenerRow & SavedScreenerSignal;
    const signal=state.closing_signal;
    const levels=forecastRows(row).find(r=>r.date===date);
    if(!signal || signal.provisional || signal.forecast_date!==date || !levels || row.split_status==="requires_refresh")return [];
    const prior=signal.value==="neutral" ? state.last_non_neutral_signal : state.previous_non_neutral_signal;
    return [{...row,...levels,actual_price:signal.price,actual_price_timestamp:signal.quote_timestamp,signal:signal.value,last_non_neutral_signal:prior}];
  });
}
export function digestMatches(alerts:SavedScreenerAlert[], rows:QuantScreenerRow[], date:string) {
  return alerts.map(alert=>({alert,rows:rows.filter(row=>rowMatchesQuery(row,alert.filters,new Date(date+"T12:00:00Z")))})).filter(match=>match.rows.length>0);
}
export function buildScreenerDigest(uid:string,date:string,matches:ReturnType<typeof digestMatches>,origin:string):Omit<NotificationEmail,"to"> {
  const lines=matches.map(m=>`${m.alert.name}: ${m.rows.length} matches — ${m.rows.slice(0,30).map(r=>`${r.ticker} (${r.signal})`).join(", ")}${m.rows.length>30?"; more in screener":""}`);
  const url=origin+"/screener#saved-alerts";
  const text=[`Your Quantura closing screener matches · ${date}`,...lines,"Signals use finalized exchange minute closes; they are not recommendations or guaranteed outcomes.",`Review filters or stop notifications: ${url}`].join("\n\n");
  return {id:`screener-digest-${uid}-${date}`,subject:`Quantura screener matches · ${date}`,text,
    html:`<!doctype html><html><body style="font-family:Arial,sans-serif;color:#17202a;background:#f4f6f8;padding:20px"><main style="max-width:600px;margin:auto;background:#fff;padding:24px;border-radius:12px"><h1 style="font-size:20px">Your closing screener matches</h1><p>${escape(date)} · Finalized exchange closes</p>${lines.map(l=>`<p>${escape(l)}</p>`).join("")}<p><a href="${escape(url)}">View saved filters / stop notifications</a></p><p style="font-size:13px">Model-derived signals are not recommendations or guaranteed outcomes.</p></main></body></html>`};
}

export function registerScreenerAlertRoutes(router:Router,options:Options):void {
  const handle=(write:boolean,fn:(req:Request,res:Response,p:ApiPrincipal)=>Promise<void>)=>async(req:Request,res:Response)=>{
    const id=randomUUID();res.setHeader("Cache-Control","private, no-store");res.setHeader("X-Request-ID",id);
    try {const p=await authenticatePlatformRequest(req,options);requireAlertAccount(p,write);await fn(req,res,p);}
    catch(error){const raw=String((error as Error).message);const code=/^[A-Z_]+$/.test(raw)?raw:raw==="insufficient_scope"?"INSUFFICIENT_SCOPE":"REQUEST_FAILED";
      const status=/api_key|token|auth/i.test(raw)?401:code==="ACCOUNT_REQUIRED"||code==="INSUFFICIENT_SCOPE"?403:code==="ALERT_LIMIT"?429:code==="EMAIL_VERIFICATION_REQUIRED"?403:code==="NOT_FOUND"?404:code.startsWith("INVALID")?422:503;
      res.status(status).json({error:{code,message:code==="EMAIL_VERIFICATION_REQUIRED"?"Verify your account email before enabling email notifications.":code==="ACCOUNT_REQUIRED"?"Sign in to save filters and receive notifications.":"The saved-filter request could not be completed.",request_id:id}});}
  };
  router.get("/v1/me/screener-alerts",handle(false,async(_req,res,p)=>{
    const snap=await options.db.collection(COLLECTION).doc(p.userId).get();res.json({data:Object.values(snap.data()?.alerts||{}),meta:{maximum:10,email_configured:process.env.NOTIFICATION_EMAIL_PROVIDER==="brevo"}});
  }));
  router.post("/v1/me/screener-alerts",handle(true,async(req,res,p)=>{
    const alert=parseSavedAlert(req.body);const user=await options.auth.getUser(p.userId);
    if(user.disabled)throw new Error("ACCOUNT_REQUIRED");
    if(alert.email && (!user.emailVerified||!user.email))throw new Error("EMAIL_VERIFICATION_REQUIRED");
    const ref=options.db.collection(COLLECTION).doc(p.userId);
    await options.db.runTransaction(async tx=>{const snap=await tx.get(ref);const alerts=snap.data()?.alerts||{};
      if(!alerts[alert.id]&&Object.keys(alerts).length>=10)throw new Error("ALERT_LIMIT");
      alerts[alert.id]={...alert,created_at:alerts[alert.id]?.created_at||alert.created_at};
      tx.set(ref,{alerts,updated_at:new Date().toISOString()});});res.status(201).json({data:alert});
  }));
  router.delete("/v1/me/screener-alerts/:alertId",handle(true,async(req,res,p)=>{
    const id=String(req.params.alertId);if(!/^[a-f0-9]{24}$/.test(id))throw new Error("INVALID_ALERT");
    const ref=options.db.collection(COLLECTION).doc(p.userId);
    await options.db.runTransaction(async tx=>{const snap=await tx.get(ref);const alerts=snap.data()?.alerts||{};if(!alerts[id])throw new Error("NOT_FOUND");delete alerts[id];tx.set(ref,{alerts,updated_at:new Date().toISOString()});});
    res.json({data:{id,removed:true}});
  }));
}

/** Bounded resumable daily sweep. One durable digest/user/day; no arbitrary recipients. */
export async function runScreenerDigests(options:Options,dataset:QuantScreenerDataset,items:QuantScreenerRow[],now=Date.now()) {
  const date=newYorkDate(new Date(now).toISOString());
  const ends=items.flatMap(r=>forecastRows(r).filter(f=>f.date===date).map(f=>Date.parse(f.session_close)));
  if(!ends.length || now<Math.max(...ends)+5*60_000)return {status:"waiting_for_exchange_close",processed:0};
  const rows=closingRows(items,date);if(!rows.length)return {status:"closing_quotes_unavailable",processed:0};
  const sweep=options.db.collection("screener_alert_sweeps").doc(date);const holder=randomUUID();
  const claimed=await options.db.runTransaction(async tx=>{const s=(await tx.get(sweep)).data()||{};if(s.done||Number(s.expires||0)>now)return null;tx.set(sweep,{...s,holder,expires:now+240_000});return {cursor:String(s.cursor||"")};});
  if(!claimed)return {status:"already_running_or_complete",processed:0};
  let query=options.db.collection(COLLECTION).orderBy("__name__").limit(5);if(claimed.cursor)query=query.startAfter(claimed.cursor);
  const docs=await query.get();const mailer=new BrevoNotificationMailer(new FirestoreEmailDeliveryLedger(options.db));let processed=0,emails=0,unavailable=0;
  for(const doc of docs.docs){
    const daily=options.db.collection("screener_daily_digests").doc(hash(`${date}:${doc.id}`));
    if(!(await daily.get()).exists){
      const user=await options.auth.getUser(doc.id).catch(()=>null);
      const alerts=Object.values(doc.data().alerts||{}) as SavedScreenerAlert[];
      const matches=user&&!user.disabled?digestMatches(alerts,rows,date):[];
      if(matches.length){
        const content=buildScreenerDigest(doc.id,date,matches,options.publicOrigin);
        const inbox=options.db.collection("notifications").doc(doc.id).collection("items").doc(hash(content.id));
        await options.db.runTransaction(async tx=>{if(!(await tx.get(inbox)).exists)tx.create(inbox,{category:"screener",title:content.subject,body:content.text,deepLink:"/screener#saved-alerts",read:false,hidden:false,createdAt:adminTimestamp(now),metadata:{scan_id:dataset.scan_id,date,matched_filters:matches.length}});});
        const emailMatches=matches.filter(m=>m.alert.email);let delivery="not_requested";
        if(emailMatches.length&&user?.emailVerified&&user.email){
          if(process.env.NOTIFICATION_EMAIL_PROVIDER!=="brevo")delivery="not_configured";
          else try{await mailer.send({...buildScreenerDigest(doc.id,date,emailMatches,options.publicOrigin),to:user.email});delivery="accepted";emails++;}
          catch(error){delivery=String((error as Error).message);if(!/^email_[a-z_]+$/.test(delivery))delivery="email_delivery_unknown";unavailable++;}
        }
        await daily.set({date,created_at:new Date().toISOString(),matched_filters:matches.length,email_status:delivery,scan_id:dataset.scan_id});
      }else await daily.set({date,created_at:new Date().toISOString(),matched_filters:0,email_status:"no_matches"});
    }
    processed++;
  }
  await options.db.runTransaction(async tx=>{const s=(await tx.get(sweep)).data()||{};if(s.holder!==holder)throw new Error("SWEEP_LEASE_LOST");tx.update(sweep,{cursor:docs.docs.at(-1)?.id||claimed.cursor,done:docs.size<5,expires:0});});
  return {status:"processed",processed,emails,unavailable,finalized_rows:rows.length};
}

// Existing inbox API orders by Firestore Timestamp, not an ISO-string field.
function adminTimestamp(now:number){return new Date(now);}
