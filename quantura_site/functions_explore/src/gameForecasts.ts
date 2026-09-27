import type { Router } from "express";
import admin from "firebase-admin";
import crypto from "node:crypto";
import {authenticatePlatformRequest} from "./apiAccess";
import {gameMarketUrl, gameMarketPrices} from "./gameMarketQuotes";
import {observedGameHistory, gameHistory} from "./gameHistory";

export function gameDate(now = Date.now()): string {
  return new Intl.DateTimeFormat("en-CA", {timeZone:"America/New_York",year:"numeric",month:"2-digit",day:"2-digit"}).format(new Date(now));
}
const validTime = (value:unknown): value is string => typeof value === "string" && Number.isFinite(Date.parse(value));
const text = (value:unknown, max=300) => typeof value === "string" ? value.slice(0,max) : "";

/** Only a small explicit public projection; private worker fields never escape. */
export function publicGameForecast(raw:Record<string,unknown>, now=Date.now(), detail=false, archive=false):Record<string,unknown>|null {
  if (!/^[a-f0-9]{32}$/.test(text(raw.id)) || !["kalshi","polymarket_us"].includes(text(raw.provider)) ||
      !validTime(raw.game_start) || raw.game_date !== gameDate(Date.parse(raw.game_start)) ||
      (archive ? Date.parse(raw.game_start)<now-3*86400000 || Date.parse(raw.game_start)>now+86400000 : raw.game_date !== gameDate(now)) ||
      !validTime(raw.forecast_end) || Date.parse(raw.forecast_end) !== Date.parse(raw.game_start)+4*3600000 ||
      !validTime(raw.generated_at) || (!validTime(raw.recomputed_at) && Date.parse(raw.generated_at) >= Math.floor(Date.parse(raw.game_start)/3600000)*3600000) ||
      Date.parse(raw.generated_at)>now || !validTime(raw.input_cutoff) || Date.parse(raw.input_cutoff)>Date.parse(raw.generated_at)) return null;
  if(raw.recomputed_at && (!validTime(raw.recomputed_at) || raw.recomputed_at!==raw.generated_at || !validTime(raw.original_generated_at) ||
      Date.parse(raw.original_generated_at)>=Math.floor(Date.parse(raw.game_start)/3600000)*3600000 || Date.parse(raw.input_cutoff)>Date.parse(raw.original_generated_at)))return null;
  const requested=["0.01","0.25","0.5","0.75","0.9","0.99"];
  const keys=raw.schema_version===2?requested:["0.1","0.5","0.9"];
  if(raw.schema_version===2 && (!Array.isArray(raw.models) || raw.models.length<4 || raw.models.length>5 ||
      new Set(raw.models).size!==raw.models.length || raw.models.some(m=>!["prophet","granite","chronos","toto","timesfm"].includes(String(m))) ||
      Number(raw.history_count)<2 || !validTime(raw.schedule_verified_at) || Date.parse(raw.schedule_verified_at)>now))return null;
  const predictions = Array.isArray(raw.predictions) ? raw.predictions : [];
  if (!predictions.length || predictions.length>512) return null;
  const rows:{timestamp:string;quantiles:Record<string,number>;interpolated_model_point:boolean}[]=[];
  let prior=Date.parse(raw.input_cutoff);
  for(const candidate of predictions) {
    if(!candidate || typeof candidate!=="object")return null;
    const row=candidate as Record<string,unknown>;
    if(!validTime(row.timestamp) || Date.parse(row.timestamp)<=prior || Date.parse(row.timestamp)>Date.parse(raw.forecast_end) || !row.quantiles || typeof row.quantiles!=="object")return null;
    const q=row.quantiles as Record<string,unknown>;
    const quantiles:Record<string,number>={};
    let lower=-1;
    for(const key of keys) {
      const value=q[key];
      if(typeof value!=="number" || !Number.isFinite(value) || value<lower || value<0 || value>1)return null;
      quantiles[key]=value;lower=value;
    }
    rows.push({timestamp:row.timestamp,quantiles,interpolated_model_point:row.interpolated_model_point===true});
    prior=Date.parse(row.timestamp);
  }
  if(rows.at(-1)?.timestamp!==raw.forecast_end)return null;
  const base:Record<string,unknown>={id:raw.id,provider:raw.provider,event_title:text(raw.event_title),outcome:text(raw.outcome),
    symbol:text(raw.symbol,220),contract_id:text(raw.contract_id,220),market_id:text(raw.market_id,220),event_id:text(raw.event_id,220),event_slug:text(raw.event_slug,220),
    market_url:gameMarketUrl(raw),
    side:["yes","no","long","short"].includes(text(raw.side))?raw.side:raw.provider==="kalshi"?text(raw.contract_id).split(":").at(-1):null,
    market_title:text(raw.market_title,320),market_type:text(raw.market_type,120),sport:text(raw.sport,80),league:text(raw.league,100),
    home_team:text(raw.home_team,160),away_team:text(raw.away_team,160),
    game_date:raw.game_date,game_start:raw.game_start,forecast_end:raw.forecast_end,generated_at:raw.generated_at,
    recomputed_at:validTime(raw.recomputed_at)?raw.recomputed_at:null,
    schedule_verified_at:validTime(raw.schedule_verified_at)?raw.schedule_verified_at:null,schedule_source:text(raw.schedule_source,120),
    input_cutoff:raw.input_cutoff,history_count:Number(raw.history_count)||0,
    models:Array.isArray(raw.models)?raw.models.map(m=>text(m,40)).slice(0,5):[],
    status:now>=Math.floor(Date.parse(raw.game_start)/3600000)*3600000?"final_pregame":"updating_pregame",
    endpoint:rows.at(-1)?.quantiles};
  if(detail)Object.assign(base,{symbol:text(raw.symbol,220),contract_id:text(raw.contract_id,220),
    history_start:validTime(raw.history_start)?raw.history_start:null,history_gap_count:Number(raw.history_gap_count)||0,
    method:text(raw.method,600),warnings:Array.isArray(raw.warnings)?raw.warnings.map(w=>text(w,600)).slice(0,20):[],predictions:rows,observations:observedGameHistory(raw)});
  return base;
}

export function registerGameForecastRoutes(router:Router,db:FirebaseFirestore.Firestore,auth?:admin.auth.Auth):void {
  let prices: {date:string; until:number; promise:Promise<Record<string,unknown>>} | undefined;
  const signedIn=async(req:any,res:any)=>{
    try {
    if(!auth)throw Error("authentication_unavailable");
    const principal=await authenticatePlatformRequest(req,{db,auth});
    if(principal.guest || principal.authMethod!=="firebase_session")throw Error("sign_in_required");
    return principal.userId;
    } catch {res.status(401).json({error:"sign_in_required"});return null;}
  };
  router.get("/screener/games/saved/:id",async(req,res)=>{
    try {
      const uid=await signedIn(req,res);if(!uid)return;const id=String(req.params.id);
      if(!/^[a-f0-9]{40}$/.test(id)){res.status(404).json({error:"not_found"});return;}
      const doc=await db.collection("user_game_forecasts").doc(id).get();
      if(!doc.exists||doc.data()?.ownerUid!==uid){res.status(404).json({error:"not_found"});return;}
      res.setHeader("Cache-Control","private, no-store");res.json({item:doc.data()!.forecast});
    }catch{res.status(503).json({error:"saved_forecast_unavailable"});}
  });
  router.post("/screener/games/:id/save",async(req,res)=>{
    try {
      const uid=await signedIn(req,res);if(!uid)return;const id=String(req.params.id);
      if(!/^[a-f0-9]{32}$/.test(id)){res.status(404).json({error:"not_found"});return;}
      const raw=await db.collection("game_forecast_catalog").doc(id).get();
      const forecast=raw.exists?publicGameForecast(raw.data()||{},Date.now(),true,true):null;
      if(!forecast){res.status(404).json({error:"not_found"});return;}
      const savedId=crypto.createHash("sha256").update(JSON.stringify([uid,id,forecast.generated_at,forecast.input_cutoff])).digest("hex").slice(0,40);
      const saved=db.collection("user_game_forecasts").doc(savedId),requestId=`game__${savedId}`;
      const request=db.collection("users").doc(uid).collection("requests").doc(requestId);
      const now=new Date(),title=[forecast.event_title,forecast.outcome].filter(Boolean).join(" · ").slice(0,180);
      await db.runTransaction(async tx=>{
        const [existing,existingRequest]=await Promise.all([tx.get(saved),tx.get(request)]);
        if(!existing.exists)tx.create(saved,{ownerUid:uid,forecast,saved_at:now.toISOString()});
        // Reopening preserves rename, archive and deletion choices; it never overwrites a saved snapshot.
        if(!existingRequest.exists)tx.create(request,{type:"forecast",ownerUid:uid,workspaceId:uid,title,
          input:{provider:forecast.provider,market_symbol:forecast.symbol,outcome:forecast.outcome,panel:"forecast"},
          outputsMeta:{status:"completed",summary:`${(forecast.models as string[]).length} models · Saved pregame probability forecast`},
          sourceRef:{collection:"user_game_forecasts",id:savedId},published:false,deleted:false,visibility:"private",share:{visibility:"private",slug:""},
          searchText:title.toLowerCase(),createdAt:now,updatedAt:now});
      });
      res.setHeader("Cache-Control","private, no-store");res.json({saved:true,id:savedId,request_id:requestId,url:`/forecasting?panel=forecast&userGameForecastId=${savedId}`});
    }catch{res.status(503).json({error:"saved_forecast_unavailable"});}
  });
  router.get("/screener/games",async(req,res)=>{
    const cursor=String(req.query.cursor||"");
    if(cursor&&!/^[a-f0-9]{32}$/.test(cursor)){res.status(400).json({error:"invalid_cursor"});return;}
    try {
      const date=gameDate();
      let query=db.collection("game_forecast_catalog").where("game_date","==",date).orderBy(admin.firestore.FieldPath.documentId());
      if(cursor)query=query.startAfter(cursor);
      const [games,status]=await Promise.all([
        query.limit(501).get(),
        db.collection("game_forecast_status").get(),
      ]);
      const page=games.docs.slice(0,500),nextCursor=games.size>500?page.at(-1)!.id:null;
      const items=page.flatMap(doc=>{const publicRow=publicGameForecast(doc.data());return publicRow?[publicRow]:[];})
        .sort((a,b)=>String(a.game_start).localeCompare(String(b.game_start))||String(a.event_title).localeCompare(String(b.event_title)));
      const current=status.docs.map(doc=>({...doc.data(),id:doc.id})).filter((data:any)=>data.game_date===date && ["kalshi","polymarket_us"].includes(data.provider));
      const coverage=["kalshi","polymarket_us"].flatMap(provider=>{
        const all=current.filter((d:any)=>d.provider===provider),sharded=all.filter((d:any)=>d.shards>1);
        const selected=sharded.length?sharded:all;
        if(!selected.length)return [];
        return [{provider,updated_at:selected.map((d:any)=>text(d.updated_at,50)).sort().at(-1),eligible:selected.reduce((n,d:any)=>n+(Number(d.eligible)||0),0),successful:selected.reduce((n,d:any)=>n+(Number(d.successful)||0),0),failed:selected.reduce((n,d:any)=>n+(Number(d.failed)||0),0),partial:selected.some((d:any)=>d.partial===true)||sharded.length>0&&sharded.length<Number((sharded[0] as any).shards)}];
      });
      res.setHeader("Cache-Control","public, max-age=30, s-maxage=30");
      res.json({date,time_zone:"America/New_York",items,coverage,next_cursor:nextCursor,bounded:nextCursor!==null});
    }catch{res.status(503).json({error:"games_unavailable",message:"Game forecasts are temporarily unavailable."});}
  });
  router.get("/screener/games/prices",async(_req,res)=>{
    try {
      const date=gameDate();
      if(!prices || prices.date!==date || prices.until<Date.now()) {
        const promise=(async()=>{
          const data=await db.collection("game_forecast_catalog").where("game_date","==",date).limit(10001).get();
          const rows=data.docs.slice(0,10000).flatMap(doc=>{const row=publicGameForecast(doc.data());return row?[row]:[];});
          const items=await gameMarketPrices(rows);
          return {date,items,bounded:data.size>10000,missing:items.filter(item=>item.latest_price===null).length};
        })();
        const entry={date,until:Date.now()+60000,promise};prices=entry;
        promise.catch(()=>{if(prices===entry)prices=undefined;});
      }
      res.setHeader("Cache-Control","public, max-age=30, s-maxage=30");res.json(await prices.promise);
    }catch{res.status(503).json({error:"game_prices_unavailable"});}
  });
  router.get("/screener/games/saved/:id/history",async(req,res)=>{
    try {
      const uid=await signedIn(req,res);if(!uid)return;const id=String(req.params.id);
      if(!/^[a-f0-9]{40}$/.test(id)){res.status(404).json({error:"not_found"});return;}
      const doc=await db.collection("user_game_forecasts").doc(id).get();
      if(!doc.exists||doc.data()?.ownerUid!==uid){res.status(404).json({error:"not_found"});return;}
      res.setHeader("Cache-Control","private, no-store");res.json(await gameHistory(doc.data()!.forecast));
    }catch{res.status(503).json({error:"game_history_unavailable"});}
  });
  router.get("/screener/games/:id/history",async(req,res)=>{
    const id=String(req.params.id);if(!/^[a-f0-9]{32}$/.test(id)){res.status(404).json({error:"not_found"});return;}
    try {
      const doc=await db.collection("game_forecast_catalog").doc(id).get();
      const item=doc.exists?publicGameForecast(doc.data()||{},Date.now(),true,true):null;
      if(!item){res.status(404).json({error:"not_found"});return;}
      res.setHeader("Cache-Control","public, max-age=30, s-maxage=30");res.json(await gameHistory(item));
    }catch{res.status(503).json({error:"game_history_unavailable"});}
  });
  router.get("/screener/games/:id",async(req,res)=>{
    const id=String(req.params.id);
    if(!/^[a-f0-9]{32}$/.test(id)){res.status(404).json({error:"not_found"});return;}
    try {
      const doc=await db.collection("game_forecast_catalog").doc(id).get();
      const item=doc.exists?publicGameForecast(doc.data()||{},Date.now(),true,true):null;
      if(!item){res.status(404).json({error:"not_found",message:"This forecast is no longer in today’s screener."});return;}
      res.setHeader("Cache-Control","public, max-age=30, s-maxage=30");res.json({item});
    }catch{res.status(503).json({error:"games_unavailable"});}
  });
}
