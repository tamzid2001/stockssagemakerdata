import type { Router } from "express";

export function gameDate(now = Date.now()): string {
  return new Intl.DateTimeFormat("en-CA", {timeZone:"America/New_York",year:"numeric",month:"2-digit",day:"2-digit"}).format(new Date(now));
}
const validTime = (value:unknown): value is string => typeof value === "string" && Number.isFinite(Date.parse(value));
const text = (value:unknown, max=300) => typeof value === "string" ? value.slice(0,max) : "";

/** Only a small explicit public projection; private worker fields never escape. */
export function publicGameForecast(raw:Record<string,unknown>, now=Date.now(), detail=false):Record<string,unknown>|null {
  if (!/^[a-f0-9]{32}$/.test(text(raw.id)) || !["kalshi","polymarket_us"].includes(text(raw.provider)) ||
      raw.game_date !== gameDate(now) || !validTime(raw.game_start) || gameDate(Date.parse(raw.game_start)) !== gameDate(now) ||
      !validTime(raw.forecast_end) || Date.parse(raw.forecast_end) !== Date.parse(raw.game_start)+4*3600000 ||
      !validTime(raw.generated_at) || Date.parse(raw.generated_at) >= Math.floor(Date.parse(raw.game_start)/3600000)*3600000 ||
      Date.parse(raw.generated_at)>now || !validTime(raw.input_cutoff) || Date.parse(raw.input_cutoff)>Date.parse(raw.generated_at)) return null;
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
    for(const key of ["0.1","0.5","0.9"]) {
      const value=q[key];
      if(typeof value!=="number" || !Number.isFinite(value) || value<lower || value<0 || value>1)return null;
      quantiles[key]=value;lower=value;
    }
    rows.push({timestamp:row.timestamp,quantiles,interpolated_model_point:row.interpolated_model_point===true});
    prior=Date.parse(row.timestamp);
  }
  if(rows.at(-1)?.timestamp!==raw.forecast_end)return null;
  const base:Record<string,unknown>={id:raw.id,provider:raw.provider,event_title:text(raw.event_title),outcome:text(raw.outcome),
    game_date:raw.game_date,game_start:raw.game_start,forecast_end:raw.forecast_end,generated_at:raw.generated_at,
    input_cutoff:raw.input_cutoff,history_count:Number(raw.history_count)||0,
    models:Array.isArray(raw.models)?raw.models.map(m=>text(m,40)).slice(0,5):[],
    status:now>=Math.floor(Date.parse(raw.game_start)/3600000)*3600000?"final_pregame":"updating_pregame",
    endpoint:rows.at(-1)?.quantiles};
  if(detail)Object.assign(base,{symbol:text(raw.symbol,220),contract_id:text(raw.contract_id,220),
    history_start:validTime(raw.history_start)?raw.history_start:null,history_gap_count:Number(raw.history_gap_count)||0,
    method:text(raw.method,600),warnings:Array.isArray(raw.warnings)?raw.warnings.map(w=>text(w,600)).slice(0,20):[],predictions:rows});
  return base;
}

export function registerGameForecastRoutes(router:Router,db:FirebaseFirestore.Firestore):void {
  router.get("/screener/games",async(_req,res)=>{
    try {
      const date=gameDate();
      const [games,status]=await Promise.all([
        db.collection("game_forecast_catalog").where("game_date","==",date).limit(500).get(),
        db.collection("game_forecast_status").get(),
      ]);
      const items=games.docs.flatMap(doc=>{const publicRow=publicGameForecast(doc.data());return publicRow?[publicRow]:[];})
        .sort((a,b)=>String(a.game_start).localeCompare(String(b.game_start))||String(a.event_title).localeCompare(String(b.event_title)));
      const coverage=status.docs.flatMap(doc=>{const data=doc.data();return data.game_date===date && ["kalshi","polymarket_us"].includes(doc.id)?[{provider:doc.id,updated_at:text(data.updated_at,50),eligible:Number(data.eligible)||0,successful:Number(data.successful)||0,failed:Number(data.failed)||0,partial:data.partial===true}]:[];});
      res.setHeader("Cache-Control","public, max-age=30, s-maxage=30");
      res.json({date,time_zone:"America/New_York",items,coverage,bounded:games.size===500});
    }catch{res.status(503).json({error:"games_unavailable",message:"Game forecasts are temporarily unavailable."});}
  });
  router.get("/screener/games/:id",async(req,res)=>{
    const id=String(req.params.id);
    if(!/^[a-f0-9]{32}$/.test(id)){res.status(404).json({error:"not_found"});return;}
    try {
      const doc=await db.collection("game_forecast_catalog").doc(id).get();
      const item=doc.exists?publicGameForecast(doc.data()||{},Date.now(),true):null;
      if(!item){res.status(404).json({error:"not_found",message:"This forecast is no longer in today’s screener."});return;}
      res.setHeader("Cache-Control","public, max-age=30, s-maxage=30");res.json({item});
    }catch{res.status(503).json({error:"games_unavailable"});}
  });
}
