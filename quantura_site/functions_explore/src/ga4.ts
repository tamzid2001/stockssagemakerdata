type RecordValue = Record<string, any>;
export type AnalyticsContext = {client_id:string;session_id:number;consent_at:string};
const record=(value:unknown):RecordValue=>value&&typeof value==="object"&&!Array.isArray(value)?value as RecordValue:{};

/** Optional telemetry can never make a forecast request fail. Use existing tag identifiers only. */
export function analyticsContext(value:unknown, now=Date.now()):AnalyticsContext|null {
  const v=record(value),session=Number(v.session_id),time=Date.parse(v.consent_at);
  if(v.analytics_consent!=="granted" || !/^\d{1,20}\.\d{1,20}$/.test(String(v.client_id)) ||
      !Number.isSafeInteger(session) || session*1000>now+300_000 || session*1000<now-86_400_000 ||
      !Number.isFinite(time) || time>now+300_000 || time<now-300_000)return null;
  return {client_id:v.client_id,session_id:session,consent_at:new Date(now).toISOString()};
}

export function forecastCompletionPayload(job:RecordValue, now=Date.now()):RecordValue|null {
  const context=record(job.analytics_context),request=record(job.request),source=record(job.source);
  if(job.status!=="completed" || !/^\d{1,20}\.\d{1,20}$/.test(String(context.client_id)) ||
      !Number.isSafeInteger(context.session_id) || context.session_id*1000>now+300_000 ||
      context.session_id*1000<now-86_400_000 || now-Date.parse(context.consent_at)>86_400_000 ||
      !Number.isFinite(Date.parse(context.consent_at)))return null;
  const provider=["alpaca","yahoo","dukascopy","kalshi","polymarket_us","kalshi_perps"].includes(source.provider)?source.provider:"dataset";
  return {client_id:context.client_id,consent:{ad_user_data:"DENIED",ad_personalization:"DENIED"},events:[{
    name:"forecast_completed",params:{session_id:context.session_id,engagement_time_msec:1,
      source_provider:provider,model_count:Number(record(job.progress).completed_models)||0,
      quantile_count:Array.isArray(request.quantiles)?request.quantiles.length:0,
      forecast_steps:Number(request.prediction_length)||0},
  }]};
}

/** Claim once before sending. HTTP 2xx is acceptance, not proof of GA reporting or attribution. */
export async function reportGa4ForecastCompletion(db:FirebaseFirestore.Firestore,ref:FirebaseFirestore.DocumentReference,request:typeof fetch=fetch):Promise<void> {
  const measurement=process.env.GA4_MEASUREMENT_ID||"",secret=process.env.GA4_API_SECRET||"";
  if(!/^G-[A-Z0-9]+$/.test(measurement)||!secret)return;
  try {
    const payload=await db.runTransaction(async tx=>{
      const snap=await tx.get(ref),job=record(snap.data());
      if(job.analytics_delivery)return null;
      const payload=forecastCompletionPayload(job);if(!payload || !job.user_id)return null;
      const user=await tx.get(db.collection("users").doc(job.user_id));
      if(user.data()?.analyticsConsent!=="accepted")return null;
      tx.set(ref,{analytics_context:null,analytics_delivery:{status:"sending",attempted_at:new Date().toISOString()}},{merge:true});
      return payload;
    });
    if(!payload)return;
    let status="unconfirmed",httpStatus:number|null=null;
    try {
      const endpoint=new URL("https://www.google-analytics.com/mp/collect");
      endpoint.searchParams.set("measurement_id",measurement);endpoint.searchParams.set("api_secret",secret);
      const response=await request(endpoint,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload),redirect:"error",signal:AbortSignal.timeout(2500)});
      httpStatus=response.status;status=response.ok?"accepted":"rejected";
    } catch { /* Never log a transport error containing the secret URL. */ }
    await ref.set({analytics_delivery:{status,http_status:httpStatus,attempted_at:new Date().toISOString()}},{merge:true});
  } catch { /* Telemetry failures never change a saved forecast's completion status. */ }
}
