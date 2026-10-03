import type { QuantScreenerDataset, QuantScreenerRow } from "./quantScreener";
import { kalshiPerps, type KalshiPerpsService } from "./kalshiPerps";
import { screenerHistory } from "./screenerHistory";

export const PERP_ENGINE = "quantura_perps_ensemble_v1";
export const PERP_LEVELS = ["p01","p25","p50","p75","p90","p99"];
const cache = new WeakMap<FirebaseFirestore.Firestore,{until:number;value:Promise<QuantScreenerRow[]>}>();

export function validPerpSnapshot(row: QuantScreenerRow, now=Date.now()): boolean {
  try {
    const config=row.forecast_config as any, models=row.forecast_models as any[], provenance=row.forecast_provenance as any;
    const history=screenerHistory(row.forecast_input_gzip), predictions=row.forecast_rows as any[];
    const generated=Date.parse(String(row.last_forecast_update)), cutoff=Date.parse(history.at(-1)!.timestamp);
    if(row.forecast_engine!==PERP_ENGINE || row.status!=="success" || provenance?.runtime?.mock || history.length<32 ||
      !Number.isFinite(generated) || generated>now || cutoff>generated || config?.prediction_length!==24 || config?.frequency!=="1h" || config?.calendar!=="NONE" ||
      config?.horizon_mode!=="frequency_periods" || JSON.stringify(config.quantiles)!==JSON.stringify([.01,.25,.5,.75,.9,.99]) ||
      !Array.isArray(models) || models.length!==5 || models.some(m=>m.status!=="completed") ||
      new Set(models.map(m=>m.id)).size!==5 || !["prophet","toto","granite","chronos","timesfm"].every(id=>models.some(m=>m.id===id) && config.models?.[id]?.enabled && config.models[id].weight===.2) ||
      !Array.isArray(predictions) || predictions.length!==24 || Date.parse(predictions.at(-1).timestamp)<=now)return false;
    let previous=cutoff;
    for(const p of predictions) {
      const timestamp=Date.parse(p.timestamp), values=PERP_LEVELS.map(key=>p[key]);
      if(!Number.isFinite(timestamp) || timestamp!==previous+3600000 || values.some((v,i)=>typeof v!=="number" || !Number.isFinite(v) || v<=0 || i>0 && v<values[i-1]))return false;
      previous=timestamp;
    }
    return true;
  }catch{return false;}
}

export async function loadPerpForecasts(db:FirebaseFirestore.Firestore):Promise<QuantScreenerRow[]> {
  let hit=cache.get(db);
  if(!hit || hit.until<Date.now()) {
    const value=db.collection("perp_forecast_catalog").limit(1000).get().then(data=>data.docs.map(doc=>doc.data() as QuantScreenerRow));
    hit={until:Date.now()+60000,value};cache.set(db,hit);
    value.catch(()=>cache.delete(db));
  }
  return hit.value;
}

export async function perpScreenerDataset(db:FirebaseFirestore.Firestore,service:KalshiPerpsService=kalshiPerps):Promise<QuantScreenerDataset> {
  const [quotes,snapshots]=await Promise.all([service.screener(),loadPerpForecasts(db)]);
  let published=0;
  const items=quotes.items.map(quote=>{
    const row=snapshots.find(item=>item.ticker===quote.ticker);
    if(!row || !validPerpSnapshot(row))return {...quote,forecast_status:"No current five-model forecast · genuine hourly history required"};
    published++;
    const merged={...row,...quote,forecast_engine:PERP_ENGINE,status:"success",forecast_status:"Five-model hourly ensemble · 24 hours",
      forecast_view_url:`/forecasting?panel=forecast&screenerTicker=${encodeURIComponent(row.ticker)}&screenerScan=${encodeURIComponent(String(row.scan_id))}&screenerSource=kalshi_perps`,forecast_action:"view"} as QuantScreenerRow;
    for(const q of PERP_LEVELS)merged[q]=row[q];
    for(const q of ["p25","p50","p90"])merged[`distance_${q}_pct`]=typeof quote.actual_price==="number" && typeof row[q]==="number"?100*(quote.actual_price/(row[q] as number)-1):null;
    return merged;
  });
  return {...quotes,schema_version:PERP_ENGINE,items,manifest:{...quotes.manifest,forecasts_published:published,forecasts_unavailable:items.length-published,forecast_engine:PERP_ENGINE,
    warnings:published<items.length?[`${published} of ${items.length} listings have a current five-model forecast. Missing or inactive trade history is not filled.`]:[]}};
}
