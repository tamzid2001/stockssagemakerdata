import type { QuantScreenerDataset, QuantScreenerRow } from "./quantScreener";
import { kalshiPerps, type KalshiPerpsService } from "./kalshiPerps";
import { screenerArtifacts } from "./screenerArtifacts";
import { screenerHistory } from "./screenerHistory";

export const PERP_ENGINE = "quantura_perps_daily_ensemble_v2";
export const PERP_LEVELS = ["p01","p25","p50","p75","p90","p99"];


export function validPerpSnapshot(row: QuantScreenerRow, now=Date.now()): boolean {
  try {
    const config=row.forecast_config as any, models=row.forecast_models as any[], provenance=row.forecast_provenance as any;
    const history=screenerHistory(row.forecast_input_gzip), predictions=row.forecast_rows as any[];
    const generated=Date.parse(String(row.last_forecast_update)), cutoff=Date.parse(history.at(-1)!.timestamp);
    if(row.forecast_engine!==PERP_ENGINE || row.status!=="success" || provenance?.runtime?.mock || history.length<32 ||
      !Number.isFinite(generated) || generated>now || cutoff>generated || config?.prediction_length!==7 || config?.frequency!=="1D" || config?.calendar!=="NONE" ||
      config?.horizon_mode!=="frequency_periods" || JSON.stringify(config.quantiles)!==JSON.stringify([.01,.25,.5,.75,.9,.99]) ||
      !Array.isArray(models) || models.length!==5 || models.some(m=>m.status!=="completed") ||
      new Set(models.map(m=>m.id)).size!==5 || !["prophet","toto","granite","chronos","timesfm"].every(id=>models.some(m=>m.id===id) && config.models?.[id]?.enabled && config.models[id].weight===.2) ||
      !Array.isArray(predictions) || predictions.length!==7 || Date.parse(predictions.at(-1).timestamp)<=now)return false;
    let previous=cutoff;
    for(const p of predictions) {
      const timestamp=Date.parse(p.timestamp), values=PERP_LEVELS.map(key=>p[key]);
      if(!Number.isFinite(timestamp) || timestamp!==previous+86400000 || values.some((v,i)=>typeof v!=="number" || !Number.isFinite(v) || v<=0 || i>0 && v<values[i-1]))return false;
      previous=timestamp;
    }
    return true;
  }catch{return false;}
}

export async function loadPerpForecasts(_db:FirebaseFirestore.Firestore):Promise<QuantScreenerRow[]> {
  const snapshots=await Promise.all([0,1,2,3].map(shard=>screenerArtifacts.read(`perps-${shard}`)));
  return snapshots.flatMap(snapshot=>snapshot.data.items as QuantScreenerRow[]);
}

export async function perpScreenerDataset(db:FirebaseFirestore.Firestore,service:KalshiPerpsService=kalshiPerps,load=loadPerpForecasts):Promise<QuantScreenerDataset> {
  const [quotes,snapshots]=await Promise.all([service.screener(),load(db)]);
  let published=0;
  const items=quotes.items.map(quote=>{
    const row=snapshots.find(item=>item.ticker===quote.ticker);
    if(!row || !validPerpSnapshot(row))return {...quote,forecast_status:"No current five-model forecast · genuine daily history required"};
    published++;
    const merged={...row,...quote,forecast_engine:PERP_ENGINE,status:"success",forecast_status:"Five-model daily ensemble · seven days",
      forecast_view_url:`/forecasting?panel=forecast&screenerTicker=${encodeURIComponent(row.ticker)}&screenerScan=${encodeURIComponent(String(row.scan_id))}&screenerSource=kalshi_perps`,forecast_action:"view"} as QuantScreenerRow;
    for(const q of PERP_LEVELS)merged[q]=row[q];
    for(const q of ["p25","p50","p90"])merged[`distance_${q}_pct`]=typeof quote.actual_price==="number" && typeof row[q]==="number"?100*(quote.actual_price/(row[q] as number)-1):null;
    return merged;
  });
  return {...quotes,schema_version:PERP_ENGINE,items,manifest:{...quotes.manifest,forecasts_published:published,forecasts_unavailable:items.length-published,forecast_engine:PERP_ENGINE,
    warnings:published<items.length?[`${published} of ${items.length} listings have a current five-model forecast. Missing or inactive trade history is not filled.`]:[]}};
}
