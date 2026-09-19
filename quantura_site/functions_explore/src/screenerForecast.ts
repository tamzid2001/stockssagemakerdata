import { gunzipSync } from "node:zlib";
import type { QuantScreenerDataset } from "./quantScreener";
import { forecastRows } from "./screenerSignals";

/** Read only a validated publication; never accept prediction arrays from a client. */
export function screenerForecastSnapshot(dataset: QuantScreenerDataset, ticker: string, scanId: string) {
  if(scanId !== dataset.scan_id)throw new Error("screener_forecast_not_found");
  const row=dataset.items.find(r=>r.ticker === ticker.toUpperCase());
  if(!row || row.forecast_engine!=="quantura_weekly_ensemble_v1" || row.status!=="success")throw new Error("screener_forecast_not_found");
  const config=row.forecast_config as Record<string,any>;
  const provenance=row.forecast_provenance as Record<string,any>;
  if(!config || !provenance || provenance.runtime?.mock || forecastRows(row).length!==7)throw new Error("screener_forecast_invalid");
  const decoded=JSON.parse(gunzipSync(Buffer.from(String(row.forecast_input_gzip),"base64"),{maxOutputLength:100_000}).toString("utf8"));
  if(!Array.isArray(decoded) || decoded.length<32 || decoded.length>512)throw new Error("screener_history_invalid");
  let prior="";
  const history=decoded.map((r:unknown[])=>{
    if(!Array.isArray(r)||r.length!==2||typeof r[0]!=="string"||!Number.isFinite(Date.parse(r[0]))||r[0]<=prior||typeof r[1]!=="number"||!Number.isFinite(r[1])||r[1]<=0)throw new Error("screener_history_invalid");
    prior=r[0];return {timestamp:r[0],target:r[1]};
  });
  const request={prediction_length:7,horizon_mode:"trading_sessions",quantiles:config.quantiles,frequency:"1D",calendar:"NYSE",context_length:config.context_length,
    transform:config.transform,failure_policy:"fail",models:config.models,toto_variant:"4m"};
  const source={type:"ticker",symbol:row.ticker,provider:String(row.price_source).startsWith("alpaca")?"alpaca":"yahoo",field:"close",frequency:"1Day",session:"regular",adjustment:"split",limit:512,daily_timestamp_convention:"session_date",
    input_cutoff_at:history.at(-1)!.timestamp,history_cutoff_at:history.at(-1)!.timestamp,history_lag_sessions:1,
    scan_id:dataset.scan_id,forecast_engine:row.forecast_engine};
  const predictions=forecastRows(row).map(r=>({timestamp:r.timestamp,quantiles:Object.fromEntries((config.quantiles as number[]).map(q=>[String(q),r[`p${String(Math.round(q*100)).padStart(2,"0")}`]]))}));
  const result={...provenance,predictions,quantiles:config.quantiles,effective_weights_by_quantile:row.effective_weights_by_quantile,models:row.forecast_models,transform:config.transform,dataset_hash:row.dataset_hash};
  const job={schema_version:"ensemble_forecast_job_v1",status:"completed",source,request,input_row_count:history.length,input_cutoff_at:history.at(-1)!.timestamp,input_timezone:"UTC",
    created_at:row.last_forecast_update,completed_at:row.last_forecast_update,model_checkpoints:config.model_checkpoints,model_revisions:config.model_revisions,
    requested_weights:Object.fromEntries(Object.keys(config.models).map(id=>[id,0.2])),effective_central_weights:Object.fromEntries(Object.keys(config.models).map(id=>[id,0.2])),
    dataset_hash:row.dataset_hash,request_hash:row.forecast_config_hash,registry_version:"screener-weekly-v1",warnings:provenance.warnings || [],
    published_screener:{scan_id:dataset.scan_id,ticker:row.ticker},runtime_mode:"production"};
  return {job,result,history};
}
