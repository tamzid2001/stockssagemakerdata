/** Examples illustrate API contracts, not observed market results. */
export function addScreenerOpenapi(document:any):void {
  document.components.schemas.ApiError={type:"object",required:["error"],properties:{error:{type:"object",required:["code","message","request_id"],properties:{code:{type:"string"},message:{type:"string"},request_id:{type:"string"}}}}};
  const error={description:"Request rejected",content:{"application/json":{schema:{$ref:"#/components/schemas/ApiError"}}}};
  const response=(description:string,example:unknown)=>({description,content:{"application/json":{schema:{type:"object"},example}}});
  const filter={name:"Average P50 opportunity",email:false,filters:{signal:"buy",quantileRules:[{quantile:"p50",statistic:"avg",operator:"gt",percent:10}]}};
  const saved={...filter,id:"a".repeat(24),created_at:"2026-09-18T21:00:00Z"};
  const errors={"401":error,"403":error,"422":error,"429":error,"503":error};
  const base={tags:["Screener"],description:"Personal saved filters, independent of workspace role. Registered account required. API scope and verified account email are checked server-side; email is opt-in. At most 10 filters/account. Daily matches use finalized exchange closes. One combined email/account/day, subject to delivery budget."};
  document.paths["/me/screener-alerts"]={
    get:{...base,operationId:"listScreenerAlerts",summary:"List your saved screener filters and daily notification preferences","x-quantura-scope":"alerts:read",responses:{"200":response("Saved filters; no pagination (maximum 10)",{data:[saved],meta:{maximum:10,email_configured:true}}),...errors}},
    post:{...base,operationId:"saveScreenerAlert",summary:"Save current screener filters for daily closing match notifications","x-quantura-scope":"alerts:write",requestBody:{required:true,content:{"application/json":{schema:{type:"object",additionalProperties:false,required:["name","filters","email"],properties:{name:{type:"string",minLength:1,maxLength:80},email:{type:"boolean",description:"Explicit email opt-in; verified account address only."},filters:{$ref:"#/components/schemas/ScreenerAlertFilters"}}},example:filter}}},responses:{"201":response("Saved or replaced identical normalized filters",{data:saved}),...errors}}
  };
  document.paths["/me/screener-alerts/{alert_id}"]={delete:{...base,operationId:"removeScreenerAlert",summary:"Remove a saved screener filter and stop its future notifications","x-quantura-scope":"alerts:write",parameters:[{name:"alert_id",in:"path",required:true,schema:{type:"string",pattern:"^[a-f0-9]{24}$"}}],responses:{"200":response("Removed from caller's account only",{data:{id:saved.id,removed:true}}),"404":error,...errors}}};
  document.components.schemas.ScreenerAlertFilters={type:"object",additionalProperties:false,properties:{
    search:{type:"string",maxLength:80,default:""},universe:{type:"string",enum:["all","sp500","nasdaq","etf"],default:"all"},marketCap:{type:"string",enum:["all","mega","large","mid","small","micro"],default:"all"},
    minMarketCap:{type:["number","null"],minimum:0,default:null},maxMarketCap:{type:["number","null"],minimum:0,default:null},signal:{type:"string",enum:["all","buy","sell","neutral","unavailable"],default:"all"},signalChanged:{type:"boolean",default:false},
    positions:{type:"array",default:[],items:{type:"string",enum:["below-p10","above-p10","below-p50","above-p50","below-p90","above-p90"]}},
    quantileRules:{type:"array",maxItems:12,default:[],items:{type:"object",additionalProperties:false,required:["quantile","statistic","operator","percent"],properties:{quantile:{type:"string",enum:["p01","p10","p25","p50","p75","p90","p99"]},statistic:{type:"string",enum:["min","max","avg"]},operator:{type:"string",enum:["gt","gte","lt","lte"]},percent:{type:"number",minimum:-100000,maximum:100000,description:"100 × (quantile statistic − price) / price"}}}},
    sort:{type:"string",enum:["ticker","company","actualPrice","marketCap","p10","p50","p90","distanceP10","distanceP50","distanceP90","lastUpdate"],default:"ticker"},direction:{type:"string",enum:["asc","desc"],default:"asc"},statistic:{type:"string",enum:["row","min","max","avg"],description:"Presentation-only; use quantileRules to filter."}}};
  const parameters=[{name:"ticker",in:"path",required:true,schema:{type:"string",maxLength:20}},{name:"scan_id",in:"query",required:true,schema:{type:"string",maxLength:160},description:"Exact published scan; old scan returns 404, never silently substitutes a new one."}];
  document.paths["/screener/forecasts/{ticker}"]={get:{tags:["Screener"],security:[],operationId:"getPublishedScreenerForecast",summary:"Open the exact published weekly forecast for a screener ticker",parameters:[...parameters,{name:"format",in:"query",required:false,schema:{type:"string",enum:["csv","json"]},description:"Optional attachment; omit for JSON data envelope."}],responses:{"200":response("Seven-session final ensemble and input history; example abbreviated",{data:{forecast_id:"screener-example-TEST",status:"completed",prediction_length:7,horizon_mode:"trading_sessions",frequency:"1D",published_screener:{ticker:"TEST",scan_id:"example"},predictions:[{timestamp:"2026-09-21T00:00:00Z",quantiles:{"0.01":80,"0.1":90,"0.25":95,"0.5":100,"0.75":105,"0.9":110,"0.99":120}}],history:[{timestamp:"2026-09-17T00:00:00Z",target:99}]}}),"404":error,"503":error}}};
  document.paths["/screener/forecasts/{ticker}/observations"]={get:{tags:["Screener"],security:[],operationId:"getPublishedScreenerObservations",summary:"Retrieve observed stock closes for the published screener forecast chart",parameters,responses:{"200":response("Actual observations; missing bars are not filled",{data:{rows:[{timestamp:"2026-09-18T20:00:00Z",target:100,interval:"1min",provider:"alpaca"}],availability:"available",observed_at:"2026-09-18T20:01:00Z"}}),"404":error,"503":error}}};
  document.paths["/screener/forecasts/{ticker}/save"]={post:{
    tags:["Screener"],operationId:"savePublishedScreenerForecast",
    summary:"Save an immutable published screener forecast copy to your workspace",
    "x-quantura-scope":"forecasts:write",
    description:"Registered account, current workspace write access and forecast.create required. Idempotent for user/workspace/scan/ticker; no new inference.",
    parameters:[parameters[0]],
    requestBody:{required:true,content:{"application/json":{
      schema:{type:"object",additionalProperties:false,required:["scan_id"],properties:{scan_id:{type:"string"},workspace_id:{type:"string",description:"Defaults to personal workspace."}}},
      example:{scan_id:"example"}
    }}},
    responses:{"200":response("Saved private forecast identity",{data:{forecast_id:"sc_example",saved:true,url:"/forecasting?panel=forecast&ensembleForecastId=sc_example"}}),"404":error,...errors}
  }};
}
