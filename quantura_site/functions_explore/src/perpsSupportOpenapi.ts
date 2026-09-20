/** Examples are illustrative API shapes, never claimed forecast returns. */
export function addPerpsSupportOpenapi(document:any, origin:string) {
  const servers=[{url:`${origin}/api`}];
  const success=(example:unknown)=>({description:"Successful response (example)",content:{"application/json":{schema:{type:"object"},example}}});
  const error={description:"Provider unavailable or request invalid",content:{"application/json":{schema:{type:"object"},example:{error:"perps_provider_unavailable",message:"Perpetual history is unavailable. No substitute prices were returned."}}}};
  const common={servers,security:[],tags:["Kalshi Perpetuals"]};
  const parameters=[
    {name:"symbol",in:"query",required:true,schema:{type:"string",pattern:"^KX[A-Z0-9]{1,36}PERP$"},example:"KXBTCPERP"},
    {name:"frequency",in:"query",schema:{type:"string",enum:["1min","1h","1D"],default:"1h"}},
    {name:"start",in:"query",schema:{type:"string",format:"date-time"}},
    {name:"end",in:"query",schema:{type:"string",format:"date-time"}},
    {name:"limit",in:"query",schema:{type:"integer",minimum:1,maximum:5000,default:500}},
    {name:"format",in:"query",schema:{type:"string",enum:["json","csv"],default:"json"}},
  ];
  document.paths["/market-data/perps/markets"]={get:{...common,operationId:"listKalshiPerpetualContracts",summary:"List all available Kalshi perpetual contracts and price units",responses:{"200":success({provider:"kalshi_perps",markets:[{resource_type:"perpetual_contract",symbol:"KXBTCPERP",name:"0.0001 BTC perpetual",asset_class:"perpetual",status:"active",contract_size:0.0001,underlying_multiplier:1,unit:"USD per contract (not underlying spot price)",available_granularities:["1min","1h","1D"]}]}),"502":error}}};
  const example={ok:true,provider:"kalshi_perps",symbol:"KXBTCPERP",frequency:"1min",rows:[{timestamp:"2026-09-20T12:01:00Z",open:8.1,high:8.2,low:8.05,close:8.15,volume:100}],count:1,metadata:{field:"price.close",units:"USD per contract",timezone:"UTC",timestamp_convention:"end_period_ts",missing_intervals:"not_filled",redistribution_status:"review_required"},warnings:[]};
  const responses={"200":{...success(example),content:{...success(example).content,"text/csv":{schema:{type:"string"},example:"timestamp,open,high,low,close,volume\r\n2026-09-20T12:01:00Z,8.1,8.2,8.05,8.15,100"}}},"422":error,"429":error,"502":error};
  const description="Completed traded-price OHLC in USD per contract, not underlying spot price or binary probability. Null closes and missing bars are not filled. Bounded backward time-window retrieval; maximum 20,000 intervals per date range and 5,000 returned rows. Market data is public; forecast execution still uses authenticated workspace authorization.";
  document.paths["/market-data/perps/history"]={
    get:{...common,operationId:"getKalshiPerpetualTradeCloses",summary:"Download completed Kalshi perpetual trade closes as CSV or JSON",description,parameters,responses},
    post:{...common,operationId:"downloadKalshiPerpetualTradeCloses",summary:"Request a bounded Kalshi perpetual historical download by date range",description,
      requestBody:{required:true,content:{"application/json":{schema:{type:"object",required:["symbol"],properties:Object.fromEntries(parameters.map(p=>[p.name,p.schema]))},example:{symbol:"KXBTCPERP",frequency:"1h",limit:500,format:"json"}}}},responses},
  };
  document.paths["/support/chat"]={post:{servers,tags:["Q Support"],operationId:"askQSupport",summary:"Find documented Quantura help with Jev typed topic routing",
    "x-quantura-scope":"account:read",description:"A verified session (including guest) or API key with account:read is required. TypeSafe Jev selects a fixed help article; no generated chat, account access or OpenAI fallback. Low-confidence routing returns Contact guidance. Conversation content is not persisted. User/global quotas apply.",
    requestBody: {
      required: true,
      content: {"application/json": {
        schema: {type:"object",additionalProperties:false,required:["messages"],properties:{
          messages:{type:"array",minItems:1,maxItems:9,items:{type:"object",required:["role","content"],properties:{role:{type:"string",enum:["user","assistant"]},content:{type:"string",maxLength:3000}}}}
        }},
        example:{messages:[{role:"user",content:"Where are my API keys?"}]}
      }}
    },
    responses:{"200":success({data:{answer:"Open Account → Developer → API Keys to create, replace or revoke a key.",references:[{id:"keys",title:"API authentication",url:"https://quantura.mintlify.app/docs/authentication"}],model:"jev-1.13.0",response_mode:"documented_help",escalate:false,knowledge_version:"q-support-2026-09-20"},meta:{request_id:"example-request-id"}}),"401":{description:"Authentication required"},"422":{description:"Question invalid or contains credentials"},"429":{description:"Request quota exceeded"},"503":{description:"Jev unavailable; use Contact"}}}};
  document.components.schemas.EnsembleForecastRequest.allOf[1].properties.source.oneOf.push({type:"object",additionalProperties:false,required:["type","symbol"],properties:{type:{const:"kalshi_perp"},symbol:{type:"string",pattern:"^KX[A-Z0-9]{1,36}PERP$"},frequency:{enum:["1min","1h","1D"],default:"1h"},limit:{type:"integer",minimum:2,maximum:500,default:500}},description:"Kalshi perpetual trade-close forecast. Set horizon_mode=frequency_periods and calendar=NONE; price transforms auto/log/none, never logit. Source/units/contract size are resolved by the server."});
}
