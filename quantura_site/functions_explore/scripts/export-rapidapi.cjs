/* Generate the provider listing from implemented routes, using Rapid's OAS 3.0 importer. */
const fs = require('node:fs');
const path = require('node:path');
const {buildOpenApiDocument} = require('../dist/openapi');
const original = buildOpenApiDocument('https://quantura.studio');
const included = new Set([
  '/economic-data/search','/economic-data/describe','/economic-data/history','/market-data/gemini/history','/market-data/gemini/prediction-contract',
  '/me/access', '/capabilities', '/forecast/models', '/ensemble-forecasts',
  '/ensemble-forecasts/{forecast_id}', '/ensemble-forecasts/{forecast_id}/download',
  '/ensemble-forecasts/{forecast_id}/proof', '/ensemble-forecasts/{forecast_id}/observations', '/ensemble-forecasts/{forecast_id}/reproduce',
  '/market-search', '/market-search/resolve', '/market-search/event', '/market-search/capabilities',
  '/market-data/dukascopy/instruments', '/market-data/history',
  '/screener/forecasts/{ticker}', '/screener/forecasts/{ticker}/observations',
]);
function convert(value) {
  if (Array.isArray(value)) return value.map(convert);
  if (!value || typeof value !== 'object') return value;
  const out = {};
  for (const [key,item] of Object.entries(value)) {
    if (key.startsWith('x-') || ['$schema','$id','contentEncoding','contentMediaType','unevaluatedProperties'].includes(key)) continue;
    if (key === 'const') { out.enum = [item]; continue; }
    if (key === 'type' && Array.isArray(item)) {
      const types = item.filter(t=>t!=='null');
      if (types.length === 1) out.type=types[0];
      else out.oneOf=types.map(type=>({type}));
      if (item.includes('null')) out.nullable=true;
      continue;
    }
    if (['exclusiveMinimum','exclusiveMaximum'].includes(key) && typeof item === 'number') {
      out[key]=true; out[key === 'exclusiveMinimum' ? 'minimum' : 'maximum']=item; continue;
    }
    out[key]=convert(item);
  }
  if (out.enum && out.enum.includes('yahoo')) out.enum=out.enum.filter(x=>x!=='yahoo');
  if (typeof out.description==='string') out.description=out.description.replace(/Alpaca, Yahoo, or Dukascopy/g,'Alpaca or Dukascopy');
  return out;
}
const document = convert(original);
document.openapi='3.0.3';
document.info={title:'Quantura',version:'1.1.0',description:'Probabilistic market forecasts, verified market discovery, observed historical data, and screener quantiles. Forecasts run asynchronously: create a job, poll its status, then download the final ensemble. Subscribe to BASIC pay per use or PRO through RapidAPI. BASIC meters newly accepted forecast jobs separately from HTTP requests; polling, cached results and idempotent replay do not add a Forecasts unit. PRO includes forecast creation within its request quota. Market data remains subject to provider coverage and licensing. Research estimates are not guaranteed returns.',contact:{name:'Quantura',url:'https://quantura.studio/contact'},termsOfService:'https://quantura.studio/terms'};
document.servers=[{url:'https://quantura.studio/api',description:'Quantura production origin behind RapidAPI'}];
document.security=[{rapidApiKey:[]}];
document.paths=Object.fromEntries(Object.entries(document.paths).filter(([key])=>included.has(key)).map(([key,value])=>['/v1'+key,value]));
document.components.securitySchemes={rapidApiKey:{type:'apiKey',in:'header',name:'X-RapidAPI-Key',description:'Your RapidAPI application key. RapidAPI supplies the API-specific gateway credential privately. An active BASIC pay-per-use, PRO, or private enterprise subscription is required. Send X-RapidAPI-Host as shown in the Hub examples.'}};
for (const route of Object.values(document.paths)) for (const operation of Object.values(route)) {
  if (operation && typeof operation==='object' && operation.operationId) {
    operation.security=[{rapidApiKey:[]}];
    operation.tags=(operation.tags||[]).map(tag=>({"Q Search":"Search","Q Download":"Download","Ensemble Forecasts":"Forecast","Economic Data":"Databases","Gemini Markets":"Databases"}[tag]||tag));
    operation.summary=operation.summary||operation.operationId.replace(/([A-Z])/g,' $1').trim();
    if (operation.parameters) operation.parameters=operation.parameters.filter(p=>p.name!=='workspace_id');
  }
}
const needed = new Set();
function refs(node) {
  if (!node || typeof node !== 'object') return;
  if (node.$ref && node.$ref.startsWith('#/components/schemas/')) {
    const name=node.$ref.split('/')[3];
    if (!needed.has(name)) {needed.add(name); refs(document.components.schemas[name]);}
  }
  for (const [key,value] of Object.entries(node)) if(key!=='$ref') refs(value);
}
refs(document.paths);
document.components.schemas=Object.fromEntries(Object.entries(document.components.schemas).filter(([key])=>needed.has(key)));
const tags=new Set(Object.values(document.paths).flatMap(p=>Object.values(p).flatMap(o=>o.tags||[])));
document.tags=[...tags].map(name=>({name,description:({Access:"Account access and provider/model capabilities",Search:"Find instruments, events and provider-verified contracts",Download:"Observed price history and CSV exports, including perpetuals",Forecast:"Create, poll and export asynchronous ensemble forecasts",Screener:"Published stock and perpetual forecast ranges",Databases:"Economic series and Gemini market data"})[name]||name}));
const output=path.resolve(__dirname,'../../..','docs/rapidapi/openapi.json');
fs.writeFileSync(output,JSON.stringify(document,null,2)+'\n');
console.log(JSON.stringify({output,openapi:document.openapi,paths:Object.keys(document.paths).length,operations:Object.values(document.paths).reduce((sum,p)=>sum+Object.values(p).filter(o=>o.operationId).length,0),bytes:fs.statSync(output).size}));

// Postman is Rapid's second supported import format. Keep it reproducible from
// the same OpenAPI contract for Hub deployments whose OAS parser rejects allOf.
const summaries={getMyAccess:'Get account access',getCapabilities:'Get capabilities',getEnsembleModelCapabilities:'Get forecast models',createEnsembleForecast:'Create forecast',getEnsembleForecast:'Get forecast result',reproduceEnsembleForecast:'Reproduce forecast',downloadEnsembleForecast:'Download forecast',getEnsembleForecastObservations:'Get forecast observations',downloadQuanturaMarketHistory:'Download market history',searchQuanturaMarkets:'Search markets',resolveQuanturaMarketLink:'Resolve market link',browseQuanturaEventMarkets:'Browse event markets',getQuanturaSearchCapabilities:'Get search capabilities',listDukascopyInstruments:'Browse Dukascopy instruments',getPublishedScreenerForecast:'Get published screener forecast',getPublishedScreenerObservations:'Get screener observations'};
const groups=new Map();
for(const [route,methods] of Object.entries(document.paths))for(const [method,operation]of Object.entries(methods)){
  if(!operation.operationId)continue;
  const group=operation.tags[0];if(!groups.has(group))groups.set(group,[]);
  const body=operation.requestBody?.content?.['application/json'];
  let example=body?.example||Object.values(body?.examples||{})[0]?.value;
  if(operation.operationId==='createEnsembleForecast')example={source:{type:'ticker',provider:'alpaca',symbol:'AAPL',frequency:'1D',limit:500},prediction_length:7,horizon_mode:'trading_sessions',quantiles:[.01,.25,.5,.75,.9,.99],models:{prophet:{enabled:true,weight:1}}};
  if(operation.operationId==='reproduceEnsembleForecast')example={};
  const query=(operation.parameters||[]).filter(p=>p.in==='query').map(p=>({key:p.name,value:String(p.example??p.schema?.default??(p.name==='q'?'AAPL':p.name==='url'?'https://kalshi.com':p.name==='source'?'auto':'')),disabled:!p.required,description:p.description||p.schema?.description||''}));
  const pathSegments=route.split('/').slice(1).map(p=>p.replace(/^\{(.+)\}$/,':$1'));
  const url={raw:'{{baseUrl}}/'+pathSegments.join('/'),host:['{{baseUrl}}'],path:pathSegments,query,variable:(operation.parameters||[]).filter(p=>p.in==='path').map(p=>({key:p.name,value:p.name==='ticker'?'AAPL':'YOUR_FORECAST_ID',description:p.description||''}))};
  const request={method:method.toUpperCase(),header:method==='post'?[{key:'Content-Type',value:'application/json'}]:[],url,description:[operation.description||operation.summary,body?.schema?'Request schema:\n```json\n'+JSON.stringify(body.schema,null,2)+'\n```':''].filter(Boolean).join('\n\n')};
  if(body)request.body={mode:'raw',raw:JSON.stringify(example||{},null,2),options:{raw:{language:'json'}}};
  groups.get(group).push({name:summaries[operation.operationId]||operation.summary,request,response:[]});
}
const collection={info:{name:'Quantura',description:document.info.description,schema:'https://schema.getpostman.com/json/collection/v2.1.0/collection.json'},variable:[{key:'baseUrl',value:'https://quantura.studio/api',type:'string'}],item:[...groups].map(([name,item])=>({name,item}))};
const collectionPath=path.resolve(__dirname,'../../..','docs/rapidapi/postman.json');
fs.writeFileSync(collectionPath,JSON.stringify(collection,null,2)+'\n');
console.log(JSON.stringify({output:collectionPath,groups:groups.size,operations:[...groups.values()].reduce((n,items)=>n+items.length,0)}));
