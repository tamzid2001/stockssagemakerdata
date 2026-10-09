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
  '/market-data/dukascopy/instruments', '/market-data/stocks/history',
  '/market-data/perps/markets', '/market-data/perps/history',
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
document.info={title:'Quantura',version:'1.0.0',description:'Probabilistic market forecasts, verified market discovery, observed historical data, and screener quantiles. Forecasts run asynchronously: create a job, poll its status, then download the final ensemble. Enterprise agreements define compute admission and API quotas. Market data remains subject to provider coverage and licensing. Research estimates are not guaranteed returns.',contact:{name:'Quantura',url:'https://quantura.studio/contact'},termsOfService:'https://quantura.studio/terms'};
document.servers=[{url:'https://quantura.studio/api',description:'Quantura production origin behind RapidAPI'}];
document.security=[{rapidApiKey:[]}];
document.paths=Object.fromEntries(Object.entries(document.paths).filter(([key])=>included.has(key)).map(([key,value])=>['/v1'+key,value]));
document.components.securitySchemes={rapidApiKey:{type:'apiKey',in:'header',name:'X-RapidAPI-Key',description:'Your RapidAPI application key. RapidAPI supplies the API-specific gateway credential privately. A private enterprise CUSTOM subscription is required.'}};
for (const route of Object.values(document.paths)) for (const operation of Object.values(route)) {
  if (operation && typeof operation==='object' && operation.operationId) {
    operation.security=[{rapidApiKey:[]}];
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
document.tags=document.tags.filter(t=>tags.has(t.name));
const output=path.resolve(__dirname,'../../..','docs/rapidapi/openapi.json');
fs.writeFileSync(output,JSON.stringify(document,null,2)+'\n');
console.log(JSON.stringify({output,openapi:document.openapi,paths:Object.keys(document.paths).length,operations:Object.values(document.paths).reduce((sum,p)=>sum+Object.values(p).filter(o=>o.operationId).length,0),bytes:fs.statSync(output).size}));
