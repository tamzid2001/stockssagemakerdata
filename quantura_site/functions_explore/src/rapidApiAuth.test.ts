import {test} from 'node:test';
import assert from 'node:assert/strict';
import type {Request} from 'express';
import fs from 'node:fs';
import path from 'node:path';
import {rapidApiPrincipal} from './rapidApiAuth';

test('RapidAPI authentication requires the private API-specific secret and a supported subscription',()=>{
  process.env.RAPIDAPI_PROXY_SECRET='a'.repeat(48);
  process.env.RAPIDAPI_API_ID='api_example';
  process.env.RAPIDAPI_PROVIDER_USER='provider_owner';
  const req=(headers:Record<string,string>,path='/v1/ensemble-forecasts',method='POST')=>({headers,path,method}) as Request;
  assert.equal(rapidApiPrincipal(req({'x-rapidapi-key':'consumer_app_key','x-rapidapi-user':'customer'})),null);
  const headers={'x-rapidapi-proxy-secret':'a'.repeat(48),'x-rapidapi-user':'customer','x-rapidapi-subscription':'CUSTOM'};
  for(const bad of ['wrong',''])assert.throws(()=>rapidApiPrincipal(req({...headers,'x-rapidapi-proxy-secret':bad})),/api_key_invalid/);
  assert.throws(()=>rapidApiPrincipal(req({...headers,'x-rapidapi-subscription':'ULTRA'})),/paid_api_required/);
  assert.throws(()=>rapidApiPrincipal(req({...headers,'x-rapidapi-user':'../customer'})),/api_key_invalid/);
  assert.throws(()=>rapidApiPrincipal(req(headers,'/v1/workspaces','POST')),/insufficient_scope/);
  assert.throws(()=>rapidApiPrincipal(req(headers,'/v1/ensemble-forecasts/test','DELETE')),/insufficient_scope/);
  for (const subscription of ['BASIC','PRO','CUSTOM']) assert.ok(rapidApiPrincipal(req({...headers,'x-rapidapi-subscription':subscription})));
  const mounted={...req(headers,'/market-data/history','POST'),originalUrl:'/api/v1/market-data/history?format=json'} as Request;
  assert.ok(rapidApiPrincipal(mounted));
  assert.ok(rapidApiPrincipal(req(headers,'/v1/ensemble-forecasts/test/proof','GET')));
  const customer=rapidApiPrincipal(req(headers))!;
  const other=rapidApiPrincipal(req({...headers,'x-rapidapi-user':'other'}))!;
  assert.notEqual(customer.userId,other.userId);
  assert.equal(customer.plan,'pro');assert.equal(customer.platformAdmin,false);
  assert.ok(!customer.tokenScopes.includes('workspaces:write'));
  assert.equal(rapidApiPrincipal(req(headers,'/api/v1/forecast/models','GET'))?.userId,customer.userId);
  assert.equal(rapidApiPrincipal(req({...headers,'x-rapidapi-user':'provider_owner','x-rapidapi-subscription':'BASIC'}))?.plan,'pro');
  delete process.env.RAPIDAPI_PROXY_SECRET;delete process.env.RAPIDAPI_API_ID;delete process.env.RAPIDAPI_PROVIDER_USER;
});


test('every published RapidAPI operation passes the scoped origin allowlist; perpetuals share download and forecast',()=>{
  const doc=JSON.parse(fs.readFileSync(path.resolve(__dirname,'../../../docs/rapidapi/openapi.json'),'utf8'));
  const old={...process.env};process.env.RAPIDAPI_PROXY_SECRET='z'.repeat(48);process.env.RAPIDAPI_API_ID='api_listing';
  try {
    for(const [route,methods] of Object.entries(doc.paths))for(const [method,operation] of Object.entries(methods as any)){
      if(!(operation as any).operationId)continue;
      const pathname=route.replace(/\{[^}]+\}/g,'fixture');
      assert.ok(rapidApiPrincipal({headers:{'x-rapidapi-proxy-secret':'z'.repeat(48),'x-rapidapi-user':'subscriber','x-rapidapi-subscription':'PRO'},path:pathname,method:method.toUpperCase()} as Request),`${method} ${route}`);
    }
    assert.ok(doc.paths['/v1/market-data/history']);assert.ok(doc.paths['/v1/ensemble-forecasts']);
    assert.ok(!Object.keys(doc.paths).some(p=>p.includes('/perps/')));
    assert.ok(!doc.tags.some((t:any)=>/^Q |Kalshi Perpetuals/.test(t.name)));
    const schemas=doc.paths['/v1/market-data/history'].post.requestBody.content['application/json'].schema.oneOf;
    assert.ok(schemas[0].properties.source.enum.includes('kalshi_perps'));
  }finally{process.env=old;}
});
