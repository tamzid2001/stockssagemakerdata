import {test} from 'node:test';
import assert from 'node:assert/strict';
import type {Request} from 'express';
import {rapidApiPrincipal} from './rapidApiAuth';

test('RapidAPI authentication requires the private API-specific secret and a supported subscription',()=>{
  process.env.RAPIDAPI_PROXY_SECRET='a'.repeat(48);
  process.env.RAPIDAPI_API_ID='api_example';
  process.env.RAPIDAPI_PROVIDER_USER='provider_owner';
  const req=(headers:Record<string,string>,path='/v1/ensemble-forecasts',method='POST')=>({headers,path,method}) as Request;
  assert.equal(rapidApiPrincipal(req({'x-rapidapi-key':'consumer_app_key','x-rapidapi-user':'customer'})),null);
  const headers={'x-rapidapi-proxy-secret':'a'.repeat(48),'x-rapidapi-user':'customer','x-rapidapi-subscription':'CUSTOM'};
  for(const bad of ['wrong',''])assert.throws(()=>rapidApiPrincipal(req({...headers,'x-rapidapi-proxy-secret':bad})),/api_key_invalid/);
  assert.throws(()=>rapidApiPrincipal(req({...headers,'x-rapidapi-subscription':'BASIC'})),/paid_api_required/);
  assert.throws(()=>rapidApiPrincipal(req({...headers,'x-rapidapi-user':'../customer'})),/api_key_invalid/);
  assert.throws(()=>rapidApiPrincipal(req(headers,'/v1/workspaces','POST')),/insufficient_scope/);
  assert.throws(()=>rapidApiPrincipal(req(headers,'/v1/ensemble-forecasts/test','DELETE')),/insufficient_scope/);
  const customer=rapidApiPrincipal(req(headers))!;
  const other=rapidApiPrincipal(req({...headers,'x-rapidapi-user':'other'}))!;
  assert.notEqual(customer.userId,other.userId);
  assert.equal(customer.plan,'research');assert.equal(customer.platformAdmin,false);
  assert.ok(!customer.tokenScopes.includes('workspaces:write'));
  assert.equal(rapidApiPrincipal(req(headers,'/api/v1/forecast/models','GET'))?.userId,customer.userId);
  assert.equal(rapidApiPrincipal(req({...headers,'x-rapidapi-user':'provider_owner','x-rapidapi-subscription':'BASIC'}))?.plan,'research');
  delete process.env.RAPIDAPI_PROXY_SECRET;delete process.env.RAPIDAPI_API_ID;delete process.env.RAPIDAPI_PROVIDER_USER;
});
