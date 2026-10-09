import assert from 'node:assert/strict';
import {test} from 'node:test';
import {Quantura,apiBase} from '../src/index.js';
import {pkce,validCallback} from '../src/oauth.js';
import {createHash} from 'node:crypto';

test('base URLs and relative paths prevent token forwarding to another origin',async()=>{
  assert.equal(apiBase('https://quantura.studio'),'https://quantura.studio/api/v1');
  assert.throws(()=>apiBase('http://remote.test'),/HTTPS/);
  const client=new Quantura({token:'test',fetch:()=>{throw Error('must not fetch');}});
  await assert.rejects(client.request('https://untrusted.test'),/relative/);
  await assert.rejects(client.request('../secret'),/relative/);
  await assert.rejects(client.request('%2e%2e/secret'),/relative/);
});
test('forecast requests preserve cutoff, caller key and whole configuration',async()=>{
  const calls=[];const client=new Quantura({token:'test',fetch:async(url,options)=>{calls.push({url,options});return Response.json({data:{id:'test'}});}});
  await client.createForecast({source:{symbol:'AAPL'},history_lag_minutes:172800},{idempotencyKey:'stable-key'});
  assert.equal(calls.length,1);assert.equal(calls[0].options.headers['Idempotency-Key'],'stable-key');
  assert.equal(JSON.parse(calls[0].options.body).history_lag_minutes,172800);
  assert.equal(calls[0].options.redirect,'error');
});
test('transport failures do not retry job creation',async()=>{
  let calls=0;const client=new Quantura({token:'test',fetch:async()=>{calls++;throw Error('connection lost');}});
  await assert.rejects(client.createForecast({}),/connection lost/);assert.equal(calls,1);
});
test('Dukascopy paging preserves settings and rejects cursor loops',async()=>{
  const bodies=[];const client=new Quantura({token:'test',fetch:async(_url,options)=>{bodies.push(JSON.parse(options.body));return Response.json({rows:[],next_cursor:bodies.length===1?'next':null});}});
  const request={source:'dukascopy',symbol:'XAUUSD',timeframe:'1Hour',start:'2025-10-08',end:'2026-10-07',price_side:'bid'};
  const pages=[];for await(const p of client.historyPages(request))pages.push(p);
  assert.equal(pages.length,2);assert.deepEqual(bodies[1],{...bodies[0],cursor:'next'});assert.equal(request.cursor,undefined);
  const looping=new Quantura({token:'test',fetch:async()=>Response.json({next_cursor:'same'})});
  await assert.rejects(async()=>{for await(const _ of looping.historyPages(request)){}},error=>error.code==='PAGINATION_STALLED');
});
test('API errors retain safe references and invalid success bodies fail',async()=>{
  const client=new Quantura({token:'test',fetch:async()=>Response.json({error:{code:'PAID_API_REQUIRED',message:'Upgrade.',request_id:'ref'}},{status:403})});
  await assert.rejects(client.models(),error=>error.status===403&&error.requestId==='ref');
  client.transport=async()=>new Response('not json');await assert.rejects(client.models(),error=>error.code==='INVALID_RESPONSE');
});
test('PKCE uses S256 and callback state and issuer are mandatory',()=>{
  const proof=pkce();assert.ok(proof.verifier.length>=43);
  assert.equal(proof.challenge,createHash('sha256').update(proof.verifier).digest('base64url'));
  const url=new URL('http://127.0.0.1:8766/callback');url.searchParams.set('state',proof.state);url.searchParams.set('iss','https://clerk.quantura.studio');
  assert.equal(validCallback(url,proof.state),true);assert.equal(validCallback(url,'wrong'),false);
  url.searchParams.set('iss','https://untrusted.test');assert.equal(validCallback(url,proof.state),false);
});
test('proof exports keep the committed bytes and verification is read-only',async()=>{
  const bytes='{"nonce":"salt","value":100}',calls=[];
  const client=new Quantura({token:'test',fetch:async(url,options)=>{
    calls.push({url,options});return calls.length===3?new Response(bytes):Response.json({data:{status:'stamped',content_matches:true,stamp_found:true}});
  }});
  await client.stampForecast('f');await client.verifyForecast('f');assert.equal(await client.downloadForecastProof('f'),bytes);
  assert.equal(calls[0].options.method,'POST');assert.equal(calls[1].options.method,'GET');assert.equal(calls[1].url.searchParams.get('verify'),'true');
});
