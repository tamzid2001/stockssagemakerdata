import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {registerMarketDataRoutes} from './marketDataRoutes';
import {rapidApiPrincipal} from './rapidApiAuth';
import {PredictionMarketDataError} from './predictionMarketData';

test('shared history authenticates mounted RapidAPI customers and preserves perpetual units, CSV and provider errors',async()=>{
  const old={...process.env};process.env.RAPIDAPI_PROXY_SECRET='x'.repeat(48);process.env.RAPIDAPI_API_ID='api_test';
  const calls:any[]=[];const rows=[{timestamp:'2026-09-20T12:00:00Z',open:null,high:81501,low:81500,close:81500,volume:100}];
  const app=express();app.use(express.json());const router=express.Router();
  router.use((req,res,next)=>{try{assert.ok(rapidApiPrincipal(req));next();}catch{res.sendStatus(403);}});
  registerMarketDataRoutes(router,{perps:{history:async(input)=>{calls.push(input);if(input.symbol==='BAD')throw new PredictionMarketDataError('perp_not_found','Not listed.',404);return {provider:'kalshi_perps',symbol:'KXBTCPERP',frequency:'1h',timeframe:'1h',rows,count:1,market:{} as any,metadata:{units:'USD per underlying unit',contract_size:.0001},warnings:[]};}}});app.use('/api/v1',router);
  const server=app.listen(0,'127.0.0.1');await new Promise<void>(resolve=>server.once('listening',resolve));
  const url=`http://127.0.0.1:${(server.address() as any).port}/api/v1/market-data/history`;
  const call=(body:any)=>fetch(url,{method:'POST',headers:{'content-type':'application/json','x-rapidapi-proxy-secret':'x'.repeat(48),'x-rapidapi-user':'test_user','x-rapidapi-subscription':'BASIC'},body:JSON.stringify(body)});
  try {
    const input={source:'kalshi_perps',symbol:'KXBTCPERP',timeframe:'1h',limit:500};
    const json=await call(input);assert.equal(json.status,200);const data=await json.json();assert.equal(data.rows[0].close,81500);assert.equal(data.metadata.units,'USD per underlying unit');assert.equal(calls[0].frequency,'1h');
    const csv=await call({...input,format:'csv'});assert.match(csv.headers.get('content-type')!,/text\/csv/);assert.match(await csv.text(),/2026-09-20T12:00:00Z,,81501,81500,81500,100/);assert.equal(csv.headers.get('x-price-unit'),'USD per underlying unit');
    assert.equal((await call({...input,symbol:'BAD'})).status,404);
    assert.equal((await call({...input,source:'unknown'})).status,422);
    assert.equal((await call({...input,format:'xml'})).status,422);
  }finally{await new Promise<void>(resolve=>server.close(()=>resolve()));process.env=old;}
});
