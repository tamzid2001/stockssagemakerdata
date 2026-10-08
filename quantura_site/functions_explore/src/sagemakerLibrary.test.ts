import test from 'node:test';
import assert from 'node:assert/strict';
import {parseCanvas,CanvasRepository,quantileColumn,registerCanvasRoutes} from './sagemakerLibrary';
import * as access from './apiAccess';
const csv='date,P01,P25,P50,P75,P90,P99\n2026-10-01,80,90,100,110,115,130\n2026-10-02,81,91,101,111,116,131\n';
test('Canvas auto-detects quantiles, preserves values and separates history from predictions',()=>{
 const p=parseCanvas(csv);assert.deepEqual(p.quantiles,[.01,.25,.5,.75,.9,.99]);assert.equal(p.predictions[0].quantiles['0.5'],100);assert.deepEqual(p.history,[]);assert.equal(p.frequency,'1D');
 assert.equal(quantileColumn('P1'),.01);assert.equal(quantileColumn('q0.25'),.25);assert.equal(quantileColumn('P100'),null);
 const h=parseCanvas('Item_Id,Datetime,Price\nBTC,2026-01-01 00:00:00,100\nBTC,2026-01-01 01:00:00,101\n');assert.equal(h.frequency,'1h');assert.equal(h.history.length,2);
});
test('malformed dates, rows, quotes, duplicates, crossed quantiles and credentials are rejected',()=>{
 for(const text of [csv.replace('2026-10-02','2026-02-31'),csv.replace('2026-10-02','2026-10-01'),csv.replace('80,90','95,90'),csv.replace('81,91','81,'),csv.replace('131','Infinity'),csv.replace('date','"date'),csv+'-----BEGIN RSA PRIVATE KEY-----'])assert.throws(()=>parseCanvas(text));
 assert.deepEqual(parseCanvas(csv.replace('date','"date"')).quantiles,[.01,.25,.5,.75,.9,.99]);
});
test('atomic Git publishing rereads the branch/catalog on a ref conflict and never force-pushes',async()=>{
 process.env.GITHUB_SAGEMAKER_TOKEN='test-only';let updates=0,reads=0;const bodies:any[]=[];
 const request:any=async(url:string,options:any={})=>{
  const body=options.body?JSON.parse(options.body):null;if(body)bodies.push(body);
  if(url.includes('raw.githubusercontent.com')){reads++;return new Response(JSON.stringify({schema_version:'sagemaker_catalog_v1',items:[]}));}
  if(options.method==='PATCH'){updates++;assert.equal(body.force,false);return new Response('{}',{status:updates===1?422:200});}
  if(url.includes('git/ref/heads/'))return Response.json({object:{sha:'parent'+updates}});
  if(url.includes('git/commits/'))return Response.json({tree:{sha:'base'}});
  return Response.json({sha:url.includes('git/commits')?'commit':'blob'});
 };
 const repo=new CanvasRepository(request),result=await repo.publish({csv,name:'Example',ticker:'EX',metrics:{rmse:1.2,smape:.05},metrics_basis:'Canvas holdout'});
 assert.equal(updates,2);assert.equal(reads,2);assert.equal(result.item.metrics?.rmse,1.2);assert.ok(bodies.some(b=>b.tree?.some((f:any)=>f.path==='sagemaker/catalog.json')));
 await assert.rejects(repo.publish({csv,name:'Example',ticker:'EX',metrics:{smape:300}}),/metrics_invalid/);
 delete process.env.GITHUB_SAGEMAKER_TOKEN;
});
test('publish denies ordinary paid users before any Git write',async t=>{
 t.mock.method(access,'authenticatePlatformRequest',async()=>({userId:'ordinary',guest:false,authMethod:'clerk_session',platformAdmin:false} as any));
 const routes=new Map<string,Function>();registerCanvasRoutes({get:(p:string,f:Function)=>routes.set(p,f),post:(p:string,f:Function)=>routes.set('POST '+p,f)} as any,{db:{} as any,auth:{} as any,adminEmails:['tamzid257@gmail.com']});
 let status=200,payload:any;const res:any={set:()=>res,status:(s:number)=>{status=s;return res;},json:(p:any)=>payload=p};await routes.get('POST /sagemaker')!({body:{csv,name:'Example',ticker:'EX'}},res);assert.equal(status,403);assert.equal(payload.error,'admin_required');
});
