import assert from "node:assert/strict";
import test from "node:test";
import {canonicalJson,contentCid,ensureForecastProof,normalizeReceipt,proofManifest,proofSeed,publicProof,VBaseClient} from "./forecastProof";

const collection = `0x${"a".repeat(64)}`;
const receipt = (cid:string) => ({object_cid:cid,set_cid:collection,transaction_hash:`0x${"b".repeat(64)}`,
  user_address:`0x${"c".repeat(40)}`,chain_id:8453,timestamp:"2026-10-09T15:00:00Z"});
const fixture = () => ({job:{status:"completed",source:{type:"ticker",provider:"alpaca",symbol:"SPY"},
  input_cutoff_at:"2025-10-09T20:00:00Z",request:{frequency:"1D",quantiles:[.01,.5,.99]},
  user_id:"private-owner",workspace_id:"private-workspace",provenance:proofSeed()},
  result:{created_at:"2026-10-09T14:00:00Z",predictions:[{timestamp:"2025-10-10T20:00:00Z",quantiles:{"0.01":90,"0.5":100,"0.99":110}}],result_hash:"immutable-result"}});

function store() {
  const {job,result} = fixture();let writes=0;
  const data:any=job;
  const snap=()=>({exists:true,data:()=>structuredClone(data)});
  const ref:any={id:"forecast-test",get:async()=>snap()};
  let tail=Promise.resolve();
  const db:any={collection:()=>({doc:()=>({get:async()=>({exists:true,data:()=>structuredClone(result)})})}),
    runTransaction:(fn:any)=>{const promise=tail.then(()=>fn({get:async()=>snap(),update:(_ref:any,patch:any)=>{Object.assign(data,structuredClone(patch));writes++;}}));tail=promise.catch(()=>{});return promise;}};
  return {db,ref,data,result,writes:()=>writes};
}
function enable() {process.env.VBASE_ENABLED="true";process.env.VBASE_COLLECTION_CID=collection;}

test("canonical commitments preserve exact UTF-8, input cutoff and generation time",()=>{
  assert.equal(contentCid("abc"),"0x3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532");
  assert.equal(canonicalJson({b:2,a:[1,0]}),'{"a":[1,0],"b":2}');
  assert.throws(()=>canonicalJson({a:NaN}),/content_invalid/);
  const {job,result}=fixture(), bytes=proofManifest("f",job,result), parsed=JSON.parse(bytes);
  assert.equal(parsed.input_cutoff_at,job.input_cutoff_at);assert.equal(parsed.generated_at,result.created_at);
  assert.doesNotMatch(bytes,/private-owner|private-workspace/);
  assert.equal(contentCid(bytes),contentCid(proofManifest("f",{...job,user_id:"transferred",updated_at:"later"},result)));
  assert.notEqual(contentCid(bytes),contentCid(proofManifest("f",job,{...result,predictions:[]})));
  assert.notEqual(contentCid(bytes),contentCid(bytes+"\n"));
  assert.equal(publicProof(job.provenance)?.nonce,undefined);
});

test("vBase requests send only a CID; recovery looks up existing stamps",async()=>{
  const cid=contentCid("secret CSV content"),calls:any[]=[];
  const client=new VBaseClient("private-test-key",collection,(async(url:any,options:any)=>{
    calls.push({url,options});
    return Response.json(String(url).endsWith("verify")?{stamp_list:[]}:{commitment_receipt:receipt(cid)});
  }) as typeof fetch);
  assert.deepEqual(await client.stamp(cid),normalizeReceipt(receipt(cid),cid,collection));
  assert.equal(calls[0].options.redirect,"error");
  assert.deepEqual(JSON.parse(calls[0].options.body),{cids:[cid],filter_by_user:true});
  const form=new URLSearchParams(calls[1].options.body);
  assert.equal(form.get("data_cid"),cid);assert.equal(form.get("store_stamped_file"),"false");assert.equal(form.get("collection_cid"),collection);
  assert.equal(form.has("file"),false);assert.doesNotMatch(calls[1].options.body,/secret CSV content/);
  let requests=0;
  const recovery=new VBaseClient("key",collection,(async()=>{requests++;return Response.json({stamp_list:[receipt(cid)]});}) as typeof fetch);
  await recovery.stamp(cid);assert.equal(requests,1);
});

test("wrong content, collection, transaction and malformed responses cannot be called verified",async()=>{
  const cid=contentCid("value");
  for(const patch of [{object_cid:collection},{set_cid:cid},{timestamp:"invalid"},{transaction_hash:"javascript:bad"},{chain_id:"8453"}])
    assert.throws(()=>normalizeReceipt({...receipt(cid),...patch},cid,collection),/receipt_invalid/);
  const client=new VBaseClient("key",collection,(async()=>Response.json({stamp_list:[{...receipt(cid),set_cid:cid}]})) as typeof fetch);
  assert.equal(await client.verify(cid),null);
  const outage=new VBaseClient("do-not-leak",collection,(async()=>{throw new Error("transport failed with do-not-leak");}) as typeof fetch);
  await assert.rejects(()=>outage.stamp(cid),e=>String(e)==="Error: forecast_proof_unavailable");
  const malformed=new VBaseClient("key",collection,(async()=>Response.json({unexpected:true})) as typeof fetch);
  await assert.rejects(()=>malformed.stamp(cid),/receipt_invalid/);
});

test("concurrent callbacks stamp once, keep the nonce and avoid subsequent writes",async()=>{
  enable();const s=store(),nonce=s.data.provenance.nonce;let stampCalls=0;
  const transport=(async(url:any,options:any)=>{
    if(String(url).endsWith("verify"))return Response.json({stamp_list:[]});
    stampCalls++;return Response.json({commitment_receipt:receipt(new URLSearchParams(options.body).get("data_cid")!)});
  }) as typeof fetch;
  await Promise.all([ensureForecastProof(s.db,s.ref,async()=>"key",transport),ensureForecastProof(s.db,s.ref,async()=>"key",transport)]);
  assert.equal(stampCalls,1);assert.equal(s.data.provenance.status,"stamped");assert.equal(s.data.provenance.nonce,nonce);
  assert.equal(s.writes(),2);
  await ensureForecastProof(s.db,s.ref,async()=>"key",transport);assert.equal(s.writes(),2);assert.equal(stampCalls,1);
});

test("an ambiguous stamp failure recovers the same commitment without a duplicate stamp",async()=>{
  enable();const s=store();let committed:string|undefined,stampCalls=0;
  const transport=(async(url:any,options:any)=>{
    if(String(url).endsWith("verify"))return Response.json({stamp_list:committed?[receipt(committed)]:[]});
    stampCalls++;committed=new URLSearchParams(options.body).get("data_cid")!;
    return new Response("upstream secret",{status:503});
  }) as typeof fetch;
  assert.equal((await ensureForecastProof(s.db,s.ref,async()=>"key",transport))?.status,"unavailable");
  assert.equal(s.data.status,"completed");assert.equal(s.data.provenance.error_code,"forecast_proof_unavailable");
  await ensureForecastProof(s.db,s.ref,async()=>"key",transport);assert.equal(stampCalls,1);
  s.data.provenance.retry_after=0;
  assert.equal((await ensureForecastProof(s.db,s.ref,async()=>"key",transport))?.status,"stamped");
  assert.equal(s.data.provenance.content_cid,committed);assert.equal(stampCalls,1);
});

test("a stale stamping worker cannot overwrite a newer lease",async()=>{
  enable();const s=store();
  const transport=(async(url:any,options:any)=>{
    if(String(url).endsWith("verify"))return Response.json({stamp_list:[]});
    s.data.provenance.lease_token="new-owner";
    return Response.json({commitment_receipt:receipt(new URLSearchParams(options.body).get("data_cid")!)});
  }) as typeof fetch;
  await ensureForecastProof(s.db,s.ref,async()=>"key",transport);
  assert.equal(s.data.provenance.lease_token,"new-owner");assert.equal(s.data.provenance.status,"stamping");
});
