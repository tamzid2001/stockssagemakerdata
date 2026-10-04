import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import express from "express";
import { registerTikTokRoutes, tiktokRawBodyMiddleware, verifyTikTokSignature, encryptTikTokTokens, TIKTOK_REDIRECT_URI } from "./tiktokIntegration";

const secret="unit-test-client-secret",key="unit-test-client-key";
const signature=(body:Buffer,t=Math.floor(Date.now()/1000))=>`t=${t},s=${crypto.createHmac("sha256",secret).update(`${t}.`).update(body).digest("hex")}`;
test("TikTok verifies exact raw bytes and rejects tampering, missing signatures and stale timestamps",()=>{
  const body=Buffer.from('{"event": "test"}');
  assert.ok(verifyTikTokSignature(body,signature(body),secret));
  assert.equal(verifyTikTokSignature(Buffer.from('{"event":"test"}'),signature(body),secret),false);
  assert.equal(verifyTikTokSignature(body,signature(body,Math.floor(Date.now()/1000)-301),secret),false);
  assert.equal(verifyTikTokSignature(body,signature(body),"wrong-secret"),false);
  for(const header of ["",`t=NaN,s=${"0".repeat(64)}`,"t=1,s=x",signature(body)+",t=1"])assert.equal(verifyTikTokSignature(body,header,secret),false);
});
test("TikTok tokens are encrypted with randomized authenticated ciphertext",()=>{
  const input={access_token:"synthetic-access-token",refresh_token:"synthetic-refresh-token"};
  const first=encryptTikTokTokens(input,secret),second=encryptTikTokTokens(input,secret);
  assert.notEqual(first,second);assert.ok(!first.includes(input.access_token));
  const [,nonce,tag,payload]=first.split(".");
  const cipher=crypto.createDecipheriv("aes-256-gcm",crypto.createHash("sha256").update(`quantura-tiktok-tokens-v1:${secret}`).digest(),Buffer.from(nonce,"base64url"));
  cipher.setAuthTag(Buffer.from(tag,"base64url"));
  assert.deepEqual(JSON.parse(Buffer.concat([cipher.update(Buffer.from(payload,"base64url")),cipher.final()]).toString()),input);
});

test("TikTok callback binds single-use state to the browser and keeps exchanged tokens off responses",async()=>{
  const records=new Map<string,any>(),calls:any[]=[];
  const db={collection:(collection:string)=>({doc:(id:string)=>({key:`${collection}/${id}`,set:async(value:any)=>{records.set(`${collection}/${id}`,value);}})}),
    runTransaction:async(fn:any)=>fn({get:async(ref:any)=>({exists:records.has(ref.key),data:()=>records.get(ref.key)}),delete:(ref:any)=>records.delete(ref.key),create:(ref:any,data:any)=>records.set(ref.key,data),set:(ref:any,data:any)=>records.set(ref.key,data)})} as any;
  const app=express(),router=express.Router();
  app.use("/api/integrations/tiktok/webhook",tiktokRawBodyMiddleware());app.use(express.json());
  registerTikTokRoutes(router,{db,clientKey:key,clientSecret:secret,authenticate:async(req)=>({userId:"admin-test",platformAdmin:req.headers.authorization==="Bearer test-admin"}),request:async(url,options)=>{calls.push({url,options});return {ok:true,json:async()=>({access_token:"synthetic-access-token",refresh_token:"synthetic-refresh-token",open_id:"test-open-id",expires_in:86400,refresh_expires_in:31536000,scope:"user.info.basic"})} as any;}});
  app.use("/api",router);
  const server=app.listen(0,"127.0.0.1");await new Promise<void>(resolve=>server.once("listening",resolve));
  const origin=`http://127.0.0.1:${(server.address() as any).port}`;
  try {
    assert.equal((await fetch(`${origin}/api/integrations/tiktok/connect`,{method:"POST"})).status,403);
    const connect=await fetch(`${origin}/api/integrations/tiktok/connect`,{method:"POST",headers:{Authorization:"Bearer test-admin"}});
    const data=await connect.json() as any,url=new URL(data.authorization_url),state=url.searchParams.get("state");
    assert.equal(url.searchParams.get("redirect_uri"),TIKTOK_REDIRECT_URI);assert.equal(url.searchParams.get("scope"),"user.info.basic");
    const cookie=connect.headers.get("set-cookie")!.split(";")[0];
    const callback=`${origin}/api/integrations/tiktok/callback?code=synthetic-code&state=${state}`;
    assert.equal((await fetch(callback)).status,400);assert.equal(calls.length,0);
    const response=await fetch(callback,{headers:{Cookie:cookie},redirect:"manual"});
    assert.equal(response.status,303);assert.equal(response.headers.get("location"),"/forecasting?panel=profile&tiktok=connected");
    assert.equal(calls.length,1);assert.equal(calls[0].options.body.get("redirect_uri"),TIKTOK_REDIRECT_URI);
    const connection=[...records.entries()].find(([path])=>path.startsWith("tiktok_connections/"))![1];
    assert.equal(connection.owner_user_id,"admin-test");assert.ok(!JSON.stringify(connection).includes("synthetic-access-token"));
    assert.equal((await fetch(callback,{headers:{Cookie:cookie}})).status,400);assert.equal(calls.length,1);
    const body=Buffer.from(JSON.stringify({client_key:key,event:"video.publish.completed",user_openid:"test-open-id",content:"{}"}));
    const send=()=>fetch(`${origin}/api/integrations/tiktok/webhook`,{method:"POST",headers:{"Content-Type":"application/json","TikTok-Signature":signature(body)},body});
    assert.equal((await send()).status,200);assert.equal((await send()).status,200);
    assert.equal([...records.keys()].filter(path=>path.startsWith("tiktok_webhook_receipts/")).length,1);
    assert.equal((await fetch(`${origin}/api/integrations/tiktok/webhook`,{method:"POST",headers:{"Content-Type":"application/json"},body})).status,401);
  } finally {server.closeAllConnections();await new Promise<void>(resolve=>server.close(()=>resolve()));}
});
