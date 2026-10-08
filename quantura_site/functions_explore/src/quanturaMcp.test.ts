import assert from "node:assert/strict";
import { test } from "node:test";
import express from "express";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { registerQuanturaMcpRoutes } from "./quanturaMcp";

test("real MCP client discovers seven OAuth tools and preserves forecast idempotency",async()=>{
  const calls:any[]=[];
  const app=express();app.use(express.json());
  registerQuanturaMcpRoutes(app,{authenticate:async(req)=>{
    if(req.get("authorization")!=="Bearer test-opaque")throw new Error("api_key_missing");
    return {authMethod:"clerk_oauth"} as any;
  },fetch:(async(url:any,options:any)=>{
    calls.push({url,options});return new Response(JSON.stringify({data:{id:"forecast_test",status:"queued"}}),{status:200});
  }) as typeof fetch});
  const server=app.listen(0,"127.0.0.1");await new Promise<void>(resolve=>server.on("listening",resolve));
  const base=`http://127.0.0.1:${(server.address() as any).port}`;
  const client=new Client({name:"test",version:"1"});
  try {
    const metadata=await fetch(base+"/.well-known/oauth-protected-resource/mcp").then(r=>r.json());
    assert.equal(metadata.resource,"https://quantura.studio");assert.deepEqual(metadata.authorization_servers,["https://clerk.quantura.studio"]);
    await client.connect(new StreamableHTTPClientTransport(new URL(base+"/mcp"),{requestInit:{headers:{Authorization:"Bearer test-opaque"}}}));
    const catalog=await client.listTools();assert.equal(catalog.tools.length,7);
    assert.equal(catalog.tools.find(x=>x.name==="quantura_create_forecast")?.annotations?.readOnlyHint,false);
    const result=await client.callTool({name:"quantura_create_forecast",arguments:{request:{source:{symbol:"AAPL"},history_lag_minutes:172800},idempotency_key:"same-logical-job"}});
    assert.equal(result.isError,false);
    assert.equal(calls[0].url,"https://quantura.studio/api/v1/ensemble-forecasts");
    assert.equal(calls[0].options.headers["Idempotency-Key"],"same-logical-job");
    assert.equal(calls[0].options.headers.Authorization,"Bearer test-opaque");
    assert.equal(JSON.parse(calls[0].options.body).history_lag_minutes,172800);
    const unauth=await fetch(base+"/mcp",{method:"POST",headers:{"Content-Type":"application/json","Accept":"application/json, text/event-stream"},
      body:JSON.stringify({jsonrpc:"2.0",id:3,method:"tools/call",params:{name:"quantura_my_access",arguments:{}}})});
    assert.equal(unauth.status,401);assert.match(unauth.headers.get("www-authenticate")!,/resource_metadata=.*oauth-protected-resource\/mcp/);
    const forbidden=await fetch(base+"/mcp",{method:"POST",headers:{"Content-Type":"application/json",Origin:"https://untrusted.test"},body:"{}"});
    assert.equal(forbidden.status,403);
  }finally {await client.close();await new Promise<void>(resolve=>server.close(()=>resolve()));}
});

test("MCP preserves paid-access denial rather than treating it as a sign-in failure",async()=>{
  const app=express();app.use(express.json());registerQuanturaMcpRoutes(app,{authenticate:async()=>{throw new Error("paid_api_required");}});
  const server=app.listen(0,"127.0.0.1");await new Promise<void>(resolve=>server.on("listening",resolve));
  try {
    const response=await fetch(`http://127.0.0.1:${(server.address() as any).port}/mcp`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({jsonrpc:"2.0",id:1,method:"tools/call"})});
    assert.equal(response.status,403);assert.match((await response.json()).error.message,/Paid Pro/);
  }finally {await new Promise<void>(resolve=>server.close(()=>resolve()));}
});
