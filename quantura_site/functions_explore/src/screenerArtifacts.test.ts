import test from "node:test";
import assert from "node:assert/strict";
import {createHash} from "node:crypto";
import {gzipSync} from "node:zlib";
import {decodeSnapshotZip,ScreenerArtifactReader} from "./screenerArtifacts";
const Zip=require("adm-zip");
const snapshot={schema_version:"quantura-public-screener-v1",feed:"perps-0",published_at:new Date().toISOString(),data:{items:[{ticker:"KXBTCPERP"}]}};
function zip(value:any=snapshot,name="snapshot.json.gz"){const z=new Zip();z.addFile("snapshot.json.gz",gzipSync(JSON.stringify(value)));z.getEntries()[0].entryName=name;return z.toBuffer();}
test("public artifacts reject traversal, multiple entries, cross-feed payloads and tampered bytes",()=>{
  assert.deepEqual(decodeSnapshotZip(zip(),"perps-0"),snapshot);
  assert.throws(()=>decodeSnapshotZip(zip(snapshot,"../snapshot.json.gz"),"perps-0"),/archive_invalid/);
  const z=new Zip();z.addFile("snapshot.json.gz",gzipSync(JSON.stringify(snapshot)));z.addFile("secret.json",Buffer.from("private"));
  assert.throws(()=>decodeSnapshotZip(z.toBuffer(),"perps-0"),/archive_invalid/);
  assert.throws(()=>decodeSnapshotZip(zip(),"games-kalshi-0"),/artifact_invalid/);
  assert.throws(()=>decodeSnapshotZip(zip(),"perps-0","sha256:wrong"),/digest_invalid/);
});
test("concurrent readers reuse verified artifacts and do not forward credentials to storage",async()=>{
  const memory=new Map<string,unknown>(),cache:any={get:async(k:string)=>memory.get(k),set:async(k:string,v:any)=>memory.set(k,v)};
  const bytes=zip(),calls:string[]=[];const artifact={id:1,name:"quantura-public-perps-0",size_in_bytes:bytes.length,expired:false,created_at:new Date().toISOString(),digest:`sha256:${createHash("sha256").update(bytes).digest("hex")}`,workflow_run:{id:2,head_branch:"main",head_sha:"abc",repository_id:3,head_repository_id:3}};
  const request:any=async(url:any,options:any)=>{calls.push(String(url));
    if(String(url).includes("blob.core.windows.net")){assert.equal(options.headers,undefined);return new Response(bytes);}
    if(String(url).endsWith("/zip"))return new Response(null,{status:302,headers:{location:"https://test.blob.core.windows.net/signed"}});
    if(String(url).includes("/actions/runs/"))return Response.json({path:".github/workflows/hourly-perpetual-screener.yml",head_branch:"main",head_sha:"abc",event:"schedule",repository:{id:3},head_repository:{id:3}});
    return Response.json({artifacts:[artifact]});
  };
  const reader=new ScreenerArtifactReader(request,"owner","repo",cache);
  assert.deepEqual(await Promise.all([reader.read("perps-0"),reader.read("perps-0")]),[snapshot,snapshot]);
  assert.equal(calls.filter(u=>u.includes("blob.core")).length,1);
  reader.clear();await reader.read("perps-0");assert.equal(calls.length,4,"shared immutable cache avoids another GitHub archive download");
  await assert.rejects(reader.read("../private"),/feed_invalid/);
});
test("forks and unknown workflow publications cannot replace the public screener",async()=>{
  const cache:any={get:async()=>null,set:async()=>{}};
  const request:any=async()=>Response.json({artifacts:[{id:1,name:"quantura-public-perps-0",expired:false,size_in_bytes:1,workflow_run:{head_branch:"main",head_repository_id:9,repository_id:3}}]});
  await assert.rejects(new ScreenerArtifactReader(request,"o","r",cache).read("perps-0"),/not_published/);
});

test("a newer upload wins even when GitHub returns an older artifact with a larger ID first",async()=>{
  const bytes=zip(),cache:any={get:async()=>null,set:async()=>{}};
  const base={name:"quantura-public-perps-0",size_in_bytes:bytes.length,expired:false,workflow_run:{id:2,head_branch:"main",head_sha:"abc",repository_id:3,head_repository_id:3}};
  let downloaded=0;
  const request:any=async(input:any)=>{
    const url=String(input);
    if(url.includes("blob.core.windows.net"))return new Response(bytes);
    if(url.endsWith("/zip")){downloaded=Number(url.split("/").at(-2));return new Response(null,{status:302,headers:{location:"https://test.blob.core.windows.net/newer"}});}
    if(url.includes("/actions/runs/"))return Response.json({path:".github/workflows/hourly-perpetual-screener.yml",head_branch:"main",head_sha:"abc",event:"workflow_dispatch",repository:{id:3},head_repository:{id:3}});
    return Response.json({artifacts:[{...base,id:99,created_at:"2026-10-07T14:41:00Z"},{...base,id:1,created_at:"2026-10-07T14:49:00Z"}]});
  };
  await new ScreenerArtifactReader(request,"o","r",cache).read("perps-0");
  assert.equal(downloaded,1);
});
