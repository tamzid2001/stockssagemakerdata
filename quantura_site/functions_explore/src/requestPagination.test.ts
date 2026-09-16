import assert from "node:assert/strict";
import test from "node:test";
import { Timestamp } from "firebase-admin/firestore";
import { decodeRequestCursor, scanRequestPage, selectRequestPage } from "./requestPagination";

test("request pagination reaches 120 records without duplicating same-timestamp rows", async () => {
  const docs = Array.from({length:120},(_,i)=>({id:`request_${String(120-i).padStart(3,"0")}`,data:{updatedAt:new Timestamp(1000,123),deleted:false}}));
  let cursor: string | null = null;
  const seen: string[]=[];
  do {
    const p=decodeRequestCursor(cursor || undefined,"owner");
    const start=p?docs.findIndex(doc=>doc.id===p.id)+1:0;
    const result=selectRequestPage(docs.slice(start),"owner",40,false,doc=>doc.id);
    seen.push(...result.items);cursor=result.next_cursor;
  } while(cursor);
  assert.equal(seen.length,120);assert.equal(new Set(seen).size,120);
});
test("cursor advances across deleted and filtered records and rejects foreign/malformed positions",async()=>{
  const docs=Array.from({length:140},(_,i)=>({id:`r${i}`,data:{updatedAt:new Timestamp(1000-i,99)}}));
  const page=selectRequestPage(docs,"owner",40,true,()=>null);
  assert.equal(page.items.length,0);assert.equal(page.has_more,true);
  const cursor=decodeRequestCursor(page.next_cursor,"owner")!;assert.equal(cursor.id,"r139");assert.equal(cursor.nanoseconds,99);
  for(const invalid of [page.next_cursor,[],"garbage",Buffer.from(JSON.stringify({...cursor,user:'stranger',id:'../x'})).toString('base64url')]) assert.throws(()=>decodeRequestCursor(invalid,"stranger"),/invalid_request_cursor/);
  const calls:unknown[][]=[];
  const query:any={collection:(...v:unknown[])=>{calls.push(['collection',...v]);return query;},doc:(...v:unknown[])=>{calls.push(['doc',...v]);return query;},orderBy:(...v:unknown[])=>{calls.push(['orderBy',...v]);return query;},startAfter:(...v:unknown[])=>{calls.push(['startAfter',...v]);return query;},limit:(...v:unknown[])=>{calls.push(['limit',...v]);return query;},get:async()=>({docs:[],size:0})};
  await scanRequestPage(query,"owner",page.next_cursor,40);
  assert.deepEqual(calls.slice(0,3),[['collection','users'],['doc','owner'],['collection','requests']]);
  assert.equal(calls.find(c=>c[0]==='startAfter')?.[2],'r139');
  assert.deepEqual(calls.at(-1),['limit',41]);
});
