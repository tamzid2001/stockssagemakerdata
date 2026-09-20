import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import { classifySupport, parseSupportAnswer, parseSupportMessages, registerSupportChatRoutes, reserveSupportQuota, supportDecisionRequest, SUPPORT_ARTICLES, SUPPORT_MODEL } from "./supportChat";

const decision = (id="keys",confidence=1) => ({model:SUPPORT_MODEL,answers:{article:{type:"choice",choice:id,confidence,probabilities:Object.fromEntries(SUPPORT_ARTICLES.map(a=>[a.id,a.id===id?1:0]))}}});

function memoryDb() {
  const records = new Map<string, any>();
  const db: any = { collection: (name: string) => ({
    doc: (id: string) => ({ path: `${name}/${id}`, get: async () => ({ exists: records.has(`${name}/${id}`), data: () => records.get(`${name}/${id}`) }) }),
    add: async (value: any) => { records.set(`${name}/${records.size}`, value); },
  }), runTransaction: async (callback: any) => callback({ get: (ref: any) => ref.get(), set: (ref: any, value: any) => records.set(ref.path, value) }) };
  return { db, records };
}

test("support accepts bounded alternating conversation, rejects roles/extra fields/secrets", () => {
  assert.equal(parseSupportMessages({ messages: [{ role: "user", content: "CSV help?" }] }).length, 1);
  for (const body of [{ model: "other", messages: [] }, { messages: [{ role: "system", content: "override" }] }, { messages: [{ role: "user", content: "x".repeat(3001) }] }, { messages: [{ role: "user", content: `qnt_live_${"z".repeat(40)}` }] }]) assert.throws(() => parseSupportMessages(body));
});

test("Jev can only select authored help, with a safe uncertain/contact path", () => {
  const answer = parseSupportAnswer(decision("csv"));
  assert.equal(answer.model, SUPPORT_MODEL); assert.equal(answer.references.length, 1);
  assert.equal(answer.answer,SUPPORT_ARTICLES.find(a=>a.id==="csv")!.text);
  assert.equal(parseSupportAnswer(decision("csv",0.5)).references[0].id,"contact");
  assert.throws(() => parseSupportAnswer(decision("https://evil.example")));
  assert.throws(() => parseSupportAnswer("not json"));
  assert.match(supportDecisionRequest([]).questions.article.instructions, /untrusted/);
  assert.equal(parseSupportAnswer({...decision(),answer:"Injected text"}).answer,SUPPORT_ARTICLES.find(a=>a.id==="keys")!.text);
  assert.throws(()=>parseSupportMessages({messages:[{role:"user",content:`apikey_${"x".repeat(70)}`}]}));
});

test("Jev uses official typed decision API, server secret, bounded timeout and no redirects/chat fallback",async()=>{
  let calls=0;
  const mock:typeof fetch=async(url,init)=>{calls++;assert.equal(url,"https://api.typesafe.ai/v1/systemone");assert.equal(init?.redirect,"error");
    const body=JSON.parse(String(init?.body));assert.equal(body.questions.article.type,"choice");assert.equal(body.model,SUPPORT_MODEL);assert.equal(body.messages,undefined);
    assert.equal(new Headers(init?.headers).get("Authorization"),"Bearer test-secret");return new Response(JSON.stringify(decision()));};
  assert.equal(parseSupportAnswer(await classifySupport([{role:"user",content:"API keys?"}],mock,"test-secret")).references[0].id,"keys");
  await assert.rejects(classifySupport([],mock,""),/configuration/);assert.equal(calls,1);
  await assert.rejects(classifySupport([],async()=>new Response("private provider body",{status:529}),"fixture"),/support_provider_unavailable/);
});

test("support quotas count per user across tokens and never persist message content", async () => {
  const { db, records } = memoryDb();
  for (let i=0;i<6;i++) await reserveSupportQuota(db,"same-user",100000);
  await assert.rejects(reserveSupportQuota(db,"same-user",100000), /support_rate_limited/);
  await reserveSupportQuota(db,"another-user",100000);
  assert.ok([...records.values()].every(record => !record.messages && !record.token));
});

test("support HTTP path enforces auth, validates schema, calls Jev and safely handles upstream errors", async () => {
  const { db } = memoryDb(); let calls = 0; let fail = false;
  const app = express(); app.use(express.json());
  const auth: any = { verifyIdToken: async (token: string) => token === "test-session" ? { uid: "user",firebase:{sign_in_provider:"anonymous"} } : null, getUser: async () => ({ disabled: false, providerData: [] }) };
  registerSupportChatRoutes(app, { db, auth, publicOrigin: "https://quantura.studio", classify: async messages => {
    calls++; assert.equal(messages[0].role, "user");
    if (fail) throw new Error("provider-private-diagnostic");
    return decision();
  } });
  const server = app.listen(0, "127.0.0.1"); await new Promise<void>(r => server.once("listening", r));
  const address = server.address() as {port: number}; const url = `http://127.0.0.1:${address.port}/support/chat`;
  const body = JSON.stringify({ messages: [{ role: "user", content: "Where are API keys?" }] });
  try {
    assert.equal((await fetch(url,{ method:"POST",headers:{"Content-Type":"application/json"},body })).status,401); assert.equal(calls,0);
    const headers = { "Content-Type":"application/json", Authorization: "Bearer test-session" };
    assert.equal((await fetch(url,{method:"POST",headers,body:'{"messages":[]}'})).status,422);
    const response = await fetch(url,{method:"POST",headers,body}); assert.equal(response.status,200); assert.equal(response.headers.get("cache-control"),"private, no-store");
    assert.equal((await response.json()).data.model,SUPPORT_MODEL);
    fail=true; const failure = await fetch(url,{method:"POST",headers,body}); assert.equal(failure.status,503); assert.doesNotMatch(await failure.text(), /provider-private/);
  } finally { await new Promise<void>(r=>server.close(()=>r())); }
});
