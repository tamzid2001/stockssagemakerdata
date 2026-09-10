import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import { parseSupportAnswer, parseSupportMessages, registerSupportChatRoutes, reserveSupportQuota, supportPrompt, SUPPORT_MODEL } from "./supportChat";

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

test("support structured answers restrict references and expose the actual requested model", () => {
  const answer = parseSupportAnswer(JSON.stringify({ answer: "Open your CSV library.", article_ids: ["csv"], escalate: true }));
  assert.equal(answer.model, "gpt-5.6-luna"); assert.equal(answer.references.length, 2);
  assert.throws(() => parseSupportAnswer(JSON.stringify({ answer: "x", article_ids: ["https://evil.example"], escalate: false })));
  assert.throws(() => parseSupportAnswer("not json"));
  assert.match(supportPrompt(), /cannot inspect accounts/); assert.match(supportPrompt(), /untrusted/);
});

test("support quotas count per user across tokens and never persist message content", async () => {
  const { db, records } = memoryDb();
  for (let i=0;i<6;i++) await reserveSupportQuota(db,"same-user",100000);
  await assert.rejects(reserveSupportQuota(db,"same-user",100000), /support_rate_limited/);
  await reserveSupportQuota(db,"another-user",100000);
  assert.ok([...records.values()].every(record => !record.messages && !record.token));
});

test("support HTTP path enforces auth, validates schema, calls Luna and safely handles upstream errors", async () => {
  const { db } = memoryDb(); let calls = 0; let fail = false;
  const app = express(); app.use(express.json());
  const auth: any = { verifyIdToken: async (token: string) => token === "test-session" ? { uid: "user" } : null, getUser: async () => ({ disabled: false, providerData: [{ providerId: "password" }] }) };
  registerSupportChatRoutes(app, { db, auth, publicOrigin: "https://quantura.studio", complete: async messages => {
    calls++; assert.equal(messages[0].role, "system");
    if (fail) throw new Error("provider-private-diagnostic");
    return JSON.stringify({ answer: "Open Account then API Keys.", article_ids: ["keys"], escalate: false });
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
