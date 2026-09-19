import test from "node:test";
import assert from "node:assert/strict";
import { BrevoNotificationMailer, createBrevoTransport, emailIdempotencyKey, fixieBillingWindow, type EmailDeliveryLedger } from "./brevoEmail";

const email = { id: "user-digest-2026-09-19", to: "test@example.com", subject: "Saved filter matches", text: "Test", html: "<p>Test</p>" };
const env = { BREVO_SENDER_EMAIL: "hello@example.com" };
function ledger(): EmailDeliveryLedger & { statuses: string[] } {
  return { statuses: [], async claim() { return {}; }, async finish(_id, status) { this.statuses.push(status); } };
}
test("Fixie is mandatory and an arbitrary proxy is rejected", () => {
  for (const FIXIE_URL of ["", "https://attacker.example", "http://user:secret@usefixie.com.evil.test", "http://user:secret@proxy.usefixie.com?secret=bad"]) {
    assert.throws(() => createBrevoTransport({ FIXIE_URL, BREVO_API_KEY: "test-only" }), /email_configuration/);
  }
});
test("billing period follows the integration reset, not calendar month", () => {
  assert.equal(fixieBillingWindow(new Date("2026-10-18T23:59:59Z")), "2026-09-19");
  assert.equal(fixieBillingWindow(new Date("2026-10-19T00:00:00Z")), "2026-10-19");
  assert.equal(fixieBillingWindow(new Date("2026-01-01T00:00:00Z")), "2025-12-19");
});
test("deterministic UUID idempotency keys", () => {
  assert.match(emailIdempotencyKey(email.id), /^[a-f\d]{8}-[a-f\d]{4}-5[a-f\d]{3}-a[a-f\d]{3}-[a-f\d]{12}$/);
  assert.equal(emailIdempotencyKey(email.id), emailIdempotencyKey(email.id));
});
test("one private recipient and provider message ID are persisted", async () => {
  const repo = ledger();
  const mailer = new BrevoNotificationMailer(repo, env, async (payload) => {
    assert.deepEqual(payload.to, [{ email: email.to }]);
    assert.deepEqual(payload.headers, { "Idempotency-Key": emailIdempotencyKey(email.id) });
    return { status: 201, messageId: "provider-accepted-123" };
  });
  assert.equal((await mailer.send(email)).messageId, "provider-accepted-123");
  assert.deepEqual(repo.statuses, ["sent"]);
});
test("already delivered email consumes no proxy request", async () => {
  const repo = ledger(); repo.claim = async () => ({ messageId: "already-sent" });
  const mailer = new BrevoNotificationMailer(repo, env, async () => { throw new Error("should not send"); });
  assert.equal((await mailer.send(email)).messageId, "already-sent");
});
test("network ambiguity is not automatically resent and secrets are not propagated", async () => {
  const repo = ledger();
  const mailer = new BrevoNotificationMailer(repo, env, async () => { throw new Error("private-proxy-password"); });
  await assert.rejects(mailer.send(email), /^EmailDeliveryError: email_delivery_unknown$/);
  assert.deepEqual(repo.statuses, ["unknown"]);
});
test("provider rejection and missing receipt never masquerade as sent", async () => {
  for (const status of [201, 302, 401, 407, 429, 500]) {
    const repo = ledger();
    await assert.rejects(new BrevoNotificationMailer(repo, env, async () => ({ status })).send(email));
    assert.ok(!repo.statuses.includes("sent"));
  }
});
test("proxy quota denial happens before any network request", async () => {
  const repo = ledger(); repo.claim = async () => { throw new Error("email_proxy_budget_exhausted"); };
  let calls = 0;
  await assert.rejects(new BrevoNotificationMailer(repo, env, async () => { calls++; return {status: 201, messageId:"x"}; }).send(email), /budget_exhausted/);
  assert.equal(calls, 0);
});
