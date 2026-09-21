import { createHash } from "node:crypto";
import https from "node:https";
import { HttpsProxyAgent } from "https-proxy-agent";
import type { Firestore } from "firebase-admin/firestore";

export type NotificationEmail = { id: string; to: string; subject: string; text: string; html: string };
export class EmailDeliveryError extends Error {
  constructor(public readonly code: string) { super(code); this.name = "EmailDeliveryError"; }
}
export type BrevoReply = { status: number; messageId?: string; code?: string };
export type BrevoTransport = (payload: Record<string, unknown>) => Promise<BrevoReply>;

/** Validate the complete server-only delivery configuration without sending. */
export function isBrevoEmailConfigured(env: NodeJS.ProcessEnv = process.env): boolean {
  const sender = env.BREVO_SENDER_EMAIL?.trim() || "";
  if (env.NOTIFICATION_EMAIL_PROVIDER !== "brevo" || !/^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/.test(sender)) return false;
  try {
    createBrevoTransport(env);
    return true;
  } catch {
    return false;
  }
}

/** No global proxy: only this fixed HTTPS endpoint is allowed through Fixie. */
export function createBrevoTransport(env: NodeJS.ProcessEnv = process.env): BrevoTransport {
  const apiKey = env.BREVO_API_KEY?.trim();
  let proxy: URL;
  try { proxy = new URL(env.FIXIE_URL || ""); } catch { throw new EmailDeliveryError("email_configuration"); }
  if (!apiKey || !["http:", "https:"].includes(proxy.protocol) || !proxy.hostname.endsWith(".usefixie.com") ||
      !proxy.username || !proxy.password || proxy.search || proxy.hash || proxy.pathname !== "/") {
    throw new EmailDeliveryError("email_configuration");
  }
  const proxyAuthorization = `Basic ${Buffer.from(`${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`).toString("base64")}`;
  // The agent's optional debug output must never see the credential-bearing URL.
  proxy.username = "";
  proxy.password = "";
  const agent = new HttpsProxyAgent(proxy, { headers: () => ({ "Proxy-Authorization": proxyAuthorization }) });
  return (payload) => new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    if (Buffer.byteLength(body) > 64 * 1024) { reject(new EmailDeliveryError("email_payload_limit")); return; }
    const req = https.request("https://api.brevo.com/v3/smtp/email", {
      method: "POST", agent, rejectUnauthorized: true,
      headers: { "api-key": apiKey, "Content-Type": "application/json", Accept: "application/json", "Content-Length": Buffer.byteLength(body) },
    }, (res) => {
      const chunks: Buffer[] = [];
      let bytes = 0;
      res.on("data", (chunk: Buffer) => {
        bytes += chunk.length;
        if (bytes > 64 * 1024) req.destroy(new EmailDeliveryError("email_response_limit"));
        else chunks.push(chunk);
      });
      res.on("error", () => { clearTimeout(timer); reject(new EmailDeliveryError("email_delivery_unknown")); });
      res.on("end", () => {
        clearTimeout(timer);
        let result: Record<string, unknown> = {};
        try { result = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { /* sanitized below */ }
        resolve({ status: res.statusCode || 502,
          messageId: typeof result.messageId === "string" ? result.messageId.slice(0, 250) : undefined,
          code: result.code === "duplicate_parameter" ? "duplicate_parameter" : undefined });
      });
    });
    const timer = setTimeout(() => req.destroy(), 20_000);
    req.on("error", () => { clearTimeout(timer); reject(new EmailDeliveryError("email_delivery_unknown")); });
    req.end(body);
  });
}

export function emailIdempotencyKey(id: string): string {
  const hash = createHash("sha256").update(`quantura-email-v1:${id}`).digest("hex");
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-5${hash.slice(13, 16)}-a${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
}

export function fixieBillingWindow(now: Date, resetDay = 19): string {
  if (!Number.isInteger(resetDay) || resetDay < 1 || resetDay > 28) throw new EmailDeliveryError("email_configuration");
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), resetDay));
  if (now < start) start.setUTCMonth(start.getUTCMonth() - 1);
  return start.toISOString().slice(0, 10);
}

export interface EmailDeliveryLedger {
  claim(id: string, contentHash: string): Promise<{ messageId?: string }>;
  finish(id: string, state: "sent" | "rejected" | "unknown", messageId?: string): Promise<void>;
}

/** Cross-instance deduplication and an atomic proxy budget; unknown sends need reconciliation. */
export class FirestoreEmailDeliveryLedger implements EmailDeliveryLedger {
  constructor(private readonly db: Firestore, private readonly env: NodeJS.ProcessEnv = process.env) {}
  async claim(id: string, contentHash: string): Promise<{ messageId?: string }> {
    const now = new Date();
    const reset = Number(this.env.FIXIE_BILLING_RESET_DAY || 19);
    const limit = Number(this.env.FIXIE_EMAIL_REQUEST_BUDGET || 450);
    if (!Number.isInteger(limit) || limit < 1 || limit > 450) throw new EmailDeliveryError("email_configuration");
    const event = this.db.collection("notification_email_outbox").doc(id);
    const budget = this.db.collection("notification_email_usage").doc(fixieBillingWindow(now, reset));
    return this.db.runTransaction(async (tx) => {
      const [existing, usage] = await Promise.all([tx.get(event), tx.get(budget)]);
      const record = existing.data();
      if (record && record.contentHash !== contentHash) throw new EmailDeliveryError("email_idempotency_conflict");
      if (record?.status === "sent") return { messageId: String(record.messageId) };
      if (record) throw new EmailDeliveryError("email_delivery_requires_review");
      const attempts = Number(usage.data()?.attempts || 0);
      if (attempts >= limit) throw new EmailDeliveryError("email_proxy_budget_exhausted");
      tx.set(budget, { attempts: attempts + 1, updatedAt: now.toISOString(), requestBudget: limit }, { merge: true });
      tx.create(event, { contentHash, status: "sending", createdAt: now.toISOString() });
      return {};
    });
  }
  async finish(id: string, status: "sent" | "rejected" | "unknown", messageId?: string): Promise<void> {
    await this.db.collection("notification_email_outbox").doc(id).update({ status, messageId: messageId || null, updatedAt: new Date().toISOString() });
  }
}

export class BrevoNotificationMailer {
  constructor(private readonly ledger: EmailDeliveryLedger, private readonly env: NodeJS.ProcessEnv = process.env,
    private readonly transport?: BrevoTransport) {}
  async send(email: NotificationEmail): Promise<{ messageId: string }> {
    const from = this.env.BREVO_SENDER_EMAIL?.trim();
    if (!from || !/^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/.test(from)) throw new EmailDeliveryError("email_configuration");
    if (!/^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/.test(email.to) || /[\r\n]/.test(email.subject) || !email.id) throw new EmailDeliveryError("invalid_recipient");
    const id = emailIdempotencyKey(email.id);
    const payload = { sender: { email: from, name: "Quantura" }, to: [{ email: email.to }], subject: email.subject,
      textContent: email.text, htmlContent: email.html, headers: { "Idempotency-Key": id }, tags: ["quantura_notification"] };
    if (Buffer.byteLength(JSON.stringify(payload)) > 64 * 1024) throw new EmailDeliveryError("email_payload_limit");
    const transport = this.transport || createBrevoTransport(this.env);
    const cached = await this.ledger.claim(id, createHash("sha256").update(JSON.stringify(payload)).digest("hex"));
    if (cached.messageId) return { messageId: cached.messageId };
    let reply: BrevoReply;
    try { reply = await transport(payload); }
    catch { await this.ledger.finish(id, "unknown"); throw new EmailDeliveryError("email_delivery_unknown"); }
    if (reply.status !== 201 || !reply.messageId) {
      // Never invent a message ID, retry uncertain delivery, or fall back to a rotating IP.
      await this.ledger.finish(id, reply.status >= 500 || reply.status === 201 ? "unknown" : "rejected");
      throw new EmailDeliveryError(reply.status === 429 ? "email_rate_limit" : "email_provider");
    }
    await this.ledger.finish(id, "sent", reply.messageId);
    return { messageId: reply.messageId };
  }
}
