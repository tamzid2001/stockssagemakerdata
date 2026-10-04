import crypto from "node:crypto";
import express, { type Request, type Router } from "express";
import admin from "firebase-admin";

export const TIKTOK_REDIRECT_URI = "https://quantura.studio/api/integrations/tiktok/callback";
export const TIKTOK_WEBHOOK_URL = "https://quantura.studio/api/integrations/tiktok/webhook";
const BASE = "/integrations/tiktok";
const COOKIE = "__Host-quantura-tiktok-state";
const hash = (value: string | Buffer) => crypto.createHash("sha256").update(value).digest("hex");
type Options = {
  db: FirebaseFirestore.Firestore;
  authenticate: (req: Request) => Promise<{ userId: string; platformAdmin?: boolean }>;
  clientKey?: string;
  clientSecret?: string;
  request?: typeof fetch;
};

export function tiktokRawBodyMiddleware() {
  return express.raw({ type: "application/json", limit: "256kb" });
}

export function verifyTikTokSignature(body: Buffer, header: string, secret: string, now = Date.now()): boolean {
  const parts = header.split(",").map(part => part.trim().split("="));
  const times = parts.filter(([key]) => key === "t");
  if (!secret || times.length !== 1 || !/^\d{1,12}$/.test(times[0][1] || "")) return false;
  const timestamp = times[0][1];
  if (Math.abs(now / 1000 - Number(timestamp)) > 300) return false;
  const expected = crypto.createHmac("sha256", secret).update(`${timestamp}.`).update(body).digest();
  return parts.filter(([key]) => key === "s").some(([, signature]) =>
    /^[a-f0-9]{64}$/i.test(signature || "") && crypto.timingSafeEqual(expected, Buffer.from(signature, "hex")));
}

export function encryptTikTokTokens(tokens: Record<string, unknown>, secret: string): string {
  const key = crypto.createHash("sha256").update(`quantura-tiktok-tokens-v1:${secret}`).digest();
  const nonce = crypto.randomBytes(12), cipher = crypto.createCipheriv("aes-256-gcm", key, nonce);
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(tokens), "utf8"), cipher.final()]);
  return ["v1",nonce.toString("base64url"),cipher.getAuthTag().toString("base64url"),ciphertext.toString("base64url")].join(".");
}

export function registerTikTokRoutes(router: Router, options: Options): void {
  const key = options.clientKey ?? process.env.TIKTOK_CLIENT_KEY ?? "";
  const secret = options.clientSecret ?? process.env.TIKTOK_CLIENT_SECRET ?? "";
  const configured = Boolean(key && secret), request = options.request || fetch;
  const noStore = (_req: Request, res: express.Response, next: express.NextFunction) => {res.set("Cache-Control","private, no-store");next();};
  router.use(BASE,noStore);
  router.get(`${BASE}/webhook`, (_req,res) => res.json({provider:"tiktok",configured,webhook_url:TIKTOK_WEBHOOK_URL}));
  router.get(`${BASE}/callback`, async (req,res) => {
    res.set("Referrer-Policy","no-referrer");
    if (!configured) {res.status(503).json({error:"tiktok_not_configured"});return;}
    const state = typeof req.query.state === "string" ? req.query.state : "";
    const cookie = (req.headers.cookie || "").split(";").map(part=>part.trim()).find(part=>part.startsWith(`${COOKIE}=`))?.slice(COOKIE.length+1) || "";
    const [identity, nonce] = state.split(".");
    if (!/^[a-f0-9]{64}$/.test(identity || "") || !/^[A-Za-z0-9_-]{43}$/.test(nonce || "") || state !== cookie) {res.status(400).json({error:"invalid_oauth_state"});return;}
    try {
      const ref = options.db.collection("tiktok_oauth_states").doc(identity);
      const owner = await options.db.runTransaction(async transaction => {
        const data = (await transaction.get(ref)).data();
        if (!data || data.state_hash !== hash(state) || Number(data.expires_at_ms) <= Date.now()) throw Error("invalid_oauth_state");
        transaction.delete(ref);return String(data.owner_user_id);
      });
      res.clearCookie(COOKIE,{secure:true,httpOnly:true,sameSite:"lax",path:"/"});
      if (req.query.error) {res.redirect(303,"/forecasting?panel=profile&tiktok=declined");return;}
      const code = typeof req.query.code === "string" ? req.query.code : "";
      if (!code || code.length > 4096) throw Error("invalid_oauth_code");
      const response = await request("https://open.tiktokapis.com/v2/oauth/token/", {
        method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},
        body:new URLSearchParams({client_key:key,client_secret:secret,code,grant_type:"authorization_code",redirect_uri:TIKTOK_REDIRECT_URI}),
        signal:AbortSignal.timeout(20000),
      });
      const token = await response.json() as Record<string, any>;
      if (!response.ok || token.error || !token.access_token || !token.refresh_token || !token.open_id || !Number.isFinite(Number(token.expires_in))) throw Error("tiktok_token_exchange_failed");
      await options.db.collection("tiktok_connections").doc(hash(String(token.open_id))).set({
        owner_user_id:owner,open_id:token.open_id,scope:token.scope || "",status:"connected",
        encrypted_tokens:encryptTikTokTokens({access_token:token.access_token,refresh_token:token.refresh_token},secret),
        access_expires_at_ms:Date.now()+Number(token.expires_in)*1000,
        refresh_expires_at_ms:Date.now()+Number(token.refresh_expires_in || 0)*1000,updated_at:new Date().toISOString(),
      });
      res.redirect(303,"/forecasting?panel=profile&tiktok=connected");
    } catch (error) {
      const code = (error as Error).message;
      res.status(code.startsWith("invalid_oauth")?400:502).json({error:code.startsWith("invalid_oauth")?code:"tiktok_connection_failed"});
    }
  });
  router.post(`${BASE}/connect`, async (req,res) => {
    try {
      const principal = await options.authenticate(req);
      if (!principal.platformAdmin) {res.status(403).json({error:"admin_required"});return;}
      if (!configured) {res.status(503).json({error:"tiktok_not_configured"});return;}
      const identity = hash(principal.userId), nonce = crypto.randomBytes(32).toString("base64url"), state = `${identity}.${nonce}`;
      await options.db.collection("tiktok_oauth_states").doc(identity).set({owner_user_id:principal.userId,state_hash:hash(state),expires_at_ms:Date.now()+600000});
      res.cookie(COOKIE,state,{httpOnly:true,secure:true,sameSite:"lax",path:"/",maxAge:600000});
      const url = new URL("https://www.tiktok.com/v2/auth/authorize/");
      url.search = new URLSearchParams({client_key:key,response_type:"code",scope:"user.info.basic",redirect_uri:TIKTOK_REDIRECT_URI,state}).toString();
      res.json({authorization_url:url.toString(),redirect_uri:TIKTOK_REDIRECT_URI});
    } catch {res.status(401).json({error:"unauthenticated"});}
  });
  router.post(`${BASE}/webhook`, async (req,res) => {
    if (!configured) {res.status(503).json({error:"tiktok_not_configured"});return;}
    if (!Buffer.isBuffer(req.body) || !verifyTikTokSignature(req.body,String(req.headers["tiktok-signature"] || ""),secret)) {res.status(401).json({error:"invalid_webhook_signature"});return;}
    let event: Record<string,any>;
    try {event=JSON.parse(req.body.toString("utf8"));} catch {res.status(400).json({error:"invalid_webhook_payload"});return;}
    if (event.client_key !== key || typeof event.event !== "string" || event.event.length > 120) {res.status(400).json({error:"invalid_webhook_payload"});return;}
    try {
      const ref = options.db.collection("tiktok_webhook_receipts").doc(hash(req.body));
      await options.db.runTransaction(async transaction => {
        if ((await transaction.get(ref)).exists) return;
        transaction.create(ref,{event:event.event,received_at:new Date().toISOString(),expires_at:admin.firestore.Timestamp.fromMillis(Date.now()+30*86400000)});
        if (event.event === "authorization.removed" && typeof event.user_openid === "string") {
          transaction.set(options.db.collection("tiktok_connections").doc(hash(event.user_openid)),{status:"revoked",encrypted_tokens:admin.firestore.FieldValue.delete(),updated_at:new Date().toISOString()},{merge:true});
        }
      });
      res.status(200).json({ok:true});
    } catch {res.status(503).json({error:"webhook_retry_required"});}
  });
}
