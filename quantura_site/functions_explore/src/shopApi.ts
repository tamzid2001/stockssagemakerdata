import express, { Request, Response } from "express";
import admin from "firebase-admin";
import helmet from "helmet";
import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import Stripe from "stripe";
import { createQuanturaAuth, type QuanturaIdentity } from "./clerkAuth";
import { clerkSubscriptionAccess } from "./clerkBilling";
import { randomUUID } from "node:crypto";
import { hasApiAccess } from "./enterpriseAccess";
import documentation from "./apiDocumentation.json";
import { proSubscriptionPlan, proCheckoutOptions, subscriptionAccess, billingAccess, BILLING_ACCOUNTS } from "./subscriptionBilling";
import {
  SHOP_SHIPPING_POLICY,
  getCatalogBySku,
  getCatalogPublicBundles,
  getCatalogPublicItems,
  getCatalogSize,
  getCatalogVisibilityDefaults,
  getShippingPolicyForCheckout,
  resolveShippingCost,
} from "./shopCatalog";

if (!admin.apps.length) {
  const rawServiceAccount = String(process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "").trim();
  const storageBucket = String(process.env.FIREBASE_STORAGE_BUCKET || process.env.STORAGE_BUCKET || "").trim();
  if (rawServiceAccount) {
    const parsed = JSON.parse(rawServiceAccount) as Record<string, string>;
    admin.initializeApp({
      credential: admin.credential.cert({
        projectId: parsed.project_id || parsed.projectId,
        clientEmail: parsed.client_email || parsed.clientEmail,
        privateKey: parsed.private_key || parsed.privateKey,
      }),
      ...(storageBucket ? { storageBucket } : {}),
    });
  } else {
    admin.initializeApp(storageBucket ? { storageBucket } : undefined);
  }
}

const db = admin.firestore();

const PUBLIC_ORIGIN = asString(process.env.PUBLIC_ORIGIN, "https://quantura.studio").replace(/\/$/, "");
const GCP_PROJECT_ID = resolveProjectId();
const SHOP_ALLOWED_ORIGINS = parseOriginList(process.env.SHOP_ALLOWED_ORIGINS);
const SECRET_MANAGER_CLIENT = GCP_PROJECT_ID ? new SecretManagerServiceClient() : null;
const STRIPE_SECRET_ENV_KEYS = ["STRIPE_SECRET_KEY", "STRIPE_PRIVATE_KEY", "STRIPE_SECRET", "STRIPE_API_KEY"];
const STRIPE_WEBHOOK_ENV_KEYS = ["STRIPE_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET_CONNECT", "STRIPE_SIGNING_SECRET"];
const STRIPE_SECRET_NAMES = ["STRIPE_SECRET_KEY", "STRIPE_PRIVATE_KEY", "STRIPE_SECRET", "STRIPE_API_KEY"];
const STRIPE_WEBHOOK_SECRET_NAMES = ["STRIPE_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET_CONNECT", "STRIPE_SIGNING_SECRET"];
const STRIPE_API_VERSION: Stripe.LatestApiVersion = "2026-02-25.clover";
let stripeClientPromise: Promise<Stripe | null> | null = null;
let stripeWebhookSecretPromise: Promise<string> | null = null;

const CHECKOUT_RATE_WINDOW_MS = 10 * 60 * 1000;
const CHECKOUT_RATE_MAX = 25;

type RateRecord = {
  count: number;
  resetAtMs: number;
};

const checkoutRateLimiter = new Map<string, RateRecord>();
const SECRET_NAME_CACHE = new Map<string, string>();

async function billingIdentity(req:Request,res:Response) {
  try {
    const token=asString(req.headers.authorization).match(/^Bearer (.+)$/i)?.[1];
    if(!token)throw Error();
    const user=await createQuanturaAuth(admin.auth()).verifyIdToken(token,true) as QuanturaIdentity;
    if(user.firebase?.sign_in_provider==='anonymous')throw Error();
    return user;
  }catch{res.status(401).json({error:'sign_in_required',message:'Sign in to manage a Pro subscription.'});return null;}
}

async function syncProSubscription(subscription:Stripe.Subscription) {
  const access=subscriptionAccess(subscription);if(!access)return;
  const ref=db.collection(BILLING_ACCOUNTS).doc(access.uid);
  await db.runTransaction(async tx=>{
    const profile=db.collection("users").doc(access.uid);
    const [current,profileSnapshot]=await Promise.all([tx.get(ref),tx.get(profile)]),data=current.data()||{};
    if(access.plan==='free' && data.stripeSubscriptionId && data.stripeSubscriptionId!==access.subscriptionId)return;
    if(data.stripeSubscriptionId!==access.subscriptionId && Number(data.stripeSubscriptionCreatedAt)>access.createdAt)return;
    const update={plan:access.plan,subscriptionTier:access.plan,stripeCustomerId:access.customerId,stripeSubscriptionId:access.subscriptionId,
      subscriptionStatus:access.status,cancelAtPeriodEnd:access.cancelAtPeriodEnd,stripeSubscriptionCreatedAt:access.createdAt,
      pendingCheckout:null,trialEnd:access.trialEnd,hasUsedTrial:access.hasUsedTrial || data.hasUsedTrial===true};
    const changed=(existing:Record<string,any>)=>Object.entries(update).some(([key,value])=>JSON.stringify(existing[key])!==JSON.stringify(value));
    if(changed(data))tx.set(ref,{...update,updatedAt:admin.firestore.FieldValue.serverTimestamp()},{merge:true});
    if(changed(profileSnapshot.data()||{}))tx.set(profile,{...update,updatedAt:admin.firestore.FieldValue.serverTimestamp()},{merge:true});
  });
}

const app = express();
app.use(helmet());
app.disable("x-powered-by");

app.post("/api/shop/webhook/stripe", express.raw({ type: "application/json" }), async (req, res) => {
  const stripe = await getStripeClient();
  const stripeWebhookSecret = await getStripeWebhookSecret();

  if (!stripe || !stripeWebhookSecret) {
    res.status(503).json({ error: "stripe_webhook_not_configured" });
    return;
  }

  const signature = asString(req.headers["stripe-signature"]);
  if (!signature) {
    res.status(400).json({ error: "missing_stripe_signature" });
    return;
  }

  let event: Stripe.Event;
  try {
    const bodyBuffer = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body || "", "utf8");
    event = stripe.webhooks.constructEvent(bodyBuffer, signature, stripeWebhookSecret);
  } catch (error: any) {
    console.error("[shopApi] Stripe webhook signature verification failed", error?.message || error);
    res.status(400).json({ error: "invalid_signature" });
    return;
  }

  try {
    if (event.type === "checkout.session.completed") {
      const session = event.data.object as Stripe.Checkout.Session;
      await persistCheckoutCompletedOrder(session, stripe);
      if(session.mode==="subscription" && typeof session.subscription==="string")await syncProSubscription(await stripe.subscriptions.retrieve(session.subscription));
    } else if (["customer.subscription.updated","customer.subscription.deleted","customer.subscription.created"].includes(event.type)) {
      const subscription=event.data.object as Stripe.Subscription;
      // Read current provider state rather than granting from an old delivery.
      await syncProSubscription(event.type==="customer.subscription.deleted"?subscription:await stripe.subscriptions.retrieve(subscription.id));
    }
    res.status(200).json({ received: true });
  } catch (error: any) {
    console.error("[shopApi] Stripe webhook processing failed", error);
    res.status(500).json({ error: "webhook_processing_failed" });
  }
});

app.use(express.json({ limit: "512kb" }));

app.get("/api/health", (_req, res) => {
  res.status(200).json({ ok: true, service: "shopApi", catalogSize: getCatalogSize() });
});

app.get("/api/shop/catalog", (_req, res) => {
  res.status(200).json({
    currency: "usd",
    products: getCatalogPublicItems(),
    bundles: getCatalogPublicBundles(),
    visibilityConfig: getCatalogVisibilityDefaults(),
    shippingPolicy: {
      pod: {
        flatRateCents: SHOP_SHIPPING_POLICY.pod.flatRateCents,
        freeOverCents: SHOP_SHIPPING_POLICY.pod.freeOverCents,
        estimate: SHOP_SHIPPING_POLICY.pod.estimate,
        detail: SHOP_SHIPPING_POLICY.pod.detail,
      },
      hardware: {
        flatRateCents: SHOP_SHIPPING_POLICY.hardware.flatRateCents,
        freeOverCents: SHOP_SHIPPING_POLICY.hardware.freeOverCents,
        estimate: SHOP_SHIPPING_POLICY.hardware.estimate,
        detail: SHOP_SHIPPING_POLICY.hardware.detail,
      },
      returns: {
        pod: "Physical products: reprint or refund for defects, transit issues, or damaged deliveries. Contact support for resolution.",
        hardware: "Physical products: return support depends on product condition and fulfillment stage. Contact support before shipping items back.",
      },
    },
  });
});

app.options("/api/shop/checkout", (req, res) => {
  if (!applyCheckoutCors(req, res)) {
    res.status(403).json({ error: "origin_not_allowed" });
    return;
  }
  res.status(204).send("");
});

app.post("/api/shop/checkout", async (req, res) => {
  if (!applyCheckoutCors(req, res)) {
    res.status(403).json({ error: "origin_not_allowed" });
    return;
  }

  const stripe = await getStripeClient();
  if (!stripe) {
    res.status(503).json({
      error: "stripe_not_configured",
      message: "Stripe secret key is missing. Configure STRIPE_SECRET_KEY or STRIPE_PRIVATE_KEY in Secret Manager.",
    });
    return;
  }

  const ip = getRequestIp(req);
  if (isRateLimited(ip)) {
    res.status(429).json({ error: "rate_limited", message: "Too many checkout attempts. Please retry shortly." });
    return;
  }

  const payload = asRecord(req.body);
  const itemsInput = Array.isArray(payload.items) ? payload.items : [];
  const user = asRecord(payload.user);

  const email = normalizeEmail(user.email || payload.email);
  const uid = sanitizeToken(user.uid, 160);

  const normalizedItems = normalizeCheckoutItems(itemsInput);
  if (!normalizedItems.length) {
    res.status(400).json({ error: "invalid_items", message: "At least one valid SKU is required." });
    return;
  }

  const subtotalCents = normalizedItems.reduce((sum, row) => sum + row.item.priceCents * row.qty, 0);
  const hasHardwareOrPrivacy = normalizedItems.some((row) => row.item.shippingClass === "hardware");
  const shippingCostCents = resolveShippingCost(subtotalCents, hasHardwareOrPrivacy);
  const shippingPolicy = getShippingPolicyForCheckout(hasHardwareOrPrivacy);
  const shippingLabel =
    shippingCostCents === 0
      ? `${shippingPolicy.label} (Free over ${formatDollars(shippingPolicy.freeOverCents)})`
      : `${shippingPolicy.label} (${formatDollars(shippingCostCents)})`;

  const lineItems: Stripe.Checkout.SessionCreateParams.LineItem[] = normalizedItems.map(({ item, qty, sku }) => ({
    quantity: qty,
    price_data: {
      currency: "usd",
      unit_amount: item.priceCents,
      product_data: {
        name: item.name,
        description: item.description,
        images: [item.imageUrl],
        metadata: {
          sku,
          shippingClass: item.shippingClass,
        },
      },
    },
  }));

  try {
    const session = await stripe.checkout.sessions.create({
      mode: "payment",
      line_items: lineItems,
      shipping_address_collection: {
        allowed_countries: ["US", "CA"],
      },
      phone_number_collection: {
        enabled: true,
      },
      shipping_options: [
        {
          shipping_rate_data: {
            type: "fixed_amount",
            fixed_amount: {
              amount: shippingCostCents,
              currency: "usd",
            },
            display_name: shippingLabel,
            delivery_estimate: {
              minimum: {
                unit: "business_day",
                value: shippingPolicy.deliveryEstimate.minBusinessDays,
              },
              maximum: {
                unit: "business_day",
                value: shippingPolicy.deliveryEstimate.maxBusinessDays,
              },
            },
          },
        },
      ],
      allow_promotion_codes: true,
      customer_email: email || undefined,
      customer_creation: "always",
      client_reference_id: uid || undefined,
      success_url: `${PUBLIC_ORIGIN}/shop/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${PUBLIC_ORIGIN}/shop?canceled=1`,
      metadata: {
        uid,
        email,
        source: "quantura_shop",
        cartSkus: normalizedItems.map((row) => row.sku).join(",").slice(0, 450),
        cartQtys: normalizedItems.map((row) => row.qty).join(",").slice(0, 120),
        shippingClass: hasHardwareOrPrivacy ? "hardware" : "pod",
      },
    });

    const url = asString(session.url);
    if (!url) {
      res.status(502).json({ error: "missing_checkout_url" });
      return;
    }

    res.status(200).json({ url });
  } catch (error: any) {
    console.error("[shopApi] checkout session creation failed", error);
    res.status(500).json({
      error: "checkout_session_failed",
      detail: sanitizeText(error?.message, 180),
    });
  }
});

app.options("/api/shop/subscription-checkout", (req, res) => {
  if (!applyCheckoutCors(req, res)) {
    res.status(403).json({ error: "origin_not_allowed" });
    return;
  }
  res.status(204).send("");
});

app.get("/api/shop/subscription-access",async (req,res)=>{
  if(!applyCheckoutCors(req,res)){res.status(403).json({error:"origin_not_allowed"});return;}
  res.set("Cache-Control","private, no-store");
  const principal=await billingIdentity(req,res);if(!principal)return;
  const value=(await db.collection(BILLING_ACCOUNTS).doc(principal.uid).get()).data()||{};
  const legacy=billingAccess(value);
  const current=principal.clerk_user_id ? await clerkSubscriptionAccess(principal.clerk_user_id) : null;
  const access=legacy.docs_available?{...legacy,billing_provider:"stripe"}:current?{...current,can_trial:current.can_trial && legacy.can_trial}:legacy;
  const apiAccess=await hasApiAccess(db,principal.uid,principal.clerk_user_id);
  const admin=current?.subscription_status==="admin";
  const effective=admin?current!:access;
  res.json({data:{...effective,admin,pro_available:effective.docs_available,api_keys_available:apiAccess,docs_available:apiAccess}});
});

app.get("/api/shop/api-docs",async (req,res)=>{
  if(!applyCheckoutCors(req,res)){res.status(403).json({error:"origin_not_allowed"});return;}
  res.set("Cache-Control","private, no-store");
  const principal=await billingIdentity(req,res);if(!principal)return;
  if(!await hasApiAccess(db,principal.uid,principal.clerk_user_id)){res.status(403).json({error:"paid_api_required",message:"API documentation requires an active Pro subscription, Pro trial, or enterprise agreement."});return;}
  res.json({data:documentation});
});

app.post("/api/shop/subscription-checkout", async (req, res) => {
  if (!applyCheckoutCors(req, res)) {
    res.status(403).json({ error: "origin_not_allowed" });
    return;
  }

  const principal=await billingIdentity(req,res);if(!principal)return;

  const stripe = await getStripeClient();
  if (!stripe) {
    res.status(503).json({
      error: "stripe_not_configured",
      message: "Stripe secret key is missing. Configure STRIPE_SECRET_KEY (or alias) in Secret Manager.",
    });
    return;
  }

  const ip = getRequestIp(req);
  if (isRateLimited(ip)) {
    res.status(429).json({ error: "rate_limited", message: "Too many checkout attempts. Please retry shortly." });
    return;
  }

  const payload = asRecord(req.body);
  const email = normalizeEmail(principal.email);
  const uid = principal.uid;
  const plan = proSubscriptionPlan(payload);
  if (!plan) {
    res.status(400).json({
      error: "invalid_plan",
      message: "Choose Pro with monthly or yearly billing.",
    });
    return;
  }

  try {
    const clerkAccess=principal.clerk_user_id ? await clerkSubscriptionAccess(principal.clerk_user_id) : null;
    if(clerkAccess?.docs_available)throw new Error("subscription_already_active");
    if(payload.trial===true && clerkAccess && !clerkAccess.can_trial)throw new Error("trial_already_used");
    const trial=payload.trial===true,ref=db.collection(BILLING_ACCOUNTS).doc(uid),now=Date.now(),key=randomUUID();
    // Server-only ledger and a durable idempotency key serialize checkout across
    // instances. A retry cannot create a second trial, including across cycles.
    const reservation=await db.runTransaction(async tx=>{
      const existing=(await tx.get(ref)).data()||{};
      if(billingAccess(existing).docs_available)throw new Error("subscription_already_active");
      if(trial && existing.hasUsedTrial===true)throw new Error("trial_already_used");
      const pending=existing.pendingCheckout;
      if(pending && Number(pending.expiresAt)>now){
        if(pending.cycle!==plan.cycle || pending.trial!==trial)throw new Error("checkout_in_progress");
        return pending;
      }
      const pendingCheckout={key,cycle:plan.cycle,trial,expiresAt:now+23*60*60*1000};
      tx.set(ref,{pendingCheckout},{merge:true});return pendingCheckout;
    });
    const checkout=proCheckoutOptions(plan,uid,email,PUBLIC_ORIGIN,trial);
    checkout.expires_at=Math.floor(reservation.expiresAt/1000);
    const session = await stripe.checkout.sessions.create(checkout,{idempotencyKey:`quantura-pro-${uid}-${reservation.key}`});
    const url = asString(session.url).trim();
    if (!url) {
      res.status(502).json({ error: "missing_checkout_url" });
      return;
    }
    res.status(200).json({ url, sessionId: sanitizeToken(session.id, 220) });
  } catch (error: any) {
    const messages:Record<string,string>={subscription_already_active:"You already have Pro. Use Manage subscription to change your billing.",trial_already_used:"Your account has already used its free trial. Choose a paid Pro subscription.",checkout_in_progress:"A checkout is already open for your account. Finish that checkout or retry after it expires."};
    if(messages[error?.message]){res.status(409).json({error:error.message,message:messages[error.message]});return;}
    console.error("[shopApi] subscription checkout session creation failed", error);
    res.status(500).json({
      error: "subscription_checkout_failed",
      detail: sanitizeText(error?.message, 200),
      message: "Unable to start subscription checkout.",
    });
  }
});

app.options("/api/shop/portal", (req, res) => {
  if (!applyCheckoutCors(req, res)) {
    res.status(403).json({ error: "origin_not_allowed" });
    return;
  }
  res.status(204).send("");
});

app.post("/api/shop/portal", async (req, res) => {
  if (!applyCheckoutCors(req, res)) {
    res.status(403).json({ error: "origin_not_allowed" });
    return;
  }

  const stripe = await getStripeClient();
  if (!stripe) {
    res.status(503).json({
      error: "stripe_not_configured",
      message: "Stripe secret key is missing. Configure STRIPE_SECRET_KEY or STRIPE_PRIVATE_KEY in Secret Manager.",
    });
    return;
  }

  const payload = asRecord(req.body);
  const principal=await billingIdentity(req,res);if(!principal)return;
  const returnUrl = normalizeReturnUrl(payload.returnUrl);
  const profile=await db.collection(BILLING_ACCOUNTS).doc(principal.uid).get();
  let customerId = sanitizeToken(profile.data()?.stripeCustomerId,180);
  try {
    if(!customerId) {
      const orders=await db.collection('orders').where('userId','==',principal.uid).limit(100).get();
      customerId=sanitizeToken(orders.docs.find(doc=>doc.data().stripeCustomerId)?.data().stripeCustomerId,180);
    }
    if (!customerId) {
      res.status(404).json({ error: "customer_not_found" });
      return;
    }

    const session = await stripe.billingPortal.sessions.create({
      customer: customerId,
      return_url: returnUrl,
    });

    const portalUrl = asString(session.url);
    if (!portalUrl) {
      res.status(502).json({ error: "billing_portal_url_missing" });
      return;
    }

    res.status(200).json({ url: portalUrl });
  } catch (error: any) {
    console.error("[shopApi] billing portal session failed", error);
    res.status(500).json({
      error: "billing_portal_failed",
      detail: sanitizeText(error?.message, 180),
    });
  }
});

app.get("/api/shop/order/:sessionId", async (req, res) => {
  const sessionId = sanitizeToken(req.params.sessionId, 220);
  if (!sessionId) {
    res.status(400).json({ error: "invalid_session_id" });
    return;
  }

  try {
    const snap = await db.collection("orders").doc(sessionId).get();
    if (!snap.exists) {
      res.status(404).json({ error: "order_not_found" });
      return;
    }

    const data = (snap.data() || {}) as Record<string, unknown>;
    res.status(200).json({
      order: {
        sessionId,
        status: sanitizeText(data.status, 40),
        paymentStatus: sanitizeText(data.paymentStatus, 40),
        amountTotal: asFinite(data.amountTotal, 0),
        amountSubtotal: asFinite(data.amountSubtotal, 0),
        currency: sanitizeText(data.currency, 10) || "usd",
        customerEmail: sanitizeText(data.customerEmail, 320),
        createdAt: timestampToIso(data.createdAt),
        shipping: asRecord(data.shipping),
        items: Array.isArray(data.items) ? data.items : [],
      },
    });
  } catch (error) {
    console.error("[shopApi] order fetch failed", error);
    res.status(500).json({ error: "order_lookup_failed" });
  }
});

app.use((error: any, _req: Request, res: Response, _next: unknown) => {
  console.error("[shopApi] unhandled error", error);
  res.status(500).json({ error: "internal_error" });
});

export const shopApi = app;

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asFinite(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function sanitizeText(value: unknown, maxLen = 500): string {
  return asString(value)
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLen);
}

function sanitizeToken(value: unknown, maxLen = 220): string {
  const clean = sanitizeText(value, maxLen);
  return clean.replace(/[^A-Za-z0-9_.,:@\-]/g, "").slice(0, maxLen);
}

function normalizeEmail(value: unknown): string {
  const email = sanitizeText(value, 320).toLowerCase();
  if (!email) return "";
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return "";
  return email;
}

function formatDollars(cents: number): string {
  const safe = Math.max(0, Math.floor(asFinite(cents, 0)));
  return `$${(safe / 100).toFixed(2)}`;
}

function getRequestIp(req: Request): string {
  const forwarded = asString(req.headers["x-forwarded-for"]);
  if (forwarded) {
    const first = forwarded.split(",")[0]?.trim() || "";
    if (first) return first.slice(0, 120);
  }
  const real = asString(req.headers["x-real-ip"]);
  if (real) return real.slice(0, 120);
  return asString((req.socket as any)?.remoteAddress).slice(0, 120);
}

function isRateLimited(ipAddress: string): boolean {
  const now = Date.now();
  const key = sanitizeToken(ipAddress || "unknown", 120) || "unknown";

  if (checkoutRateLimiter.size > 2000) {
    for (const [mapKey, record] of checkoutRateLimiter.entries()) {
      if (record.resetAtMs <= now) checkoutRateLimiter.delete(mapKey);
    }
  }

  const current = checkoutRateLimiter.get(key);
  if (!current || current.resetAtMs <= now) {
    checkoutRateLimiter.set(key, {
      count: 1,
      resetAtMs: now + CHECKOUT_RATE_WINDOW_MS,
    });
    return false;
  }

  current.count += 1;
  checkoutRateLimiter.set(key, current);
  return current.count > CHECKOUT_RATE_MAX;
}

function normalizeCheckoutItems(itemsInput: unknown[]): Array<{ sku: string; qty: number; item: NonNullable<ReturnType<typeof getCatalogBySku>> }> {
  const itemMap = new Map<string, { sku: string; qty: number; item: NonNullable<ReturnType<typeof getCatalogBySku>> }>();

  itemsInput.slice(0, 40).forEach((entryRaw) => {
    const entry = asRecord(entryRaw);
    const sku = sanitizeToken(entry.sku, 64).toUpperCase();
    const item = getCatalogBySku(sku);
    if (!item) return;

    const qtyRaw = Math.floor(asFinite(entry.qty, 1));
    const qty = Math.max(1, Math.min(10, qtyRaw));

    const existing = itemMap.get(item.sku);
    if (existing) {
      existing.qty = Math.max(1, Math.min(10, existing.qty + qty));
      itemMap.set(item.sku, existing);
      return;
    }

    itemMap.set(item.sku, {
      sku: item.sku,
      qty,
      item,
    });
  });

  return Array.from(itemMap.values());
}

function normalizeReturnUrl(value: unknown): string {
  const fallback = `${PUBLIC_ORIGIN}/shop`;
  const raw = sanitizeText(value, 1000);
  if (!raw) return fallback;
  try {
    const parsed = new URL(raw);
    const origin = parsed.origin.replace(/\/$/, "");
    if (!isAllowedOrigin(origin.toLowerCase(), null)) return fallback;
    return parsed.toString();
  } catch {
    return fallback;
  }
}

function timestampToIso(value: unknown): string | null {
  if (value instanceof admin.firestore.Timestamp) return value.toDate().toISOString();
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "number" && Number.isFinite(value)) return new Date(value).toISOString();
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return new Date(parsed).toISOString();
  }
  return null;
}

function applyCheckoutCors(req: Request, res: Response): boolean {
  const origin = asString(req.headers.origin).trim();
  if (!origin) return true;

  const normalizedOrigin = normalizeOrigin(origin);
  const requestHosts = extractRequestHosts(req);
  if (!isAllowedOrigin(normalizedOrigin, requestHosts)) {
    return false;
  }

  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");
  return true;
}

async function persistCheckoutCompletedOrder(session: Stripe.Checkout.Session, stripe: Stripe): Promise<void> {
  const sessionId = sanitizeToken(session.id, 220);
  if (!sessionId) return;
  const rawSession = session as unknown as Record<string, unknown>;

  const metadata = asRecord(session.metadata);
  const metaSkus = sanitizeText(metadata.cartSkus, 450)
    .split(",")
    .map((part) => sanitizeToken(part, 64).toUpperCase())
    .filter(Boolean);

  const lineItemsResp = await stripe.checkout.sessions.listLineItems(sessionId, { limit: 100 });
  const orderItems = lineItemsResp.data.map((line, index) => {
    const qty = Math.max(1, Math.min(10, Math.floor(asFinite(line.quantity, 1))));
    const amount = asFinite(line.amount_subtotal, asFinite(line.amount_total, 0));
    const unitAmount = qty > 0 ? Math.round(amount / qty) : 0;
    const title = sanitizeText(line.description, 180) || "Item";
    const fallbackSku = metaSkus[index] || "";
    const skuByName = findSkuByProductName(title);
    const sku = fallbackSku || skuByName || "";

    return {
      sku,
      name: title,
      unitAmount,
      qty,
      amountSubtotal: amount,
      currency: sanitizeText(line.currency, 10) || "usd",
    };
  });

  const customerDetails = asRecord(session.customer_details);
  const shippingDetails = asRecord(rawSession.shipping_details);

  const shippingAddress = asRecord(customerDetails.address);
  const shippingAddressAlt = asRecord(shippingDetails.address);
  const selectedAddress = Object.keys(shippingAddress).length ? shippingAddress : shippingAddressAlt;

  const payload = {
    sessionId,
    paymentStatus: sanitizeText(session.payment_status, 40),
    amountTotal: asFinite(session.amount_total, 0),
    amountSubtotal: asFinite(session.amount_subtotal, 0),
    currency: sanitizeText(session.currency, 10) || "usd",
    customerEmail: normalizeEmail(customerDetails.email || session.customer_email),
    customerId: sanitizeToken(session.customer as string, 180),
    uid: sanitizeToken(metadata.uid, 180),
    userId: sanitizeToken(metadata.uid, 180),
    userEmail: normalizeEmail(customerDetails.email || session.customer_email),
    subscriptionTier: metadata.source==="quantura_pricing"?"pro":null,
    product: metadata.source==="quantura_pricing"?"Quantura Pro":"Shop purchase",
    price: asFinite(session.amount_total,0)/100,
    paymentProvider:"stripe",
    stripeCustomerId: sanitizeToken(session.customer as string,180),
    stripeSubscriptionId: typeof session.subscription==="string"?session.subscription:null,
    source: sanitizeText(metadata.source, 120) || "quantura_shop",
    shipping: {
      name: sanitizeText(customerDetails.name || shippingDetails.name, 200),
      phone: sanitizeText(customerDetails.phone || shippingDetails.phone, 60),
      address: {
        line1: sanitizeText(selectedAddress.line1, 200),
        line2: sanitizeText(selectedAddress.line2, 200),
        city: sanitizeText(selectedAddress.city, 120),
        state: sanitizeText(selectedAddress.state, 120),
        postalCode: sanitizeText(selectedAddress.postal_code, 40),
        country: sanitizeText(selectedAddress.country, 8),
      },
      shippingCost: asFinite(asRecord(session.shipping_cost).amount_total, 0),
      shippingRate: sanitizeToken(asRecord(session.shipping_cost).shipping_rate, 180),
    },
    items: orderItems,
    stripeCheckoutSessionId: sessionId,
    stripePaymentStatus: sanitizeText(session.payment_status, 40),
    status: sanitizeText(session.payment_status, 40).toLowerCase() === "paid" ? "paid" : "completed",
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  await db.collection("orders").doc(sessionId).set(payload, { merge: true });
}

function resolveProjectId(): string {
  return asString(process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || process.env.PROJECT_ID).trim();
}

function parseOriginList(raw: unknown): Set<string> {
  const values = asString(raw)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => normalizeOrigin(value));
  return new Set(values);
}

function readEnvSecret(keys: string[]): string {
  for (const key of keys) {
    const value = asString(process.env[key]).trim();
    if (value) return value;
  }
  return "";
}

async function readSecretManager(secretName: string): Promise<string> {
  if (!SECRET_MANAGER_CLIENT || !GCP_PROJECT_ID || !secretName) return "";
  try {
    const resource = `projects/${GCP_PROJECT_ID}/secrets/${secretName}/versions/latest`;
    const [version] = await SECRET_MANAGER_CLIENT.accessSecretVersion({ name: resource });
    const rawBytes = version.payload?.data;
    const raw = rawBytes ? Buffer.from(rawBytes).toString("utf8") : "";
    return raw.trim();
  } catch (error: any) {
    const message = asString(error?.message).toLowerCase();
    if (message.includes("not found") || message.includes("permission")) {
      return "";
    }
    console.warn(`[shopApi] secret lookup failed for ${secretName}:`, error?.message || error);
    return "";
  }
}

async function resolveSecretValue(envKeys: string[], secretNames: string[]): Promise<string> {
  const fromEnv = readEnvSecret(envKeys);
  if (fromEnv) return fromEnv;

  for (const secretName of secretNames) {
    const fromManager = await readSecretManager(secretName);
    if (fromManager) return fromManager;
  }

  const patterns = envKeys.some((key) => key.startsWith("STRIPE_WEBHOOK"))
    ? [/^stripe[-_]?webhook/i, /^stripe[-_]?.*sign/i]
    : [/^stripe[-_]?.*(secret|private|api)/i, /^stripe[-_]?sk/i];
  for (const pattern of patterns) {
    const discovered = await discoverSecretValueByPattern(pattern);
    if (discovered) return discovered;
  }
  return "";
}

async function getStripeClient(): Promise<Stripe | null> {
  if (stripeClientPromise) return stripeClientPromise;
  stripeClientPromise = (async () => {
    const secret = await resolveSecretValue(STRIPE_SECRET_ENV_KEYS, STRIPE_SECRET_NAMES);
    if (!secret) return null;
    return new Stripe(secret, {
      apiVersion: STRIPE_API_VERSION,
    });
  })();
  return stripeClientPromise;
}

async function getStripeWebhookSecret(): Promise<string> {
  if (stripeWebhookSecretPromise) return stripeWebhookSecretPromise;
  stripeWebhookSecretPromise = resolveSecretValue(STRIPE_WEBHOOK_ENV_KEYS, STRIPE_WEBHOOK_SECRET_NAMES);
  return stripeWebhookSecretPromise;
}

function normalizeOrigin(origin: string): string {
  const trimmed = String(origin || "").trim();
  if (!trimmed) return "";
  if (trimmed === "null") return "null";
  try {
    const parsed = new URL(trimmed);
    return parsed.origin.replace(/\/$/, "").toLowerCase();
  } catch {
    return trimmed.replace(/\/$/, "").toLowerCase();
  }
}

function extractRequestHosts(req: Request): Set<string> {
  const hosts = new Set<string>();
  const addHost = (raw: unknown) => {
    const value = asString(raw).trim();
    if (!value) return;
    value
      .split(",")
      .map((item) => item.trim().toLowerCase())
      .filter(Boolean)
      .forEach((item) => {
        const host = item.split("/")[0]?.split(":")[0]?.trim();
        if (host) hosts.add(host);
      });
  };
  addHost(req.headers["x-forwarded-host"]);
  addHost(req.headers.host);
  return hosts;
}

function isAllowedOrigin(normalizedOrigin: string, requestHosts: Set<string> | null): boolean {
  if (!normalizedOrigin) return false;
  if (normalizedOrigin === "null") return true;
  if (normalizedOrigin === "capacitor://localhost" || normalizedOrigin === "ionic://localhost") return true;

  const staticAllowlist = new Set<string>([
    normalizeOrigin(PUBLIC_ORIGIN),
    "https://quantura.studio",
    "https://www.quantura.studio",
    "https://quantura-e2e3d.web.app",
    "https://quantura-e2e3d.firebaseapp.com",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    ...SHOP_ALLOWED_ORIGINS,
  ]);
  if (staticAllowlist.has(normalizedOrigin)) return true;

  try {
    const parsed = new URL(normalizedOrigin);
    const host = parsed.host.toLowerCase();
    const hostname = parsed.hostname.toLowerCase();
    if (hostname.endsWith(".quantura.studio")) return true;
    if (hostname.endsWith(".web.app") || hostname.endsWith(".firebaseapp.com")) return true;
    if (hostname === "localhost" || hostname === "127.0.0.1") return true;
    if (requestHosts && requestHosts.has(host)) return true;
  } catch {
    return false;
  }

  return false;
}

function findSkuByProductName(name: string): string {
  const clean = sanitizeText(name, 220).toLowerCase();
  if (!clean) return "";
  const item = getCatalogPublicItems().find((row) => sanitizeText(row.name, 220).toLowerCase() === clean);
  return item ? sanitizeToken(item.sku, 64).toUpperCase() : "";
}

async function discoverSecretValueByPattern(pattern: RegExp): Promise<string> {
  if (!SECRET_MANAGER_CLIENT || !GCP_PROJECT_ID) return "";
  const cacheKey = pattern.source;
  const cachedName = SECRET_NAME_CACHE.get(cacheKey);
  if (cachedName) {
    return readSecretManager(cachedName);
  }

  try {
    const [secrets] = await SECRET_MANAGER_CLIENT.listSecrets({
      parent: `projects/${GCP_PROJECT_ID}`,
    });
    const names = (secrets || [])
      .map((secret) => asString(secret.name).split("/").pop() || "")
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    const matched = names.find((name) => pattern.test(name));
    if (!matched) return "";
    SECRET_NAME_CACHE.set(cacheKey, matched);
    return readSecretManager(matched);
  } catch (error: any) {
    console.warn("[shopApi] secret discovery failed:", error?.message || error);
    return "";
  }
}
