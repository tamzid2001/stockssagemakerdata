import express, { Request, Response } from "express";
import admin from "firebase-admin";
import Stripe from "stripe";
import {
  SHOP_SHIPPING_POLICY,
  getCatalogBySku,
  getCatalogPublicItems,
  getCatalogSize,
  getShippingPolicyForCheckout,
  resolveShippingCost,
} from "./shopCatalog";

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();

const PUBLIC_ORIGIN = asString(process.env.PUBLIC_ORIGIN, "https://quantura.studio").replace(/\/$/, "");

// Stripe secrets must be injected with env vars or Secret Manager bindings in gcloud deploy.
const STRIPE_SECRET_KEY = asString(process.env.STRIPE_SECRET_KEY).trim();
const STRIPE_WEBHOOK_SECRET = asString(process.env.STRIPE_WEBHOOK_SECRET).trim();

const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

const CHECKOUT_RATE_WINDOW_MS = 10 * 60 * 1000;
const CHECKOUT_RATE_MAX = 25;

type RateRecord = {
  count: number;
  resetAtMs: number;
};

const checkoutRateLimiter = new Map<string, RateRecord>();

const app = express();
app.disable("x-powered-by");

app.post("/api/shop/webhook/stripe", express.raw({ type: "application/json" }), async (req, res) => {
  if (!stripe || !STRIPE_WEBHOOK_SECRET) {
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
    event = stripe.webhooks.constructEvent(bodyBuffer, signature, STRIPE_WEBHOOK_SECRET);
  } catch (error: any) {
    console.error("[shopApi] Stripe webhook signature verification failed", error?.message || error);
    res.status(400).json({ error: "invalid_signature" });
    return;
  }

  try {
    if (event.type === "checkout.session.completed") {
      const session = event.data.object as Stripe.Checkout.Session;
      await persistCheckoutCompletedOrder(session);
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
        pod: "POD: reprint/refund for defects or lost shipments. Contact support for resolution.",
        hardware: "Hardware/privacy: 30-day returns (unopened preferred). Warranty varies by brand.",
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

  if (!stripe) {
    res.status(503).json({ error: "stripe_not_configured" });
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

  if (!stripe) {
    res.status(503).json({ error: "stripe_not_configured" });
    return;
  }

  const payload = asRecord(req.body);
  const inputCustomerId = sanitizeToken(payload.customerId, 120);
  const inputEmail = normalizeEmail(payload.email);
  const returnUrl = normalizeReturnUrl(payload.returnUrl);

  if (!inputCustomerId && !inputEmail) {
    res.status(400).json({ error: "customer_lookup_required", message: "Provide customerId or email." });
    return;
  }

  let customerId = inputCustomerId;
  try {
    if (!customerId && inputEmail) {
      const customers = await stripe.customers.list({
        email: inputEmail,
        limit: 1,
      });
      customerId = asString(customers.data?.[0]?.id);
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

app.use((_req, res) => {
  res.status(404).json({ error: "not_found" });
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
    const allowedOrigins = new Set<string>([PUBLIC_ORIGIN, "https://quantura.studio", "https://www.quantura.studio"]);
    if (!allowedOrigins.has(origin)) return fallback;
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

  const normalizedOrigin = origin.replace(/\/$/, "").toLowerCase();
  const allowedOrigins = new Set<string>([
    PUBLIC_ORIGIN.toLowerCase(),
    "https://quantura.studio",
    "https://www.quantura.studio",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
  ]);

  if (!allowedOrigins.has(normalizedOrigin)) {
    return false;
  }

  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");
  return true;
}

async function persistCheckoutCompletedOrder(session: Stripe.Checkout.Session): Promise<void> {
  if (!stripe) return;

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

function findSkuByProductName(name: string): string {
  const clean = sanitizeText(name, 220).toLowerCase();
  if (!clean) return "";
  const item = getCatalogPublicItems().find((row) => sanitizeText(row.name, 220).toLowerCase() === clean);
  return item ? sanitizeToken(item.sku, 64).toUpperCase() : "";
}
