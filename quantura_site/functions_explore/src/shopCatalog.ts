export type ShopTab = "merch" | "desk_setup" | "privacy";
export type ShippingClass = "pod" | "hardware";

export type ShopCatalogItem = {
  sku: string;
  name: string;
  description: string;
  priceCents: number;
  currency: "usd";
  ratingValue: number;
  ratingCount: number;
  ships: string;
  imageUrl: string;
  compareAtCents?: number;
  tab: ShopTab;
  shippingClass: ShippingClass;
};

export const SHOP_PLACEHOLDER_IMAGE = "/assets/shop/placeholder.png";

export const SHOP_SHIPPING_POLICY = {
  pod: {
    label: "POD shipping",
    flatRateCents: 695,
    freeOverCents: 7500,
    estimate: "Estimated delivery 5-12 business days total.",
    detail: "Production 2-5 business days + shipping 3-7 business days.",
  },
  hardware: {
    label: "Hardware and privacy shipping",
    flatRateCents: 1295,
    freeOverCents: 25000,
    estimate: "Estimated delivery 5-10 business days total.",
    detail: "Warehouse handling 2-5 business days + transit 3-7 business days.",
  },
} as const;

const CATALOG: ShopCatalogItem[] = [
  {
    sku: "QNT-POD-DESKMAT-01",
    name: "Quantura Terminal Desk Mat",
    description: "Premium desk mat sized for multi-monitor setups. Smooth glide, anti-slip base.",
    priceCents: 3900,
    currency: "usd",
    ratingValue: 4.8,
    ratingCount: 124,
    ships: "Production 2-5 biz days + Shipping 3-7 biz days",
    imageUrl: "https://images-api.printify.com/mockup/65bb824a65ed94493f0fc0ec/975/92341/desk-mats.jpg?camera_label=front",
    tab: "merch",
    shippingClass: "pod",
  },
  {
    sku: "QNT-POD-JOURNAL-01",
    name: "Quantura Research Journal (Hardcover)",
    description: "Matte hardcover journal for weekly briefings, thesis tracking, and trade reviews.",
    priceCents: 2400,
    currency: "usd",
    ratingValue: 4.7,
    ratingCount: 88,
    ships: "Production 2-5 biz days + Shipping 3-7 biz days",
    imageUrl: "https://images-api.printify.com/mockup/644151ae31e780d3f60499b1/560/67028/hardcover-journals-matte.jpg?camera_label=front",
    tab: "merch",
    shippingClass: "pod",
  },
  {
    sku: "QNT-POD-NOTEBOOK-01",
    name: "Quantura Forecast Notebook (Spiral)",
    description: "Ruled spiral notebook for scenario maps, levels, and catalyst logs.",
    priceCents: 1800,
    currency: "usd",
    ratingValue: 4.6,
    ratingCount: 141,
    ships: "Production 2-5 biz days + Shipping 3-7 biz days",
    imageUrl: "https://images-api.printify.com/mockup/64c124c8e3fffd6b940aa6d1/74/62076/spiral-notebooks.jpg?camera_label=front",
    tab: "merch",
    shippingClass: "pod",
  },
  {
    sku: "QNT-POD-POSTER-ROLLED-01",
    name: "Quantura Scenario Map Poster (Rolled)",
    description: "Rolled poster for office walls-print your favorite scenario map layout.",
    priceCents: 2900,
    currency: "usd",
    ratingValue: 4.5,
    ratingCount: 57,
    ships: "Production 2-5 biz days + Shipping 3-7 biz days",
    imageUrl: "https://images-api.printify.com/mockup/65a6852767c0a461f90ea2c4/1220/88599/rolled-posters.jpg?camera_label=front",
    tab: "merch",
    shippingClass: "pod",
  },
  {
    sku: "QNT-POD-POSTER-FINEART-01",
    name: "Quantura Macro Wall Print (Fine Art)",
    description: "Fine art poster for flagship charts and macro dashboards.",
    priceCents: 4900,
    currency: "usd",
    ratingValue: 4.9,
    ratingCount: 33,
    ships: "Production 2-5 biz days + Shipping 3-7 biz days",
    imageUrl: "https://images-api.printify.com/mockup/6597f13bd5ee52ec410178b7/804/88607/fine-art-posters.jpg?camera_label=front",
    tab: "merch",
    shippingClass: "pod",
  },
  {
    sku: "QNT-POD-CANVAS-01",
    name: "Quantura Terminal Canvas (Stretched)",
    description: "1.25\" stretched canvas for your main office or desk backdrop.",
    priceCents: 10900,
    currency: "usd",
    ratingValue: 4.8,
    ratingCount: 19,
    ships: "Production 2-5 biz days + Shipping 3-10 biz days",
    imageUrl: "https://images-api.printify.com/mockup/6597e8c57fb44cea3c0613e6/546/90714/matte-canvas-stretched-125.jpg?camera_label=front",
    tab: "merch",
    shippingClass: "pod",
  },
  {
    sku: "QNT-DSK-DOCK-MP-29434",
    name: "USB-C Dual Monitor Docking Station (100W PD)",
    description: "Clean one-cable desk setup. Dual display outputs + 100W power delivery.",
    priceCents: 21900,
    currency: "usd",
    ratingValue: 4.6,
    ratingCount: 210,
    ships: "2-5 biz days (warehouse) + 3-7 transit (estimate)",
    imageUrl: "https://www.zoro.com/static/cms/product/large/8a102d13-0f00-4584-acf2-77c1c5708138.jpeg",
    tab: "desk_setup",
    shippingClass: "hardware",
  },
  {
    sku: "QNT-DSK-MOUNT-MP-36083",
    name: "Single Monitor Desk Mount (0-18.7\" Height)",
    description: "Dial-in your perfect monitor height for longer research sessions.",
    priceCents: 10900,
    currency: "usd",
    ratingValue: 4.7,
    ratingCount: 98,
    ships: "2-5 biz days + 3-7 transit",
    imageUrl: "https://www.zoro.com/static/cms/product/large/b5b2d2d6-2b24-4bd4-b4e9-bd46c2c99adb.jpeg",
    tab: "desk_setup",
    shippingClass: "hardware",
  },
  {
    sku: "QNT-DSK-WALLMOUNT-MP-36082",
    name: "Single Monitor Wall Mount (23\"-43\")",
    description: "Wall-mount your display for a clean terminal-style workspace.",
    priceCents: 8900,
    currency: "usd",
    ratingValue: 4.5,
    ratingCount: 64,
    ships: "2-5 biz days + 3-7 transit",
    imageUrl: "https://www.zoro.com/static/cms/product/large/8941f76a-4b14-489b-be1b-9ba3175b046e.jpeg",
    tab: "desk_setup",
    shippingClass: "hardware",
  },
  {
    sku: "QNT-SEC-PRIVACY-46174",
    name: "Magnetic Privacy Screen Filter (24\" 16:10)",
    description: "Reduce shoulder-surfing risk in cafes, coworking, and travel.",
    priceCents: 10900,
    currency: "usd",
    ratingValue: 4.4,
    ratingCount: 52,
    ships: "2-5 biz days + 3-7 transit",
    imageUrl: "https://www.zoro.com/static/cms/product/petit/fe4e36ce-cdeb-463e-b0bb-fb68710f5778.jpeg",
    tab: "privacy",
    shippingClass: "hardware",
  },
  {
    sku: "QNT-DSK-DOCK-ST-DK31C2DHSPD",
    name: "StarTech USB-C Dock (Dual HDMI/DP)",
    description: "Standardize premium desk setups for power users and business accounts.",
    priceCents: 27900,
    currency: "usd",
    ratingValue: 4.6,
    ratingCount: 73,
    ships: "2-5 biz days + 3-7 transit",
    imageUrl: "https://www.zoro.com/static/cms/product/large/8ef346ef-a2ec-4a1c-862a-a741a5a14ed7.jpeg",
    tab: "desk_setup",
    shippingClass: "hardware",
  },
  {
    sku: "QNT-SEC-PRIVACY-ST-2461A",
    name: "24\" 16:10 Monitor Privacy Screen (Hanging)",
    description: "Hanging privacy shield for consistent multi-seat security.",
    priceCents: 21900,
    currency: "usd",
    ratingValue: 4.5,
    ratingCount: 41,
    ships: "2-5 biz days + 3-7 transit",
    imageUrl: "https://www.zoro.com/static/cms/product/petit/fe4e36ce-cdeb-463e-b0bb-fb68710f5778.jpeg",
    tab: "privacy",
    shippingClass: "hardware",
  },
];

const CATALOG_BY_SKU = new Map<string, ShopCatalogItem>();
CATALOG.forEach((item) => CATALOG_BY_SKU.set(item.sku, item));

function sanitizeSku(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9-]/g, "")
    .slice(0, 64);
}

export function getCatalogBySku(sku: unknown): ShopCatalogItem | null {
  const cleanSku = sanitizeSku(sku);
  if (!cleanSku) return null;
  return CATALOG_BY_SKU.get(cleanSku) || null;
}

export function getCatalogPublicItems(): Array<Record<string, unknown>> {
  return CATALOG.map((item) => ({
    sku: item.sku,
    name: item.name,
    description: item.description,
    priceCents: item.priceCents,
    compareAtCents: item.compareAtCents || null,
    currency: item.currency,
    rating: {
      value: item.ratingValue,
      count: item.ratingCount,
    },
    ships: item.ships,
    imageUrl: item.imageUrl,
    placeholderImageUrl: SHOP_PLACEHOLDER_IMAGE,
    tab: item.tab,
    shippingClass: item.shippingClass,
  }));
}

export function resolveShippingCost(subtotalCents: number, hasHardwareOrPrivacy: boolean): number {
  const policy = hasHardwareOrPrivacy ? SHOP_SHIPPING_POLICY.hardware : SHOP_SHIPPING_POLICY.pod;
  if (subtotalCents >= policy.freeOverCents) return 0;
  return policy.flatRateCents;
}

export function getShippingPolicyForCheckout(hasHardwareOrPrivacy: boolean): {
  label: string;
  deliveryEstimate: { minBusinessDays: number; maxBusinessDays: number };
  freeOverCents: number;
} {
  if (hasHardwareOrPrivacy) {
    return {
      label: "Hardware shipping",
      deliveryEstimate: { minBusinessDays: 5, maxBusinessDays: 10 },
      freeOverCents: SHOP_SHIPPING_POLICY.hardware.freeOverCents,
    };
  }
  return {
    label: "POD shipping",
    deliveryEstimate: { minBusinessDays: 5, maxBusinessDays: 12 },
    freeOverCents: SHOP_SHIPPING_POLICY.pod.freeOverCents,
  };
}

export function getCatalogSize(): number {
  return CATALOG.length;
}
