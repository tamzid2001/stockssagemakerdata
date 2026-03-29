export type ShopTab = "cases" | "audio" | "workspace" | "wearables";
export type ShippingClass = "pod" | "hardware";
export type ShopItemKind = "product" | "bundle";

export type ShopCatalogAsset = {
  url: string;
  alt: string;
};

export type ShopCatalogFact = {
  label: string;
  value: string;
};

export type ShopCatalogItem = {
  sku: string;
  kind: ShopItemKind;
  name: string;
  description: string;
  longDescription: string;
  priceCents: number;
  compareAtCents?: number;
  currency: "usd";
  ratingValue: number;
  ratingCount: number;
  ships: string;
  imageUrl: string;
  images: ShopCatalogAsset[];
  tab: ShopTab;
  shippingClass: ShippingClass;
  provider: string;
  providerScore: number;
  providerMethod?: string;
  location?: string;
  productionTime?: string;
  badge?: string;
  highlights: string[];
  detailBullets: string[];
  materials?: string[];
  options?: string[];
  compliance?: string[];
  careInstructions?: string;
  origin?: string;
  factGrid: ShopCatalogFact[];
  rewardUnlockRequired?: boolean;
  unlockCtaCopy?: string;
  bundleComponents?: string[];
};

export const SHOP_PLACEHOLDER_IMAGE = "/assets/shop/placeholder.png";

export const SHOP_SHIPPING_POLICY = {
  pod: {
    label: "Quantura physical product shipping",
    flatRateCents: 539,
    freeOverCents: 9900,
    estimate: "Production usually clears in 1-3 business days, then delivery lands in roughly 4-7 business days.",
    detail: "Provider and size can move shipping slightly, but checkout always shows the final carrier rate before payment.",
  },
  hardware: {
    label: "Quantura physical product shipping",
    flatRateCents: 539,
    freeOverCents: 9900,
    estimate: "Production usually clears in 1-3 business days, then delivery lands in roughly 4-7 business days.",
    detail: "Provider and size can move shipping slightly, but checkout always shows the final carrier rate before payment.",
  },
} as const;

function normalizeAssetUrl(rawUrl: string): string {
  const input = String(rawUrl || "").trim();
  if (!input) return "";
  try {
    const url = new URL(input);
    const normalizedHost = url.hostname.trim().toLowerCase();
    if (url.protocol !== "https:" && url.protocol !== "http:") return "";
    if (normalizedHost === "dropbox.com" || normalizedHost.endsWith(".dropbox.com")) {
      url.searchParams.delete("dl");
      url.searchParams.set("raw", "1");
    }
    return url.toString();
  } catch (_error) {
    return "";
  }
}

function buildAssetList(label: string, urls: string[]): ShopCatalogAsset[] {
  return urls
    .map((url, index) => ({
      url: normalizeAssetUrl(url),
      alt: `${label} mockup ${index + 1}`,
    }))
    .filter((asset) => asset.url);
}

const ROOT_CAMPAIGN_IMAGES = buildAssetList("Quantura campaign", [
  "https://www.dropbox.com/scl/fi/ubcenrr3j766i3pbjh9kx/IMG_4204.PNG?rlkey=h4g2mezvqxqiikfqy38028vye&raw=1",
  "https://www.dropbox.com/scl/fi/m228wtp7m5i2c3sdec95u/IMG_4199.PNG?rlkey=24hnguf6ycnnnrqqb8558iup0&raw=1",
  "https://www.dropbox.com/scl/fi/hha9k8pt3qmayr5vgicqq/IMG_4206.PNG?rlkey=cgtsgdbl1ups8yu8s0p1hmgge&raw=1",
  "https://www.dropbox.com/scl/fi/3zx6wo8l9r0sn18isf9xp/IMG_4207.PNG?rlkey=zb3u9ff61tp47iqzqeq0mrfbm&raw=1",
  "https://www.dropbox.com/scl/fi/ehx7mggvidwazz764al1o/IMG_4200.PNG?rlkey=ofje993ypidj6k4z95p4r59v1&raw=1",
  "https://www.dropbox.com/scl/fi/wuacvoy653yn0cx2ntixk/IMG_4201.png?rlkey=c0o39b8mm0wkvcxibh4xm3izc&raw=1",
]);

const AIRPODS_CASE_IMAGES = buildAssetList("AirPods case", [
  "https://www.dropbox.com/scl/fi/x9698vzmltssuqg6gc3jy/airpod-cases.jpg?rlkey=f6vy6qltihfvt5fsjvw2sick7&raw=1",
  "https://www.dropbox.com/scl/fi/tmshxnsncl21g1ng7rr2n/airpod-cases-1.jpg?rlkey=xtiruc2im758hmswn02mefccd&raw=1",
  "https://www.dropbox.com/scl/fi/mt6ssge9xqwdhil3cnmtq/airpod-cases-2.jpg?rlkey=idkve1thmqa2pphgig2gq63nf&raw=1",
  "https://www.dropbox.com/scl/fi/ovkgn16gdxslkq3dag4jo/airpod-cases-3.jpg?rlkey=xjaauza8ctbgj9uzrsf6d9xyk&raw=1",
  "https://www.dropbox.com/scl/fi/f9mgarvnh301jiy11xotx/airpod-cases-4.jpg?rlkey=cbbgmol7bww9i0749ptf2f112&raw=1",
  "https://www.dropbox.com/scl/fi/eemvhlssu2l1vcqpkm5cm/airpod-cases-5.jpg?rlkey=lbrvzyliowtaq9susygzlok0u&raw=1",
]);

const MACBOOK_CASE_IMAGES = buildAssetList("MacBook case", [
  "https://www.dropbox.com/scl/fi/r9rdn1cnum4spg0iwlwgd/macbook-cases.jpg?rlkey=vl0rju69ynojq2pyw8gcyvrzu&raw=1",
  "https://www.dropbox.com/scl/fi/n4diekbgbfubz8sbfb9rb/macbook-cases-1.jpg?rlkey=ofnwl8irkfabc2ojf1ptmb7ak&raw=1",
  "https://www.dropbox.com/scl/fi/wfumvs1vt2bwte76p4wz0/macbook-cases-2.jpg?rlkey=9mce4co7ti6n0mwyid9wvsdap&raw=1",
  "https://www.dropbox.com/scl/fi/zfwop7gh1umy6n198115d/macbook-cases-3.jpg?rlkey=jnc8ajrggw8ylxpq4jyutaxj3&raw=1",
  "https://www.dropbox.com/scl/fi/0v4uq8s1rmxzs6nbjxgwc/macbook-cases-4.jpg?rlkey=5o17gn85gc7yslqlamlmm1qz0&raw=1",
]);

const WATCH_BAND_IMAGES = buildAssetList("Watch band", [
  "https://www.dropbox.com/scl/fi/kntwiiiy7wkvft0gklsrs/watch-band.jpg?rlkey=e9yk0upfo0n2c6nlzisd9eqnb&raw=1",
  "https://www.dropbox.com/scl/fi/lv02i37yretpezs5ifmur/watch-band-1.jpg?rlkey=itlmct5e5bnz4g3oog2c7z3j5&raw=1",
  "https://www.dropbox.com/scl/fi/jm9smuochc5rhyd7aclpj/watch-band-2.jpg?rlkey=0egd8ozh05niieglorwtd3hg3&raw=1",
  "https://www.dropbox.com/scl/fi/qvjttcsd7gsnlpk9xemxn/watch-band-3.jpg?rlkey=9e2zxlboz6x53aasegtnntxcu&raw=1",
  "https://www.dropbox.com/scl/fi/su76tcexyifls6nutk4h2/watch-band-4.jpg?rlkey=db73vw5m8nqyujjpfvcgw5bhm&raw=1",
  "https://www.dropbox.com/scl/fi/fg9n9qxpcqb2ynezukl97/watch-band-5.jpg?rlkey=uvp2jfh5jf7w6zvyaphi5tpkx&raw=1",
]);

const JABBA_SPEAKER_IMAGES = buildAssetList("Jabba Bluetooth speaker", [
  "https://www.dropbox.com/scl/fi/jmjzmxmhkbntm2jqrp62h/jabba-bluetooth-speaker.jpg?rlkey=1q3nftiu9w5tboadrzla5wd4j&raw=1",
  "https://www.dropbox.com/scl/fi/d3z1waz7zridwch7c1tu1/jabba-bluetooth-speaker-1.jpg?rlkey=h64j4hpmcrtmotx18rwxp3586&raw=1",
  "https://www.dropbox.com/scl/fi/6bqvwmofqfx76jkool3v8/jabba-bluetooth-speaker-2.jpg?rlkey=fd4jxx2hhdhncvomn3vhyt24s&raw=1",
  "https://www.dropbox.com/scl/fi/jmscuytp9ci0ktxtl6wn9/jabba-bluetooth-speaker-3.jpg?rlkey=0h9v1gt6vpo6srg2w4kg8mh0g&raw=1",
  "https://www.dropbox.com/scl/fi/v2xj4suxq296hk8tkp12y/jabba-bluetooth-speaker-4.jpg?rlkey=zdnhcn3v7j709yjvobw80aw5a&raw=1",
]);

const DESK_MAT_IMAGES = buildAssetList("Desk mat", [
  "https://www.dropbox.com/scl/fi/ayalumxaflb9vbcnk6p59/desk-mat.jpg?rlkey=7i0f2iplr1zkj1cksrofowr5t&raw=1",
  "https://www.dropbox.com/scl/fi/n6f4to4flajw9wgrnjfah/desk-mat-1.jpg?rlkey=8dqii94dl0p0id6f0efyadezj&raw=1",
  "https://www.dropbox.com/scl/fi/c49w2pjx2tt45hmc9ho8x/desk-mat-2.jpg?rlkey=8zz2h3h2oa4131t7nzpt3xfjo&raw=1",
  "https://www.dropbox.com/scl/fi/mjt9wwxg6e9rahlbqt5ei/desk-mat-3.jpg?rlkey=cg8ppahtvictwgn69u1fq84cl&raw=1",
  "https://www.dropbox.com/scl/fi/yuskz2hdyvc0pc8mfzq0p/desk-mat-4.jpg?rlkey=25di8t0u061ofrstzehovpt71&raw=1",
  "https://www.dropbox.com/scl/fi/nk3q912wuu62sn2assu2c/desk-mat-5.jpg?rlkey=prfr4fpy7qf4p4tx6nhpbbfvd&raw=1",
]);

const AIRPODS_MAX_CASE_IMAGES = buildAssetList("AirPods Max case", [
  "https://www.dropbox.com/scl/fi/xa7s6m7t923pglw6ah8ol/airpod-max-cases-aop.jpg?rlkey=7i7ipbjqsqp60xe31sofqbp3w&raw=1",
  "https://www.dropbox.com/scl/fi/lqbcyxn7bpqmylyujoqmj/airpod-max-cases-aop-1.jpg?rlkey=zrk4b2neso5m3khcz0xg66qy7&raw=1",
  "https://www.dropbox.com/scl/fi/i77b1essaudypvgf35anw/airpod-max-cases-aop-2.jpg?rlkey=vb97667oibc3p41pyugy0gm50&raw=1",
  "https://www.dropbox.com/scl/fi/sa7cl870elo83s6k4r4um/airpod-max-cases-aop-3.jpg?rlkey=t4n1z4d60jhva87fvlurjegrj&raw=1",
  "https://www.dropbox.com/scl/fi/68t0oit20c437ren97sfn/airpod-max-cases-aop-4.jpg?rlkey=9ecb8s32ordtvnqp42stpqmzz&raw=1",
  "https://www.dropbox.com/scl/fi/hi35vlxe2ke721t8yvyw4/airpod-max-cases-aop-5.jpg?rlkey=86qrfr5j0bnxv753c2qyght4l&raw=1",
]);

const QUAKE_CHARGER_IMAGES = buildAssetList("Quake wireless charger", [
  "https://www.dropbox.com/scl/fi/zskho6uzccyr2jczvzvoe/quake-wireless-charging-pad.jpg?rlkey=b7ev5cio0ub0w4jxjldfvvjvu&raw=1",
  "https://www.dropbox.com/scl/fi/lhir84pzizygszai8l8qg/quake-wireless-charging-pad-1.jpg?rlkey=q13pcngavjn4umezsxln2xxs8&raw=1",
  "https://www.dropbox.com/scl/fi/0egdz3q9r2lr76yu2no3e/quake-wireless-charging-pad-2.jpg?rlkey=1eu9qmpsmsqpv083zn8xulr3a&raw=1",
  "https://www.dropbox.com/scl/fi/bc49frigi75b3ejd62udl/quake-wireless-charging-pad-3.jpg?rlkey=df72i5a7nr2tpnanh58s8kdxu&raw=1",
  "https://www.dropbox.com/scl/fi/d7oopk56fhcqil2ru7jdp/quake-wireless-charging-pad-4.jpg?rlkey=ctujhiepq8wvt4chi285xkobt&raw=1",
  "https://www.dropbox.com/scl/fi/cmg057jyd4ho3xazkolc8/quake-wireless-charging-pad-5.jpg?rlkey=5dzedn9g306yy2u9hdq8w8f20&raw=1",
]);

const ESSOS_EARBUD_IMAGES = buildAssetList("Essos wireless earbuds", [
  "https://www.dropbox.com/scl/fi/nk80q28lyfo3bv4e5zxbn/essos-wireless-earbuds.jpg?rlkey=f5j4fplx6ui4w76l2gkncvr2z&raw=1",
  "https://www.dropbox.com/scl/fi/ejwuhdwsn41jajul1ly27/essos-wireless-earbuds-1.jpg?rlkey=2qhf0paj1z4c5d3vfcuhxfrsg&raw=1",
  "https://www.dropbox.com/scl/fi/w881xz0r1txtp69xela5j/essos-wireless-earbuds-2.jpg?rlkey=kddhsghelsu2onazzox517rqx&raw=1",
  "https://www.dropbox.com/scl/fi/96yp17oldibhx3ryssxid/essos-wireless-earbuds-3.jpg?rlkey=eztiusjtb4ugc7667soabnzf5&raw=1",
  "https://www.dropbox.com/scl/fi/7bj7rutcsw278ymcql1eb/essos-wireless-earbuds-4.jpg?rlkey=q21yljf5mwjw4jeksjqxlx5q8&raw=1",
  "https://www.dropbox.com/scl/fi/y6w6khagzq28ud0isv1gs/essos-wireless-earbuds-5.jpg?rlkey=nemv64758rvmjqd1nv58xb72h&raw=1",
]);

const PRODUCTS: ShopCatalogItem[] = [
  {
    sku: "QNT-CASE-AIRPODS",
    kind: "product",
    name: "Quantura AirPods Cases",
    description: "3D full-wrap AirPods shells with polycarbonate protection, gloss finish, and Qi-friendly access.",
    longDescription:
      "A lightweight everyday shell for AirPods owners who want the design to reach the lid and sidewalls instead of stopping at the front panel. The polycarbonate body stays slim in-hand, protects against scratches, and keeps charging access clean.",
    priceCents: 2799,
    compareAtCents: 2999,
    currency: "usd",
    ratingValue: 4.7,
    ratingCount: 91,
    ships: "From $4.39 shipping. Avg. production time 1.7 days.",
    imageUrl: AIRPODS_CASE_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: AIRPODS_CASE_IMAGES,
    tab: "cases",
    shippingClass: "pod",
    provider: "WOYC",
    providerScore: 9.1,
    providerMethod: "Dye Sublimation",
    location: "Local USA providers / South Korea blank source",
    productionTime: "1.7 days",
    badge: "Popular",
    highlights: [
      "Full-wrap dye sublimation covers the front, side walls, and top lid.",
      "Slim, lightweight polycarbonate shell with included carabiner.",
      "Charging ports stay accessible and the case remains Qi compatible.",
    ],
    detailBullets: [
      "Fits AirPods Pro Gen 1 and 2 plus AirPods Gen 1 through Gen 4.",
      "Gloss finish keeps artwork crisp while staying scratch resistant.",
      "Compliance exceeds BPA levels and meets heavy-metal and phthalate thresholds.",
      "Wipe down with a clean, dry cloth to keep the shell clear of dust and fingerprints.",
    ],
    materials: ["100% polycarbonate", "Gloss finish", "Scratch-resistant shell", "Carabiner included"],
    options: [
      "AirPods Pro Gen 1",
      "AirPods Pro Gen 2",
      "AirPods Gen 1",
      "AirPods Gen 2",
      "AirPods Gen 3",
      "AirPods Gen 4",
    ],
    compliance: [
      "Lead, cadmium, mercury, and phthalate thresholds compliant",
      "Azo dye and aromatic hydrocarbon thresholds compliant",
      "Exceeds BPA safety requirement",
    ],
    careInstructions: "Wipe the dust or any dirt off gently with a clean, dry cloth.",
    origin: "Made in South Korea",
    factGrid: [
      { label: "Retail", value: "From $27.99" },
      { label: "Provider", value: "WOYC 9.1 / Dye sublimation" },
      { label: "Print areas", value: "Front, back, and full wrap" },
      { label: "Fit range", value: "AirPods Pro Gen 1 to AirPods Gen 4" },
    ],
  },
  {
    sku: "QNT-CASE-MACBOOK",
    kind: "product",
    name: "Quantura MacBook Cases",
    description: "Frosted or clear MacBook shells with matte graphics, port access, and a low-profile non-slip finish.",
    longDescription:
      "A quiet protective layer for operators who want a case that does not bulk up a MacBook. The shell snaps on and off fast, keeps vents and buttons clear, and adds a matte printed statement that still feels understated.",
    priceCents: 4435,
    compareAtCents: 4999,
    currency: "usd",
    ratingValue: 4.8,
    ratingCount: 74,
    ships: "From $5.99 shipping. Avg. production time 2.0 days.",
    imageUrl: MACBOOK_CASE_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: MACBOOK_CASE_IMAGES,
    tab: "cases",
    shippingClass: "pod",
    provider: "WOYC",
    providerScore: 9.1,
    providerMethod: "UV Print",
    location: "Local USA providers",
    productionTime: "2.0 days",
    badge: "Desk favorite",
    highlights: [
      "Protective coated finish resists scratches, blurring, and image fade.",
      "Snap-on installation keeps every port, vent, and button fully accessible.",
      "Non-slip, low-profile shell keeps your laptop planted without adding travel weight.",
    ],
    detailBullets: [
      "Available across MacBook Air 13.6 and 15.3 plus MacBook Pro 13.3, 14, and 16 inch lines.",
      "Choose between frosted or clear shell styling with a matte printed face.",
      "The soft olive-heart concept stays subtle and tactile instead of loud or glossy.",
      "Built for everyday desk movement, cafe sessions, and transit without losing the laptop's original feel.",
    ],
    materials: ["Frosted or clear shell", "Matte print finish", "Non-slip outer surface", "Durable snap-on construction"],
    options: [
      "MacBook Air 13.6",
      "MacBook Air 15.3",
      "MacBook Pro 13.3",
      "MacBook Pro 14",
      "MacBook Pro 16",
    ],
    careInstructions: "Wipe gently with a clean, dry microfiber cloth.",
    factGrid: [
      { label: "Retail", value: "$44.35" },
      { label: "Provider", value: "WOYC 9.1 / UV print" },
      { label: "Production", value: "2.0 days avg." },
      { label: "Model span", value: "MacBook Air 13.6 to MacBook Pro 16" },
    ],
  },
  {
    sku: "QNT-WEAR-WATCHBAND",
    kind: "product",
    name: "Quantura Watch Bands",
    description: "Faux-leather Apple Watch bands with stainless hardware and a print-ready outer face.",
    longDescription:
      "A fashion-first Apple Watch strap for nights out, travel, and everyday office wear. The faux-leather band keeps the look refined while the printed outer face carries the design language of the rest of the Quantura accessories line.",
    priceCents: 3599,
    compareAtCents: 3999,
    currency: "usd",
    ratingValue: 4.6,
    ratingCount: 63,
    ships: "From $4.39 shipping. Avg. production time 2.8 days.",
    imageUrl: WATCH_BAND_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: WATCH_BAND_IMAGES,
    tab: "wearables",
    shippingClass: "pod",
    provider: "WOYC",
    providerScore: 9.1,
    providerMethod: "Printed faux leather strap",
    location: "Local USA providers / South Korea blank source",
    productionTime: "2.8 days",
    badge: "Wearable",
    highlights: [
      "Animal-friendly faux leather strap with stainless buckle and adapters.",
      "Designed for Apple Watch Series 1 through 9, Ultra, and SE devices.",
      "Printed face turns the band into a subtle accent instead of a generic swap.",
    ],
    detailBullets: [
      "Available in 38-41 mm and 42-45 mm sizing.",
      "RoHS, REACH, and Intertek certified compliance profile.",
      "Ideal for dressier fits, events, and cleaner office styling.",
      "Clean with a dry cloth and avoid aggressive moisture exposure.",
    ],
    materials: ["100% polyester faux leather strap", "100% stainless steel buckle", "100% stainless steel adapters"],
    options: ["38 - 41 mm", "42 - 45 mm", "Four colorways"],
    compliance: ["RoHS", "REACH", "Intertek certified"],
    careInstructions: "Wipe the dust or any dirt off gently with a clean, dry cloth.",
    origin: "Blank product sourced from South Korea",
    factGrid: [
      { label: "Retail", value: "From $35.99" },
      { label: "Provider", value: "WOYC 9.1" },
      { label: "Sizing", value: "38 - 41 mm and 42 - 45 mm" },
      { label: "Materials", value: "Faux leather + stainless hardware" },
    ],
  },
  {
    sku: "QNT-AUDIO-JABBA",
    kind: "product",
    name: "Quantura Jabba Bluetooth Speaker",
    description: "Compact Bluetooth speaker with 33-foot range, Aux input, and quick-travel form factor.",
    longDescription:
      "A lightweight desktop or travel speaker that can hop from workspace to hotel desk to quick outdoor hang. It keeps the setup simple with Bluetooth 4.2, two hours of playback, and a compact shell that still carries a bold print surface.",
    priceCents: 3499,
    compareAtCents: 3899,
    currency: "usd",
    ratingValue: 4.7,
    ratingCount: 54,
    ships: "From $5.79 shipping. Production usually under one day.",
    imageUrl: JABBA_SPEAKER_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: JABBA_SPEAKER_IMAGES,
    tab: "audio",
    shippingClass: "pod",
    provider: "SwagRabbit",
    providerScore: 9.3,
    providerMethod: "Printed ABS speaker shell",
    location: "Local USA providers",
    productionTime: "Under 1 day",
    badge: "Travel audio",
    highlights: [
      "Bluetooth 4.2 with a 10-meter working range.",
      "Two hours of playback at max volume with 3W output.",
      "Aux cable support gives you a wired fallback when needed.",
    ],
    detailBullets: [
      "Durable ABS shell keeps the footprint small while protecting the printed face.",
      "Works well for hotel-room briefings, light outdoor use, or quick desk playback.",
      "Battery is built in at 3.7V / 500mAh.",
      "Unplug the charger before cleaning and wipe with a dry cloth only.",
    ],
    materials: ["100% ABS plastic exterior", "Built-in Li-ion battery", "3.5mm Aux input"],
    options: ['One size: 4.25" x 2.25"'],
    careInstructions: "Unplug the charger first. Wipe the dust or any dirt off gently with a clean, dry cloth.",
    factGrid: [
      { label: "Retail", value: "From $34.99" },
      { label: "Provider", value: "SwagRabbit 9.3" },
      { label: "Playback", value: "2 hours" },
      { label: "Range", value: "33 feet / 10 meters" },
    ],
  },
  {
    sku: "QNT-WORK-DESKMAT",
    kind: "product",
    name: "Quantura Desk Mats",
    description: "Polyester-and-rubber desk mats with smooth mouse glide, anti-fray edges, and clean command-center coverage.",
    longDescription:
      "A low-friction desk surface for people who live inside dashboards, terminals, and notebooks all day. The mat protects the desk, keeps the mouse movement even, and gives a workspace a more deliberate visual anchor without overcomplicating the setup.",
    priceCents: 2399,
    compareAtCents: 2799,
    currency: "usd",
    ratingValue: 4.8,
    ratingCount: 118,
    ships: "From $7.09 shipping. Avg. production time 1.6 days.",
    imageUrl: DESK_MAT_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: DESK_MAT_IMAGES,
    tab: "workspace",
    shippingClass: "pod",
    provider: "Colorway",
    providerScore: 8.9,
    providerMethod: "Printed polyester desk mat",
    location: "Local USA providers / China blank source",
    productionTime: "1.6 days",
    badge: "Workspace",
    highlights: [
      "Smooth polyester top supports both optical and laser mice.",
      "Natural rubber base prevents sliding during longer sessions.",
      "Anti-fray edges and sewn label keep the finish tidy over time.",
    ],
    detailBullets: [
      "Available in 14.4 x 12.1, 23.6 x 13.8, and 31.5 x 15.5 inch sizes.",
      "Great for turning a desk into a cleaner multi-screen control surface.",
      "Surface resists scratches and stains while keeping color crisp.",
      "Spot clean with warm water and dish soap, using a soft brush for stubborn marks.",
    ],
    materials: ["100% polyester front", "100% natural rubber backing", "Anti-fray stitched edge", "Black non-slip base"],
    options: ['14.4" x 12.1"', '23.6" x 13.8"', '31.5" x 15.5"'],
    careInstructions:
      "Use warm water and dish soap to clean spots off your pad. For hard-to-clean spots use a soft-bristled brush.",
    origin: "Blank sourced from China",
    factGrid: [
      { label: "Retail", value: "From $23.99" },
      { label: "Provider", value: "Colorway 8.9" },
      { label: "Sizes", value: "Three desk coverage formats" },
      { label: "Base", value: "Natural rubber non-slip backing" },
    ],
  },
  {
    sku: "QNT-CASE-AIRPODSMAX",
    kind: "product",
    name: "Quantura AirPods Max Cases",
    description: "Full-cover AirPods Max shells in lightweight polycarbonate with matte or gloss finish options.",
    longDescription:
      "An all-over printed shell for AirPods Max owners who want a lighter case than a travel clamshell but still need scratch resistance and better daily carry protection. The shell keeps the headphones case-friendly and works with both wired and wireless charging habits.",
    priceCents: 3199,
    compareAtCents: 3599,
    currency: "usd",
    ratingValue: 4.6,
    ratingCount: 47,
    ships: "From $4.39 shipping. Avg. production time 1.8 days.",
    imageUrl: AIRPODS_MAX_CASE_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: AIRPODS_MAX_CASE_IMAGES,
    tab: "cases",
    shippingClass: "pod",
    provider: "WOYC",
    providerScore: 9.1,
    providerMethod: "All-over sublimation print",
    location: "Local USA providers / South Korea blank source",
    productionTime: "1.8 days",
    badge: "Apple carry",
    highlights: [
      "Full-cover sublimation print keeps the artwork continuous around the shell.",
      "Available in gloss or matte presentation depending the finish you want.",
      "Shell stays slim enough to remain case-friendly and charging compatible.",
    ],
    detailBullets: [
      "AirPods Max only. Headphones are not included.",
      "Polycarbonate body balances scratch resistance with low carry weight.",
      "Ideal when you want daily bag protection without bulky travel storage.",
      "Clean with a dry cloth to keep the surface clear and vivid.",
    ],
    materials: ["100% polycarbonate", "Gloss or matte finish options", "Lightweight shell"],
    options: ["AirPods Max fit", "Gloss finish", "Matte finish"],
    careInstructions: "Wipe the dust or any dirt off gently with a clean, dry cloth.",
    origin: "Made in South Korea",
    factGrid: [
      { label: "Retail", value: "From $31.99" },
      { label: "Provider", value: "WOYC 9.1" },
      { label: "Finish", value: "Matte or gloss" },
      { label: "Compatibility", value: "AirPods Max" },
    ],
  },
  {
    sku: "QNT-WORK-QUAKE",
    kind: "product",
    name: "Quantura Quake Wireless Charging Pad",
    description: "Durable 5W wireless charging pad with vivid top print, Micro USB cable, and broad iPhone/Android support.",
    longDescription:
      "A simple charging puck for workstations and travel kits that need less clutter. The double-wall plastic body keeps it durable, and the printed face turns a generic charger into a branded desk detail instead of a forgettable cable island.",
    priceCents: 2499,
    compareAtCents: 2899,
    currency: "usd",
    ratingValue: 4.6,
    ratingCount: 57,
    ships: "From $5.39 shipping. Production usually under one day.",
    imageUrl: QUAKE_CHARGER_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: QUAKE_CHARGER_IMAGES,
    tab: "workspace",
    shippingClass: "pod",
    provider: "SwagRabbit",
    providerScore: 9.3,
    providerMethod: "Printed charging pad",
    location: "Local USA providers",
    productionTime: "Under 1 day",
    badge: "Desk utility",
    highlights: [
      "Supports 5W wireless charging for iPhone and many Android models.",
      "Ships with an 11.8 inch USB to Micro USB cable for standard charging.",
      "Lightweight 3.2 oz body makes it easy to keep one at desk and one in a bag.",
    ],
    detailBullets: [
      "Works with most plastic phone cases.",
      "Designed around a durable double-wall plastic build.",
      "Compatibility list includes iPhone, Samsung Galaxy S/Note lines, Google Nexus, HTC, and LG legacy wireless models.",
      "Always unplug before cleaning and wipe with a clean, dry cloth.",
    ],
    materials: ["Durable double-wall plastic", '11.8" USB to Micro USB cable included'],
    options: ['One size: 2.75" x 2.75"', "Two colorways"],
    careInstructions: "Unplug charger first. Wipe the dust or any dirt off gently with a clean, dry cloth.",
    factGrid: [
      { label: "Retail", value: "From $24.99" },
      { label: "Provider", value: "SwagRabbit 9.3" },
      { label: "Charging", value: "5W wireless" },
      { label: "Footprint", value: '2.75" round pad' },
    ],
  },
  {
    sku: "QNT-AUDIO-ESSOS",
    kind: "product",
    name: "Quantura Essos Wireless Earbuds",
    description: "True wireless earbuds with custom case, built-in controls, and a charging case that doubles as a power bank.",
    longDescription:
      "A cable-free audio option for commuting, quick calls, or focus sessions between meetings. The earbuds auto-pair, the case carries the visual design, and the 400mAh charging case can top the earbuds up multiple times across a day.",
    priceCents: 3399,
    compareAtCents: 3799,
    currency: "usd",
    ratingValue: 4.7,
    ratingCount: 69,
    ships: "From $5.39 shipping. Production usually under one day.",
    imageUrl: ESSOS_EARBUD_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: ESSOS_EARBUD_IMAGES,
    tab: "audio",
    shippingClass: "pod",
    provider: "SwagRabbit",
    providerScore: 9.3,
    providerMethod: "Printed wireless earbud case",
    location: "Local USA providers",
    productionTime: "Under 1 day",
    badge: "Wireless audio",
    highlights: [
      "Bluetooth 5.0 with auto-pairing and auto power-on.",
      "Built-in music controls and dual microphones for calls.",
      "400mAh charging case can recharge the earbuds up to four times.",
    ],
    detailBullets: [
      "Two hours of playback on the earbuds themselves.",
      "Includes USB charging cable plus three pairs of rubber tips.",
      "Ergonomic fit keeps them usable across commuting, gym, and desk sessions.",
      "Clean only after unplugging, using a dry cloth.",
    ],
    materials: ["ABS plastic", "400mAh charging case", "USB charging cable included"],
    options: ["One size", "Three ear-tip sizes included"],
    careInstructions: "Unplug charger first. Wipe the dust or any dirt off gently with a clean, dry cloth.",
    factGrid: [
      { label: "Retail", value: "From $33.99" },
      { label: "Provider", value: "SwagRabbit 9.3" },
      { label: "Playback", value: "2 hours per charge" },
      { label: "Case battery", value: "400mAh / 4 recharges" },
    ],
  },
];

const BUNDLES: ShopCatalogItem[] = [
  {
    sku: "QNT-BUNDLE-DESKCOMMAND",
    kind: "bundle",
    name: "Desk Command Bundle",
    description: "Desk mat, MacBook case, and wireless charger grouped into one cleaner workstation reset.",
    longDescription:
      "A workspace-focused bundle for operators who want the desk to feel intentional in one move. It pairs the desk mat, MacBook shell, and Quake charger into a single offer that is revealed inside the native app after the rewarded unlock completes.",
    priceCents: 9499,
    compareAtCents: 10899,
    currency: "usd",
    ratingValue: 4.9,
    ratingCount: 24,
    ships: "Native unlock offer. Ships on the same physical-product timeline after checkout.",
    imageUrl: DESK_MAT_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: [DESK_MAT_IMAGES[0], MACBOOK_CASE_IMAGES[0], QUAKE_CHARGER_IMAGES[0]].filter(Boolean),
    tab: "workspace",
    shippingClass: "pod",
    provider: "Quantura bundle offer",
    providerScore: 10,
    providerMethod: "Reward-unlocked native bundle",
    location: "Unlock on iOS and Android builds",
    productionTime: "Bundle unlock after rewarded interstitial",
    badge: "Reward bundle",
    highlights: [
      "Reward-gated offer in native app after rewarded interstitial completes.",
      "Discounted workstation trio built around desk utility and clean carry.",
      "One checkout line item for the bundle instead of piecing every item together.",
    ],
    detailBullets: [
      "Bundle includes the Desk Mat, MacBook Case, and Quake Wireless Charging Pad.",
      "Native app users unlock the discounted price after the rewarded flow completes.",
      "Web keeps the bundle visible, but the discounted unlock is reserved for iOS and Android.",
      "Shipping follows the same physical-product policy as the underlying items.",
    ],
    factGrid: [
      { label: "Bundle price", value: "$94.99" },
      { label: "Compare at", value: "$108.99" },
      { label: "Unlock", value: "Rewarded interstitial on native" },
      { label: "Includes", value: "Desk mat + MacBook case + charging pad" },
    ],
    rewardUnlockRequired: true,
    unlockCtaCopy: "Unlock bundle",
    bundleComponents: ["Quantura Desk Mats", "Quantura MacBook Cases", "Quantura Quake Wireless Charging Pad"],
  },
  {
    sku: "QNT-BUNDLE-AUDIOCARRY",
    kind: "bundle",
    name: "Audio Carry Bundle",
    description: "Jabba speaker, Essos earbuds, and AirPods case grouped for commute-ready audio carry.",
    longDescription:
      "A travel-ready audio set that pairs compact playback, pocket earbuds, and an AirPods shell into one discountable native bundle. It is meant for users who bounce between desk, meetings, and travel but want a tighter gear stack.",
    priceCents: 8999,
    compareAtCents: 10199,
    currency: "usd",
    ratingValue: 4.8,
    ratingCount: 21,
    ships: "Native unlock offer. Ships on the same physical-product timeline after checkout.",
    imageUrl: JABBA_SPEAKER_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: [JABBA_SPEAKER_IMAGES[0], ESSOS_EARBUD_IMAGES[0], AIRPODS_CASE_IMAGES[0]].filter(Boolean),
    tab: "audio",
    shippingClass: "pod",
    provider: "Quantura bundle offer",
    providerScore: 10,
    providerMethod: "Reward-unlocked native bundle",
    location: "Unlock on iOS and Android builds",
    productionTime: "Bundle unlock after rewarded interstitial",
    badge: "Reward bundle",
    highlights: [
      "Made for commutes, quick travel, and all-day desk-to-go audio.",
      "Bundle unlock happens after rewarded flow in the native shells.",
      "Pairs one pocketable speaker, one earbud case, and one AirPods shell.",
    ],
    detailBullets: [
      "Bundle includes the Jabba Bluetooth Speaker, Essos Wireless Earbuds, and AirPods Cases line.",
      "Great when you want a tighter audio setup without buying every piece separately.",
      "Discounted unlock state persists locally after the reward is earned.",
      "Final shipping cost is still confirmed at checkout.",
    ],
    factGrid: [
      { label: "Bundle price", value: "$89.99" },
      { label: "Compare at", value: "$101.99" },
      { label: "Unlock", value: "Rewarded interstitial on native" },
      { label: "Includes", value: "Speaker + earbuds + AirPods case" },
    ],
    rewardUnlockRequired: true,
    unlockCtaCopy: "Unlock bundle",
    bundleComponents: ["Quantura Jabba Bluetooth Speaker", "Quantura Essos Wireless Earbuds", "Quantura AirPods Cases"],
  },
  {
    sku: "QNT-BUNDLE-APPLETRAVEL",
    kind: "bundle",
    name: "Apple Travel Bundle",
    description: "Watch band, AirPods Max shell, and MacBook case grouped for a tighter Apple carry kit.",
    longDescription:
      "A travel-oriented Apple accessory bundle built around carry protection and one cleaner visual language. The offer is locked behind the rewarded flow in native builds and then behaves like a normal cart line item.",
    priceCents: 10499,
    compareAtCents: 11899,
    currency: "usd",
    ratingValue: 4.9,
    ratingCount: 18,
    ships: "Native unlock offer. Ships on the same physical-product timeline after checkout.",
    imageUrl: WATCH_BAND_IMAGES[0]?.url || SHOP_PLACEHOLDER_IMAGE,
    images: [WATCH_BAND_IMAGES[0], AIRPODS_MAX_CASE_IMAGES[0], MACBOOK_CASE_IMAGES[0]].filter(Boolean),
    tab: "wearables",
    shippingClass: "pod",
    provider: "Quantura bundle offer",
    providerScore: 10,
    providerMethod: "Reward-unlocked native bundle",
    location: "Unlock on iOS and Android builds",
    productionTime: "Bundle unlock after rewarded interstitial",
    badge: "Reward bundle",
    highlights: [
      "Built for Apple-heavy carry setups across work, travel, and meetings.",
      "Reward unlock reveals the discounted bundle inside native builds.",
      "Keeps case, band, and headphone shell aligned as one accessory drop.",
    ],
    detailBullets: [
      "Bundle includes the Watch Band, AirPods Max Case, and MacBook Case line.",
      "Best fit for users who want coordinated Apple accessories with less checkout friction.",
      "Bundle CTA stays locked on web and unlocks through rewarded flow on native.",
      "The unlocked bundle can then be added to cart like any other item.",
    ],
    factGrid: [
      { label: "Bundle price", value: "$104.99" },
      { label: "Compare at", value: "$118.99" },
      { label: "Unlock", value: "Rewarded interstitial on native" },
      { label: "Includes", value: "Watch band + AirPods Max case + MacBook case" },
    ],
    rewardUnlockRequired: true,
    unlockCtaCopy: "Unlock bundle",
    bundleComponents: ["Quantura Watch Bands", "Quantura AirPods Max Cases", "Quantura MacBook Cases"],
  },
];

const CATALOG: ShopCatalogItem[] = [...PRODUCTS, ...BUNDLES];
const CATALOG_BY_SKU = new Map<string, ShopCatalogItem>();
CATALOG.forEach((item) => CATALOG_BY_SKU.set(item.sku, item));

const CATALOG_VISIBILITY_DEFAULTS = Object.freeze(
  Object.fromEntries(CATALOG.map((item) => [item.sku, true] as const))
) as Readonly<Record<string, boolean>>;

function sanitizeSku(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9-]/g, "")
    .slice(0, 64);
}

function toPublicCatalogItem(item: ShopCatalogItem): Record<string, unknown> {
  return {
    sku: item.sku,
    kind: item.kind,
    name: item.name,
    description: item.description,
    longDescription: item.longDescription,
    priceCents: item.priceCents,
    compareAtCents: item.compareAtCents || null,
    currency: item.currency,
    rating: {
      value: item.ratingValue,
      count: item.ratingCount,
    },
    ships: item.ships,
    imageUrl: item.imageUrl,
    images: item.images,
    placeholderImageUrl: SHOP_PLACEHOLDER_IMAGE,
    tab: item.tab,
    shippingClass: item.shippingClass,
    provider: item.provider,
    providerScore: item.providerScore,
    providerMethod: item.providerMethod || "",
    location: item.location || "",
    productionTime: item.productionTime || "",
    badge: item.badge || "",
    highlights: item.highlights,
    detailBullets: item.detailBullets,
    materials: item.materials || [],
    options: item.options || [],
    compliance: item.compliance || [],
    careInstructions: item.careInstructions || "",
    origin: item.origin || "",
    factGrid: item.factGrid,
    rewardUnlockRequired: Boolean(item.rewardUnlockRequired),
    unlockCtaCopy: item.unlockCtaCopy || "",
    bundleComponents: item.bundleComponents || [],
  };
}

export function getCatalogBySku(sku: unknown): ShopCatalogItem | null {
  const cleanSku = sanitizeSku(sku);
  if (!cleanSku) return null;
  return CATALOG_BY_SKU.get(cleanSku) || null;
}

export function getCatalogPublicItems(): Array<Record<string, unknown>> {
  return PRODUCTS.map(toPublicCatalogItem);
}

export function getCatalogPublicBundles(): Array<Record<string, unknown>> {
  return BUNDLES.map(toPublicCatalogItem);
}

export function getCatalogVisibilityDefaults(): { enabled: boolean; items: Record<string, boolean> } {
  return {
    enabled: true,
    items: { ...CATALOG_VISIBILITY_DEFAULTS },
  };
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
      label: SHOP_SHIPPING_POLICY.hardware.label,
      deliveryEstimate: { minBusinessDays: 5, maxBusinessDays: 10 },
      freeOverCents: SHOP_SHIPPING_POLICY.hardware.freeOverCents,
    };
  }
  return {
    label: SHOP_SHIPPING_POLICY.pod.label,
    deliveryEstimate: { minBusinessDays: 5, maxBusinessDays: 10 },
    freeOverCents: SHOP_SHIPPING_POLICY.pod.freeOverCents,
  };
}

export function getCatalogSize(): number {
  return CATALOG.length;
}
