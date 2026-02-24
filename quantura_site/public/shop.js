(() => {
  const root = document.getElementById("shop-root");
  if (!root) return;

  const platformNode = document.getElementById("shop-platform");
  const statusNode = document.getElementById("shop-status");
  const productsNode = document.getElementById("shop-products");

  const setStatus = (text, isError = false) => {
    if (!statusNode) return;
    statusNode.textContent = text;
    statusNode.classList.toggle("error", Boolean(isError));
  };

  const runtime = (() => {
    const cap = window.Capacitor;
    const platform = (() => {
      try {
        const p = String(cap?.getPlatform?.() || "").toLowerCase();
        if (p === "ios" || p === "android") return p;
      } catch {
        // Ignore platform check errors.
      }
      return null;
    })();
    const native = (() => {
      try {
        if (typeof cap?.isNativePlatform === "function") return Boolean(cap.isNativePlatform());
      } catch {
        // Ignore check errors.
      }
      return Boolean(platform);
    })();

    return {
      isNativeApp: native,
      platform,
      isInstalledPwa:
        window.matchMedia?.("(display-mode: standalone)")?.matches || Boolean(window.navigator.standalone),
    };
  })();

  const runtimeLabel = runtime.isNativeApp
    ? `Native (${runtime.platform || "unknown"})`
    : runtime.isInstalledPwa
      ? "Installed PWA"
      : "Web";
  if (platformNode) platformNode.textContent = runtimeLabel;

  const normalizeText = (value) => String(value ?? "").trim();

  const toCurrency = (amount, currency) => {
    const num = Number(amount);
    if (!Number.isFinite(num)) return "-";
    try {
      return new Intl.NumberFormat(undefined, {
        style: "currency",
        currency: String(currency || "USD").toUpperCase(),
      }).format(num / 100);
    } catch {
      return `${(num / 100).toFixed(2)} ${currency || "USD"}`;
    }
  };

  const isDigitalProduct = (product) => {
    const metadata = product?.metadata && typeof product.metadata === "object" ? product.metadata : {};
    const directFlag =
      metadata.isDigital === true ||
      metadata.is_digital === "true" ||
      String(metadata.productType || "").toLowerCase() === "digital" ||
      String(metadata.kind || "").toLowerCase() === "digital";
    return Boolean(directFlag);
  };

  const openNativeIap = ({ productId, priceId }) => {
    const detail = { productId, priceId };
    try {
      window.dispatchEvent(new CustomEvent("quantura:iap:open", { detail }));
    } catch {
      // Ignore event dispatch failures.
    }

    // iOS webkit bridge
    try {
      const handler = window.webkit?.messageHandlers?.quanturaIap;
      if (handler && typeof handler.postMessage === "function") {
        handler.postMessage({ action: "openPaywall", ...detail });
        return true;
      }
    } catch {
      // Ignore webkit bridge errors.
    }

    // Android bridge
    try {
      if (window.Android && typeof window.Android.openIapPaywall === "function") {
        window.Android.openIapPaywall(JSON.stringify(detail));
        return true;
      }
    } catch {
      // Ignore Android bridge errors.
    }

    return false;
  };

  const createStripeExtensionSession = async ({ uid, priceId, productId, mode }) => {
    const db = firebase.firestore();
    const checkoutRef = await db
      .collection("customers")
      .doc(uid)
      .collection("checkout_sessions")
      .add({
        price: priceId,
        mode: mode || "payment",
        allow_promotion_codes: true,
        success_url: `${window.location.origin}/shop?checkout=success&product=${encodeURIComponent(productId)}`,
        cancel_url: `${window.location.origin}/shop?checkout=cancel&product=${encodeURIComponent(productId)}`,
        metadata: {
          productId,
          source: "quantura_shop",
          runtime: runtime.isNativeApp ? runtime.platform || "native" : runtime.isInstalledPwa ? "pwa" : "web",
        },
      });

    return new Promise((resolve, reject) => {
      const unsubscribe = checkoutRef.onSnapshot(
        (snap) => {
          const data = snap.data();
          const err = data?.error;
          if (err) {
            unsubscribe();
            reject(new Error(normalizeText(err.message || err.code || "Checkout session failed.")));
            return;
          }

          const url = normalizeText(data?.url);
          if (url) {
            unsubscribe();
            resolve(url);
          }
        },
        (error) => {
          unsubscribe();
          reject(error);
        }
      );
    });
  };

  const loadProducts = async () => {
    if (!(window.firebase && firebase.firestore && firebase.auth)) {
      setStatus("Firebase SDK unavailable on this page.", true);
      return;
    }

    const db = firebase.firestore();
    const productsSnap = await db.collection("products").where("active", "==", true).get();
    const cards = [];

    for (const doc of productsSnap.docs) {
      const product = doc.data() || {};
      const pricesSnap = await doc.ref.collection("prices").where("active", "==", true).limit(5).get();
      const prices = pricesSnap.docs
        .map((priceDoc) => ({ id: priceDoc.id, ...(priceDoc.data() || {}) }))
        .filter((price) => price && Number.isFinite(Number(price.unit_amount)));

      if (!prices.length) continue;

      const defaultPrice = prices[0];
      const digital = isDigitalProduct(product);
      const isSubscription = String(defaultPrice.type || "").toLowerCase() === "recurring";

      cards.push({
        id: doc.id,
        name: normalizeText(product.name || doc.id),
        description: normalizeText(product.description || ""),
        active: Boolean(product.active),
        digital,
        isSubscription,
        prices,
        defaultPrice,
      });
    }

    if (!cards.length) {
      productsNode.innerHTML = '<div class="card"><div class="small muted">No active shop products found in Firestore.</div></div>';
      setStatus("No active products configured.");
      return;
    }

    productsNode.innerHTML = cards
      .map((card) => {
        const priceOptions = card.prices
          .map(
            (price) =>
              `<option value="${price.id}">${toCurrency(price.unit_amount, price.currency)}${
                String(price.type || "").toLowerCase() === "recurring" ? " / recurring" : ""
              }</option>`
          )
          .join("");

        return `
          <article class="card" data-shop-product="${card.id}">
            <h3>${card.name}</h3>
            <p class="small">${card.description || "No description."}</p>
            <div class="profile-item"><span class="label">Type</span><span class="value">${card.digital ? "Digital" : "Physical/Web"}</span></div>
            <div class="profile-item"><span class="label">Runtime lane</span><span class="value">${
              card.digital && runtime.isNativeApp ? "Native IAP" : "Stripe Checkout"
            }</span></div>
            <label class="label" for="shop-price-${card.id}" style="margin-top: 8px;">Price</label>
            <select id="shop-price-${card.id}" class="input" data-shop-price="${card.id}">
              ${priceOptions}
            </select>
            <div class="hero-actions" style="margin-top: 10px;">
              <button type="button" class="cta" data-shop-buy="${card.id}"><i class="iconoir-shopping-bag-check" aria-hidden="true"></i><span>Buy</span></button>
            </div>
          </article>
        `;
      })
      .join("");

    setStatus(`Loaded ${cards.length} active products.`);

    const byId = Object.fromEntries(cards.map((item) => [item.id, item]));

    productsNode.addEventListener("click", async (event) => {
      const buyButton = event.target.closest("[data-shop-buy]");
      if (!buyButton) return;

      const productId = String(buyButton.getAttribute("data-shop-buy") || "");
      const product = byId[productId];
      if (!product) return;

      const select = productsNode.querySelector(`[data-shop-price="${productId}"]`);
      const priceId = normalizeText(select?.value || product.defaultPrice?.id || "");
      if (!priceId) {
        setStatus("No active price found for this product.", true);
        return;
      }

      if (product.digital && runtime.isNativeApp) {
        const opened = openNativeIap({ productId, priceId });
        if (!opened) {
          setStatus("Native paywall bridge not found. Ensure wrapper exposes IAP bridge.", true);
        } else {
          setStatus("Opening native IAP paywall...");
        }
        return;
      }

      const user = firebase.auth().currentUser;
      if (!user) {
        setStatus("Sign in before checkout.", true);
        return;
      }

      buyButton.disabled = true;
      setStatus("Creating Stripe Checkout session...");

      try {
        const checkoutUrl = await createStripeExtensionSession({
          uid: user.uid,
          priceId,
          productId,
          mode: product.isSubscription ? "subscription" : "payment",
        });
        window.location.assign(checkoutUrl);
      } catch (error) {
        setStatus(error?.message || "Unable to start checkout.", true);
        buyButton.disabled = false;
      }
    });
  };

  const query = new URL(window.location.href).searchParams;
  const checkoutStatus = normalizeText(query.get("checkout"));
  if (checkoutStatus === "success") {
    setStatus("Checkout completed. Your order status will appear in dashboard shortly.");
  } else if (checkoutStatus === "cancel") {
    setStatus("Checkout canceled.", true);
  }

  const init = () => {
    if (!(window.firebase && firebase.auth)) {
      setStatus("Firebase auth unavailable.", true);
      return;
    }

    firebase.auth().onAuthStateChanged(async () => {
      try {
        await loadProducts();
      } catch (error) {
        setStatus(error?.message || "Failed to load shop products.", true);
      }
    });
  };

  init();
})();
