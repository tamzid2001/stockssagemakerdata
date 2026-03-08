(() => {
  const CART_STORAGE_KEY = "quantura_shop_cart_v1";
  const EMAIL_STORAGE_KEY = "quantura_shop_email_v1";

  const dom = {
    grid: document.getElementById("shop-grid"),
    message: document.getElementById("shop-message"),
    tabs: Array.from(document.querySelectorAll("[data-filter]")),
    search: document.getElementById("shop-search"),
    cartToggle: document.getElementById("cart-toggle"),
    cartClose: document.getElementById("cart-close"),
    cartDrawer: document.getElementById("cart-drawer"),
    cartOverlay: document.getElementById("cart-overlay"),
    cartCount: document.getElementById("cart-count"),
    cartItems: document.getElementById("cart-items"),
    subtotal: document.getElementById("subtotal-value"),
    shipping: document.getElementById("shipping-value"),
    total: document.getElementById("total-value"),
    shippingNote: document.getElementById("shipping-note"),
    checkoutButton: document.getElementById("checkout-button"),
    portalButton: document.getElementById("portal-button"),
    email: document.getElementById("checkout-email"),
  };

  if (!dom.grid) return;

  const state = {
    catalogProducts: [],
    products: [],
    hiddenProducts: [],
    shippingPolicy: null,
    filter: "all",
    query: "",
    loading: true,
    cart: readStoredCart(),
    email: readStoredEmail(),
    visibilityConfig: {
      enabled: true,
      items: {},
    },
  };

  const money = (cents, currency = "USD") => {
    const value = Number(cents);
    if (!Number.isFinite(value)) return "$0.00";
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: String(currency || "USD").toUpperCase(),
    }).format(value / 100);
  };

  const showMessage = (text, type = "info") => {
    if (!dom.message) return;
    if (!text) {
      dom.message.textContent = "";
      dom.message.classList.add("hidden");
      dom.message.classList.remove("warn", "success");
      return;
    }
    dom.message.textContent = text;
    dom.message.classList.remove("hidden", "warn", "success");
    if (type === "warn") dom.message.classList.add("warn");
    if (type === "success") dom.message.classList.add("success");
  };

  function readStoredCart() {
    try {
      const parsed = JSON.parse(localStorage.getItem(CART_STORAGE_KEY) || "{}");
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
      const safe = {};
      Object.entries(parsed).forEach(([sku, qtyRaw]) => {
        const qty = Number(qtyRaw);
        if (!Number.isFinite(qty)) return;
        safe[String(sku)] = Math.max(1, Math.min(10, Math.floor(qty)));
      });
      return safe;
    } catch {
      return {};
    }
  }

  function persistCart() {
    localStorage.setItem(CART_STORAGE_KEY, JSON.stringify(state.cart));
  }

  function readStoredEmail() {
    return String(localStorage.getItem(EMAIL_STORAGE_KEY) || "").trim();
  }

  function persistEmail(value) {
    localStorage.setItem(EMAIL_STORAGE_KEY, String(value || "").trim());
  }

  function filteredProducts() {
    const query = String(state.query || "").trim().toLowerCase();
    return state.products.filter((product) => {
      if (state.filter !== "all" && product.tab !== state.filter) return false;
      if (!query) return true;
      const haystack = `${product.name} ${product.description}`.toLowerCase();
      return haystack.includes(query);
    });
  }

  function filteredHiddenProducts() {
    if (!state.visibilityConfig.enabled) return [];
    const query = String(state.query || "").trim().toLowerCase();
    return state.hiddenProducts.filter((product) => {
      if (state.filter !== "all" && product.tab !== state.filter) return false;
      if (!query) return true;
      const haystack = `${product.name} ${product.description}`.toLowerCase();
      return haystack.includes(query);
    });
  }

  function productBySku(sku) {
    return state.products.find((item) => item.sku === sku) || null;
  }

  function cartEntries() {
    return Object.entries(state.cart)
      .map(([sku, qty]) => ({ sku, qty: Number(qty), product: productBySku(sku) }))
      .filter((row) => row.product && Number.isFinite(row.qty) && row.qty > 0);
  }

  function computeTotals() {
    const rows = cartEntries();
    const subtotal = rows.reduce((sum, row) => sum + row.product.priceCents * row.qty, 0);
    const hasHardware = rows.some((row) => row.product.shippingClass === "hardware");
    const policy = hasHardware ? state.shippingPolicy?.hardware : state.shippingPolicy?.pod;
    const shipping = policy
      ? subtotal >= Number(policy.freeOverCents || 0)
        ? 0
        : Number(policy.flatRateCents || 0)
      : 0;
    return {
      rows,
      subtotal,
      shipping,
      total: subtotal + shipping,
      hasHardware,
      shippingPolicy: policy || null,
    };
  }

  function buildDefaultVisibility(products, explicitDefault) {
    const items = {};
    (Array.isArray(products) ? products : []).forEach((product) => {
      if (!product?.sku) return;
      items[String(product.sku)] = true;
    });
    if (explicitDefault && typeof explicitDefault === "object" && explicitDefault.items && typeof explicitDefault.items === "object") {
      Object.entries(explicitDefault.items).forEach(([sku, value]) => {
        if (typeof value !== "boolean") return;
        items[String(sku)] = value;
      });
    }
    const enabledDefault =
      explicitDefault && typeof explicitDefault === "object" && typeof explicitDefault.enabled === "boolean"
        ? explicitDefault.enabled
        : true;
    return {
      enabled: enabledDefault,
      items,
    };
  }

  function normalizeVisibilityConfig(raw, fallback) {
    const base = {
      enabled: typeof fallback?.enabled === "boolean" ? fallback.enabled : true,
      items: { ...(fallback?.items || {}) },
    };

    if (raw == null) return base;

    let candidate = raw;
    if (typeof candidate === "string") {
      const trimmed = candidate.trim();
      if (!trimmed) return base;
      try {
        candidate = JSON.parse(trimmed);
      } catch (error) {
        console.warn("[Shop][RC] Invalid JSON in shop_visibility_config. Falling back to defaults.");
        return base;
      }
    }

    if (!candidate || typeof candidate !== "object") return base;

    if (typeof candidate.enabled === "boolean") {
      base.enabled = candidate.enabled;
    }

    if (candidate.items && typeof candidate.items === "object") {
      Object.entries(candidate.items).forEach(([sku, value]) => {
        if (typeof value === "boolean") {
          base.items[String(sku)] = value;
        }
      });
    }

    return base;
  }

  async function resolveShopVisibility(defaultConfig) {
    const fallback = normalizeVisibilityConfig(defaultConfig, defaultConfig);
    const firebase = window.firebase;

    if (!firebase?.remoteConfig) {
      console.info("[Shop][RC] Firebase Remote Config unavailable. Using fallback visibility config.");
      return fallback;
    }

    try {
      firebase.app();
    } catch (_error) {
      console.info("[Shop][RC] Firebase app unavailable. Using fallback visibility config.");
      return fallback;
    }

    try {
      const rc = firebase.remoteConfig();
      if (!rc) return fallback;

      const isDevelopmentHost =
        window.location.hostname === "localhost" ||
        window.location.hostname === "127.0.0.1" ||
        window.location.hostname.endsWith(".web.app") ||
        window.location.hostname.endsWith(".firebaseapp.com");

      rc.settings = {
        ...(rc.settings || {}),
        minimumFetchIntervalMillis: isDevelopmentHost ? 60 * 1000 : 15 * 60 * 1000,
        fetchTimeoutMillis: 10000,
      };
      rc.defaultConfig = {
        ...(rc.defaultConfig || {}),
        shop_visibility_config: JSON.stringify(fallback),
      };

      console.info("[Shop][RC] Fetching Remote Config visibility...");
      const activated = await rc.fetchAndActivate();
      const value = rc.getValue("shop_visibility_config");
      const rawValue = typeof value?.asString === "function" ? value.asString() : "";
      const normalized = normalizeVisibilityConfig(rawValue, fallback);
      const hiddenCount = Object.values(normalized.items || {}).filter((flag) => flag === false).length;
      console.info("[Shop][RC] Visibility fetched", {
        activated: Boolean(activated),
        enabled: normalized.enabled,
        hiddenCount,
      });
      return normalized;
    } catch (error) {
      console.warn("[Shop][RC] Failed to fetch visibility config. Using fallback.", error);
      return fallback;
    }
  }

  function applyVisibilityConfig(config) {
    const normalized = normalizeVisibilityConfig(config, buildDefaultVisibility(state.catalogProducts));
    state.visibilityConfig = normalized;

    if (!normalized.enabled) {
      state.products = [];
      state.hiddenProducts = Array.isArray(state.catalogProducts) ? [...state.catalogProducts] : [];
    } else {
      state.products = state.catalogProducts.filter((product) => normalized.items[String(product.sku)] !== false);
      state.hiddenProducts = state.catalogProducts.filter((product) => normalized.items[String(product.sku)] === false);
    }

    const visibleSkus = new Set(state.products.map((product) => String(product.sku)));
    let removedCount = 0;
    Object.keys(state.cart).forEach((sku) => {
      if (!visibleSkus.has(String(sku))) {
        delete state.cart[sku];
        removedCount += 1;
      }
    });
    if (removedCount > 0) {
      persistCart();
      showMessage("Some unavailable items were removed from your cart.", "warn");
    }

    console.info("[Shop] Visibility applied", {
      enabled: normalized.enabled,
      visibleCount: state.products.length,
      hiddenCount: state.hiddenProducts.length,
    });
  }

  function renderProducts() {
    if (state.loading) return;

    const rows = filteredProducts();
    const hiddenRows = filteredHiddenProducts();

    if (!state.visibilityConfig.enabled) {
      dom.grid.innerHTML = `
        <article class="shop-coming-soon-card" role="status" aria-live="polite">
          <p class="eyebrow">Quantura Shop</p>
          <h2>Coming soon</h2>
          <p>We’re preparing the next drop for Quantura Shop. Check back shortly for new inventory.</p>
        </article>
      `;
      return;
    }

    if (!rows.length && !hiddenRows.length) {
      dom.grid.innerHTML = '<div class="shop-message">No matching products for this filter.</div>';
      return;
    }

    const visibleMarkup = rows.map((product) => {
        const fullStars = Math.floor(Number(product.rating?.value || 0));
        const stars = "★".repeat(Math.max(0, Math.min(5, fullStars))).padEnd(5, "☆");
        return `
          <article class="product-card" data-sku="${escapeHtml(product.sku)}">
            <img
              class="product-image"
              src="${escapeHtml(product.imageUrl)}"
              alt="${escapeHtml(product.name)}"
              loading="lazy"
              onerror="this.onerror=null;this.src='${escapeHtml(product.placeholderImageUrl || "/assets/shop/placeholder.png")}';"
            />
            <div class="product-body">
              <h2 class="product-name">${escapeHtml(product.name)}</h2>
              <p class="product-desc">${escapeHtml(product.description)}</p>
              <div class="product-rating">
                <span class="product-stars" aria-hidden="true">${stars}</span>
                <span>${Number(product.rating?.value || 0).toFixed(1)} (${Number(product.rating?.count || 0)})</span>
              </div>
              <div class="product-price">${money(product.priceCents, product.currency)}</div>
              <p class="product-shipping">${escapeHtml(product.ships)}</p>
              <div class="card-actions">
                <button type="button" class="cta" data-action="add" data-sku="${escapeHtml(product.sku)}">Add to cart</button>
              </div>
            </div>
          </article>
        `;
      });

    const hiddenMarkup = hiddenRows.map((product) => {
      const fullStars = Math.floor(Number(product.rating?.value || 0));
      const stars = "★".repeat(Math.max(0, Math.min(5, fullStars))).padEnd(5, "☆");
      return `
        <article class="product-card product-card-coming-soon" data-sku="${escapeHtml(product.sku)}">
          <img
            class="product-image"
            src="${escapeHtml(product.imageUrl)}"
            alt="${escapeHtml(product.name)}"
            loading="lazy"
            onerror="this.onerror=null;this.src='${escapeHtml(product.placeholderImageUrl || "/assets/shop/placeholder.png")}';"
          />
          <div class="product-body">
            <h2 class="product-name">${escapeHtml(product.name)}</h2>
            <p class="product-desc">${escapeHtml(product.description)}</p>
            <div class="product-rating">
              <span class="product-stars" aria-hidden="true">${stars}</span>
              <span>${Number(product.rating?.value || 0).toFixed(1)} (${Number(product.rating?.count || 0)})</span>
            </div>
            <div class="coming-soon-pill">Coming soon</div>
            <p class="product-shipping">This product is temporarily unavailable.</p>
            <div class="card-actions">
              <button type="button" class="cta secondary" disabled>Coming soon</button>
            </div>
          </div>
        </article>
      `;
    });

    dom.grid.innerHTML = visibleMarkup.concat(hiddenMarkup).join("");
  }

  function renderCart() {
    const totals = computeTotals();

    if (dom.cartCount) {
      const count = totals.rows.reduce((sum, row) => sum + row.qty, 0);
      dom.cartCount.textContent = String(count);
    }

    if (dom.cartItems) {
      if (!totals.rows.length) {
        dom.cartItems.innerHTML = '<p class="muted">Your cart is empty.</p>';
      } else {
        dom.cartItems.innerHTML = totals.rows
          .map(
            (row) => `
              <article class="cart-item" data-cart-sku="${escapeHtml(row.sku)}">
                <div class="cart-item-top">
                  <div>
                    <h3>${escapeHtml(row.product.name)}</h3>
                    <div class="cart-item-price">${money(row.product.priceCents, row.product.currency)} each</div>
                  </div>
                  <button class="ghost" type="button" data-action="remove" data-sku="${escapeHtml(row.sku)}">Remove</button>
                </div>
                <div class="qty-row">
                  <button class="qty-btn" type="button" data-action="decrease" data-sku="${escapeHtml(row.sku)}" aria-label="Decrease quantity">-</button>
                  <span class="qty-value">${row.qty}</span>
                  <button class="qty-btn" type="button" data-action="increase" data-sku="${escapeHtml(row.sku)}" aria-label="Increase quantity">+</button>
                </div>
              </article>
            `
          )
          .join("");
      }
    }

    if (dom.subtotal) dom.subtotal.textContent = money(totals.subtotal);
    if (dom.shipping) dom.shipping.textContent = money(totals.shipping);
    if (dom.total) dom.total.textContent = money(totals.total);

    if (dom.shippingNote) {
      if (!totals.rows.length) {
        dom.shippingNote.textContent = "Shipping cost is finalized at checkout after address selection.";
      } else if (!totals.shippingPolicy) {
        dom.shippingNote.textContent = "Shipping estimate unavailable.";
      } else {
        const freeOver = Number(totals.shippingPolicy.freeOverCents || 0);
        const thresholdText = `Free over ${money(freeOver)}.`;
        dom.shippingNote.textContent = `${totals.shippingPolicy.estimate || ""} ${thresholdText}`.trim();
      }
    }

    if (dom.checkoutButton) dom.checkoutButton.disabled = !totals.rows.length;
    if (dom.portalButton) dom.portalButton.disabled = false;
  }

  function setFilter(nextFilter) {
    state.filter = nextFilter;
    dom.tabs.forEach((tab) => {
      const active = String(tab.dataset.filter || "") === nextFilter;
      tab.classList.toggle("active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
    });
    renderProducts();
  }

  function openCartDrawer() {
    document.body.classList.add("cart-open");
    if (dom.cartOverlay) dom.cartOverlay.hidden = false;
    if (dom.cartToggle) dom.cartToggle.setAttribute("aria-expanded", "true");
  }

  function closeCartDrawer() {
    document.body.classList.remove("cart-open");
    if (dom.cartOverlay) dom.cartOverlay.hidden = true;
    if (dom.cartToggle) dom.cartToggle.setAttribute("aria-expanded", "false");
  }

  function updateCartSku(sku, nextQty) {
    const qty = Math.max(0, Math.min(10, Math.floor(Number(nextQty) || 0)));
    if (qty <= 0) {
      delete state.cart[sku];
    } else {
      state.cart[sku] = qty;
    }
    persistCart();
    renderCart();
  }

  function addToCart(sku) {
    if (!productBySku(sku)) {
      showMessage("This product is not available right now.", "warn");
      return;
    }
    const existing = Number(state.cart[sku] || 0);
    updateCartSku(sku, Math.min(10, existing + 1));
    showMessage("Added to cart.", "success");
  }

  async function fetchCatalog() {
    state.loading = true;
    showMessage("");

    try {
      const response = await fetch("/api/shop/catalog", {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) throw new Error("Unable to load products.");

      const payload = await response.json();
      state.catalogProducts = Array.isArray(payload.products) ? payload.products : [];
      state.shippingPolicy = payload.shippingPolicy || null;
      const defaultVisibility = buildDefaultVisibility(state.catalogProducts, payload.visibilityConfig || null);
      const remoteVisibility = await resolveShopVisibility(defaultVisibility);
      applyVisibilityConfig(remoteVisibility);
      state.loading = false;
      renderProducts();
      renderCart();
    } catch (error) {
      state.loading = false;
      dom.grid.innerHTML = '<div class="shop-message warn">Unable to load the product catalog right now.</div>';
      showMessage(error.message || "Unable to load catalog.", "warn");
    }
  }

  async function handleCheckout() {
    const totals = computeTotals();
    if (!totals.rows.length) {
      showMessage("Add at least one item before checkout.", "warn");
      return;
    }

    const email = String(dom.email?.value || "").trim();
    if (email && !isValidEmail(email)) {
      showMessage("Enter a valid email address or leave it blank.", "warn");
      return;
    }

    persistEmail(email);

    const payload = {
      items: totals.rows.map((row) => ({
        sku: row.sku,
        qty: row.qty,
      })),
      user: {
        email,
      },
    };

    if (dom.checkoutButton) {
      dom.checkoutButton.disabled = true;
      dom.checkoutButton.textContent = "Redirecting...";
    }

    try {
      const response = await fetch("/api/shop/checkout", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data.url) {
        throw new Error(String(data.message || data.error || "Unable to start checkout."));
      }

      window.location.assign(String(data.url));
    } catch (error) {
      showMessage(error.message || "Unable to start checkout.", "warn");
      if (dom.checkoutButton) {
        dom.checkoutButton.disabled = false;
        dom.checkoutButton.textContent = "Checkout";
      }
    }
  }

  async function handleBillingPortal() {
    const email = String(dom.email?.value || "").trim();
    if (!isValidEmail(email)) {
      showMessage("Enter your checkout email to open billing portal.", "warn");
      return;
    }

    persistEmail(email);

    if (dom.portalButton) {
      dom.portalButton.disabled = true;
      dom.portalButton.textContent = "Opening...";
    }

    try {
      const response = await fetch("/api/shop/portal", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          email,
          returnUrl: `${window.location.origin}/shop`,
        }),
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data.url) {
        throw new Error(String(data.message || data.error || "Unable to open billing portal."));
      }

      window.location.assign(String(data.url));
    } catch (error) {
      showMessage(error.message || "Unable to open billing portal.", "warn");
      if (dom.portalButton) {
        dom.portalButton.disabled = false;
        dom.portalButton.textContent = "Open billing portal";
      }
    }
  }

  function bindEvents() {
    dom.tabs.forEach((tab) => {
      tab.addEventListener("click", () => {
        setFilter(String(tab.dataset.filter || "all"));
      });
    });

    dom.search?.addEventListener("input", () => {
      state.query = String(dom.search.value || "").trim();
      renderProducts();
    });

    dom.grid.addEventListener("click", (event) => {
      const target = event.target.closest("[data-action]");
      if (!target) return;
      const action = String(target.dataset.action || "");
      const sku = String(target.dataset.sku || "").trim();
      if (!sku) return;

      if (action === "add") addToCart(sku);
    });

    dom.cartItems?.addEventListener("click", (event) => {
      const target = event.target.closest("[data-action]");
      if (!target) return;
      const action = String(target.dataset.action || "");
      const sku = String(target.dataset.sku || "").trim();
      if (!sku) return;

      const current = Number(state.cart[sku] || 0);
      if (action === "increase") updateCartSku(sku, current + 1);
      if (action === "decrease") updateCartSku(sku, current - 1);
      if (action === "remove") updateCartSku(sku, 0);
    });

    dom.cartToggle?.addEventListener("click", () => {
      const open = document.body.classList.contains("cart-open");
      if (open) closeCartDrawer();
      else openCartDrawer();
    });

    dom.cartClose?.addEventListener("click", closeCartDrawer);
    dom.cartOverlay?.addEventListener("click", closeCartDrawer);

    dom.checkoutButton?.addEventListener("click", handleCheckout);
    dom.portalButton?.addEventListener("click", handleBillingPortal);

    dom.email?.addEventListener("change", () => {
      persistEmail(String(dom.email.value || "").trim());
    });
  }

  function hydrateFromQuery() {
    const params = new URLSearchParams(window.location.search);
    if (params.get("canceled") === "1") {
      showMessage("Checkout canceled. Your cart is still saved.", "warn");
    }
  }

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function isValidEmail(value) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || "").trim());
  }

  function bindMobileBottomNav() {
    const normalizePath = (value) => {
      const cleaned = String(value || "/").split("?")[0].split("#")[0].trim() || "/";
      const normalized = cleaned.startsWith("/") ? cleaned : `/${cleaned}`;
      return normalized.length > 1 ? normalized.replace(/\/+$/, "") : normalized;
    };
    const path = normalizePath(window.location.pathname || "/shop");
    const links = [
      { href: "/explore", label: "Explore", icon: "iconoir-binocular" },
      { href: "/research", label: "Research", icon: "iconoir-bookmark-book" },
      { href: "/pricing", label: "Pricing", icon: "iconoir-wallet" },
      { href: "/shop", label: "Shop", icon: "iconoir-shop" },
      { href: "/contact", label: "Contact", icon: "iconoir-mail" },
    ];

    let nav = document.getElementById("mobile-bottom-nav");
    if (!nav) {
      nav = document.createElement("nav");
      nav.id = "mobile-bottom-nav";
      nav.className = "mobile-bottom-nav hidden";
      nav.setAttribute("aria-label", "Mobile navigation");
      nav.innerHTML = '<div class="mobile-bottom-nav-inner"></div>';
      document.body.appendChild(nav);
    }
    const inner = nav.querySelector(".mobile-bottom-nav-inner");
    if (!inner) return;

    inner.innerHTML = links
      .map((entry) => {
        const active = normalizePath(entry.href) === path ? " active" : "";
        return `
          <a class="mobile-bottom-link${active}" href="${escapeHtml(entry.href)}" aria-label="${escapeHtml(entry.label)}">
            <i class="${escapeHtml(entry.icon)}" aria-hidden="true"></i>
            <span class="mobile-bottom-label">${escapeHtml(entry.label)}</span>
          </a>
        `;
      })
      .join("");

    const syncVisibility = () => {
      const visible = window.innerWidth <= 980;
      nav.classList.toggle("hidden", !visible);
      document.body.classList.toggle("mobile-bottom-nav-enabled", visible);
    };
    syncVisibility();
    window.addEventListener("resize", syncVisibility);
  }

  function init() {
    if (dom.email && state.email) dom.email.value = state.email;
    bindEvents();
    hydrateFromQuery();
    bindMobileBottomNav();
    renderCart();
    fetchCatalog();
  }

  init();
})();
