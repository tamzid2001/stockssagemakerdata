(function () {
  const STORAGE_KEYS = {
    cart: "quantura_shop_cart_v2",
    email: "quantura_shop_email_v2",
    unlockedBundles: "quantura_shop_unlocked_bundles_v1",
  };

  const TAB_LABELS = {
    all: "All gear",
    cases: "Cases",
    audio: "Audio",
    workspace: "Workspace",
    wearables: "Wearables",
  };

  const state = {
    products: [],
    bundles: [],
    visibleProducts: [],
    visibleBundles: [],
    catalogBySku: new Map(),
    visibility: { enabled: true, items: {} },
    shippingPolicy: null,
    activeFilter: "all",
    query: "",
    cart: loadJson(STORAGE_KEYS.cart, {}),
    email: loadText(STORAGE_KEYS.email),
    unlockedBundles: loadJson(STORAGE_KEYS.unlockedBundles, {}),
    carousel: {},
    loading: true,
    shopEnabled: true,
    rewardBusySku: "",
    checkoutBusy: false,
    portalBusy: false,
    isNative: false,
    adFreeEntitlement: false,
    messageTimer: 0,
  };

  const ui = {
    message: document.getElementById("shop-message"),
    productCount: document.getElementById("product-count"),
    bundleCount: document.getElementById("bundle-count"),
    visibilitySummary: document.getElementById("visibility-summary"),
    catalogSummary: document.getElementById("catalog-summary"),
    bundleStatus: document.getElementById("bundle-status"),
    shopGrid: document.getElementById("shop-grid"),
    bundleGrid: document.getElementById("bundle-grid"),
    cartToggle: document.getElementById("cart-toggle"),
    cartClose: document.getElementById("cart-close"),
    cartDrawer: document.getElementById("cart-drawer"),
    cartOverlay: document.getElementById("cart-overlay"),
    cartItems: document.getElementById("cart-items"),
    cartCount: document.getElementById("cart-count"),
    subtotalValue: document.getElementById("subtotal-value"),
    shippingValue: document.getElementById("shipping-value"),
    totalValue: document.getElementById("total-value"),
    shippingNote: document.getElementById("shipping-note"),
    emailInput: document.getElementById("checkout-email"),
    checkoutButton: document.getElementById("checkout-button"),
    portalButton: document.getElementById("portal-button"),
    searchInput: document.getElementById("shop-search"),
    tabs: Array.from(document.querySelectorAll(".tab[data-filter]")),
    policyPodCopy: document.getElementById("policy-pod-copy"),
    policyHardwareCopy: document.getElementById("policy-hardware-copy"),
    policyReturnsCopy: document.getElementById("policy-returns-copy"),
  };

  if (!ui.shopGrid || !ui.bundleGrid || !ui.cartItems) return;

  bootstrap().catch((error) => {
    console.error("[shop] bootstrap failed", error);
    state.loading = false;
    showMessage("Unable to load the shop right now.", "warn", 0);
    render();
  });

  async function bootstrap() {
    hydrateRuntimeBridge();
    bindEvents();
    if (ui.emailInput) ui.emailInput.value = state.email;
    handlePageFlags();
    await loadCatalog();
    render();
  }

  function hydrateRuntimeBridge() {
    const bridge = window.QuanturaShopRuntime || null;
    state.isNative = Boolean(bridge && bridge.isNativeApp);
    state.adFreeEntitlement = false;
    if (bridge && typeof bridge.hasAdFreeEntitlement === "function") {
      try {
        state.adFreeEntitlement = Boolean(bridge.hasAdFreeEntitlement());
      } catch (_error) {
        state.adFreeEntitlement = false;
      }
    }
  }

  function bindEvents() {
    document.addEventListener("click", onDocumentClick);
    document.addEventListener("keydown", onDocumentKeydown);
    if (ui.searchInput) {
      ui.searchInput.addEventListener("input", (event) => {
        state.query = String(event.target.value || "").trim();
        renderCatalogSections();
      });
    }
    if (ui.emailInput) {
      ui.emailInput.addEventListener("input", (event) => {
        state.email = String(event.target.value || "").trim();
        localStorage.setItem(STORAGE_KEYS.email, state.email);
      });
    }
    if (ui.checkoutButton) {
      ui.checkoutButton.addEventListener("click", startCheckout);
    }
    if (ui.portalButton) {
      ui.portalButton.addEventListener("click", openBillingPortal);
    }
  }

  function onDocumentClick(event) {
    const actionNode = event.target.closest("[data-action]");
    if (actionNode) {
      const action = String(actionNode.getAttribute("data-action") || "");
      const sku = String(actionNode.getAttribute("data-sku") || "").trim().toUpperCase();
      if (action === "add-to-cart" && sku) {
        addToCart(sku, 1);
        return;
      }
      if (action === "remove-item" && sku) {
        removeFromCart(sku);
        return;
      }
      if (action === "qty-up" && sku) {
        updateCartQty(sku, getCartQty(sku) + 1);
        return;
      }
      if (action === "qty-down" && sku) {
        updateCartQty(sku, getCartQty(sku) - 1);
        return;
      }
      if (action === "open-cart") {
        setCartOpen(true);
        return;
      }
      if (action === "unlock-bundle" && sku) {
        unlockBundle(sku);
        return;
      }
      if (action === "carousel-prev" && sku) {
        advanceCarousel(sku, -1);
        return;
      }
      if (action === "carousel-next" && sku) {
        advanceCarousel(sku, 1);
        return;
      }
      if (action === "carousel-goto" && sku) {
        const index = Number(actionNode.getAttribute("data-index") || 0);
        setCarouselIndex(sku, index);
        return;
      }
    }

    if (event.target === ui.cartOverlay) {
      setCartOpen(false);
      return;
    }

    if (event.target === ui.cartToggle) {
      const isOpen = document.body.classList.contains("cart-open");
      setCartOpen(!isOpen);
      return;
    }

    if (event.target === ui.cartClose) {
      setCartOpen(false);
      return;
    }

    const tabNode = event.target.closest(".tab[data-filter]");
    if (tabNode) {
      const nextFilter = String(tabNode.getAttribute("data-filter") || "all");
      if (state.activeFilter !== nextFilter) {
        state.activeFilter = nextFilter;
        renderCatalogSections();
      }
    }
  }

  function onDocumentKeydown(event) {
    if (event.key === "Escape") {
      setCartOpen(false);
    }
  }

  function handlePageFlags() {
    const params = new URLSearchParams(window.location.search);
    if (params.get("canceled") === "1") {
      showMessage("Checkout was canceled. Your cart is still here.", "warn");
    }
  }

  async function loadCatalog() {
    state.loading = true;
    render();
    const response = await fetch("/api/shop/catalog", {
      credentials: "same-origin",
      headers: {
        accept: "application/json",
      },
    });
    if (!response.ok) {
      throw new Error(`catalog_request_failed_${response.status}`);
    }

    const payload = await response.json();
    const defaultVisibility = sanitizeVisibilityConfig(payload.visibilityConfig);
    state.visibility = await resolveShopVisibility(defaultVisibility);
    state.shopEnabled = state.visibility.enabled !== false;
    state.shippingPolicy = sanitizeShippingPolicy(payload.shippingPolicy);

    const allProducts = Array.isArray(payload.products) ? payload.products.map(normalizeItem).filter(Boolean) : [];
    const allBundles = Array.isArray(payload.bundles) ? payload.bundles.map(normalizeItem).filter(Boolean) : [];

    state.catalogBySku = new Map();
    allProducts.concat(allBundles).forEach((item) => {
      state.catalogBySku.set(item.sku, item);
      if (!(item.sku in state.carousel)) state.carousel[item.sku] = 0;
    });

    if (state.adFreeEntitlement) {
      allBundles.forEach((item) => {
        if (item.rewardUnlockRequired) {
          state.unlockedBundles[item.sku] = true;
        }
      });
      persistUnlockedBundles();
    }

    state.products = allProducts;
    state.bundles = allBundles;
    syncVisibleCatalog();
    pruneCart();
    syncPolicyCopy();
    state.loading = false;
  }

  async function resolveShopVisibility(defaultConfig) {
    const fallback = sanitizeVisibilityConfig(defaultConfig);
    try {
      await waitForFirebase(2600);
      if (!window.firebase || typeof window.firebase.remoteConfig !== "function") {
        return fallback;
      }
      const remoteConfig = window.firebase.remoteConfig();
      remoteConfig.settings = Object.assign({}, remoteConfig.settings || {}, {
        minimumFetchIntervalMillis: 60 * 1000,
      });
      remoteConfig.defaultConfig = Object.assign({}, remoteConfig.defaultConfig || {}, {
        shop_visibility_config: JSON.stringify(fallback),
        shop_enabled: true,
        shop_section_enabled: true,
        shop_store_enabled: true,
      });
      try {
        await remoteConfig.fetchAndActivate();
      } catch (error) {
        console.warn("[shop] Remote Config fetch failed, using cached/default values.", error);
      }
      const remoteJson = parseJson(remoteConfig.getString("shop_visibility_config"));
      const merged = sanitizeVisibilityConfig(remoteJson, fallback);
      merged.enabled =
        merged.enabled !== false &&
        remoteConfig.getBoolean("shop_enabled") &&
        remoteConfig.getBoolean("shop_section_enabled") &&
        remoteConfig.getBoolean("shop_store_enabled");
      return merged;
    } catch (error) {
      console.warn("[shop] Remote Config unavailable, using defaults.", error);
      return fallback;
    }
  }

  async function waitForFirebase(timeoutMs) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (window.firebase && typeof window.firebase.remoteConfig === "function") {
        if (Array.isArray(window.firebase.apps) && window.firebase.apps.length) {
          return true;
        }
      }
      await sleep(120);
    }
    return false;
  }

  function syncVisibleCatalog() {
    state.visibleProducts = state.products.filter((item) => isSkuVisible(item.sku));
    state.visibleBundles = state.bundles.filter((item) => isSkuVisible(item.sku));
  }

  function syncPolicyCopy() {
    if (!state.shippingPolicy) return;
    const pod = state.shippingPolicy.pod || {};
    const hardware = state.shippingPolicy.hardware || {};
    const returnsCopy = state.shippingPolicy.returns || {};

    if (ui.policyPodCopy) {
      ui.policyPodCopy.textContent = [
        `Standard shipping: ${formatMoney(pod.flatRateCents || 0)} flat, free over ${formatMoney(pod.freeOverCents || 0)}.`,
        pod.estimate || "",
      ]
        .join(" ")
        .trim();
    }
    if (ui.policyHardwareCopy) {
      ui.policyHardwareCopy.textContent = [
        hardware.detail || hardware.estimate || "",
        hardware.flatRateCents != null ? `Checkout will confirm the final carrier rate before payment.` : "",
      ]
        .join(" ")
        .trim();
    }
    if (ui.policyReturnsCopy) {
      ui.policyReturnsCopy.textContent = returnsCopy.pod || "Contact Quantura support for return and defect handling.";
    }
  }

  function render() {
    renderSummary();
    renderCatalogSections();
    renderCart();
  }

  function renderSummary() {
    const totalVisible = state.visibleProducts.length + state.visibleBundles.length;
    const filteredProducts = getFilteredItems(state.visibleProducts);
    const filteredBundles = getFilteredItems(state.visibleBundles);
    if (ui.productCount) {
      ui.productCount.textContent = `${state.visibleProducts.length} products`;
    }
    if (ui.bundleCount) {
      ui.bundleCount.textContent = `${state.visibleBundles.length} reward bundles`;
    }
    if (ui.visibilitySummary) {
      if (!state.shopEnabled) {
        ui.visibilitySummary.textContent = "Shop is currently hidden by Firebase Remote Config.";
      } else {
        ui.visibilitySummary.textContent = `${totalVisible} live listings are currently enabled by Firebase Remote Config.`;
      }
    }
    if (ui.catalogSummary) {
      if (!state.shopEnabled) {
        ui.catalogSummary.textContent =
          "The shop is disabled right now. Flip the Firebase Remote Config shop flag back on to restore the catalog.";
      } else {
        ui.catalogSummary.textContent = `${filteredProducts.length} product results and ${filteredBundles.length} bundle results match the active view.`;
      }
    }
    if (ui.bundleStatus) {
      if (!state.visibleBundles.length) {
        ui.bundleStatus.textContent = "No active bundles";
      } else if (state.adFreeEntitlement) {
        ui.bundleStatus.textContent = "Ad-free entitlement: bundles unlocked";
      } else if (state.isNative) {
        const unlockedCount = state.visibleBundles.filter((item) => isBundleUnlocked(item)).length;
        ui.bundleStatus.textContent =
          unlockedCount > 0
            ? `${unlockedCount} bundle${unlockedCount === 1 ? "" : "s"} unlocked on this device`
            : "Reward unlock ready on this device";
      } else {
        ui.bundleStatus.textContent = "Visible on web, unlock in Quantura iOS/Android";
      }
    }
  }

  function renderCatalogSections() {
    renderProducts();
    renderBundles();
    updateTabState();
  }

  function renderProducts() {
    if (!ui.shopGrid) return;
    if (state.loading) {
      return;
    }
    if (!state.shopEnabled) {
      ui.shopGrid.innerHTML = renderEmptyCard(
        "Shop disabled",
        "Firebase Remote Config currently has the shop turned off. Set the shop flag back to true to show products again."
      );
      return;
    }

    const items = getFilteredItems(state.visibleProducts);
    if (!items.length) {
      ui.shopGrid.innerHTML = renderEmptyCard(
        "No products match this view",
        "Try a different category or search term to widen the catalog results."
      );
      return;
    }
    ui.shopGrid.innerHTML = items.map((item) => renderProductCard(item)).join("");
  }

  function renderBundles() {
    if (!ui.bundleGrid) return;
    if (state.loading) {
      return;
    }
    if (!state.shopEnabled) {
      ui.bundleGrid.innerHTML = renderEmptyCard(
        "Bundles paused",
        "Reward bundles are hidden while the shop is disabled from Firebase Remote Config."
      );
      return;
    }

    const items = getFilteredItems(state.visibleBundles);
    if (!items.length) {
      ui.bundleGrid.innerHTML = renderEmptyCard(
        "No bundle offers in this view",
        "Switch the category filter or remove the current search term to see more bundle offers."
      );
      return;
    }
    ui.bundleGrid.innerHTML = items.map((item) => renderBundleCard(item)).join("");
  }

  function renderCart() {
    const rows = getCartRows();
    if (!rows.length) {
      ui.cartItems.innerHTML = '<p class="muted">Your cart is empty.</p>';
    } else {
      ui.cartItems.innerHTML = rows.map((row) => renderCartRow(row)).join("");
    }

    const totalQty = rows.reduce((sum, row) => sum + row.qty, 0);
    const subtotal = rows.reduce((sum, row) => sum + row.item.priceCents * row.qty, 0);
    const shipping = resolveShipping(subtotal, rows);
    const total = subtotal + shipping.amountCents;

    if (ui.cartCount) ui.cartCount.textContent = String(totalQty);
    if (ui.subtotalValue) ui.subtotalValue.textContent = formatMoney(subtotal);
    if (ui.shippingValue) ui.shippingValue.textContent = formatMoney(shipping.amountCents);
    if (ui.totalValue) ui.totalValue.textContent = formatMoney(total);
    if (ui.shippingNote) ui.shippingNote.textContent = shipping.note;
    if (ui.checkoutButton) ui.checkoutButton.disabled = rows.length === 0 || state.checkoutBusy || !state.shopEnabled;
    if (ui.portalButton) ui.portalButton.disabled = state.portalBusy;
  }

  function renderProductCard(item) {
    return `
      <article class="product-card" data-sku="${escapeHtml(item.sku)}">
        <div class="product-media">
          <div class="product-badge-row">
            ${item.badge ? `<span class="product-badge">${escapeHtml(item.badge)}</span>` : ""}
            <span class="product-badge secondary">${escapeHtml(labelForTab(item.tab))}</span>
          </div>
          ${renderCarousel(item)}
        </div>
        <div class="product-body">
          <div class="product-topline">
            <span class="detail-chip">${escapeHtml(item.provider)}</span>
            <span class="detail-chip subtle">${escapeHtml(`${item.providerScore.toFixed(1)} / ${item.providerMethod || "Provider rated"}`)}</span>
          </div>
          <h3 class="product-name">${escapeHtml(item.name)}</h3>
          <p class="product-desc">${escapeHtml(item.description)}</p>
          ${renderRating(item)}
          ${renderPriceRow(item)}
          <p class="product-shipping">${escapeHtml(item.ships)}</p>
          ${renderFactGrid(item.factGrid)}
          ${renderBulletList(item.highlights.slice(0, 3))}
          <details class="product-details">
            <summary>Specs, fit, and care</summary>
            <div class="detail-stack">
              <p class="detail-copy">${escapeHtml(item.longDescription)}</p>
              ${renderDetailGroup("Key details", item.detailBullets)}
              ${renderDetailGroup("Materials", item.materials)}
              ${renderDetailGroup("Options", item.options)}
              ${renderDetailGroup("Compliance", item.compliance)}
              ${item.origin ? `<div class="detail-block"><h4>Origin</h4><p>${escapeHtml(item.origin)}</p></div>` : ""}
              ${item.location ? `<div class="detail-block"><h4>Provider location</h4><p>${escapeHtml(item.location)}</p></div>` : ""}
              ${item.productionTime ? `<div class="detail-block"><h4>Production time</h4><p>${escapeHtml(item.productionTime)}</p></div>` : ""}
              ${item.careInstructions ? `<div class="detail-block"><h4>Care</h4><p>${escapeHtml(item.careInstructions)}</p></div>` : ""}
            </div>
          </details>
          <div class="card-actions">
            <button class="cta" type="button" data-action="add-to-cart" data-sku="${escapeHtml(item.sku)}">Add to cart</button>
            <button class="ghost" type="button" data-action="open-cart">View cart</button>
          </div>
        </div>
      </article>
    `;
  }

  function renderBundleCard(item) {
    const unlocked = isBundleUnlocked(item);
    const busy = state.rewardBusySku === item.sku;
    const nativeUnlockNote = state.isNative
      ? "Unlock runs after the rewarded interstitial finishes on this device."
      : "Install Quantura on iOS or Android to unlock the discounted bundle.";
    let actionMarkup = "";
    if (unlocked || !item.rewardUnlockRequired) {
      actionMarkup = `<button class="cta" type="button" data-action="add-to-cart" data-sku="${escapeHtml(item.sku)}">Add bundle</button>`;
    } else if (state.isNative) {
      actionMarkup = `<button class="cta" type="button" data-action="unlock-bundle" data-sku="${escapeHtml(item.sku)}" ${
        busy ? "disabled" : ""
      }>${escapeHtml(busy ? "Unlocking…" : item.unlockCtaCopy || "Unlock bundle")}</button>`;
    } else {
      actionMarkup = '<button class="cta secondary" type="button" disabled>Unlock in app</button>';
    }

    return `
      <article class="bundle-card ${unlocked ? "is-unlocked" : "is-locked"}" data-sku="${escapeHtml(item.sku)}">
        <div class="product-media">
          <div class="product-badge-row">
            <span class="product-badge accent">${unlocked ? "Unlocked" : "Reward bundle"}</span>
            <span class="product-badge secondary">${escapeHtml(labelForTab(item.tab))}</span>
          </div>
          ${renderCarousel(item)}
        </div>
        <div class="product-body">
          <div class="product-topline">
            <span class="detail-chip">${escapeHtml(item.provider)}</span>
            <span class="detail-chip subtle">${escapeHtml(item.providerMethod || "Reward-unlocked bundle")}</span>
          </div>
          <h3 class="product-name">${escapeHtml(item.name)}</h3>
          <p class="product-desc">${escapeHtml(item.description)}</p>
          <p class="bundle-note">${escapeHtml(nativeUnlockNote)}</p>
          ${renderPriceRow(item)}
          ${renderFactGrid(item.factGrid)}
          <div class="bundle-components">
            <h4>Bundle includes</h4>
            ${renderBulletList(item.bundleComponents.length ? item.bundleComponents : item.highlights.slice(0, 3))}
          </div>
          <details class="product-details">
            <summary>Bundle details</summary>
            <div class="detail-stack">
              <p class="detail-copy">${escapeHtml(item.longDescription)}</p>
              ${renderDetailGroup("Why this bundle", item.detailBullets)}
              ${renderDetailGroup("Highlights", item.highlights)}
            </div>
          </details>
          <div class="card-actions">
            ${actionMarkup}
            <button class="ghost" type="button" data-action="open-cart">View cart</button>
          </div>
        </div>
      </article>
    `;
  }

  function renderCarousel(item) {
    const images = Array.isArray(item.images) && item.images.length ? item.images : [{ url: item.imageUrl, alt: item.name }];
    const activeIndex = getCarouselIndex(item.sku, images.length);
    const activeImage = images[activeIndex] || images[0];
    const dots =
      images.length > 1
        ? `<div class="carousel-dots">${images
            .map(
              (asset, index) => `
                <button
                  class="carousel-dot ${index === activeIndex ? "is-active" : ""}"
                  type="button"
                  data-action="carousel-goto"
                  data-sku="${escapeHtml(item.sku)}"
                  data-index="${index}"
                  aria-label="Show image ${index + 1} for ${escapeHtml(item.name)}"
                ></button>
              `
            )
            .join("")}</div>`
        : "";

    return `
      <div class="carousel-shell" data-carousel-sku="${escapeHtml(item.sku)}">
        <img
          class="product-image"
          data-role="carousel-image"
          src="${escapeHtml(activeImage.url || item.imageUrl)}"
          alt="${escapeHtml(activeImage.alt || item.name)}"
          loading="lazy"
        />
        ${
          images.length > 1
            ? `
              <div class="carousel-controls">
                <button type="button" class="carousel-button" data-action="carousel-prev" data-sku="${escapeHtml(item.sku)}" aria-label="Previous image">
                  <i class="iconoir-nav-arrow-left" aria-hidden="true"></i>
                </button>
                <button type="button" class="carousel-button" data-action="carousel-next" data-sku="${escapeHtml(item.sku)}" aria-label="Next image">
                  <i class="iconoir-nav-arrow-right" aria-hidden="true"></i>
                </button>
              </div>
              ${dots}
            `
            : ""
        }
      </div>
    `;
  }

  function renderCartRow(row) {
    return `
      <article class="cart-item">
        <div class="cart-item-top">
          <div>
            <h3>${escapeHtml(row.item.name)}</h3>
            <div class="cart-item-price">${escapeHtml(labelForTab(row.item.tab))} · ${formatMoney(row.item.priceCents)} each</div>
          </div>
          <button class="ghost" type="button" data-action="remove-item" data-sku="${escapeHtml(row.item.sku)}">Remove</button>
        </div>
        <div class="qty-row">
          <button class="qty-btn" type="button" data-action="qty-down" data-sku="${escapeHtml(row.item.sku)}" aria-label="Decrease quantity">-</button>
          <span class="qty-value">${row.qty}</span>
          <button class="qty-btn" type="button" data-action="qty-up" data-sku="${escapeHtml(row.item.sku)}" aria-label="Increase quantity">+</button>
        </div>
      </article>
    `;
  }

  function renderPriceRow(item) {
    const compareAt =
      item.compareAtCents && item.compareAtCents > item.priceCents
        ? `<span class="product-compare">${formatMoney(item.compareAtCents)}</span>`
        : "";
    return `
      <div class="product-price-row">
        <div class="product-price">${formatMoney(item.priceCents)}</div>
        ${compareAt}
      </div>
    `;
  }

  function renderRating(item) {
    const stars = "★★★★★";
    return `
      <div class="product-rating" aria-label="Rated ${item.ratingValue.toFixed(1)} out of 5">
        <span class="product-stars">${stars}</span>
        <span>${item.ratingValue.toFixed(1)} · ${item.ratingCount} reviews</span>
      </div>
    `;
  }

  function renderFactGrid(facts) {
    if (!Array.isArray(facts) || !facts.length) return "";
    return `
      <div class="fact-grid">
        ${facts
          .slice(0, 4)
          .map(
            (fact) => `
              <div class="fact-chip">
                <span>${escapeHtml(fact.label)}</span>
                <strong>${escapeHtml(fact.value)}</strong>
              </div>
            `
          )
          .join("")}
      </div>
    `;
  }

  function renderDetailGroup(title, values) {
    if (!Array.isArray(values) || !values.length) return "";
    return `
      <div class="detail-block">
        <h4>${escapeHtml(title)}</h4>
        <div class="detail-chip-row">
          ${values.map((value) => `<span class="detail-chip">${escapeHtml(value)}</span>`).join("")}
        </div>
      </div>
    `;
  }

  function renderBulletList(items) {
    if (!Array.isArray(items) || !items.length) return "";
    return `<ul class="feature-list">${items.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`;
  }

  function renderEmptyCard(title, copy) {
    return `
      <article class="shop-empty-card">
        <h3>${escapeHtml(title)}</h3>
        <p>${escapeHtml(copy)}</p>
      </article>
    `;
  }

  function updateTabState() {
    ui.tabs.forEach((tab) => {
      const active = String(tab.getAttribute("data-filter") || "all") === state.activeFilter;
      tab.classList.toggle("active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
    });
  }

  function addToCart(sku, qty) {
    const item = state.catalogBySku.get(sku);
    if (!item) return;
    if (item.kind === "bundle" && item.rewardUnlockRequired && !isBundleUnlocked(item)) {
      showMessage("Unlock this bundle in the native app before adding it to cart.", "warn");
      return;
    }
    const nextQty = getCartQty(sku) + qty;
    state.cart[sku] = clampNumber(nextQty, 0, 99);
    persistCart();
    renderCart();
    showMessage(`${item.name} added to cart.`, "success");
    if (window.innerWidth <= 960) {
      setCartOpen(true);
    }
  }

  function removeFromCart(sku) {
    if (sku in state.cart) {
      delete state.cart[sku];
      persistCart();
      renderCart();
    }
  }

  function updateCartQty(sku, qty) {
    if (qty <= 0) {
      removeFromCart(sku);
      return;
    }
    state.cart[sku] = clampNumber(qty, 1, 99);
    persistCart();
    renderCart();
  }

  function pruneCart() {
    const validSkus = new Set(state.catalogBySku.keys());
    let changed = false;
    Object.keys(state.cart).forEach((sku) => {
      if (!validSkus.has(sku) || !Number.isFinite(Number(state.cart[sku])) || Number(state.cart[sku]) <= 0) {
        delete state.cart[sku];
        changed = true;
      }
    });
    if (changed) {
      persistCart();
    }
  }

  function persistCart() {
    localStorage.setItem(STORAGE_KEYS.cart, JSON.stringify(state.cart));
  }

  function persistUnlockedBundles() {
    localStorage.setItem(STORAGE_KEYS.unlockedBundles, JSON.stringify(state.unlockedBundles));
  }

  async function unlockBundle(sku) {
    const item = state.catalogBySku.get(sku);
    if (!item || !item.rewardUnlockRequired) return;
    if (!state.isNative || !window.QuanturaShopRuntime || typeof window.QuanturaShopRuntime.runRewardUnlock !== "function") {
      showMessage("Bundle unlocks only run inside Quantura iOS and Android builds.", "warn");
      return;
    }

    try {
      state.rewardBusySku = sku;
      renderBundles();
      const result = await window.QuanturaShopRuntime.runRewardUnlock({
        reason: `shop_bundle_${sku.toLowerCase()}`,
      });
      if (result && result.ok) {
        state.unlockedBundles[sku] = true;
        persistUnlockedBundles();
        showMessage(`${item.name} unlocked. You can add it to cart now.`, "success");
      } else {
        const message =
          (result && result.message) || "Rewarded interstitial was unavailable. Try again in a few seconds.";
        showMessage(message, "warn");
      }
    } catch (error) {
      console.error("[shop] bundle unlock failed", error);
      showMessage("Rewarded interstitial failed. Try again shortly.", "warn");
    } finally {
      state.rewardBusySku = "";
      renderSummary();
      renderBundles();
    }
  }

  async function startCheckout() {
    const rows = getCartRows();
    if (!rows.length) {
      showMessage("Add at least one item before starting checkout.", "warn");
      return;
    }

    const button = ui.checkoutButton;
    const originalText = button ? button.textContent : "";
    state.checkoutBusy = true;
    renderCart();
    if (button) button.textContent = "Opening checkout…";

    try {
      const response = await fetch("/api/shop/checkout", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "content-type": "application/json",
          accept: "application/json",
        },
        body: JSON.stringify({
          email: state.email,
          items: rows.map((row) => ({
            sku: row.item.sku,
            qty: row.qty,
          })),
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.url) {
        throw new Error(payload.detail || payload.message || "Unable to create checkout session.");
      }
      window.location.assign(payload.url);
    } catch (error) {
      console.error("[shop] checkout failed", error);
      showMessage(error.message || "Unable to open checkout.", "warn");
    } finally {
      state.checkoutBusy = false;
      if (button) button.textContent = originalText || "Checkout";
      renderCart();
    }
  }

  async function openBillingPortal() {
    if (!state.email) {
      showMessage("Enter the email used for checkout before opening the billing portal.", "warn");
      if (ui.emailInput) ui.emailInput.focus();
      return;
    }
    const button = ui.portalButton;
    const originalText = button ? button.textContent : "";
    state.portalBusy = true;
    renderCart();
    if (button) button.textContent = "Opening portal…";

    try {
      const response = await fetch("/api/shop/portal", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "content-type": "application/json",
          accept: "application/json",
        },
        body: JSON.stringify({
          email: state.email,
          returnUrl: window.location.href,
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.url) {
        throw new Error(payload.detail || payload.message || "Unable to open the billing portal.");
      }
      window.location.assign(payload.url);
    } catch (error) {
      console.error("[shop] portal failed", error);
      showMessage(error.message || "Unable to open the billing portal.", "warn");
    } finally {
      state.portalBusy = false;
      if (button) button.textContent = originalText || "Open billing portal";
      renderCart();
    }
  }

  function setCartOpen(nextOpen) {
    document.body.classList.toggle("cart-open", nextOpen);
    if (ui.cartOverlay) {
      ui.cartOverlay.hidden = !nextOpen;
    }
    if (ui.cartToggle) {
      ui.cartToggle.setAttribute("aria-expanded", nextOpen ? "true" : "false");
    }
  }

  function getFilteredItems(items) {
    const query = state.query.toLowerCase();
    return items.filter((item) => {
      if (state.activeFilter !== "all" && item.tab !== state.activeFilter) {
        return false;
      }
      if (!query) return true;
      const haystack = [
        item.name,
        item.description,
        item.longDescription,
        item.provider,
        item.providerMethod,
        item.location,
        item.badge,
      ]
        .concat(item.highlights, item.detailBullets, item.materials, item.options, item.compliance, item.bundleComponents)
        .join(" ")
        .toLowerCase();
      return haystack.includes(query);
    });
  }

  function getCartRows() {
    return Object.entries(state.cart)
      .map(([sku, qty]) => {
        const item = state.catalogBySku.get(sku);
        return item ? { item, qty: clampNumber(Number(qty), 1, 99) } : null;
      })
      .filter(Boolean);
  }

  function getCartQty(sku) {
    return clampNumber(Number(state.cart[sku] || 0), 0, 99);
  }

  function isSkuVisible(sku) {
    const cleanSku = String(sku || "").trim().toUpperCase();
    if (!cleanSku) return false;
    if (!(cleanSku in state.visibility.items)) return true;
    return state.visibility.items[cleanSku] !== false;
  }

  function isBundleUnlocked(item) {
    return !item.rewardUnlockRequired || state.unlockedBundles[item.sku] === true || state.adFreeEntitlement;
  }

  function advanceCarousel(sku, delta) {
    const item = state.catalogBySku.get(sku);
    if (!item || !Array.isArray(item.images) || item.images.length <= 1) return;
    const nextIndex = modulo(getCarouselIndex(sku, item.images.length) + delta, item.images.length);
    setCarouselIndex(sku, nextIndex);
  }

  function setCarouselIndex(sku, nextIndex) {
    const item = state.catalogBySku.get(sku);
    if (!item || !Array.isArray(item.images) || !item.images.length) return;
    state.carousel[sku] = modulo(nextIndex, item.images.length);
    updateCarouselDisplay(sku);
  }

  function updateCarouselDisplay(sku) {
    const item = state.catalogBySku.get(sku);
    if (!item || !Array.isArray(item.images) || !item.images.length) return;
    const index = getCarouselIndex(sku, item.images.length);
    const asset = item.images[index] || item.images[0];
    const nodes = Array.from(document.querySelectorAll(`[data-sku="${sku}"] [data-role="carousel-image"]`));
    nodes.forEach((node) => {
      node.src = asset.url || item.imageUrl;
      node.alt = asset.alt || item.name;
    });
    Array.from(document.querySelectorAll(`[data-sku="${sku}"] .carousel-dot`)).forEach((dot, dotIndex) => {
      dot.classList.toggle("is-active", dotIndex === index);
    });
  }

  function getCarouselIndex(sku, length) {
    const current = Number(state.carousel[sku] || 0);
    if (!Number.isFinite(current) || current < 0) return 0;
    return modulo(current, Math.max(length, 1));
  }

  function resolveShipping(subtotal, rows) {
    const hasHardware = rows.some((row) => row.item.shippingClass === "hardware");
    const policyRoot = state.shippingPolicy || {};
    const policy = hasHardware ? policyRoot.hardware || {} : policyRoot.pod || {};
    const flatRate = clampNumber(Number(policy.flatRateCents || 0), 0, 999999);
    const freeOver = clampNumber(Number(policy.freeOverCents || 0), 0, 999999);
    const amountCents = freeOver > 0 && subtotal >= freeOver ? 0 : flatRate;
    let note = policy.estimate || "Shipping cost is finalized at checkout after address selection.";
    if (amountCents === 0 && freeOver > 0) {
      note = `Free shipping unlocked over ${formatMoney(freeOver)}. Final address-based shipping is still confirmed at checkout.`;
    }
    return { amountCents, note };
  }

  function normalizeItem(raw) {
    if (!raw || typeof raw !== "object") return null;
    const sku = String(raw.sku || "").trim().toUpperCase();
    if (!sku) return null;
    const images = Array.isArray(raw.images)
      ? raw.images
          .map((asset, index) => ({
            url: String(asset && asset.url ? asset.url : raw.imageUrl || "").trim(),
            alt: String(asset && asset.alt ? asset.alt : `${raw.name || sku} image ${index + 1}`).trim(),
          }))
          .filter((asset) => asset.url)
      : [];
    const normalizedImages = images.length
      ? images
      : [{ url: String(raw.imageUrl || raw.placeholderImageUrl || "").trim(), alt: String(raw.name || sku) }];

    return {
      sku,
      kind: String(raw.kind || "product"),
      name: String(raw.name || sku),
      description: String(raw.description || ""),
      longDescription: String(raw.longDescription || raw.description || ""),
      priceCents: clampNumber(Number(raw.priceCents || 0), 0, 9999999),
      compareAtCents: clampNumber(Number(raw.compareAtCents || 0), 0, 9999999),
      currency: String(raw.currency || "usd").toLowerCase(),
      ratingValue: clampNumber(Number(raw.rating && raw.rating.value), 0, 5) || 0,
      ratingCount: clampNumber(Number(raw.rating && raw.rating.count), 0, 100000),
      ships: String(raw.ships || ""),
      imageUrl: String(raw.imageUrl || raw.placeholderImageUrl || ""),
      images: normalizedImages,
      tab: String(raw.tab || "all"),
      shippingClass: String(raw.shippingClass || "pod"),
      provider: String(raw.provider || ""),
      providerScore: clampNumber(Number(raw.providerScore || 0), 0, 10),
      providerMethod: String(raw.providerMethod || ""),
      location: String(raw.location || ""),
      productionTime: String(raw.productionTime || ""),
      badge: String(raw.badge || ""),
      highlights: toStringList(raw.highlights),
      detailBullets: toStringList(raw.detailBullets),
      materials: toStringList(raw.materials),
      options: toStringList(raw.options),
      compliance: toStringList(raw.compliance),
      careInstructions: String(raw.careInstructions || ""),
      origin: String(raw.origin || ""),
      factGrid: Array.isArray(raw.factGrid)
        ? raw.factGrid
            .map((fact) => ({
              label: String(fact && fact.label ? fact.label : "").trim(),
              value: String(fact && fact.value ? fact.value : "").trim(),
            }))
            .filter((fact) => fact.label && fact.value)
        : [],
      rewardUnlockRequired: Boolean(raw.rewardUnlockRequired),
      unlockCtaCopy: String(raw.unlockCtaCopy || ""),
      bundleComponents: toStringList(raw.bundleComponents),
    };
  }

  function sanitizeVisibilityConfig(raw, fallback) {
    const base = {
      enabled: fallback && typeof fallback.enabled === "boolean" ? fallback.enabled : true,
      items: Object.assign({}, (fallback && fallback.items) || {}),
    };
    if (!raw || typeof raw !== "object") return base;
    if (typeof raw.enabled === "boolean") base.enabled = raw.enabled;
    if (raw.items && typeof raw.items === "object") {
      Object.entries(raw.items).forEach(([sku, enabled]) => {
        const cleanSku = String(sku || "").trim().toUpperCase();
        if (!cleanSku) return;
        base.items[cleanSku] = Boolean(enabled);
      });
    }
    return base;
  }

  function sanitizeShippingPolicy(raw) {
    const source = raw && typeof raw === "object" ? raw : {};
    return {
      pod: sanitizePolicyBranch(source.pod),
      hardware: sanitizePolicyBranch(source.hardware),
      returns: {
        pod: String(source.returns && source.returns.pod ? source.returns.pod : ""),
        hardware: String(source.returns && source.returns.hardware ? source.returns.hardware : ""),
      },
    };
  }

  function sanitizePolicyBranch(raw) {
    const branch = raw && typeof raw === "object" ? raw : {};
    return {
      flatRateCents: clampNumber(Number(branch.flatRateCents || 0), 0, 999999),
      freeOverCents: clampNumber(Number(branch.freeOverCents || 0), 0, 999999),
      estimate: String(branch.estimate || ""),
      detail: String(branch.detail || ""),
    };
  }

  function labelForTab(tab) {
    return TAB_LABELS[String(tab || "all")] || "Catalog";
  }

  function showMessage(text, tone, durationMs) {
    if (!ui.message) return;
    const nextTone = tone || "info";
    ui.message.textContent = text;
    ui.message.classList.remove("hidden", "warn", "success");
    if (nextTone === "warn") ui.message.classList.add("warn");
    if (nextTone === "success") ui.message.classList.add("success");

    clearTimeout(state.messageTimer);
    const nextDuration = durationMs === 0 ? 0 : durationMs || 4200;
    if (nextDuration > 0) {
      state.messageTimer = window.setTimeout(() => {
        ui.message.classList.add("hidden");
      }, nextDuration);
    }
  }

  function loadJson(key, fallback) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return fallback;
      return JSON.parse(raw);
    } catch (_error) {
      return fallback;
    }
  }

  function loadText(key) {
    try {
      return String(localStorage.getItem(key) || "").trim();
    } catch (_error) {
      return "";
    }
  }

  function toStringList(value) {
    return Array.isArray(value)
      ? value
          .map((entry) => String(entry || "").trim())
          .filter(Boolean)
      : [];
  }

  function parseJson(value) {
    try {
      return JSON.parse(String(value || ""));
    } catch (_error) {
      return null;
    }
  }

  function formatMoney(cents) {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 2,
    }).format((Number(cents) || 0) / 100);
  }

  function clampNumber(value, min, max) {
    if (!Number.isFinite(value)) return min;
    return Math.min(Math.max(value, min), max);
  }

  function modulo(value, size) {
    if (!size) return 0;
    return ((value % size) + size) % size;
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function sleep(ms) {
    return new Promise((resolve) => {
      window.setTimeout(resolve, ms);
    });
  }
})();
