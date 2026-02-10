(() => {
  const ADMIN_EMAIL = "tamzid257@gmail.com";
  const STRIPE_URL = "https://buy.stripe.com/8x24gze7a86K1zE9M20co08";

  const ui = {
    headerAuth: document.getElementById("header-auth"),
    headerSignOut: document.getElementById("header-signout"),
    headerDashboard: document.getElementById("header-dashboard"),
    emailForm: document.getElementById("email-auth-form"),
    emailInput: document.getElementById("auth-email"),
    passwordInput: document.getElementById("auth-password"),
    emailCreate: document.getElementById("email-create"),
    emailMessage: document.getElementById("auth-email-message"),
    googleSignin: document.getElementById("google-signin"),
    userEmail: document.getElementById("user-email"),
    userProvider: document.getElementById("user-provider"),
    userStatus: document.getElementById("user-status"),
    dashboardCta: document.getElementById("dashboard-cta"),
    userOrders: document.getElementById("user-orders"),
    userForecasts: document.getElementById("user-forecasts"),
    adminSection: document.getElementById("admin"),
    adminOrders: document.getElementById("admin-orders"),
    contactForm: document.getElementById("contact-form"),
    forecastForm: document.getElementById("forecast-form"),
    forecastOutput: document.getElementById("forecast-output"),
    forecastService: document.getElementById("forecast-service"),
    technicalsForm: document.getElementById("technicals-form"),
    technicalsOutput: document.getElementById("technicals-output"),
    downloadForm: document.getElementById("download-form"),
    downloadStatus: document.getElementById("download-status"),
    trendingButton: document.getElementById("load-trending"),
    trendingList: document.getElementById("trending-list"),
    newsForm: document.getElementById("news-form"),
    newsOutput: document.getElementById("news-output"),
    optionsForm: document.getElementById("options-form"),
    optionsOutput: document.getElementById("options-output"),
    screenerForm: document.getElementById("screener-form"),
    screenerOutput: document.getElementById("screener-output"),
    predictionsForm: document.getElementById("predictions-upload-form"),
    predictionsOutput: document.getElementById("predictions-output"),
    predictionsStatus: document.getElementById("predictions-status"),
    autopilotForm: document.getElementById("autopilot-form"),
    autopilotOutput: document.getElementById("autopilot-output"),
    autopilotStatus: document.getElementById("autopilot-status"),
    alpacaForm: document.getElementById("alpaca-order-form"),
    alpacaOutput: document.getElementById("alpaca-output"),
    alpacaStatus: document.getElementById("alpaca-status"),
    alpacaLoadAccount: document.getElementById("alpaca-load-account"),
    alpacaAccountOutput: document.getElementById("alpaca-account-output"),
    alpacaLoadPositions: document.getElementById("alpaca-load-positions"),
    alpacaPositionsOutput: document.getElementById("alpaca-positions-output"),
    alpacaLoadOrders: document.getElementById("alpaca-load-orders"),
    alpacaOrderStatusFilter: document.getElementById("alpaca-order-status-filter"),
    alpacaBrokerOrdersOutput: document.getElementById("alpaca-broker-orders-output"),
    alpacaCancelOrderForm: document.getElementById("alpaca-cancel-order-form"),
    alpacaCancelOrderId: document.getElementById("alpaca-cancel-order-id"),
    alpacaCancelStatus: document.getElementById("alpaca-cancel-status"),
    toast: document.getElementById("toast"),
    purchasePanels: Array.from(document.querySelectorAll(".purchase-panel")),
  };

  const state = {
    user: null,
    unsubscribeOrders: null,
    unsubscribeAdmin: null,
    unsubscribeForecasts: null,
    unsubscribeAutopilot: null,
    unsubscribePredictions: null,
    unsubscribeAlpaca: null,
  };

  const showToast = (message, variant = "default") => {
    if (!ui.toast) return;
    ui.toast.textContent = message;
    ui.toast.dataset.variant = variant;
    ui.toast.classList.add("show");
    window.clearTimeout(ui.toast._timeout);
    ui.toast._timeout = window.setTimeout(() => {
      ui.toast.classList.remove("show");
    }, 3200);
  };

  const getAnalytics = () => {
    try {
      if (typeof firebase === "undefined") return null;
      if (!firebase.analytics) return null;
      return firebase.analytics();
    } catch (error) {
      return null;
    }
  };

  const logEvent = (name, params = {}) => {
    const analytics = getAnalytics();
    if (!analytics) return;
    try {
      analytics.logEvent(name, params);
    } catch (error) {
      // Ignore analytics errors.
    }
  };

  const setUserId = (uid) => {
    const analytics = getAnalytics();
    if (!analytics || !analytics.setUserId) return;
    try {
      analytics.setUserId(uid || null);
    } catch (error) {
      // Ignore analytics errors.
    }
  };

  const getSessionId = () => {
    const key = "quantura_session_id";
    const existing = localStorage.getItem(key);
    if (existing) return existing;
    const sessionId = `qs_${Math.random().toString(36).slice(2, 11)}${Date.now().toString(36)}`;
    localStorage.setItem(key, sessionId);
    return sessionId;
  };

  const getUtm = () => {
    const params = new URLSearchParams(window.location.search);
    return {
      source: params.get("utm_source") || "",
      medium: params.get("utm_medium") || "",
      campaign: params.get("utm_campaign") || "",
      content: params.get("utm_content") || "",
      term: params.get("utm_term") || "",
    };
  };

  const buildMeta = () => ({
    sessionId: getSessionId(),
    pagePath: window.location.pathname,
    pageTitle: document.title,
    referrer: document.referrer || "",
    userAgent: navigator.userAgent,
    language: navigator.language,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    screen: `${window.screen.width}x${window.screen.height}`,
    platform: navigator.platform,
  });

  const setPurchaseState = (user) => {
    ui.purchasePanels.forEach((panel) => {
      const button = panel.querySelector('[data-action="purchase"]');
      const note = panel.querySelector(".purchase-note");
      const success = panel.querySelector(".purchase-success");
      const stripe = panel.querySelector('[data-action="stripe"]');
      if (!button || !note) return;

      if (user) {
        button.disabled = false;
        button.textContent = button.dataset.labelAuth || "Request Deep Forecast";
        note.textContent = "Orders appear in your dashboard instantly.";
      } else {
        button.disabled = true;
        button.textContent = button.dataset.labelGuest || "Sign in to purchase";
        note.textContent = "You must sign in to purchase. We use Firebase Auth to secure checkout.";
        stripe?.classList.add("hidden");
        success?.classList.add("hidden");
      }
    });
  };

  const setAuthUi = (user) => {
    const isAuthed = Boolean(user);
    ui.headerAuth?.classList.toggle("hidden", isAuthed);
    ui.headerSignOut?.classList.toggle("hidden", !isAuthed);
    ui.headerDashboard?.classList.toggle("hidden", !isAuthed);

    if (ui.userEmail) ui.userEmail.textContent = user?.email || "Not signed in";
    if (ui.userProvider) ui.userProvider.textContent = user?.providerData?.[0]?.providerId || "—";
    if (ui.userStatus) {
      ui.userStatus.textContent = isAuthed ? "Member" : "Guest";
      ui.userStatus.classList.toggle("pill", true);
    }
    ui.dashboardCta?.classList.toggle("hidden", isAuthed);

    setPurchaseState(user);
  };

  const formatTimestamp = (value) => {
    if (!value) return "Processing";
    if (value.toDate) {
      return value.toDate().toLocaleString();
    }
    return new Date(value).toLocaleString();
  };

  const formatEpoch = (value) => {
    if (!value) return "";
    const ts = typeof value === "number" ? value * 1000 : Date.parse(value);
    if (!ts) return "";
    return new Date(ts).toLocaleString();
  };

  const renderOrderList = (orders, container, opts = {}) => {
    if (!container) return;
    container.innerHTML = "";
    if (!orders.length) {
      container.innerHTML = "<p class=\"small\">No orders yet. Your Deep Forecast request will appear here.</p>";
      return;
    }

    orders.forEach((order) => {
      const card = document.createElement("div");
      card.className = "order-card";
      card.dataset.orderId = order.id;

      const status = order.status || "pending";
      const statusLabel = status.replace("_", " ");
      const filesMarkup = renderFileList(order.fulfillmentFiles || []);

      const adminTools = opts.admin
        ? `
          <div class="order-actions">
            <select class="status-select">
              ${["pending", "in_progress", "fulfilled", "cancelled"]
                .map((option) => `
                  <option value="${option}" ${option === status ? "selected" : ""}>
                    ${option.replace("_", " ")}
                  </option>
                `)
                .join("")}
            </select>
            <textarea class="input notes-input" rows="2" placeholder="Fulfillment notes">${order.fulfillmentNotes || ""}</textarea>
            <button class="cta small update-status" type="button">Update status</button>
          </div>
          <div class="upload-row">
            <input class="file-input" type="file" />
            <button class="cta secondary small upload-file" type="button">Upload file</button>
          </div>
        `
        : "";

      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">${order.product || "Deep Forecast"}</div>
            <div class="small">Order ID: ${order.id}</div>
          </div>
          <span class="status ${status}">${statusLabel}</span>
        </div>
        <div class="order-meta">
          <div><strong>Requested</strong> ${formatTimestamp(order.createdAt)}</div>
          <div><strong>Price</strong> $${order.price || 349} ${order.currency || "USD"}</div>
          ${opts.admin ? `<div><strong>Client</strong> ${order.userEmail || "—"}</div>` : ""}
          ${order.fulfillmentNotes ? `<div><strong>Notes</strong> ${order.fulfillmentNotes}</div>` : ""}
        </div>
        ${filesMarkup ? `<div><strong>Files</strong>${filesMarkup}</div>` : ""}
        ${adminTools}
      `;

      container.appendChild(card);
    });
  };

  const renderFileList = (files = []) => {
    if (!Array.isArray(files) || files.length === 0) return "";
    const items = files
      .map((file) => `
        <li>
          <a href="${file.url}" target="_blank" rel="noreferrer">${file.name || "Report file"}</a>
          <span class="small">${file.uploadedAt ? formatTimestamp(file.uploadedAt) : ""}</span>
        </li>
      `)
      .join("");
    return `<ul class="file-list">${items}</ul>`;
  };

  const renderRequestList = (items, container, emptyMessage) => {
    if (!container) return;
    container.innerHTML = "";
    if (!items.length) {
      container.innerHTML = `<p class=\"small\">${emptyMessage}</p>`;
      return;
    }

    items.forEach((item) => {
      const card = document.createElement("div");
      card.className = "order-card";
      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">${item.title || item.ticker || "Request"}</div>
            <div class="small">ID: ${item.id}</div>
          </div>
          <span class="status ${item.status || "pending"}">${item.status || "pending"}</span>
        </div>
        <div class="order-meta">
          <div><strong>Requested</strong> ${formatTimestamp(item.createdAt)}</div>
          ${item.summary ? `<div><strong>Summary</strong> ${item.summary}</div>` : ""}
        </div>
      `;
      container.appendChild(card);
    });
  };

  const startUserOrders = (db, user) => {
    if (state.unsubscribeOrders) state.unsubscribeOrders();
    if (!user) return;

    state.unsubscribeOrders = db
      .collection("orders")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot((snapshot) => {
        const orders = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderOrderList(orders, ui.userOrders);
      });
  };

  const startUserForecasts = (db, user) => {
    if (state.unsubscribeForecasts) state.unsubscribeForecasts();
    if (!user || !ui.userForecasts) return;

    state.unsubscribeForecasts = db
      .collection("forecast_requests")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot((snapshot) => {
        const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderRequestList(items, ui.userForecasts, "No forecast requests yet.");
      });
  };

  const startAutopilotRequests = (db, user) => {
    if (state.unsubscribeAutopilot) state.unsubscribeAutopilot();
    if (!user || !ui.autopilotOutput) return;

    state.unsubscribeAutopilot = db
      .collection("autopilot_requests")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot((snapshot) => {
        const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderRequestList(items, ui.autopilotOutput, "No autopilot requests yet.");
      });
  };

  const startPredictionsUploads = (db, user) => {
    if (state.unsubscribePredictions) state.unsubscribePredictions();
    if (!user || !ui.predictionsOutput) return;

    state.unsubscribePredictions = db
      .collection("prediction_uploads")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot((snapshot) => {
        const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderRequestList(items, ui.predictionsOutput, "No uploads yet.");
      });
  };

  const startAlpacaOrders = (db, user) => {
    if (state.unsubscribeAlpaca) state.unsubscribeAlpaca();
    if (!user || !ui.alpacaOutput) return;

    state.unsubscribeAlpaca = db
      .collection("alpaca_orders")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot((snapshot) => {
        const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderRequestList(items, ui.alpacaOutput, "No Alpaca orders yet.");
      });
  };

  const startAdminOrders = (db) => {
    if (state.unsubscribeAdmin) state.unsubscribeAdmin();
    state.unsubscribeAdmin = db
      .collection("orders")
      .orderBy("createdAt", "desc")
      .limit(100)
      .onSnapshot((snapshot) => {
        const orders = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        renderOrderList(orders, ui.adminOrders, { admin: true });
      });
  };

  const ensureUserProfile = async (db, user) => {
    if (!user) return;
    const userRef = db.collection("users").doc(user.uid);
    const snapshot = await userRef.get();
    const existing = snapshot.exists ? snapshot.data() : {};
    const createdAt = existing?.createdAt || firebase.firestore.FieldValue.serverTimestamp();

    await userRef.set(
      {
        email: user.email,
        name: user.displayName || "",
        provider: user.providerData?.[0]?.providerId || "email",
        photoURL: user.photoURL || "",
        lastLoginAt: firebase.firestore.FieldValue.serverTimestamp(),
        createdAt,
        metadata: buildMeta(),
      },
      { merge: true }
    );
  };

  const buildCsv = (rows, headers) => {
    const escape = (value) => {
      if (value === null || value === undefined) return "";
      const text = String(value);
      if (text.includes(",") || text.includes("\n") || text.includes("\"")) {
        return `"${text.replace(/"/g, '""')}"`;
      }
      return text;
    };

    const headerLine = headers.map(escape).join(",");
    const dataLines = rows.map((row) => headers.map((key) => escape(row[key])).join(","));
    return [headerLine, ...dataLines].join("\n");
  };

  const triggerDownload = (filename, content) => {
    const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
  };

  const handlePurchase = async (panel, functions) => {
    if (!state.user) {
      showToast("Sign in to continue.", "warn");
      return;
    }

    const button = panel.querySelector('[data-action="purchase"]');
    const note = panel.querySelector(".purchase-note");
    const success = panel.querySelector(".purchase-success");
    const stripe = panel.querySelector('[data-action="stripe"]');
    if (!button) return;

    button.disabled = true;
    button.textContent = "Creating order...";

    const meta = {
      ...buildMeta(),
      utm: getUtm(),
    };

    try {
      logEvent("begin_checkout", { currency: panel.dataset.currency || "USD", value: Number(panel.dataset.price || 349) });
      const createOrder = functions.httpsCallable("create_order");
      const result = await createOrder({
        product: panel.dataset.product || "Deep Forecast",
        price: Number(panel.dataset.price || 349),
        currency: panel.dataset.currency || "USD",
        meta,
      });
      const orderId = result.data?.orderId;
      if (success) {
        success.textContent = `Order ${orderId} created. Proceed to payment to finalize.`;
        success.classList.remove("hidden");
      }
      stripe?.classList.remove("hidden");
      note.textContent = "Order created. Proceed to payment to finalize.";
      logEvent("order_created", { order_id: orderId, currency: panel.dataset.currency || "USD" });
      showToast("Order created. Proceed to payment.");
    } catch (error) {
      showToast(error.message || "Unable to create order.", "warn");
    } finally {
      button.disabled = false;
      button.textContent = button.dataset.labelAuth || "Request Deep Forecast";
    }
  };

  const init = () => {
    if (typeof firebase === "undefined") {
      console.error("Firebase SDK not loaded.");
      return;
    }

    const auth = firebase.auth();
    const db = firebase.firestore();
    const functions = firebase.functions();
    const storage = firebase.storage ? firebase.storage() : null;

    logEvent("page_view", {
      page_title: document.title,
      page_path: window.location.pathname,
      page_location: window.location.href,
    });

    document.addEventListener("click", (event) => {
      const target = event.target.closest("[data-analytics]");
      if (!target) return;
      logEvent(target.dataset.analytics, {
        label: target.dataset.label || target.textContent.trim(),
        page_path: window.location.pathname,
      });
    });

    auth.setPersistence(firebase.auth.Auth.Persistence.LOCAL).catch(() => {
      showToast("Unable to set auth persistence.", "warn");
    });

    ui.headerAuth?.addEventListener("click", () => {
      document.getElementById("auth")?.scrollIntoView({ behavior: "smooth" });
    });

    ui.headerSignOut?.addEventListener("click", async () => {
      await auth.signOut();
      showToast("Signed out.");
      logEvent("logout", { method: "firebase" });
    });

    ui.emailForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      try {
        await auth.signInWithEmailAndPassword(ui.emailInput.value, ui.passwordInput.value);
        showToast("Signed in successfully.");
        logEvent("login", { method: "password" });
      } catch (error) {
        if (ui.emailMessage) ui.emailMessage.textContent = error.message;
      }
    });

    ui.emailCreate?.addEventListener("click", async () => {
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      try {
        await auth.createUserWithEmailAndPassword(ui.emailInput.value, ui.passwordInput.value);
        showToast("Account created.");
        logEvent("sign_up", { method: "password" });
      } catch (error) {
        if (ui.emailMessage) ui.emailMessage.textContent = error.message;
      }
    });

    ui.googleSignin?.addEventListener("click", async () => {
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      try {
        const provider = new firebase.auth.GoogleAuthProvider();
        await auth.signInWithPopup(provider);
        showToast("Signed in with Google.");
        logEvent("login", { method: "google" });
      } catch (error) {
        if (ui.emailMessage) ui.emailMessage.textContent = error.message;
      }
    });

    ui.purchasePanels.forEach((panel) => {
      const purchaseBtn = panel.querySelector('[data-action="purchase"]');
      const stripeBtn = panel.querySelector('[data-action="stripe"]');
      purchaseBtn?.addEventListener("click", () => handlePurchase(panel, functions));
      stripeBtn?.addEventListener("click", () => {
        logEvent("purchase_intent", { product: panel.dataset.product || "Deep Forecast" });
        window.open(STRIPE_URL, "_blank", "noreferrer");
      });
    });

    ui.adminOrders?.addEventListener("click", async (event) => {
      const updateButton = event.target.closest(".update-status");
      const uploadButton = event.target.closest(".upload-file");
      const card = event.target.closest(".order-card");
      if (!card) return;

      if (updateButton) {
        const orderId = card.dataset.orderId;
        const statusSelect = card.querySelector(".status-select");
        const notesInput = card.querySelector(".notes-input");
        if (!orderId || !statusSelect) return;

        updateButton.disabled = true;
        updateButton.textContent = "Updating...";

        try {
          const updateOrder = functions.httpsCallable("update_order_status");
          await updateOrder({
            orderId,
            status: statusSelect.value,
            notes: notesInput?.value || "",
          });
          logEvent("admin_update_status", { order_id: orderId, status: statusSelect.value });
          showToast("Order updated.");
        } catch (error) {
          showToast(error.message || "Unable to update order.", "warn");
        } finally {
          updateButton.disabled = false;
          updateButton.textContent = "Update status";
        }
      }

      if (uploadButton) {
        if (!storage) {
          showToast("File uploads are not available.", "warn");
          return;
        }
        const orderId = card.dataset.orderId;
        const fileInput = card.querySelector(".file-input");
        if (!orderId || !fileInput || !fileInput.files?.length) {
          showToast("Select a file first.", "warn");
          return;
        }

        const file = fileInput.files[0];
        uploadButton.disabled = true;
        uploadButton.textContent = "Uploading...";

        try {
          const path = `fulfillment/${orderId}/${Date.now()}_${file.name}`;
          const storageRef = storage.ref().child(path);
          const snapshot = await storageRef.put(file, {
            contentType: file.type || "application/octet-stream",
          });
          const url = await snapshot.ref.getDownloadURL();
          const fileMeta = {
            name: file.name,
            url,
            path,
            size: file.size,
            contentType: file.type || "application/octet-stream",
            uploadedAt: firebase.firestore.FieldValue.serverTimestamp(),
          };
          await db.collection("orders").doc(orderId).update({
            fulfillmentFiles: firebase.firestore.FieldValue.arrayUnion(fileMeta),
            updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
          });
          fileInput.value = "";
          logEvent("admin_upload_file", { order_id: orderId, name: file.name });
          showToast("File uploaded.");
        } catch (error) {
          showToast(error.message || "Upload failed.", "warn");
        } finally {
          uploadButton.disabled = false;
          uploadButton.textContent = "Upload file";
        }
      }
    });

    ui.contactForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.contactForm);
      const payload = {
        name: formData.get("name"),
        email: formData.get("email"),
        company: formData.get("company"),
        role: formData.get("role"),
        message: formData.get("message"),
        sourcePage: window.location.pathname,
        utm: getUtm(),
        meta: buildMeta(),
      };

      try {
        const submitContact = functions.httpsCallable("submit_contact");
        await submitContact(payload);
        ui.contactForm.reset();
        showToast("Message sent. We'll respond within one business day.");
        logEvent("contact_submit", { source: window.location.pathname });
      } catch (error) {
        showToast(error.message || "Unable to send message.", "warn");
      }
    });

    ui.forecastForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.forecastForm);
      const quantiles = formData.getAll("quantiles").map((value) => Number(value));
      const payload = {
        ticker: formData.get("ticker"),
        start: formData.get("start"),
        horizon: Number(formData.get("horizon")),
        interval: formData.get("interval"),
        quantiles,
        meta: buildMeta(),
        utm: getUtm(),
      };

      try {
        const runForecast = functions.httpsCallable("run_prophet_forecast");
        const result = await runForecast(payload);
        const data = result.data || {};
        if (ui.forecastOutput) {
          ui.forecastOutput.innerHTML = `
            <div class="small"><strong>Request ID:</strong> ${data.requestId || "—"}</div>
            <div class="small"><strong>Last close:</strong> ${data.lastClose || "—"}</div>
            <div class="small"><strong>MAE (recent):</strong> ${data.mae || "—"}</div>
            <div class="small"><strong>Coverage P10–P90:</strong> ${data.coverage10_90 || "—"}</div>
          `;
        }
        logEvent("forecast_request", { ticker: payload.ticker, interval: payload.interval });
        showToast("Forecast queued and stored in your dashboard.");
      } catch (error) {
        showToast(error.message || "Unable to run forecast.", "warn");
      }
    });

    ui.technicalsForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.technicalsForm);
      const indicators = formData.getAll("indicators");
      const payload = {
        ticker: formData.get("ticker"),
        interval: formData.get("interval"),
        lookback: Number(formData.get("lookback")),
        indicators,
        meta: buildMeta(),
      };

      try {
        const runIndicators = functions.httpsCallable("get_technicals");
        const result = await runIndicators(payload);
        const data = result.data || {};
        const rows = data.latest || [];
        if (ui.technicalsOutput) {
          if (!rows.length) {
            ui.technicalsOutput.textContent = "No indicator data returned.";
          } else {
            ui.technicalsOutput.innerHTML = `
              <table class="data-table">
                <thead><tr><th>Indicator</th><th>Value</th></tr></thead>
                <tbody>
                  ${rows.map((row) => `<tr><td>${row.name}</td><td>${row.value}</td></tr>`).join("")}
                </tbody>
              </table>
            `;
          }
        }
        logEvent("technicals_request", { ticker: payload.ticker });
      } catch (error) {
        showToast(error.message || "Unable to run indicators.", "warn");
      }
    });

    ui.downloadForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.downloadForm);
      const payload = {
        ticker: formData.get("ticker"),
        start: formData.get("start"),
        end: formData.get("end"),
        interval: formData.get("interval"),
      };

      try {
        ui.downloadStatus.textContent = "Fetching data...";
        const fetchHistory = functions.httpsCallable("get_ticker_history");
        const result = await fetchHistory(payload);
        const data = result.data || {};
        const rows = data.rows || [];
        if (!rows.length) {
          ui.downloadStatus.textContent = "No data returned.";
          return;
        }
        const headers = Object.keys(rows[0]);
        const csv = buildCsv(rows, headers);
        triggerDownload(`${payload.ticker}_history.csv`, csv);
        ui.downloadStatus.textContent = "Download ready.";
        logEvent("download_history", { ticker: payload.ticker, interval: payload.interval });
      } catch (error) {
        ui.downloadStatus.textContent = "Download failed.";
        showToast(error.message || "Unable to fetch history.", "warn");
      }
    });

    ui.trendingButton?.addEventListener("click", async () => {
      try {
        const getTrending = functions.httpsCallable("get_trending_tickers");
        const result = await getTrending({ meta: buildMeta() });
        const tickers = result.data?.tickers || [];
        if (ui.trendingList) {
          ui.trendingList.innerHTML = tickers.map((ticker) => `<div class="ticker-pill">${ticker}</div>`).join("");
        }
        logEvent("trending_loaded", { count: tickers.length });
      } catch (error) {
        showToast(error.message || "Unable to load trending tickers.", "warn");
      }
    });

    ui.newsForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.newsForm);
      const payload = {
        ticker: formData.get("ticker"),
      };

      try {
        const getNews = functions.httpsCallable("get_ticker_news");
        const result = await getNews(payload);
        const items = result.data?.news || [];
        if (ui.newsOutput) {
          if (!items.length) {
            ui.newsOutput.textContent = "No news returned.";
          } else {
            ui.newsOutput.innerHTML = items
              .map(
                (item) => `
                  <div class="card" style="margin-bottom:12px;">
                    <strong>${item.title}</strong>
                    <div class="small">${item.publisher || ""} · ${formatEpoch(item.publishedAt)}</div>
                    <a href="${item.link}" target="_blank" rel="noreferrer">Read article</a>
                  </div>
                `
              )
              .join("");
          }
        }
        logEvent("news_loaded", { ticker: payload.ticker, count: items.length });
      } catch (error) {
        showToast(error.message || "Unable to fetch news.", "warn");
      }
    });

    ui.optionsForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.optionsForm);
      const payload = {
        ticker: formData.get("ticker"),
        expiration: formData.get("expiration"),
      };

      try {
        const getOptions = functions.httpsCallable("alpaca_get_options");
        const result = await getOptions(payload);
        const items = result.data?.options || [];
        if (ui.optionsOutput) {
          if (!items.length) {
            ui.optionsOutput.textContent = "No options returned.";
          } else {
            ui.optionsOutput.innerHTML = `
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Type</th>
                    <th>Strike</th>
                    <th>Expiry</th>
                    <th>Price</th>
                    <th>Delta</th>
                    <th>Implied Prob.</th>
                  </tr>
                </thead>
                <tbody>
                  ${items
                    .map(
                      (opt) => `
                        <tr>
                          <td>${opt.symbol}</td>
                          <td>${opt.type}</td>
                          <td>${opt.strike}</td>
                          <td>${opt.expiration}</td>
                          <td>${opt.lastPrice}</td>
                          <td>${opt.delta ?? "—"}</td>
                          <td>${opt.impliedProbability ?? "—"}</td>
                        </tr>
                      `
                    )
                    .join("")}
                </tbody>
              </table>
            `;
          }
        }
        logEvent("options_loaded", { ticker: payload.ticker });
      } catch (error) {
        showToast(error.message || "Unable to load options.", "warn");
      }
    });

    ui.screenerForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.screenerForm);
      const payload = {
        universe: formData.get("universe"),
        market: formData.get("market"),
        minCap: Number(formData.get("minCap")),
        maxNames: Number(formData.get("maxNames")),
        notes: formData.get("notes"),
        meta: buildMeta(),
      };

      try {
        const queueRun = functions.httpsCallable("queue_screener_run");
        const result = await queueRun(payload);
        if (ui.screenerOutput) {
          ui.screenerOutput.innerHTML = `Run queued. ID: ${result.data?.runId || "—"}`;
        }
        showToast("Screener run queued.");
        logEvent("screener_request", { universe: payload.universe });
      } catch (error) {
        showToast(error.message || "Unable to queue screener run.", "warn");
      }
    });

    ui.predictionsForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.user) {
        showToast("Sign in to upload predictions.", "warn");
        return;
      }
      if (!storage) {
        showToast("File uploads are not available.", "warn");
        return;
      }

      const fileInput = document.getElementById("predictions-file");
      const notesInput = document.getElementById("predictions-notes");
      if (!fileInput?.files?.length) {
        showToast("Select a predictions.csv file.", "warn");
        return;
      }

      const file = fileInput.files[0];
      ui.predictionsStatus.textContent = "Uploading...";

      try {
        const path = `predictions/${state.user.uid}/${Date.now()}_${file.name}`;
        const storageRef = storage.ref().child(path);
        const snapshot = await storageRef.put(file, {
          contentType: file.type || "text/csv",
        });
        const url = await snapshot.ref.getDownloadURL();
        const doc = {
          userId: state.user.uid,
          title: file.name,
          status: "uploaded",
          notes: notesInput?.value || "",
          fileUrl: url,
          filePath: path,
          createdAt: firebase.firestore.FieldValue.serverTimestamp(),
          meta: buildMeta(),
        };
        await db.collection("prediction_uploads").add(doc);
        ui.predictionsStatus.textContent = "Upload complete.";
        fileInput.value = "";
        if (notesInput) notesInput.value = "";
        logEvent("predictions_upload", { file: file.name });
        showToast("Predictions uploaded.");
      } catch (error) {
        ui.predictionsStatus.textContent = "Upload failed.";
        showToast(error.message || "Upload failed.", "warn");
      }
    });

    ui.autopilotForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.user) {
        showToast("Sign in to queue autopilot runs.", "warn");
        return;
      }
      const formData = new FormData(ui.autopilotForm);
      const payload = {
        ticker: formData.get("ticker"),
        horizon: Number(formData.get("horizon")),
        quantiles: formData.get("quantiles"),
        interval: formData.get("interval"),
        notes: formData.get("notes"),
        meta: buildMeta(),
      };

      try {
        ui.autopilotStatus.textContent = "Queuing...";
        const queueRun = functions.httpsCallable("queue_autopilot_run");
        const result = await queueRun(payload);
        ui.autopilotStatus.textContent = `Queued: ${result.data?.requestId || "—"}`;
        logEvent("autopilot_request", { ticker: payload.ticker });
        showToast("Autopilot run queued.");
      } catch (error) {
        ui.autopilotStatus.textContent = "Unable to queue run.";
        showToast(error.message || "Unable to queue run.", "warn");
      }
    });

    ui.alpacaForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.user) {
        showToast("Sign in to place orders.", "warn");
        return;
      }
      const formData = new FormData(ui.alpacaForm);
      const payload = {
        symbol: formData.get("symbol"),
        qty: Number(formData.get("qty")),
        side: formData.get("side"),
        type: formData.get("type"),
        limitPrice: formData.get("limitPrice"),
        timeInForce: formData.get("timeInForce"),
        meta: buildMeta(),
      };

      try {
        ui.alpacaStatus.textContent = "Submitting order...";
        const placeOrder = functions.httpsCallable("alpaca_place_order");
        const result = await placeOrder(payload);
        ui.alpacaStatus.textContent = `Order placed: ${result.data?.orderId || "—"}`;
        logEvent("alpaca_order", { symbol: payload.symbol, side: payload.side });
        showToast("Order submitted.");
      } catch (error) {
        ui.alpacaStatus.textContent = "Unable to place order.";
        showToast(error.message || "Unable to place order.", "warn");
      }
    });

    auth.onAuthStateChanged(async (user) => {
      state.user = user;
      setAuthUi(user);
      setUserId(user?.uid || null);

      if (!user) {
        renderOrderList([], ui.userOrders);
        renderRequestList([], ui.userForecasts, "No forecast requests yet.");
        renderRequestList([], ui.autopilotOutput, "No autopilot requests yet.");
        renderRequestList([], ui.predictionsOutput, "No uploads yet.");
        renderRequestList([], ui.alpacaOutput, "No Alpaca orders yet.");
        ui.adminSection?.classList.add("hidden");
        if (state.unsubscribeOrders) state.unsubscribeOrders();
        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
        if (state.unsubscribeForecasts) state.unsubscribeForecasts();
        if (state.unsubscribeAutopilot) state.unsubscribeAutopilot();
        if (state.unsubscribePredictions) state.unsubscribePredictions();
        if (state.unsubscribeAlpaca) state.unsubscribeAlpaca();
        return;
      }

      await ensureUserProfile(db, user);
      startUserOrders(db, user);
      startUserForecasts(db, user);
      startAutopilotRequests(db, user);
      startPredictionsUploads(db, user);
      startAlpacaOrders(db, user);

      if (user.email === ADMIN_EMAIL) {
        ui.adminSection?.classList.remove("hidden");
        startAdminOrders(db);
      } else {
        ui.adminSection?.classList.add("hidden");
        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
      }
    });
  };

  window.addEventListener("load", init);
})();
