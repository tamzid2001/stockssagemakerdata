(() => {
  const ADMIN_EMAIL = "tamzid257@gmail.com";
  const STRIPE_URL = "https://buy.stripe.com/8x24gze7a86K1zE9M20co08";
  const FCM_TOKEN_CACHE_KEY = "quantura_fcm_token";
  const COOKIE_CONSENT_KEY = "quantura_cookie_consent";
  const FEEDBACK_PROMPT_KEY = "quantura_feedback_prompt_v1";
  const LAST_TICKER_KEY = "quantura_last_ticker";
  const WORKSPACE_KEY = "quantura_active_workspace";

  const ui = {
    headerAuth: document.getElementById("header-auth"),
    headerSignOut: document.getElementById("header-signout"),
    headerDashboard: document.getElementById("header-dashboard"),
    headerUserEmail: document.getElementById("header-user-email"),
    headerUserStatus: document.getElementById("header-user-status"),
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
    navAdmin: document.getElementById("nav-admin"),
    terminalForm: document.getElementById("terminal-form"),
    terminalTicker: document.getElementById("terminal-ticker"),
    terminalInterval: document.getElementById("terminal-interval"),
    terminalStatus: document.getElementById("terminal-status"),
    tickerChart: document.getElementById("ticker-chart"),
    indicatorChart: document.getElementById("indicator-chart"),
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
    savedForecastsList: document.getElementById("saved-forecasts-list"),
    workspaceSelect: document.getElementById("workspace-select"),
    collabInviteForm: document.getElementById("collab-invite-form"),
    collabInviteEmail: document.getElementById("collab-invite-email"),
    collabInviteRole: document.getElementById("collab-invite-role"),
    collabInviteStatus: document.getElementById("collab-invite-status"),
    collabInvitesList: document.getElementById("collab-invites-list"),
    collabCollaboratorsList: document.getElementById("collab-collaborators-list"),
    notificationsEnable: document.getElementById("notifications-enable"),
    notificationsRefresh: document.getElementById("notifications-refresh"),
    notificationsSendTest: document.getElementById("notifications-send-test"),
    notificationsStatus: document.getElementById("notifications-status"),
    notificationsToken: document.getElementById("notifications-token"),
    toast: document.getElementById("toast"),
    purchasePanels: Array.from(document.querySelectorAll(".purchase-panel")),
  };

  const state = {
    user: null,
    cookieConsent: (() => {
      try {
        return localStorage.getItem(COOKIE_CONSENT_KEY) || "";
      } catch (error) {
        return "";
      }
    })(),
    initialPageViewSent: false,
    tickerContext: {
      ticker: "",
      interval: "1d",
      rows: [],
      forecastId: "",
      forecastDoc: null,
      indicatorOverlays: [],
    },
    unsubscribeOrders: null,
    unsubscribeAdmin: null,
    unsubscribeForecasts: null,
    unsubscribeAutopilot: null,
    unsubscribePredictions: null,
    unsubscribeAlpaca: null,
    messagingBound: false,
    activeWorkspaceId: (() => {
      try {
        return localStorage.getItem(WORKSPACE_KEY) || "";
      } catch (error) {
        return "";
      }
    })(),
    sharedWorkspaces: [],
    unsubscribeSharedWorkspaces: null,
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
      if (state.cookieConsent !== "accepted") return null;
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

  const getMessagingClient = () => {
    if (typeof firebase === "undefined" || !firebase.messaging) return null;
    try {
      return firebase.messaging();
    } catch (error) {
      return null;
    }
  };

  const isPushSupported = () =>
    typeof window !== "undefined" &&
    "Notification" in window &&
    "serviceWorker" in navigator &&
    "PushManager" in window;

  const setNotificationStatus = (text) => {
    if (ui.notificationsStatus) {
      ui.notificationsStatus.textContent = text;
    }
  };

  const setNotificationTokenPreview = (token) => {
    if (!ui.notificationsToken) return;
    if (!token) {
      ui.notificationsToken.textContent = "No token registered yet.";
      return;
    }
    ui.notificationsToken.textContent = `${token.slice(0, 20)}...${token.slice(-12)}`;
  };

  const setNotificationControlsEnabled = (enabled) => {
    if (ui.notificationsEnable) ui.notificationsEnable.disabled = !enabled;
    if (ui.notificationsRefresh) ui.notificationsRefresh.disabled = !enabled;
    if (ui.notificationsSendTest) ui.notificationsSendTest.disabled = !enabled;
  };

  const safeLocalStorageGet = (key) => {
    try {
      return localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  };

  const safeLocalStorageSet = (key, value) => {
    try {
      localStorage.setItem(key, value);
    } catch (error) {
      // Ignore storage failures.
    }
  };

  const safeLocalStorageRemove = (key) => {
    try {
      localStorage.removeItem(key);
    } catch (error) {
      // Ignore storage failures.
    }
  };

  const buildWorkspaceOptions = (user) => {
    const opts = [];
    if (!user) return opts;
    opts.push({ id: user.uid, label: "My workspace" });
    state.sharedWorkspaces.forEach((ws) => {
      const id = ws.workspaceUserId || ws.id;
      if (!id) return;
      const label = ws.workspaceEmail ? `Shared: ${ws.workspaceEmail}` : `Shared workspace ${id.slice(0, 6)}`;
      opts.push({ id, label });
    });
    return opts;
  };

  const resolveActiveWorkspaceId = (user) => {
    if (!user) return "";
    const allowed = new Set(buildWorkspaceOptions(user).map((o) => o.id));
    const desired = state.activeWorkspaceId || "";
    return allowed.has(desired) ? desired : user.uid;
  };

  const setActiveWorkspaceId = (workspaceId) => {
    state.activeWorkspaceId = workspaceId || "";
    if (workspaceId) {
      safeLocalStorageSet(WORKSPACE_KEY, workspaceId);
    } else {
      safeLocalStorageRemove(WORKSPACE_KEY);
    }
  };

  const renderWorkspaceSelect = (user) => {
    if (!ui.workspaceSelect) return;
    ui.workspaceSelect.innerHTML = "";
    if (!user) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "Sign in to manage workspaces";
      ui.workspaceSelect.appendChild(opt);
      ui.workspaceSelect.disabled = true;
      return;
    }

    const options = buildWorkspaceOptions(user);
    const active = resolveActiveWorkspaceId(user);
    options.forEach((item) => {
      const opt = document.createElement("option");
      opt.value = item.id;
      opt.textContent = item.label;
      ui.workspaceSelect.appendChild(opt);
    });
    ui.workspaceSelect.value = active;
    ui.workspaceSelect.disabled = options.length <= 1;
  };

  const subscribeSharedWorkspaces = (db, user) => {
    if (state.unsubscribeSharedWorkspaces) state.unsubscribeSharedWorkspaces();
    state.sharedWorkspaces = [];
    if (!user) {
      renderWorkspaceSelect(null);
      return;
    }

    state.unsubscribeSharedWorkspaces = db
      .collection("users")
      .doc(user.uid)
      .collection("shared_workspaces")
      .onSnapshot(
        (snapshot) => {
          state.sharedWorkspaces = snapshot.docs.map((doc) => ({ id: doc.id, workspaceUserId: doc.id, ...doc.data() }));
          renderWorkspaceSelect(user);
          const resolved = resolveActiveWorkspaceId(user);
          if (resolved !== state.activeWorkspaceId) {
            setActiveWorkspaceId(resolved);
            logEvent("workspace_resolved", { workspace_id: resolved });
          }
          if (resolved) startUserForecasts(db, resolved);
        },
        () => {
          // Ignore workspace subscription errors.
        }
      );
  };

  const ensureInitialPageView = () => {
    if (state.initialPageViewSent) return;
    logEvent("page_view", {
      page_title: document.title,
      page_path: window.location.pathname,
      page_location: window.location.href,
    });
    state.initialPageViewSent = true;
  };

  const setCookieConsent = (value) => {
    state.cookieConsent = value;
    safeLocalStorageSet(COOKIE_CONSENT_KEY, value);
    if (value === "accepted") {
      ensureInitialPageView();
      setUserId(state.user?.uid || null);
      showToast("Thanks. Analytics is enabled.");
    } else {
      showToast("Preferences saved.");
    }
  };

  const buildModalShell = (id) => {
    const wrapper = document.createElement("div");
    wrapper.id = id;
    wrapper.className = "modal hidden";
    wrapper.innerHTML = `
      <div class="modal-backdrop" data-action="close"></div>
      <div class="modal-card card" role="dialog" aria-modal="true"></div>
    `;
    document.body.appendChild(wrapper);
    return wrapper;
  };

  const ensureCookieModal = () => {
    let modal = document.getElementById("cookie-modal");
    if (!modal) modal = buildModalShell("cookie-modal");
    const card = modal.querySelector(".modal-card");
    card.innerHTML = `
      <h3>Cookies and analytics</h3>
      <p class="small">
        Quantura uses cookies for analytics and to improve reliability. You can opt out at any time.
      </p>
      <div class="modal-actions">
        <button class="cta secondary" type="button" data-action="decline">No thanks</button>
        <button class="cta" type="button" data-action="accept">Accept</button>
      </div>
    `;
    modal.addEventListener("click", (event) => {
      const action = event.target?.dataset?.action;
      if (!action) return;
      if (action === "accept") {
        setCookieConsent("accepted");
        modal.classList.add("hidden");
      }
      if (action === "decline") {
        setCookieConsent("declined");
        modal.classList.add("hidden");
      }
      if (action === "close") {
        modal.classList.add("hidden");
      }
    });
    return modal;
  };

  const ensureFeedbackModal = () => {
    let modal = document.getElementById("feedback-modal");
    if (!modal) modal = buildModalShell("feedback-modal");
    const card = modal.querySelector(".modal-card");
    card.innerHTML = `
      <h3>Help us improve Quantura</h3>
      <p class="small">
        Share what you were trying to do and what could be better. This feedback is stored privately in Firestore.
      </p>
      <label class="label" for="feedback-rating">Rating</label>
      <select id="feedback-rating" class="status-select">
        <option value="">Select</option>
        <option value="5">5 (Excellent)</option>
        <option value="4">4</option>
        <option value="3">3</option>
        <option value="2">2</option>
        <option value="1">1 (Poor)</option>
      </select>
      <label class="label" for="feedback-message">Feedback</label>
      <textarea id="feedback-message" placeholder="What should we improve?"></textarea>
      <div class="modal-actions">
        <button class="cta secondary" type="button" data-action="close">Cancel</button>
        <button class="cta" type="button" data-action="send">Send feedback</button>
      </div>
      <p class="small" id="feedback-status"></p>
    `;
    modal.addEventListener("click", async (event) => {
      const action = event.target?.dataset?.action;
      if (!action) return;
      if (action === "close") {
        modal.classList.add("hidden");
        return;
      }
      if (action !== "send") return;

      const rating = modal.querySelector("#feedback-rating")?.value || "";
      const message = modal.querySelector("#feedback-message")?.value || "";
      const status = modal.querySelector("#feedback-status");
      if (status) status.textContent = "Sending...";

      try {
        if (typeof firebase === "undefined") throw new Error("Firebase not loaded.");
        const functions = firebase.functions();
        const submitFeedback = functions.httpsCallable("submit_feedback");
        await submitFeedback({
          rating,
          message,
          pagePath: window.location.pathname,
          meta: buildMeta(),
        });
        if (status) status.textContent = "Sent. Thank you.";
        logEvent("feedback_submitted", { rating: rating || "n/a" });
        showToast("Feedback sent.");
        safeLocalStorageSet(FEEDBACK_PROMPT_KEY, String(Date.now()));
      } catch (error) {
        if (status) status.textContent = error.message || "Unable to send feedback.";
        showToast(error.message || "Unable to send feedback.", "warn");
      }
    });
    return modal;
  };

  const ensureFeedbackPrompt = () => {
    const lastShown = Number(safeLocalStorageGet(FEEDBACK_PROMPT_KEY) || "0");
    const weekMs = 7 * 24 * 60 * 60 * 1000;
    if (lastShown && Date.now() - lastShown < weekMs) return null;
    if (document.getElementById("feedback-banner")) return null;

    const banner = document.createElement("div");
    banner.id = "feedback-banner";
    banner.className = "feedback-banner";
    banner.innerHTML = `
      <div>
        <strong>Help us improve Firebase Hosting.</strong>
        <div class="small">By continuing, you agree Google may use feedback and basic system info to improve services.</div>
      </div>
      <div class="banner-actions">
        <button class="cta secondary small" type="button" data-action="dismiss">No thanks</button>
        <button class="cta small" type="button" data-action="open">Show question</button>
      </div>
    `;
    document.body.appendChild(banner);

    banner.addEventListener("click", (event) => {
      const action = event.target?.dataset?.action;
      if (!action) return;
      if (action === "dismiss") {
        safeLocalStorageSet(FEEDBACK_PROMPT_KEY, String(Date.now()));
        banner.remove();
      }
      if (action === "open") {
        ensureFeedbackModal().classList.remove("hidden");
      }
    });
    return banner;
  };

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

    if (ui.headerUserEmail) ui.headerUserEmail.textContent = user?.email || "Guest";
    if (ui.headerUserStatus) {
      ui.headerUserStatus.textContent = isAuthed ? "Member" : "Guest";
      ui.headerUserStatus.classList.toggle("pill", true);
    }

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
      const isForecast = Boolean(item.service && item.ticker);
      const forecastMeta = isForecast
        ? `
          <div><strong>Ticker</strong> ${item.ticker}</div>
          <div><strong>Service</strong> ${item.service}</div>
          <div><strong>Engine</strong> ${item.engine || "—"}</div>
          ${item.serviceMessage ? `<div><strong>Message</strong> ${escapeHtml(item.serviceMessage)}</div>` : ""}
        `
        : "";
      const actions = isForecast
        ? `
          <div class="order-actions" style="grid-template-columns: 1fr;">
            <button class="cta secondary small" type="button" data-action="plot-forecast" data-forecast-id="${item.id}" data-ticker="${item.ticker}">
              Plot on chart
            </button>
          </div>
        `
        : "";
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
          ${forecastMeta}
        </div>
        ${actions}
      `;
      container.appendChild(card);
    });
  };

  const renderCollabInvites = (invites) => {
    if (!ui.collabInvitesList) return;
    ui.collabInvitesList.innerHTML = "";
    if (!Array.isArray(invites) || invites.length === 0) {
      ui.collabInvitesList.textContent = "No invites right now.";
      return;
    }

    invites.forEach((invite) => {
      const card = document.createElement("div");
      card.className = "order-card";
      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">Workspace invite</div>
            <div class="small">From: ${escapeHtml(invite.fromEmail || "Unknown")}</div>
          </div>
          <span class="status pending">${escapeHtml(invite.role || "viewer")}</span>
        </div>
        <div class="order-meta">
          <div><strong>Workspace</strong> ${escapeHtml(invite.workspaceEmail || invite.workspaceUserId || "")}</div>
          <div><strong>Invite ID</strong> ${escapeHtml(invite.inviteId || "")}</div>
        </div>
        <div class="order-actions" style="grid-template-columns: 1fr;">
          <button class="cta secondary small" type="button" data-action="accept-collab-invite" data-invite-id="${escapeHtml(invite.inviteId || "")}">
            Accept invite
          </button>
        </div>
      `;
      ui.collabInvitesList.appendChild(card);
    });
  };

  const renderCollaborators = (collaborators) => {
    if (!ui.collabCollaboratorsList) return;
    ui.collabCollaboratorsList.innerHTML = "";
    if (!Array.isArray(collaborators) || collaborators.length === 0) {
      ui.collabCollaboratorsList.textContent = "No collaborators yet.";
      return;
    }

    collaborators.forEach((collab) => {
      const card = document.createElement("div");
      card.className = "order-card";
      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">${escapeHtml(collab.email || collab.userId || "Collaborator")}</div>
            <div class="small">Role: ${escapeHtml(collab.role || "viewer")}</div>
          </div>
          <span class="status completed">active</span>
        </div>
        <div class="order-actions" style="grid-template-columns: 1fr;">
          <button class="cta secondary small" type="button" data-action="remove-collaborator" data-collaborator-id="${escapeHtml(collab.userId || "")}">
            Remove
          </button>
        </div>
      `;
      ui.collabCollaboratorsList.appendChild(card);
    });
  };

  const refreshCollaboration = async (functions) => {
    if (!state.user) return;
    if (!ui.collabInvitesList && !ui.collabCollaboratorsList) return;
    try {
      const listInvites = functions.httpsCallable("list_collab_invites");
      const listCollaborators = functions.httpsCallable("list_collaborators");
      const [invitesRes, collabsRes] = await Promise.all([
        ui.collabInvitesList ? listInvites({ meta: buildMeta() }) : Promise.resolve({ data: { invites: [] } }),
        ui.collabCollaboratorsList ? listCollaborators({ meta: buildMeta() }) : Promise.resolve({ data: { collaborators: [] } }),
      ]);
      const invites = invitesRes.data?.invites || [];
      const collaborators = collabsRes.data?.collaborators || [];
      renderCollabInvites(invites);
      renderCollaborators(collaborators);
      logEvent("collaboration_loaded", { invites: invites.length, collaborators: collaborators.length });
    } catch (error) {
      if (ui.collabInvitesList) ui.collabInvitesList.textContent = "Unable to load invites.";
      if (ui.collabCollaboratorsList) ui.collabCollaboratorsList.textContent = "Unable to load collaborators.";
    }
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

  const startUserForecasts = (db, workspaceUserId) => {
    if (state.unsubscribeForecasts) state.unsubscribeForecasts();
    const containers = [ui.userForecasts, ui.savedForecastsList].filter(Boolean);
    if (!workspaceUserId || containers.length === 0) return;

    state.unsubscribeForecasts = db
      .collection("forecast_requests")
      .where("userId", "==", workspaceUserId)
      .orderBy("createdAt", "desc")
      .onSnapshot((snapshot) => {
        const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        containers.forEach((container) => renderRequestList(items, container, "No forecast requests yet."));
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

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");

  const toPrettyJson = (value) => `<pre class="small">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;

  const normalizeTicker = (value) =>
    String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9.\\-]/g, "");

  const setTerminalStatus = (text) => {
    if (!ui.terminalStatus) return;
    ui.terminalStatus.textContent = text || "";
  };

  const getQueryParam = (key) => {
    try {
      return new URLSearchParams(window.location.search).get(key);
    } catch (error) {
      return null;
    }
  };

  const syncTickerInputs = (ticker) => {
    const t = normalizeTicker(ticker);
    if (!t) return;
    const ids = [
      "forecast-ticker",
      "technicals-ticker",
      "download-ticker",
      "news-ticker",
      "options-ticker",
      "autopilot-ticker",
      "alpaca-symbol",
    ];
    ids.forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      if ("value" in el && !String(el.value || "").trim()) {
        el.value = t;
      } else if ("value" in el) {
        el.value = t;
      }
    });
  };

  const computeHistoryStart = (interval) => {
    const days = interval === "1h" ? 45 : 730;
    const dt = new Date();
    dt.setDate(dt.getDate() - days);
    return dt.toISOString().slice(0, 10);
  };

  const getPlotly = () => (typeof window !== "undefined" ? window.Plotly : null);

  const extractDateKey = (rows) => {
    if (!rows?.length) return null;
    const sample = rows[0] || {};
    if ("Datetime" in sample) return "Datetime";
    if ("Date" in sample) return "Date";
    if ("ds" in sample) return "ds";
    return null;
  };

  const renderTickerChart = async (rows, ticker, interval, overlays = []) => {
    if (!ui.tickerChart) return;
    const Plotly = getPlotly();
    if (!Plotly) {
      ui.tickerChart.textContent = "Chart library not loaded.";
      return;
    }

    if (!rows?.length) {
      ui.tickerChart.textContent = "No price data to plot.";
      return;
    }

    const dateKey = extractDateKey(rows);
    if (!dateKey) {
      ui.tickerChart.textContent = "Unable to find timestamp column.";
      return;
    }

    const x = rows.map((row) => row[dateKey]);
    const hasOhlc = ["Open", "High", "Low", "Close"].every((key) => key in (rows[0] || {}));

    const baseTraces = hasOhlc
      ? [
          {
            type: "candlestick",
            name: `${ticker} price`,
            x,
            open: rows.map((row) => row.Open),
            high: rows.map((row) => row.High),
            low: rows.map((row) => row.Low),
            close: rows.map((row) => row.Close),
            increasing: { line: { color: "#1c6a50" } },
            decreasing: { line: { color: "#9b2b1a" } },
          },
        ]
      : [
          {
            type: "scatter",
            mode: "lines",
            name: `${ticker} close`,
            x,
            y: rows.map((row) => row.Close ?? row.close ?? null),
            line: { color: "#12182a", width: 2 },
          },
        ];

    const layout = {
      title: { text: `${ticker} (${interval})`, font: { family: "Manrope, sans-serif", size: 16 } },
      paper_bgcolor: "rgba(255,255,255,0)",
      plot_bgcolor: "#ffffff",
      margin: { l: 50, r: 30, t: 40, b: 40 },
      xaxis: {
        rangeslider: { visible: true },
        showspikes: true,
        spikemode: "across",
        spikesnap: "cursor",
      },
      yaxis: { showspikes: true, spikemode: "across", spikesnap: "cursor" },
      legend: { orientation: "h" },
    };

    await Plotly.react(ui.tickerChart, [...baseTraces, ...overlays], layout, {
      responsive: true,
      displaylogo: false,
    });
  };

  const renderIndicatorChart = async (series) => {
    if (!ui.indicatorChart) return;
    const Plotly = getPlotly();
    if (!Plotly) {
      ui.indicatorChart.textContent = "Chart library not loaded.";
      return;
    }

    const dates = series?.dates || [];
    const items = series?.items || [];
    if (!dates.length || !items.length) {
      ui.indicatorChart.textContent = "No indicator series to plot.";
      return;
    }

    const traces = items.map((item) => ({
      type: "scatter",
      mode: "lines",
      name: item.name,
      x: dates,
      y: item.values,
      line: { width: 2 },
    }));

    const layout = {
      title: { text: "Technical indicators", font: { family: "Manrope, sans-serif", size: 14 } },
      paper_bgcolor: "rgba(255,255,255,0)",
      plot_bgcolor: "#ffffff",
      margin: { l: 50, r: 30, t: 40, b: 40 },
      xaxis: { showspikes: true, spikemode: "across", spikesnap: "cursor" },
      yaxis: { zeroline: false },
      legend: { orientation: "h" },
    };

    await Plotly.react(ui.indicatorChart, traces, layout, {
      responsive: true,
      displaylogo: false,
    });
  };

  const buildIndicatorOverlays = (series) => {
    const dates = series?.dates || [];
    const items = series?.items || [];
    if (!dates.length || !items.length) return [];
    const overlayNames = new Set(["SMA", "EMA", "BBANDS_UPPER", "BBANDS_MIDDLE", "BBANDS_LOWER"]);
    return items
      .filter((item) => overlayNames.has(item.name))
      .map((item) => ({
        type: "scatter",
        mode: "lines",
        name: item.name,
        x: dates,
        y: item.values,
        line: { width: 1.8 },
        opacity: 0.85,
      }));
  };

  const buildForecastOverlays = (forecastRows) => {
    if (!Array.isArray(forecastRows) || forecastRows.length === 0) return [];
    const keys = Object.keys(forecastRows[0] || {});
    const quantKeys = keys.filter((key) => /^q\\d\\d$/.test(key)).sort();
    const pick = (q) => (quantKeys.includes(q) ? q : null);
    const q10 = pick("q10");
    const q25 = pick("q25");
    const q50 = pick("q50") || pick("q50");
    const q75 = pick("q75");
    const q90 = pick("q90");

    const x = forecastRows.map((row) => row.ds);
    const overlays = [];

    const addBand = (lowerKey, upperKey, label, color) => {
      if (!lowerKey || !upperKey) return;
      overlays.push({
        type: "scatter",
        mode: "lines",
        name: `${label} lower`,
        x,
        y: forecastRows.map((row) => row[lowerKey]),
        line: { width: 1, color: "rgba(0,0,0,0)" },
        hoverinfo: "skip",
        showlegend: false,
      });
      overlays.push({
        type: "scatter",
        mode: "lines",
        name: label,
        x,
        y: forecastRows.map((row) => row[upperKey]),
        line: { width: 1, color: "rgba(0,0,0,0)" },
        fill: "tonexty",
        fillcolor: color,
      });
    };

    addBand(q10, q90, "P10-P90", "rgba(58, 181, 162, 0.16)");
    addBand(q25, q75, "P25-P75", "rgba(240, 180, 41, 0.18)");

    if (q50) {
      overlays.push({
        type: "scatter",
        mode: "lines",
        name: "Median forecast",
        x,
        y: forecastRows.map((row) => row[q50]),
        line: { width: 2, color: "#12182a", dash: "dot" },
      });
    }
    return overlays;
  };

  const loadTickerHistory = async (functions, ticker, interval) => {
    const fetchHistory = functions.httpsCallable("get_ticker_history");
    const start = computeHistoryStart(interval);
    const result = await fetchHistory({ ticker, interval, start, end: "" });
    const rows = result.data?.rows || [];
    return Array.isArray(rows) ? rows : [];
  };

  const loadForecastDoc = async (db, forecastId) => {
    const snap = await db.collection("forecast_requests").doc(forecastId).get();
    if (!snap.exists) throw new Error("Forecast not found.");
    return { id: snap.id, ...snap.data() };
  };

  const plotForecastById = async (db, functions, forecastId) => {
    if (!forecastId) return;
    const doc = await loadForecastDoc(db, forecastId);
    const ticker = normalizeTicker(doc.ticker);
    const interval = doc.interval || state.tickerContext.interval || "1d";
    state.tickerContext.forecastId = forecastId;
    state.tickerContext.forecastDoc = doc;
    safeLocalStorageSet(LAST_TICKER_KEY, ticker);

    if (!ticker) throw new Error("Forecast ticker is missing.");
    if (!state.tickerContext.rows.length || state.tickerContext.ticker !== ticker || state.tickerContext.interval !== interval) {
      setTerminalStatus("Loading price history...");
      const rows = await loadTickerHistory(functions, ticker, interval);
      state.tickerContext.rows = rows;
      state.tickerContext.ticker = ticker;
      state.tickerContext.interval = interval;
      syncTickerInputs(ticker);
    }

    const forecastOverlays = buildForecastOverlays(doc.forecastRows || []);
    const overlays = [...forecastOverlays, ...(state.tickerContext.indicatorOverlays || [])];
    await renderTickerChart(state.tickerContext.rows, ticker, interval, overlays);
    setTerminalStatus(`Plotted forecast ${forecastId}.`);
  };

  const ensureMessagingServiceWorker = async () => {
    if (!("serviceWorker" in navigator)) {
      throw new Error("Service workers are not available in this browser.");
    }
    return navigator.serviceWorker.register("/firebase-messaging-sw.js");
  };

  const loadVapidKey = async (functions) => {
    if (window.QUANTURA_VAPID_KEY) return String(window.QUANTURA_VAPID_KEY);
    const getWebPushConfig = functions.httpsCallable("get_web_push_config");
    const response = await getWebPushConfig({ meta: buildMeta() });
    return response.data?.vapidKey || "";
  };

  const registerNotificationToken = async (functions, messaging, opts = {}) => {
    if (!state.user) throw new Error("Sign in before enabling notifications.");
    if (!messaging) throw new Error("Messaging SDK is not available.");
    if (!isPushSupported()) throw new Error("Push notifications are not supported in this browser.");

    const permission = await Notification.requestPermission();
    if (permission !== "granted") {
      throw new Error("Notification permission was not granted.");
    }

    const vapidKey = await loadVapidKey(functions);
    if (!vapidKey) {
      throw new Error("FCM Web Push key is missing. Configure FCM_WEB_VAPID_KEY in Functions.");
    }

    const serviceWorkerRegistration = await ensureMessagingServiceWorker();
    const token = await messaging.getToken({
      vapidKey,
      serviceWorkerRegistration,
    });

    if (!token) {
      throw new Error("No registration token generated.");
    }

    const registerPushToken = functions.httpsCallable("register_notification_token");
    await registerPushToken({
      token,
      forceRefresh: Boolean(opts.forceRefresh),
      meta: buildMeta(),
    });

    localStorage.setItem(FCM_TOKEN_CACHE_KEY, token);
    setNotificationTokenPreview(token);
    return token;
  };

  const unregisterCachedNotificationToken = async (functions) => {
    const token = localStorage.getItem(FCM_TOKEN_CACHE_KEY);
    if (!token) return;
    try {
      const unregisterToken = functions.httpsCallable("unregister_notification_token");
      await unregisterToken({ token, meta: buildMeta() });
    } catch (error) {
      // Ignore token cleanup errors.
    }
    localStorage.removeItem(FCM_TOKEN_CACHE_KEY);
    setNotificationTokenPreview("");
  };

  const bindForegroundPushHandler = (messaging) => {
    if (!messaging || state.messagingBound) return;
    try {
      messaging.onMessage((payload) => {
        const title = payload?.notification?.title || "Quantura update";
        const body = payload?.notification?.body || "You have a new dashboard update.";
        showToast(`${title}: ${body}`);
        setNotificationStatus(`Last message: ${title}`);
        logEvent("push_received_foreground", { title });
      });
      state.messagingBound = true;
    } catch (error) {
      // Ignore foreground messaging bind errors.
    }
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
    const messaging = getMessagingClient();

    if (ui.notificationsStatus) {
      if (!isPushSupported()) {
        setNotificationStatus("Push notifications are not supported in this browser.");
        setNotificationControlsEnabled(false);
      } else if (!messaging) {
        setNotificationStatus("Messaging SDK is not loaded on this page.");
        setNotificationControlsEnabled(false);
      } else {
        setNotificationStatus("Sign in and enable notifications.");
        setNotificationControlsEnabled(true);
      }
      setNotificationTokenPreview(safeLocalStorageGet(FCM_TOKEN_CACHE_KEY) || "");
    }

    bindForegroundPushHandler(messaging);

    if (state.cookieConsent === "accepted") {
      ensureInitialPageView();
    } else if (state.cookieConsent !== "declined") {
      ensureCookieModal().classList.remove("hidden");
    }
    window.setTimeout(() => ensureFeedbackPrompt(), 1400);

	    document.addEventListener("click", (event) => {
	      const target = event.target.closest("[data-analytics]");
	      if (!target) return;
	      logEvent(target.dataset.analytics, {
	        label: target.dataset.label || target.textContent.trim(),
	        page_path: window.location.pathname,
	      });
	    });

	    document.addEventListener("click", (event) => {
	      const social = event.target.closest(".social-link");
	      if (!social) return;
	      const href = social.getAttribute("href") || "";
	      if (!href || href === "#") {
	        event.preventDefault();
	        showToast("Social links are coming soon.");
	      }
	    });

	    document.addEventListener("click", async (event) => {
	      const plotButton = event.target.closest('[data-action="plot-forecast"]');
	      if (!plotButton) return;
	      const forecastId = plotButton.dataset.forecastId;
	      const ticker = plotButton.dataset.ticker || "";
	      if (!forecastId) return;

      if (!state.user) {
        showToast("Sign in to view saved forecasts.", "warn");
        return;
      }

	      const onForecastingPage = window.location.pathname === "/forecasting";
	      if (!onForecastingPage) {
	        logEvent("forecast_plot_navigate", { forecast_id: forecastId, ticker });
	        const params = new URLSearchParams();
	        params.set("forecastId", forecastId);
	        if (ticker) params.set("ticker", ticker);
	        window.location.href = `/forecasting?${params.toString()}`;
	        return;
	      }

	      try {
	        setTerminalStatus("Loading saved forecast...");
	        await plotForecastById(db, functions, forecastId);
	        logEvent("forecast_plotted", { forecast_id: forecastId, ticker });
	        document.getElementById("terminal")?.scrollIntoView({ behavior: "smooth" });
	      } catch (error) {
	        setTerminalStatus(error.message || "Unable to plot forecast.");
	        showToast(error.message || "Unable to plot forecast.", "warn");
	      }
	    });

    const persistenceReady = auth.setPersistence(firebase.auth.Auth.Persistence.LOCAL).catch(() => {
      showToast("Unable to set auth persistence.", "warn");
    });

	    ui.headerAuth?.addEventListener("click", () => {
	      const authSection = document.getElementById("auth");
	      if (authSection) {
	        authSection.scrollIntoView({ behavior: "smooth" });
	      } else {
	        window.location.href = "/dashboard#auth";
	      }
	    });

	    ui.workspaceSelect?.addEventListener("change", () => {
	      if (!state.user) {
	        showToast("Sign in first.", "warn");
	        renderWorkspaceSelect(null);
	        return;
	      }
	      const next = String(ui.workspaceSelect.value || "");
	      const allowed = new Set(buildWorkspaceOptions(state.user).map((o) => o.id));
	      if (!allowed.has(next)) {
	        showToast("Workspace is not available.", "warn");
	        renderWorkspaceSelect(state.user);
	        return;
	      }
	      setActiveWorkspaceId(next);
	      logEvent("workspace_switched", { workspace_id: next });
	      startUserForecasts(db, next);
	      showToast("Workspace updated.");
	    });

	    ui.collabInviteForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
	      if (!state.user) {
	        showToast("Sign in to invite collaborators.", "warn");
	        return;
	      }
	      const email = String(ui.collabInviteEmail?.value || "").trim();
	      const role = String(ui.collabInviteRole?.value || "viewer");
	      if (!email) {
	        showToast("Enter an email address.", "warn");
	        return;
	      }
	      if (ui.collabInviteStatus) ui.collabInviteStatus.textContent = "Sending invite...";
	      try {
	        const createInvite = functions.httpsCallable("create_collab_invite");
	        const result = await createInvite({ email, role, meta: buildMeta() });
	        const inviteId = result.data?.inviteId || "";
	        if (ui.collabInviteStatus) ui.collabInviteStatus.textContent = inviteId ? `Invite sent (ID: ${inviteId}).` : "Invite sent.";
	        showToast("Invite sent.");
	        logEvent("collab_invite_sent", { role });
	        if (ui.collabInviteEmail) ui.collabInviteEmail.value = "";
	        await refreshCollaboration(functions);
	      } catch (error) {
	        if (ui.collabInviteStatus) ui.collabInviteStatus.textContent = error.message || "Unable to send invite.";
	        showToast(error.message || "Unable to send invite.", "warn");
	      }
	    });

	    document.addEventListener("click", async (event) => {
	      const acceptButton = event.target.closest('[data-action="accept-collab-invite"]');
	      if (acceptButton) {
	        const inviteId = acceptButton.dataset.inviteId;
	        if (!inviteId) return;
	        if (!state.user) {
	          showToast("Sign in to accept invites.", "warn");
	          return;
	        }
	        acceptButton.disabled = true;
	        try {
	          const acceptInvite = functions.httpsCallable("accept_collab_invite");
	          await acceptInvite({ inviteId, meta: buildMeta() });
	          showToast("Invite accepted.");
	          logEvent("collab_invite_accepted", { invite_id: inviteId });
	          await refreshCollaboration(functions);
	        } catch (error) {
	          showToast(error.message || "Unable to accept invite.", "warn");
	        } finally {
	          acceptButton.disabled = false;
	        }
	        return;
	      }

	      const removeButton = event.target.closest('[data-action="remove-collaborator"]');
	      if (!removeButton) return;
	      const collaboratorUserId = removeButton.dataset.collaboratorId;
	      if (!collaboratorUserId) return;
	      if (!state.user) {
	        showToast("Sign in to manage collaborators.", "warn");
	        return;
	      }
	      removeButton.disabled = true;
	      try {
	        const remove = functions.httpsCallable("remove_collaborator");
	        await remove({ collaboratorUserId, meta: buildMeta() });
	        showToast("Collaborator removed.");
	        logEvent("collaborator_removed", { collaborator_id: collaboratorUserId });
	        await refreshCollaboration(functions);
	      } catch (error) {
	        showToast(error.message || "Unable to remove collaborator.", "warn");
	      } finally {
	        removeButton.disabled = false;
	      }
	    });

    ui.headerSignOut?.addEventListener("click", async () => {
      await unregisterCachedNotificationToken(functions);
      await auth.signOut();
      showToast("Signed out.");
      logEvent("logout", { method: "firebase" });
    });

    ui.emailForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      try {
        await persistenceReady;
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
        await persistenceReady;
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
        await persistenceReady;
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
	        const url = panel.dataset.stripeUrl || STRIPE_URL;
	        if (!url || url === "#") {
	          showToast("Payment link is not configured yet.", "warn");
	          return;
	        }
	        window.open(url, "_blank", "noreferrer");
	      });
	    });

    if (ui.terminalTicker) {
      const initialTicker =
        normalizeTicker(getQueryParam("ticker")) ||
        normalizeTicker(getQueryParam("symbol")) ||
        normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY)) ||
        normalizeTicker(ui.terminalTicker.value) ||
        "AAPL";
      const initialInterval = getQueryParam("interval") || (ui.terminalInterval?.value || "1d");
      ui.terminalTicker.value = initialTicker;
      if (ui.terminalInterval) ui.terminalInterval.value = initialInterval;
      state.tickerContext.ticker = initialTicker;
      state.tickerContext.interval = initialInterval;
      syncTickerInputs(initialTicker);

      const forecastId = getQueryParam("forecastId");
      if (forecastId) state.tickerContext.forecastId = forecastId;

      ui.terminalForm?.addEventListener("submit", async (event) => {
        event.preventDefault();
        const ticker = normalizeTicker(ui.terminalTicker?.value);
        const interval = ui.terminalInterval?.value || "1d";
        if (!ticker) {
          showToast("Enter a ticker.", "warn");
          return;
        }
        safeLocalStorageSet(LAST_TICKER_KEY, ticker);
        state.tickerContext.ticker = ticker;
        state.tickerContext.interval = interval;
        state.tickerContext.forecastDoc = null;
        state.tickerContext.forecastId = "";
        setTerminalStatus("Loading price history...");
        try {
          const rows = await loadTickerHistory(functions, ticker, interval);
          state.tickerContext.rows = rows;
          await renderTickerChart(rows, ticker, interval, state.tickerContext.indicatorOverlays || []);
          setTerminalStatus("Loaded.");
          logEvent("terminal_load", { ticker, interval });
          syncTickerInputs(ticker);
        } catch (error) {
          setTerminalStatus(error.message || "Unable to load ticker data.");
          showToast(error.message || "Unable to load ticker data.", "warn");
        }
      });

      if (ui.tickerChart) {
        setTerminalStatus("Loading price history...");
        loadTickerHistory(functions, initialTicker, initialInterval)
          .then(async (rows) => {
            state.tickerContext.rows = rows;
            await renderTickerChart(rows, initialTicker, initialInterval, state.tickerContext.indicatorOverlays || []);
            setTerminalStatus("Loaded.");
          })
          .catch((error) => {
            setTerminalStatus(error.message || "Unable to load ticker data.");
          });
      }
    }

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
        service: formData.get("service") || ui.forecastService?.value || "prophet",
        quantiles,
        meta: buildMeta(),
        utm: getUtm(),
      };

      try {
        const runForecast = functions.httpsCallable("run_timeseries_forecast");
        const result = await runForecast(payload);
        const data = result.data || {};
        const previewRows = Array.isArray(data.forecastPreview) ? data.forecastPreview.slice(0, 8) : [];
        const previewTable = previewRows.length
          ? `
            <table class="data-table" style="margin-top:12px;">
              <thead>
                <tr>
                  ${Object.keys(previewRows[0])
                    .map((key) => `<th>${escapeHtml(key)}</th>`)
                    .join("")}
                </tr>
              </thead>
              <tbody>
                ${previewRows
                  .map(
                    (row) => `
                      <tr>
                        ${Object.keys(previewRows[0])
                          .map((key) => `<td>${escapeHtml(row[key])}</td>`)
                          .join("")}
                      </tr>
                    `
                  )
                  .join("")}
              </tbody>
            </table>
          `
          : "";
        if (ui.forecastOutput) {
          ui.forecastOutput.innerHTML = `
            <div class="small"><strong>Request ID:</strong> ${data.requestId || "—"}</div>
            <div class="small"><strong>Service:</strong> ${data.service || payload.service}</div>
            <div class="small"><strong>Engine:</strong> ${data.engine || "—"}</div>
            <div class="small"><strong>Message:</strong> ${data.serviceMessage || "—"}</div>
            <div class="small"><strong>Last close:</strong> ${data.lastClose || "—"}</div>
            <div class="small"><strong>MAE (recent):</strong> ${data.mae || "—"}</div>
            <div class="small"><strong>Coverage P10–P90:</strong> ${data.coverage10_90 || "—"}</div>
            ${previewTable}
          `;
        }
        logEvent("forecast_request", { ticker: payload.ticker, interval: payload.interval, service: payload.service });
        showToast("Forecast queued and stored in your dashboard.");
        if (ui.tickerChart && data.requestId) {
          try {
            setTerminalStatus("Loading forecast for chart...");
            await plotForecastById(db, functions, data.requestId);
            document.getElementById("terminal")?.scrollIntoView({ behavior: "smooth" });
          } catch (plotError) {
            setTerminalStatus(plotError.message || "Unable to plot forecast.");
          }
        }
      } catch (error) {
        showToast(error.message || "Unable to run forecast.", "warn");
      }
    });

    ui.technicalsForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const formData = new FormData(ui.technicalsForm);
      const indicators = formData.getAll("indicators");
      const includeSeries = Boolean(ui.indicatorChart || ui.tickerChart);
      const payload = {
        ticker: formData.get("ticker"),
        interval: formData.get("interval"),
        lookback: Number(formData.get("lookback")),
        indicators,
        includeSeries,
        maxPoints: formData.get("interval") === "1h" ? 240 : 260,
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

        if (includeSeries && data.series) {
          await renderIndicatorChart(data.series);
          state.tickerContext.indicatorOverlays = buildIndicatorOverlays(data.series);
          const ticker = normalizeTicker(payload.ticker);
          if (ticker && ui.tickerChart && state.tickerContext.rows.length && state.tickerContext.ticker === ticker) {
            const forecastOverlays =
              state.tickerContext.forecastDoc && normalizeTicker(state.tickerContext.forecastDoc.ticker) === ticker
                ? buildForecastOverlays(state.tickerContext.forecastDoc.forecastRows || [])
                : [];
            await renderTickerChart(state.tickerContext.rows, ticker, payload.interval, [
              ...forecastOverlays,
              ...(state.tickerContext.indicatorOverlays || []),
            ]);
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
      if (!state.user) {
        showToast("Sign in to run the screener.", "warn");
        return;
      }
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
        if (ui.screenerOutput) ui.screenerOutput.textContent = "Running screener...";
        const runScreener = functions.httpsCallable("run_quick_screener");
        const result = await runScreener(payload);
        const rows = result.data?.results || [];
        if (ui.screenerOutput) {
          if (!rows.length) {
            ui.screenerOutput.textContent = `No results returned. Run ID: ${result.data?.runId || "—"}`;
          } else {
            ui.screenerOutput.innerHTML = `
              <div class="small"><strong>Run ID:</strong> ${result.data?.runId || "—"}</div>
              <table class="data-table" style="margin-top:12px;">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Last close</th>
                    <th>Return 1M (%)</th>
                    <th>Return 3M (%)</th>
                    <th>RSI 14</th>
                    <th>Volatility</th>
                    <th>Score</th>
                  </tr>
                </thead>
                <tbody>
                  ${rows
                    .map(
                      (row) => `
                        <tr>
                          <td>${escapeHtml(row.symbol)}</td>
                          <td>${row.lastClose ?? "—"}</td>
                          <td>${row.return1m ?? "—"}</td>
                          <td>${row.return3m ?? "—"}</td>
                          <td>${row.rsi14 ?? "—"}</td>
                          <td>${row.volatility ?? "—"}</td>
                          <td>${row.score ?? "—"}</td>
                        </tr>
                      `
                    )
                    .join("")}
                </tbody>
              </table>
            `;
          }
        }
        showToast("Screener run completed.");
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

    ui.alpacaLoadAccount?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in to load account data.", "warn");
        return;
      }
      try {
        ui.alpacaAccountOutput.textContent = "Loading account...";
        const getAccount = functions.httpsCallable("alpaca_get_account");
        const result = await getAccount({ meta: buildMeta() });
        const account = result.data?.account || {};
        ui.alpacaAccountOutput.innerHTML = toPrettyJson(account);
        logEvent("alpaca_account_loaded");
      } catch (error) {
        ui.alpacaAccountOutput.textContent = "Unable to load account.";
        showToast(error.message || "Unable to load account.", "warn");
      }
    });

    ui.alpacaLoadPositions?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in to load positions.", "warn");
        return;
      }
      try {
        ui.alpacaPositionsOutput.textContent = "Loading positions...";
        const getPositions = functions.httpsCallable("alpaca_get_positions");
        const result = await getPositions({ meta: buildMeta() });
        const positions = result.data?.positions || [];
        ui.alpacaPositionsOutput.innerHTML = toPrettyJson(positions);
        logEvent("alpaca_positions_loaded", { count: positions.length });
      } catch (error) {
        ui.alpacaPositionsOutput.textContent = "Unable to load positions.";
        showToast(error.message || "Unable to load positions.", "warn");
      }
    });

    ui.alpacaLoadOrders?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in to load broker orders.", "warn");
        return;
      }
      try {
        ui.alpacaBrokerOrdersOutput.textContent = "Loading broker orders...";
        const listOrders = functions.httpsCallable("alpaca_list_orders");
        const result = await listOrders({
          status: ui.alpacaOrderStatusFilter?.value || "all",
          limit: 50,
          meta: buildMeta(),
        });
        const orders = result.data?.orders || [];
        ui.alpacaBrokerOrdersOutput.innerHTML = toPrettyJson(orders);
        logEvent("alpaca_broker_orders_loaded", { count: orders.length });
      } catch (error) {
        ui.alpacaBrokerOrdersOutput.textContent = "Unable to load broker orders.";
        showToast(error.message || "Unable to load broker orders.", "warn");
      }
    });

    ui.alpacaCancelOrderForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.user) {
        showToast("Sign in to cancel orders.", "warn");
        return;
      }
      const orderId = ui.alpacaCancelOrderId?.value?.trim();
      if (!orderId) {
        showToast("Enter an order ID.", "warn");
        return;
      }
      try {
        ui.alpacaCancelStatus.textContent = "Cancelling order...";
        const cancelOrder = functions.httpsCallable("alpaca_cancel_order");
        const result = await cancelOrder({ orderId, meta: buildMeta() });
        const cancelled = Boolean(result.data?.cancelled);
        ui.alpacaCancelStatus.textContent = cancelled
          ? `Order cancelled: ${result.data?.orderId || orderId}`
          : "Cancel request submitted.";
        logEvent("alpaca_order_cancelled", { order_id: orderId });
        showToast("Order cancel request submitted.");
      } catch (error) {
        ui.alpacaCancelStatus.textContent = "Unable to cancel order.";
        showToast(error.message || "Unable to cancel order.", "warn");
      }
    });

    ui.notificationsEnable?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in to enable notifications.", "warn");
        return;
      }
      if (!messaging) {
        showToast("Messaging SDK is unavailable on this page.", "warn");
        return;
      }
      try {
        setNotificationStatus("Registering notification token...");
        const token = await registerNotificationToken(functions, messaging, { forceRefresh: false });
        setNotificationStatus("Notifications are enabled for this browser.");
        setNotificationTokenPreview(token);
        logEvent("notifications_enabled", { channel: "webpush" });
        showToast("Notifications enabled.");
      } catch (error) {
        setNotificationStatus(error.message || "Unable to enable notifications.");
        showToast(error.message || "Unable to enable notifications.", "warn");
      }
    });

    ui.notificationsRefresh?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in first.", "warn");
        return;
      }
      if (!messaging) {
        showToast("Messaging SDK is unavailable on this page.", "warn");
        return;
      }
      try {
        setNotificationStatus("Refreshing notification token...");
        const token = await registerNotificationToken(functions, messaging, { forceRefresh: true });
        setNotificationStatus("Notification token refreshed.");
        setNotificationTokenPreview(token);
        logEvent("notifications_token_refreshed", { channel: "webpush" });
      } catch (error) {
        setNotificationStatus(error.message || "Unable to refresh notification token.");
        showToast(error.message || "Unable to refresh notification token.", "warn");
      }
    });

    ui.notificationsSendTest?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in first.", "warn");
        return;
      }
      try {
        setNotificationStatus("Sending test notification...");
        const sendTestNotification = functions.httpsCallable("send_test_notification");
        const result = await sendTestNotification({
          title: "Quantura test",
          body: "Web push is active for your dashboard.",
          data: {
            source: "dashboard_test",
            timestamp: new Date().toISOString(),
          },
          meta: buildMeta(),
        });
        const sent = result.data?.successCount ?? 0;
        setNotificationStatus(`Test sent. Delivered to ${sent} token(s).`);
        logEvent("notifications_test_sent", { delivered: sent });
        showToast("Test notification sent.");
      } catch (error) {
        setNotificationStatus(error.message || "Unable to send test notification.");
        showToast(error.message || "Unable to send test notification.", "warn");
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
	        if (ui.collabInvitesList) ui.collabInvitesList.textContent = "Sign in to view invites.";
	        if (ui.collabCollaboratorsList) ui.collabCollaboratorsList.textContent = "Sign in to manage collaborators.";
	        ui.adminSection?.classList.add("hidden");
	        ui.navAdmin?.classList.add("hidden");
        if (ui.notificationsStatus) {
          if (!isPushSupported()) {
            setNotificationStatus("Push notifications are not supported in this browser.");
          } else if (!messaging) {
            setNotificationStatus("Messaging SDK is not loaded on this page.");
          } else {
            setNotificationStatus("Sign in and enable notifications.");
          }
          setNotificationControlsEnabled(false);
        }
	        if (state.unsubscribeOrders) state.unsubscribeOrders();
	        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
	        if (state.unsubscribeForecasts) state.unsubscribeForecasts();
	        if (state.unsubscribeAutopilot) state.unsubscribeAutopilot();
	        if (state.unsubscribePredictions) state.unsubscribePredictions();
	        if (state.unsubscribeAlpaca) state.unsubscribeAlpaca();
	        if (state.unsubscribeSharedWorkspaces) state.unsubscribeSharedWorkspaces();
	        state.sharedWorkspaces = [];
	        setActiveWorkspaceId("");
	        renderWorkspaceSelect(null);
	        return;
	      }

	      await ensureUserProfile(db, user);
	      startUserOrders(db, user);
	      subscribeSharedWorkspaces(db, user);
	      const activeWorkspaceId = resolveActiveWorkspaceId(user);
	      setActiveWorkspaceId(activeWorkspaceId);
	      renderWorkspaceSelect(user);
	      startUserForecasts(db, activeWorkspaceId);
	      startAutopilotRequests(db, user);
	      startPredictionsUploads(db, user);
	      startAlpacaOrders(db, user);
	      refreshCollaboration(functions);

      if (ui.notificationsStatus) {
        if (messaging && isPushSupported()) {
          setNotificationControlsEnabled(true);
          const cachedToken = localStorage.getItem(FCM_TOKEN_CACHE_KEY) || "";
          setNotificationTokenPreview(cachedToken);
          setNotificationStatus(cachedToken ? "Notifications enabled for this browser." : "Click Enable notifications.");
          if (Notification.permission === "granted") {
            try {
              await registerNotificationToken(functions, messaging, { forceRefresh: !cachedToken });
              setNotificationStatus("Notifications enabled for this browser.");
            } catch (error) {
              setNotificationStatus(error.message || "Unable to initialize notifications.");
            }
          }
        } else {
          setNotificationControlsEnabled(false);
        }
      }

      if (
        window.location.pathname === "/forecasting" &&
        ui.tickerChart &&
        state.tickerContext.forecastId &&
        !state.tickerContext.forecastDoc
      ) {
        try {
          setTerminalStatus("Loading saved forecast...");
          await plotForecastById(db, functions, state.tickerContext.forecastId);
        } catch (error) {
          setTerminalStatus(error.message || "Unable to load saved forecast.");
        }
      }

      if (user.email === ADMIN_EMAIL) {
        ui.adminSection?.classList.remove("hidden");
        ui.navAdmin?.classList.remove("hidden");
        startAdminOrders(db);
      } else {
        ui.adminSection?.classList.add("hidden");
        ui.navAdmin?.classList.add("hidden");
        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
      }
    });
  };

  window.addEventListener("load", init);
})();
