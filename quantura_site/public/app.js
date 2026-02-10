(() => {
  const ADMIN_EMAIL = "tamzid257@gmail.com";
  const STRIPE_URL = "https://buy.stripe.com/8x24gze7a86K1zE9M20co08";
  const FCM_TOKEN_CACHE_KEY = "quantura_fcm_token";
  const COOKIE_CONSENT_KEY = "quantura_cookie_consent";
  const FEEDBACK_PROMPT_KEY = "quantura_feedback_prompt_v1";
  const LAST_TICKER_KEY = "quantura_last_ticker";
  const WORKSPACE_KEY = "quantura_active_workspace";
  const OPTIONS_EXPIRATION_PREFIX = "quantura_options_expiration_";
  const THEME_KEY = "quantura_theme";

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
    optionsExpiration: document.getElementById("options-expiration"),
    optionsOutput: document.getElementById("options-output"),
    screenerForm: document.getElementById("screener-form"),
    screenerOutput: document.getElementById("screener-output"),
    watchlistForm: document.getElementById("watchlist-form"),
    watchlistTicker: document.getElementById("watchlist-ticker"),
    watchlistNotes: document.getElementById("watchlist-notes"),
    watchlistList: document.getElementById("watchlist-list"),
    alertForm: document.getElementById("alert-form"),
    alertTicker: document.getElementById("alert-ticker"),
    alertCondition: document.getElementById("alert-condition"),
    alertPrice: document.getElementById("alert-price"),
    alertNotes: document.getElementById("alert-notes"),
    alertsList: document.getElementById("alerts-list"),
    alertsStatus: document.getElementById("alerts-status"),
    alertsCheck: document.getElementById("alerts-check"),
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
	    taskForm: document.getElementById("task-form"),
	    taskTitle: document.getElementById("task-title"),
	    taskDue: document.getElementById("task-due"),
	    taskStatus: document.getElementById("task-status"),
	    taskAssignee: document.getElementById("task-assignee"),
	    taskNotes: document.getElementById("task-notes"),
	    taskStatusText: document.getElementById("task-status-text"),
	    productivityBoard: document.getElementById("productivity-board"),
	    tasksCalendar: document.getElementById("tasks-calendar"),
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
	    authResolved: false,
	    tickerContext: {
	      ticker: "",
	      interval: "1d",
	      rows: [],
      forecastId: "",
      forecastDoc: null,
      indicatorOverlays: [],
      forecastTablePage: 0,
    },
    unsubscribeOrders: null,
    unsubscribeAdmin: null,
	    unsubscribeForecasts: null,
	    unsubscribeAutopilot: null,
	    unsubscribePredictions: null,
	    unsubscribeAlpaca: null,
	    unsubscribeTasks: null,
	    unsubscribeWatchlist: null,
	    unsubscribeAlerts: null,
	    messagingBound: false,
	    remoteConfigLoaded: false,
	    remoteFlags: {
	      assistantEnabled: true,
	      watchlistEnabled: true,
	      webPushVapidKey: "",
	    },
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

	  const skeletonHtml = (lines = 4) => {
	    const widths = ["92%", "78%", "88%", "64%", "84%"];
	    const blocks = Array.from({ length: Math.max(2, Math.min(lines, 8)) }).map((_, idx) => {
	      const width = widths[idx % widths.length];
	      return `<div class="skeleton-line" style="width:${width}"></div>`;
	    });
	    return `<div class="skeleton" aria-hidden="true">${blocks.join("")}</div>`;
	  };

	  const setOutputLoading = (el, label = "Loading...") => {
	    if (!el) return;
	    el.setAttribute("aria-busy", "true");
	    el.innerHTML = `${skeletonHtml()}<div class="small muted" style="margin-top:10px;">${label}</div>`;
	  };

		  const setOutputReady = (el) => {
		    if (!el) return;
		    el.removeAttribute("aria-busy");
		  };

		  const bindPanelNavigation = () => {
		    const panelsRoot = document.querySelector("[data-panels]");
		    if (!panelsRoot) return;
		    const panels = Array.from(panelsRoot.querySelectorAll("[data-panel]"));
		    const panelNames = new Set(panels.map((panel) => String(panel.dataset.panel || "").trim()).filter(Boolean));
		    const buttons = Array.from(document.querySelectorAll("[data-panel-target]")).filter((btn) =>
		      panelNames.has(String(btn.dataset.panelTarget || "").trim())
		    );
		    if (buttons.length === 0 || panels.length === 0) return;

		    const routerKey = String(panelsRoot.dataset.panelRouter || "").trim();
		    const routers = {
		      terminal: {
		        defaultPanel: "forecast",
		        panelToPath: {
		          forecast: "/forecasting",
		          indicators: "/indicators",
		          news: "/news",
		          options: "/options",
		          saved: "/saved-forecasts",
		          learn: "/studio",
		        },
		      },
		      dashboard: {
		        defaultPanel: "orders",
		        panelToPath: {
		          orders: "/dashboard",
		          watchlist: "/watchlist",
		          productivity: "/productivity",
		          collaboration: "/collaboration",
		          uploads: "/uploads",
		          autopilot: "/autopilot",
		          alpaca: "/trading",
		          notifications: "/notifications",
		          purchase: "/purchase",
		          auth: "/account",
		        },
		      },
		    };
		    const router = routers[routerKey] || null;
		    const pathToPanel = (() => {
		      if (!router) return {};
		      const mapping = {};
		      Object.entries(router.panelToPath || {}).forEach(([panel, path]) => {
		        mapping[String(path)] = String(panel);
		      });
		      return mapping;
		    })();

		    const setActive = (target, { pushPath = true } = {}) => {
		      const next = String(target || "").trim();
		      if (!next) return;
		      panels.forEach((panel) => panel.classList.toggle("hidden", panel.dataset.panel !== next));
		      buttons.forEach((btn) => btn.classList.toggle("active", btn.dataset.panelTarget === next));
		      if (pushPath && router?.panelToPath?.[next]) {
		        const desired = router.panelToPath[next];
		        if (desired && window.location.pathname !== desired) {
		          try {
		            history.pushState({ panel: next }, "", `${desired}${window.location.search}`);
		          } catch (error) {
		            // Ignore.
		          }
		        }
		      }
		      logEvent("panel_view", { panel: next, page_path: window.location.pathname });
		    };

		    const initialFromUrl = () => {
		      try {
		        const params = new URLSearchParams(window.location.search);
		        const panel = params.get("panel");
		        if (panel) return panel;
		      } catch (error) {
		        // Ignore.
		      }
		      if (router && pathToPanel[window.location.pathname]) {
		        return pathToPanel[window.location.pathname];
		      }
		      return (window.location.hash || "").replace(/^#/, "");
		    };

		    buttons.forEach((btn) => {
		      btn.addEventListener("click", (event) => {
		        event.preventDefault?.();
		        setActive(btn.dataset.panelTarget);
		      });
		    });

		    const initial = initialFromUrl() || router?.defaultPanel || buttons[0].dataset.panelTarget;
		    setActive(initial, { pushPath: false });

		    window.addEventListener("popstate", () => {
		      const next = initialFromUrl();
		      if (next) setActive(next, { pushPath: false });
		    });
		  };

  const syncStickyOffsets = () => {
    const header = document.querySelector(".header");
    const headerHeight = header ? header.getBoundingClientRect().height : 88;
    document.documentElement.style.setProperty("--header-height", `${Math.round(headerHeight)}px`);

    const gutter = 16;
    const dockTop = Math.round(headerHeight + gutter);
    document.documentElement.style.setProperty("--dock-top", `${dockTop}px`);

    const dock = document.querySelector(".ticker-dock");
    const dockHeight = dock ? dock.getBoundingClientRect().height : 0;
    if (dockHeight) {
      const chartTop = Math.round(dockTop + dockHeight + gutter);
      document.documentElement.style.setProperty("--studio-chart-top", `${chartTop}px`);
    }
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

  const getRemoteConfigClient = () => {
    try {
      if (typeof firebase === "undefined") return null;
      if (!firebase.remoteConfig) return null;
      const rc = firebase.remoteConfig();
      if (rc.settings) {
        rc.settings.minimumFetchIntervalMillis = 5 * 60 * 1000;
      } else {
        rc.settings = { minimumFetchIntervalMillis: 5 * 60 * 1000 };
      }
      rc.defaultConfig = {
        assistant_enabled: true,
        watchlist_enabled: true,
        forecast_prophet_enabled: true,
        forecast_timemixer_enabled: true,
        webpush_vapid_key: "",
      };
      return rc;
    } catch (error) {
      return null;
    }
  };

  const applyRemoteFlags = (flags) => {
    const assistantRoot = document.getElementById("assistant-widget");
    if (assistantRoot) assistantRoot.classList.toggle("hidden", !flags.assistantEnabled);

    document.querySelectorAll('[data-panel-target="watchlist"]').forEach((el) => {
      el.classList.toggle("hidden", !flags.watchlistEnabled);
    });
    document.querySelectorAll('[data-panel="watchlist"]').forEach((el) => {
      if (!flags.watchlistEnabled) el.classList.add("hidden");
    });
  };

  const loadRemoteConfig = async () => {
    const rc = getRemoteConfigClient();
    if (!rc) return state.remoteFlags;
    try {
      await rc.fetchAndActivate();
    } catch (error) {
      // Ignore fetch errors and fall back to defaults.
    }
    const assistantEnabled = typeof rc.getBoolean === "function" ? rc.getBoolean("assistant_enabled") : String(rc.getString("assistant_enabled")).toLowerCase() === "true";
    const watchlistEnabled = typeof rc.getBoolean === "function" ? rc.getBoolean("watchlist_enabled") : String(rc.getString("watchlist_enabled")).toLowerCase() === "true";
    const webPushVapidKey = typeof rc.getString === "function" ? rc.getString("webpush_vapid_key") : "";
    state.remoteFlags = {
      ...state.remoteFlags,
      assistantEnabled: Boolean(assistantEnabled),
      watchlistEnabled: Boolean(watchlistEnabled),
      webPushVapidKey: String(webPushVapidKey || ""),
    };
    state.remoteConfigLoaded = true;
    applyRemoteFlags(state.remoteFlags);
    logEvent("remote_config_loaded", { assistant: state.remoteFlags.assistantEnabled, watchlist: state.remoteFlags.watchlistEnabled });
    return state.remoteFlags;
  };

  let ephemeralSessionId = "";
  const getSessionId = () => {
    if (ephemeralSessionId) return ephemeralSessionId;
    const key = "quantura_session_id";
    try {
      const existing = localStorage.getItem(key);
      if (existing) {
        ephemeralSessionId = existing;
        return existing;
      }
      const sessionId = `qs_${Math.random().toString(36).slice(2, 11)}${Date.now().toString(36)}`;
      localStorage.setItem(key, sessionId);
      ephemeralSessionId = sessionId;
      return sessionId;
    } catch (error) {
      // Some browsers/extensions block storage access. Keep a stable per-page session id anyway.
      ephemeralSessionId = `qs_${Math.random().toString(36).slice(2, 11)}${Date.now().toString(36)}`;
      return ephemeralSessionId;
    }
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

	    const resolveThemePreference = () => {
      try {
        const stored = localStorage.getItem(THEME_KEY);
        if (stored === "dark" || stored === "light") return stored;
      } catch (error) {
        // Ignore storage errors.
      }
      try {
        return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
      } catch (error) {
        return "light";
      }
	    };

	    const isDarkMode = () => document.documentElement.dataset.theme === "dark";

	    const LOGO_LIGHT = "/assets/quantura-logo.svg";
	    const LOGO_DARK = "/assets/quantura-logo-dark.svg";

	    const syncBrandAssets = (theme) => {
	      const desiredLogo = theme === "dark" ? LOGO_DARK : LOGO_LIGHT;
	      document
	        .querySelectorAll('img[src$="quantura-logo.svg"], img[src$="quantura-logo-dark.svg"]')
	        .forEach((img) => {
	          if (img.getAttribute("src") !== desiredLogo) {
	            img.setAttribute("src", desiredLogo);
	          }
	        });
	    };

	    const applyTheme = (theme, { persist = true } = {}) => {
	      const next = theme === "dark" ? "dark" : "light";
	      document.documentElement.dataset.theme = next;
	      syncBrandAssets(next);
	      if (persist) safeLocalStorageSet(THEME_KEY, next);
	      const button = document.getElementById("theme-toggle");
	      if (button) {
	        button.textContent = next === "dark" ? "Light mode" : "Dark mode";
	        button.setAttribute("aria-label", next === "dark" ? "Switch to light mode" : "Switch to dark mode");
	      }
	    };

    const ensureThemeToggle = () => {
      if (document.getElementById("theme-toggle")) return;
      const host = document.querySelector(".nav-actions");
      if (!host) return;
      const button = document.createElement("button");
      button.id = "theme-toggle";
      button.type = "button";
      button.className = "cta secondary small theme-toggle";
      button.textContent = isDarkMode() ? "Light mode" : "Dark mode";
      button.addEventListener("click", () => {
        const next = isDarkMode() ? "light" : "dark";
        applyTheme(next, { persist: true });
        logEvent("theme_toggled", { theme: next, page_path: window.location.pathname });
      });
      host.insertBefore(button, host.firstChild);
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
	          if (resolved) {
	            startUserForecasts(db, resolved);
	            startWorkspaceTasks(db, resolved);
	            startWatchlist(db, resolved);
	            startPriceAlerts(db, resolved);
	          }
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
        Share what you were trying to do and what could be better. This feedback is stored privately to your account.
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
        if (typeof firebase === "undefined") throw new Error("App services are not loaded.");
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
		    if (document.getElementById("feedback-fab")) return null;

      const lastShown = Number(safeLocalStorageGet(FEEDBACK_PROMPT_KEY) || "0");
      const weekMs = 7 * 24 * 60 * 60 * 1000;
      const shouldPulse = !lastShown || Date.now() - lastShown > weekMs;

      const button = document.createElement("button");
      button.id = "feedback-fab";
      button.type = "button";
      button.className = `feedback-fab${shouldPulse ? " pulse" : ""}`;
      button.textContent = "Feedback";
      button.setAttribute("aria-label", "Send feedback to Quantura");
      document.body.appendChild(button);

      button.addEventListener("click", () => {
        ensureFeedbackModal().classList.remove("hidden");
        button.classList.remove("pulse");
        safeLocalStorageSet(FEEDBACK_PROMPT_KEY, String(Date.now()));
        logEvent("feedback_opened", { page_path: window.location.pathname });
      });

      return button;
		  };

	  const ensureAssistantWidget = (functions) => {
	    if (document.getElementById("assistant-widget")) return;

	    const root = document.createElement("div");
	    root.id = "assistant-widget";

		    const launcher = document.createElement("button");
		    launcher.type = "button";
		    launcher.className = "assistant-launcher";
		    launcher.setAttribute("aria-label", "Open Quantura assistant");
		    launcher.textContent = "Ask";

	    const panel = document.createElement("div");
	    panel.className = "assistant-panel hidden";
	    panel.setAttribute("role", "dialog");
	    panel.setAttribute("aria-modal", "false");

	    const header = document.createElement("div");
	    header.className = "assistant-header";
	    const title = document.createElement("div");
	    title.className = "assistant-title";
	    title.textContent = "Quantura Assistant";
	    const close = document.createElement("button");
	    close.type = "button";
	    close.className = "assistant-close";
	    close.textContent = "Close";
	    header.appendChild(title);
	    header.appendChild(close);

	    const messages = document.createElement("div");
	    messages.className = "assistant-messages";

	    const form = document.createElement("form");
	    form.className = "assistant-form";
	    const input = document.createElement("input");
	    input.className = "assistant-input";
	    input.type = "text";
	    input.placeholder = "Ask about forecasts, indicators, or this ticker...";
	    input.autocomplete = "off";
	    const send = document.createElement("button");
	    send.type = "submit";
	    send.className = "assistant-send";
	    send.textContent = "Send";
	    form.appendChild(input);
	    form.appendChild(send);

	    const hint = document.createElement("div");
	    hint.className = "assistant-hint";
	    hint.textContent = "Tip: load a ticker first, then ask for a plan (forecast, indicators, news, options).";

	    panel.appendChild(header);
	    panel.appendChild(messages);
	    panel.appendChild(hint);
	    panel.appendChild(form);

	    root.appendChild(launcher);
	    root.appendChild(panel);
	    document.body.appendChild(root);

	    const appendMessage = (role, text, opts = {}) => {
	      const bubble = document.createElement("div");
	      bubble.className = `assistant-msg assistant-msg--${role}`;
	      if (opts.loading) bubble.classList.add("assistant-msg--loading");
	      bubble.textContent = text;
	      messages.appendChild(bubble);
	      messages.scrollTop = messages.scrollHeight;
	      return bubble;
	    };

	    const openPanel = () => {
	      panel.classList.remove("hidden");
	      input.focus();
	      logEvent("assistant_opened", { page_path: window.location.pathname });
	      if (!messages.childElementCount) {
	        appendMessage(
	          "assistant",
	          state.user
	            ? "Ask me to interpret indicators, suggest a forecasting setup, or summarize what to check next for this ticker."
	            : "Sign in to use the assistant. Messages are saved privately to your account."
	        );
	      }
	    };

	    const closePanel = () => panel.classList.add("hidden");

	    launcher.addEventListener("click", () => {
	      if (panel.classList.contains("hidden")) openPanel();
	      else closePanel();
	    });

	    close.addEventListener("click", closePanel);

	    form.addEventListener("submit", async (event) => {
	      event.preventDefault();
	      const text = String(input.value || "").trim();
	      if (!text) return;
	      input.value = "";
	      appendMessage("user", text);
	      if (!state.user) {
	        appendMessage("assistant", "Sign in to use the assistant.");
	        showToast("Sign in to use the assistant.", "warn");
	        return;
	      }

	      const loading = appendMessage("assistant", "Thinking...", { loading: true });
	      try {
	        const ticker = state.tickerContext.ticker || safeLocalStorageGet(LAST_TICKER_KEY) || "";
	        const chat = functions.httpsCallable("assistant_chat");
	        const result = await chat({ message: text, ticker, page: window.location.pathname, meta: buildMeta() });
	        const reply = result.data?.text || "No response returned.";
	        loading.textContent = reply;
	        loading.classList.remove("assistant-msg--loading");
	        logEvent("assistant_message", { page_path: window.location.pathname });
	      } catch (error) {
	        loading.textContent = error.message || "Assistant request failed.";
	        loading.classList.remove("assistant-msg--loading");
	      }
	    });
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
        note.textContent = "You must sign in to purchase. Checkout is secured to your account.";
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

	  const resolveWorkspaceRole = (workspaceId) => {
	    if (!state.user || !workspaceId) return "guest";
	    if (state.user.uid === workspaceId) return "owner";
	    const shared = (state.sharedWorkspaces || []).find((ws) => ws.workspaceUserId === workspaceId || ws.id === workspaceId);
	    return shared?.role || "viewer";
	  };

	  const canWriteWorkspace = (workspaceId) => {
	    const role = resolveWorkspaceRole(workspaceId);
	    return role === "owner" || role === "editor";
	  };

	  const renderTaskBoard = (tasks, workspaceId) => {
	    if (!ui.productivityBoard) return;
	    const editable = canWriteWorkspace(workspaceId);
	    const buckets = { backlog: [], doing: [], done: [] };
	    (tasks || []).forEach((task) => {
	      const status = String(task.status || "backlog");
	      if (status in buckets) buckets[status].push(task);
	      else buckets.backlog.push(task);
	    });

	    const taskCard = (task) => {
	      const title = escapeHtml(task.title || "Untitled");
	      const due = task.dueDate ? new Date(task.dueDate).toLocaleDateString() : "";
	      const assignee = escapeHtml(task.assigneeEmail || "");
	      const meta = [due ? `Due ${due}` : "", assignee ? `Assignee: ${assignee}` : ""].filter(Boolean).join(" · ");
	      const actions = editable
	        ? `
	          <div class="task-actions">
	            <button class="task-chip" type="button" data-action="task-move" data-task-id="${escapeHtml(task.id)}" data-to="backlog">Backlog</button>
	            <button class="task-chip" type="button" data-action="task-move" data-task-id="${escapeHtml(task.id)}" data-to="doing">Doing</button>
	            <button class="task-chip" type="button" data-action="task-move" data-task-id="${escapeHtml(task.id)}" data-to="done">Done</button>
	            <button class="task-chip danger" type="button" data-action="task-delete" data-task-id="${escapeHtml(task.id)}">Delete</button>
	          </div>
	        `
	        : "";
	      return `
	        <div class="task-card" draggable="${editable ? "true" : "false"}" data-task-id="${escapeHtml(task.id)}">
	          <div class="task-title">${title}</div>
	          ${meta ? `<div class="small task-meta muted">${meta}</div>` : ""}
	          ${actions}
	        </div>
	      `;
	    };

	    const col = (key, label, items) => `
	      <div class="kanban-col" data-task-dropzone="${key}">
	        <div class="kanban-col-header">
	          <strong>${label}</strong>
	          <span class="pill">${items.length}</span>
	        </div>
	        <div class="kanban-col-body">
	          ${items.length ? items.map(taskCard).join("") : `<div class="small muted">No tasks</div>`}
	        </div>
	      </div>
	    `;

	    ui.productivityBoard.innerHTML = `
	      <div class="kanban" data-workspace-id="${escapeHtml(workspaceId)}">
	        ${col("backlog", "Backlog", buckets.backlog)}
	        ${col("doing", "Doing", buckets.doing)}
	        ${col("done", "Done", buckets.done)}
	      </div>
	    `;
	  };

	  const renderTaskCalendar = (tasks) => {
	    if (!ui.tasksCalendar) return;
	    const upcoming = (tasks || [])
	      .filter((t) => t.dueDate)
	      .slice()
	      .sort((a, b) => String(a.dueDate).localeCompare(String(b.dueDate)))
	      .slice(0, 12);
	    if (!upcoming.length) {
	      ui.tasksCalendar.textContent = "Tasks with due dates will appear here.";
	      ui.tasksCalendar.classList.add("muted");
	      return;
	    }
	    ui.tasksCalendar.classList.remove("muted");
	    ui.tasksCalendar.innerHTML = `
	      <div class="task-upcoming">
	        ${upcoming
	          .map((t) => {
	            const due = new Date(t.dueDate).toLocaleDateString();
	            return `
	              <div class="task-upcoming-row">
	                <div class="small"><strong>${escapeHtml(t.title || "Untitled")}</strong></div>
	                <div class="small muted">${escapeHtml(due)} · ${escapeHtml(String(t.status || "backlog"))}</div>
	              </div>
	            `;
	          })
	          .join("")}
	      </div>
	    `;
	  };

	  const startWorkspaceTasks = (db, workspaceId) => {
	    if (state.unsubscribeTasks) state.unsubscribeTasks();
	    const containers = [ui.productivityBoard, ui.tasksCalendar].filter(Boolean);
	    if (!workspaceId || containers.length === 0) return;

	    state.unsubscribeTasks = db
	      .collection("users")
	      .doc(workspaceId)
	      .collection("tasks")
	      .orderBy("createdAt", "desc")
	      .limit(200)
	      .onSnapshot(
	        (snapshot) => {
	          const tasks = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
	          renderTaskBoard(tasks, workspaceId);
	          renderTaskCalendar(tasks);
	        },
	        () => {
	          if (ui.productivityBoard) ui.productivityBoard.innerHTML = `<div class="small muted">Unable to load tasks.</div>`;
	          if (ui.tasksCalendar) ui.tasksCalendar.textContent = "Unable to load tasks.";
	        }
	      );
	  };

	  const renderWatchlist = (items, workspaceId) => {
	    if (!ui.watchlistList) return;
	    const editable = canWriteWorkspace(workspaceId);
	    const list = Array.isArray(items) ? items : [];
	    if (!list.length) {
	      ui.watchlistList.innerHTML = `<div class="small muted">No watchlist items yet.</div>`;
	      return;
	    }

	    ui.watchlistList.innerHTML = list
	      .map((item) => {
	        const ticker = normalizeTicker(item.ticker || item.id || "");
	        if (!ticker) return "";
	        const notes = escapeHtml(item.notes || "");
	        const addedBy = escapeHtml(item.addedBy?.email || item.addedByEmail || "");
	        const metaParts = [addedBy ? `Added by ${addedBy}` : "", item.updatedAt ? `Updated ${formatTimestamp(item.updatedAt)}` : ""].filter(Boolean);
	        const meta = metaParts.length ? `<div class="small muted">${metaParts.join(" · ")}</div>` : "";
	        const actions = editable
	          ? `
	            <div class="task-actions">
	              <button class="task-chip danger" type="button" data-action="watchlist-remove" data-ticker="${escapeHtml(ticker)}">Remove</button>
	            </div>
	          `
	          : "";
	        return `
	          <div class="watchlist-item">
	            <div>
	              <button class="ticker-pill" type="button" data-action="pick-ticker" data-ticker="${escapeHtml(ticker)}">${escapeHtml(ticker)}</button>
	              ${meta}
	              ${notes ? `<div class="small">${notes}</div>` : ""}
	            </div>
	            ${actions}
	          </div>
	        `;
	      })
	      .join("");
	  };

	  const startWatchlist = (db, workspaceId) => {
	    if (state.unsubscribeWatchlist) state.unsubscribeWatchlist();
	    if (!workspaceId || !ui.watchlistList || !state.remoteFlags.watchlistEnabled) return;
	    ui.watchlistList.innerHTML = `<div class="small muted">Loading watchlist...</div>`;
	    state.unsubscribeWatchlist = db
	      .collection("users")
	      .doc(workspaceId)
	      .collection("watchlist")
	      .orderBy("createdAt", "desc")
	      .limit(250)
	      .onSnapshot(
	        (snapshot) => {
	          const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
	          renderWatchlist(items, workspaceId);
	        },
	        () => {
	          if (ui.watchlistList) ui.watchlistList.innerHTML = `<div class="small muted">Unable to load watchlist.</div>`;
	        }
	      );
	  };

	  const renderAlerts = (items, workspaceId) => {
	    if (!ui.alertsList) return;
	    const editable = canWriteWorkspace(workspaceId);
	    const list = Array.isArray(items) ? items : [];
	    if (!list.length) {
	      ui.alertsList.innerHTML = `<div class="small muted">No alerts yet.</div>`;
	      return;
	    }

	    ui.alertsList.innerHTML = list
	      .map((item) => {
	        const ticker = normalizeTicker(item.ticker || "");
	        const condition = String(item.condition || "above");
	        const target = Number(item.targetPrice ?? item.target ?? item.price);
	        const active = Boolean(item.active);
	        const status = String(item.status || (active ? "active" : "disabled"));
	        const createdBy = escapeHtml(item.createdByEmail || item.createdBy?.email || "");
	        const lastPrice = typeof item.lastPrice === "number" ? `$${item.lastPrice.toFixed(2)}` : "";
	        const lastChecked = item.lastCheckedAt ? `Checked ${formatTimestamp(item.lastCheckedAt)}` : "";
	        const triggeredAt = item.triggeredAt ? `Triggered ${formatTimestamp(item.triggeredAt)}` : "";
	        const metaParts = [createdBy ? `By ${createdBy}` : "", lastChecked, lastPrice, triggeredAt].filter(Boolean);
	        const meta = metaParts.length ? `<div class="small muted">${metaParts.join(" · ")}</div>` : "";
	        const title = `${escapeHtml(ticker)} ${condition === "below" ? "below" : "above"} ${Number.isFinite(target) ? `$${target.toFixed(2)}` : "—"}`;
	        const actions = editable
	          ? `
	            <div class="task-actions">
	              <button class="task-chip" type="button" data-action="alert-toggle" data-alert-id="${escapeHtml(item.id)}" data-active="${active ? "1" : "0"}">
	                ${active ? "Disable" : "Enable"}
	              </button>
	              <button class="task-chip danger" type="button" data-action="alert-delete" data-alert-id="${escapeHtml(item.id)}">Delete</button>
	            </div>
	          `
	          : "";
	        return `
	          <div class="alert-item" data-status="${escapeHtml(status)}">
	            <div class="alert-title"><strong>${title}</strong> <span class="pill">${escapeHtml(status)}</span></div>
	            ${meta}
	            ${item.notes ? `<div class="small">${escapeHtml(item.notes)}</div>` : ""}
	            ${actions}
	          </div>
	        `;
	      })
	      .join("");
	  };

	  const startPriceAlerts = (db, workspaceId) => {
	    if (state.unsubscribeAlerts) state.unsubscribeAlerts();
	    if (!workspaceId || !ui.alertsList || !state.remoteFlags.watchlistEnabled) return;
	    ui.alertsList.innerHTML = `<div class="small muted">Loading alerts...</div>`;
	    state.unsubscribeAlerts = db
	      .collection("users")
	      .doc(workspaceId)
	      .collection("price_alerts")
	      .orderBy("createdAt", "desc")
	      .limit(250)
	      .onSnapshot(
	        (snapshot) => {
	          const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
	          renderAlerts(items, workspaceId);
	        },
	        () => {
	          if (ui.alertsList) ui.alertsList.innerHTML = `<div class="small muted">Unable to load alerts.</div>`;
	        }
	      );
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

  const parseQuantilesInput = (raw) => {
    const parts = Array.isArray(raw) ? raw : String(raw || "").split(",");
    const values = [];
    const seen = new Set();
    for (const part of parts) {
      const trimmed = String(part).trim();
      if (!trimmed) continue;
      const q = Number(trimmed);
      if (!Number.isFinite(q)) {
        throw new Error(`Invalid quantile value: ${trimmed}`);
      }
      if (!(q > 0 && q < 1)) {
        throw new Error("Quantiles must be between 0 and 1 (exclusive).");
      }
      const key = Math.round(q * 10000);
      if (seen.has(key)) continue;
      seen.add(key);
      values.push(q);
    }
    if (!values.length) {
      throw new Error("Enter at least one quantile (example: 0.1,0.5,0.9).");
    }
    if (values.length > 12) {
      throw new Error("Too many quantiles (max 12).");
    }
    return values;
  };

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
      "watchlist-ticker",
      "alert-ticker",
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

      const dark = isDarkMode();
      const plotBg = dark ? "#0b0f1a" : "#ffffff";
      const textColor = dark ? "rgba(246, 244, 238, 0.92)" : "#12182a";
      const gridColor = dark ? "rgba(246, 244, 238, 0.14)" : "rgba(18, 24, 42, 0.12)";
	    const layout = {
	      title: { text: `${ticker} (${interval})`, font: { family: "Manrope, sans-serif", size: 16, color: textColor } },
	      font: { family: "Manrope, sans-serif", color: textColor },
	      paper_bgcolor: "rgba(0,0,0,0)",
	      plot_bgcolor: plotBg,
	      margin: { l: 50, r: 30, t: 40, b: 40 },
	      xaxis: {
	        rangeslider: { visible: true },
	        showspikes: true,
	        spikemode: "across",
	        spikesnap: "cursor",
          gridcolor: gridColor,
          zerolinecolor: gridColor,
	      },
	      yaxis: {
          showspikes: true,
          spikemode: "across",
          spikesnap: "cursor",
          gridcolor: gridColor,
          zerolinecolor: gridColor,
        },
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

      const dark = isDarkMode();
      const plotBg = dark ? "#0b0f1a" : "#ffffff";
      const textColor = dark ? "rgba(246, 244, 238, 0.92)" : "#12182a";
      const gridColor = dark ? "rgba(246, 244, 238, 0.14)" : "rgba(18, 24, 42, 0.12)";
	    const layout = {
	      title: { text: "Technical indicators", font: { family: "Manrope, sans-serif", size: 14, color: textColor } },
	      font: { family: "Manrope, sans-serif", color: textColor },
	      paper_bgcolor: "rgba(0,0,0,0)",
	      plot_bgcolor: plotBg,
	      margin: { l: 50, r: 30, t: 40, b: 40 },
	      xaxis: { showspikes: true, spikemode: "across", spikesnap: "cursor", gridcolor: gridColor, zerolinecolor: gridColor },
	      yaxis: { zeroline: false, gridcolor: gridColor },
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
    const sample = forecastRows[0] || {};
    const quantKeys = Object.keys(sample)
      .filter((key) => /^q\\d\\d$/.test(key))
      .sort();
    if (!quantKeys.length) return [];

    const entries = quantKeys
      .map((key) => ({ key, q: Number(key.slice(1)) / 100 }))
      .filter((item) => Number.isFinite(item.q))
      .sort((a, b) => a.q - b.q);

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

    if (entries.length >= 2) {
      const low = entries[0];
      const high = entries[entries.length - 1];
      addBand(low.key, high.key, `P${Math.round(low.q * 100)}-P${Math.round(high.q * 100)}`, "rgba(58, 181, 162, 0.16)");
    }
    if (entries.length >= 4) {
      const low = entries[1];
      const high = entries[entries.length - 2];
      addBand(low.key, high.key, `P${Math.round(low.q * 100)}-P${Math.round(high.q * 100)}`, "rgba(240, 180, 41, 0.18)");
    }

    const median =
      entries.find((item) => item.key === "q50") ||
      entries.reduce((best, item) => (Math.abs(item.q - 0.5) < Math.abs(best.q - 0.5) ? item : best), entries[0]);
    const medianColor = isDarkMode() ? "#e9edf7" : "#12182a";
    overlays.push({
      type: "scatter",
      mode: "lines",
      name: median.key === "q50" ? "Median forecast" : `Quantile P${Math.round(median.q * 100)}`,
      x,
      y: forecastRows.map((row) => row[median.key]),
      line: { width: 2, color: medianColor, dash: "dot" },
    });

    return overlays;
  };

  const formatForecastCell = (value) => {
    if (value === null || value === undefined) return "—";
    if (typeof value === "number" && Number.isFinite(value)) {
      return value.toFixed(2);
    }
    const asNum = Number(value);
    if (Number.isFinite(asNum)) return asNum.toFixed(2);
    return String(value);
  };

  const labelForecastService = (raw) => {
    const key = String(raw || "").trim().toLowerCase();
    if (key === "prophet") return "Meta Prophet";
    if (key === "ibm_timemixer") return "IBM TimeMixer";
    return raw ? String(raw) : "Forecast";
  };

  const renderForecastDetails = (forecastDoc) => {
    if (!ui.forecastOutput || !forecastDoc) return;
    const rows = Array.isArray(forecastDoc.forecastRows) ? forecastDoc.forecastRows : [];
    if (!rows.length) {
      setOutputReady(ui.forecastOutput);
      ui.forecastOutput.innerHTML = `<div class="small muted">No forecast rows were stored for this run.</div>`;
      return;
    }

    const quantKeys = Object.keys(rows[0] || {})
      .filter((key) => /^q\\d\\d$/.test(key))
      .sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
    const headers = ["ds", ...quantKeys];

    const pageSize = 25;
    const total = rows.length;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const page = Math.max(0, Math.min(totalPages - 1, Number(state.tickerContext.forecastTablePage || 0)));
    state.tickerContext.forecastTablePage = page;

    const start = page * pageSize;
    const end = Math.min(total, start + pageSize);
    const slice = rows.slice(start, end);

    const quantileLabel = Array.isArray(forecastDoc.quantiles)
      ? forecastDoc.quantiles.map((q) => `P${Math.round(Number(q) * 100)}`).filter(Boolean).join(", ")
      : "";
    const metrics = forecastDoc.metrics || {};
    const summary = [
      `<div class="small"><strong>Forecast ID:</strong> ${escapeHtml(forecastDoc.id || "")}</div>`,
      `<div class="small"><strong>Service:</strong> ${escapeHtml(labelForecastService(forecastDoc.service))}</div>`,
      forecastDoc.engine ? `<div class="small"><strong>Engine:</strong> ${escapeHtml(forecastDoc.engine)}</div>` : "",
      quantileLabel ? `<div class="small"><strong>Quantiles:</strong> ${escapeHtml(quantileLabel)}</div>` : "",
      metrics.lastClose ? `<div class="small"><strong>Last close:</strong> ${escapeHtml(metrics.lastClose)}</div>` : "",
      metrics.mae ? `<div class="small"><strong>MAE (recent):</strong> ${escapeHtml(metrics.mae)}</div>` : "",
    ]
      .filter(Boolean)
      .join("");

    setOutputReady(ui.forecastOutput);
    ui.forecastOutput.innerHTML = `
      <div class="output-stack">
        ${summary}
        <div class="table-controls">
          <button class="cta secondary small" type="button" data-action="forecast-page" data-delta="-1" ${
            page === 0 ? "disabled" : ""
          }>Prev</button>
          <div class="small muted">Rows ${start + 1}-${end} of ${total} · Page ${page + 1}/${totalPages}</div>
          <button class="cta secondary small" type="button" data-action="forecast-page" data-delta="1" ${
            page >= totalPages - 1 ? "disabled" : ""
          }>Next</button>
          <button class="cta secondary small" type="button" data-action="forecast-csv">Download CSV</button>
        </div>
        <div class="table-wrap" style="margin-top:10px;">
          <table class="data-table">
            <thead>
              <tr>${headers.map((key) => `<th>${escapeHtml(key)}</th>`).join("")}</tr>
            </thead>
            <tbody>
              ${slice
                .map(
                  (row) => `
                    <tr>
                      ${headers
                        .map((key) => `<td>${escapeHtml(key === "ds" ? row[key] : formatForecastCell(row[key]))}</td>`)
                        .join("")}
                    </tr>
                  `
                )
                .join("")}
            </tbody>
          </table>
        </div>
        <details class="learn-more">
          <summary>Learn more</summary>
          <p class="small">
            Forecasts are saved to your account so you can re-plot them later. Use Indicators to overlay trend signals, then switch to News and Options for context.
          </p>
        </details>
      </div>
    `;
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
    state.tickerContext.forecastTablePage = 0;
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
    renderForecastDetails(doc);
    return doc;
  };

  const ensureMessagingServiceWorker = async () => {
    if (!("serviceWorker" in navigator)) {
      throw new Error("Service workers are not available in this browser.");
    }
    return navigator.serviceWorker.register("/firebase-messaging-sw.js");
  };

  const loadVapidKey = async (functions) => {
    if (state.remoteFlags?.webPushVapidKey) return String(state.remoteFlags.webPushVapidKey);
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
	      throw new Error("Web push key is missing. Configure the VAPID public key on the server.");
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
	      console.error("App SDK not loaded.");
	      return;
	    }

      applyTheme(resolveThemePreference(), { persist: false });

		    const auth = firebase.auth();
		    const db = firebase.firestore();
		    const functions = firebase.functions();
		    const storage = firebase.storage ? firebase.storage() : null;
		    const messaging = getMessagingClient();

      ensureThemeToggle();

	    if (!state.authResolved) {
	      if (ui.headerUserEmail) ui.headerUserEmail.textContent = "Restoring session...";
	      if (ui.headerUserStatus) ui.headerUserStatus.textContent = "Loading";
	    }

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
	    bindPanelNavigation();
	    syncStickyOffsets();
	    window.addEventListener("resize", () => window.requestAnimationFrame(syncStickyOffsets));
	    window.setTimeout(syncStickyOffsets, 280);
	    if (state.remoteFlags.assistantEnabled) ensureAssistantWidget(functions);
	    loadRemoteConfig().then((flags) => {
	      if (flags.assistantEnabled) ensureAssistantWidget(functions);
	    });

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

		    const pickTicker = async (rawTicker) => {
		      const ticker = normalizeTicker(rawTicker);
		      if (!ticker) return;
		      safeLocalStorageSet(LAST_TICKER_KEY, ticker);
		      syncTickerInputs(ticker);
		      logEvent("ticker_selected", { ticker, page_path: window.location.pathname });

			      // If we're in the terminal, load the chart immediately. Otherwise, jump to the terminal.
			      if (ui.terminalForm && ui.terminalTicker && ui.tickerChart) {
			        ui.terminalTicker.value = ticker;
			        ui.terminalForm.requestSubmit?.();
			      } else {
		        const params = new URLSearchParams();
		        params.set("ticker", ticker);
		        window.location.href = `/forecasting?${params.toString()}`;
		      }
		    };

		    document.addEventListener("click", async (event) => {
		      const button = event.target.closest('[data-action="pick-ticker"]');
		      if (!button) return;
		      event.preventDefault();
		      await pickTicker(button.dataset.ticker || button.textContent);
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

		      const onTerminalPage = Boolean(ui.terminalForm && ui.tickerChart);
		      if (!onTerminalPage) {
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

		    document.addEventListener("click", (event) => {
		      const pageBtn = event.target.closest('[data-action="forecast-page"]');
		      if (pageBtn) {
		        event.preventDefault();
		        const delta = Number(pageBtn.dataset.delta || "0");
		        const doc = state.tickerContext.forecastDoc;
		        if (!doc || !Array.isArray(doc.forecastRows) || !doc.forecastRows.length) return;
		        const pageSize = 25;
		        const totalPages = Math.max(1, Math.ceil(doc.forecastRows.length / pageSize));
		        const next = Math.max(0, Math.min(totalPages - 1, Number(state.tickerContext.forecastTablePage || 0) + delta));
		        state.tickerContext.forecastTablePage = next;
		        renderForecastDetails(doc);
		        return;
		      }

		      const csvBtn = event.target.closest('[data-action="forecast-csv"]');
		      if (!csvBtn) return;
		      event.preventDefault();
		      const doc = state.tickerContext.forecastDoc;
		      if (!doc || !Array.isArray(doc.forecastRows) || !doc.forecastRows.length) {
		        showToast("No forecast rows available to export.", "warn");
		        return;
		      }
		      const rows = doc.forecastRows;
		      const quantKeys = Object.keys(rows[0] || {})
		        .filter((key) => /^q\\d\\d$/.test(key))
		        .sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
		      const headers = ["ds", ...quantKeys];
		      const csv = buildCsv(rows, headers);
		      const ticker = normalizeTicker(doc.ticker || "ticker") || "ticker";
		      const service = String(doc.service || "forecast").replace(/[^a-z0-9_\\-]+/gi, "_");
		      triggerDownload(`${ticker}_${service}_${doc.id || "run"}.csv`, csv);
		      showToast("CSV downloaded.");
		    });

	    const persistenceReady = auth
	      .setPersistence(firebase.auth.Auth.Persistence.LOCAL)
	      .catch(async () => {
        // Some browsers block persistent storage (e.g., private browsing). Fall back to session persistence.
        try {
          await auth.setPersistence(firebase.auth.Auth.Persistence.SESSION);
        } catch (error) {
          // Ignore persistence failures.
        }
        showToast("Using session-only sign-in in this browser.", "warn");
      });

			    ui.headerAuth?.addEventListener("click", () => {
			      window.location.href = "/account";
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
		      startWorkspaceTasks(db, next);
		      startWatchlist(db, next);
		      startPriceAlerts(db, next);
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

	    ui.taskForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
	      if (!state.user) {
	        showToast("Sign in to manage tasks.", "warn");
	        return;
	      }
	      const workspaceId = state.activeWorkspaceId || state.user.uid;
	      if (!canWriteWorkspace(workspaceId)) {
	        showToast("Editor access required to create tasks.", "warn");
	        return;
	      }
	      const title = String(ui.taskTitle?.value || "").trim();
	      if (!title) {
	        showToast("Enter a task title.", "warn");
	        return;
	      }
	      const dueDate = String(ui.taskDue?.value || "").trim();
	      const status = String(ui.taskStatus?.value || "backlog");
	      const assigneeEmail = String(ui.taskAssignee?.value || "").trim();
	      const notes = String(ui.taskNotes?.value || "").trim();
	      if (ui.taskStatusText) ui.taskStatusText.textContent = "Saving...";

	      try {
	        await db
	          .collection("users")
	          .doc(workspaceId)
	          .collection("tasks")
	          .add({
	            title,
	            notes,
	            status,
	            dueDate: dueDate || null,
	            assigneeEmail: assigneeEmail || "",
	            createdBy: { uid: state.user.uid, email: state.user.email || "" },
	            createdAt: firebase.firestore.FieldValue.serverTimestamp(),
	            updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
	            meta: buildMeta(),
	          });
	        if (ui.taskStatusText) ui.taskStatusText.textContent = "Task created.";
	        if (ui.taskTitle) ui.taskTitle.value = "";
	        if (ui.taskNotes) ui.taskNotes.value = "";
	        showToast("Task created.");
	        logEvent("task_created", { workspace_id: workspaceId });
	      } catch (error) {
	        if (ui.taskStatusText) ui.taskStatusText.textContent = error.message || "Unable to create task.";
	        showToast(error.message || "Unable to create task.", "warn");
	      }
	    });

	    ui.watchlistForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
	      if (!state.user) {
	        showToast("Sign in to manage your watchlist.", "warn");
	        return;
	      }
	      const workspaceId = state.activeWorkspaceId || state.user.uid;
	      if (!canWriteWorkspace(workspaceId)) {
	        showToast("Editor access required to update this workspace watchlist.", "warn");
	        return;
	      }
	      const formData = new FormData(ui.watchlistForm);
	      const ticker = normalizeTicker(formData.get("ticker"));
	      const notes = String(formData.get("notes") || "").trim().slice(0, 2400);
	      if (!ticker) {
	        showToast("Enter a ticker.", "warn");
	        return;
	      }
	      try {
	        await db
	          .collection("users")
	          .doc(workspaceId)
	          .collection("watchlist")
	          .doc(ticker)
	          .set(
	            {
	              ticker,
	              notes,
	              addedBy: { uid: state.user.uid, email: state.user.email || "" },
	              createdAt: firebase.firestore.FieldValue.serverTimestamp(),
	              updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
	              meta: buildMeta(),
	            },
	            { merge: true }
	          );
	        if (ui.watchlistNotes) ui.watchlistNotes.value = "";
	        showToast(`${ticker} added to watchlist.`);
	        logEvent("watchlist_added", { ticker, workspace_id: workspaceId });
	      } catch (error) {
	        showToast(error.message || "Unable to update watchlist.", "warn");
	      }
	    });

	    ui.alertForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
	      if (!state.user) {
	        showToast("Sign in to create alerts.", "warn");
	        return;
	      }
	      const workspaceId = state.activeWorkspaceId || state.user.uid;
	      if (!canWriteWorkspace(workspaceId)) {
	        showToast("Editor access required to create alerts in this workspace.", "warn");
	        return;
	      }
	      const formData = new FormData(ui.alertForm);
	      const ticker = normalizeTicker(formData.get("ticker"));
	      const condition = String(formData.get("condition") || "above").trim().toLowerCase();
	      const targetPrice = Number(formData.get("target"));
	      const notes = String(formData.get("notes") || "").trim().slice(0, 2000);
	      if (!ticker) {
	        showToast("Enter a ticker.", "warn");
	        return;
	      }
	      if (!Number.isFinite(targetPrice) || targetPrice <= 0) {
	        showToast("Enter a valid target price.", "warn");
	        return;
	      }
	      try {
	        await db
	          .collection("users")
	          .doc(workspaceId)
	          .collection("price_alerts")
	          .add({
	            ticker,
	            condition: condition === "below" ? "below" : "above",
	            targetPrice,
	            notes,
	            active: true,
	            status: "active",
	            createdByUid: state.user.uid,
	            createdByEmail: state.user.email || "",
	            createdAt: firebase.firestore.FieldValue.serverTimestamp(),
	            updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
	            meta: buildMeta(),
	          });
	        if (ui.alertNotes) ui.alertNotes.value = "";
	        showToast("Alert created.");
	        logEvent("alert_created", { ticker, condition, workspace_id: workspaceId });
	      } catch (error) {
	        showToast(error.message || "Unable to create alert.", "warn");
	      }
	    });

	    ui.alertsCheck?.addEventListener("click", async () => {
	      if (!state.user) {
	        showToast("Sign in first.", "warn");
	        return;
	      }
	      if (ui.alertsStatus) ui.alertsStatus.textContent = "Checking alerts...";
	      try {
	        const workspaceId = state.activeWorkspaceId || state.user.uid;
	        const check = functions.httpsCallable("check_price_alerts");
	        const result = await check({ workspaceId, meta: buildMeta() });
	        const data = result.data || {};
	        const triggered = Number(data.triggered || 0);
	        const checked = Number(data.checked || 0);
	        if (ui.alertsStatus) ui.alertsStatus.textContent = triggered ? `${triggered} alert(s) triggered (checked ${checked}).` : `No alerts triggered (checked ${checked}).`;
	        showToast(triggered ? `${triggered} alert(s) triggered.` : "No alerts triggered.");
	        logEvent("alert_scan", { checked, triggered, workspace_id: workspaceId });
	      } catch (error) {
	        if (ui.alertsStatus) ui.alertsStatus.textContent = error.message || "Unable to check alerts.";
	        showToast(error.message || "Unable to check alerts.", "warn");
	      }
	    });

	    document.addEventListener("click", async (event) => {
	      const move = event.target.closest('[data-action="task-move"]');
	      if (move) {
	        if (!state.user) return;
	        const workspaceId = state.activeWorkspaceId || state.user.uid;
	        if (!canWriteWorkspace(workspaceId)) {
	          showToast("Editor access required to move tasks.", "warn");
	          return;
	        }
	        const taskId = move.dataset.taskId;
	        const to = move.dataset.to;
	        if (!taskId || !to) return;
	        try {
	          await db
	            .collection("users")
	            .doc(workspaceId)
	            .collection("tasks")
	            .doc(taskId)
	            .set(
	              {
	                status: to,
	                updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
	                movedBy: { uid: state.user.uid, email: state.user.email || "" },
	              },
	              { merge: true }
	            );
	          logEvent("task_moved", { to, workspace_id: workspaceId });
	        } catch (error) {
	          showToast(error.message || "Unable to move task.", "warn");
	        }
	        return;
	      }

	      const del = event.target.closest('[data-action="task-delete"]');
	      if (!del) return;
	      if (!state.user) return;
	      const workspaceId = state.activeWorkspaceId || state.user.uid;
	      if (!canWriteWorkspace(workspaceId)) {
	        showToast("Editor access required to delete tasks.", "warn");
	        return;
	      }
	      const taskId = del.dataset.taskId;
	      if (!taskId) return;
	      del.disabled = true;
	      try {
	        await db.collection("users").doc(workspaceId).collection("tasks").doc(taskId).delete();
	        showToast("Task deleted.");
	        logEvent("task_deleted", { workspace_id: workspaceId });
	      } catch (error) {
	        showToast(error.message || "Unable to delete task.", "warn");
	      } finally {
	        del.disabled = false;
	      }
	    });

	    document.addEventListener("click", async (event) => {
	      const remove = event.target.closest('[data-action="watchlist-remove"]');
	      if (remove) {
	        if (!state.user) return;
	        const workspaceId = state.activeWorkspaceId || state.user.uid;
	        if (!canWriteWorkspace(workspaceId)) {
	          showToast("Editor access required to update this workspace.", "warn");
	          return;
	        }
	        const ticker = normalizeTicker(remove.dataset.ticker || "");
	        if (!ticker) return;
	        remove.disabled = true;
	        try {
	          await db.collection("users").doc(workspaceId).collection("watchlist").doc(ticker).delete();
	          showToast(`${ticker} removed.`);
	          logEvent("watchlist_removed", { ticker, workspace_id: workspaceId });
	        } catch (error) {
	          showToast(error.message || "Unable to remove ticker.", "warn");
	        } finally {
	          remove.disabled = false;
	        }
	        return;
	      }

	      const toggle = event.target.closest('[data-action="alert-toggle"]');
	      if (toggle) {
	        if (!state.user) return;
	        const workspaceId = state.activeWorkspaceId || state.user.uid;
	        if (!canWriteWorkspace(workspaceId)) {
	          showToast("Editor access required to update alerts.", "warn");
	          return;
	        }
	        const alertId = toggle.dataset.alertId || "";
	        if (!alertId) return;
	        const active = toggle.dataset.active === "1";
	        toggle.disabled = true;
	        try {
	          await db
	            .collection("users")
	            .doc(workspaceId)
	            .collection("price_alerts")
	            .doc(alertId)
	            .set(
	              {
	                active: !active,
	                status: !active ? "active" : "disabled",
	                updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
	              },
	              { merge: true }
	            );
	          logEvent("alert_toggled", { active: !active, workspace_id: workspaceId });
	        } catch (error) {
	          showToast(error.message || "Unable to update alert.", "warn");
	        } finally {
	          toggle.disabled = false;
	        }
	        return;
	      }

	      const delAlert = event.target.closest('[data-action="alert-delete"]');
	      if (!delAlert) return;
	      if (!state.user) return;
	      const workspaceId = state.activeWorkspaceId || state.user.uid;
	      if (!canWriteWorkspace(workspaceId)) {
	        showToast("Editor access required to delete alerts.", "warn");
	        return;
	      }
	      const alertId = delAlert.dataset.alertId || "";
	      if (!alertId) return;
	      delAlert.disabled = true;
	      try {
	        await db.collection("users").doc(workspaceId).collection("price_alerts").doc(alertId).delete();
	        showToast("Alert deleted.");
	        logEvent("alert_deleted", { workspace_id: workspaceId });
	      } catch (error) {
	        showToast(error.message || "Unable to delete alert.", "warn");
	      } finally {
	        delAlert.disabled = false;
	      }
	    });

	    document.addEventListener("dragstart", (event) => {
	      const card = event.target.closest(".task-card");
	      if (!card) return;
	      if (card.getAttribute("draggable") !== "true") return;
	      const taskId = card.dataset.taskId;
	      if (!taskId) return;
	      event.dataTransfer?.setData("text/plain", taskId);
	      event.dataTransfer?.setData("application/x-quantura-task", taskId);
	      event.dataTransfer && (event.dataTransfer.effectAllowed = "move");
	      card.classList.add("dragging");
	    });

	    document.addEventListener("dragend", (event) => {
	      const card = event.target.closest(".task-card");
	      card?.classList.remove("dragging");
	      document.querySelectorAll(".kanban-col.drag-over").forEach((el) => el.classList.remove("drag-over"));
	    });

	    document.addEventListener("dragover", (event) => {
	      const col = event.target.closest("[data-task-dropzone]");
	      if (!col) return;
	      event.preventDefault();
	      col.classList.add("drag-over");
	    });

	    document.addEventListener("dragleave", (event) => {
	      const col = event.target.closest("[data-task-dropzone]");
	      if (!col) return;
	      col.classList.remove("drag-over");
	    });

	    document.addEventListener("drop", async (event) => {
	      const col = event.target.closest("[data-task-dropzone]");
	      if (!col) return;
	      event.preventDefault();
	      col.classList.remove("drag-over");
	      if (!state.user) return;
	      const workspaceId = state.activeWorkspaceId || state.user.uid;
	      if (!canWriteWorkspace(workspaceId)) return;
	      const to = col.dataset.taskDropzone;
	      const taskId = event.dataTransfer?.getData("application/x-quantura-task") || event.dataTransfer?.getData("text/plain");
	      if (!taskId || !to) return;
	      try {
	        await db
	          .collection("users")
	          .doc(workspaceId)
	          .collection("tasks")
	          .doc(taskId)
	          .set(
	            {
	              status: to,
	              updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
	              movedBy: { uid: state.user.uid, email: state.user.email || "" },
	            },
	            { merge: true }
	          );
	        logEvent("task_drag_moved", { to, workspace_id: workspaceId });
	      } catch (error) {
	        showToast(error.message || "Unable to move task.", "warn");
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
		      if (!state.user) {
		        showToast("Sign in to run a forecast.", "warn");
		        return;
		      }
		      const formData = new FormData(ui.forecastForm);
		      let quantiles = [];
		      try {
		        const raw = formData.getAll("quantiles");
		        if (raw.length === 1 && String(raw[0]).includes(",")) {
		          quantiles = parseQuantilesInput(String(raw[0]));
		        } else {
		          quantiles = parseQuantilesInput(raw);
		        }
		      } catch (error) {
		        showToast(error.message || "Invalid quantiles.", "warn");
		        return;
		      }
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
		        setOutputLoading(ui.forecastOutput, "Generating forecast...");
		        const runForecast = functions.httpsCallable("run_timeseries_forecast");
		        const result = await runForecast(payload);
		        const data = result.data || {};
		        const requestId = String(data.requestId || "").trim();
		        if (!requestId) {
		          throw new Error("Forecast run did not return a request ID.");
		        }

		        logEvent("forecast_request", { ticker: payload.ticker, interval: payload.interval, service: payload.service });
		        showToast("Forecast saved.");

		        try {
		          if (ui.tickerChart) {
		            setTerminalStatus("Loading forecast for chart...");
		            await plotForecastById(db, functions, requestId);
		            document.getElementById("terminal")?.scrollIntoView({ behavior: "smooth" });
		          } else {
		            const doc = await loadForecastDoc(db, requestId);
		            state.tickerContext.forecastDoc = doc;
		            state.tickerContext.forecastId = requestId;
		            state.tickerContext.forecastTablePage = 0;
		            renderForecastDetails(doc);
		          }
		        } catch (plotError) {
		          setOutputReady(ui.forecastOutput);
		          if (ui.forecastOutput) {
		            ui.forecastOutput.innerHTML = `
		              <div class="small"><strong>Forecast ID:</strong> ${escapeHtml(requestId)}</div>
		              <div class="small"><strong>Service:</strong> ${escapeHtml(labelForecastService(payload.service))}</div>
		              <div class="small muted" style="margin-top:10px;">${escapeHtml(plotError.message || "Forecast saved, but could not be loaded yet.")}</div>
		            `;
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
	        setOutputLoading(ui.technicalsOutput, "Computing indicators...");
	        const runIndicators = functions.httpsCallable("get_technicals");
	        const result = await runIndicators(payload);
	        const data = result.data || {};
	        const rows = data.latest || [];
	        if (ui.technicalsOutput) {
	          setOutputReady(ui.technicalsOutput);
		          if (!rows.length) {
		            ui.technicalsOutput.textContent = "No indicator data returned.";
		          } else {
		            ui.technicalsOutput.innerHTML = `
                  <div class="table-wrap">
		                <table class="data-table">
	                    <thead><tr><th>Indicator</th><th>Value</th></tr></thead>
	                    <tbody>
	                      ${rows.map((row) => `<tr><td>${row.name}</td><td>${row.value}</td></tr>`).join("")}
	                    </tbody>
	                  </table>
                  </div>
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
	        setOutputLoading(ui.trendingList, "Loading trending tickers...");
	        const getTrending = functions.httpsCallable("get_trending_tickers");
	        const result = await getTrending({ meta: buildMeta() });
	        const tickers = result.data?.tickers || [];
	        if (ui.trendingList) {
	          setOutputReady(ui.trendingList);
	          if (!tickers.length) {
	            ui.trendingList.innerHTML = `<div class="small muted">No trending tickers returned.</div>`;
	          } else {
	            ui.trendingList.innerHTML = tickers
	              .map(
	                (ticker) =>
	                  `<button class="ticker-pill" type="button" data-action="pick-ticker" data-ticker="${escapeHtml(
	                    ticker
	                  )}">${escapeHtml(ticker)}</button>`
	              )
	              .join("");
	          }
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
	        setOutputLoading(ui.newsOutput, "Fetching headlines...");
	        const getNews = functions.httpsCallable("get_ticker_news");
	        const result = await getNews(payload);
	        const items = result.data?.news || [];
	        if (ui.newsOutput) {
	          setOutputReady(ui.newsOutput);
	          if (!items.length) {
	            ui.newsOutput.innerHTML = `
	              <div class="small muted">No news returned for ${escapeHtml(payload.ticker || "")}.</div>
	              <div class="small" style="margin-top:10px;">Try a different symbol, or load a trending ticker and retry.</div>
	            `;
	          } else {
	            ui.newsOutput.innerHTML = items
	              .map((item) => {
	                const title = escapeHtml(item.title || "");
	                const publisher = escapeHtml(item.publisher || "");
	                const published = formatEpoch(item.publishedAt);
	                const summary = escapeHtml(item.summary || "");
	                const link = item.link ? escapeHtml(item.link) : "";
	                const meta = [publisher, published].filter(Boolean).join(" · ");
	                return `
	                  <article class="news-card">
	                    <div class="news-title">${title}</div>
	                    <div class="news-meta small">${meta}</div>
	                    ${summary ? `<div class="news-summary small">${summary}</div>` : ""}
	                    ${link ? `<a class="news-link" href="${link}" target="_blank" rel="noreferrer">Read article</a>` : ""}
	                  </article>
	                `;
	              })
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
	      if (!state.user) {
	        showToast("Sign in to load options.", "warn");
	        return;
	      }
	      const formData = new FormData(ui.optionsForm);
	      const ticker = normalizeTicker(formData.get("ticker"));
	      const cacheKey = ticker ? `${OPTIONS_EXPIRATION_PREFIX}${ticker}` : "";
	      let expiration = String(formData.get("expiration") || "").trim();
	      if (!expiration && cacheKey) {
	        expiration = String(safeLocalStorageGet(cacheKey) || "").trim();
	      }
	      const payload = { ticker, expiration };

	      try {
	        setOutputLoading(ui.optionsOutput, "Loading options chain...");
	        const getOptions = functions.httpsCallable("get_options_chain");
	        const result = await getOptions({ ...payload, limit: 36, meta: buildMeta() });
	        const data = result.data || {};
	        const underlyingPrice = data.underlyingPrice;
	        const riskFreeRate = data.riskFreeRate;
	        const timeToExpiryYears = data.timeToExpiryYears;
	        const selectedExpiration = data.selectedExpiration || payload.expiration || "";
	        const expirations = data.expirations || [];
	        const calls = data.calls || [];
	        const puts = data.puts || [];

	        if (cacheKey && selectedExpiration) {
	          safeLocalStorageSet(cacheKey, selectedExpiration);
	        }

	        const expirationEl = document.getElementById("options-expiration");
	        if (expirationEl && expirationEl.tagName === "SELECT" && Array.isArray(expirations) && expirations.length) {
	          expirationEl.innerHTML = [
	            `<option value="">Auto (nearest)</option>`,
	            ...expirations.map((exp) => `<option value="${escapeHtml(exp)}">${escapeHtml(exp)}</option>`),
	          ].join("");
	          if (expirations.includes(selectedExpiration)) {
	            expirationEl.value = selectedExpiration;
	          }
	        }

	        if (ui.optionsOutput) {
	          setOutputReady(ui.optionsOutput);
	          if (!expirations.length) {
	            ui.optionsOutput.innerHTML = `<div class="small muted">No options expirations returned for ${escapeHtml(payload.ticker || "")}.</div>`;
	          } else {
	            const fmt = (value, digits = 2) => {
	              const num = typeof value === "number" ? value : Number(value);
	              if (!Number.isFinite(num)) return "—";
	              return num.toFixed(digits);
	            };
	            const money = (value) => {
	              const num = typeof value === "number" ? value : Number(value);
	              if (!Number.isFinite(num)) return "—";
	              return `$${num.toFixed(2)}`;
	            };
	            const table = (rows, label) => {
	              if (!Array.isArray(rows) || rows.length === 0) {
	                return `<div class="small muted">No ${label.toLowerCase()} returned.</div>`;
	              }
	              return `
	                <div class="table-wrap">
	                  <table class="data-table">
	                    <thead>
	                      <tr>
	                        <th>Strike</th>
	                        <th>Last</th>
	                        <th>Bid</th>
	                        <th>Ask</th>
	                        <th>Mid</th>
	                        <th>IV</th>
	                        <th>Delta</th>
	                        <th>OI</th>
	                        <th>Vol</th>
	                        <th>Prob ITM</th>
	                      </tr>
	                    </thead>
	                    <tbody>
	                      ${rows
	                        .map((opt) => {
	                          const iv = typeof opt.impliedVolatility === "number" ? `${fmt(opt.impliedVolatility * 100, 1)}%` : "—";
	                          const delta = typeof opt.delta === "number" ? fmt(opt.delta, 3) : "—";
	                          const prob = typeof opt.probabilityITM === "number" ? `${fmt(opt.probabilityITM, 2)}%` : "—";
	                          const rowClass = opt.inTheMoney ? "row-itm" : "";
	                          return `
	                            <tr class="${rowClass}">
	                              <td>${fmt(opt.strike, 2)}</td>
	                              <td>${money(opt.lastPrice)}</td>
	                              <td>${money(opt.bid)}</td>
	                              <td>${money(opt.ask)}</td>
	                              <td>${money(opt.mid)}</td>
	                              <td>${iv}</td>
	                              <td>${delta}</td>
	                              <td>${Number(opt.openInterest || 0).toLocaleString()}</td>
	                              <td>${Number(opt.volume || 0).toLocaleString()}</td>
	                              <td>${prob}</td>
	                            </tr>
	                          `;
	                        })
	                        .join("")}
	                    </tbody>
	                  </table>
	                </div>
	              `;
	            };

	            ui.optionsOutput.innerHTML = `
	              <div class="options-meta">
	                <div class="small"><strong>Underlying:</strong> ${money(underlyingPrice)}</div>
	                <div class="small"><strong>Expiration:</strong> ${escapeHtml(selectedExpiration)}</div>
	                <div class="small"><strong>RFR:</strong> ${typeof riskFreeRate === "number" ? fmt(riskFreeRate, 3) : "—"} · <strong>T:</strong> ${
	                  typeof timeToExpiryYears === "number" ? fmt(timeToExpiryYears, 3) : "—"
	                }y</div>
	              </div>
	              <details class="option-block" open>
	                <summary>Calls</summary>
	                ${table(calls, "Calls")}
	              </details>
	              <details class="option-block">
	                <summary>Puts</summary>
	                ${table(puts, "Puts")}
	              </details>
	              <div class="small muted" style="margin-top:10px;">
	                Prob ITM and delta are Black-Scholes style approximations derived from implied volatility and time to expiry. They are not guarantees.
	              </div>
	            `;
	          }
	        }
	        logEvent("options_loaded", { ticker: payload.ticker });
	      } catch (error) {
	        showToast(error.message || "Unable to load options.", "warn");
	      }
	    });

	    ui.optionsExpiration?.addEventListener("change", () => {
	      if (!ui.optionsForm) return;
	      if (!state.user) {
	        showToast("Sign in to load options.", "warn");
	        return;
	      }
	      try {
	        ui.optionsForm.requestSubmit?.();
	      } catch (error) {
	        // Ignore.
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
	        setOutputLoading(ui.screenerOutput, "Running screener...");
	        const runScreener = functions.httpsCallable("run_quick_screener");
	        const result = await runScreener(payload);
	        const rows = result.data?.results || [];
	        if (ui.screenerOutput) {
	          setOutputReady(ui.screenerOutput);
	          if (!rows.length) {
	            ui.screenerOutput.innerHTML = `
	              <div class="small muted">No results returned.</div>
	              <div class="small" style="margin-top:10px;"><strong>Run ID:</strong> ${escapeHtml(result.data?.runId || "—")}</div>
	            `;
		          } else {
		            ui.screenerOutput.innerHTML = `
		              <div class="small"><strong>Run ID:</strong> ${result.data?.runId || "—"}</div>
                  <div class="table-wrap" style="margin-top:12px;">
		                <table class="data-table">
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
		                            <td>
		                              <button class="link-button" type="button" data-action="pick-ticker" data-ticker="${escapeHtml(row.symbol)}">
		                                ${escapeHtml(row.symbol)}
		                              </button>
		                            </td>
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
                  </div>
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

		    persistenceReady.finally(() => {
		      auth.onAuthStateChanged(async (user) => {
		      state.authResolved = true;
		      state.user = user;
		      setAuthUi(user);
		      setUserId(user?.uid || null);

	      if (!user) {
	        renderOrderList([], ui.userOrders);
	        renderRequestList([], ui.userForecasts, "No forecast requests yet.");
	        renderRequestList([], ui.autopilotOutput, "No autopilot requests yet.");
	        renderRequestList([], ui.predictionsOutput, "No uploads yet.");
	        renderRequestList([], ui.alpacaOutput, "No Alpaca orders yet.");
	        if (ui.watchlistList) ui.watchlistList.textContent = "Sign in to manage your watchlist.";
	        if (ui.alertsList) ui.alertsList.textContent = "Sign in to manage your alerts.";
	        if (ui.alertsStatus) ui.alertsStatus.textContent = "";
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
		        if (state.unsubscribeTasks) state.unsubscribeTasks();
		        if (state.unsubscribeWatchlist) state.unsubscribeWatchlist();
		        if (state.unsubscribeAlerts) state.unsubscribeAlerts();
		        if (state.unsubscribeSharedWorkspaces) state.unsubscribeSharedWorkspaces();
		        state.sharedWorkspaces = [];
		        setActiveWorkspaceId("");
		        renderWorkspaceSelect(null);
		        if (ui.productivityBoard) ui.productivityBoard.innerHTML = "";
		        if (ui.tasksCalendar) ui.tasksCalendar.textContent = "Tasks with due dates will appear here.";
			        const gated = new Set([
			          "/dashboard",
			          "/watchlist",
			          "/productivity",
			          "/collaboration",
			          "/uploads",
			          "/autopilot",
			          "/trading",
			          "/notifications",
			          "/purchase",
			        ]);
			        if (gated.has(window.location.pathname) && window.location.pathname !== "/account") {
			          window.location.href = "/account";
			        }
			        return;
			      }

	      await ensureUserProfile(db, user);
	      startUserOrders(db, user);
	      subscribeSharedWorkspaces(db, user);
		      const activeWorkspaceId = resolveActiveWorkspaceId(user);
		      setActiveWorkspaceId(activeWorkspaceId);
		      renderWorkspaceSelect(user);
		      startUserForecasts(db, activeWorkspaceId);
		      startWorkspaceTasks(db, activeWorkspaceId);
		      startWatchlist(db, activeWorkspaceId);
		      startPriceAlerts(db, activeWorkspaceId);
		      startAutopilotRequests(db, user);
		      startPredictionsUploads(db, user);
		      startAlpacaOrders(db, user);
		      refreshCollaboration(functions);
			      if (window.location.pathname === "/account") {
			        window.location.href = "/dashboard";
			      }

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

	      if (ui.terminalForm && ui.tickerChart && state.tickerContext.forecastId && !state.tickerContext.forecastDoc) {
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
  });
  };

  window.addEventListener("load", init);
})();
