(() => {
  const ADMIN_EMAIL = "tamzid257@gmail.com";
  const FCM_TOKEN_CACHE_KEY = "quantura_fcm_token";
  const COOKIE_CONSENT_KEY = "quantura_cookie_consent";
  const LAST_TICKER_KEY = "quantura_last_ticker";
  const WORKSPACE_KEY = "quantura_active_workspace";
  const OPTIONS_EXPIRATION_PREFIX = "quantura_options_expiration_";
  const THEME_KEY = "quantura_theme";
  const PENDING_SHARE_KEY = "quantura_pending_share_v1";
  const PROMO_BANNER_DISMISSED_KEY = "quantura_promo_banner_dismissed_v2";
  const PROMO_MODAL_DISMISSED_KEY = "quantura_promo_modal_dismissed_v2";
  const PROMO_SESSION_COUNT_KEY = "quantura_promo_session_count_v1";
  const PROMO_FORECAST_COUNT_KEY = "quantura_promo_forecast_count_v1";
  const PROMO_LAST_SESSION_KEY = "quantura_promo_last_session_v1";
  const AUTH_PENDING_CREDENTIAL_KEY = "quantura_auth_pending_credential_v1";
  const AUTH_POST_SIGNIN_REFRESH_KEY = "quantura_auth_post_signin_refresh_v1";
  const NATIVE_IAP_PENDING_EVENTS_KEY = "quantura_native_iap_pending_events_v1";
  const NOTIFICATION_PRIVACY_CACHE_KEY = "quantura_notification_privacy_v1";
  const FCM_LOG_CACHE_KEY = "quantura_fcm_log_v1";
  const CHART_RANGE_CACHE_KEY = "quantura_chart_range_v1";
  const CHART_VIEW_CACHE_KEY = "quantura_chart_view_v1";
  const TRADINGVIEW_THEME_CACHE_KEY = "quantura_tv_theme_v1";
  const SIDEBAR_COLLAPSED_KEY = "quantura_sidebar_collapsed_v1";
  const LANGUAGE_PREFERENCE_KEY = "quantura_language_v1";
  const COUNTRY_PREFERENCE_KEY = "quantura_country_v1";
  const TICKER_QUERY_MODEL_KEY = "quantura_ticker_query_model_v1";
  const TICKER_QUERY_PROVIDER_KEY = "quantura_ticker_query_provider_v1";
  const TICKER_QUERY_MODULES_KEY = "quantura_ticker_query_modules_v1";
  const TICKER_QUERY_IMPROVE_TOGGLE_KEY = "quantura_ticker_query_improve_toggle_v1";
  const TICKER_HISTORY_KEY_PREFIX = "quantura_ticker_history_v1";
  const FORECAST_CACHE_DB_NAME = "quantura_forecast_cache_v1";
  const FORECAST_CACHE_STORE_NAME = "forecast_series";
  const FORECAST_CACHE_INDEX_KEY = "quantura_forecast_cache_index_v1";
  const FORECAST_CACHE_LOCAL_PREFIX = "quantura_forecast_cache_entry_v1::";
  const TICKER_HISTORY_LIMIT = 14;
  const TRADINGVIEW_LOAD_TIMEOUT_MS = 9000;
  const AI_LEADERBOARD_DEFAULT_HORIZON = "1y";
  const DEFAULT_VOLATILITY_THRESHOLD = 0.05;
  const META_PIXEL_ID = "1643823927053003";
  const META_CAPI_CALLABLE = "track_meta_conversion_event";
  const META_STANDARD_EVENTS = new Set([
    "PageView",
    "CustomizeProduct",
    "AddToWishlist",
    "CompleteRegistration",
    "Search",
    "SubmitApplication",
    "AddToCart",
    "ViewContent",
    "Schedule",
    "Lead",
    "Contact",
    "Purchase",
  ]);
  const MODEL_COUNCIL_OUTPUT_DISCLAIMER = "LLMs can sometimes make mistakes.";
  const MODEL_COUNCIL_PROMPT_VISIBLE_COUNT = 3;
  const MODEL_COUNCIL_PROMPT_SUGGESTIONS = Object.freeze([
    "What is the strongest bullish and bearish case for {ticker} over the next 3 months?",
    "Summarize the top 5 catalysts for {ticker} and rank them by probability and impact.",
    "Is {ticker} trading rich or cheap versus peers on forward growth and margins?",
    "Build a base/bull/bear scenario table for {ticker} with key assumptions.",
    "What are the biggest risks in the latest quarter for {ticker}?",
    "Compare {ticker} trend strength versus sector ETF and closest competitors.",
    "Which earnings metrics should I monitor next for {ticker}, and why?",
    "Create an entry plan for {ticker} with invalidation and risk controls.",
    "What does the options market imply about {ticker} volatility into the next event?",
    "Estimate whether recent price action in {ticker} is momentum or mean-reversion likely.",
    "Give a 7-day watchlist checklist for {ticker} before making a decision.",
    "What macro variables are most likely to move {ticker} this quarter?",
    "Should {ticker} be considered a compounder, cyclical trade, or event-driven setup?",
    "Summarize analyst sentiment changes on {ticker} and what matters most.",
    "What would invalidate the current thesis on {ticker}?",
    "Create a decision memo for {ticker}: thesis, evidence, risks, and next steps.",
    "What are the most important balance sheet signals for {ticker} right now?",
    "How sensitive is {ticker} to rates, FX, and commodity inputs?",
    "If I can only track 3 indicators for {ticker}, which ones and why?",
    "Draft a portfolio sizing suggestion for {ticker} under conservative risk assumptions.",
  ]);
  const POLYMARKET_CLIENT_CACHE_TTL_MS = 10 * 60 * 1000;
  const POLYMARKET_CLIENT_CACHE_MAX_ENTRIES = 80;
  const POLYMARKET_DEFAULT_MARKET_LIMIT = 12;
  const POLYMARKET_SEARCH_DEBOUNCE_MS = 400;
  const TERMINAL_FX_RECENT_KEY = "quantura_terminal_fx_recent_v1";
  const TERMINAL_FX_RECENT_LIMIT = 8;
  const MY_REQUEST_TYPES = new Set(["forecast", "screener", "indicator", "modelCouncil"]);
  const MY_REQUEST_TYPE_LABELS = {
    forecast: "Forecasting",
    screener: "Screeners",
    indicator: "Indicators",
    modelCouncil: "Model Council",
  };
  const getNativePlatform = () => {
    try {
      const explicit = String(window.__QUANTURA_NATIVE_PLATFORM__ || "").trim().toLowerCase();
      if (explicit === "ios" || explicit === "android") return explicit;
      if (window.Capacitor?.isNativePlatform?.() === true) {
        const platform = String(window.Capacitor.getPlatform?.() || "").trim().toLowerCase();
        if (platform === "ios" || platform === "android") return platform;
      }
      if (window.quanturaAuth?.postMessage) return "android";
      if (window.QuanturaBridge?.postMessage) return "android";
      if (window.webkit?.messageHandlers?.quanturaAuth?.postMessage) return "ios";
      if (window.webkit?.messageHandlers?.QuanturaBridge?.postMessage) return "ios";
    } catch (error) {
      return null;
    }
    return null;
  };

  const isNativeApp = () => Boolean(window.__QUANTURA_NATIVE_APP__ || getNativePlatform());

  const isInstalledPwa = () => {
    try {
      if (window.matchMedia?.("(display-mode: standalone)")?.matches) return true;
      if (navigator.standalone === true) return true;
    } catch (error) {
      return false;
    }
    return false;
  };

  const isMobileBrowser = () => {
    const ua = String(navigator.userAgent || "").toLowerCase();
    return /iphone|ipad|ipod|android/.test(ua);
  };

  const resolveRuntimeLabel = () => {
    if (isNativeApp()) return "native";
    if (isInstalledPwa()) return "pwa";
    if (isMobileBrowser()) return "mobile_web";
    return "web";
  };

  const applyRuntimeBodyClasses = () => {
    if (!document?.body) return;
    const nativePlatform = getNativePlatform();
    const runtimeLabel = resolveRuntimeLabel();
    document.body.classList.toggle("native-runtime", runtimeLabel === "native");
    document.body.classList.toggle("native-platform-ios", nativePlatform === "ios");
    document.body.classList.toggle("native-platform-android", nativePlatform === "android");
  };

  const triggerSubtleHaptic = () => {
    try {
      if (typeof navigator !== "undefined" && typeof navigator.vibrate === "function") {
        navigator.vibrate(8);
      }
    } catch (error) {
      // Best-effort only.
    }
  };

  const sendNativeBridgeMessage = (payload) => {
    const message = payload && typeof payload === "object" ? payload : {};
    try {
      if (window.QuanturaBridge?.postMessage) {
        window.QuanturaBridge.postMessage(JSON.stringify(message));
        return true;
      }
    } catch (error) {
      // Try iOS bridge fallback.
    }
    try {
      const iosHandler = window.webkit?.messageHandlers?.QuanturaBridge?.postMessage;
      if (iosHandler) {
        iosHandler(message);
        return true;
      }
    } catch (error) {
      // Try standard ReactNativeWebView bridge below.
    }
    try {
      if (window.ReactNativeWebView?.postMessage) {
        window.ReactNativeWebView.postMessage(JSON.stringify(message));
        return true;
      }
    } catch (error) {
      return false;
    }
    return false;
  };

  const sendNativeAuthMessage = (payload) => {
    const message = payload && typeof payload === "object" ? payload : {};
    try {
      if (window.quanturaAuth?.postMessage) {
        window.quanturaAuth.postMessage(JSON.stringify(message));
        return true;
      }
    } catch (error) {
      // Try fallbacks below.
    }
    try {
      if (window.QuanturaBridge?.postMessage) {
        window.QuanturaBridge.postMessage(JSON.stringify(message));
        return true;
      }
    } catch (error) {
      // Try iOS handlers below.
    }
    try {
      const iosAuthHandler = window.webkit?.messageHandlers?.quanturaAuth?.postMessage;
      if (iosAuthHandler) {
        iosAuthHandler(message);
        return true;
      }
    } catch (error) {
      // Ignore and try legacy bridge.
    }
    try {
      const iosLegacyHandler = window.webkit?.messageHandlers?.QuanturaBridge?.postMessage;
      if (iosLegacyHandler) {
        const type = String(message.type || "").trim().toUpperCase();
        if (type === "SIGN_OUT") {
          iosLegacyHandler({ action: "authSignOut" });
        } else if (type === "GET_AUTH_STATE") {
          return false;
        } else {
          iosLegacyHandler({ action: "authSignIn", provider: String(message.provider || "").trim().toLowerCase() });
        }
        return true;
      }
    } catch (error) {
      // Try standard ReactNativeWebView bridge below.
    }
    try {
      if (window.ReactNativeWebView?.postMessage) {
        window.ReactNativeWebView.postMessage(JSON.stringify(message));
        return true;
      }
    } catch (error) {
      return false;
    }
    return false;
  };

  const requestNativeBridgeSignOut = () => sendNativeAuthMessage({ type: "SIGN_OUT" });

  const waitForNativeAuthCompletion = (auth, timeoutMs = 90000) =>
    new Promise((resolve, reject) => {
      if (!auth) {
        reject(new Error("Firebase Auth is unavailable."));
        return;
      }
      if (auth.currentUser && !auth.currentUser.isAnonymous) {
        resolve(auth.currentUser);
        return;
      }
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        try {
          unsubscribe?.();
        } catch (_) {
          // no-op
        }
        reject(new Error("Native sign-in timed out."));
      }, timeoutMs);
      const unsubscribe = auth.onAuthStateChanged((user) => {
        if (settled) return;
        if (user && !user.isAnonymous) {
          settled = true;
          clearTimeout(timer);
          unsubscribe();
          resolve(user);
        }
      });
    });
  const DEFAULT_BRIEF_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "NFLX",
    "AMD", "AVGO", "CRM", "ORCL", "JPM", "BAC", "GS", "V", "MA",
    "UNH", "LLY", "JNJ", "XOM", "CVX", "CAT", "DE", "KO", "PEP",
    "COST", "WMT", "NKE", "PLTR",
  ];
  const FEATURE_VOTE_KEYS = new Set(["uploads", "autopilot"]);
  const FEATURE_VOTE_LABELS = Object.freeze({
    uploads: "Upload predictions CSV",
    autopilot: "Weekly Brief Autopilot",
  });
  const FISCALDATA_DEFAULT_PREFERRED_COLUMNS = Object.freeze([
    "record_date",
    "country_currency_desc",
    "security_desc",
    "exchange_rate",
    "avg_interest_rate_amt",
    "close_today_bal",
    "tot_pub_debt_out_amt",
  ]);
  const AI_MODEL_CATALOG = [
    {
      id: "gpt-5-nano",
      provider: "openai",
      tier: "Core",
      label: "Nano Scout",
      personality: "efficient",
      helper: "Ultra-low-cost triage and quick breadth scans.",
      pricing: { input: 0.05, cached_input: 0.005, output: 0.4 },
    },
    {
      id: "gpt-5-mini",
      provider: "openai",
      tier: "Core",
      label: "Balanced Analyst",
      personality: "balanced",
      helper: "Fast all-round screening for most workflows.",
      pricing: { input: 0.25, cached_input: 0.025, output: 2.0 },
    },
    {
      id: "gpt-5",
      provider: "openai",
      tier: "Pro",
      label: "Research Core",
      personality: "deep_research",
      helper: "Higher-depth thesis and cross-factor reasoning.",
      pricing: { input: 1.25, cached_input: 0.125, output: 10.0 },
    },
    {
      id: "gpt-5.1",
      provider: "openai",
      tier: "Pro",
      label: "Macro Strategist",
      personality: "momentum",
      helper: "Stronger macro synthesis and scenario framing.",
      pricing: { input: 1.25, cached_input: 0.125, output: 10.0 },
    },
    {
      id: "gpt-5.2",
      provider: "openai",
      tier: "Desk",
      label: "Contrarian Strategist",
      personality: "contrarian",
      helper: "Looks for crowded trades and asymmetric reversals.",
      pricing: { input: 1.75, cached_input: 0.175, output: 14.0 },
    },
    {
      id: "amazon.nova-lite-v1:0",
      provider: "amazon_nova",
      tier: "Nova",
      label: "Nova Flow",
      personality: "balanced",
      helper: "Amazon Nova lightweight reasoning path.",
      pricing: { input: null, cached_input: null, output: null },
    },
    {
      id: "amazon.nova-pro-v1:0",
      provider: "amazon_nova",
      tier: "Nova",
      label: "Nova Operator",
      personality: "deep_research",
      helper: "Amazon Nova high-depth analysis path.",
      pricing: { input: null, cached_input: null, output: null },
    },
    {
      id: "gemini-2.0-flash",
      provider: "gemini",
      tier: "Council",
      label: "Gemini 2.0 Flash",
      personality: "balanced",
      helper: "Google Gemini fast-response path.",
      pricing: { input: null, cached_input: null, output: null },
    },
    {
      id: "mistral-small-latest",
      provider: "mistral",
      tier: "Council",
      label: "Mistral Small",
      personality: "balanced",
      helper: "Mistral low-latency reasoning path.",
      pricing: { input: null, cached_input: null, output: null },
    },
    {
      id: "sonar",
      provider: "perplexity",
      tier: "Council",
      label: "Perplexity Sonar",
      personality: "research",
      helper: "Perplexity web-grounded answer path.",
      pricing: { input: null, cached_input: null, output: null },
    },
  ];
  const DEFAULT_LLM_ALLOWED_MODELS = ["gpt-5-nano", "gpt-5-mini", "gpt-5", "gpt-5.1", "gpt-5.2"];
  const AI_USAGE_TIER_DEFAULTS = {
    free: {
      allowed_models: ["gpt-5-nano", "gpt-5-mini"],
      weekly_limit: 3,
      daily_limit: 3,
      volatility_alerts: false,
      workspace_limit: 0,
      ad_free: false,
    },
    go: {
      allowed_models: ["gpt-5-nano", "gpt-5-mini"],
      weekly_limit: 10,
      daily_limit: 10,
      volatility_alerts: true,
      workspace_limit: 1,
      ad_free: true,
    },
    plus: {
      allowed_models: ["gpt-5-mini", "gpt-5"],
      weekly_limit: 25,
      daily_limit: 25,
      volatility_alerts: true,
      workspace_limit: 3,
      ad_free: true,
    },
    pro: {
      allowed_models: ["gpt-5-mini", "gpt-5", "gpt-5.1"],
      weekly_limit: 60,
      daily_limit: 60,
      volatility_alerts: true,
      workspace_limit: 8,
      ad_free: true,
    },
    business: {
      allowed_models: ["gpt-5-nano", "gpt-5-mini", "gpt-5", "gpt-5.1", "gpt-5.2", "amazon.nova-lite-v1:0", "amazon.nova-pro-v1:0"],
      weekly_limit: 150,
      daily_limit: 150,
      volatility_alerts: true,
      workspace_limit: 30,
      ad_free: true,
    },
    desk: {
      allowed_models: ["gpt-5-nano", "gpt-5-mini", "gpt-5", "gpt-5.1", "gpt-5.2"],
      weekly_limit: 150,
      daily_limit: 150,
      volatility_alerts: true,
      workspace_limit: 30,
      ad_free: true,
    },
  };
  const DEFAULT_NATIVE_IAP_PRODUCT_IDS = Object.freeze({
    ios: Object.freeze({
      go: "goplan",
      plus: "premium",
      pro: "pro",
      business: "businessplan",
      desk: "businessplan",
      forecast: "goplan",
      annual_go: "annualgoplan",
      annual_plus: "annualplusplan",
      annual_pro: "pro",
      annual_business: "annualbusinessplan",
      default: "pro",
    }),
    android: Object.freeze({
      go: "goplan",
      plus: "premium",
      pro: "quanturapro",
      business: "quanturabusiness",
      desk: "quanturabusiness",
      forecast: "goplan",
      annual_go: "goplanyearly",
      annual_plus: "annualplusplan",
      annual_pro: "quanturapro",
      annual_business: "annualbusinessplan",
      default: "quanturapro",
    }),
  });
  const MODEL_PROVIDER_LABEL = {
    openai: "OpenAI",
    amazon_nova: "Amazon Nova",
    gemini: "Gemini",
    mistral: "Mistral",
    perplexity: "Perplexity Sonar",
    other: "Other",
  };
  const MODEL_COUNCIL_MODULE_CATALOG = Object.freeze([
    { id: "info", label: "Info", checkedByDefault: true },
    { id: "history", label: "History", checkedByDefault: true },
    { id: "actions", label: "Actions", checkedByDefault: false },
    { id: "dividends", label: "Dividends", checkedByDefault: false },
    { id: "splits", label: "Splits", checkedByDefault: false },
    { id: "calendar", label: "Calendar", checkedByDefault: false },
    { id: "news", label: "News", checkedByDefault: true },
    { id: "recommendations", label: "Recommendations", checkedByDefault: true },
    { id: "balance_sheet", label: "Balance sheet", checkedByDefault: false },
    { id: "quarterly_balance_sheet", label: "Quarterly balance sheet", checkedByDefault: false },
    { id: "income_stmt", label: "Income statement", checkedByDefault: false },
    { id: "quarterly_income_stmt", label: "Quarterly income statement", checkedByDefault: false },
    { id: "cashflow", label: "Cashflow", checkedByDefault: false },
    { id: "quarterly_cashflow", label: "Quarterly cashflow", checkedByDefault: false },
  ]);
  const SUPPORTED_LANGUAGES = new Set(["en", "es", "fr", "de", "ar", "bn"]);
  const COUNTRY_DEFAULT_LANGUAGE = Object.freeze({
    US: "en",
    CA: "en",
    GB: "en",
    DE: "de",
    FR: "fr",
    ES: "es",
    BD: "bn",
    SA: "ar",
    AE: "ar",
    EG: "ar",
    IN: "en",
    JP: "en",
    CN: "en",
    BR: "en",
    AU: "en",
  });
  const UI_I18N_TEXT = Object.freeze({
    en: Object.freeze({
      nav_terminal: "Terminal",
      nav_research: "Research",
      nav_blog: "Blog",
      nav_pricing: "Pricing",
      nav_contact: "Contact Us",
      nav_notifications: "Notifications",
      sign_in: "Sign in",
      sign_out: "Sign out",
      dashboard: "Dashboard",
      open_dashboard: "Open dashboard",
      go_to_dashboard: "Go to dashboard",
      start_free: "Start free",
      logged_in: "Logged In",
      logged_out: "Logged Out",
      not_signed_in: "Not signed in",
      open_billing_portal: "Open Stripe billing portal",
      signin_manage_billing: "Sign in to manage billing",
      signin_set_profile: "Sign in to set your public profile.",
      open_notifications: "Open notifications",
      signin_manage_notifications: "Sign in to manage notifications",
      account: "Account",
      leaderboard_profile: "Public profile",
      sidebar_forecast: "Forecast",
      sidebar_ticker_intelligence: "Ticker",
      sidebar_indicators: "Indicators",
      sidebar_trending: "Trending",
      sidebar_news_data: "News and data",
      sidebar_corporate_events: "Earnings calendar",
      sidebar_market_headlines: "Market headlines",
      sidebar_ask_gpt5: "Model Council",
      sidebar_options: "Options",
      sidebar_currency_conversion: "Currency conversion",
      sidebar_learn_more: "Learn more",
      sidebar_screener: "Screener",
      sidebar_watchlist_alerts: "Watchlist and alerts",
      panel_forecast_title: "Forecast",
      panel_forecast_subtitle: "Generate quantile bands for the ticker in your chart and save the run so you can re-plot it later.",
      panel_market_headlines_title: "Top market headlines",
      panel_market_headlines_subtitle: "Top country-level market headlines plus social posts from X, Reddit, Facebook, and Instagram.",
      panel_ticker_query_title: "Model Council",
      panel_ticker_query_subtitle: "Run multi-provider analysis with structured Yahoo Finance context modules.",
      label_ticker: "Ticker",
      label_timeframe: "Timeframe",
      button_load_chart: "Load chart",
      terminal_tip: "Tip: pick a panel on the left, then click any ticker in results to update the chart immediately.",
      label_market_country: "Country",
      button_load_market_feed: "Load market feed",
      label_response_language: "Response language",
      label_question: "Question",
      button_ask_gpt5: "Prepare Model Council",
      query_result: "Model Council output",
      language_selector_label: "Language",
      language_auto: "Auto",
      language_english: "English",
      language_spanish: "Spanish",
      language_french: "French",
      language_german: "German",
      language_arabic: "Arabic",
      language_bengali: "Bengali",
      question_placeholder: "Example: Is this stock overvalued vs peers, and what are the top near-term risks?",
    }),
    es: Object.freeze({
      nav_terminal: "Terminal",
      nav_research: "Investigacion",
      nav_blog: "Blog",
      nav_pricing: "Precios",
      nav_contact: "Contacto",
      nav_notifications: "Notificaciones",
      sign_in: "Iniciar sesion",
      sign_out: "Cerrar sesion",
      dashboard: "Panel",
      open_dashboard: "Abrir panel",
      go_to_dashboard: "Ir al panel",
      start_free: "Comenzar gratis",
      logged_in: "Sesion iniciada",
      logged_out: "Sesion cerrada",
      not_signed_in: "No has iniciado sesion",
      open_billing_portal: "Abrir portal de facturacion de Stripe",
      signin_manage_billing: "Inicia sesion para gestionar la facturacion",
      signin_set_profile: "Inicia sesion para configurar tu perfil publico del ranking.",
      open_notifications: "Abrir notificaciones",
      signin_manage_notifications: "Inicia sesion para gestionar notificaciones",
      account: "Cuenta",
      leaderboard_profile: "Perfil publico",
      sidebar_forecast: "Pronostico",
      sidebar_ticker_intelligence: "Ticker",
      sidebar_indicators: "Indicadores",
      sidebar_trending: "Tendencias",
      sidebar_news_data: "Noticias y datos",
      sidebar_corporate_events: "Calendario de resultados",
      sidebar_market_headlines: "Titulares del mercado",
      sidebar_ask_gpt5: "Model Council",
      sidebar_options: "Opciones",
      sidebar_currency_conversion: "Conversion de divisas",
      sidebar_learn_more: "Mas informacion",
      sidebar_screener: "Screener",
      sidebar_watchlist_alerts: "Lista y alertas",
      panel_forecast_title: "Pronostico",
      panel_forecast_subtitle: "Genera bandas de cuantiles para el ticker de tu grafico y guarda la ejecucion para volver a trazarla despues.",
      panel_market_headlines_title: "Titulares del mercado",
      panel_market_headlines_subtitle: "Principales titulares por pais junto con publicaciones de X, Reddit, Facebook e Instagram.",
      panel_ticker_query_title: "Model Council",
      panel_ticker_query_subtitle: "Analisis multi-modelo con contexto estructurado de Yahoo Finance.",
      label_ticker: "Ticker",
      label_timeframe: "Periodo",
      button_load_chart: "Cargar grafico",
      terminal_tip: "Consejo: elige un panel a la izquierda y luego haz clic en cualquier ticker para actualizar el grafico al instante.",
      label_market_country: "Pais",
      button_load_market_feed: "Cargar mercado",
      label_response_language: "Idioma de respuesta",
      label_question: "Pregunta",
      button_ask_gpt5: "Preparar Model Council",
      query_result: "Salida de Model Council",
      language_selector_label: "Idioma",
      language_auto: "Automatico",
      language_english: "Ingles",
      language_spanish: "Espanol",
      language_french: "Frances",
      language_german: "Aleman",
      language_arabic: "Arabe",
      language_bengali: "Bengali",
      question_placeholder: "Ejemplo: Este valor esta sobrevalorado frente a sus pares y cuales son los principales riesgos a corto plazo?",
    }),
    fr: Object.freeze({
      nav_terminal: "Terminal",
      nav_research: "Recherche",
      nav_blog: "Blog",
      nav_pricing: "Tarifs",
      nav_contact: "Contact",
      nav_notifications: "Notifications",
      sign_in: "Se connecter",
      sign_out: "Se deconnecter",
      dashboard: "Tableau de bord",
      open_dashboard: "Ouvrir le tableau de bord",
      go_to_dashboard: "Aller au tableau de bord",
      start_free: "Commencer gratuitement",
      logged_in: "Connecte",
      logged_out: "Deconnecte",
      not_signed_in: "Non connecte",
      open_billing_portal: "Ouvrir le portail de facturation Stripe",
      signin_manage_billing: "Connectez-vous pour gerer la facturation",
      signin_set_profile: "Connectez-vous pour configurer votre profil public du classement.",
      open_notifications: "Ouvrir les notifications",
      signin_manage_notifications: "Connectez-vous pour gerer les notifications",
      account: "Compte",
      leaderboard_profile: "Profil public",
      sidebar_forecast: "Prevision",
      sidebar_ticker_intelligence: "Ticker",
      sidebar_indicators: "Indicateurs",
      sidebar_trending: "Tendances",
      sidebar_news_data: "Actualites et donnees",
      sidebar_corporate_events: "Calendrier des resultats",
      sidebar_market_headlines: "Titres du marche",
      sidebar_ask_gpt5: "Model Council",
      sidebar_options: "Options",
      sidebar_currency_conversion: "Conversion de devises",
      sidebar_learn_more: "En savoir plus",
      sidebar_screener: "Screener",
      sidebar_watchlist_alerts: "Watchlist et alertes",
      panel_forecast_title: "Prevision",
      panel_forecast_subtitle: "Generez des bandes de quantiles pour le ticker de votre graphique et enregistrez l'execution pour la recharger plus tard.",
      panel_market_headlines_title: "Titres du marche",
      panel_market_headlines_subtitle: "Principaux titres par pays avec des publications de X, Reddit, Facebook et Instagram.",
      panel_ticker_query_title: "Model Council",
      panel_ticker_query_subtitle: "Analyse multi-modeles avec contexte Yahoo Finance structure.",
      label_ticker: "Ticker",
      label_timeframe: "Horizon",
      button_load_chart: "Charger le graphique",
      terminal_tip: "Astuce: choisissez un panneau a gauche puis cliquez sur un ticker pour mettre a jour le graphique immediatement.",
      label_market_country: "Pays",
      button_load_market_feed: "Charger le flux marche",
      label_response_language: "Langue de reponse",
      label_question: "Question",
      button_ask_gpt5: "Preparer Model Council",
      query_result: "Sortie Model Council",
      language_selector_label: "Langue",
      language_auto: "Auto",
      language_english: "Anglais",
      language_spanish: "Espagnol",
      language_french: "Francais",
      language_german: "Allemand",
      language_arabic: "Arabe",
      language_bengali: "Bengali",
      question_placeholder: "Exemple: Cette action est-elle survaluee par rapport a ses pairs et quels sont les principaux risques a court terme?",
    }),
    de: Object.freeze({
      nav_terminal: "Terminal",
      nav_research: "Research",
      nav_blog: "Blog",
      nav_pricing: "Preise",
      nav_contact: "Kontakt",
      nav_notifications: "Benachrichtigungen",
      sign_in: "Anmelden",
      sign_out: "Abmelden",
      dashboard: "Dashboard",
      open_dashboard: "Dashboard offnen",
      go_to_dashboard: "Zum Dashboard",
      start_free: "Kostenlos starten",
      logged_in: "Angemeldet",
      logged_out: "Abgemeldet",
      not_signed_in: "Nicht angemeldet",
      open_billing_portal: "Stripe-Abrechnungsportal offnen",
      signin_manage_billing: "Zum Verwalten der Abrechnung anmelden",
      signin_set_profile: "Melden Sie sich an, um Ihr offentliches Profil einzurichten.",
      open_notifications: "Benachrichtigungen offnen",
      signin_manage_notifications: "Zum Verwalten von Benachrichtigungen anmelden",
      account: "Konto",
      leaderboard_profile: "Offentliches Profil",
      sidebar_forecast: "Forecast",
      sidebar_ticker_intelligence: "Ticker",
      sidebar_indicators: "Indikatoren",
      sidebar_trending: "Trending",
      sidebar_news_data: "News und Daten",
      sidebar_corporate_events: "Ergebnis-Kalender",
      sidebar_market_headlines: "Markt-Schlagzeilen",
      sidebar_ask_gpt5: "Model Council",
      sidebar_options: "Optionen",
      sidebar_currency_conversion: "Waehrungsumrechnung",
      sidebar_learn_more: "Mehr erfahren",
      sidebar_screener: "Screener",
      sidebar_watchlist_alerts: "Watchlist und Alarme",
      panel_forecast_title: "Forecast",
      panel_forecast_subtitle: "Erzeuge Quantil-Bander fur den Ticker in deinem Chart und speichere den Lauf fur spatere Vergleiche.",
      panel_market_headlines_title: "Top-Markt-Schlagzeilen",
      panel_market_headlines_subtitle: "Wichtigste Schlagzeilen je Land plus Social-Posts von X, Reddit, Facebook und Instagram.",
      panel_ticker_query_title: "Model Council",
      panel_ticker_query_subtitle: "Multi-Provider-Analyse mit strukturiertem Yahoo-Finance-Kontext.",
      label_ticker: "Ticker",
      label_timeframe: "Zeitrahmen",
      button_load_chart: "Chart laden",
      terminal_tip: "Tipp: Wahle links ein Panel und klicke dann auf einen Ticker, um den Chart sofort zu aktualisieren.",
      label_market_country: "Land",
      button_load_market_feed: "Markt-Feed laden",
      label_response_language: "Antwortsprache",
      label_question: "Frage",
      button_ask_gpt5: "Model Council vorbereiten",
      query_result: "Model Council Ausgabe",
      language_selector_label: "Sprache",
      language_auto: "Auto",
      language_english: "Englisch",
      language_spanish: "Spanisch",
      language_french: "Franzosisch",
      language_german: "Deutsch",
      language_arabic: "Arabisch",
      language_bengali: "Bengalisch",
      question_placeholder: "Beispiel: Ist diese Aktie gegenuber Peers uberbewertet und was sind die wichtigsten kurzfristigen Risiken?",
    }),
    ar: Object.freeze({
      nav_terminal: "المحطة",
      nav_research: "الابحاث",
      nav_blog: "المدونة",
      nav_pricing: "الاسعار",
      nav_contact: "تواصل",
      nav_notifications: "الاشعارات",
      sign_in: "تسجيل الدخول",
      sign_out: "تسجيل الخروج",
      dashboard: "لوحة التحكم",
      open_dashboard: "فتح لوحة التحكم",
      go_to_dashboard: "اذهب الى لوحة التحكم",
      start_free: "ابدأ مجانا",
      logged_in: "تم تسجيل الدخول",
      logged_out: "تم تسجيل الخروج",
      not_signed_in: "غير مسجل الدخول",
      open_billing_portal: "فتح بوابة فواتير Stripe",
      signin_manage_billing: "سجل الدخول لادارة الفواتير",
      signin_set_profile: "سجل الدخول لاعداد ملفك العام في لوحة المتصدرين.",
      open_notifications: "فتح الاشعارات",
      signin_manage_notifications: "سجل الدخول لادارة الاشعارات",
      account: "الحساب",
      leaderboard_profile: "الملف العام",
      sidebar_forecast: "التوقع",
      sidebar_ticker_intelligence: "الرمز",
      sidebar_indicators: "المؤشرات",
      sidebar_trending: "الترند",
      sidebar_news_data: "الاخبار والبيانات",
      sidebar_corporate_events: "تقويم الأرباح",
      sidebar_market_headlines: "عناوين السوق",
      sidebar_ask_gpt5: "Model Council",
      sidebar_options: "الخيارات",
      sidebar_currency_conversion: "تحويل العملات",
      sidebar_learn_more: "اعرف المزيد",
      sidebar_screener: "الفلتر",
      sidebar_watchlist_alerts: "قائمة المراقبة والتنبيهات",
      panel_forecast_title: "التوقع",
      panel_forecast_subtitle: "انشئ نطاقات الكوانتايل للرمز في الرسم واحفظ التشغيل لاعادة عرضه لاحقا.",
      panel_market_headlines_title: "ابرز عناوين السوق",
      panel_market_headlines_subtitle: "ابرز عناوين السوق حسب البلد مع منشورات من X وReddit وFacebook وInstagram.",
      panel_ticker_query_title: "Model Council",
      panel_ticker_query_subtitle: "تحليل متعدد النماذج مع سياق Yahoo Finance المنظم.",
      label_ticker: "الرمز",
      label_timeframe: "الاطار الزمني",
      button_load_chart: "تحميل الرسم",
      terminal_tip: "نصيحة: اختر لوحة من اليسار ثم اضغط على اي رمز لتحديث الرسم فورا.",
      label_market_country: "البلد",
      button_load_market_feed: "تحميل موجز السوق",
      label_response_language: "لغة الاجابة",
      label_question: "السؤال",
      button_ask_gpt5: "تحضير Model Council",
      query_result: "مخرجات Model Council",
      language_selector_label: "اللغة",
      language_auto: "تلقائي",
      language_english: "الانجليزية",
      language_spanish: "الاسبانية",
      language_french: "الفرنسية",
      language_german: "الالمانية",
      language_arabic: "العربية",
      language_bengali: "البنغالية",
      question_placeholder: "مثال: هل هذا السهم مبالغ في تقييمه مقارنة بنظرائه وما اهم المخاطر القريبة؟",
    }),
    bn: Object.freeze({
      nav_terminal: "টার্মিনাল",
      nav_research: "গবেষণা",
      nav_blog: "ব্লগ",
      nav_pricing: "মূল্য",
      nav_contact: "যোগাযোগ",
      nav_notifications: "নোটিফিকেশন",
      sign_in: "সাইন ইন",
      sign_out: "সাইন আউট",
      dashboard: "ড্যাশবোর্ড",
      open_dashboard: "ড্যাশবোর্ড খুলুন",
      go_to_dashboard: "ড্যাশবোর্ডে যান",
      start_free: "ফ্রি শুরু করুন",
      logged_in: "লগড ইন",
      logged_out: "লগড আউট",
      not_signed_in: "সাইন ইন করা হয়নি",
      open_billing_portal: "Stripe বিলিং পোর্টাল খুলুন",
      signin_manage_billing: "বিলিং পরিচালনা করতে সাইন ইন করুন",
      signin_set_profile: "আপনার পাবলিক প্রোফাইল সেট করতে সাইন ইন করুন।",
      open_notifications: "নোটিফিকেশন খুলুন",
      signin_manage_notifications: "নোটিফিকেশন পরিচালনা করতে সাইন ইন করুন",
      account: "অ্যাকাউন্ট",
      leaderboard_profile: "পাবলিক প্রোফাইল",
      sidebar_forecast: "ফোরকাস্ট",
      sidebar_ticker_intelligence: "টিকার",
      sidebar_indicators: "ইন্ডিকেটর",
      sidebar_trending: "ট্রেন্ডিং",
      sidebar_news_data: "খবর ও ডেটা",
      sidebar_corporate_events: "আর্নিংস ক্যালেন্ডার",
      sidebar_market_headlines: "মার্কেট হেডলাইন",
      sidebar_ask_gpt5: "Model Council",
      sidebar_options: "অপশন",
      sidebar_currency_conversion: "কারেন্সি কনভার্সন",
      sidebar_learn_more: "আরও জানুন",
      sidebar_screener: "স্ক্রিনার",
      sidebar_watchlist_alerts: "ওয়াচলিস্ট ও অ্যালার্ট",
      panel_forecast_title: "ফোরকাস্ট",
      panel_forecast_subtitle: "চার্টে থাকা টিকারের জন্য কোয়ান্টাইল ব্যান্ড তৈরি করুন এবং পরে পুনরায় দেখার জন্য রান সংরক্ষণ করুন।",
      panel_market_headlines_title: "শীর্ষ মার্কেট হেডলাইন",
      panel_market_headlines_subtitle: "দেশভিত্তিক শীর্ষ বাজারের খবরের সাথে X, Reddit, Facebook এবং Instagram পোস্ট দেখুন।",
      panel_ticker_query_title: "Model Council",
      panel_ticker_query_subtitle: "স্ট্রাকচার্ড Yahoo Finance কনটেক্সটে মাল্টি-মডেল বিশ্লেষণ।",
      label_ticker: "টিকার",
      label_timeframe: "টাইমফ্রেম",
      button_load_chart: "চার্ট লোড করুন",
      terminal_tip: "টিপ: বামে একটি প্যানেল বেছে নিন, তারপর যেকোনো টিকারে ক্লিক করলে চার্ট সাথে সাথে আপডেট হবে।",
      label_market_country: "দেশ",
      button_load_market_feed: "মার্কেট ফিড লোড করুন",
      label_response_language: "উত্তরের ভাষা",
      label_question: "প্রশ্ন",
      button_ask_gpt5: "Model Council প্রস্তুত করুন",
      query_result: "Model Council আউটপুট",
      language_selector_label: "ভাষা",
      language_auto: "অটো",
      language_english: "ইংরেজি",
      language_spanish: "স্প্যানিশ",
      language_french: "ফরাসি",
      language_german: "জার্মান",
      language_arabic: "আরবি",
      language_bengali: "বাংলা",
      question_placeholder: "উদাহরণ: এই স্টকটি সহকর্মীদের তুলনায় বেশি মূল্যায়িত কি না, এবং নিকটমেয়াদি প্রধান ঝুঁকি কী?",
    }),
  });
  const UI_I18N_SELECTOR_MAP = Object.freeze({
    nav_terminal: ['a[data-analytics="nav_terminal"] span'],
    nav_research: ['a[data-analytics="nav_research"] span'],
    nav_blog: ['a[data-analytics="nav_blog"] span'],
    nav_pricing: ['a[data-analytics="nav_pricing"] span'],
    nav_contact: ['a[data-analytics="nav_contact"] span'],
    nav_notifications: ["#header-notifications span"],
    account: [".sidebar-card .small strong"],
    leaderboard_profile: [".profile-settings > summary"],
    open_dashboard: ['.sidebar-card a[href="/dashboard"] span'],
    sidebar_forecast: ['[data-panel-target="forecast"] span'],
    sidebar_ticker_intelligence: ['[data-panel-target="ticker"] span'],
    sidebar_indicators: ['[data-panel-target="indicators"] span'],
    sidebar_trending: ['[data-panel-target="trending"] span'],
    sidebar_news_data: ['[data-panel-target="news"] span'],
    sidebar_corporate_events: ['[data-panel-target="events-calendar"] span'],
    sidebar_market_headlines: ['[data-panel-target="market-headlines"] span'],
    sidebar_ask_gpt5: ['[data-panel-target="ticker-query"] span'],
    sidebar_options: ['[data-panel-target="options"] span'],
    sidebar_currency_conversion: ['[data-panel-target="fx"] span'],
    sidebar_learn_more: ['[data-panel-target="learn"] span'],
    sidebar_screener: ['a[href="/screener"] span'],
    sidebar_watchlist_alerts: ['a[href="/watchlist"] span'],
    panel_forecast_title: ['[data-panel="forecast"] .panel-header h2'],
    panel_forecast_subtitle: ['[data-panel="forecast"] .panel-header p.small'],
    panel_market_headlines_title: ['[data-panel="market-headlines"] .panel-header h2'],
    panel_market_headlines_subtitle: ['[data-panel="market-headlines"] .panel-header p.small'],
    panel_ticker_query_title: ['[data-panel="ticker-query"] .panel-header h2'],
    panel_ticker_query_subtitle: ['[data-panel="ticker-query"] .panel-header p.small'],
    label_ticker: ['label[for="terminal-ticker"]', 'label[for="forecast-ticker"]', 'label[for="ticker-query-ticker"]'],
    label_timeframe: ['label[for="terminal-interval"]'],
    button_load_chart: ['button[data-analytics="terminal_load"] span'],
    terminal_tip: [".ticker-hint"],
    label_market_country: ['label[for="market-headlines-country"]'],
    button_load_market_feed: ['button[data-analytics="market_headlines_load"] span'],
    label_response_language: ['label[for="ticker-query-language"]'],
    label_question: ['label[for="ticker-query-question"]'],
    button_ask_gpt5: ['button[data-analytics="model_council_submit"] span'],
    query_result: ['[data-panel="ticker-query"] .results-panel h3'],
  });
  const UI_I18N_OPTION_MAP = Object.freeze({
    auto: "language_auto",
    en: "language_english",
    es: "language_spanish",
    fr: "language_french",
    de: "language_german",
    ar: "language_arabic",
    bn: "language_bengali",
  });
  const PROFILE_AVATAR_OPTIONS = Object.freeze({
    bull: { emoji: "\u{1F402}", label: "Bull Trader" },
    bear: { emoji: "\u{1F43B}", label: "Bear Analyst" },
    owl: { emoji: "\u{1F989}", label: "Night Researcher" },
    fox: { emoji: "\u{1F98A}", label: "Momentum Scout" },
    hawk: { emoji: "\u{1F985}", label: "Macro Hawk" },
    orca: { emoji: "\u{1F40B}", label: "Quant Orca" },
  });
  const DEFAULT_PROFILE_SOCIAL_LINKS = Object.freeze({
    website: "",
    x: "",
    linkedin: "",
    github: "",
    youtube: "",
    tiktok: "",
    facebook: "",
    instagram: "",
    reddit: "",
  });
  const PROFILE_SOCIAL_URL_RULES = Object.freeze({
    website: {
      hosts: [],
      allowAnyHost: true,
      requirePath: false,
    },
    x: {
      hosts: ["x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"],
      requirePath: true,
    },
    linkedin: {
      hosts: ["linkedin.com", "www.linkedin.com"],
      requirePath: true,
    },
    github: {
      hosts: ["github.com", "www.github.com"],
      requirePath: true,
    },
    youtube: {
      hosts: ["youtube.com", "www.youtube.com", "youtu.be"],
      requirePath: true,
    },
    tiktok: {
      hosts: ["tiktok.com", "www.tiktok.com", "vm.tiktok.com"],
      requirePath: true,
    },
    facebook: {
      hosts: ["facebook.com", "www.facebook.com", "m.facebook.com"],
      requirePath: true,
    },
    instagram: {
      hosts: ["instagram.com", "www.instagram.com"],
      requirePath: true,
    },
    reddit: {
      hosts: ["reddit.com", "www.reddit.com", "old.reddit.com"],
      requirePath: true,
    },
  });
  const UNSPLASH_CACHE_KEY = "quantura_unsplash_gallery_v1";
  const UNSPLASH_CACHE_TTL_MS = 1000 * 60 * 60 * 6;
  const UNSPLASH_FALLBACK_IMAGES = [
    {
      url: "https://images.unsplash.com/photo-1535320903710-d993d3d77d29?auto=format&fit=crop&w=1280&q=80",
      alt: "Finance workspace with market charts",
      link: "https://unsplash.com/photos/laptop-computer-on-glass-top-table-near-window-nA0UDNDbxys",
      photographer: "Adeolu Eletu",
      photographerLink: "https://unsplash.com/@adeolueletu",
    },
    {
      url: "https://images.unsplash.com/photo-1590283603385-17ffb3a7f29f?auto=format&fit=crop&w=1280&q=80",
      alt: "Stock market dashboard on laptop",
      link: "https://unsplash.com/photos/macbook-air-near-white-paper-BStWzy4M7vA",
      photographer: "Austin Distel",
      photographerLink: "https://unsplash.com/@austindistel",
    },
    {
      url: "https://images.unsplash.com/photo-1642790106117-e829e14a795f?auto=format&fit=crop&w=1280&q=80",
      alt: "Tablet with candlestick chart",
      link: "https://unsplash.com/photos/black-and-white-smartphone-on-brown-wooden-table-8wVYO8rK1j0",
      photographer: "Tech Daily",
      photographerLink: "https://unsplash.com/@techdailyca",
    },
    {
      url: "https://images.unsplash.com/photo-1559526324-4b87b5e36e44?auto=format&fit=crop&w=1280&q=80",
      alt: "Financial team reviewing growth metrics",
      link: "https://unsplash.com/photos/people-sitting-in-front-of-computer-MYbhN8KaaEc",
      photographer: "Campaign Creators",
      photographerLink: "https://unsplash.com/@campaign_creators",
    },
  ];
  const DEFAULT_AI_AGENTS = [
    {
      id: "quantura-oracle",
      name: "Quantura Oracle",
      description: "High-probability blue chips.",
      strategy: "quality_blue_chip",
      holdings: ["AAPL", "MSFT", "GOOGL", "V", "LLY", "COST"],
      returns: { "1m": 0.019, "3m": 0.057, "6m": 0.11, "1y": 0.183, "5y": 0.745, max: 0.745 },
      rationale:
        "This basket emphasizes high free cash flow consistency, durable balance sheets, and resilient earnings cadence. It is designed for steadier compounding across market regimes.",
      modelId: "gpt-5-mini",
      modelProvider: "openai",
      modelTier: "Core",
    },
    {
      id: "quantura-velocity",
      name: "Quantura Velocity",
      description: "Momentum and volatility leaders.",
      strategy: "momentum_volatility",
      holdings: ["NVDA", "TSLA", "AMD", "PLTR", "META", "AVGO"],
      returns: { "1m": 0.034, "3m": 0.102, "6m": 0.186, "1y": 0.322, "5y": 1.18, max: 1.18 },
      rationale:
        "Names are selected for strong relative strength, liquidity, and acceleration in trend metrics. The agent favors upside capture over downside smoothness.",
      modelId: "gpt-5",
      modelProvider: "openai",
      modelTier: "Pro",
    },
    {
      id: "quantura-dividend-king",
      name: "Quantura Dividend King",
      description: "Yield and stability.",
      strategy: "dividend_stability",
      holdings: ["JNJ", "KO", "PEP", "XOM", "CVX", "UNH"],
      returns: { "1m": 0.012, "3m": 0.033, "6m": 0.064, "1y": 0.121, "5y": 0.392, max: 0.392 },
      rationale:
        "The portfolio tilts toward durable payout profiles and lower drawdown sensitivity. It is tuned for investors prioritizing consistency and downside control.",
      modelId: "gpt-5-mini",
      modelProvider: "openai",
      modelTier: "Core",
    },
    {
      id: "quantura-horizon",
      name: "Quantura Horizon",
      description: "Long-term growth from Quantura Horizon scoring.",
      strategy: "prophet_growth",
      holdings: ["AAPL", "MSFT", "NVDA", "AMZN", "META", "LLY"],
      returns: { "1m": 0.026, "3m": 0.078, "6m": 0.142, "1y": 0.251, "5y": 0.984, max: 0.984 },
      rationale:
        "Quantura Horizon trend structure favors names with stable long-horizon slope and persistent seasonality. The set is filtered to avoid negative lower-bound outcomes.",
      modelId: "gpt-5",
      modelProvider: "openai",
      modelTier: "Pro",
    },
    {
      id: "quantura-contrarian",
      name: "Quantura Contrarian",
      description: "Oversold rebound opportunities.",
      strategy: "contrarian_rebound",
      holdings: ["NKE", "DIS", "PYPL", "SBUX", "BA", "INTC"],
      returns: { "1m": 0.016, "3m": 0.049, "6m": 0.091, "1y": 0.164, "5y": 0.46, max: 0.46 },
      rationale:
        "This set targets deep pullbacks with improving momentum breadth and valuation support. It is tuned for mean-reversion windows with defined upside asymmetry.",
      modelId: "gpt-5.2",
      modelProvider: "openai",
      modelTier: "Desk",
    },
    {
      id: "quantura-alphagen",
      name: "Quantura AlphaGen",
      description: "Balanced multi-factor alpha basket.",
      strategy: "multi_factor",
      holdings: ["AAPL", "NVDA", "JPM", "XOM", "COST", "CAT"],
      returns: { "1m": 0.021, "3m": 0.061, "6m": 0.116, "1y": 0.198, "5y": 0.71, max: 0.71 },
      rationale:
        "AlphaGen blends quality, momentum, valuation, and macro sensitivity into one portfolio. The goal is balanced risk-adjusted return through factor diversification.",
      modelId: "gpt-5-mini",
      modelProvider: "openai",
      modelTier: "Core",
    },
    {
      id: "quantura-deepvalue",
      name: "Quantura DeepValue",
      description: "Valuation compression reversals.",
      strategy: "deep_value",
      holdings: ["BAC", "CVX", "INTC", "BA", "C", "F"],
      returns: { "1m": 0.014, "3m": 0.041, "6m": 0.083, "1y": 0.146, "5y": 0.402, max: 0.402 },
      rationale:
        "DeepValue looks for discounted multiples with stabilization signals in earnings and cash flow. The portfolio is built for re-rating potential rather than headline momentum.",
      modelId: "gpt-5",
      modelProvider: "openai",
      modelTier: "Pro",
    },
    {
      id: "quantura-momenta",
      name: "Quantura Momenta",
      description: "Trend persistence and breakout continuation.",
      strategy: "trend_following",
      holdings: ["NVDA", "AVGO", "META", "AMD", "CRM", "MSFT"],
      returns: { "1m": 0.031, "3m": 0.094, "6m": 0.171, "1y": 0.302, "5y": 1.05, max: 1.05 },
      rationale:
        "Momenta emphasizes high-conviction trend continuation where breadth and liquidity remain supportive. It is optimized for sustained breakout environments.",
      modelId: "gpt-5.2",
      modelProvider: "openai",
      modelTier: "Desk",
    },
    {
      id: "preset-pelosi-radar",
      name: "Pelosi Radar",
      description: "Crossover from widely tracked Pelosi-style holdings.",
      strategy: "celebrity_portfolio",
      holdings: ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "PANW"],
      returns: { "1m": 0.028, "3m": 0.085, "6m": 0.151, "1y": 0.264, "5y": 0.99, max: 0.99 },
      rationale:
        "Preset built from high-liquidity names commonly discussed in public-trade trackers tied to Nancy Pelosi themed searches.",
      modelId: "gpt-5.2",
      modelProvider: "openai",
      modelTier: "Desk",
    },
    {
      id: "preset-bezos-growth",
      name: "Bezos Growth",
      description: "Mega-cap growth stack from Bezos-themed screens.",
      strategy: "celebrity_portfolio",
      holdings: ["AMZN", "MSFT", "GOOGL", "NVDA", "META", "SHOP"],
      returns: { "1m": 0.024, "3m": 0.074, "6m": 0.137, "1y": 0.232, "5y": 0.88, max: 0.88 },
      rationale:
        "Focuses on cloud, commerce, and AI infrastructure names frequently associated with Jeff Bezos portfolio-interest queries.",
      modelId: "gpt-5",
      modelProvider: "openai",
      modelTier: "Pro",
    },
    {
      id: "preset-cnbc-desk",
      name: "CNBC Desk",
      description: "High-velocity names that dominate financial media flow.",
      strategy: "media_signal",
      holdings: ["NVDA", "TSLA", "AAPL", "AMD", "PLTR", "META"],
      returns: { "1m": 0.03, "3m": 0.09, "6m": 0.16, "1y": 0.289, "5y": 1.04, max: 1.04 },
      rationale:
        "Uses media-intensity themes from CNBC-style market coverage where momentum and liquidity concentration are highest.",
      modelId: "gpt-5-mini",
      modelProvider: "openai",
      modelTier: "Core",
    },
    {
      id: "preset-bloomberg-macro",
      name: "Bloomberg Macro",
      description: "Cross-sector macro leaders from Bloomberg-style themes.",
      strategy: "media_signal",
      holdings: ["AAPL", "MSFT", "NVDA", "JPM", "XOM", "UNH"],
      returns: { "1m": 0.018, "3m": 0.058, "6m": 0.108, "1y": 0.191, "5y": 0.67, max: 0.67 },
      rationale:
        "Blends tech leadership with macro-sensitive financials and energy, mirroring recurring Bloomberg market narratives.",
      modelId: "gpt-5-mini",
      modelProvider: "openai",
      modelTier: "Core",
    },
    {
      id: "preset-blackrock-core",
      name: "BlackRock Core",
      description: "Institutional core equity basket.",
      strategy: "institutional_portfolio",
      holdings: ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"],
      returns: { "1m": 0.02, "3m": 0.064, "6m": 0.121, "1y": 0.212, "5y": 0.79, max: 0.79 },
      rationale:
        "Tracks liquid institutional leaders aligned with broad asset-management allocation patterns in BlackRock-themed screens.",
      modelId: "gpt-5",
      modelProvider: "openai",
      modelTier: "Pro",
    },
    {
      id: "preset-vanguard-factor",
      name: "Vanguard Factor",
      description: "Low-friction quality and profitability blend.",
      strategy: "institutional_portfolio",
      holdings: ["AAPL", "MSFT", "BRK.B", "JPM", "LLY", "COST"],
      returns: { "1m": 0.016, "3m": 0.049, "6m": 0.097, "1y": 0.176, "5y": 0.62, max: 0.62 },
      rationale:
        "Designed for consistency-first investors inspired by index-heavy Vanguard-style core factor exposure.",
      modelId: "gpt-5-mini",
      modelProvider: "openai",
      modelTier: "Core",
    },
    {
      id: "preset-ark-disruptors",
      name: "ARK Disruptors",
      description: "High-beta innovation and disruption stack.",
      strategy: "institutional_portfolio",
      holdings: ["TSLA", "COIN", "ROKU", "SQ", "PATH", "CRSP"],
      returns: { "1m": 0.033, "3m": 0.103, "6m": 0.186, "1y": 0.318, "5y": 1.22, max: 1.22 },
      rationale:
        "Captures disruptive-growth themes frequently associated with ARK Invest screens and innovation-centric flows.",
      modelId: "gpt-5.2",
      modelProvider: "openai",
      modelTier: "Desk",
    },
    {
      id: "preset-hedgefund-consensus",
      name: "Hedge Fund Consensus",
      description: "Concentrated consensus megacap picks.",
      strategy: "institutional_portfolio",
      holdings: ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"],
      returns: { "1m": 0.021, "3m": 0.067, "6m": 0.129, "1y": 0.224, "5y": 0.82, max: 0.82 },
      rationale:
        "Consensus-weighted megacap exposure based on recurring overlap across hedge fund and prime-broker commentary themes.",
      modelId: "gpt-5",
      modelProvider: "openai",
      modelTier: "Pro",
    },
  ];
  const ADMIN_SCREENER_PRESET_RUNS = [
    {
      id: "pelosi-tracker",
      title: "Nancy Pelosi Portfolio Tracker",
      notes: "Nancy Pelosi stock portfolio",
      modelUsed: "gpt-5.2",
      symbols: ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "PANW"],
    },
    {
      id: "bezos-favorites",
      title: "Jeff Bezos Favorite Stocks",
      notes: "Jeff Bezos favorite stocks",
      modelUsed: "gpt-5",
      symbols: ["AMZN", "MSFT", "GOOGL", "NVDA", "META", "SHOP"],
    },
    {
      id: "cnbc-theme",
      title: "CNBC Market Leaders",
      notes: "Top CNBC discussed growth stocks",
      modelUsed: "gpt-5-mini",
      symbols: ["NVDA", "TSLA", "AAPL", "AMD", "PLTR", "META"],
    },
    {
      id: "bloomberg-theme",
      title: "Bloomberg Macro Focus",
      notes: "Bloomberg market favorites and macro leaders",
      modelUsed: "gpt-5-mini",
      symbols: ["AAPL", "MSFT", "NVDA", "JPM", "XOM", "UNH"],
    },
    {
      id: "blackrock-core",
      title: "BlackRock Core Exposure",
      notes: "BlackRock top holdings style portfolio",
      modelUsed: "gpt-5",
      symbols: ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"],
    },
    {
      id: "vanguard-factor",
      title: "Vanguard Quality Factor",
      notes: "Vanguard core holdings and quality factor ideas",
      modelUsed: "gpt-5-mini",
      symbols: ["AAPL", "MSFT", "BRK.B", "JPM", "LLY", "COST"],
    },
    {
      id: "ark-disruptors",
      title: "ARK Innovation Disruptors",
      notes: "ARK Invest disruptive innovation stocks",
      modelUsed: "gpt-5.2",
      symbols: ["TSLA", "COIN", "ROKU", "SQ", "PATH", "CRSP"],
    },
    {
      id: "hedge-fund-consensus",
      title: "Hedge Fund Consensus Mega Caps",
      notes: "Most common hedge fund long positions this quarter",
      modelUsed: "gpt-5",
      symbols: ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"],
    },
  ];

  const hydrateUnsplashGallery = async (functionsClient) => {
    const gallery = document.getElementById("unsplash-grid");
    if (!gallery) return;

    const cards = Array.from(gallery.querySelectorAll("[data-unsplash-slot]"));
    if (!cards.length) return;

    const applyPhotos = (photos) => {
      if (!Array.isArray(photos) || !photos.length) return;
      cards.forEach((card, idx) => {
        const photo = photos[idx % photos.length];
        if (!photo || !photo.url) return;
        const img = card.querySelector("[data-unsplash-img]");
        if (img) {
          img.src = photo.url;
          img.alt = photo.alt || "Market imagery from Unsplash";
          img.loading = "lazy";
          img.decoding = "async";
        }
        const creditLink = card.querySelector("[data-unsplash-credit]");
        if (creditLink) {
          const creditText = photo.photographer
            ? `Photo by ${photo.photographer} on Unsplash`
            : "Photo on Unsplash";
          creditLink.textContent = creditText;
          creditLink.href = photo.photographerLink || photo.link || "https://unsplash.com/";
          creditLink.setAttribute("target", "_blank");
          creditLink.setAttribute("rel", "noopener noreferrer");
        }
      });
    };

    try {
      const raw = sessionStorage.getItem(UNSPLASH_CACHE_KEY);
      if (raw) {
        const cached = JSON.parse(raw);
        if (
          cached &&
          Array.isArray(cached.photos) &&
          cached.timestamp &&
          Date.now() - Number(cached.timestamp) < UNSPLASH_CACHE_TTL_MS
        ) {
          applyPhotos(cached.photos);
          return;
        }
      }
    } catch (error) {
      // Ignore cache read failures and continue to fetch.
    }

    applyPhotos(UNSPLASH_FALLBACK_IMAGES);

    if (!functionsClient || typeof functionsClient.httpsCallable !== "function") return;

    const rawQuery = String(gallery.dataset.unsplashQuery || "stock market, trading desk");
    const count = Math.max(1, Math.min(8, Number(gallery.dataset.unsplashCount || cards.length || 4)));

    try {
      const getGallery = functionsClient.httpsCallable("get_unsplash_gallery");
      const result = await getGallery({ query: rawQuery, count });
      const payload = result?.data && typeof result.data === "object" ? result.data : {};
      const photos = Array.isArray(payload.photos) ? payload.photos : [];

      if (!photos.length) return;
      applyPhotos(photos);
      try {
        sessionStorage.setItem(
          UNSPLASH_CACHE_KEY,
          JSON.stringify({
            timestamp: Date.now(),
            photos,
          })
        );
      } catch (error) {
        // Ignore cache write failures.
      }
    } catch (error) {
      // Keep fallback visuals when API is unavailable.
    }
  };

  const ensureTerminalFxPanelScaffold = () => {
    const terminalRoot = document.querySelector('[data-panels][data-panel-router="terminal"]');
    const panelColumn = terminalRoot?.querySelector(".studio-panel");
    if (!terminalRoot || !panelColumn) return;

    const fxSidebarLink = document.querySelector('[data-panel-target="fx"]');
    if (fxSidebarLink) {
      fxSidebarLink.setAttribute("href", "/forecasting?panel=fx");
    }

    if (panelColumn.querySelector('[data-panel="fx"]')) return;

    const fxPanel = document.createElement("section");
    fxPanel.className = "panel hidden";
    fxPanel.dataset.panel = "fx";
    fxPanel.innerHTML = `
      <div class="panel-header">
        <h2>Currency conversion</h2>
        <p class="small">Convert with live FX rates and keep a quick recent list for repeat checks.</p>
      </div>
      <form id="terminal-fx-form" class="card" autocomplete="off">
        <div class="form-grid">
          <div class="field">
            <label class="label" for="terminal-fx-amount">Amount</label>
            <input id="terminal-fx-amount" name="amount" type="number" min="0" step="0.0001" value="1" required />
          </div>
          <div class="field">
            <label class="label" for="terminal-fx-base">Base currency</label>
            <select id="terminal-fx-base" name="base" required>
              <option value="USD">USD</option>
              <option value="EUR">EUR</option>
              <option value="GBP">GBP</option>
              <option value="JPY">JPY</option>
              <option value="CAD">CAD</option>
              <option value="AUD">AUD</option>
              <option value="CHF">CHF</option>
              <option value="BDT">BDT</option>
              <option value="INR">INR</option>
              <option value="BTC">BTC</option>
            </select>
          </div>
          <div class="field">
            <label class="label" for="terminal-fx-quote">Quote currency</label>
            <select id="terminal-fx-quote" name="quote" required>
              <option value="EUR">EUR</option>
              <option value="USD" selected>USD</option>
              <option value="GBP">GBP</option>
              <option value="JPY">JPY</option>
              <option value="CAD">CAD</option>
              <option value="AUD">AUD</option>
              <option value="CHF">CHF</option>
              <option value="BDT">BDT</option>
              <option value="INR">INR</option>
              <option value="BTC">BTC</option>
            </select>
          </div>
        </div>
        <div class="hero-actions" style="margin-top: 12px;">
          <button type="button" class="cta secondary small" id="terminal-fx-swap">
            <i class="iconoir-arrows-up-from-line" aria-hidden="true"></i><span>Swap</span>
          </button>
          <button type="submit" class="cta small fx-convert-cta" id="terminal-fx-submit">
            <i class="iconoir-calculator" aria-hidden="true"></i><span>Convert</span>
          </button>
        </div>
        <p id="terminal-fx-status" class="small muted" style="margin-top: 10px;">Ready.</p>
      </form>
      <div class="results-panel">
        <h3>Conversion result</h3>
        <div id="terminal-fx-result" class="panel-output small">Run a conversion to view rate details.</div>
      </div>
      <div class="card">
        <h3>Recent conversions</h3>
        <div id="terminal-fx-recent" class="order-list panel-output small">No recent conversions yet.</div>
      </div>
    `;

    const screenerPanel = panelColumn.querySelector('[data-panel="screener"]');
    if (screenerPanel?.parentNode) {
      screenerPanel.parentNode.insertBefore(fxPanel, screenerPanel);
    } else {
      panelColumn.appendChild(fxPanel);
    }
  };

  ensureTerminalFxPanelScaffold();
  const ensureFiscalMacroPanelScaffold = () => {
    const macroSidebarLink = document.querySelector('[data-panel-target="macro"] span');
    if (macroSidebarLink) macroSidebarLink.textContent = "Macro Dashboard";
    const macroPanel = document.querySelector('[data-panel="macro"]');
    if (!macroPanel) return;
    if (macroPanel.querySelector("#fiscaldata-macro-groups")) return;
    macroPanel.innerHTML = `
      <div class="panel-header">
        <h2>Macro Dashboard</h2>
        <p class="small">Registry-driven Fiscal Data cards with schema-aware rendering and pagination.</p>
      </div>
      <div class="card" style="margin-bottom:16px;">
        <h3>U.S. Treasury Fiscal Data</h3>
        <p class="small muted" id="fiscaldata-macro-status">Loading registry and default macro cards...</p>
      </div>
      <div id="fiscaldata-macro-groups" class="content-grid">
        <div class="card">
          <div class="small muted">Loading cards...</div>
        </div>
      </div>
      <div id="fiscaldata-macro-details" class="modal hidden"></div>
    `;
  };

  ensureFiscalMacroPanelScaffold();

	  const ui = {
    headerAuth: document.getElementById("header-auth"),
    headerSignOut: document.getElementById("header-signout"),
    headerDashboard: document.getElementById("header-dashboard"),
    headerUserEmail: document.getElementById("header-user-email"),
    headerUserStatus: document.getElementById("header-user-status"),
    headerNotifications: document.getElementById("header-notifications"),
    pricingAuthCta: document.getElementById("pricing-auth-cta"),
    pricingStarterCta: document.getElementById("pricing-starter-cta"),
    emailForm: document.getElementById("email-auth-form"),
    emailInput: document.getElementById("auth-email"),
    passwordInput: document.getElementById("auth-password"),
    authForgotPassword: document.getElementById("auth-forgot-password"),
    emailCreate: document.getElementById("email-create"),
    emailMessage: document.getElementById("auth-email-message"),
    googleSignin: document.getElementById("google-signin"),
    facebookSignin: document.getElementById("facebook-signin"),
    githubSignin: document.getElementById("github-signin"),
    twitterSignin: document.getElementById("twitter-signin"),
    microsoftSignin: document.getElementById("microsoft-signin"),
    yahooSignin: document.getElementById("yahoo-signin"),
    anonymousSignin: document.getElementById("anonymous-signin"),
    languageSelect: document.getElementById("language-select"),
    userEmail: document.getElementById("user-email"),
    userProvider: document.getElementById("user-provider"),
    userStatus: document.getElementById("user-status"),
    profileForm: document.getElementById("profile-form"),
    profileUsername: document.getElementById("profile-username"),
    profileAvatar: document.getElementById("profile-avatar"),
    profileBio: document.getElementById("profile-bio"),
    profilePublicEnabled: document.getElementById("profile-public-enabled"),
    profilePublicScreener: document.getElementById("profile-public-screener"),
    profilePublicEmail: document.getElementById("profile-public-email"),
    profileWebsite: document.getElementById("profile-social-website"),
    profileX: document.getElementById("profile-social-x"),
    profileLinkedin: document.getElementById("profile-social-linkedin"),
    profileGithub: document.getElementById("profile-social-github"),
    profileYoutube: document.getElementById("profile-social-youtube"),
    profileTiktok: document.getElementById("profile-social-tiktok"),
    profileFacebook: document.getElementById("profile-social-facebook"),
    profileInstagram: document.getElementById("profile-social-instagram"),
    profileReddit: document.getElementById("profile-social-reddit"),
    profileConnectStripe: document.getElementById("profile-connect-stripe"),
    profileStatus: document.getElementById("profile-status"),
    dashboardCta: document.getElementById("dashboard-cta"),
    userOrders: document.getElementById("user-orders"),
    userForecasts: document.getElementById("user-forecasts"),
    adminSection: document.getElementById("admin"),
    adminOrders: document.getElementById("admin-orders"),
    adminAutopilot: document.getElementById("admin-autopilot"),
    adminFeatureVoteResults: document.getElementById("admin-feature-vote-results"),
    adminFiscaldataCapabilitiesStatus: document.getElementById("admin-fiscaldata-capabilities-status"),
    adminFiscaldataCapabilities: document.getElementById("admin-fiscaldata-capabilities"),
    contactForm: document.getElementById("contact-form"),
    navAdmin: document.getElementById("nav-admin"),
    terminalForm: document.getElementById("terminal-form"),
    terminalTicker: document.getElementById("terminal-ticker"),
    terminalInterval: document.getElementById("terminal-interval"),
    terminalStatus: document.getElementById("terminal-status"),
    tickerHistory: document.getElementById("ticker-history"),
    tickerChart: document.getElementById("ticker-chart"),
    studioChartShell: document.querySelector(".chart-shell.studio-chart"),
    indicatorChart: document.getElementById("indicator-chart"),
    intelStrip: document.getElementById("intel-strip"),
    tickerIntelligenceOutput: document.getElementById("ticker-output"),
    tickerPredictionsOutput: document.getElementById("ticker-predictions-output"),
    tickerIntelTabs: Array.from(document.querySelectorAll("[data-intel-tab]")),
    forecastForm: document.getElementById("forecast-form"),
    forecastTicker: document.getElementById("forecast-ticker"),
    forecastOutput: document.getElementById("forecast-output"),
    forecastLoadSelect: document.getElementById("forecast-load-select"),
    forecastLoadButton: document.getElementById("forecast-load-button"),
    forecastLoadStatus: document.getElementById("forecast-load-status"),
    technicalsForm: document.getElementById("technicals-form"),
    technicalsOutput: document.getElementById("technicals-output"),
    downloadForm: document.getElementById("download-form"),
    downloadStatus: document.getElementById("download-status"),
    downloadPreview: document.getElementById("download-preview"),
    trendingButton: document.getElementById("load-trending"),
    trendingList: document.getElementById("trending-list"),
    intelOutput: document.getElementById("intel-output"),
    newsOutput: document.getElementById("news-output"),
    xTrendingOutput: document.getElementById("x-trending-output"),
    eventsCalendarPreset: document.getElementById("events-calendar-preset"),
    eventsCalendarPrev: document.getElementById("events-calendar-prev"),
    eventsCalendarNext: document.getElementById("events-calendar-next"),
    eventsCalendarRangeLabel: document.getElementById("events-calendar-range-label"),
    eventsCalendarSearch: document.getElementById("events-calendar-search"),
    eventsCalendarSearchClear: document.getElementById("events-calendar-search-clear"),
    eventsCalendarDayStrip: document.getElementById("events-calendar-day-strip"),
    eventsCalendarSelectedDayTitle: document.getElementById("events-calendar-selected-day-title"),
    eventsCalendarStatus: document.getElementById("events-calendar-status"),
    eventsCalendarOutput: document.getElementById("events-calendar-output"),
    terminalFxForm: document.getElementById("terminal-fx-form"),
    terminalFxAmount: document.getElementById("terminal-fx-amount"),
    terminalFxBase: document.getElementById("terminal-fx-base"),
    terminalFxQuote: document.getElementById("terminal-fx-quote"),
    terminalFxSwap: document.getElementById("terminal-fx-swap"),
    terminalFxSubmit: document.getElementById("terminal-fx-submit"),
    terminalFxStatus: document.getElementById("terminal-fx-status"),
    terminalFxResult: document.getElementById("terminal-fx-result"),
    terminalFxRecent: document.getElementById("terminal-fx-recent"),
    marketHeadlinesForm: document.getElementById("market-headlines-form"),
    marketHeadlinesCountry: document.getElementById("market-headlines-country"),
    marketHeadlinesLimit: document.getElementById("market-headlines-limit"),
    marketHeadlinesStatus: document.getElementById("market-headlines-status"),
    marketHeadlinesOutput: document.getElementById("market-headlines-output"),
    marketSocialOutput: document.getElementById("market-social-output"),
    macroDashboardStatus: document.getElementById("fiscaldata-macro-status"),
    macroDashboardGroups: document.getElementById("fiscaldata-macro-groups"),
    macroDetailsModal: document.getElementById("fiscaldata-macro-details"),
    tickerQueryForm: document.getElementById("ticker-query-form"),
    tickerQueryTicker: document.getElementById("ticker-query-ticker"),
    tickerQueryQuestion: document.getElementById("ticker-query-question"),
    tickerQueryPromptCards: document.getElementById("ticker-query-prompt-cards"),
    tickerQueryPromptShuffle: document.getElementById("ticker-query-prompt-shuffle"),
    tickerQueryLanguage: document.getElementById("ticker-query-language"),
    tickerQueryProvider: document.getElementById("ticker-query-provider"),
    tickerQueryProviderHint: document.getElementById("ticker-query-provider-hint"),
    tickerQueryModel: document.getElementById("ticker-query-model"),
    tickerQueryModelHint: document.getElementById("ticker-query-model-hint"),
    tickerQueryModulesPicker: document.getElementById("ticker-query-modules-picker"),
    tickerQueryModulesOutput: document.getElementById("ticker-query-modules-output"),
    tickerQueryImproveToggle: document.getElementById("ticker-query-improve-toggle"),
    tickerQueryImprovePreviewWrap: document.getElementById("ticker-query-improve-preview-wrap"),
    tickerQueryImprovePreview: document.getElementById("ticker-query-improve-preview"),
    tickerQueryRunFinal: document.getElementById("ticker-query-run-final"),
    tickerQueryModelInfo: document.getElementById("ticker-query-model-info"),
    tickerQueryCacheToggleWrap: document.getElementById("ticker-query-cache-toggle-wrap"),
    tickerQueryShowCacheStats: document.getElementById("ticker-query-show-cache-stats"),
    tickerQueryCacheStats: document.getElementById("ticker-query-cache-stats"),
    tickerQueryStatus: document.getElementById("ticker-query-status"),
    tickerQueryOutput: document.getElementById("ticker-query-output"),
	    optionsForm: document.getElementById("options-form"),
	    optionsExpiration: document.getElementById("options-expiration"),
	    optionsOutput: document.getElementById("options-output"),
	    screenerForm: document.getElementById("screener-form"),
	    screenerOutput: document.getElementById("screener-output"),
    screenerModel: document.getElementById("screener-model"),
    screenerModelMeta: document.getElementById("screener-model-meta"),
    screenerCreditsText: document.getElementById("screener-credits-text"),
    screenerCreditsFill: document.getElementById("screener-credits-fill"),
    screenerResultsCount: document.getElementById("screener-results-count"),
    screenerGenerateButton: document.getElementById("screener-generate-button"),
	    screenerLoadSelect: document.getElementById("screener-load-select"),
	    screenerLoadButton: document.getElementById("screener-load-button"),
	    screenerLoadStatus: document.getElementById("screener-load-status"),
    myRequestsPanels: Array.from(document.querySelectorAll("[data-my-requests-panel]")),
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
    predictionsTicker: document.getElementById("predictions-ticker"),
    predictionsOutput: document.getElementById("predictions-output"),
    predictionsStatus: document.getElementById("predictions-status"),
    uploadsVoteBlock: document.getElementById("uploads-vote-block"),
    uploadsAdminBlock: document.getElementById("uploads-admin-block"),
    featureVoteUploadsForm: document.getElementById("feature-vote-uploads-form"),
    featureVoteUploadsStatus: document.getElementById("feature-vote-uploads-status"),
    autopilotForm: document.getElementById("autopilot-form"),
    autopilotOutput: document.getElementById("autopilot-output"),
    autopilotStatus: document.getElementById("autopilot-status"),
    autopilotVoteBlock: document.getElementById("autopilot-vote-block"),
    autopilotAdminBlock: document.getElementById("autopilot-admin-block"),
    featureVoteAutopilotForm: document.getElementById("feature-vote-autopilot-form"),
    featureVoteAutopilotStatus: document.getElementById("feature-vote-autopilot-status"),
    savedForecastsList: document.getElementById("saved-forecasts-list"),
    workspaceSelect: document.getElementById("workspace-select"),
    dashboardAuthLink: document.getElementById("dashboard-auth-link"),
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
    notificationsItems: document.getElementById("notifications-items"),
    notificationsUnreadCount: document.getElementById("notifications-unread-count"),
    notificationsMarkAll: document.getElementById("notifications-mark-all"),
    notificationFilterButtons: Array.from(document.querySelectorAll("[data-notification-filter]")),
    notificationsLog: document.getElementById("notifications-log"),
    notificationsClear: document.getElementById("notifications-clear"),
    notificationsPrivacyContainer: document.getElementById("notifications-privacy-controls"),
    notificationsLocationOptIn: document.getElementById("notifications-location-optin"),
    notificationsIpOptIn: document.getElementById("notifications-ip-optin"),
    notificationsRequestLocation: document.getElementById("notifications-request-location"),
    notificationsPrivacyStatus: document.getElementById("notifications-privacy-status"),
    billingPortalLink: document.getElementById("billing-portal-link"),
    chartRangeButtons: Array.from(document.querySelectorAll("[data-chart-range]")),
    chartViewButtons: Array.from(document.querySelectorAll("[data-chart-view]")),
    chartThemeButtons: Array.from(document.querySelectorAll("[data-tv-theme]")),
    tradingViewRoot: document.getElementById("tradingview-terminal-root"),
    tradingViewStatus: document.getElementById("tv-widget-status"),
    tradingViewTickerTape: document.getElementById("tv-ticker-tape"),
    tradingViewSymbolInfo: document.getElementById("tv-symbol-info"),
    tradingViewAdvanced: document.getElementById("tv-advanced-chart"),
    tradingViewCompanyProfile: document.getElementById("tv-company-profile"),
    tradingViewFundamentalData: document.getElementById("tv-fundamental-data"),
    tradingViewTechnicalAnalysis: document.getElementById("tv-technical-analysis"),
    tradingViewTopStories: document.getElementById("tv-top-stories"),
    tradingViewFallback: document.getElementById("tv-widget-fallback"),
    predictionsChart: document.getElementById("predictions-chart"),
    predictionsPreview: document.getElementById("predictions-preview"),
    predictionsPlotMeta: document.getElementById("predictions-plot-meta"),
    predictionsAgentButton: document.getElementById("predictions-agent-button"),
    predictionsAgentOutput: document.getElementById("predictions-agent-output"),
    toast: document.getElementById("toast"),
	    purchasePanels: Array.from(document.querySelectorAll(".purchase-panel")),
	  };

  if (ui.anonymousSignin) {
    ui.anonymousSignin.style.display = "none";
    ui.anonymousSignin.disabled = true;
    ui.anonymousSignin.setAttribute("aria-hidden", "true");
  }

  const polymarketClientCache = new Map();
  const trendingLogoCache = new Map();
  const trendingLogoInFlight = new Map();
  let polymarketSearchDebounceTimer = 0;
  let polymarketInFlightController = null;
  let polymarketInFlightNonce = 0;

	  const state = {
	    user: null,
    userHasPaidPlan: false,
    userSubscriptionTier: "free",
    userProfile: {
      username: "",
      socialLinks: { ...DEFAULT_PROFILE_SOCIAL_LINKS },
      avatar: "bull",
      bio: "",
      publicProfile: false,
      publicScreenerSharing: false,
      publicEmailOptIn: false,
      stripeConnectAccountId: "",
    },
    preferredLanguage: "en",
    preferredCountry: "US",
    notificationPrivacy: (() => {
      let cached = {};
      try {
        const raw = localStorage.getItem(NOTIFICATION_PRIVACY_CACHE_KEY);
        cached = raw ? JSON.parse(raw) : {};
      } catch (error) {
        cached = {};
      }
      return {
        locationConsent: Boolean(cached?.locationConsent),
        ipRegionConsent: Boolean(cached?.ipRegionConsent),
        coarseLocation: cached?.coarseLocation && typeof cached.coarseLocation === "object" ? cached.coarseLocation : null,
        ipRegion: String(cached?.ipRegion || "").trim().slice(0, 80),
        timezone: String(cached?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || "").trim().slice(0, 80),
        lastUpdatedMs: Number(cached?.lastUpdatedMs || 0) || 0,
      };
    })(),
    cookieConsent: (() => {
      try {
        return localStorage.getItem(COOKIE_CONSENT_KEY) || "";
      } catch (error) {
        return "";
      }
    })(),
		    initialPageViewSent: false,
	    authResolved: false,
	      authInFlight: false,
      anonymousBootstrapInFlight: false,
      nativeAuthPromptRequested: false,
      nativeAuthState: null,
    intelActiveTab: "intelligence",
    tickerContext: {
	      ticker: "",
      activeTicker: "",
	      interval: "1d",
	      rows: [],
      forecastId: "",
      forecastDoc: null,
	      indicatorOverlays: [],
	      forecastTablePage: 0,
      forecastAiSummary: null,
      forecastCacheMeta: null,
      newsTicker: "",
      xTicker: "",
      intelTicker: "",
      predictionsTicker: "",
      predictionsMode: "ticker",
      predictionsQuery: "",
      predictionsIncludeClosed: false,
      predictionsExpanded: false,
      predictionsRequestKey: "",
      optionsTicker: "",
      predictionsData: null,
      tickerHistory: [],
      tickerQueryProvider: "openai",
      tickerQueryModel: "gpt-5-mini",
      tickerQueryModels: [],
      tickerQueryProviders: [],
      tickerQueryModelsLoaded: false,
      tickerQueryModules: [],
      tickerQueryLastResponseId: "",
      tickerQueryLastResponse: null,
      tickerQueryShareUrl: "",
      tickerQueryPendingQuestion: "",
      tickerQueryPendingProvider: "",
      tickerQueryPendingModel: "",
      tickerQueryFeedback: "",
      tickerQueryPromptDeck: [],
      tickerQueryPromptCursor: 0,
    },
    predictionsContext: {
      uploadId: "",
      uploadDoc: null,
      table: null,
      previewPage: 0,
      previewPageSize: 25,
    },
    aiLeaderboardHorizon: AI_LEADERBOARD_DEFAULT_HORIZON,
    aiModelFilter: "all",
    aiAgents: [],
    aiFollowSet: new Set(),
    aiLikeSet: new Set(),
    fiscaldataCapabilities: null,
    fiscaldataCapabilitiesLoadedAt: 0,
    fiscaldataRegistry: [],
    fiscaldataRegistryLoadedAt: 0,
    fiscaldataMacroPages: {},
    earningsCalendar: {
      preset: "this-week",
      rangeStart: "",
      rangeEnd: "",
      selectedDate: "",
      search: "",
      rows: [],
      rowsByDate: {},
      rangeDates: [],
      requestCache: new Map(),
      inFlightController: null,
      pageByDate: {},
      follows: new Set(),
      followsUid: "",
    },
    aiUsageToday: 0,
    aiUsageDateKey: "",
    aiUsageTierKey: "free",
    selectedScreenerModel: "gpt-5-mini",
    aiDefaultsSeededWorkspaceId: "",
    recentWatchlistItems: [],
    volatilityMonitorTimer: null,
    clients: {
      auth: null,
      db: null,
      functions: null,
      storage: null,
      messaging: null,
    },
    panelAutoloaded: {},
    sideDataRefreshTimer: null,
    pendingShareId: "",
    pendingShareProcessed: false,
    sharedScreenerView: null,
    promoStatus: null,
    promoClockOffsetMs: 0,
    promoTimer: null,
    promoSessionCount: 0,
    promoForecastCount: 0,
    promoSessionTouched: false,
    promoModalShown: false,
    taskCalendarCursor: null,
    taskCalendarTasks: [],
	    unsubscribeOrders: null,
	    unsubscribeAdmin: null,
	    unsubscribeAdminAutopilot: null,
	    featureVoteSummaryTimer: null,
	    unsubscribeForecasts: null,
	    unsubscribeAutopilot: null,
	    unsubscribePredictions: null,
	    unsubscribeTasks: null,
	    unsubscribeWatchlist: null,
	    unsubscribeAlerts: null,
	    unsubscribeScreenerRuns: null,
      unsubscribeAIAgents: null,
      unsubscribeAIFollows: null,
      unsubscribeAILikes: null,
      collaboratorCount: 0,
      pendingCollabInviteCount: 0,
	    screenerUrlRunLoaded: false,
      uploadUrlLoaded: false,
	    messagingBound: false,
	    remoteConfigLoaded: false,
    remoteFlags: {
	      watchlistEnabled: true,
	      forecastProphetEnabled: true,
	      forecastCanvasEnabled: true,
        enableSocialLeaderboard: true,
        forecastModelPrimary: "Quantura Horizon",
        promoBannerText: "",
        maintenanceMode: false,
	      pushEnabled: true,
	      webPushVapidKey: "",
        volatilityThreshold: DEFAULT_VOLATILITY_THRESHOLD,
        llmAllowedModels: DEFAULT_LLM_ALLOWED_MODELS,
        aiUsageTiers: AI_USAGE_TIER_DEFAULTS,
        stripeCheckoutEnabled: true,
        stripePublicKey: "",
        nativeIosStoreKitCheckoutOnly: true,
        nativeAndroidPlayBillingEnabled: true,
        nativeIapProductIds: DEFAULT_NATIVE_IAP_PRODUCT_IDS,
        adsUseRealIOS: true,
        adsUseRealAndroid: true,
	    },
	    remoteConfigRefreshTimer: null,
	    remoteConfigUnsubscribe: null,
    activeWorkspaceId: (() => {
	      try {
	        return localStorage.getItem(WORKSPACE_KEY) || "";
	      } catch (error) {
        return "";
      }
    })(),
    chartRangePreset: (() => {
      const allowed = new Set(["1d", "5d", "1m", "3m", "ytd", "1y", "5y", "max"]);
      let raw = "max";
      try {
        raw = String(localStorage.getItem(CHART_RANGE_CACHE_KEY) || "max").trim().toLowerCase();
      } catch (error) {
        raw = "max";
      }
      return allowed.has(raw) ? raw : "max";
    })(),
    chartViewMode: (() => {
      let raw = "candlestick";
      try {
        raw = String(localStorage.getItem(CHART_VIEW_CACHE_KEY) || "candlestick").trim().toLowerCase();
      } catch (error) {
        raw = "candlestick";
      }
      return raw === "line" ? "line" : "candlestick";
    })(),
    tradingViewTheme: (() => {
      try {
        const raw = String(localStorage.getItem(TRADINGVIEW_THEME_CACHE_KEY) || "auto").trim().toLowerCase();
        return raw === "dark" || raw === "light" ? raw : "auto";
      } catch (error) {
        return "auto";
      }
    })(),
    tradingViewRenderNonce: 0,
    tradingViewLoadTimer: null,
    tradingViewLoadFailed: false,
    notificationLog: (() => {
      try {
        const raw = localStorage.getItem(FCM_LOG_CACHE_KEY);
        if (!raw) return [];
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return [];
        return parsed
          .slice(0, 30)
          .map((entry) => ({
            title: String(entry?.title || "Quantura update"),
            body: String(entry?.body || ""),
            source: String(entry?.source || "unknown"),
            at: String(entry?.at || new Date().toISOString()),
            personalized: Boolean(entry?.personalized),
            nextSteps: Array.isArray(entry?.nextSteps) ? entry.nextSteps.slice(0, 4).map((item) => String(item)) : [],
          }));
      } catch (error) {
        return [];
      }
    })(),
    notificationFeed: {
      items: [],
      unreadCount: 0,
      filter: "all",
      unreadOnly: false,
      loading: false,
    },
    myRequests: [],
    myRequestsById: {},
    myRequestsLoading: false,
    myRequestsLoadedAt: 0,
    myRequestsPanelState: {},
    sharedWorkspaces: [],
    unsubscribeSharedWorkspaces: null,
    authStateBootstrapped: false,
    postSignInReloadInFlight: false,
    rewardIncentiveLimits: {},
  };
  applyRuntimeBodyClasses();

  const remoteConfigStore = (() => {
    const listeners = new Set();
    return {
      getSnapshot: () => ({ ...state.remoteFlags }),
      subscribe: (listener) => {
        if (typeof listener !== "function") return () => {};
        listeners.add(listener);
        return () => listeners.delete(listener);
      },
      publish: (flags) => {
        listeners.forEach((listener) => {
          try {
            listener({ ...flags });
          } catch (error) {
            // Ignore listener errors.
          }
        });
      },
    };
  })();

  // React-style hook analogue for this vanilla app: subscribe to Remote Config updates.
  const useRemoteConfig = (listener) => remoteConfigStore.subscribe(listener);

  const hasSessionUser = (user = state.user) => Boolean(user?.uid);
  const isAnonymousUser = (user = state.user) => Boolean(user?.isAnonymous);
  const hasFullAccount = (user = state.user) => Boolean(user && !user.isAnonymous);
  const isAdminUser = (user = state.user) =>
    String(user?.email || "").trim().toLowerCase() === String(ADMIN_EMAIL).trim().toLowerCase();
  const requestNativeAuthGate = ({ reason = "sign_in_required", message = "Sign in to continue." } = {}) => {
    if (!isNativeApp()) return false;
    const branding = {
      name: "Quantura",
      logoUrl: `${window.location.origin}/assets/logo.png`,
      primaryColor: "#0f2a61",
      accentColor: "#3ab5a2",
      ctaColor: "#1d5ed8",
      textColor: "#12182a",
    };
    const nativeAuthRequested = sendNativeAuthMessage({
      type: "REQUEST_SIGN_IN",
      reason: String(reason || "").trim().slice(0, 80),
      message: String(message || "").trim().slice(0, 280),
      branding,
      sourcePath: window.location.pathname,
      sourceRuntime: resolveRuntimeLabel(),
    });
    sendNativeBridgeMessage({
      action: "showAuthGate",
      reason: String(reason || "").trim().slice(0, 80),
      message: String(message || "").trim().slice(0, 280),
      branding,
      sourcePath: window.location.pathname,
      sourceRuntime: resolveRuntimeLabel(),
      ts: Date.now(),
    });
    return nativeAuthRequested;
  };
  const ensureSessionUser = async ({ reason = "session_required", message = "Initializing guest session..." } = {}) => {
    if (hasSessionUser()) return state.user;
    const auth = state.clients?.auth;
    if (!auth) throw new Error("Authentication is not initialized.");
    const ownsBootstrap = !state.anonymousBootstrapInFlight;
    if (ownsBootstrap) {
      state.anonymousBootstrapInFlight = true;
    }
    try {
      await auth.signInAnonymously();
    } catch (error) {
      requestNativeAuthGate({ reason, message });
      throw error;
    } finally {
      if (ownsBootstrap) {
        state.anonymousBootstrapInFlight = false;
      }
    }
    const nextUser = auth.currentUser || state.user;
    if (nextUser) {
      state.user = nextUser;
      return nextUser;
    }
    requestNativeAuthGate({ reason, message });
    throw new Error("Unable to initialize guest session.");
  };
  const requireFullAccount = (message = "Sign in to continue.", opts = {}) => {
    if (hasFullAccount()) return true;
    const redirect = opts && opts.redirect === true;
    const nextMessage = isAnonymousUser()
      ? "Create an account or sign in to use this feature."
      : String(message || "Sign in to continue.");
    showToast(nextMessage, "warn");
    requestNativeAuthGate({
      reason: "full_account_required",
      message: nextMessage,
    });
    if (redirect && !isNativeApp() && window.location.pathname !== "/account") {
      window.location.href = "/account";
    }
    return false;
  };
  const requireAdminAccess = (message = "Admin access required for this feature.") => {
    if (!requireFullAccount(message, { redirect: true })) return false;
    if (isAdminUser()) return true;
    showToast(String(message || "Admin access required."), "warn");
    return false;
  };

  const shouldSkipNativeRewardAds = () => {
    if (!isNativeApp()) return true;
    if (!state.authResolved) return true;
    return false;
  };

  const maybeShowNativeRewardGate = async ({
    reason = "action",
    title = "Watch a short ad first?",
    message = "This action can unlock additional output. Continue to show a rewarded ad.",
  } = {}) => {
    if (shouldSkipNativeRewardAds()) return true;
    const normalizedReason = String(reason || "action").trim() || "action";
    const isNavigationGate = normalizedReason === "nav";
    const declinedAt = Number(state.rewardIncentiveLimits?.[normalizedReason] || 0);
    if (Number.isFinite(declinedAt) && declinedAt > 0 && Date.now() - declinedAt < 10 * 60 * 1000) {
      sendNativeBridgeMessage({
        action: "showInterstitial",
        reason: `${normalizedReason}_reward_limited`,
        mode: "fallback",
        ts: Date.now(),
      });
      if (isNavigationGate) return true;
      showToast("Reward incentives are limited after skipping the video ad. Try again shortly or watch the reward ad.", "warn");
      return false;
    }
    const confirmed = await openConfirmModal({
      title,
      message,
      confirmLabel: "Watch video ad",
      cancelLabel: "Skip reward",
    });
    if (!confirmed) {
      if (!isNavigationGate) {
        state.rewardIncentiveLimits[normalizedReason] = Date.now();
      }
      sendNativeBridgeMessage({
        action: "showInterstitial",
        reason: `${normalizedReason}_reward_declined`,
        mode: "fallback",
        ts: Date.now(),
      });
      if (isNavigationGate) return true;
      showToast("Reward skipped. Incentive output is limited for this action.", "warn");
      return false;
    }
    delete state.rewardIncentiveLimits[normalizedReason];
    sendNativeBridgeMessage({
      action: "showRewardedInterstitial",
      reason: normalizedReason,
      ts: Date.now(),
    });
    return true;
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
	    el.innerHTML = `<div data-skeleton>${skeletonHtml()}<div class="small muted" style="margin-top:10px;">${label}</div></div>`;
	  };

		  const setOutputReady = (el) => {
		    if (!el) return;
		    el.removeAttribute("aria-busy");
        const skeleton = el.querySelector?.("[data-skeleton]");
        if (skeleton) skeleton.remove();
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
              ticker: "/ticker-intelligence",
              predictions: "/ticker-intelligence?panel=predictions",
		          indicators: "/indicators",
              trending: "/trending",
		          news: "/news",
              "events-calendar": "/events-calendar",
              "market-headlines": "/market-headlines",
              "ticker-query": "/model-council",
		          options: "/options",
              fx: "/forecasting",
		          learn: "/studio",
            },
            pathAliases: {
              "/ticker-intelligence": "ticker",
              "/ticker-query": "ticker-query",
              "/model-council": "ticker-query",
              "/tools/fx": "fx",
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
			          notifications: "/notifications",
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
          Object.entries(router.pathAliases || {}).forEach(([path, panel]) => {
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
            const desiredUrl = desired.includes("?") ? desired : `${desired}${window.location.search}`;
            const currentUrl = `${window.location.pathname}${window.location.search}`;
		        if (desiredUrl && currentUrl !== desiredUrl) {
		          try {
		            history.pushState({ panel: next }, "", desiredUrl);
		          } catch (error) {
		            // Ignore.
		          }
		        }
		      }
		      logEvent("panel_view", { panel: next, page_path: window.location.pathname });
		      try {
		        if (typeof window !== "undefined" && typeof window.__quanturaPanelActivated === "function") {
		          window.__quanturaPanelActivated(next);
		        }
		      } catch (error) {
		        // Ignore.
		      }
          try {
            if (typeof window !== "undefined" && typeof window.__quanturaMobileBottomNavSync === "function") {
              window.__quanturaMobileBottomNavSync();
            }
            if (typeof bindMobileBottomNav === "function") {
              bindMobileBottomNav();
            }
          } catch (error) {
            // Ignore.
          }
		    };

		    const initialFromUrl = () => {
		      try {
		        const params = new URLSearchParams(window.location.search);
		        const panel = String(params.get("panel") || "").trim();
		        if (panel === "ticker-intelligence") return "ticker";
		        if (panel) return panel;
            const intel = String(params.get("intel") || "").trim().toLowerCase();
            if (intel === "predictions") return "predictions";
		      } catch (error) {
		        // Ignore.
		      }
		      if (router && pathToPanel[window.location.pathname]) {
		        return pathToPanel[window.location.pathname];
		      }
		      return (window.location.hash || "").replace(/^#/, "");
		    };

		    buttons.forEach((btn) => {
		      btn.addEventListener("click", async (event) => {
		        event.preventDefault?.();
            triggerSubtleHaptic();
            const targetPanel = String(btn.dataset.panelTarget || "").trim();
            const proceed = await maybeShowNativeRewardGate({
              reason: "nav",
              title: "Watch an ad before navigating?",
              message: "Navigation inside the native app can trigger a rewarded interstitial.",
            });
            if (!proceed) return;
		        setActive(targetPanel);
		      });
		    });

		    const initial = initialFromUrl() || router?.defaultPanel || buttons[0].dataset.panelTarget;
		    setActive(initial, { pushPath: false });

        if (typeof window !== "undefined") {
          window.__quanturaSetPanel = (panel, options = {}) => {
            const next = String(panel || "").trim();
            if (!next) return;
            setActive(next, options);
          };
        }

		    window.addEventListener("popstate", () => {
		      const next = initialFromUrl();
		      if (next) setActive(next, { pushPath: false });
		    });
		  };

  const bindFaqAccordion = () => {
    const grids = Array.from(document.querySelectorAll(".faq-grid"));
    grids.forEach((grid) => {
      const items = Array.from(grid.querySelectorAll(".faq-item"));
      if (!items.length) return;
      items.forEach((item) => {
        item.addEventListener("toggle", () => {
          if (!item.open) return;
          items.forEach((other) => {
            if (other !== item && other.open) {
              other.open = false;
            }
          });
        });
      });
    });
  };

  const bindMobileNav = () => {
    const header = document.querySelector(".header");
    const nav = header?.querySelector(".nav");
    const logo = nav?.querySelector(".logo");
    const links = header?.querySelector(".nav-links");
    const actions = header?.querySelector(".nav-actions");
    if (!header || !nav || !links || !actions) return;

    let toggle = header.querySelector(".mobile-nav-toggle");
    let backdrop = header.querySelector(".mobile-nav-backdrop");
    if (!toggle) {
      toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "mobile-nav-toggle";
      toggle.setAttribute("aria-label", "Open navigation menu");
      toggle.setAttribute("aria-expanded", "false");
      toggle.innerHTML = icon("menu-scale");
    }
    if (logo?.parentNode === nav) {
      nav.insertBefore(toggle, logo);
    } else {
      nav.prepend(toggle);
    }
    if (!backdrop) {
      backdrop = document.createElement("button");
      backdrop.type = "button";
      backdrop.className = "mobile-nav-backdrop hidden";
      backdrop.setAttribute("aria-label", "Close navigation menu");
      header.appendChild(backdrop);
    }

    const setToggleVisualState = (open) => {
      toggle?.setAttribute("aria-expanded", open ? "true" : "false");
      toggle?.setAttribute("aria-label", open ? "Close navigation menu" : "Open navigation menu");
      toggle.innerHTML = open ? icon("xmark") : icon("menu-scale");
    };

    const close = () => {
      header.classList.remove("nav-open");
      setToggleVisualState(false);
      backdrop?.classList.add("hidden");
      document.body.classList.remove("mobile-nav-lock");
      links.style.removeProperty("top");
      actions.style.removeProperty("top");
    };
    const syncOverlayPositions = () => {
      if (window.innerWidth > 980) return;
      const baseTop = Math.round(header.getBoundingClientRect().height + 8);
      links.style.top = `${baseTop}px`;
      const linksHeight = Math.round(links.getBoundingClientRect().height || 0);
      actions.style.top = `${baseTop + linksHeight + 10}px`;
    };
    const open = () => {
      header.classList.add("nav-open");
      setToggleVisualState(true);
      backdrop?.classList.remove("hidden");
      document.body.classList.add("mobile-nav-lock");
      syncOverlayPositions();
    };

    toggle.addEventListener("click", () => {
      if (header.classList.contains("nav-open")) {
        close();
      } else {
        open();
      }
    });
    backdrop.addEventListener("click", close);
    links.querySelectorAll("a").forEach((a) => a.addEventListener("click", close));
    actions.querySelectorAll("a,button").forEach((el) => {
      if (el === toggle) return;
      el.addEventListener("click", close);
    });
    window.addEventListener("resize", () => {
      if (window.innerWidth > 980) close();
      else if (header.classList.contains("nav-open")) syncOverlayPositions();
    });

    setToggleVisualState(header.classList.contains("nav-open"));
  };

  const bindMobileBottomNav = () => {
    const panelRoot = document.querySelector("[data-panels][data-panel-router]");
    const sidebarNav = document.querySelector(".app-sidebar .sidebar-nav");
    if (!sidebarNav) return;
    const path = normalizePath(window.location.pathname || "/");
    const routeRouterFallback = (() => {
      if (
        [
          "/terminal",
          "/forecasting",
          "/ticker-intelligence",
          "/indicators",
          "/trending",
          "/news",
          "/events-calendar",
          "/market-headlines",
          "/model-council",
          "/options",
          "/tools/fx",
          "/screener",
        ].includes(path)
      ) {
        return "terminal";
      }
      if (["/dashboard", "/account", "/watchlist", "/productivity", "/collaboration", "/uploads", "/autopilot", "/notifications"].includes(path)) {
        return "dashboard";
      }
      return "";
    })();
    const routerName = String(panelRoot?.dataset?.panelRouter || routeRouterFallback || "").trim();
    if (!routerName) return;

    let nav = document.getElementById("mobile-bottom-nav");
    if (!nav) {
      nav = document.createElement("nav");
      nav.id = "mobile-bottom-nav";
      nav.className = "mobile-bottom-nav hidden";
      nav.setAttribute("aria-label", "Mobile navigation");
      nav.innerHTML = `<div class="mobile-bottom-nav-inner"></div>`;
      document.body.appendChild(nav);
    }
    const inner = nav.querySelector(".mobile-bottom-nav-inner");
    if (!inner) return;

    const sidebarLinks = Array.from(sidebarNav.querySelectorAll("a[href]"));
    const byPanel = new Map();
    sidebarLinks.forEach((link) => {
      const panel = String(link.dataset.panelTarget || "").trim();
      const href = String(link.getAttribute("href") || "").trim();
      const key = panel || href;
      if (!key || byPanel.has(key)) return;
      byPanel.set(key, link);
    });

    const preferredByRouter = {
      terminal: [
        "forecast",
        "/forecasting",
        "ticker",
        "/ticker-intelligence",
        "predictions",
        "/ticker-intelligence?panel=predictions",
        "/ticker-intelligence?intel=predictions",
        "ticker-query",
        "/model-council",
        "fx",
        "/forecasting?panel=fx",
        "/tools/fx",
      ],
      dashboard: ["orders", "profile", "watchlist", "uploads", "notifications", "/explore"],
    };
    const preferredPanels = preferredByRouter[routerName] || [];
    const selected = preferredPanels
      .map((panel) => byPanel.get(panel))
      .filter(Boolean)
      .slice(0, 5);

    if (selected.length < 5) {
      for (const link of byPanel.values()) {
        if (selected.length >= 5) break;
        if (!selected.includes(link)) selected.push(link);
      }
    }

    if (!selected.length) return;

    inner.innerHTML = selected
      .map((link) => {
        const panel = String(link.dataset.panelTarget || "").trim();
        const href = String(link.getAttribute("href") || "#");
        const iconMarkup = link.querySelector("i")?.outerHTML || icon("nav-arrow-right");
        const label = String(link.textContent || "").trim();
        const compactLabelMap = {
          "Currency conversion": "FX",
          "Model Council": "Council",
          "Watchlist and alerts": "Watchlist",
          "News and data": "News",
          "Earnings calendar": "Earnings",
          Productivity: "Tasks",
        };
        const compactLabel = compactLabelMap[label] || label;
        const panelAttr = panel ? ` data-panel-target="${escapeHtml(panel)}"` : "";
        const hrefPath = normalizePath(href.split("?")[0].split("#")[0] || "");
        const hrefQuery = href.includes("?") ? href.split("?")[1].split("#")[0] : "";
        const currentQuery = String(window.location.search || "").replace(/^\?/, "");
        const hrefMatchesPath = Boolean(hrefPath && hrefPath === path);
        const hrefMatchesQuery = !hrefQuery || hrefQuery === currentQuery;
        const activeClass =
          link.classList.contains("active") || (hrefMatchesPath && hrefMatchesQuery) ? " active" : "";
        return `
          <a class="mobile-bottom-link${activeClass}" href="${escapeHtml(href)}"${panelAttr} aria-label="${escapeHtml(label)}" title="${escapeHtml(label)}">
            ${iconMarkup}
            <span class="mobile-bottom-label">${escapeHtml(compactLabel)}</span>
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
    window.__quanturaMobileBottomNavSync = syncVisibility;
  };

  const bindNativeRewardedNavigationAds = () => {
    if (!isNativeApp()) return;
    if (document.body.dataset.nativeNavRewardBound === "1") return;
    document.body.dataset.nativeNavRewardBound = "1";

    document.addEventListener("click", async (event) => {
      const link = event.target.closest("a[href]");
      if (!link) return;
      if (link.dataset.panelTarget) return;
      if (link.dataset.skipRewardGate === "1") return;
      if (event.defaultPrevented) return;
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      const target = String(link.getAttribute("target") || "").trim().toLowerCase();
      if (target === "_blank") return;

      const href = String(link.getAttribute("href") || "").trim();
      if (!href || href.startsWith("#") || href.startsWith("mailto:") || href.startsWith("tel:")) return;
      if (/^https?:\/\//i.test(href)) return;
      if (!href.startsWith("/")) return;

      const proceed = await maybeShowNativeRewardGate({
        reason: "nav",
        title: "Watch an ad before opening this section?",
        message: "Navigation inside the native app can trigger a rewarded interstitial.",
      });
      if (!proceed) {
        event.preventDefault();
        return;
      }

      sendNativeBridgeMessage({
        action: "showRewardedInterstitial",
        reason: "nav",
        href,
        ts: Date.now(),
      });
    });
  };

  const bindMobileSidebarDrawer = () => {
    const sidebar = document.querySelector(".app-sidebar");
    const sidebarNav = sidebar?.querySelector(".sidebar-nav");
    if (!sidebar || !sidebarNav) return;

    if (!sidebarNav.id) sidebarNav.id = "mobile-sidebar-nav";

    let toggle = document.getElementById("mobile-sidebar-toggle");
    if (!toggle) {
      toggle = document.createElement("button");
      toggle.type = "button";
      toggle.id = "mobile-sidebar-toggle";
      toggle.className = "mobile-sidebar-toggle hidden";
      toggle.setAttribute("aria-controls", sidebarNav.id);
      toggle.setAttribute("aria-expanded", "false");
      toggle.setAttribute("aria-label", "Open terminal navigation");
      toggle.innerHTML = `${icon("menu-scale")}<span>Nav</span>`;
      document.body.appendChild(toggle);
    }

    let backdrop = document.getElementById("mobile-sidebar-backdrop");
    if (!backdrop) {
      backdrop = document.createElement("button");
      backdrop.type = "button";
      backdrop.id = "mobile-sidebar-backdrop";
      backdrop.className = "mobile-sidebar-backdrop hidden";
      backdrop.setAttribute("aria-label", "Close terminal navigation");
      document.body.appendChild(backdrop);
    }

    let focusReturnNode = null;
    const close = ({ restoreFocus = true } = {}) => {
      sidebar.classList.remove("mobile-sidebar-open");
      backdrop?.classList.add("hidden");
      document.body.classList.remove("mobile-sidebar-lock");
      toggle?.setAttribute("aria-expanded", "false");
      if (restoreFocus) {
        const target = focusReturnNode instanceof HTMLElement ? focusReturnNode : toggle;
        target?.focus?.();
      }
    };

    const open = () => {
      if (window.innerWidth > 980) return;
      focusReturnNode = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      sidebar.classList.add("mobile-sidebar-open");
      backdrop?.classList.remove("hidden");
      document.body.classList.add("mobile-sidebar-lock");
      toggle?.setAttribute("aria-expanded", "true");
      const firstLink = sidebarNav.querySelector("a.sidebar-link, button.sidebar-link");
      firstLink?.focus?.();
    };

    const syncVisibility = () => {
      const visible = window.innerWidth <= 980;
      toggle?.classList.toggle("hidden", !visible);
      if (!visible) close({ restoreFocus: false });
    };

    if (!toggle.__quanturaSidebarDrawerBound) {
      toggle.__quanturaSidebarDrawerBound = true;
      toggle.addEventListener("click", () => {
        if (sidebar.classList.contains("mobile-sidebar-open")) close();
        else open();
      });
    }

    if (!backdrop.__quanturaSidebarDrawerBound) {
      backdrop.__quanturaSidebarDrawerBound = true;
      backdrop.addEventListener("click", () => close());
    }

    if (!sidebarNav.__quanturaSidebarDrawerBound) {
      sidebarNav.__quanturaSidebarDrawerBound = true;
      sidebarNav.querySelectorAll("a.sidebar-link, button.sidebar-link").forEach((node) => {
        node.addEventListener("click", () => close({ restoreFocus: false }));
      });
    }

    if (!document.body.__quanturaSidebarDrawerKeyBound) {
      document.body.__quanturaSidebarDrawerKeyBound = true;
      document.addEventListener("keydown", (event) => {
        if (event.key !== "Escape") return;
        if (!sidebar.classList.contains("mobile-sidebar-open")) return;
        event.preventDefault();
        close();
      });
    }

    syncVisibility();
    window.addEventListener("resize", syncVisibility);
  };

  const syncStickyOffsets = () => {
    const header = document.querySelector(".header");
    const headerHeight = header ? header.getBoundingClientRect().height : 88;
    document.documentElement.style.setProperty("--header-height", `${Math.round(headerHeight)}px`);

    const gutter = 16;
    const dockTop = Math.round(headerHeight + gutter);
    document.documentElement.style.setProperty("--dock-top", `${dockTop}px`);

    const dock = document.querySelector(".terminal-context");
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

  let metaPixelInitialized = false;
  let trackMetaConversionCallable = null;

  const resolveMetaEventName = (sourceEventName) => {
    const raw = String(sourceEventName || "").trim();
    if (!raw) return "";
    const key = raw.toLowerCase();

    if (key === "page_view") return "PageView";
    if (key.includes("customize")) return "CustomizeProduct";
    if (key.includes("wishlist")) return "AddToWishlist";
    if (key.includes("complete_registration") || key.includes("registration_complete") || key.includes("signup")) return "CompleteRegistration";
    if (key === "search" || key.includes("_search") || key.includes("screener_search")) return "Search";
    if (key.includes("submit_application") || key.includes("application_submitted")) return "SubmitApplication";
    if (key.includes("add_to_cart") || key.includes("cart_add")) return "AddToCart";
    if (key.includes("view_content")) return "ViewContent";
    if (key.includes("schedule")) return "Schedule";
    if (key === "lead" || key.includes("_lead")) return "Lead";
    if (key.includes("contact")) return "Contact";
    if (key.includes("purchase") || key.includes("checkout_completed") || key.includes("payment_confirmed") || key.includes("order_paid")) {
      return "Purchase";
    }
    return raw;
  };

  const createMetaEventId = (name) =>
    `q_${String(name || "event").replace(/[^a-z0-9_]/gi, "_").slice(0, 40)}_${Date.now().toString(36)}_${Math.random()
      .toString(36)
      .slice(2, 10)}`;

  const normalizeMetaEventParams = (params = {}) => {
    if (!params || typeof params !== "object") return {};
    const out = {};
    const entries = Object.entries(params).slice(0, 40);
    for (const [rawKey, rawValue] of entries) {
      const key = String(rawKey || "").trim();
      if (!key) continue;
      if (rawValue === null || rawValue === undefined) continue;
      if (typeof rawValue === "number") {
        if (!Number.isFinite(rawValue)) continue;
        out[key] = rawValue;
        continue;
      }
      if (typeof rawValue === "boolean") {
        out[key] = rawValue;
        continue;
      }
      if (typeof rawValue === "string") {
        const value = rawValue.trim();
        if (!value) continue;
        out[key] = value.slice(0, 512);
      }
    }
    return out;
  };

  const ensureMetaPixelLoaded = () => {
    if (state.cookieConsent !== "accepted") return false;
    if (!META_PIXEL_ID || typeof window === "undefined" || typeof document === "undefined") return false;
    try {
      if (!window.fbq) {
        (function (f, b, e, v, n, t, s) {
          if (f.fbq) return;
          n = f.fbq = function () {
            n.callMethod ? n.callMethod.apply(n, arguments) : n.queue.push(arguments);
          };
          if (!f._fbq) f._fbq = n;
          n.push = n;
          n.loaded = true;
          n.version = "2.0";
          n.queue = [];
          t = b.createElement(e);
          t.async = true;
          t.src = v;
          s = b.getElementsByTagName(e)[0];
          s.parentNode.insertBefore(t, s);
        })(window, document, "script", "https://connect.facebook.net/en_US/fbevents.js");
      }
      if (!metaPixelInitialized) {
        window.fbq("init", META_PIXEL_ID);
        metaPixelInitialized = true;
      }
      return typeof window.fbq === "function";
    } catch (error) {
      return false;
    }
  };

  const emitMetaPixelEvent = (name, params = {}) => {
    const sourceEventName = String(name || "").trim();
    if (!sourceEventName) return null;
    if (!ensureMetaPixelLoaded()) return null;

    const metaParams = normalizeMetaEventParams(params);
    const eventId = createMetaEventId(sourceEventName);
    const eventName = resolveMetaEventName(sourceEventName);

    try {
      if (META_STANDARD_EVENTS.has(eventName)) {
        window.fbq("track", eventName, metaParams, { eventID: eventId });
      } else {
        window.fbq("trackCustom", eventName, metaParams, { eventID: eventId });
      }
    } catch (error) {
      // Ignore Meta Pixel failures.
    }

    return { eventName, sourceEventName, eventId, params: metaParams };
  };

  const forwardMetaConversionEvent = (metaEvent) => {
    if (!metaEvent || state.cookieConsent !== "accepted") return;
    const functionsClient = state.clients?.functions;
    if (!functionsClient || !functionsClient.httpsCallable) return;
    try {
      if (!trackMetaConversionCallable) {
        trackMetaConversionCallable = functionsClient.httpsCallable(META_CAPI_CALLABLE);
      }
      const payload = {
        eventName: metaEvent.eventName,
        sourceEventName: metaEvent.sourceEventName,
        eventId: metaEvent.eventId,
        eventSourceUrl: window.location.href,
        actionSource: "website",
        params: metaEvent.params || {},
        email: String(state.user?.email || ""),
        fbp: readCookie("_fbp"),
        fbc: readCookie("_fbc"),
        userAgent: navigator.userAgent || "",
      };
      trackMetaConversionCallable(payload).catch(() => {
        // Ignore Meta CAPI forwarding errors.
      });
    } catch (error) {
      // Ignore callable setup errors.
    }
  };

  const logEvent = (name, params = {}) => {
    const analytics = getAnalytics();
    if (analytics) {
      try {
        analytics.logEvent(name, params);
      } catch (error) {
        // Ignore analytics errors.
      }
    }
    const metaEvent = emitMetaPixelEvent(name, params);
    forwardMetaConversionEvent(metaEvent);
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

	  const getRemoteConfigClientCompat = () => {
	    try {
	      if (typeof firebase === "undefined") return null;
	      if (!firebase.remoteConfig) return null;
	      const rc = firebase.remoteConfig();
	      const host = (typeof window !== "undefined" && window.location && window.location.hostname) ? window.location.hostname : "";
	      const isDev = host === "localhost" || host === "127.0.0.1" || host.endsWith(".localhost");
	      const minFetchIntervalMillis = isDev ? 0 : 60 * 60 * 1000;
	      if (rc.settings) {
	        rc.settings.minimumFetchIntervalMillis = minFetchIntervalMillis;
	      } else {
	        rc.settings = { minimumFetchIntervalMillis };
	      }
	          rc.defaultConfig = {
	            welcome_message: "",
	            watchlist_enabled: true,
	            forecast_prophet_enabled: true,
	            forecast_canvas_enabled: true,
              enable_social_leaderboard: true,
              forecast_model_primary: "Quantura Horizon",
              promo_banner_text: "",
              maintenance_mode: false,
              volatility_threshold: "0.05",
              llm_allowed_models: DEFAULT_LLM_ALLOWED_MODELS.join(","),
              ai_usage_tiers: JSON.stringify(AI_USAGE_TIER_DEFAULTS),
	            push_notifications_enabled: true,
	            webpush_vapid_key: "",
	            stripe_checkout_enabled: true,
	            stripe_public_key: "",
              native_ios_storekit_checkout_only: true,
              native_android_play_billing_enabled: true,
              native_iap_product_ids: JSON.stringify(DEFAULT_NATIVE_IAP_PRODUCT_IDS),
              ads_use_real_ios: true,
              ads_use_real_android: true,
	          };
	      return rc;
	    } catch (error) {
	      return null;
	    }
	  };

    const getSsrInitialFetchResponse = () => {
      try {
        if (typeof window === "undefined") return null;
        return window.__QUANTURA_RC_INITIAL_FETCH_RESPONSE__ || null;
      } catch (error) {
        return null;
      }
    };

    const getSsrTemplateId = () => {
      try {
        if (typeof window === "undefined") return "";
        return String(window.__QUANTURA_RC_TEMPLATE_ID__ || "").trim();
      } catch (error) {
        return "";
      }
    };

    let modularRemoteConfigPromise = null;
    const getRemoteConfigClient = async () => {
      const ssrFetchResponse = getSsrInitialFetchResponse();
      const ssrTemplateId = getSsrTemplateId() || "firebase-server";
      if (!ssrFetchResponse) return getRemoteConfigClientCompat();

      if (!modularRemoteConfigPromise) {
        modularRemoteConfigPromise = (async () => {
          const version = "12.9.0";
          const appUrl = `https://www.gstatic.com/firebasejs/${version}/firebase-app.js`;
          const rcUrl = `https://www.gstatic.com/firebasejs/${version}/firebase-remote-config.js`;

          const [{ initializeApp, getApps }, rcLib] = await Promise.all([import(appUrl), import(rcUrl)]);

          const resolveFirebaseConfig = async () => {
            try {
              if (typeof firebase !== "undefined" && typeof firebase.app === "function") {
                const options = firebase.app().options;
                if (options && typeof options === "object") return options;
              }
            } catch (error) {
              // Ignore.
            }
            try {
              const resp = await fetch("/__/firebase/init.json");
              if (resp.ok) return await resp.json();
            } catch (error) {
              // Ignore.
            }
            return null;
          };

          const firebaseConfig = await resolveFirebaseConfig();
          if (!firebaseConfig) throw new Error("Unable to resolve Firebase config for Remote Config.");

          const appName = "quantura-rc";
          const existing = typeof getApps === "function" ? getApps().find((app) => app.name === appName) : null;
          const app = existing || initializeApp(firebaseConfig, appName);

          const rc = rcLib.getRemoteConfig(app, {
            templateId: ssrTemplateId,
            initialFetchResponse: ssrFetchResponse,
          });

          const host = (typeof window !== "undefined" && window.location && window.location.hostname) ? window.location.hostname : "";
          const isDev = host === "localhost" || host === "127.0.0.1" || host.endsWith(".localhost");
          const minFetchIntervalMillis = isDev ? 0 : 60 * 60 * 1000;
          rc.settings.minimumFetchIntervalMillis = minFetchIntervalMillis;
          rc.defaultConfig = {
            welcome_message: "",
            watchlist_enabled: true,
            forecast_prophet_enabled: true,
            forecast_canvas_enabled: true,
            enable_social_leaderboard: true,
            forecast_model_primary: "Quantura Horizon",
            promo_banner_text: "",
            maintenance_mode: false,
            volatility_threshold: "0.05",
            llm_allowed_models: DEFAULT_LLM_ALLOWED_MODELS.join(","),
            ai_usage_tiers: JSON.stringify(AI_USAGE_TIER_DEFAULTS),
            push_notifications_enabled: true,
            webpush_vapid_key: "",
            stripe_checkout_enabled: true,
            stripe_public_key: "",
            native_ios_storekit_checkout_only: true,
            native_android_play_billing_enabled: true,
            native_iap_product_ids: JSON.stringify(DEFAULT_NATIVE_IAP_PRODUCT_IDS),
            ads_use_real_ios: true,
            ads_use_real_android: true,
          };

          const wrap = {
            __ssrHydrated: true,
            fetchAndActivate: () => rcLib.fetchAndActivate(rc),
            activate: () => rcLib.activate(rc),
            getBoolean: (key) => rcLib.getBoolean(rc, key),
            getString: (key) => rcLib.getString(rc, key),
            onConfigUpdate: (handlers) =>
              typeof rcLib.onConfigUpdate === "function" ? rcLib.onConfigUpdate(rc, handlers) : null,
          };
          return wrap;
        })();
      }

      try {
        return await modularRemoteConfigPromise;
      } catch (error) {
        // Fall back to compat Remote Config if the SSR hydration path fails.
        return getRemoteConfigClientCompat();
      }
    };

	  const readRemoteConfigFlags = (rc) => {
	    const getBool = (key, fallback) => {
	      try {
	        if (typeof rc.getBoolean === "function") return Boolean(rc.getBoolean(key));
	        const raw = typeof rc.getString === "function" ? rc.getString(key) : "";
	        if (raw === "") return fallback;
	        return String(raw).trim().toLowerCase() === "true";
	      } catch (error) {
	        return fallback;
	      }
	    };
	    const getString = (key, fallback) => {
	      try {
	        if (typeof rc.getString === "function") return String(rc.getString(key) || fallback);
	        return fallback;
	      } catch (error) {
	        return fallback;
	      }
	    };
      const getInt = (key, fallback) => {
        const raw = String(getString(key, "") || "").trim();
        if (!raw) return fallback;
        const parsed = Number.parseInt(raw, 10);
        return Number.isFinite(parsed) ? parsed : fallback;
      };
      const getFloat = (key, fallback) => {
        const raw = String(getString(key, "") || "").trim();
        if (!raw) return fallback;
        const parsed = Number(raw);
        return Number.isFinite(parsed) ? parsed : fallback;
      };
      const getJson = (key, fallback) => {
        const raw = String(getString(key, "") || "").trim();
        if (!raw) return fallback;
        try {
          const parsed = JSON.parse(raw);
          return parsed && typeof parsed === "object" ? parsed : fallback;
        } catch (error) {
          return fallback;
        }
      };
      const getModelList = (key, fallback) => {
        const raw = String(getString(key, "") || "").trim();
        if (!raw) return fallback;
        let values = [];
        try {
          const parsed = JSON.parse(raw);
          if (Array.isArray(parsed)) values = parsed;
        } catch (error) {
          values = raw.split(",");
        }
        const normalized = values
          .map((value) => String(value || "").trim().toLowerCase())
          .filter((value) => value && (value.startsWith("gpt-5") || value.startsWith("amazon.nova")));
        return normalized.length ? normalized : fallback;
      };
		    return {
          welcomeMessage: getString("welcome_message", ""),
		      watchlistEnabled: getBool("watchlist_enabled", true),
		      forecastProphetEnabled: getBool("forecast_prophet_enabled", true),
		      forecastCanvasEnabled: getBool("forecast_canvas_enabled", true),
          enableSocialLeaderboard: getBool("enable_social_leaderboard", true),
          forecastModelPrimary: getString("forecast_model_primary", "Quantura Horizon"),
          promoBannerText: getString("promo_banner_text", ""),
          maintenanceMode: getBool("maintenance_mode", false),
		      pushEnabled: getBool("push_notifications_enabled", true),
		      webPushVapidKey: getString("webpush_vapid_key", ""),
          volatilityThreshold: getFloat("volatility_threshold", DEFAULT_VOLATILITY_THRESHOLD),
          llmAllowedModels: getModelList("llm_allowed_models", DEFAULT_LLM_ALLOWED_MODELS),
          aiUsageTiers: getJson("ai_usage_tiers", AI_USAGE_TIER_DEFAULTS),
	        stripeCheckoutEnabled: getBool("stripe_checkout_enabled", true),
	        stripePublicKey: getString("stripe_public_key", ""),
          nativeIosStoreKitCheckoutOnly: getBool("native_ios_storekit_checkout_only", true),
          nativeAndroidPlayBillingEnabled: getBool("native_android_play_billing_enabled", true),
          nativeIapProductIds: getJson("native_iap_product_ids", DEFAULT_NATIVE_IAP_PRODUCT_IDS),
          adsUseRealIOS: getBool("ads_use_real_ios", true),
          adsUseRealAndroid: getBool("ads_use_real_android", true),
		    };
		  };

  const applyRemoteFlags = (flags) => {
      updateWelcomeMessageBanner();
	    document.querySelectorAll('[data-panel-target="watchlist"]').forEach((el) => {
	      el.classList.toggle("hidden", !flags.watchlistEnabled);
	    });
	    document.querySelectorAll('[data-panel="watchlist"]').forEach((el) => {
	      if (!flags.watchlistEnabled) el.classList.add("hidden");
	    });
      document.querySelectorAll('[data-panel-target="notifications"]').forEach((el) => {
        el.classList.toggle("hidden", !flags.pushEnabled);
      });
      document.querySelectorAll('[data-panel="notifications"]').forEach((el) => {
        if (!flags.pushEnabled) el.classList.add("hidden");
      });

	    if (!flags.pushEnabled) {
	      setNotificationStatus("Notifications are temporarily disabled.");
	      setNotificationControlsEnabled(false);
	    }

      const leaderboardCard = document.getElementById("ai-agent-leaderboard")?.closest(".card");
      if (leaderboardCard) leaderboardCard.classList.toggle("hidden", !flags.enableSocialLeaderboard);

      updateMaintenanceModeUi(Boolean(flags.maintenanceMode));
      updateDynamicPromoBanner(String(flags.promoBannerText || "").trim());
      renderServerPromoBanner();
      ensureHeaderNotificationsCta();
      const headerNotificationsLink = document.getElementById("header-notifications");
      if (headerNotificationsLink) headerNotificationsLink.classList.toggle("hidden", !flags.pushEnabled);
      refreshScreenerModelUi();
      refreshScreenerCreditsUi();
      hydrateFundamentalFilterFields();
      bindScreenerFilterTabs();
      bindAIAgentLeaderboardControls();
      // Ensure existing alerts inherit the configured default threshold when no explicit value is set.
      if (Number.isFinite(Number(flags.volatilityThreshold))) {
        state.remoteFlags.volatilityThreshold = Math.max(0.01, Math.min(0.5, Number(flags.volatilityThreshold)));
      }

      remoteConfigStore.publish(flags);
	  };

    const updateWelcomeMessageBanner = () => {
      const existing = document.getElementById("welcome-message-banner");
      existing?.remove();
    };

    const persistNotificationPrivacyCache = () => {
      try {
        localStorage.setItem(
          NOTIFICATION_PRIVACY_CACHE_KEY,
          JSON.stringify({
            locationConsent: Boolean(state.notificationPrivacy?.locationConsent),
            ipRegionConsent: Boolean(state.notificationPrivacy?.ipRegionConsent),
            coarseLocation: state.notificationPrivacy?.coarseLocation || null,
            ipRegion: String(state.notificationPrivacy?.ipRegion || "").trim(),
            timezone: String(state.notificationPrivacy?.timezone || "").trim(),
            lastUpdatedMs: Number(state.notificationPrivacy?.lastUpdatedMs || Date.now()),
          })
        );
      } catch (error) {
        // Ignore storage issues.
      }
    };

    const getStoredNumber = (key, fallback = 0) => {
      const raw = String(safeLocalStorageGet(key) || "").trim();
      if (!raw) return fallback;
      const parsed = Number(raw);
      return Number.isFinite(parsed) ? parsed : fallback;
    };

    const setStoredNumber = (key, value) => {
      safeLocalStorageSet(key, String(Math.max(0, Math.floor(Number(value) || 0))));
    };

    const recordPromoSessionUsage = () => {
      if (state.promoSessionTouched) return;
      state.promoSessionTouched = true;
      const now = Date.now();
      const last = getStoredNumber(PROMO_LAST_SESSION_KEY, 0);
      let sessions = getStoredNumber(PROMO_SESSION_COUNT_KEY, 0);
      if (now - last >= 25 * 60 * 1000) {
        sessions += 1;
      }
      state.promoSessionCount = Math.max(1, sessions || 1);
      setStoredNumber(PROMO_SESSION_COUNT_KEY, state.promoSessionCount);
      setStoredNumber(PROMO_LAST_SESSION_KEY, now);
    };

    const recordPromoForecastUsage = () => {
      const current = getStoredNumber(PROMO_FORECAST_COUNT_KEY, state.promoForecastCount || 0) + 1;
      state.promoForecastCount = current;
      setStoredNumber(PROMO_FORECAST_COUNT_KEY, current);
      maybeShowPromoModal();
    };

    const isPromoEligibleViewer = () => {
      const user = state.user;
      if (!user || user.isAnonymous) return true;
      if (isAdminUser(user)) return false;
      if (state.userHasPaidPlan || String(state.userSubscriptionTier || "").toLowerCase() !== "free") return false;
      return true;
    };

    const clearPromoTimer = () => {
      if (state.promoTimer) {
        window.clearInterval(state.promoTimer);
        state.promoTimer = null;
      }
    };

    const promoNowMs = () => Date.now() + Number(state.promoClockOffsetMs || 0);

    const formatPromoCountdown = (remainingMs) => {
      const safe = Math.max(0, Math.floor(remainingMs / 1000));
      const days = Math.floor(safe / 86400);
      const hours = Math.floor((safe % 86400) / 3600);
      const mins = Math.floor((safe % 3600) / 60);
      if (days > 0) return `${days}d ${hours}h ${mins}m`;
      return `${hours}h ${mins}m`;
    };

    const dismissPromoBanner = () => {
      safeLocalStorageSet(PROMO_BANNER_DISMISSED_KEY, "1");
      const node = document.getElementById("server-promo-banner");
      node?.remove();
      logEvent("promo_banner_dismissed", {});
    };

    const renderServerPromoBanner = () => {
      const promo = state.promoStatus;
      const existing = document.getElementById("server-promo-banner");
      if (!promo?.active || !isPromoEligibleViewer() || String(safeLocalStorageGet(PROMO_BANNER_DISMISSED_KEY) || "") === "1") {
        existing?.remove();
        clearPromoTimer();
        return;
      }
      const remaining = Number(promo.endsAtMs || 0) - promoNowMs();
      if (!Number.isFinite(remaining) || remaining <= 0) {
        existing?.remove();
        clearPromoTimer();
        return;
      }
      const badge = `${Number(promo.discountPercent || 50)}% off`;
      const headline = String(promo.headline || "Limited-time promotional offer");
      const details = String(promo.body || `Use code ${promo.code || "QUANTURA50"} on checkout.`);
      const code = String(promo.code || "QUANTURA50").toUpperCase();
      const countdown = formatPromoCountdown(remaining);
      const html = `
        <div class="promo-inner">
          <div>
            <div class="promo-badge">${escapeHtml(badge)}</div>
            <div class="promo-title">${escapeHtml(headline)}</div>
            <div class="small muted">${escapeHtml(details)}</div>
            <div class="small" style="margin-top:6px;">
              Code <strong>${escapeHtml(code)}</strong> · Ends in
              <span id="promo-countdown">${escapeHtml(countdown)}</span>
            </div>
          </div>
          <div class="promo-actions">
            <a class="cta small" href="/pricing" data-action="promo-view-plans">Claim offer</a>
            <button class="cta secondary small" type="button" data-action="promo-dismiss-banner">Dismiss</button>
          </div>
        </div>
      `;
      if (existing) {
        existing.innerHTML = html;
      } else {
        const banner = document.createElement("section");
        banner.id = "server-promo-banner";
        banner.className = "promo-banner";
        banner.innerHTML = html;
        const header = document.querySelector("header.header");
        if (header && typeof header.insertAdjacentElement === "function") header.insertAdjacentElement("afterend", banner);
        else document.body.prepend(banner);
      }
      const bannerNode = document.getElementById("server-promo-banner");
      bannerNode?.querySelector('[data-action="promo-dismiss-banner"]')?.addEventListener("click", dismissPromoBanner);
      bannerNode?.querySelector('[data-action="promo-view-plans"]')?.addEventListener("click", () => {
        logEvent("promo_banner_clicked", { code });
      });
      clearPromoTimer();
      state.promoTimer = window.setInterval(() => {
        const current = document.getElementById("promo-countdown");
        if (!current) {
          clearPromoTimer();
          return;
        }
        const ms = Number(state.promoStatus?.endsAtMs || 0) - promoNowMs();
        if (ms <= 0) {
          current.textContent = "expired";
          clearPromoTimer();
          return;
        }
        current.textContent = formatPromoCountdown(ms);
      }, 1000);
    };

    const closePromoModal = () => {
      const modal = document.getElementById("promo-offer-modal");
      modal?.remove();
    };

    const showPromoModal = () => {
      if (document.getElementById("promo-offer-modal")) return;
      const promo = state.promoStatus;
      if (!promo?.active || !isPromoEligibleViewer()) return;

      const modal = document.createElement("div");
      modal.id = "promo-offer-modal";
      modal.className = "modal";
      modal.innerHTML = `
        <div class="modal-backdrop" data-action="close-promo-modal"></div>
        <div class="modal-card card" role="dialog" aria-modal="true" aria-label="Quantura promotional offer">
          <h3>Unlock Quantura at ${escapeHtml(String(Number(promo.discountPercent || 50)))}% off</h3>
          <p class="small">
            You have started active usage. Use code <strong>${escapeHtml(String(promo.code || "QUANTURA50").toUpperCase())}</strong>
            for a limited-time discount.
          </p>
          <div class="small muted">Offer timer is synced to server time for consistency across devices.</div>
          <div class="modal-actions">
            <button class="cta secondary" type="button" data-action="close-promo-modal">Not now</button>
            <a class="cta" href="/pricing" data-action="promo-modal-cta">Claim 50% off</a>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
      modal.querySelectorAll('[data-action="close-promo-modal"]').forEach((node) => {
        node.addEventListener("click", () => {
          safeLocalStorageSet(PROMO_MODAL_DISMISSED_KEY, "1");
          closePromoModal();
          logEvent("promo_modal_dismissed", {});
        });
      });
      modal.querySelector('[data-action="promo-modal-cta"]')?.addEventListener("click", () => {
        safeLocalStorageSet(PROMO_MODAL_DISMISSED_KEY, "1");
        logEvent("promo_modal_clicked", { code: String(promo.code || "QUANTURA50").toUpperCase() });
      });
      state.promoModalShown = true;
      logEvent("promo_modal_shown", {
        sessions: state.promoSessionCount,
        forecasts: state.promoForecastCount,
      });
    };

    function maybeShowPromoModal() {
      const promo = state.promoStatus;
      if (!promo?.active) return;
      if (!isPromoEligibleViewer()) return;
      if (String(safeLocalStorageGet(PROMO_MODAL_DISMISSED_KEY) || "") === "1") return;
      if (state.promoModalShown) return;
      const sessionCount = getStoredNumber(PROMO_SESSION_COUNT_KEY, state.promoSessionCount || 0);
      const forecastCount = getStoredNumber(PROMO_FORECAST_COUNT_KEY, state.promoForecastCount || 0);
      state.promoSessionCount = sessionCount;
      state.promoForecastCount = forecastCount;
      if (sessionCount >= 3 || forecastCount >= 2) {
        showPromoModal();
      }
    }

    const loadServerPromoStatus = async () => {
      try {
        const response = await fetch("/api/explore/promo/status", { method: "GET", credentials: "omit" });
        if (!response.ok) return;
        const payload = await response.json().catch(() => ({}));
        const promo = payload?.promo && typeof payload.promo === "object" ? payload.promo : null;
        if (!promo) return;
        const serverTimeMs = Number(payload?.serverTimeMs || promo?.serverTimeMs || Date.now());
        if (Number.isFinite(serverTimeMs)) {
          state.promoClockOffsetMs = serverTimeMs - Date.now();
        }
        state.promoStatus = {
          id: String(promo.id || "promo"),
          active: Boolean(promo.active),
          code: String(promo.code || "QUANTURA50"),
          discountPercent: Number(promo.discountPercent || 50),
          headline: String(promo.headline || "Limited-time promotional offer"),
          body: String(promo.body || ""),
          startsAtMs: Number(promo.startsAtMs || 0) || 0,
          endsAtMs: Number(promo.endsAtMs || 0) || 0,
        };
        renderServerPromoBanner();
        maybeShowPromoModal();
      } catch (error) {
        // Promo UI is optional; keep page resilient.
      }
    };

    const updateDynamicPromoBanner = (text) => {
      const existing = document.getElementById("dynamic-promo-banner");
      const message = String(text || "").trim();
      if (!message) {
        existing?.remove();
        return;
      }
      if (existing) {
        const node = existing.querySelector(".promo-title");
        if (node) node.textContent = message;
        return;
      }
      const banner = document.createElement("section");
      banner.id = "dynamic-promo-banner";
      banner.className = "promo-banner";
      banner.innerHTML = `
        <div class="promo-inner">
          <div>
            <div class="promo-badge">Announcement</div>
            <div class="promo-title">${escapeHtml(message)}</div>
          </div>
          <div class="promo-actions">
            <a class="cta secondary small" href="/pricing">View plans</a>
          </div>
        </div>
      `;
      const header = document.querySelector("header.header");
      if (header && typeof header.insertAdjacentElement === "function") header.insertAdjacentElement("afterend", banner);
      else document.body.prepend(banner);
    };

    const updateMaintenanceModeUi = (enabled) => {
      const existing = document.getElementById("maintenance-mode-gate");
      if (!enabled) {
        existing?.remove();
        return;
      }
      if (existing) return;

      const gate = document.createElement("div");
      gate.id = "maintenance-mode-gate";
      gate.className = "modal";
      gate.innerHTML = `
        <div class="modal-backdrop"></div>
        <div class="modal-card card" role="dialog" aria-modal="true" aria-label="Maintenance mode">
          <h3>Maintenance Mode</h3>
          <p class="small">
            Quantura is temporarily locked for infrastructure updates. Forecasting, screening, and write actions are paused.
          </p>
          <div class="modal-actions">
            <a class="cta secondary" href="/contact">Contact support</a>
            <a class="cta" href="/pricing">View plans</a>
          </div>
        </div>
      `;
      document.body.appendChild(gate);
    };

	  const subscribeRemoteConfigUpdates = (rc) => {
	    if (!rc) return;
	    if (state.remoteConfigUnsubscribe) return;

	    const handler = async (configUpdate) => {
	      let updatedKeys = [];
	      try {
	        if (configUpdate?.getUpdatedKeys) {
	          updatedKeys = Array.from(configUpdate.getUpdatedKeys());
	        } else if (configUpdate?.updatedKeys) {
	          updatedKeys = Array.from(configUpdate.updatedKeys);
	        }
	      } catch (error) {
	        updatedKeys = [];
	      }

	      try {
	        if (typeof rc.activate === "function") {
	          await rc.activate();
	        }
	      } catch (error) {
	        // Ignore activate errors.
	      }

	      const nextFlags = readRemoteConfigFlags(rc);
	      state.remoteFlags = { ...state.remoteFlags, ...nextFlags };
	      applyRemoteFlags(state.remoteFlags);
	      logEvent("remote_config_updated", { updated_keys: updatedKeys.slice(0, 25).join(",") });
	    };

	    const onUpdate =
	      typeof rc.onConfigUpdated === "function"
	        ? rc.onConfigUpdated.bind(rc)
	        : typeof rc.onConfigUpdate === "function"
	          ? rc.onConfigUpdate.bind(rc)
	          : null;

	    if (!onUpdate) return;
	    try {
	      const unsub = onUpdate({
	        next: handler,
	        error: (err) => {
	          logEvent("remote_config_update_error", { message: String(err?.message || err || "") });
	        },
	        complete: () => {
	          // Ignore.
	        },
	      });
	      if (typeof unsub === "function") state.remoteConfigUnsubscribe = unsub;
	    } catch (error) {
	      // Ignore.
	    }
	  };

	  const startRemoteConfigRefreshLoop = (rc) => {
	    if (!rc) return;
	    if (state.remoteConfigRefreshTimer) return;
	    const refresh = async () => {
	      if (document.visibilityState && document.visibilityState !== "visible") return;
	      try {
	        await rc.fetchAndActivate();
	        const nextFlags = readRemoteConfigFlags(rc);
	        state.remoteFlags = { ...state.remoteFlags, ...nextFlags };
	        applyRemoteFlags(state.remoteFlags);
	      } catch (error) {
	        // Ignore.
	      }
	    };
	    state.remoteConfigRefreshTimer = window.setInterval(refresh, 15 * 60 * 1000);
	    document.addEventListener("visibilitychange", () => {
	      if (document.visibilityState === "visible") refresh();
	    });
	  };

	  const loadRemoteConfig = async () => {
	    const rc = await getRemoteConfigClient();
	    if (!rc) return state.remoteFlags;

      if (!rc.__ssrHydrated) {
	      try {
	        await rc.fetchAndActivate();
	      } catch (error) {
	        // Ignore fetch errors and fall back to defaults.
	      }
      } else {
        // Server-side Remote Config values are already present on first paint. Refresh later.
        window.setTimeout(() => {
          rc.fetchAndActivate?.().catch?.(() => {});
        }, 5000);
      }
	    const nextFlags = readRemoteConfigFlags(rc);
	    state.remoteFlags = {
	      ...state.remoteFlags,
	      ...nextFlags,
	    };
	    state.remoteConfigLoaded = true;
	    applyRemoteFlags(state.remoteFlags);
      loadServerPromoStatus().catch(() => {});
	    subscribeRemoteConfigUpdates(rc);
	    startRemoteConfigRefreshLoop(rc);
	    logEvent("remote_config_loaded", {
	      watchlist: state.remoteFlags.watchlistEnabled,
	      prophet: state.remoteFlags.forecastProphetEnabled,
	      canvas: state.remoteFlags.forecastCanvasEnabled,
	    });
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

  const normalizeLanguageCode = (raw) => {
    const text = String(raw || "").trim().toLowerCase();
    if (!text || text === "auto") return "auto";
    const base = text.split(/[-_]/)[0];
    return SUPPORTED_LANGUAGES.has(base) ? base : "en";
  };

  const normalizeCountryCode = (raw) => {
    const text = String(raw || "").trim().toUpperCase();
    return /^[A-Z]{2}$/.test(text) ? text : "US";
  };

  const resolveLanguageFromNavigator = () => {
    const options = [
      ...(Array.isArray(navigator.languages) ? navigator.languages : []),
      navigator.language,
      navigator.userLanguage,
    ];
    for (const candidate of options) {
      const normalized = normalizeLanguageCode(candidate);
      if (normalized !== "auto") return normalized;
    }
    return "en";
  };

  const resolveLanguageFromCountry = (country) => {
    const key = normalizeCountryCode(country);
    return normalizeLanguageCode(COUNTRY_DEFAULT_LANGUAGE[key] || "en");
  };

  const buildMeta = () => {
    const privacy = state.notificationPrivacy || {};
    const locationConsent = Boolean(privacy.locationConsent);
    const ipRegionConsent = Boolean(privacy.ipRegionConsent);
    const rawCountry = String(privacy?.coarseLocation?.countryCode || "").trim();
    const country = locationConsent && rawCountry ? normalizeCountryCode(rawCountry) : "";
    const timezone = locationConsent
      ? String(privacy.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || "").trim()
      : "";
    const ipRegion = locationConsent && ipRegionConsent ? String(privacy.ipRegion || "").trim().slice(0, 80) : "";
    return {
      sessionId: getSessionId(),
      pagePath: window.location.pathname,
      pageTitle: document.title,
      referrer: document.referrer || "",
      userAgent: navigator.userAgent,
      language: state.preferredLanguage || normalizeLanguageCode(navigator.language),
      country,
      timezone,
      ipRegion,
      locationConsent,
      ipRegionConsent,
      screen: `${window.screen.width}x${window.screen.height}`,
      platform: navigator.platform,
      runtime: resolveRuntimeLabel(),
      nativePlatform: getNativePlatform() || "",
      nativeApp: isNativeApp(),
      installedPwa: isInstalledPwa(),
    };
  };

  const exchangeNativeIdTokenForCustomToken = async (idToken) => {
    const token = String(idToken || "").trim();
    if (!token) throw new Error("Native ID token is missing.");
    const response = await fetch("/api/auth/exchange", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({}),
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch (error) {
      payload = null;
    }
    if (!response.ok || !payload?.customToken) {
      throw new Error(String(payload?.error || "Unable to sync native auth session."));
    }
    return String(payload.customToken);
  };

  const installNativeAuthBridge = (auth) => {
    if (!isNativeApp() || !auth) return null;
    applyRuntimeBodyClasses();
    const existingBridge = window.__quanturaAuthBridge && typeof window.__quanturaAuthBridge === "object"
      ? window.__quanturaAuthBridge
      : {};

    if (existingBridge.__nativeInstalled === true) {
      return existingBridge;
    }

    const bridge = existingBridge;
    bridge.receiveCustomToken = async (token) => {
      const cleanToken = String(token || "").trim();
      if (!cleanToken) return false;
      try {
        await auth.signInWithCustomToken(cleanToken);
        window.dispatchEvent(new CustomEvent("quantura:native-custom-token-consumed", { detail: { ok: true } }));
        return true;
      } catch (error) {
        window.dispatchEvent(new CustomEvent("quantura:native-custom-token-consumed", { detail: { ok: false, error: error?.message || "custom token failed" } }));
        throw error;
      }
    };

    bridge.onNativeAuthState = (authState) => {
      const nextState = authState && typeof authState === "object" ? authState : {};
      state.nativeAuthState = nextState;
      applyRuntimeBodyClasses();
      window.dispatchEvent(new CustomEvent("quantura:native-auth-state-bridge", { detail: nextState }));
      return nextState;
    };

    bridge.requestSignIn = (provider = "") =>
      sendNativeAuthMessage({
        type: "REQUEST_SIGN_IN",
        provider: String(provider || "").trim().toLowerCase(),
      });

    bridge.requestAuthState = () => sendNativeAuthMessage({ type: "GET_AUTH_STATE" });
    bridge.signOut = () => sendNativeAuthMessage({ type: "SIGN_OUT" });
    bridge.__nativeInstalled = true;

    window.__quanturaAuthBridge = bridge;

    try {
      if (window.__QUANTURA_PENDING_AUTH_STATE__) {
        bridge.onNativeAuthState(window.__QUANTURA_PENDING_AUTH_STATE__);
      }
    } catch (error) {
      // Ignore stale pending state errors.
    }
    try {
      const pendingToken = String(window.__QUANTURA_PENDING_CUSTOM_TOKEN__ || "").trim();
      if (pendingToken) {
        bridge.receiveCustomToken(pendingToken).catch(() => {});
      }
    } catch (error) {
      // Ignore stale pending token errors.
    }

    bridge.requestAuthState();
    return bridge;
  };

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

  const isPushChannelAvailable = () => isPushSupported() || isNativeApp();

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

  const refreshNotificationPrivacyRefs = () => {
    ui.notificationsPrivacyContainer = document.getElementById("notifications-privacy-controls");
    ui.notificationsLocationOptIn = document.getElementById("notifications-location-optin");
    ui.notificationsIpOptIn = document.getElementById("notifications-ip-optin");
    ui.notificationsRequestLocation = document.getElementById("notifications-request-location");
    ui.notificationsPrivacyStatus = document.getElementById("notifications-privacy-status");
  };

  const setNotificationPrivacyStatus = (text, isError = false) => {
    if (!ui.notificationsPrivacyStatus) return;
    ui.notificationsPrivacyStatus.textContent = String(text || "");
    ui.notificationsPrivacyStatus.style.color = isError ? "#d83446" : "";
  };

  const syncNotificationPrivacyControls = () => {
    refreshNotificationPrivacyRefs();
    if (!ui.notificationsPrivacyContainer) return;
    const privacy = state.notificationPrivacy || {};
    const locationConsent = Boolean(privacy.locationConsent);
    const ipRegionConsent = Boolean(privacy.ipRegionConsent);
    if (ui.notificationsLocationOptIn) ui.notificationsLocationOptIn.checked = locationConsent;
    if (ui.notificationsIpOptIn) {
      ui.notificationsIpOptIn.checked = locationConsent && ipRegionConsent;
      ui.notificationsIpOptIn.disabled = !locationConsent;
    }
    if (ui.notificationsRequestLocation) ui.notificationsRequestLocation.disabled = !locationConsent;
    if (!locationConsent) {
      setNotificationPrivacyStatus("Location consent is off. Notification text stays generic.");
      return;
    }
    const timezone = String(privacy.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || "").trim();
    const country = String(privacy?.coarseLocation?.countryCode || "").trim();
    const region = String(privacy.ipRegion || "").trim();
    const summary = [`Consent on`, country ? `country ${country}` : "", region ? `region ${region}` : "", timezone ? timezone : ""]
      .filter(Boolean)
      .join(" · ");
    setNotificationPrivacyStatus(summary);
  };

  const ensureNotificationPrivacyControls = () => {
    if (!ui.notificationsStatus) return;
    refreshNotificationPrivacyRefs();
    if (ui.notificationsPrivacyContainer) {
      syncNotificationPrivacyControls();
      return;
    }
    const anchor = ui.notificationsStatus.parentElement || ui.notificationsStatus;
    const wrap = document.createElement("div");
    wrap.id = "notifications-privacy-controls";
    wrap.className = "notice small";
    wrap.style.marginTop = "12px";
    wrap.innerHTML = `
      <strong>Personalized notifications (optional)</strong>
      <p class="small" style="margin: 6px 0 8px;">
        Location and IP-derived region are sensitive. We only store coarse location, timezone, and region after explicit consent.
      </p>
      <label class="feature" style="align-items:flex-start;"><span></span><input id="notifications-location-optin" type="checkbox" /> <span>Allow coarse location + timezone for notification context</span></label>
      <label class="feature" style="align-items:flex-start;"><span></span><input id="notifications-ip-optin" type="checkbox" /> <span>Allow IP-derived region lookup/storage</span></label>
      <div class="hero-actions" style="margin-top:8px;">
        <button class="cta secondary small" id="notifications-request-location" type="button">Capture coarse location</button>
      </div>
      <p id="notifications-privacy-status" class="small muted" style="margin-top:8px;"></p>
    `;
    anchor.insertAdjacentElement("afterend", wrap);
    refreshNotificationPrivacyRefs();
    syncNotificationPrivacyControls();
  };

  const requestCoarseNotificationLocation = async () => {
    if (!state.notificationPrivacy?.locationConsent) {
      throw new Error("Enable location consent first.");
    }
    if (!navigator.geolocation) {
      throw new Error("Geolocation is not available in this browser.");
    }
    const position = await new Promise((resolve, reject) => {
      navigator.geolocation.getCurrentPosition(resolve, reject, {
        enableHighAccuracy: false,
        timeout: 8000,
        maximumAge: 15 * 60 * 1000,
      });
    }).catch((error) => {
      throw new Error(error?.message || "Location permission was denied.");
    });
    const latitude = Number(position?.coords?.latitude);
    const longitude = Number(position?.coords?.longitude);
    const coarse = {
      lat: Number.isFinite(latitude) ? Number(latitude.toFixed(1)) : null,
      lon: Number.isFinite(longitude) ? Number(longitude.toFixed(1)) : null,
      accuracyM: Number.isFinite(Number(position?.coords?.accuracy)) ? Math.round(Number(position.coords.accuracy)) : null,
      countryCode: state.preferredCountry || "US",
      capturedAt: new Date().toISOString(),
    };
    state.notificationPrivacy.coarseLocation = coarse;
    state.notificationPrivacy.timezone = Intl.DateTimeFormat().resolvedOptions().timeZone || "";
    if (state.notificationPrivacy.ipRegionConsent) {
      const ipContext = await fetchIpLocationContext();
      if (ipContext.countryCode) {
        state.notificationPrivacy.coarseLocation.countryCode = ipContext.countryCode;
        applyCountryPreference(ipContext.countryCode, { persist: true });
      }
      state.notificationPrivacy.ipRegion = ipContext.region || "";
    }
    state.notificationPrivacy.lastUpdatedMs = Date.now();
    persistNotificationPrivacyCache();
    syncNotificationPrivacyControls();
  };

  const saveNotificationPrivacySettings = async () => {
    const locationConsent = Boolean(ui.notificationsLocationOptIn?.checked);
    const ipRegionConsent = locationConsent && Boolean(ui.notificationsIpOptIn?.checked);
    if (!locationConsent) {
      state.notificationPrivacy.coarseLocation = null;
      state.notificationPrivacy.ipRegion = "";
    }
    state.notificationPrivacy.locationConsent = locationConsent;
    state.notificationPrivacy.ipRegionConsent = ipRegionConsent;
    state.notificationPrivacy.timezone = locationConsent ? (Intl.DateTimeFormat().resolvedOptions().timeZone || "") : "";
    state.notificationPrivacy.lastUpdatedMs = Date.now();
    persistNotificationPrivacyCache();
    syncNotificationPrivacyControls();

    if (!hasSessionUser()) return;
    const headers = await buildApiAuthHeaders({ includeJson: true });
    const response = await fetch("/api/notifications/preferences", {
      method: "POST",
      headers,
      body: JSON.stringify({
        locationConsent,
        ipRegionConsent,
        timezone: state.notificationPrivacy.timezone || "",
        coarseLocation: state.notificationPrivacy.coarseLocation || null,
        ipRegion: state.notificationPrivacy.ipRegion || "",
      }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(String(payload?.error || "Unable to save notification privacy settings."));
    }
  };

  const loadNotificationPrivacySettings = async () => {
    if (!hasSessionUser()) {
      syncNotificationPrivacyControls();
      return;
    }
    const headers = await buildApiAuthHeaders({ includeJson: false });
    const response = await fetch("/api/me/notification-settings", {
      method: "GET",
      headers,
    });
    if (!response.ok) return;
    const payload = await response.json().catch(() => ({}));
    const privacy = payload?.notificationPrivacy && typeof payload.notificationPrivacy === "object" ? payload.notificationPrivacy : {};
    state.notificationPrivacy.locationConsent = Boolean(privacy.locationConsent);
    state.notificationPrivacy.ipRegionConsent = Boolean(privacy.ipRegionConsent);
    state.notificationPrivacy.coarseLocation = privacy.coarseLocation && typeof privacy.coarseLocation === "object" ? privacy.coarseLocation : null;
    state.notificationPrivacy.ipRegion = String(privacy.ipRegion || "").trim().slice(0, 80);
    state.notificationPrivacy.timezone = String(privacy.timezone || state.notificationPrivacy.timezone || "").trim().slice(0, 80);
    state.notificationPrivacy.lastUpdatedMs = Number(privacy.updatedAtMs || Date.now()) || Date.now();
    persistNotificationPrivacyCache();
    syncNotificationPrivacyControls();
  };

  function safeLocalStorageGet(key) {
    try {
      return localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  }

  function safeLocalStorageSet(key, value) {
    try {
      localStorage.setItem(key, value);
    } catch (error) {
      // Ignore storage failures.
    }
  }

  function safeLocalStorageRemove(key) {
    try {
      localStorage.removeItem(key);
    } catch (error) {
      // Ignore storage failures.
    }
  }

  let forecastCacheDbPromise = null;

  const computeFastHash = (input) => {
    const text = String(input || "");
    let hash = 2166136261;
    for (let i = 0; i < text.length; i += 1) {
      hash ^= text.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(16).padStart(8, "0");
  };

  const buildForecastParamsHash = ({ ticker, interval, horizon, service, quantiles, start } = {}) => {
    const sortedQuantiles = Array.isArray(quantiles)
      ? quantiles
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value) && value > 0 && value < 1)
          .sort((a, b) => a - b)
      : [];
    const payload = JSON.stringify({
      ticker: String(ticker || "").trim().toUpperCase(),
      interval: String(interval || "1d").trim().toLowerCase(),
      horizon: Number.isFinite(Number(horizon)) ? Number(horizon) : 0,
      service: String(service || "prophet").trim().toLowerCase(),
      quantiles: sortedQuantiles,
      start: String(start || "").trim(),
    });
    return computeFastHash(payload);
  };

  const buildForecastCacheOwnerId = () => {
    const uid = String(state?.user?.uid || "").trim();
    if (uid) return uid;
    return "anon";
  };

  const buildForecastCacheKey = ({ ownerId, ticker, paramsHash } = {}) => {
    const cleanOwner = String(ownerId || "anon").trim() || "anon";
    const cleanTicker = String(normalizeTicker(ticker || "") || "TICKER").trim() || "TICKER";
    const cleanHash = String(paramsHash || "").trim() || computeFastHash(`${cleanOwner}:${cleanTicker}:${Date.now()}`);
    return `${cleanOwner}::${cleanTicker}::${cleanHash}`;
  };

  const readForecastCacheIndex = () => {
    const raw = safeLocalStorageGet(FORECAST_CACHE_INDEX_KEY);
    if (!raw) return {};
    try {
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return {};
      return parsed;
    } catch (error) {
      return {};
    }
  };

  const writeForecastCacheIndex = (index) => {
    try {
      safeLocalStorageSet(FORECAST_CACHE_INDEX_KEY, JSON.stringify(index || {}));
    } catch (error) {
      // Ignore cache index persistence failures.
    }
  };

  const setForecastCacheKeyForRequest = (requestId, cacheKey) => {
    const reqId = String(requestId || "").trim();
    const key = String(cacheKey || "").trim();
    if (!reqId || !key) return;
    const index = readForecastCacheIndex();
    index[reqId] = { cacheKey: key, updatedAtMs: Date.now() };
    const keys = Object.keys(index);
    if (keys.length > 220) {
      keys
        .sort((a, b) => Number(index[a]?.updatedAtMs || 0) - Number(index[b]?.updatedAtMs || 0))
        .slice(0, keys.length - 220)
        .forEach((entryKey) => {
          delete index[entryKey];
        });
    }
    writeForecastCacheIndex(index);
  };

  const getForecastCacheKeyForRequest = (requestId) => {
    const reqId = String(requestId || "").trim();
    if (!reqId) return "";
    const index = readForecastCacheIndex();
    return String(index?.[reqId]?.cacheKey || "").trim();
  };

  const normalizeForecastSeriesRows = (rows) => {
    if (!Array.isArray(rows)) return [];
    return rows
      .map((rawRow) => {
        if (!rawRow || typeof rawRow !== "object") return null;
        const row = rawRow;
        const rawDs = String(row.ds || row.date || row.datetime || "").trim();
        if (!rawDs) return null;
        const normalized = { ds: rawDs };
        Object.entries(row).forEach(([key, value]) => {
          if (!/^q\d{1,3}$/.test(String(key || ""))) return;
          const numeric = Number(value);
          if (!Number.isFinite(numeric)) return;
          normalized[key] = Number(numeric.toFixed(6));
        });
        if (Object.keys(normalized).length <= 1) return null;
        return normalized;
      })
      .filter(Boolean);
  };

  const openForecastCacheDb = () => {
    if (forecastCacheDbPromise) return forecastCacheDbPromise;
    if (typeof window === "undefined" || !window.indexedDB) {
      forecastCacheDbPromise = Promise.resolve(null);
      return forecastCacheDbPromise;
    }
    forecastCacheDbPromise = new Promise((resolve) => {
      try {
        const request = window.indexedDB.open(FORECAST_CACHE_DB_NAME, 1);
        request.onupgradeneeded = () => {
          const dbInstance = request.result;
          if (!dbInstance.objectStoreNames.contains(FORECAST_CACHE_STORE_NAME)) {
            dbInstance.createObjectStore(FORECAST_CACHE_STORE_NAME, { keyPath: "id" });
          }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => resolve(null);
      } catch (error) {
        resolve(null);
      }
    });
    return forecastCacheDbPromise;
  };

  const withForecastCacheStore = async (mode, callback) => {
    const dbInstance = await openForecastCacheDb();
    if (!dbInstance) return null;
    return new Promise((resolve) => {
      try {
        const tx = dbInstance.transaction(FORECAST_CACHE_STORE_NAME, mode);
        const store = tx.objectStore(FORECAST_CACHE_STORE_NAME);
        const request = callback(store);
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => resolve(null);
      } catch (error) {
        resolve(null);
      }
    });
  };

  const putForecastCacheEntry = async (entry) => {
    const payload = entry && typeof entry === "object" ? { ...entry } : null;
    const id = String(payload?.id || "").trim();
    if (!payload || !id) return false;
    payload.id = id;
    payload.updatedAtMs = Number(payload.updatedAtMs || Date.now()) || Date.now();
    payload.createdAtMs = Number(payload.createdAtMs || payload.updatedAtMs) || payload.updatedAtMs;
    let persisted = false;
    const dbResult = await withForecastCacheStore("readwrite", (store) => store.put(payload));
    if (dbResult) persisted = true;
    if (!persisted) {
      safeLocalStorageSet(`${FORECAST_CACHE_LOCAL_PREFIX}${id}`, JSON.stringify(payload));
      persisted = true;
    }
    return persisted;
  };

  const getForecastCacheEntryByKey = async (cacheKey) => {
    const id = String(cacheKey || "").trim();
    if (!id) return null;
    const dbValue = await withForecastCacheStore("readonly", (store) => store.get(id));
    if (dbValue && typeof dbValue === "object") return dbValue;
    const raw = safeLocalStorageGet(`${FORECAST_CACHE_LOCAL_PREFIX}${id}`);
    if (!raw) return null;
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch (error) {
      return null;
    }
  };

  const saveForecastSeriesToClientCache = async ({
    requestId = "",
    ticker = "",
    interval = "1d",
    horizon = 0,
    service = "prophet",
    quantiles = [],
    start = "",
    forecastRows = [],
    historicalRows = [],
    chartConfig = {},
    metrics = {},
  } = {}) => {
    const normalizedRows = normalizeForecastSeriesRows(forecastRows);
    if (!normalizedRows.length) return null;
    const ownerId = buildForecastCacheOwnerId();
    const paramsHash = buildForecastParamsHash({ ticker, interval, horizon, service, quantiles, start });
    const cacheKey = buildForecastCacheKey({
      ownerId,
      ticker,
      paramsHash,
    });
    const entry = {
      id: cacheKey,
      requestId: String(requestId || "").trim(),
      ownerId,
      ticker: String(normalizeTicker(ticker || "") || "").trim(),
      interval: String(interval || "1d").trim().toLowerCase(),
      horizon: Number(horizon) || normalizedRows.length,
      service: String(service || "prophet").trim().toLowerCase(),
      quantiles: Array.isArray(quantiles) ? quantiles : [],
      start: String(start || "").trim(),
      paramsHash,
      forecastRows: normalizedRows,
      historicalRows: Array.isArray(historicalRows) ? historicalRows.slice(-1200) : [],
      chartConfig: chartConfig && typeof chartConfig === "object" ? chartConfig : {},
      metrics: metrics && typeof metrics === "object" ? metrics : {},
      createdAtMs: Date.now(),
      updatedAtMs: Date.now(),
    };
    await putForecastCacheEntry(entry);
    if (entry.requestId) setForecastCacheKeyForRequest(entry.requestId, cacheKey);
    return {
      cacheKey,
      paramsHash,
      rowCount: normalizedRows.length,
    };
  };

  const loadForecastSeriesFromClientCache = async ({
    requestId = "",
    ticker = "",
    interval = "1d",
    horizon = 0,
    service = "prophet",
    quantiles = [],
    start = "",
  } = {}) => {
    const directCacheKey = getForecastCacheKeyForRequest(requestId);
    if (directCacheKey) {
      const byRequest = await getForecastCacheEntryByKey(directCacheKey);
      if (byRequest) return byRequest;
    }
    const paramsHash = buildForecastParamsHash({ ticker, interval, horizon, service, quantiles, start });
    const ownerId = buildForecastCacheOwnerId();
    const derivedCacheKey = buildForecastCacheKey({ ownerId, ticker, paramsHash });
    const byParams = await getForecastCacheEntryByKey(derivedCacheKey);
    if (byParams) {
      const reqId = String(requestId || byParams.requestId || "").trim();
      if (reqId) setForecastCacheKeyForRequest(reqId, derivedCacheKey);
      return byParams;
    }
    return null;
  };

  const i18nTextDefaults = new WeakMap();
  const i18nAttrDefaults = new WeakMap();

  const setLocalizedText = (element, text) => {
    if (!element) return;
    if (!i18nTextDefaults.has(element)) {
      i18nTextDefaults.set(element, element.textContent || "");
    }
    if (typeof text === "string" && text.length) {
      element.textContent = text;
      return;
    }
    element.textContent = i18nTextDefaults.get(element) || "";
  };

  const setLocalizedAttribute = (element, attribute, text) => {
    if (!element || !attribute) return;
    let attrs = i18nAttrDefaults.get(element);
    if (!attrs) {
      attrs = {};
      i18nAttrDefaults.set(element, attrs);
    }
    if (!(attribute in attrs)) {
      attrs[attribute] = element.getAttribute(attribute) || "";
    }
    if (typeof text === "string" && text.length) {
      element.setAttribute(attribute, text);
      return;
    }
    element.setAttribute(attribute, attrs[attribute] || "");
  };

  const getUiLanguagePack = (languageCode) => {
    const normalized = normalizeLanguageCode(languageCode);
    const resolved = normalized === "auto" ? resolveLanguageFromNavigator() : normalized;
    return UI_I18N_TEXT[resolved] || UI_I18N_TEXT.en;
  };

  const applyUiTranslations = (languageCode) => {
    const pack = getUiLanguagePack(languageCode);
    const fallback = UI_I18N_TEXT.en;
    const accountAuthed = hasFullAccount();
    const sessionAuthed = Boolean(state.user);
    const guestSession = isAnonymousUser();

    Object.entries(UI_I18N_SELECTOR_MAP).forEach(([key, selectors]) => {
      const text = pack[key] || fallback[key] || "";
      selectors.forEach((selector) => {
        document.querySelectorAll(selector).forEach((el) => setLocalizedText(el, text));
      });
    });

    const placeholderText = pack.question_placeholder || fallback.question_placeholder || "";
    if (ui.tickerQueryQuestion) {
      setLocalizedAttribute(ui.tickerQueryQuestion, "placeholder", placeholderText);
    }

    const selectorLabel = pack.language_selector_label || fallback.language_selector_label || "Language";
    if (ui.languageSelect) {
      setLocalizedAttribute(ui.languageSelect, "aria-label", selectorLabel);
      const languageLabelNode = document.querySelector('label[for="language-select"]');
      if (languageLabelNode) setLocalizedText(languageLabelNode, selectorLabel);
      Object.entries(UI_I18N_OPTION_MAP).forEach(([value, key]) => {
        const option = ui.languageSelect.querySelector(`option[value="${value}"]`);
        if (option) setLocalizedText(option, pack[key] || fallback[key] || option.textContent || "");
      });
    }
    if (ui.tickerQueryLanguage) {
      Object.entries(UI_I18N_OPTION_MAP).forEach(([value, key]) => {
        const option = ui.tickerQueryLanguage.querySelector(`option[value="${value}"]`);
        if (option) setLocalizedText(option, pack[key] || fallback[key] || option.textContent || "");
      });
    }

    if (ui.headerAuth) {
      const authLabel = accountAuthed ? (pack.dashboard || fallback.dashboard || "Dashboard") : (pack.sign_in || fallback.sign_in || "Sign in");
      setLocalizedAttribute(ui.headerAuth, "title", authLabel);
      setLocalizedAttribute(
        ui.headerAuth,
        "aria-label",
        accountAuthed ? (pack.open_dashboard || fallback.open_dashboard || "Open dashboard") : (pack.sign_in || fallback.sign_in || "Sign in")
      );
    }

    if (ui.headerUserStatus) {
      setLocalizedText(
        ui.headerUserStatus,
        accountAuthed
          ? (pack.logged_in || fallback.logged_in || "Logged In")
          : guestSession
          ? "Guest Session"
          : (pack.logged_out || fallback.logged_out || "Logged Out")
      );
    }
    if (ui.userEmail && !sessionAuthed) {
      setLocalizedText(ui.userEmail, pack.not_signed_in || fallback.not_signed_in || "Not signed in");
    }
    if (ui.userStatus) {
      setLocalizedText(
        ui.userStatus,
        accountAuthed
          ? (pack.logged_in || fallback.logged_in || "Logged In")
          : guestSession
          ? "Guest Session"
          : (pack.logged_out || fallback.logged_out || "Logged Out")
      );
    }
    if (ui.dashboardAuthLink) {
      const linkSpan = ui.dashboardAuthLink.querySelector("span");
      const linkLabel = sessionAuthed ? (pack.sign_out || fallback.sign_out || "Sign out") : (pack.sign_in || fallback.sign_in || "Sign in");
      if (linkSpan) setLocalizedText(linkSpan, linkLabel);
      setLocalizedAttribute(ui.dashboardAuthLink, "aria-label", linkLabel);
    }
    if (ui.billingPortalLink) {
      const nativeBilling = isNativeIosStoreKitCheckoutOnly() || isNativeAndroidPlayBillingCheckout();
      setLocalizedText(
        ui.billingPortalLink,
        accountAuthed
          ? nativeBilling
            ? nativeBillingPortalLabel()
            : (pack.open_billing_portal || fallback.open_billing_portal || "Open billing portal")
          : (pack.signin_manage_billing || fallback.signin_manage_billing || "Sign in to manage billing")
      );
    }
    if (ui.headerNotifications) {
      setLocalizedAttribute(ui.headerNotifications, "title", pack.nav_notifications || fallback.nav_notifications || "Notifications");
      setLocalizedAttribute(
        ui.headerNotifications,
        "aria-label",
        accountAuthed
          ? (pack.open_notifications || fallback.open_notifications || "Open notifications")
          : (pack.signin_manage_notifications || fallback.signin_manage_notifications || "Sign in to manage notifications")
      );
    }
    if (ui.pricingAuthCta) {
      const pricingAuthSpan = ui.pricingAuthCta.querySelector("span");
      if (pricingAuthSpan) {
        setLocalizedText(
          pricingAuthSpan,
          accountAuthed ? (pack.open_dashboard || fallback.open_dashboard || "Open dashboard") : (pack.sign_in || fallback.sign_in || "Sign in")
        );
      }
    }
    if (ui.pricingStarterCta) {
      const pricingStarterSpan = ui.pricingStarterCta.querySelector("span");
      if (pricingStarterSpan) {
        setLocalizedText(
          pricingStarterSpan,
          accountAuthed ? (pack.go_to_dashboard || fallback.go_to_dashboard || "Go to dashboard") : (pack.start_free || fallback.start_free || "Start free")
        );
      }
    }
    if (!accountAuthed && ui.profileStatus) {
      setLocalizedText(ui.profileStatus, pack.signin_set_profile || fallback.signin_set_profile || "Sign in to set your public profile.");
    }
  };

  const setCountryControls = (countryCode) => {
    const code = normalizeCountryCode(countryCode);
    state.preferredCountry = code;
    if (ui.marketHeadlinesCountry && ui.marketHeadlinesCountry.value !== code) ui.marketHeadlinesCountry.value = code;
  };

  const applyLanguagePreference = (languageCode, { persist = true } = {}) => {
    const normalized = normalizeLanguageCode(languageCode);
    const resolved = normalized === "auto" ? resolveLanguageFromNavigator() : normalized;
    state.preferredLanguage = resolved;
    document.documentElement.lang = resolved || "en";
    document.documentElement.dir = resolved === "ar" ? "rtl" : "ltr";
    if (ui.languageSelect) ui.languageSelect.value = normalized;
    if (ui.tickerQueryLanguage && ui.tickerQueryLanguage.value === "auto") {
      ui.tickerQueryLanguage.value = resolved;
    }
    applyUiTranslations(resolved);
    if (persist) safeLocalStorageSet(LANGUAGE_PREFERENCE_KEY, normalized);
  };

  const applyCountryPreference = (countryCode, { persist = true } = {}) => {
    const normalized = normalizeCountryCode(countryCode);
    setCountryControls(normalized);
    if (persist) safeLocalStorageSet(COUNTRY_PREFERENCE_KEY, normalized);
  };

  const fetchIpLocationContext = async () => {
    if (!state.notificationPrivacy?.locationConsent || !state.notificationPrivacy?.ipRegionConsent) {
      return { countryCode: "", region: "" };
    }
    try {
      const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      const timeout = window.setTimeout(() => controller?.abort(), 2600);
      const response = await fetch("https://ipapi.co/json/", {
        method: "GET",
        mode: "cors",
        signal: controller?.signal,
      });
      window.clearTimeout(timeout);
      if (!response.ok) return { countryCode: "", region: "" };
      const payload = await response.json();
      const raw = String(payload?.country_code || payload?.country || "").trim();
      const region = String(payload?.region_code || payload?.region || payload?.city || "").trim().slice(0, 80);
      return {
        countryCode: raw ? normalizeCountryCode(raw) : "",
        region,
      };
    } catch (error) {
      return { countryCode: "", region: "" };
    }
  };

  const initializeLanguageControls = async () => {
    const storedLanguage = normalizeLanguageCode(safeLocalStorageGet(LANGUAGE_PREFERENCE_KEY) || "");
    const urlLanguage = (() => {
      try {
        const params = new URLSearchParams(window.location.search);
        return normalizeLanguageCode(params.get("lang") || "");
      } catch (error) {
        return "auto";
      }
    })();
    const nextLanguage = urlLanguage !== "auto" ? urlLanguage : storedLanguage !== "auto" ? storedLanguage : "auto";
    applyLanguagePreference(nextLanguage, { persist: true });

    if (ui.languageSelect && ui.languageSelect.dataset.bound !== "1") {
      ui.languageSelect.value = nextLanguage;
      ui.languageSelect.addEventListener("change", () => {
        const selected = normalizeLanguageCode(ui.languageSelect.value || "auto");
        applyLanguagePreference(selected, { persist: true });
      });
      ui.languageSelect.dataset.bound = "1";
    }

    const storedCountry = normalizeCountryCode(safeLocalStorageGet(COUNTRY_PREFERENCE_KEY) || "");
    const urlCountry = (() => {
      try {
        const params = new URLSearchParams(window.location.search);
        const raw = String(params.get("country") || "").trim();
        return raw ? normalizeCountryCode(raw) : "";
      } catch (error) {
        return "";
      }
    })();
    const coarseCountryRaw = String(state.notificationPrivacy?.coarseLocation?.countryCode || "").trim();
    const coarseCountry =
      state.notificationPrivacy?.locationConsent && coarseCountryRaw ? normalizeCountryCode(coarseCountryRaw) : "";
    let country = urlCountry || (storedCountry !== "US" ? storedCountry : "") || (coarseCountry !== "US" ? coarseCountry : "");
    if (!country && state.notificationPrivacy?.locationConsent && state.notificationPrivacy?.ipRegionConsent) {
      const ipContext = await fetchIpLocationContext();
      country = ipContext.countryCode || "";
      if (ipContext.region) {
        state.notificationPrivacy.ipRegion = ipContext.region;
        state.notificationPrivacy.lastUpdatedMs = Date.now();
        persistNotificationPrivacyCache();
      }
    }
    if (!country) {
      const locale = String(navigator.language || "").split("-")[1] || "";
      country = locale ? normalizeCountryCode(locale) : "US";
    }
    applyCountryPreference(country || "US", { persist: true });
    if (storedLanguage === "auto" || !storedLanguage) {
      const best = resolveLanguageFromCountry(country || "US");
      applyLanguagePreference(best, { persist: false });
      if (ui.languageSelect) ui.languageSelect.value = "auto";
    }
  };

    const readCookie = (name) => {
      try {
        const raw = document.cookie || "";
        const parts = raw.split(";").map((p) => p.trim());
        for (const part of parts) {
          if (!part) continue;
          const idx = part.indexOf("=");
          if (idx < 0) continue;
          const key = part.slice(0, idx).trim();
          if (key !== name) continue;
          return decodeURIComponent(part.slice(idx + 1));
        }
      } catch (error) {
        // Ignore.
      }
      return "";
    };

    const writeCookie = (name, value, { days = 14 } = {}) => {
      try {
        const maxAge = Math.max(1, Number(days) || 14) * 24 * 60 * 60;
        document.cookie = `${name}=${encodeURIComponent(String(value || ""))}; Path=/; Max-Age=${maxAge}; SameSite=Lax; Secure`;
      } catch (error) {
        // Ignore.
      }
    };

    const deleteCookie = (name) => {
      try {
        document.cookie = `${name}=; Path=/; Max-Age=0; SameSite=Lax; Secure`;
      } catch (error) {
        // Ignore.
      }
    };

    const setPendingShareId = (shareId) => {
      const id = String(shareId || "").trim();
      const previous = String(state.pendingShareId || "").trim();
      state.pendingShareId = id;
      if (id !== previous) {
        state.pendingShareProcessed = false;
      }
      if (id) {
        safeLocalStorageSet(PENDING_SHARE_KEY, id);
        writeCookie(PENDING_SHARE_KEY, id, { days: 14 });
      } else {
        state.pendingShareProcessed = false;
        safeLocalStorageRemove(PENDING_SHARE_KEY);
        deleteCookie(PENDING_SHARE_KEY);
      }
    };

    const getPendingShareId = () => {
      if (state.pendingShareId) return state.pendingShareId;
      const fromStorage = String(safeLocalStorageGet(PENDING_SHARE_KEY) || "").trim();
      if (fromStorage) return fromStorage;
      const fromCookie = String(readCookie(PENDING_SHARE_KEY) || "").trim();
      return fromCookie;
    };

    const captureShareFromUrl = () => {
      let share = "";
      try {
        const url = new URL(window.location.href);
        share = String(url.searchParams.get("share") || "").trim();
        if (!share) return "";
        url.searchParams.delete("share");
        history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
      } catch (error) {
        return "";
      }
      if (share) setPendingShareId(share);
      return share;
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
    const themeToggleIconHtml = (theme) => (theme === "dark" ? icon("sun-light") : icon("half-moon"));

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
	        button.innerHTML = themeToggleIconHtml(next);
        button.setAttribute("aria-label", next === "dark" ? "Switch to light mode" : "Switch to dark mode");
        button.setAttribute("title", next === "dark" ? "Light mode" : "Dark mode");
      }
      if (getActiveTicker() && isPanelVisible("ticker")) {
        renderTradingViewTerminal({
          ticker: getActiveTicker(),
          interval: state.tickerContext.interval || "1d",
        });
      }
      applyChartControlState();
    };

    const ensureThemeToggle = () => {
      if (document.getElementById("theme-toggle")) return;
      const host = document.querySelector(".nav-actions");
      if (!host) return;
      const button = document.createElement("button");
      button.id = "theme-toggle";
      button.type = "button";
      button.className = "cta secondary small theme-toggle";
      const initialTheme = isDarkMode() ? "dark" : "light";
      button.innerHTML = themeToggleIconHtml(initialTheme);
      button.setAttribute("aria-label", initialTheme === "dark" ? "Switch to light mode" : "Switch to dark mode");
      button.setAttribute("title", initialTheme === "dark" ? "Light mode" : "Dark mode");
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
              startScreenerRuns(db, resolved);
	            startWorkspaceTasks(db, resolved);
	            startWatchlist(db, resolved);
	            startPriceAlerts(db, resolved);
              fetchMyRequestsList({ force: true }).then(() => {
                renderMyRequestsPanels();
              });
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
      loadRemoteConfig();
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

  const ensureActionModal = () => {
    let modal = document.getElementById("action-modal");
    if (!modal) modal = buildModalShell("action-modal");
    return modal;
  };

  const openConfirmModal = ({ title, message, confirmLabel = "Confirm", cancelLabel = "Cancel", danger = false } = {}) =>
    new Promise((resolve) => {
      const modal = ensureActionModal();
      const card = modal.querySelector(".modal-card");
      if (!card) {
        resolve(false);
        return;
      }

      card.innerHTML = "";
      const h = document.createElement("h3");
      h.textContent = title || "Confirm";
      const p = document.createElement("p");
      p.className = "small";
      p.textContent = message || "";
      const actions = document.createElement("div");
      actions.className = "modal-actions";

      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "cta secondary";
      cancelBtn.dataset.action = "cancel";
      cancelBtn.textContent = cancelLabel;

      const confirmBtn = document.createElement("button");
      confirmBtn.type = "button";
      confirmBtn.className = danger ? "cta secondary danger" : "cta";
      confirmBtn.dataset.action = "confirm";
      confirmBtn.textContent = confirmLabel;

      actions.appendChild(cancelBtn);
      actions.appendChild(confirmBtn);

      card.appendChild(h);
      card.appendChild(p);
      card.appendChild(actions);

      modal.classList.remove("hidden");
      logEvent("modal_opened", { modal: "confirm", title: String(title || "").slice(0, 60) });

      const cleanup = (value) => {
        modal.classList.add("hidden");
        modal.removeEventListener("click", onClick);
        window.removeEventListener("keydown", onKeyDown, true);
        resolve(Boolean(value));
      };

      const onClick = (event) => {
        const action = event.target?.dataset?.action;
        if (!action) return;
        if (action === "close" || action === "cancel") {
          cleanup(false);
          return;
        }
        if (action === "confirm") {
          logEvent("modal_confirmed", { modal: "confirm", title: String(title || "").slice(0, 60) });
          cleanup(true);
        }
      };

      const onKeyDown = (event) => {
        if (event.key === "Escape") cleanup(false);
      };

      modal.addEventListener("click", onClick);
      window.addEventListener("keydown", onKeyDown, true);
      window.setTimeout(() => confirmBtn.focus(), 0);
    });

  const openPromptModal = ({
    title,
    message,
    label,
    placeholder = "",
    initialValue = "",
    confirmLabel = "Save",
    cancelLabel = "Cancel",
    maxLen = 180,
  } = {}) =>
    new Promise((resolve) => {
      const modal = ensureActionModal();
      const card = modal.querySelector(".modal-card");
      if (!card) {
        resolve(null);
        return;
      }

      card.innerHTML = "";
      const h = document.createElement("h3");
      h.textContent = title || "Update";
      const p = document.createElement("p");
      p.className = "small";
      p.textContent = message || "";

      const labelEl = document.createElement("label");
      labelEl.className = "label";
      labelEl.textContent = label || "Value";

      const input = document.createElement("input");
      input.type = "text";
      input.className = "modal-input";
      input.placeholder = placeholder;
      input.maxLength = Math.max(1, Number(maxLen) || 180);
      input.value = String(initialValue || "");

      const status = document.createElement("p");
      status.className = "small muted";
      status.style.marginTop = "10px";
      status.textContent = "";

      const actions = document.createElement("div");
      actions.className = "modal-actions";

      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "cta secondary";
      cancelBtn.dataset.action = "cancel";
      cancelBtn.textContent = cancelLabel;

      const confirmBtn = document.createElement("button");
      confirmBtn.type = "button";
      confirmBtn.className = "cta";
      confirmBtn.dataset.action = "confirm";
      confirmBtn.textContent = confirmLabel;

      actions.appendChild(cancelBtn);
      actions.appendChild(confirmBtn);

      card.appendChild(h);
      if (message) card.appendChild(p);
      card.appendChild(labelEl);
      card.appendChild(input);
      card.appendChild(actions);
      card.appendChild(status);

      modal.classList.remove("hidden");
      logEvent("modal_opened", { modal: "prompt", title: String(title || "").slice(0, 60) });

      const cleanup = (value) => {
        modal.classList.add("hidden");
        modal.removeEventListener("click", onClick);
        window.removeEventListener("keydown", onKeyDown, true);
        resolve(value);
      };

      const onConfirm = () => {
        const next = String(input.value || "").trim();
        if (!next) {
          status.textContent = "Enter a value.";
          return;
        }
        cleanup(next);
      };

      const onClick = (event) => {
        const action = event.target?.dataset?.action;
        if (!action) return;
        if (action === "close" || action === "cancel") {
          cleanup(null);
          return;
        }
        if (action === "confirm") {
          logEvent("modal_confirmed", { modal: "prompt", title: String(title || "").slice(0, 60) });
          onConfirm();
        }
      };

      const onKeyDown = (event) => {
        if (event.key === "Escape") cleanup(null);
        if (event.key === "Enter") {
          event.preventDefault();
          onConfirm();
        }
      };

      modal.addEventListener("click", onClick);
      window.addEventListener("keydown", onKeyDown, true);
      window.setTimeout(() => {
        input.focus();
        input.select?.();
      }, 0);
    });

  const buildSolveNowPrompt = (request, ticker) =>
    [
      "You are the Quantura AI assistant.",
      `Primary ticker context: ${ticker}.`,
      "Respond in this exact structure:",
      "Summary:",
      "- one concise paragraph",
      "Suggested next steps:",
      "1. action one",
      "2. action two",
      "3. action three",
      "Keep the guidance practical and risk-aware.",
      "",
      `User request: ${String(request || "").trim()}`,
    ].join("\n");

  const extractSolveNowSuggestedSteps = (answer) => {
    const text = String(answer || "").trim();
    if (!text) return [];
    const listMatches = Array.from(
      text.matchAll(/(?:^|\n)\s*(?:[-*]\s+|\d+\.\s+)(.+?)(?=\n|$)/g)
    )
      .map((match) => String(match[1] || "").trim())
      .filter(Boolean);
    if (listMatches.length) return listMatches.slice(0, 4);
    const sentenceMatches = text
      .split(/(?<=[.!?])\s+/)
      .map((row) => row.trim())
      .filter((row) => row.length >= 20);
    return sentenceMatches.slice(0, 3);
  };

  const ensureSolveNowModal = () => {
    let modal = document.getElementById("solve-now-modal");
    if (!modal) {
      modal = buildModalShell("solve-now-modal");
      modal.classList.add("solve-now-modal");
      const card = modal.querySelector(".modal-card");
      if (card) {
        card.classList.add("solve-now-modal-card");
        card.innerHTML = `
          <div class="solve-now-head">
            <div>
              <h3>Solve now</h3>
              <p class="small muted">Powered by AI. Tell us what you need and we will map out the next steps.</p>
            </div>
            <button class="cta secondary icon-only" type="button" data-action="close-solve-now" aria-label="Close Solve now">
              ${icon("xmark")}
            </button>
          </div>
          <form id="solve-now-form" class="solve-now-form">
            <div class="field">
              <label class="label" for="solve-now-request">What do you need help with?</label>
              <textarea id="solve-now-request" class="modal-input solve-now-input" rows="4" placeholder="Example: I need a macro-aware plan for NVDA earnings risk this week." required></textarea>
            </div>
            <div class="solve-now-controls">
              <div class="field">
                <label class="label" for="solve-now-ticker">Ticker context</label>
                <input id="solve-now-ticker" class="modal-input solve-now-ticker" type="text" maxlength="12" placeholder="SPY" />
              </div>
              <div class="modal-actions solve-now-actions">
                <button class="cta secondary" type="button" data-action="close-solve-now">Cancel</button>
                <button class="cta" type="submit" data-action="run-solve-now">${icon("brain")}<span>Solve now</span></button>
              </div>
            </div>
            <p id="solve-now-status" class="small muted" aria-live="polite"></p>
            <section id="solve-now-output" class="solve-now-output hidden" aria-live="polite"></section>
          </form>
        `;
      }
    }
    return modal;
  };

  const closeSolveNowModal = () => {
    const modal = document.getElementById("solve-now-modal");
    if (!modal) return;
    modal.classList.add("hidden");
    if (window.location.hash === "#solve-now" && window.history?.replaceState) {
      window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}`);
    }
  };

  const renderSolveNowOutput = ({ answer, model, provider, ticker }) => {
    const output = document.getElementById("solve-now-output");
    if (!output) return;
    const cleanAnswer = String(answer || "").trim();
    const summaryParagraph =
      cleanAnswer
        .split(/\n{2,}/)
        .map((block) => block.trim())
        .find(Boolean) || cleanAnswer;
    const steps = extractSolveNowSuggestedSteps(cleanAnswer);
    output.innerHTML = `
      <article class="solve-now-response">
        <h4>Summary</h4>
        <p>${escapeHtml(summaryParagraph || "No summary was returned.")}</p>
        <h4>Suggested next steps</h4>
        <ol>
          ${steps.length ? steps.map((step) => `<li>${escapeHtml(step)}</li>`).join("") : "<li>Refine your request with ticker, timeframe, and risk limits.</li>"}
        </ol>
        <details>
          <summary>Full AI output</summary>
          <pre class="small">${escapeHtml(cleanAnswer || "No output returned.")}</pre>
        </details>
        <div class="small muted solve-now-meta">Context ticker: ${escapeHtml(ticker)} · Model: ${escapeHtml(model || "gpt-5-mini")} · Provider: ${escapeHtml(provider || "openai")}</div>
        <p class="small muted solve-now-disclaimer">LLMs can sometimes make mistakes.</p>
      </article>
    `;
    output.classList.remove("hidden");
  };

  const openSolveNowModal = ({ prefillPrompt = "", source = "header" } = {}) => {
    const modal = ensureSolveNowModal();
    const form = modal.querySelector("#solve-now-form");
    const input = modal.querySelector("#solve-now-request");
    const tickerInput = modal.querySelector("#solve-now-ticker");
    const status = modal.querySelector("#solve-now-status");
    const output = modal.querySelector("#solve-now-output");
    const submitButton = modal.querySelector('[data-action="run-solve-now"]');
    if (!form || !input || !tickerInput || !status || !output || !submitButton) return;

    if (modal.dataset.bound !== "1") {
      modal.dataset.bound = "1";
      modal.addEventListener("click", (event) => {
        const action = event.target?.dataset?.action;
        if (action === "close" || action === "close-solve-now") {
          closeSolveNowModal();
        }
      });
      window.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && !modal.classList.contains("hidden")) {
          closeSolveNowModal();
        }
      });
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const request = String(input.value || "").trim();
        const tickerFallback = getActiveTicker() || normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY) || "") || "SPY";
        const ticker = normalizeTicker(tickerInput.value || tickerFallback) || "SPY";
        if (!request) {
          status.textContent = "Tell us what you need so AI can help.";
          return;
        }
        const language = normalizeLanguageCode(state.preferredLanguage || ui.tickerQueryLanguage?.value || "en");
        const model = normalizeAiModelId(ui.tickerQueryModel?.value || state.tickerContext.tickerQueryModel || "gpt-5-mini") || "gpt-5-mini";
        const prompt = buildSolveNowPrompt(request, ticker);
        input.disabled = true;
        tickerInput.disabled = true;
        submitButton.disabled = true;
        status.textContent = "Generating your plan...";
        output.classList.add("hidden");
        output.innerHTML = "";
        try {
          const streamed = await streamTickerQueryInsight({
            ticker,
            prompt,
            language,
            model,
            technicalContext: null,
          });
          renderSolveNowOutput({
            answer: streamed.answer || "",
            model: streamed.model || model,
            provider: streamed.provider || "openai",
            ticker,
          });
          status.textContent = "Done.";
          logEvent("solve_now_completed", {
            source,
            ticker,
            model: streamed.model || model,
            page_path: window.location.pathname,
          });
        } catch (error) {
          const message = String(error?.message || "Unable to complete Solve now right now.");
          status.textContent = message;
          output.classList.remove("hidden");
          output.innerHTML = `<div class="small muted">${escapeHtml(message)}</div><p class="small muted solve-now-disclaimer">LLMs can sometimes make mistakes.</p>`;
          logEvent("solve_now_failed", {
            source,
            ticker,
            error: message.slice(0, 120),
            page_path: window.location.pathname,
          });
        } finally {
          input.disabled = false;
          tickerInput.disabled = false;
          submitButton.disabled = false;
        }
      });
    }

    const fallbackTicker = getActiveTicker() || normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY) || "") || "SPY";
    if (!String(tickerInput.value || "").trim()) tickerInput.value = fallbackTicker;
    if (prefillPrompt && !String(input.value || "").trim()) input.value = String(prefillPrompt || "").trim();
    status.textContent = "";
    modal.classList.remove("hidden");
    if (window.history?.replaceState) {
      window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}#solve-now`);
    }
    window.setTimeout(() => input.focus(), 0);
    logEvent("solve_now_opened", { source, page_path: window.location.pathname });
  };

  const bindSolveNowModalTriggers = () => {
    const bindClick = (element, source) => {
      if (!(element instanceof HTMLElement)) return;
      if (element.dataset.solveNowBound === "1") return;
      element.dataset.solveNowBound = "1";
      element.addEventListener("click", (event) => {
        event.preventDefault();
        openSolveNowModal({ source });
      });
    };
    bindClick(document.getElementById("header-solve-now"), "header");
    document.querySelectorAll('[data-action="open-solve-now"]').forEach((element) => {
      const source = String(element.getAttribute("data-solve-source") || "").trim() || "contact";
      bindClick(element, source);
    });
    if (!state.solveNowHashChecked) {
      state.solveNowHashChecked = true;
      if (window.location.hash === "#solve-now") {
        window.setTimeout(() => openSolveNowModal({ source: "hash" }), 80);
      }
    }
  };

  const ensureCookieModal = () => {
    let banner = document.getElementById("cookie-banner");
    if (!banner) {
      banner = document.createElement("aside");
      banner.id = "cookie-banner";
      banner.className = "cookie-banner hidden";
      banner.innerHTML = `
        <div class="cookie-banner-content">
          <div>
            <h3>Cookies and analytics</h3>
            <p class="small">
              Quantura uses cookies for analytics and reliability. You can opt out at any time.
            </p>
          </div>
          <div class="cookie-banner-actions">
            <button class="cta secondary small" type="button" data-action="decline">No thanks</button>
            <button class="cta small" type="button" data-action="accept">Accept</button>
          </div>
        </div>
      `;
      document.body.appendChild(banner);
    }
    if (banner.dataset.bound !== "1") {
      banner.dataset.bound = "1";
      banner.addEventListener("click", (event) => {
        const action = event.target?.dataset?.action;
        if (!action) return;
        if (action === "accept") {
          setCookieConsent("accepted");
          banner.classList.add("hidden");
        }
        if (action === "decline") {
          setCookieConsent("declined");
          banner.classList.add("hidden");
        }
      });
    }
    return banner;
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
      } catch (error) {
        if (status) status.textContent = error.message || "Unable to send feedback.";
        showToast(error.message || "Unable to send feedback.", "warn");
      }
    });
    return modal;
  };

  const ensureProfileFeedbackButtons = () => {
    const editors = Array.from(document.querySelectorAll(".profile-editor"));
    if (!editors.length) return;
    editors.forEach((editor) => {
      if (!(editor instanceof HTMLElement)) return;
      if (editor.querySelector('[data-action="open-feedback-modal"]')) return;
      const button = document.createElement("button");
      button.type = "button";
      button.className = "cta secondary small profile-feedback-button";
      button.dataset.action = "open-feedback-modal";
      button.innerHTML = `${icon("message-text")}<span>Send feedback</span>`;
      button.addEventListener("click", () => {
        ensureFeedbackModal().classList.remove("hidden");
        logEvent("feedback_opened", { page_path: window.location.pathname, source: "profile_settings" });
      });
      editor.appendChild(button);
    });
  };

  const SUBSCRIPTION_TIER_RANK = Object.freeze({
    free: 0,
    go: 1,
    plus: 2,
    pro: 3,
    business: 4,
    desk: 4,
  });

  const normalizeSubscriptionTier = (value) => {
    const raw = String(value || "").trim().toLowerCase();
    if (!raw) return "free";
    if (raw === "desk") return "business";
    if (raw in SUBSCRIPTION_TIER_RANK) return raw;
    return "free";
  };

  const subscriptionTierFromOrder = (order = {}) => {
    const parts = [];
    parts.push(String(order?.subscriptionTier || order?.tier || order?.plan || "").trim().toLowerCase());
    parts.push(String(order?.productId || order?.sku || "").trim().toLowerCase());
    parts.push(String(order?.product || "").trim().toLowerCase());
    const meta = order?.meta && typeof order.meta === "object" ? order.meta : {};
    parts.push(String(meta?.subscriptionTier || meta?.tier || meta?.plan || "").trim().toLowerCase());
    parts.push(String(meta?.productId || meta?.sku || "").trim().toLowerCase());
    const bag = parts.filter(Boolean).join(" ");

    if (
      bag.includes("annualbusinessplan") ||
      bag.includes("businessplan") ||
      bag.includes("quanturabusiness") ||
      bag.includes("quantura business") ||
      bag.includes("business")
    ) {
      return "business";
    }
    if (bag.includes("quanturapro") || bag.includes(" pro")) return "pro";
    if (bag.includes("annualplusplan") || bag.includes("premium") || bag.includes("plus")) return "plus";
    if (bag.includes("goplanyearly") || bag.includes("annualgoplan") || bag.includes("goplan") || bag.includes(" go")) return "go";
    return "free";
  };

  const deriveSubscriptionTierFromOrders = (orders = []) => {
    let bestTier = "free";
    let bestRank = SUBSCRIPTION_TIER_RANK.free;
    orders.forEach((order) => {
      const tier = subscriptionTierFromOrder(order);
      const rank = Number(SUBSCRIPTION_TIER_RANK[tier] ?? 0);
      if (rank > bestRank) {
        bestTier = tier;
        bestRank = rank;
      }
    });
    return normalizeSubscriptionTier(bestTier);
  };

  const hasAdFreeEntitlement = () =>
    hasFullAccount() && normalizeSubscriptionTier(state.userSubscriptionTier) !== "free";

  const applyAdFreeExperience = () => {
    const adFree = hasAdFreeEntitlement();
    document.body.classList.toggle("ad-free-user", adFree);
    if (!adFree) {
      scheduleNativeInlineAdsRefresh();
      return;
    }
    const selectors = [
      '[data-ad-slot]',
      '.ad-slot',
      '.ad-banner',
      '#ad-banner',
      '#ad-container',
      '.promo-banner-ad',
      '.native-inline-ad-slot',
    ];
    selectors.forEach((selector) => {
      document.querySelectorAll(selector).forEach((node) => {
        node.classList.add("hidden");
        node.setAttribute("aria-hidden", "true");
      });
    });
    scheduleNativeInlineAdsRefresh();
  };

  const nativeInlineAdState = {
    listenerBound: false,
    sequence: 0,
    pendingBySlot: new Map(),
    impressionObserver: null,
    seenImpressions: new Set(),
    observedContainers: new WeakSet(),
    observerMap: new Map(),
    refreshTimer: 0,
  };

  const getNativeInlineAdRules = () => {
    const defaults = { feedStart: 6, feedInterval: 8, pageMidpoint: 0.55 };
    const raw = window.__QUANTURA_NATIVE_AD_RULES__ && typeof window.__QUANTURA_NATIVE_AD_RULES__ === "object"
      ? window.__QUANTURA_NATIVE_AD_RULES__
      : {};
    const feedStart = Number(raw.feedStart);
    const feedInterval = Number(raw.feedInterval);
    const pageMidpoint = Number(raw.pageMidpoint);
    return {
      feedStart: Number.isFinite(feedStart) ? Math.max(3, Math.min(20, Math.floor(feedStart))) : defaults.feedStart,
      feedInterval: Number.isFinite(feedInterval) ? Math.max(3, Math.min(20, Math.floor(feedInterval))) : defaults.feedInterval,
      pageMidpoint: Number.isFinite(pageMidpoint) ? Math.max(0.2, Math.min(0.9, pageMidpoint)) : defaults.pageMidpoint,
    };
  };

  const isNativeInlineAdEligible = () => {
    if (!isNativeApp()) return false;
    if (hasAdFreeEntitlement()) return false;
    if (state.authGateVisible) return false;
    const pathname = String(window.location.pathname || "").trim().toLowerCase();
    if (pathname === "/account" || pathname === "/pricing") return false;
    return true;
  };

  const ensureNativeInlineAdListener = () => {
    if (nativeInlineAdState.listenerBound) return;
    nativeInlineAdState.listenerBound = true;
    window.addEventListener("quantura:native-feed-ad", (event) => {
      const detail = event?.detail && typeof event.detail === "object" ? event.detail : {};
      const slotId = String(detail.slotId || "").trim();
      if (!slotId) return;
      const pending = nativeInlineAdState.pendingBySlot.get(slotId);
      if (!pending) return;
      nativeInlineAdState.pendingBySlot.delete(slotId);
      if (detail.ok === false) {
        pending.reject(new Error(String(detail.error || "Native ad failed.")));
        return;
      }
      pending.resolve(detail);
    });
  };

  const reportNativeInlineAdBridgeEvent = (action, payload = {}) => {
    if (!isNativeApp()) return false;
    return sendNativeBridgeMessage({
      action,
      slotId: String(payload.slotId || "").trim(),
      placement: String(payload.placement || "").trim(),
      adUnitId: String(payload.adUnitId || "").trim(),
    });
  };

  const requestNativeInlineAd = ({ slotId, placement, variant = "nativeAdvanced", timeoutMs = 12000 } = {}) =>
    new Promise((resolve, reject) => {
      if (!isNativeInlineAdEligible()) {
        reject(new Error("native_ads_ineligible"));
        return;
      }
      ensureNativeInlineAdListener();
      const resolvedSlotId = String(slotId || "").trim();
      if (!resolvedSlotId) {
        reject(new Error("slot_id_missing"));
        return;
      }
      const timeoutHandle = window.setTimeout(() => {
        nativeInlineAdState.pendingBySlot.delete(resolvedSlotId);
        reject(new Error("native_ad_timeout"));
      }, Math.max(2000, Number(timeoutMs) || 12000));
      nativeInlineAdState.pendingBySlot.set(resolvedSlotId, {
        resolve: (detail) => {
          clearTimeout(timeoutHandle);
          resolve(detail);
        },
        reject: (error) => {
          clearTimeout(timeoutHandle);
          reject(error);
        },
      });
      let sent = sendNativeBridgeMessage({
        action: "requestNativeFeedAd",
        slotId: resolvedSlotId,
        placement: String(placement || "inline").trim(),
        variant: String(variant || "nativeAdvanced").trim(),
      });
      if (!sent) {
        sent =
          sendNativeBridgeMessage({
            action: "showNativeAd",
            slotId: resolvedSlotId,
            placement: String(placement || "inline").trim(),
            variant: String(variant || "nativeAdvanced").trim(),
          }) ||
          sendNativeBridgeMessage({
            action: "loadNativeAd",
            slotId: resolvedSlotId,
            placement: String(placement || "inline").trim(),
            variant: String(variant || "nativeAdvanced").trim(),
          });
      }
      if (!sent) {
        sent =
          sendNativeAuthMessage({
            type: "REQUEST_NATIVE_FEED_AD",
            slotId: resolvedSlotId,
            placement: String(placement || "inline").trim(),
            variant: String(variant || "nativeAdvanced").trim(),
          }) ||
          sendNativeAuthMessage({
            type: "SHOW_NATIVE_AD",
            slotId: resolvedSlotId,
            placement: String(placement || "inline").trim(),
            variant: String(variant || "nativeAdvanced").trim(),
          });
      }
      if (!sent) {
        clearTimeout(timeoutHandle);
        nativeInlineAdState.pendingBySlot.delete(resolvedSlotId);
        reject(new Error("native_bridge_unavailable"));
      }
    });

  const toNativeInlineAdImage = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (/^data:image\//i.test(raw)) return raw;
    if (/^https?:\/\//i.test(raw)) return raw;
    return `data:image/png;base64,${raw}`;
  };

  const ensureNativeInlineAdImpressionObserver = () => {
    if (nativeInlineAdState.impressionObserver || typeof IntersectionObserver !== "function") return;
    nativeInlineAdState.impressionObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting || entry.intersectionRatio < 0.45) return;
          const target = entry.target;
          const slotId = String(target.dataset.nativeInlineAdSlot || "").trim();
          if (!slotId || nativeInlineAdState.seenImpressions.has(slotId)) return;
          nativeInlineAdState.seenImpressions.add(slotId);
          const placement = String(target.dataset.placement || "inline").trim();
          const adUnitId = String(target.dataset.adUnitId || "").trim();
          logEvent("ad_impression", { slot_id: slotId, placement, ad_unit_id: adUnitId, runtime: resolveRuntimeLabel() });
          reportNativeInlineAdBridgeEvent("nativeFeedAdImpression", { slotId, placement, adUnitId });
        });
      },
      { threshold: [0.45] }
    );
  };

  const buildNativeInlineAdSlot = (slotId, placement) => {
    const node = document.createElement("article");
    node.className = "native-inline-ad-slot native-inline-ad-loading";
    node.dataset.nativeInlineAdSlot = slotId;
    node.dataset.placement = placement;
    node.dataset.adSlot = "native-inline";
    node.setAttribute("aria-live", "polite");
    node.innerHTML = `
      <div class="native-inline-ad-skeleton">
        <div class="skeleton-line w50"></div>
        <div class="skeleton-line w85"></div>
        <div class="skeleton-line w70"></div>
        <div class="native-inline-ad-media"></div>
      </div>
    `;
    return node;
  };

  const hydrateNativeInlineAdSlot = (slotNode, detail = {}) => {
    const ad = detail?.ad && typeof detail.ad === "object" ? detail.ad : detail;
    const slotId = String(detail.slotId || slotNode.dataset.nativeInlineAdSlot || "").trim();
    const placement = String(detail.placement || slotNode.dataset.placement || "inline").trim();
    const adUnitId = String(detail.adUnitId || ad?.adUnitId || "").trim();
    const headline = String(ad?.headline || "Sponsored insight").trim();
    const body = String(ad?.body || "This section is sponsored.").trim();
    const cta = String(ad?.callToAction || "Learn more").trim();
    const advertiser = String(ad?.advertiser || ad?.store || "Sponsored").trim();
    const iconUrl = toNativeInlineAdImage(ad?.iconDataUrl || ad?.iconUrl || "");
    const mediaUrl = toNativeInlineAdImage(ad?.mediaDataUrl || ad?.mediaUrl || "");
    const destinationUrl = /^https?:\/\//i.test(String(ad?.destinationUrl || "").trim()) ? String(ad.destinationUrl).trim() : "";

    slotNode.classList.remove("native-inline-ad-loading");
    slotNode.classList.add("native-inline-ad-ready");
    slotNode.dataset.adUnitId = adUnitId;
    slotNode.innerHTML = `
      <div class="native-inline-ad-inner">
        <div class="native-inline-ad-top">
          <span class="native-inline-ad-badge">Ad</span>
          <span class="native-inline-ad-choices">AdChoices</span>
        </div>
        <div class="native-inline-ad-main">
          <div class="native-inline-ad-copy">
            <h4 class="native-inline-ad-title">${escapeHtml(headline)}</h4>
            <p class="native-inline-ad-body">${escapeHtml(body)}</p>
            <div class="native-inline-ad-meta">${escapeHtml(advertiser)}</div>
          </div>
          ${
            iconUrl
              ? `<img class="native-inline-ad-icon" src="${escapeHtml(iconUrl)}" alt="" loading="lazy" />`
              : `<div class="native-inline-ad-icon native-inline-ad-icon-fallback">Q</div>`
          }
        </div>
        ${
          mediaUrl
            ? `<img class="native-inline-ad-media" src="${escapeHtml(mediaUrl)}" alt="" loading="lazy" />`
            : `<div class="native-inline-ad-media native-inline-ad-media-fallback" aria-hidden="true"></div>`
        }
        <button type="button" class="task-chip native-inline-ad-cta" data-native-inline-ad-click="1">${escapeHtml(cta)}</button>
      </div>
    `;

    slotNode.addEventListener("click", (event) => {
      if (!event.target.closest("[data-native-inline-ad-click='1']")) return;
      logEvent("ad_click", { slot_id: slotId, placement, ad_unit_id: adUnitId, runtime: resolveRuntimeLabel() });
      reportNativeInlineAdBridgeEvent("nativeFeedAdClick", { slotId, placement, adUnitId });
      if (destinationUrl) {
        window.open(destinationUrl, "_blank", "noopener,noreferrer");
      }
    });
    ensureNativeInlineAdImpressionObserver();
    nativeInlineAdState.impressionObserver?.observe(slotNode);
  };

  const loadNativeInlineAdSlot = async (slotNode) => {
    if (!slotNode || !isNativeInlineAdEligible()) {
      slotNode?.remove();
      return;
    }
    const slotId = String(slotNode.dataset.nativeInlineAdSlot || "").trim();
    const placement = String(slotNode.dataset.placement || "inline").trim();
    logEvent("ad_request", { slot_id: slotId, placement, runtime: resolveRuntimeLabel() });
    try {
      const detail = await requestNativeInlineAd({ slotId, placement, variant: "nativeAdvanced" });
      hydrateNativeInlineAdSlot(slotNode, detail || {});
      const adUnitId = String(detail?.adUnitId || detail?.ad?.adUnitId || "").trim();
      logEvent("ad_loaded", { slot_id: slotId, placement, ad_unit_id: adUnitId, runtime: resolveRuntimeLabel() });
    } catch (error) {
      logEvent("ad_failed", {
        slot_id: slotId,
        placement,
        reason: String(error?.message || "load_failed").slice(0, 120),
        runtime: resolveRuntimeLabel(),
      });
      hydrateNativeInlineAdSlot(slotNode, {
        slotId,
        placement,
        adUnitId: "native_fallback",
        ad: {
          headline: "Sponsored insight",
          body: "Ad inventory is loading. You can continue using Quantura while this slot refreshes.",
          callToAction: "View plans",
          advertiser: "Quantura",
          destinationUrl: `${window.location.origin}/pricing`,
        },
      });
      slotNode.dataset.nativeInlineAdFallback = "1";
    }
  };

  const collectNativeInlineAdTargets = () => {
    const ids = [
      "trending-list",
      "intel-output",
      "news-output",
      "x-trending-output",
      "events-calendar-output",
      "market-headlines-output",
      "market-social-output",
      "fiscaldata-macro-groups",
      "ticker-output",
      "ticker-predictions-output",
      "screener-output",
      "watchlist-list",
      "alerts-list",
      "notifications-items",
      "saved-forecasts-list",
      "user-orders",
      "user-forecasts",
      "options-output",
      "predictions-output",
      "autopilot-output",
      "productivity-board",
      "collab-collaborators-list",
      "collab-invites-list",
    ];
    const set = new Set();
    ids.forEach((id) => {
      const node = document.getElementById(id);
      if (node) set.add(node);
    });
    document.querySelectorAll(".panel-output, .order-list, .news-stream").forEach((node) => {
      if (node?.id === "profile-status" || node?.id === "auth-email-message") return;
      set.add(node);
    });
    return Array.from(set);
  };

  const maybeInjectNativeInlineAd = (container) => {
    if (!container) return;
    if (!isNativeInlineAdEligible()) {
      container.querySelectorAll("[data-native-inline-ad-slot]").forEach((node) => node.remove());
      return;
    }
    if (container.closest("form, .auth-card, .checkout-shell, .purchase-panel")) return;
    if (container.querySelector("[data-native-inline-ad-slot]")) return;
    if (container.classList.contains("hidden")) return;
    if (String(container.dataset.loading || "") === "true") return;

    const children = Array.from(container.children).filter((child) => !child.matches("[data-native-inline-ad-slot]"));
    const textLength = String(container.textContent || "").trim().length;
    if (children.length < 2 && textLength < 240) return;

    nativeInlineAdState.sequence += 1;
    const slotId = `inline-${Date.now()}-${nativeInlineAdState.sequence}`;
    const placement = container.id ? `section_${container.id}` : "section_panel";
    const slotNode = buildNativeInlineAdSlot(slotId, placement);
    const rules = getNativeInlineAdRules();
    const index = Math.max(0, Math.min(children.length - 1, Math.floor(children.length * rules.pageMidpoint)));
    const anchor = children[index] || null;
    if (anchor && anchor.parentElement === container && anchor.nextSibling) {
      container.insertBefore(slotNode, anchor.nextSibling);
    } else if (anchor && anchor.parentElement === container) {
      container.appendChild(slotNode);
    } else {
      container.appendChild(slotNode);
    }
    loadNativeInlineAdSlot(slotNode).catch(() => undefined);
  };

  const observeNativeInlineAdContainer = (container) => {
    if (!container || nativeInlineAdState.observedContainers.has(container)) return;
    nativeInlineAdState.observedContainers.add(container);
    const observer = new MutationObserver(() => {
      if (nativeInlineAdState.refreshTimer) {
        clearTimeout(nativeInlineAdState.refreshTimer);
      }
      nativeInlineAdState.refreshTimer = window.setTimeout(() => {
        maybeInjectNativeInlineAd(container);
      }, 180);
    });
    observer.observe(container, { childList: true, subtree: false });
    nativeInlineAdState.observerMap.set(container, observer);
  };

  const refreshNativeInlineAds = () => {
    const targets = collectNativeInlineAdTargets();
    targets.forEach((container) => {
      observeNativeInlineAdContainer(container);
      maybeInjectNativeInlineAd(container);
    });
  };

  const scheduleNativeInlineAdsRefresh = () => {
    if (nativeInlineAdState.refreshTimer) clearTimeout(nativeInlineAdState.refreshTimer);
    nativeInlineAdState.refreshTimer = window.setTimeout(refreshNativeInlineAds, 200);
  };

  const getWorkspaceSeatLimitForTier = () => {
    const tier = normalizeSubscriptionTier(state.userSubscriptionTier);
    const config =
      state.remoteFlags?.aiUsageTiers?.[tier] && typeof state.remoteFlags.aiUsageTiers[tier] === "object"
        ? state.remoteFlags.aiUsageTiers[tier]
        : AI_USAGE_TIER_DEFAULTS[tier] || AI_USAGE_TIER_DEFAULTS.free;
    const raw = Number(config.workspace_limit ?? config.workspaceLimit ?? 0);
    return Number.isFinite(raw) ? Math.max(0, Math.floor(raw)) : 0;
  };

  const setPurchaseState = (user) => {
    const accountAuthed = hasFullAccount(user);
    const sessionAuthed = hasSessionUser(user);
    const guestSession = isAnonymousUser(user);
    const nativeIapRuntime = isNativeIapRuntime();
    ui.purchasePanels.forEach((panel) => {
      const button = panel.querySelector('[data-action="purchase"]');
      const note = panel.querySelector(".purchase-note");
      const success = panel.querySelector(".purchase-success");
      const stripe = panel.querySelector('[data-action="stripe"]');
      if (!button || !note) return;

      if (accountAuthed || (nativeIapRuntime && sessionAuthed)) {
        button.disabled = false;
        button.textContent = accountAuthed
          ? button.dataset.labelAuth || "Choose plan"
          : "Start as guest";
        note.textContent =
          accountAuthed
            ? "Subscriptions activate in your dashboard after payment confirmation."
            : "Guest checkout is enabled in native app. Purchases can be merged after sign-in.";
      } else {
        button.disabled = true;
        button.textContent = button.dataset.labelGuest || "Sign in to purchase";
        note.textContent =
          nativeIapRuntime
            ? "Initializing guest checkout session..."
            : "You must sign in to purchase. Checkout is secured to your account.";
        stripe?.classList.add("hidden");
        success?.classList.add("hidden");
      }
    });
  };

  const setAuthUi = (user) => {
    const accountAuthed = hasFullAccount(user);
    const sessionAuthed = Boolean(user);
    const guestSession = isAnonymousUser(user);
    const authLabel = accountAuthed ? "Logged In" : guestSession ? "Guest Session" : "Logged Out";
    ensureProfileFeedbackButtons();
    ensureHeaderNotificationsCta();
    if (ui.headerAuth) {
      ui.headerAuth.classList.add("icon-only");
      ui.headerAuth.innerHTML = accountAuthed
        ? `${icon("dashboard")}`
        : `${icon("log-in")}`;
      ui.headerAuth.setAttribute("title", accountAuthed ? "Dashboard" : "Sign in");
      ui.headerAuth.setAttribute("aria-label", accountAuthed ? "Open dashboard" : "Sign in");
      if (ui.headerAuth.tagName.toLowerCase() === "button") {
        ui.headerAuth.dataset.route = accountAuthed ? "/dashboard" : "/account";
      } else {
        ui.headerAuth.setAttribute("href", accountAuthed ? "/dashboard" : "/account");
      }
    }

    if (ui.headerUserEmail) {
      ui.headerUserEmail.textContent = "";
      ui.headerUserEmail.classList.add("hidden");
      ui.headerUserEmail.setAttribute("aria-hidden", "true");
    }
    if (ui.headerUserStatus) {
      ui.headerUserStatus.textContent = authLabel;
      ui.headerUserStatus.classList.toggle("pill", true);
    }

    if (ui.userEmail) ui.userEmail.textContent = accountAuthed ? user?.email || "Not signed in" : guestSession ? "Guest session" : "Not signed in";
    if (ui.userProvider) ui.userProvider.textContent = accountAuthed ? user?.providerData?.[0]?.providerId || "—" : guestSession ? "anonymous" : "—";
    if (ui.userStatus) {
      ui.userStatus.textContent = authLabel;
      ui.userStatus.classList.toggle("pill", true);
    }
    if (ui.billingPortalLink) {
      const nativeBilling = isNativeIosStoreKitCheckoutOnly() || isNativeAndroidPlayBillingCheckout();
      ui.billingPortalLink.textContent = (accountAuthed || (nativeBilling && guestSession))
        ? nativeBilling
          ? nativeBillingPortalLabel()
          : "Open billing portal"
        : "Sign in to manage billing";
      ui.billingPortalLink.setAttribute("href", (accountAuthed || (nativeBilling && guestSession)) ? "#" : "/account");
      ui.billingPortalLink.setAttribute("target", "_self");
      ui.billingPortalLink.removeAttribute("rel");
    }
    ui.dashboardCta?.classList.toggle("hidden", accountAuthed);
    setProfileFormEnabled(accountAuthed);
    if (!accountAuthed) {
      setProfileStatus("Sign in to set your public profile.");
    }

    if (ui.pricingAuthCta) {
      ui.pricingAuthCta.innerHTML = accountAuthed
        ? `${icon("dashboard")}<span>Open dashboard</span>`
        : `${icon("log-in")}<span>Sign in</span>`;
      ui.pricingAuthCta.setAttribute("href", accountAuthed ? "/dashboard" : "/account");
    }

    if (ui.pricingStarterCta) {
      ui.pricingStarterCta.innerHTML = accountAuthed
        ? `${icon("dashboard")}<span>Go to dashboard</span>`
        : `${icon("user-plus")}<span>Start free</span>`;
      ui.pricingStarterCta.setAttribute("href", accountAuthed ? "/dashboard" : "/account");
    }

    if (ui.dashboardAuthLink) {
      ui.dashboardAuthLink.innerHTML = sessionAuthed
        ? `${icon("log-out")}<span>Sign out</span>`
        : `${icon("log-in")}<span>Sign in</span>`;
      ui.dashboardAuthLink.setAttribute("href", sessionAuthed ? "#" : "/account");
      ui.dashboardAuthLink.setAttribute("aria-label", sessionAuthed ? "Sign out" : "Sign in");
    }

    setPurchaseState(user);
    applyAdFreeExperience();
    setAdminOnlyFeaturePanels(user);
    applyUiTranslations(state.preferredLanguage || "en");
  };

  const setAdminOnlyFeaturePanels = (user = state.user) => {
    const allowAdminTools = hasFullAccount(user) && isAdminUser(user);
    ui.uploadsAdminBlock?.classList.toggle("hidden", !allowAdminTools);
    ui.autopilotAdminBlock?.classList.toggle("hidden", !allowAdminTools);
    ui.uploadsVoteBlock?.classList.toggle("hidden", allowAdminTools);
    ui.autopilotVoteBlock?.classList.toggle("hidden", allowAdminTools);
    if (!allowAdminTools) {
      if (ui.predictionsStatus) ui.predictionsStatus.textContent = "Admin-only capability.";
      if (ui.autopilotStatus) ui.autopilotStatus.textContent = "Admin-only capability.";
    }
  };

  const setFeatureVoteStatus = (node, message, variant = "muted") => {
    if (!node) return;
    node.textContent = String(message || "");
    node.dataset.variant = String(variant || "muted");
  };

  const bindFeatureVoteForm = (functions, form, statusNode) => {
    if (!functions || !form || form.dataset.bound === "1") return;
    const voteInput = form.querySelector('input[name="vote"]');
    const voteButtons = Array.from(form.querySelectorAll('[data-action="feature-vote-select"]'));
    const applySelection = (nextVote) => {
      const normalized = String(nextVote || "").trim().toLowerCase();
      if (voteInput) voteInput.value = normalized;
      voteButtons.forEach((btn) => {
        btn.classList.toggle("active", String(btn.dataset.vote || "").trim().toLowerCase() === normalized);
      });
    };

    voteButtons.forEach((button) => {
      button.addEventListener("click", () => {
        applySelection(button.dataset.vote);
      });
    });

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!requireFullAccount("Sign in to submit feature votes.", { redirect: true })) return;
      const featureKey = String(form.dataset.featureKey || "").trim().toLowerCase();
      if (!FEATURE_VOTE_KEYS.has(featureKey)) {
        setFeatureVoteStatus(statusNode, "Feature key is invalid.", "warn");
        return;
      }
      const vote = String(voteInput?.value || "").trim().toLowerCase();
      if (vote !== "yes" && vote !== "no") {
        setFeatureVoteStatus(statusNode, "Pick Yes or No first.", "warn");
        return;
      }
      const feedbackInput = form.querySelector('textarea[name="feedback"]');
      const feedback = String(feedbackInput?.value || "").trim().slice(0, 2000);

      setFeatureVoteStatus(statusNode, "Submitting vote...");
      const submitVote = functions.httpsCallable("submit_feature_vote");
      try {
        const result = await submitVote({
          featureKey,
          vote,
          feedback,
          meta: buildMeta(),
        });
        const totals = result.data?.totals && typeof result.data.totals === "object" ? result.data.totals : {};
        const yes = Number(totals.yes || 0);
        const no = Number(totals.no || 0);
        const total = Number(totals.total || yes + no);
        setFeatureVoteStatus(statusNode, `Vote saved. Running total: Yes ${yes}, No ${no} (${total} vote${total === 1 ? "" : "s"}).`, "success");
        applySelection(vote);
        showToast("Vote submitted.");
        logEvent("feature_vote_submitted", { feature_key: featureKey, vote });
        if (isAdminUser()) {
          loadAdminFeatureVoteSummary(functions).catch(() => {});
        }
      } catch (error) {
        setFeatureVoteStatus(statusNode, extractErrorMessage(error, "Unable to submit vote."), "warn");
        showToast(extractErrorMessage(error, "Unable to submit vote."), "warn");
      }
    });

    form.dataset.bound = "1";
  };

  const bindFeatureVoteForms = (functions) => {
    bindFeatureVoteForm(functions, ui.featureVoteUploadsForm, ui.featureVoteUploadsStatus);
    bindFeatureVoteForm(functions, ui.featureVoteAutopilotForm, ui.featureVoteAutopilotStatus);
  };

  const renderAdminFeatureVoteSummary = (payload = {}) => {
    if (!ui.adminFeatureVoteResults) return;
    const rawFeatures = payload && typeof payload.features === "object" ? payload.features : {};
    const cards = Array.from(FEATURE_VOTE_KEYS).map((featureKey) => {
      const item = rawFeatures[featureKey] && typeof rawFeatures[featureKey] === "object" ? rawFeatures[featureKey] : {};
      const yes = Math.max(0, Number(item.yes || 0));
      const no = Math.max(0, Number(item.no || 0));
      const total = Math.max(0, Number(item.total || yes + no));
      const yesPercent = total > 0 ? Math.round((yes / total) * 100) : 0;
      const updatedAt = item.updatedAt ? formatTimestamp(item.updatedAt) : "—";
      return `
        <div class="order-card">
          <div class="order-header">
            <div class="order-title">${escapeHtml(FEATURE_VOTE_LABELS[featureKey] || featureKey)}</div>
            <span class="status completed">${yesPercent}% yes</span>
          </div>
          <div class="order-meta">
            <div><strong>Total votes</strong> ${total}</div>
            <div><strong>Yes</strong> ${yes}</div>
            <div><strong>No</strong> ${no}</div>
            <div><strong>Updated</strong> ${escapeHtml(updatedAt)}</div>
          </div>
        </div>
      `;
    });

    const recentItems = Array.isArray(payload.recent) ? payload.recent : [];
    const recentMarkup = recentItems.length
      ? recentItems
          .map((entry) => {
            const featureKey = String(entry.featureKey || "").trim().toLowerCase();
            const label = FEATURE_VOTE_LABELS[featureKey] || featureKey || "feature";
            const vote = String(entry.vote || "").trim().toLowerCase();
            const voteLabel = vote === "yes" ? "Yes" : vote === "no" ? "No" : "—";
            const when = entry.updatedAt ? formatTimestamp(entry.updatedAt) : "—";
            const who = String(entry.userEmail || "unknown");
            const feedback = String(entry.feedback || "").trim();
            return `
              <div class="order-card">
                <div class="order-header">
                  <div class="order-title">${escapeHtml(label)}</div>
                  <span class="status ${vote === "yes" ? "completed" : "pending"}">${escapeHtml(voteLabel)}</span>
                </div>
                <div class="order-meta">
                  <div><strong>User</strong> ${escapeHtml(who)}</div>
                  <div><strong>Updated</strong> ${escapeHtml(when)}</div>
                  ${feedback ? `<div><strong>Feedback</strong> ${escapeHtml(feedback)}</div>` : "<div><strong>Feedback</strong> —</div>"}
                </div>
              </div>
            `;
          })
          .join("")
      : `<div class="small muted">No feature votes yet.</div>`;

    ui.adminFeatureVoteResults.innerHTML = `
      <div class="feature-vote-admin-grid">
        ${cards.join("")}
      </div>
      <div class="feature-vote-recent">
        <h3>Recent feedback</h3>
        ${recentMarkup}
      </div>
    `;
  };

  const loadAdminFeatureVoteSummary = async (functions) => {
    if (!ui.adminFeatureVoteResults || !functions) return;
    if (!hasFullAccount() || !isAdminUser()) {
      ui.adminFeatureVoteResults.innerHTML = `<div class="small muted">Admin access required.</div>`;
      return;
    }
    const getSummary = functions.httpsCallable("get_feature_vote_summary");
    const result = await getSummary({ limit: 25, meta: buildMeta() });
    renderAdminFeatureVoteSummary(result.data || {});
  };

  const renderFiscaldataCapabilitiesAudit = (payload = null) => {
    if (!ui.adminFiscaldataCapabilities) return;
    const rows = Array.isArray(payload?.results) ? payload.results : [];
    if (!rows.length) {
      ui.adminFiscaldataCapabilities.innerHTML = `<div class="small muted">No Fiscal Data endpoint checks have run yet.</div>`;
      if (ui.adminFiscaldataCapabilitiesStatus) {
        ui.adminFiscaldataCapabilitiesStatus.textContent = "Endpoint check returned no rows.";
      }
      return;
    }

    ui.adminFiscaldataCapabilities.innerHTML = rows
      .map((row) => {
        const status = String(row?.status || "error").toLowerCase();
        const statusClass = status === "available" ? "completed" : status === "warning" ? "pending" : "cancelled";
        const detail = String(row?.detail || "").trim();
        return `
          <div class="order-card">
            <div class="order-header">
              <div class="order-title">${escapeHtml(String(row?.title || row?.id || "Fiscal endpoint"))}</div>
              <span class="status ${statusClass}">${escapeHtml(status.toUpperCase())}</span>
            </div>
            <div class="order-meta">
              <div><strong>Endpoint</strong> ${escapeHtml(String(row?.endpoint || "—"))}</div>
              <div><strong>Category</strong> ${escapeHtml(String(row?.category || "—"))}</div>
              <div><strong>Rows</strong> ${Number.isFinite(Number(row?.count)) ? Number(row?.count) : "—"}</div>
              <div><strong>Detail</strong> ${escapeHtml(detail || "—")}</div>
            </div>
          </div>
        `;
      })
      .join("");

    if (ui.adminFiscaldataCapabilitiesStatus) {
      const generated = payload?.generatedAt ? formatTimestamp(payload.generatedAt) : "—";
      const fromCache = payload?.fromCache ? " (cached)" : "";
      ui.adminFiscaldataCapabilitiesStatus.textContent = `Last Fiscal Data endpoint check: ${generated}${fromCache}.`;
    }
  };

  const loadFiscaldataRegistry = async ({ force = false } = {}) => {
    const now = Date.now();
    if (!force && Array.isArray(state.fiscaldataRegistry) && state.fiscaldataRegistry.length && now - Number(state.fiscaldataRegistryLoadedAt || 0) < 5 * 60 * 1000) {
      return state.fiscaldataRegistry;
    }
    const headers = await buildApiAuthHeaders();
    const response = await fetch("/api/fiscaldata/registry", {
      method: "GET",
      headers,
      credentials: "same-origin",
    });
    if (!response.ok) throw new Error("Unable to load Fiscal Data registry.");
    const payload = await response.json().catch(() => ({}));
    const endpoints = Array.isArray(payload?.endpoints) ? payload.endpoints : [];
    state.fiscaldataRegistry = endpoints;
    state.fiscaldataRegistryLoadedAt = now;
    return endpoints;
  };

  const loadFiscaldataCapabilities = async ({ force = false } = {}) => {
    const now = Date.now();
    if (!force && state.fiscaldataCapabilities && now - Number(state.fiscaldataCapabilitiesLoadedAt || 0) < 5 * 60 * 1000) {
      renderFiscaldataCapabilitiesAudit({
        generatedAt: new Date(state.fiscaldataCapabilitiesLoadedAt).toISOString(),
        fromCache: true,
        results: state.fiscaldataCapabilities,
      });
      return state.fiscaldataCapabilities;
    }
    if (ui.adminFiscaldataCapabilitiesStatus) {
      ui.adminFiscaldataCapabilitiesStatus.textContent = "Running Fiscal Data endpoint check...";
    }
    const endpoints = await loadFiscaldataRegistry({ force });
    const checks = await Promise.all(
      endpoints.map(async (entry) => {
        const endpoint = String(entry?.endpoint || "").trim();
        const params = new URLSearchParams({
          endpoint,
          format: "json",
          "page[number]": "1",
          "page[size]": "1",
        });
        if (Array.isArray(entry?.defaultQuery?.fields) && entry.defaultQuery.fields.length) {
          params.set("fields", entry.defaultQuery.fields.join(","));
        }
        try {
          const headers = await buildApiAuthHeaders();
          const response = await fetch(`/api/fiscaldata?${params.toString()}`, {
            method: "GET",
            headers,
            credentials: "same-origin",
          });
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) {
            return {
              id: entry?.id,
              title: entry?.title,
              category: entry?.category,
              endpoint,
              status: "error",
              detail: String(payload?.detail || payload?.error || `HTTP ${response.status}`),
              count: 0,
            };
          }
          const count = Number(payload?.meta?.count || payload?.data?.length || 0);
          return {
            id: entry?.id,
            title: entry?.title,
            category: entry?.category,
            endpoint,
            status: "available",
            detail: String(payload?.links?.self || "OK"),
            count,
          };
        } catch (error) {
          return {
            id: entry?.id,
            title: entry?.title,
            category: entry?.category,
            endpoint,
            status: "error",
            detail: String(error?.message || "request_failed"),
            count: 0,
          };
        }
      })
    );

    state.fiscaldataCapabilities = checks;
    state.fiscaldataCapabilitiesLoadedAt = now;
    renderFiscaldataCapabilitiesAudit({
      generatedAt: new Date(now).toISOString(),
      fromCache: false,
      results: checks,
    });
    return checks;
  };

  const fetchFiscaldataApi = async ({
    endpoint,
    fields = [],
    filter = "",
    sort = [],
    pageNumber = 1,
    pageSize = 100,
  }) => {
    const cleanEndpoint = String(endpoint || "").trim();
    if (!cleanEndpoint) throw new Error("Fiscal Data endpoint is required.");
    const params = new URLSearchParams();
    params.set("endpoint", cleanEndpoint);
    params.set("format", "json");
    params.set("page[number]", String(Math.max(1, Number(pageNumber || 1))));
    params.set("page[size]", String(Math.max(1, Math.min(5000, Number(pageSize || 100)))));
    if (Array.isArray(fields) && fields.length) params.set("fields", fields.join(","));
    if (Array.isArray(sort) && sort.length) params.set("sort", sort.join(","));
    if (String(filter || "").trim()) params.set("filter", String(filter || "").trim());

    const headers = await buildApiAuthHeaders();
    const response = await fetch(`/api/fiscaldata?${params.toString()}`, {
      method: "GET",
      headers,
      credentials: "same-origin",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = String(payload?.detail || payload?.error || `HTTP ${response.status}`).trim();
      throw new Error(detail || "Fiscal Data request failed.");
    }
    return payload;
  };

  const buildMiniLineSvg = (values = []) => {
    const points = (Array.isArray(values) ? values : [])
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
    if (points.length < 2) return "";
    const min = Math.min(...points);
    const max = Math.max(...points);
    const span = max - min || 1;
    const width = 220;
    const height = 48;
    const mapped = points
      .map((value, idx) => {
        const x = (idx / (points.length - 1)) * width;
        const y = height - ((value - min) / span) * height;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .join(" ");
    return `
      <svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" role="img" aria-label="Series trend">
        <polyline fill="none" stroke="currentColor" stroke-width="2" points="${mapped}" />
      </svg>
    `;
  };

  const inferFiscalFieldKind = (field, dataType) => {
    const fieldName = String(field || "").toLowerCase();
    const type = String(dataType || "").toLowerCase();
    if (type.includes("date") || fieldName.endsWith("_date")) return "date";
    if (type.includes("currency") || fieldName.includes("amount") || fieldName.includes("balance")) return "currency";
    if (type.includes("percent") || fieldName.includes("percent") || fieldName.includes("rate")) return "percent";
    if (type.includes("number") || type.includes("integer") || type.includes("float") || type.includes("double")) return "number";
    return "string";
  };

  const normalizeFiscalCellValue = (value, kind) => {
    if (value == null) return null;
    if (typeof value === "string" && ["", "null", "undefined", "na", "n/a", "-"].includes(value.trim().toLowerCase())) return null;
    if (kind === "number" || kind === "currency" || kind === "percent") {
      const parsed = Number(String(value).replace(/,/g, "").trim());
      return Number.isFinite(parsed) ? parsed : null;
    }
    if (kind === "date") {
      const parsed = Date.parse(String(value).trim());
      return Number.isFinite(parsed) ? new Date(parsed).toISOString() : String(value);
    }
    return String(value);
  };

  const formatFiscalCellValue = (value, kind) => {
    if (value == null || value === "") return "—";
    if (kind === "date") {
      const parsed = Date.parse(String(value));
      return Number.isFinite(parsed) ? new Date(parsed).toLocaleDateString() : String(value);
    }
    if (kind === "currency") {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      return new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(numeric);
    }
    if (kind === "percent") {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }).format(numeric)}%`;
    }
    if (kind === "number") {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "—";
      return new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }).format(numeric);
    }
    return String(value);
  };

  const chooseFiscalDisplayColumns = (meta, rows, preferredOrder = []) => {
    const labels = meta && typeof meta === "object" && meta.labels && typeof meta.labels === "object" ? meta.labels : {};
    const candidates = Object.keys(labels).length ? Object.keys(labels) : (Array.isArray(rows) && rows[0] ? Object.keys(rows[0]) : []);
    const order = [];
    const seen = new Set();
    [...preferredOrder, ...FISCALDATA_DEFAULT_PREFERRED_COLUMNS].forEach((field) => {
      if (!candidates.includes(field) || seen.has(field)) return;
      seen.add(field);
      order.push(field);
    });
    candidates.forEach((field) => {
      if (seen.has(field)) return;
      seen.add(field);
      order.push(field);
    });
    return order.slice(0, 8);
  };

  const renderFiscalMacroTable = (payload, preferredOrder = []) => {
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    if (!rows.length) return `<div class="small muted">No rows returned for this query.</div>`;
    const meta = payload?.meta && typeof payload.meta === "object" ? payload.meta : {};
    const labels = meta.labels && typeof meta.labels === "object" ? meta.labels : {};
    const dataTypes = meta.dataTypes && typeof meta.dataTypes === "object" ? meta.dataTypes : {};
    const columns = chooseFiscalDisplayColumns(meta, rows, preferredOrder);
    if (!columns.length) return `<div class="small muted">No displayable columns.</div>`;

    const normalized = rows.slice(0, 20).map((row) => {
      const source = row && typeof row === "object" ? row : {};
      const next = {};
      columns.forEach((field) => {
        const kind = inferFiscalFieldKind(field, dataTypes[field]);
        next[field] = normalizeFiscalCellValue(source[field], kind);
      });
      return next;
    });

    const seriesValues = normalized
      .map((row) => {
        const candidateField = columns.find((field) => {
          const kind = inferFiscalFieldKind(field, dataTypes[field]);
          return kind === "number" || kind === "currency" || kind === "percent";
        });
        if (!candidateField) return null;
        const numeric = Number(row[candidateField]);
        return Number.isFinite(numeric) ? numeric : null;
      })
      .filter((value) => value != null);
    const sparkline = seriesValues.length >= 2 ? buildMiniLineSvg(seriesValues.reverse()) : "";

    const head = columns
      .map((field) => `<th>${escapeHtml(String(labels[field] || field).trim())}</th>`)
      .join("");
    const body = normalized
      .map((row) => {
        const cells = columns
          .map((field) => {
            const kind = inferFiscalFieldKind(field, dataTypes[field]);
            return `<td>${escapeHtml(formatFiscalCellValue(row[field], kind))}</td>`;
          })
          .join("");
        return `<tr>${cells}</tr>`;
      })
      .join("");

    return `
      ${sparkline}
      <div style="overflow:auto; margin-top:8px;">
        <table class="insider-table">
          <thead><tr>${head}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    `;
  };

  const buildFiscalCardQueryParams = (entry, pageNumber) => {
    const defaultQuery = entry?.defaultQuery && typeof entry.defaultQuery === "object" ? entry.defaultQuery : {};
    const fields = Array.isArray(defaultQuery.fields) ? defaultQuery.fields : [];
    const sort = Array.isArray(defaultQuery.sort) ? defaultQuery.sort : [];
    const filter = String(defaultQuery.filter || "").trim();
    const pageSize = Number(defaultQuery?.page?.size || 100);
    const nextPageNumber = Math.max(1, Number(pageNumber || defaultQuery?.page?.number || 1));
    return { fields, sort, filter, pageSize, pageNumber: nextPageNumber };
  };

  const renderFiscalMacroDetailsModal = (entry, cardState) => {
    const modal = document.getElementById("fiscaldata-macro-details");
    if (!modal) return;
    const payload = cardState?.payload || {};
    const query = cardState?.query || {};
    const endpoint = String(entry?.endpoint || "");
    const queryString = new URLSearchParams({
      endpoint,
      format: "json",
      "page[number]": String(query.pageNumber || 1),
      "page[size]": String(query.pageSize || 100),
      ...(Array.isArray(query.fields) && query.fields.length ? { fields: query.fields.join(",") } : {}),
      ...(Array.isArray(query.sort) && query.sort.length ? { sort: query.sort.join(",") } : {}),
      ...(String(query.filter || "").trim() ? { filter: String(query.filter || "").trim() } : {}),
    }).toString();

    modal.innerHTML = `
      <div class="modal-backdrop" data-fiscaldata-close></div>
      <div class="modal-dialog card" style="max-width: 980px; width: calc(100% - 24px); max-height: calc(100vh - 40px); overflow:auto;">
        <button class="modal-close" type="button" data-fiscaldata-close aria-label="Close details">×</button>
        <h3>${escapeHtml(String(entry?.title || "Fiscal Data details"))}</h3>
        <p class="small muted">Endpoint: <code>${escapeHtml(endpoint)}</code></p>
        <p class="small muted">Query: <code>${escapeHtml(queryString)}</code></p>
        <div>${renderFiscalMacroTable(payload, [entry?.ui?.primaryDateField, entry?.ui?.primaryValueField].filter(Boolean))}</div>
      </div>
    `;
    modal.classList.remove("hidden");
  };

  const renderFiscalMacroDashboard = () => {
    if (!ui.macroDashboardGroups) return;
    const entries = Array.isArray(state.fiscaldataRegistry) ? state.fiscaldataRegistry : [];
    if (!entries.length) {
      ui.macroDashboardGroups.innerHTML = `<div class="small muted">No macro cards configured.</div>`;
      return;
    }

    const grouped = new Map();
    entries.forEach((entry) => {
      const category = String(entry?.category || "Other");
      if (!grouped.has(category)) grouped.set(category, []);
      grouped.get(category).push(entry);
    });

    const blocks = Array.from(grouped.entries()).map(([category, cards]) => {
      const cardMarkup = cards
        .map((entry) => {
          const cardState = state.fiscaldataMacroPages?.[entry.id] || {};
          const payload = cardState.payload || {};
          const rows = Array.isArray(payload?.data) ? payload.data : [];
          const meta = payload?.meta && typeof payload.meta === "object" ? payload.meta : {};
          const count = Number(meta?.count || rows.length || 0);
          const totalPages = Number(meta?.totalPages || 1);
          const pageNumber = Number(cardState.pageNumber || 1);
          const hasNextLink = Boolean(String(payload?.links?.next || "").trim());
          const canLoadMore = hasNextLink || totalPages > pageNumber;
          const frequency = String(entry?.updateCadence || "periodic").trim();
          const table = rows.length
            ? renderFiscalMacroTable(payload, [entry?.ui?.primaryDateField, entry?.ui?.primaryValueField].filter(Boolean))
            : `<div class="small muted">${escapeHtml(String(cardState.error || "Loading..."))}</div>`;
          return `
            <article class="card" data-fiscaldata-card-id="${escapeHtml(entry.id)}">
              <div class="order-header">
                <div class="order-title">${escapeHtml(String(entry?.title || entry?.id || "Macro card"))}</div>
                <span class="status pending">${escapeHtml(frequency)}</span>
              </div>
              <div class="small muted" style="margin-top:6px;">${escapeHtml(String(entry?.endpoint || ""))}</div>
              <div style="margin-top:10px;">${table}</div>
              <div class="hero-actions" style="margin-top:12px;">
                <button class="cta secondary small" type="button" data-fiscaldata-view-details="${escapeHtml(entry.id)}">View details</button>
                ${
                  canLoadMore
                    ? `<button class="cta secondary small" type="button" data-fiscaldata-load-more="${escapeHtml(entry.id)}">Load more</button>`
                    : ""
                }
              </div>
              <div class="small muted" style="margin-top:8px;">Rows: ${Number.isFinite(count) ? count : rows.length} · Page ${pageNumber}${totalPages > 1 ? ` of ${totalPages}` : ""}</div>
            </article>
          `;
        })
        .join("");
      return `
        <section style="margin-bottom:18px;">
          <h3 style="margin-bottom:10px;">${escapeHtml(String(category))}</h3>
          <div class="content-grid">${cardMarkup}</div>
        </section>
      `;
    });

    ui.macroDashboardGroups.innerHTML = blocks.join("");
  };

  const loadFiscalMacroCard = async (entry, { pageNumber = 1, append = false } = {}) => {
    const query = buildFiscalCardQueryParams(entry, pageNumber);
    const cardId = String(entry?.id || "").trim();
    if (!cardId) return;
    try {
      const payload = await fetchFiscaldataApi({
        endpoint: entry.endpoint,
        fields: query.fields,
        filter: query.filter,
        sort: query.sort,
        pageNumber: query.pageNumber,
        pageSize: query.pageSize,
      });
      const previous = append ? state.fiscaldataMacroPages?.[cardId]?.payload : null;
      const mergedRows = append
        ? [...(Array.isArray(previous?.data) ? previous.data : []), ...(Array.isArray(payload?.data) ? payload.data : [])]
        : (Array.isArray(payload?.data) ? payload.data : []);
      let nextPageNumber = null;
      const nextHref = String(payload?.links?.next || "").trim();
      if (nextHref) {
        try {
          const nextUrl = new URL(nextHref, window.location.origin);
          const parsedPage = Number(nextUrl.searchParams.get("page[number]") || "");
          if (Number.isFinite(parsedPage) && parsedPage > 0) {
            nextPageNumber = Math.floor(parsedPage);
          }
        } catch (error) {
          nextPageNumber = null;
        }
      }
      const nextPayload = {
        ...payload,
        data: mergedRows,
      };
      state.fiscaldataMacroPages[cardId] = {
        payload: nextPayload,
        pageNumber: query.pageNumber,
        nextPageNumber,
        query,
        error: "",
      };
    } catch (error) {
      state.fiscaldataMacroPages[cardId] = {
        ...(state.fiscaldataMacroPages?.[cardId] || {}),
        error: extractErrorMessage(error, "Unable to load macro card."),
      };
    }
  };

  const loadFiscalMacroDashboard = async ({ force = false } = {}) => {
    if (!ui.macroDashboardStatus || !ui.macroDashboardGroups) return;
    ui.macroDashboardStatus.textContent = "Loading Fiscal Data macro cards...";
    logEvent("macro_dashboard_load_started", { source: "fiscaldata" });
    try {
      const entries = await loadFiscaldataRegistry({ force });
      await Promise.all(entries.map((entry) => loadFiscalMacroCard(entry, { pageNumber: 1, append: false })));
      renderFiscalMacroDashboard();
      ui.macroDashboardStatus.textContent = `Loaded ${entries.length} Fiscal Data cards.`;
      logEvent("macro_dashboard_loaded", { source: "fiscaldata", cards: entries.length });
    } catch (error) {
      ui.macroDashboardStatus.textContent = extractErrorMessage(error, "Macro dashboard is unavailable.");
      ui.macroDashboardGroups.innerHTML = `<div class="small muted">${escapeHtml(
        extractErrorMessage(error, "Macro dashboard is unavailable.")
      )}</div>`;
      logEvent("macro_dashboard_error", {
        message: String(error?.message || "load_failed").slice(0, 120),
      });
    }
  };

  const setFeatureVoteSummaryPolling = (functions, enabled) => {
    if (state.featureVoteSummaryTimer) {
      window.clearInterval(state.featureVoteSummaryTimer);
      state.featureVoteSummaryTimer = null;
    }
    if (!enabled || !ui.adminFeatureVoteResults || !functions) return;
    loadAdminFeatureVoteSummary(functions).catch((error) => {
      ui.adminFeatureVoteResults.innerHTML = `<div class="small muted">${escapeHtml(
        extractErrorMessage(error, "Unable to load feature vote summary.")
      )}</div>`;
    });
    state.featureVoteSummaryTimer = window.setInterval(() => {
      loadAdminFeatureVoteSummary(functions).catch(() => {});
    }, 60000);
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

  const renderOrderStatusBadge = (rawStatus) => {
    const status = String(rawStatus || "pending").trim().toLowerCase();
    const statusLabel = status.replace(/_/g, " ");
    if (status === "cancelled") {
      return `
        <span class="status ${escapeHtml(status)} status-icon-only" aria-label="Cancelled">
          <span class="status-icon status-icon-cancelled" aria-hidden="true">${icon("cancel")}</span>
        </span>
      `;
    }
    if (status === "completed" || status === "fulfilled") {
      return `
        <span class="status ${escapeHtml(status)} status-icon-only" aria-label="Completed">
          <span class="status-icon status-icon-completed" aria-hidden="true">${icon("check-circle")}</span>
        </span>
      `;
    }
    return `<span class="status ${escapeHtml(status)}">${escapeHtml(statusLabel)}</span>`;
  };

  const renderOrderList = (orders, container, opts = {}) => {
    if (!container) return;
    container.innerHTML = "";
    if (!orders.length) {
      const emptyMessage = String(opts.emptyMessage || "No subscription orders yet.");
      container.innerHTML = `<p class="small">${escapeHtml(emptyMessage)}</p>`;
      return;
    }

    orders.forEach((order) => {
      const card = document.createElement("div");
      card.className = "order-card";
      card.dataset.orderId = order.id;

      const status = order.status || "pending";
      const paymentStatus = String(order.paymentStatus || "unpaid");
      const paymentLabel = paymentStatus.replace(/_/g, " ");
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
            <button class="cta small update-status" type="button">${icon("check-circle")}<span>Update status</span></button>
          </div>
          <div class="upload-row">
            <input class="file-input" type="file" />
            <button class="cta secondary small upload-file" type="button">${icon("upload")}<span>Upload file</span></button>
          </div>
        `
        : "";

      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">${order.product || "Quantura Subscription"}</div>
            <div class="small">Order ID: ${order.id}</div>
          </div>
          ${renderOrderStatusBadge(status)}
        </div>
        <div class="order-meta">
          <div><strong>Requested</strong> ${formatTimestamp(order.createdAt)}</div>
          <div><strong>Price</strong> $${order.price || 349} ${order.currency || "USD"}</div>
          <div><strong>Payment</strong> ${escapeHtml(paymentLabel)}</div>
          ${opts.admin ? `<div><strong>Client</strong> ${order.userEmail || "—"}</div>` : ""}
          ${opts.admin && order.stripeCheckoutSessionId ? `<div><strong>Stripe session</strong> ${escapeHtml(order.stripeCheckoutSessionId)}</div>` : ""}
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
      const isAutopilot = Boolean(!isForecast && (item.horizon || item.quantiles || item.interval));
      const isUpload = Boolean(!isForecast && !isAutopilot && (item.filePath || item.fileUrl));
      const metrics = item.metrics && typeof item.metrics === "object" ? item.metrics : {};
      const forecastMeta = isForecast
        ? `
          <div><strong>Ticker</strong> ${item.ticker}</div>
          <div><strong>Service</strong> ${item.service}</div>
          <div><strong>Engine</strong> ${item.engine || "—"}</div>
          ${metrics.lastClose ? `<div><strong>Last close</strong> ${escapeHtml(metrics.lastClose)}</div>` : ""}
          ${metrics.medianEnd ? `<div><strong>Median end</strong> ${escapeHtml(metrics.medianEnd)}</div>` : ""}
          ${metrics.mae ? `<div><strong>MAE</strong> ${escapeHtml(metrics.mae)}</div>` : ""}
          ${metrics.coverage10_90 ? `<div><strong>Coverage</strong> ${escapeHtml(metrics.coverage10_90)}</div>` : ""}
          ${item.serviceMessage ? `<div><strong>Message</strong> ${escapeHtml(item.serviceMessage)}</div>` : ""}
        `
        : "";
      const autopilotMeta = isAutopilot
        ? `
          ${item.ticker ? `<div><strong>Ticker</strong> ${escapeHtml(item.ticker)}</div>` : ""}
          ${item.interval ? `<div><strong>Interval</strong> ${escapeHtml(item.interval)}</div>` : ""}
          ${item.horizon ? `<div><strong>Horizon</strong> ${escapeHtml(item.horizon)}</div>` : ""}
          ${item.quantiles ? `<div><strong>Quantiles</strong> ${escapeHtml(item.quantiles)}</div>` : ""}
          ${item.userEmail ? `<div><strong>User</strong> ${escapeHtml(item.userEmail)}</div>` : ""}
          ${item.notes ? `<div><strong>Notes</strong> ${escapeHtml(item.notes)}</div>` : ""}
        `
        : "";
      const uploadMeta = isUpload
        ? `
          ${item.ticker ? `<div><strong>Ticker</strong> ${escapeHtml(item.ticker)}</div>` : ""}
          ${item.notes ? `<div><strong>Notes</strong> ${escapeHtml(item.notes)}</div>` : ""}
          ${item.filePath ? `<div><strong>Path</strong> ${escapeHtml(item.filePath)}</div>` : ""}
        `
        : "";

      const titleText = escapeHtml(item.title || item.ticker || "Request");
      const titleMarkup = isUpload
        ? `<button class="link-button" type="button" data-action="plot-upload" data-upload-id="${escapeHtml(item.id)}">${titleText}</button>`
        : titleText;

      const actions = isForecast
        ? `
          <div class="order-actions" style="display:flex; gap:10px; flex-wrap:wrap;">
            <button class="cta secondary small" type="button" data-action="plot-forecast" data-forecast-id="${escapeHtml(item.id)}">
              ${icon("candlestick-chart")}<span>Plot on chart</span>
            </button>
            <button class="cta secondary small" type="button" data-action="download-forecast" data-forecast-id="${escapeHtml(item.id)}">
              ${icon("download")}<span>Download CSV</span>
            </button>
            <button class="cta secondary small" type="button" data-action="share-forecast" data-forecast-id="${escapeHtml(item.id)}">
              ${icon("share-ios")}<span>Share link</span>
            </button>
            <button class="cta secondary small danger" type="button" data-action="delete-forecast" data-forecast-id="${escapeHtml(item.id)}">
              ${icon("trash")}<span>Delete</span>
            </button>
          </div>
        `
        : isAutopilot
          ? `
          <div class="order-actions" style="display:flex; gap:10px; flex-wrap:wrap;">
            <button class="cta secondary small danger" type="button" data-action="delete-autopilot" data-request-id="${escapeHtml(item.id)}">
              ${icon("trash")}<span>Delete</span>
            </button>
          </div>
        `
        : isUpload
          ? `
          <div class="order-actions" style="display:flex; gap:10px; flex-wrap:wrap;">
            <button class="cta secondary small" type="button" data-action="plot-upload" data-upload-id="${escapeHtml(item.id)}">${icon("graph-up")}<span>Plot</span></button>
            <button class="cta secondary small" type="button" data-action="download-upload" data-upload-id="${escapeHtml(item.id)}">${icon("download")}<span>Download</span></button>
            <button class="cta secondary small" type="button" data-action="rename-upload" data-upload-id="${escapeHtml(item.id)}">${icon("edit-pencil")}<span>Rename</span></button>
            <button class="cta secondary small" type="button" data-action="share-upload" data-upload-id="${escapeHtml(item.id)}">${icon("share-ios")}<span>Share link</span></button>
            <button class="cta secondary small danger" type="button" data-action="delete-upload" data-upload-id="${escapeHtml(item.id)}">${icon("trash")}<span>Delete</span></button>
          </div>
        `
          : "";
      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">${titleMarkup}</div>
            <div class="small">ID: ${item.id}</div>
          </div>
          <span class="status ${item.status || "pending"}">${item.status || "pending"}</span>
        </div>
        <div class="order-meta">
          <div><strong>Requested</strong> ${formatTimestamp(item.createdAt)}</div>
          ${item.summary ? `<div><strong>Summary</strong> ${item.summary}</div>` : ""}
          ${forecastMeta}
          ${autopilotMeta}
          ${uploadMeta}
        </div>
        ${actions}
      `;
      container.appendChild(card);
    });
  };

  const renderCollabInvites = (invites) => {
    if (!ui.collabInvitesList) return;
    state.pendingCollabInviteCount = Array.isArray(invites) ? invites.length : 0;
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
    state.collaboratorCount = Array.isArray(collaborators) ? collaborators.length : 0;
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
    if (!hasFullAccount()) return;
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
      .onSnapshot(
        (snapshot) => {
          const orders = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
          const paidOrders = orders.filter((order) => {
            const status = String(order?.paymentStatus || "").trim().toLowerCase();
            return status === "paid" || status === "succeeded" || status === "active" || status === "complete";
          });
          state.userSubscriptionTier = deriveSubscriptionTierFromOrders(paidOrders);
          state.userHasPaidPlan = normalizeSubscriptionTier(state.userSubscriptionTier) !== "free";
          renderOrderList(orders, ui.userOrders);
          applyAdFreeExperience();
          refreshScreenerModelUi();
          refreshScreenerCreditsUi();
        },
        () => {
          renderOrderList([], ui.userOrders, { emptyMessage: "Unable to load orders right now." });
        }
      );
  };

	  const startUserForecasts = (db, workspaceUserId) => {
	    if (state.unsubscribeForecasts) state.unsubscribeForecasts();
	    const containers = [ui.userForecasts, ui.savedForecastsList].filter(Boolean);
	    if (!workspaceUserId || containers.length === 0) return;

	    state.unsubscribeForecasts = db
	      .collection("forecast_requests")
	      .where("userId", "==", workspaceUserId)
	      .orderBy("createdAt", "desc")
	      .onSnapshot(
          (snapshot) => {
            const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
            containers.forEach((container) => renderRequestList(items, container, "No forecast requests yet."));
            renderForecastPicker(items);
          },
          () => {
            containers.forEach((container) => renderRequestList([], container, "Unable to load forecasts right now."));
            renderForecastPicker([]);
          }
        );
	  };

    const startScreenerRuns = (db, workspaceUserId) => {
      if (state.unsubscribeScreenerRuns) state.unsubscribeScreenerRuns();
      if (!ui.screenerLoadSelect && !ui.screenerOutput) return;
      if (!workspaceUserId || !db) return;

      state.unsubscribeScreenerRuns = db
        .collection("screener_runs")
        .where("userId", "==", workspaceUserId)
        .orderBy("createdAt", "desc")
        .limit(60)
        .onSnapshot(
          (snapshot) => {
            const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
            renderScreenerRunPicker(items);

            const params = new URLSearchParams(window.location.search);
            const urlRunId = String(params.get("screenerRunId") || params.get("runId") || "").trim();
            if (urlRunId && !state.screenerUrlRunLoaded) {
              state.screenerUrlRunLoaded = true;
              loadScreenerRunById(db, urlRunId).catch(() => {});
            }
          },
          () => {
            renderScreenerRunPicker([]);
          }
        );
    };

	  const resolveWorkspaceRole = (workspaceId) => {
	    if (!hasFullAccount() || !workspaceId) return "guest";
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
	      const notes = escapeHtml(String(task.notes || "").trim());
	      const meta = [due ? `Due ${due}` : "", assignee ? `Assignee: ${assignee}` : ""].filter(Boolean).join(" · ");
	      const actions = editable
	        ? `
	          <div class="task-actions">
              <div class="task-move-group">
	              <button class="task-chip" type="button" data-action="task-move" data-task-id="${escapeHtml(task.id)}" data-to="backlog">Backlog</button>
	              <button class="task-chip" type="button" data-action="task-move" data-task-id="${escapeHtml(task.id)}" data-to="doing">Doing</button>
	              <button class="task-chip" type="button" data-action="task-move" data-task-id="${escapeHtml(task.id)}" data-to="done">Done</button>
              </div>
	            <button class="task-chip danger" type="button" data-action="task-delete" data-task-id="${escapeHtml(task.id)}">Delete</button>
	          </div>
	        `
	        : "";
	      return `
	        <div class="task-card" draggable="${editable ? "true" : "false"}" data-task-id="${escapeHtml(task.id)}">
	          <div class="task-title">${title}</div>
	          ${meta ? `<div class="small task-meta muted">${meta}</div>` : ""}
            ${notes ? `<div class="small task-notes muted">${notes}</div>` : ""}
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
	    state.taskCalendarTasks = Array.isArray(tasks) ? tasks : [];

	    const pad2 = (num) => String(num).padStart(2, "0");
	    const toDateKey = (dt) => `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())}`;
	    const parseDateKey = (key) => {
	      const parts = String(key || "").split("-").map((p) => Number(p));
	      if (parts.length !== 3 || parts.some((n) => !Number.isFinite(n))) return null;
	      const [y, m, d] = parts;
	      if (y < 1970 || m < 1 || m > 12 || d < 1 || d > 31) return null;
	      return new Date(y, m - 1, d);
	    };

	    const ensureCursor = () => {
	      const cursor = state.taskCalendarCursor instanceof Date ? new Date(state.taskCalendarCursor) : null;
	      if (cursor && Number.isFinite(cursor.getTime())) {
	        return new Date(cursor.getFullYear(), cursor.getMonth(), 1);
	      }
	      const now = new Date();
	      const next = new Date(now.getFullYear(), now.getMonth(), 1);
	      state.taskCalendarCursor = next;
	      return next;
	    };

	    const cursor = ensureCursor();
	    const year = cursor.getFullYear();
	    const month = cursor.getMonth();

	    const monthLabel = cursor.toLocaleString(undefined, { month: "long", year: "numeric" });
	    const firstOfMonth = new Date(year, month, 1);
	    const weekday = firstOfMonth.getDay(); // 0=Sun
	    const weekStartMonday = true;
	    const offset = weekStartMonday ? (weekday + 6) % 7 : weekday;
	    const gridStart = new Date(year, month, 1 - offset);
	    const todayKey = toDateKey(new Date());

	    const tasksByDate = new Map();
	    for (const t of state.taskCalendarTasks) {
	      const dueKey = String(t?.dueDate || "").slice(0, 10);
	      if (!/^\d{4}-\d{2}-\d{2}$/.test(dueKey)) continue;
	      const list = tasksByDate.get(dueKey) || [];
	      list.push(t);
	      tasksByDate.set(dueKey, list);
	    }

	    const dow = weekStartMonday ? ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"] : ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
	    const cells = [];
	    for (let i = 0; i < 42; i += 1) {
	      const dt = new Date(gridStart);
	      dt.setDate(gridStart.getDate() + i);
	      const key = toDateKey(dt);
	      const inMonth = dt.getMonth() === month;
	      const isToday = key === todayKey;
	      const items = tasksByDate.get(key) || [];

	      const summary = items.slice(0, 2).map((task) => {
	        const title = escapeHtml(task?.title || "Untitled");
	        const status = escapeHtml(String(task?.status || "backlog"));
	        return `<div class="calendar-task" data-status="${status}">${title}</div>`;
	      });
	      const overflow = items.length > 2 ? `<div class="calendar-task calendar-task--more">+${items.length - 2} more</div>` : "";

	      cells.push(`
	        <div class="calendar-cell${inMonth ? "" : " calendar-cell--out"}${isToday ? " calendar-cell--today" : ""}">
	          <div class="calendar-cell-head">
	            <span class="calendar-day">${dt.getDate()}</span>
	            ${items.length ? `<span class="calendar-count">${items.length}</span>` : ""}
	          </div>
	          <div class="calendar-tasks">
	            ${summary.join("")}
	            ${overflow}
	          </div>
	        </div>
	      `);
	    }

	    ui.tasksCalendar.classList.remove("muted");
	    ui.tasksCalendar.innerHTML = `
	      <div class="calendar-wrap">
	        <div class="calendar-head">
	          <div class="calendar-title"><strong>${escapeHtml(monthLabel)}</strong></div>
	          <div class="calendar-nav">
	            <button class="cta secondary small" type="button" data-action="calendar-prev" aria-label="Previous month">Prev</button>
	            <button class="cta secondary small" type="button" data-action="calendar-today" aria-label="Jump to current month">Today</button>
	            <button class="cta secondary small" type="button" data-action="calendar-next" aria-label="Next month">Next</button>
	          </div>
	        </div>
	        <div class="calendar-grid" role="grid" aria-label="Task calendar">
	          ${dow.map((d) => `<div class="calendar-dow" role="columnheader">${escapeHtml(d)}</div>`).join("")}
	          ${cells.join("")}
	        </div>
	        <div class="small muted" style="margin-top:10px;">
	          Tip: add a due date in the Create task form. Tasks are visible to workspace collaborators.
	        </div>
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

  const getConfiguredVolatilityThreshold = () => {
    const raw = Number(state.remoteFlags?.volatilityThreshold);
    if (!Number.isFinite(raw)) return DEFAULT_VOLATILITY_THRESHOLD;
    return Math.max(0.01, Math.min(0.5, raw));
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
          state.recentWatchlistItems = items;
          renderWatchlist(items, workspaceId);
          ensureVolatilityAlertsForWatchlist({ db, workspaceId, items }).catch(() => {});
        },
        () => {
          if (ui.watchlistList) ui.watchlistList.innerHTML = `<div class="small muted">Unable to load watchlist.</div>`;
        }
      );
  };

  const ensureVolatilityAlertsForWatchlist = async ({ db, workspaceId, items }) => {
    if (!workspaceId || !state.user) return;
    const threshold = getConfiguredVolatilityThreshold();
    const list = Array.isArray(items) ? items : [];
    for (const item of list) {
      const ticker = normalizeTicker(item?.ticker || item?.id || "");
      if (!ticker) continue;
      const alertId = `volatility_${ticker}`;
      const ref = db.collection("users").doc(workspaceId).collection("price_alerts").doc(alertId);
      const snap = await ref.get();
      if (snap.exists) continue;
      await ref.set(
        {
          ticker,
          condition: "volatility",
          thresholdPercent: threshold,
          baselinePrice: null,
          active: true,
          status: "active",
          isDefault: true,
          createdByUid: state.user.uid,
          createdByEmail: state.user.email || "",
          createdAt: firebase.firestore.FieldValue.serverTimestamp(),
          updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
          notes: `Default volatility alert (±${Math.round(threshold * 100)}%) for followed assets.`,
          meta: buildMeta(),
        },
        { merge: true }
      );
    }
  };

  const runVolatilityAlertsCheck = async ({ db, functions, workspaceId, sendPush = true }) => {
    if (!workspaceId || !state.user) return { checked: 0, triggered: 0 };
    const querySnap = await db
      .collection("users")
      .doc(workspaceId)
      .collection("price_alerts")
      .where("condition", "==", "volatility")
      .where("active", "==", true)
      .get();
    if (querySnap.empty) return { checked: 0, triggered: 0 };

    const getHistory = functions.httpsCallable("get_ticker_history");
    const sendTestNotification = functions.httpsCallable("send_test_notification");
    let checked = 0;
    let triggered = 0;

    const today = new Date();
    const start = new Date(today);
    start.setDate(start.getDate() - 14);
    const startKey = start.toISOString().slice(0, 10);
    const endKey = today.toISOString().slice(0, 10);

    for (const doc of querySnap.docs) {
      const alertId = doc.id;
      const data = doc.data() || {};
      const ticker = normalizeTicker(data.ticker || "");
      if (!ticker) continue;
      checked += 1;

      try {
        const historyResult = await getHistory({ ticker, interval: "1d", start: startKey, end: endKey, meta: buildMeta() });
        const rows = Array.isArray(historyResult.data?.rows) ? historyResult.data.rows : [];
        const current = rows.length ? extractCloseFromHistoryRow(rows[rows.length - 1]) : null;
        if (current === null || current <= 0) continue;

        const threshold = toFiniteOrNull(data.thresholdPercent) ?? getConfiguredVolatilityThreshold();
        const baseline = toFiniteOrNull(data.baselinePrice);
        if (baseline === null || baseline <= 0) {
          await doc.ref.set(
            {
              baselinePrice: current,
              lastPrice: current,
              lastCheckedAt: firebase.firestore.FieldValue.serverTimestamp(),
              updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          );
          continue;
        }

        const change = (current - baseline) / baseline;
        const hit = Math.abs(change) >= threshold;
        await doc.ref.set(
          {
            lastPrice: current,
            percentChange: change,
            lastCheckedAt: firebase.firestore.FieldValue.serverTimestamp(),
            updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
            status: hit ? "triggered" : "active",
            triggeredAt: hit ? firebase.firestore.FieldValue.serverTimestamp() : null,
            baselinePrice: hit ? current : baseline,
          },
          { merge: true }
        );
        if (!hit) continue;

        triggered += 1;
        if (sendPush) {
          try {
            const direction = change >= 0 ? "up" : "down";
            await sendTestNotification({
              title: `Volatility alert: ${ticker}`,
              body: `${ticker} moved ${formatPercent(change * 100, { signed: true, digits: 2 })} (${direction}) from baseline.`,
              meta: buildMeta(),
            });
          } catch (error) {
            // Ignore push failures.
          }
        }
      } catch (error) {
        // Ignore per-ticker failures so one feed issue does not block the loop.
      }
    }
    return { checked, triggered };
  };

  const startVolatilityMonitor = (db, functions, workspaceId) => {
    if (state.volatilityMonitorTimer) {
      window.clearInterval(state.volatilityMonitorTimer);
      state.volatilityMonitorTimer = null;
    }
    if (!workspaceId || !state.user) return;
    state.volatilityMonitorTimer = window.setInterval(() => {
      runVolatilityAlertsCheck({ db, functions, workspaceId, sendPush: true }).catch(() => {});
    }, 15 * 60 * 1000);
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
	        const title = condition === "volatility"
	          ? `${escapeHtml(ticker)} volatility ±${Math.round((toFiniteOrNull(item.thresholdPercent) ?? getConfiguredVolatilityThreshold()) * 100)}%`
	          : `${escapeHtml(ticker)} ${condition === "below" ? "below" : "above"} ${Number.isFinite(target) ? `$${target.toFixed(2)}` : "—"}`;
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
    if (!isAdminUser(user)) {
      renderRequestList([], ui.autopilotOutput, "Autopilot is currently admin-only.");
      return;
    }

    state.unsubscribeAutopilot = db
      .collection("autopilot_requests")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot(
        (snapshot) => {
          const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
          renderRequestList(items, ui.autopilotOutput, "No autopilot requests yet.");
        },
        () => {
          renderRequestList([], ui.autopilotOutput, "Unable to load autopilot requests.");
        }
      );
  };

  const startPredictionsUploads = (db, user) => {
    if (state.unsubscribePredictions) state.unsubscribePredictions();
    if (!user || !ui.predictionsOutput) return;
    if (!isAdminUser(user)) {
      renderRequestList([], ui.predictionsOutput, "Prediction uploads are currently admin-only.");
      return;
    }

    state.unsubscribePredictions = db
      .collection("prediction_uploads")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .onSnapshot(
        (snapshot) => {
          const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
          renderRequestList(items, ui.predictionsOutput, "No uploads yet.");

          const params = new URLSearchParams(window.location.search);
          const urlUploadId = String(params.get("uploadId") || "").trim();
          if (urlUploadId && !state.uploadUrlLoaded) {
            state.uploadUrlLoaded = true;
            plotPredictionUploadById(db, state.clients?.storage, urlUploadId).catch(() => {});
          }
        },
        () => {
          renderRequestList([], ui.predictionsOutput, "Unable to load prediction uploads.");
        }
      );
  };

  const startAdminOrders = (db) => {
    if (state.unsubscribeAdmin) state.unsubscribeAdmin();
    state.unsubscribeAdmin = db
      .collection("orders")
      .orderBy("createdAt", "desc")
      .limit(100)
      .onSnapshot(
        (snapshot) => {
          const orders = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
          renderOrderList(orders, ui.adminOrders, { admin: true });
        },
        () => {
          renderOrderList([], ui.adminOrders, { admin: true, emptyMessage: "Unable to load admin orders." });
        }
      );
  };

  const startAdminAutopilotQueue = (db) => {
    if (state.unsubscribeAdminAutopilot) state.unsubscribeAdminAutopilot();
    if (!ui.adminAutopilot) return;
    state.unsubscribeAdminAutopilot = db
      .collection("autopilot_requests")
      .orderBy("createdAt", "desc")
      .limit(150)
      .onSnapshot(
        (snapshot) => {
          const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
          renderRequestList(items, ui.adminAutopilot, "No autopilot requests yet.");
        },
        () => {
          ui.adminAutopilot.textContent = "Unable to load autopilot requests.";
        }
      );
  };

  const cloneDefaultProfileSocialLinks = () => ({ ...DEFAULT_PROFILE_SOCIAL_LINKS });

  const normalizeProfileAvatar = (raw) => {
    const value = String(raw || "").trim().toLowerCase();
    if (value && PROFILE_AVATAR_OPTIONS[value]) return value;
    return "bull";
  };

  const normalizeProfileBio = (raw) => String(raw || "").trim().slice(0, 300);

  const getProfileAvatarMeta = (avatar) => {
    const key = normalizeProfileAvatar(avatar);
    return PROFILE_AVATAR_OPTIONS[key] || PROFILE_AVATAR_OPTIONS.bull;
  };

  const getDefaultProfileUsername = (user) => {
    const display = String(user?.displayName || "").trim();
    const emailLocal = String(user?.email || "")
      .trim()
      .split("@")[0];
    const source = display || emailLocal || "quantura_user";
    const compact = source
      .toLowerCase()
      .replace(/\s+/g, "_")
      .replace(/[^a-z0-9_.-]/g, "")
      .slice(0, 32);
    return compact || "quantura_user";
  };

  const sanitizeProfileUsername = (raw, user = null) => {
    const value = String(raw || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_")
      .replace(/[^a-z0-9_.-]/g, "")
      .slice(0, 32);
    if (value) return value;
    return user ? getDefaultProfileUsername(user) : "";
  };

  const normalizeProfileUrlInput = (raw) => {
    const text = String(raw || "").trim();
    if (!text) return "";
    if (/^https?:\/\//i.test(text)) return text;
    if (/^[a-z]+:\/\//i.test(text)) return text;
    return `https://${text}`;
  };

  const validateProfileSocialUrl = (key, raw) => {
    const value = String(raw || "").trim();
    if (!value) return { ok: true, url: "" };

    const normalized = normalizeProfileUrlInput(value);
    let parsed;
    try {
      parsed = new URL(normalized);
    } catch (error) {
      return { ok: false, error: `Invalid URL for ${key}.` };
    }
    if (parsed.protocol !== "https:") {
      return { ok: false, error: `${key} URL must use HTTPS.` };
    }

    const rule = PROFILE_SOCIAL_URL_RULES[key] || PROFILE_SOCIAL_URL_RULES.website;
    const host = String(parsed.hostname || "").toLowerCase();
    if (rule.allowAnyHost) {
      if (!host || !host.includes(".")) {
        return { ok: false, error: `${key} URL must include a valid host.` };
      }
    } else if (!Array.isArray(rule.hosts) || !rule.hosts.includes(host)) {
      return { ok: false, error: `${key} URL must be on ${key}.` };
    }

    const path = String(parsed.pathname || "").trim();
    if (rule.requirePath && (path === "/" || path === "")) {
      return { ok: false, error: `${key} URL must include your profile path.` };
    }

    parsed.hash = "";
    return { ok: true, url: parsed.toString() };
  };

  const normalizeProfileSocialLinks = (raw, { strict = false } = {}) => {
    const source = raw && typeof raw === "object" ? raw : {};
    const next = cloneDefaultProfileSocialLinks();
    const invalid = [];
    Object.keys(DEFAULT_PROFILE_SOCIAL_LINKS).forEach((key) => {
      const check = validateProfileSocialUrl(key, source[key]);
      if (check.ok) {
        next[key] = check.url;
      } else if (strict && String(source[key] || "").trim()) {
        invalid.push(check.error || `Invalid ${key} URL.`);
      }
    });
    if (invalid.length && strict) {
      throw new Error(invalid.join(" "));
    }
    return next;
  };

  const setProfileStatus = (message, variant = "muted") => {
    if (!ui.profileStatus) return;
    ui.profileStatus.textContent = String(message || "");
    ui.profileStatus.dataset.variant = String(variant || "muted");
  };

  const setProfileFormEnabled = (enabled) => {
    if (!ui.profileForm) return;
    const isEnabled = Boolean(enabled);
    const controls = Array.from(ui.profileForm.querySelectorAll("input, button, select, textarea"));
    controls.forEach((node) => {
      node.disabled = !isEnabled;
    });
  };

  const renderProfileForm = (profile = null, user = null) => {
    const safeProfile = profile && typeof profile === "object" ? profile : {};
    const username = sanitizeProfileUsername(safeProfile.username || "", user);
    const socialLinks = normalizeProfileSocialLinks(safeProfile.socialLinks || {});
    const avatar = normalizeProfileAvatar(safeProfile.avatar);
    const bio = normalizeProfileBio(safeProfile.bio);
    const publicProfile = Boolean(safeProfile.publicProfile);
    const publicScreenerSharing = Boolean(safeProfile.publicScreenerSharing);
    const publicEmailOptIn = Boolean(safeProfile.publicEmailOptIn);
    const stripeConnectAccountId = String(safeProfile.stripeConnectAccountId || "").trim();
    state.userProfile = { username, socialLinks, avatar, bio, publicProfile, publicScreenerSharing, publicEmailOptIn, stripeConnectAccountId };

    if (ui.profileUsername) ui.profileUsername.value = username;
    if (ui.profileAvatar) ui.profileAvatar.value = avatar;
    if (ui.profileBio) ui.profileBio.value = bio;
    if (ui.profilePublicEnabled) ui.profilePublicEnabled.checked = publicProfile;
    if (ui.profilePublicScreener) ui.profilePublicScreener.checked = publicScreenerSharing;
    if (ui.profilePublicEmail) ui.profilePublicEmail.checked = publicEmailOptIn;
    if (ui.profileWebsite) ui.profileWebsite.value = socialLinks.website || "";
    if (ui.profileX) ui.profileX.value = socialLinks.x || "";
    if (ui.profileLinkedin) ui.profileLinkedin.value = socialLinks.linkedin || "";
    if (ui.profileGithub) ui.profileGithub.value = socialLinks.github || "";
    if (ui.profileYoutube) ui.profileYoutube.value = socialLinks.youtube || "";
    if (ui.profileTiktok) ui.profileTiktok.value = socialLinks.tiktok || "";
    if (ui.profileFacebook) ui.profileFacebook.value = socialLinks.facebook || "";
    if (ui.profileInstagram) ui.profileInstagram.value = socialLinks.instagram || "";
    if (ui.profileReddit) ui.profileReddit.value = socialLinks.reddit || "";
    if (ui.profileConnectStripe) {
      ui.profileConnectStripe.classList.toggle("secondary", !stripeConnectAccountId);
      ui.profileConnectStripe.classList.toggle("success", Boolean(stripeConnectAccountId));
      ui.profileConnectStripe.innerHTML = stripeConnectAccountId
        ? `${icon("check-circle")}<span>Stripe connected</span>`
        : `${icon("wallet")}<span>Connect Stripe</span>`;
    }
  };

  const loadUserProfile = async (db, user) => {
    if (!db || !user) {
      state.userProfile = {
        username: "",
        socialLinks: cloneDefaultProfileSocialLinks(),
        avatar: "bull",
        bio: "",
        publicProfile: false,
        publicScreenerSharing: false,
        publicEmailOptIn: false,
        stripeConnectAccountId: "",
      };
      renderProfileForm(state.userProfile, null);
      return;
    }
    try {
      const snap = await db.collection("users").doc(user.uid).get();
      const doc = snap.exists ? snap.data() || {} : {};
      const rawProfile = doc.profile && typeof doc.profile === "object" ? doc.profile : {};
      if (!rawProfile.stripeConnectAccountId && doc?.stripeConnectAccountId) {
        rawProfile.stripeConnectAccountId = doc.stripeConnectAccountId;
      }
      renderProfileForm(rawProfile, user);
    } catch (error) {
      renderProfileForm({ username: getDefaultProfileUsername(user) }, user);
    }
  };

  const ensureUserProfile = async (db, user) => {
    if (!user) return;
    const userRef = db.collection("users").doc(user.uid);
    const snapshot = await userRef.get();
    const existing = snapshot.exists ? snapshot.data() : {};
    const createdAt = existing?.createdAt || firebase.firestore.FieldValue.serverTimestamp();
    const existingProfile = existing?.profile && typeof existing.profile === "object" ? existing.profile : {};
    const profileUsername = sanitizeProfileUsername(existingProfile.username || existing?.name || "", user);
    const profileSocialLinks = normalizeProfileSocialLinks(existingProfile.socialLinks || {});
    const profileAvatar = normalizeProfileAvatar(existingProfile.avatar);
    const profileBio = normalizeProfileBio(existingProfile.bio);
    const publicProfile = Boolean(existingProfile.publicProfile);
    const publicScreenerSharing = Boolean(existingProfile.publicScreenerSharing);
    const publicEmailOptIn = Boolean(existingProfile.publicEmailOptIn);
    const stripeConnectAccountId = String(
      existingProfile.stripeConnectAccountId || existing?.stripeConnectAccountId || ""
    ).trim();
    const autopublishDefaults = {
      autoPublishForecasts: existing?.autoPublishForecasts !== undefined ? Boolean(existing.autoPublishForecasts) : true,
      autoPublishIndicators: existing?.autoPublishIndicators !== undefined ? Boolean(existing.autoPublishIndicators) : true,
      autoPublishModelCouncilConvos:
        existing?.autoPublishModelCouncilConvos !== undefined ? Boolean(existing.autoPublishModelCouncilConvos) : true,
    };

    await userRef.set(
      {
        email: user.email,
        name: user.displayName || "",
        provider: user.providerData?.[0]?.providerId || "email",
        photoURL: user.photoURL || "",
        lastLoginAt: firebase.firestore.FieldValue.serverTimestamp(),
        createdAt,
        profile: {
          username: profileUsername,
          socialLinks: profileSocialLinks,
          avatar: profileAvatar,
          bio: profileBio,
          publicProfile,
          publicScreenerSharing,
          publicEmailOptIn,
          stripeConnectAccountId,
        },
        ...autopublishDefaults,
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

  const triggerDownload = (filename, content, opts = {}) => {
    const mimeType = String(opts?.mimeType || "text/plain;charset=utf-8;");
    const blob = content instanceof Blob ? content : new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
  };

  const triggerDownloadFromUrl = async (url, filename = "") => {
    const href = String(url || "").trim();
    if (!href) throw new Error("Download URL is unavailable.");
    try {
      const resp = await fetch(href, { credentials: "omit" });
      if (!resp.ok) throw new Error(`Download failed (${resp.status}).`);
      const blob = await resp.blob();
      if (!blob || !blob.size) throw new Error("Downloaded file is empty.");
      triggerDownload(filename || "quantura_report", blob, {
        mimeType: blob.type || "application/octet-stream",
      });
      return;
    } catch (error) {
      const link = document.createElement("a");
      link.href = href;
      link.rel = "noopener noreferrer";
      link.target = "_blank";
      if (filename) link.download = filename;
      link.click();
    }
  };

  const extractErrorMessage = (error, fallback = "Unexpected error.") => {
    const direct = String(error?.message || "").trim();
    const details = error?.details && typeof error.details === "object" ? error.details : null;
    const detailText = String(details?.detail || details?.message || details?.error || "").trim();
    return detailText || direct || fallback;
  };

  const copyToClipboard = async (text) => {
    const value = String(text || "");
    if (!value) return false;
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value);
        return true;
      }
    } catch (error) {
      // Fall back.
    }
    try {
      const textarea = document.createElement("textarea");
      textarea.value = value;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "absolute";
      textarea.style.left = "-9999px";
      document.body.appendChild(textarea);
      textarea.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(textarea);
      return Boolean(ok);
    } catch (error) {
      return false;
    }
  };

  const performShare = async ({ url, title = "Quantura", text = "" } = {}) => {
    const shareUrl = String(url || "").trim();
    if (!shareUrl) return false;
    await copyToClipboard(shareUrl);

    const payload = {
      action: "share",
      url: shareUrl,
      title: String(title || "Quantura"),
      text: String(text || "").trim(),
    };

    try {
      window.dispatchEvent(new CustomEvent("quantura:native-share", { detail: payload }));
    } catch (error) {
      // CustomEvent dispatch is best-effort.
    }

    if (isNativeApp() && sendNativeBridgeMessage(payload)) {
      return true;
    }

    if (navigator.share) {
      try {
        await navigator.share({
          title: payload.title,
          text: payload.text || undefined,
          url: shareUrl,
        });
        return true;
      } catch (error) {
        if (error?.name === "AbortError") return true;
      }
    }
    return true;
  };

  window.__quanturaNativeTokenReady = (token) => {
    const cleanToken = String(token || "").trim();
    if (!cleanToken) return;
    try {
      window.__NATIVE_FCM_TOKEN__ = cleanToken;
      localStorage.setItem(FCM_TOKEN_CACHE_KEY, cleanToken);
    } catch (error) {
      // Ignore local storage failures.
    }
    setNotificationTokenPreview(cleanToken);
    if (state.user && state.clients.functions && state.remoteFlags.pushEnabled) {
      syncNotificationToken(state.clients.functions, cleanToken, { source: "native" }).catch(() => {});
    }
  };

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");

  const icon = (name) => `<i class="iconoir-${name}" aria-hidden="true"></i>`;

  const toPrettyJson = (value) => `<pre class="small">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;

  const normalizePath = (rawPath = "/") => {
    const pathname = String(rawPath || "/").split("?")[0] || "/";
    if (pathname.length > 1 && pathname.endsWith("/")) return pathname.slice(0, -1);
    return pathname;
  };

  const FOOTER_SOCIAL_LINKS = [
    {
      key: "tiktok",
      label: "TikTok",
      href: "http://www.tiktok.com/@quanturaai",
      icon: "/assets/social/tiktok.svg",
    },
    {
      key: "instagram",
      label: "Instagram",
      href: "https://www.instagram.com/quanturaai_market_forecasts?igsh=ZTZuNW16ZmxuaHl4&utm_source=qr",
      icon: "/assets/social/instagram.svg",
    },
    {
      key: "facebook",
      label: "Facebook",
      href: "https://www.facebook.com/quanturaai/",
      icon: "/assets/social/facebook-f.svg",
    },
    {
      key: "threads",
      label: "Threads",
      href: "https://www.threads.com/@quanturaai_market_forecasts",
      icon: "/assets/social/threads.svg",
    },
    {
      key: "reddit",
      label: "Reddit",
      href: "https://www.reddit.com/r/Quantura_AI/",
      icon: "/assets/social/reddit-alien.svg",
    },
    {
      key: "linkedin",
      label: "LinkedIn",
      href: "https://www.linkedin.com/company/quanturaai/?viewAsMember=true",
      icon: "/assets/social/linkedin-in.svg",
    },
  ];

  const normalizeTopNavigation = () => {
    const navs = Array.from(document.querySelectorAll(".header .nav-links"));
    if (!navs.length) return;
    navs.forEach((nav) => {
      nav.innerHTML = `
        <a href="/forecasting" data-analytics="nav_terminal">${icon("candlestick-chart")}<span>Terminal</span></a>
        <a href="/explore" data-analytics="nav_explore">${icon("binocular")}<span>Explore</span></a>
        <a href="/research" data-analytics="nav_research">${icon("bookmark-book")}<span>Research</span></a>
        <a href="/blog" data-analytics="nav_blog">${icon("page")}<span>Blog</span></a>
        <a href="/events" data-analytics="nav_events">${icon("calendar")}<span>Events</span></a>
        <a href="/shop" data-analytics="nav_shop">${icon("shop")}<span>Shop</span></a>
        <a href="/about" data-analytics="nav_about">${icon("info-circle")}<span>About</span></a>
        <a href="/pricing" data-analytics="nav_pricing">${icon("wallet")}<span>Pricing</span></a>
        <a href="/contact" data-analytics="nav_contact">${icon("mail")}<span>Contact Us</span></a>
      `;
    });
  };

  const normalizeFooterSocialLinks = () => {
    const groups = Array.from(document.querySelectorAll(".footer-social"));
    if (!groups.length) return;
    groups.forEach((group) => {
      group.innerHTML = FOOTER_SOCIAL_LINKS.map(
        (entry) => `
          <a class="social-link" href="${entry.href}" target="_blank" rel="noopener noreferrer" data-analytics="social_${entry.key}" aria-label="${entry.label}">
            <img src="${entry.icon}" alt="" loading="lazy" decoding="async" />
          </a>
        `
      ).join("");
    });
  };

  const normalizeFooterContactInfo = () => {
    const roots = Array.from(document.querySelectorAll(".footer .footer-grid > div:first-child"));
    if (!roots.length) return;
    roots.forEach((root) => {
      if (!(root instanceof HTMLElement)) return;
      if (root.querySelector(".footer-contact-block")) return;
      const node = document.createElement("p");
      node.className = "small footer-contact-block";
      node.innerHTML =
        '1603 Robertson PL, Bronx, New York 10465<br /><a href="mailto:hello@quantura.studio">hello@quantura.studio</a>';
      root.appendChild(node);
    });
  };

  const normalizeHeaderBranding = () => {
    const brandIcon = "/assets/logo.png";
    document.querySelectorAll(".header .logo").forEach((logo) => {
      if (!(logo instanceof HTMLElement)) return;
      const existing = logo.querySelector("img.logo-img");
      if (existing instanceof HTMLImageElement) {
        if (existing.getAttribute("src") !== brandIcon) existing.setAttribute("src", brandIcon);
        return;
      }
      const iconImg = document.createElement("img");
      iconImg.className = "logo-img";
      iconImg.src = brandIcon;
      iconImg.alt = "";
      iconImg.setAttribute("aria-hidden", "true");
      logo.prepend(iconImg);
    });
    let favicon = document.querySelector('link[rel="icon"]');
    if (!(favicon instanceof HTMLLinkElement)) {
      favicon = document.createElement("link");
      favicon.setAttribute("rel", "icon");
      document.head.appendChild(favicon);
    }
    favicon.setAttribute("type", "image/png");
    favicon.setAttribute("href", brandIcon);
  };

    const ensureSidebarCollapseToggle = () => {
      const layout = document.querySelector(".app-layout");
      const sidebarNav = document.querySelector(".app-sidebar .sidebar-nav");
      if (!layout || !sidebarNav) return;

      const collapsedClass = "is-sidebar-collapsed";
      const shouldApplyCollapse = () => {
        try {
          return window.matchMedia && window.matchMedia("(min-width: 981px)").matches;
        } catch (error) {
          return false;
        }
      };

      const setCollapsed = (collapsed, { persist = true } = {}) => {
        if (!shouldApplyCollapse()) {
          layout.classList.remove(collapsedClass);
          return;
        }
        layout.classList.toggle(collapsedClass, Boolean(collapsed));
        if (persist) {
          safeLocalStorageSet(SIDEBAR_COLLAPSED_KEY, collapsed ? "1" : "0");
        }

        const links = Array.from(sidebarNav.querySelectorAll("a.sidebar-link"));
        links.forEach((link) => {
          if (!(link instanceof HTMLElement)) return;
          const label = String(link.textContent || "").trim();
          if (!label) return;
          if (layout.classList.contains(collapsedClass)) {
            link.setAttribute("title", label);
          } else {
            link.removeAttribute("title");
          }
        });

        syncStickyOffsets();
      };

      let toggle = sidebarNav.querySelector('[data-action="sidebar-toggle"]');
      if (!toggle) {
        toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "sidebar-link sidebar-toggle";
        toggle.dataset.action = "sidebar-toggle";
        sidebarNav.prepend(toggle);
      }

      const updateToggleUi = () => {
        const collapsible = shouldApplyCollapse();
        const collapsed = layout.classList.contains(collapsedClass) && collapsible;
        toggle.disabled = !collapsible;
        toggle.classList.toggle("hidden", !collapsible);
        toggle.setAttribute("aria-hidden", collapsible ? "false" : "true");
        if (!collapsible) return;
        const iconName = collapsed ? "nav-arrow-right" : "nav-arrow-left";
        toggle.innerHTML = `${icon(iconName)}<span>${collapsed ? "Expand" : "Collapse"}</span>`;
        toggle.setAttribute("aria-pressed", collapsed ? "true" : "false");
        toggle.setAttribute("aria-label", collapsed ? "Expand sidebar" : "Collapse sidebar");
        toggle.setAttribute("title", collapsed ? "Expand sidebar" : "Collapse sidebar");
      };

      if (!toggle.__quanturaSidebarBound) {
        toggle.__quanturaSidebarBound = true;
        toggle.addEventListener("click", () => {
          if (!shouldApplyCollapse()) return;
          const next = !layout.classList.contains(collapsedClass);
          setCollapsed(next, { persist: true });
          updateToggleUi();
          logEvent("sidebar_toggled", { collapsed: next, page_path: window.location.pathname });
        });
      }

      const initialPref = String(safeLocalStorageGet(SIDEBAR_COLLAPSED_KEY) || "") === "1";
      setCollapsed(initialPref, { persist: false });
      updateToggleUi();

      window.addEventListener("resize", () => {
        // Ensure the layout resets on mobile, but preserve preference for desktop.
        const collapsed = String(safeLocalStorageGet(SIDEBAR_COLLAPSED_KEY) || "") === "1";
        setCollapsed(collapsed, { persist: false });
        updateToggleUi();
      });
    };

  const ensureHeaderSolveNowCta = () => {};

  const removeHeaderSolveNowCta = () => {
    const headerRoots = Array.from(document.querySelectorAll(".header, .top-nav, .nav-actions"));
    if (!headerRoots.length) return;
    headerRoots.forEach((root) => {
      root
        .querySelectorAll('#header-solve-now, [data-action="open-solve-now"], a[href*="#solve-now"]')
        .forEach((node) => node.remove());
      root.querySelectorAll("a, button").forEach((node) => {
        const text = String(node.textContent || "").trim().toLowerCase();
        if (text === "solve now") node.remove();
      });
    });
  };

  const ensureHeaderNotificationsCta = () => {
    const actions = document.querySelector(".header .nav-actions");
    if (!actions) return;
    let link = document.getElementById("header-notifications");
    if (!link) {
      link = document.createElement("a");
      link.id = "header-notifications";
      link.className = "cta secondary icon-only";
      link.setAttribute("data-analytics", "nav_notifications");
      const authButton = actions.querySelector("#header-auth");
      if (authButton?.parentElement === actions) {
        actions.insertBefore(link, authButton);
      } else {
        actions.appendChild(link);
      }
	      ui.headerNotifications = link;
    }
    const authed = hasFullAccount();
    link.href = authed ? "/notifications" : "/account";
    link.innerHTML = `${icon("bell-notification")}`;
    link.classList.add("icon-only");
    link.setAttribute("title", "Notifications");
    link.setAttribute("aria-label", authed ? "Open notifications" : "Sign in to manage notifications");
  };

  const renderNotificationLog = () => {
    if (!ui.notificationsLog) return;
    const entries = Array.isArray(state.notificationLog) ? state.notificationLog : [];
    if (!entries.length) {
      ui.notificationsLog.innerHTML = `<div class="small muted">No live notifications yet.</div>`;
      return;
    }
    ui.notificationsLog.innerHTML = entries
      .map((entry) => {
        const title = escapeHtml(String(entry.title || "Quantura update"));
        const body = escapeHtml(String(entry.body || ""));
        const source = escapeHtml(String(entry.source || "foreground"));
        const at = escapeHtml(new Date(entry.at || Date.now()).toLocaleString());
        const nextSteps = Array.isArray(entry.nextSteps) ? entry.nextSteps.filter(Boolean).slice(0, 4) : [];
        return `
          <article class="notification-log-item">
            <div class="notification-log-head">
              <strong>${title}</strong>
              <span class="small muted">${at}</span>
            </div>
            <p class="small">${body || "No message body provided."}</p>
            ${
              nextSteps.length
                ? `<ol class="small" style="margin: 0 0 6px 18px;">
                    ${nextSteps.map((step) => `<li>${escapeHtml(String(step))}</li>`).join("")}
                  </ol>`
                : ""
            }
            ${entry.personalized ? `<div class="small muted">${escapeHtml(MODEL_COUNCIL_OUTPUT_DISCLAIMER)}</div>` : ""}
            <div class="small muted">Source: ${source}</div>
          </article>
        `;
      })
      .join("");
  };

  const notificationCategoryLabel = (category) => {
    const key = String(category || "").trim().toLowerCase();
    switch (key) {
      case "watchlist":
        return "Watchlist";
      case "explore":
        return "Explore Feed";
      case "ipo":
        return "IPO";
      case "earnings":
        return "Earnings";
      case "daily":
        return "Daily";
      case "weekly":
        return "Weekly";
      case "inactive":
        return "Inactive";
      default:
        return "General";
    }
  };

  const renderNotificationFeed = () => {
    if (!ui.notificationsItems) return;
    const feed = state.notificationFeed || {};
    const entries = Array.isArray(feed.items) ? feed.items : [];
    if (ui.notificationsUnreadCount) {
      const unread = Math.max(0, Number(feed.unreadCount || entries.filter((item) => !item?.read).length) || 0);
      ui.notificationsUnreadCount.textContent = `Unread ${unread}`;
    }

    if (Array.isArray(ui.notificationFilterButtons)) {
      ui.notificationFilterButtons.forEach((button) => {
        const filter = String(button?.dataset?.notificationFilter || "").trim().toLowerCase();
        const active = filter && (feed.unreadOnly ? filter === "unread" : filter === (feed.filter || "all"));
        button.classList.toggle("active", Boolean(active));
      });
    }

    if (feed.loading) {
      ui.notificationsItems.innerHTML = `<div class="small muted">Loading notifications...</div>`;
      return;
    }
    if (!entries.length) {
      ui.notificationsItems.innerHTML = `<div class="small muted">No notifications in this view.</div>`;
      return;
    }

    ui.notificationsItems.innerHTML = entries
      .map((entry) => {
        const id = escapeHtml(String(entry?.id || ""));
        const title = escapeHtml(String(entry?.title || "Quantura update"));
        const body = escapeHtml(String(entry?.body || ""));
        const category = escapeHtml(notificationCategoryLabel(entry?.category));
        const deepLink = String(entry?.deepLink || "").trim();
        const link = deepLink.startsWith("http") ? deepLink : deepLink ? `/${deepLink.replace(/^\/+/, "")}` : "/notifications";
        const createdAt = new Date(Number(entry?.createdAtMs || Date.now()) || Date.now()).toLocaleString();
        const isRead = Boolean(entry?.read);
        return `
          <article class="notification-log-item ${isRead ? "is-read" : "is-unread"}" data-notification-item="${id}">
            <div class="notification-log-head">
              <strong>${title}</strong>
              <span class="small muted">${escapeHtml(createdAt)}</span>
            </div>
            <div class="small muted" style="margin-bottom:6px;">Category: ${category}</div>
            <p class="small">${body || "No message body provided."}</p>
            <div class="hero-actions" style="margin-top:8px;">
              <a class="cta secondary small" href="${escapeHtml(link)}">${icon("open-in-window")}<span>Open</span></a>
              ${
                isRead
                  ? ""
                  : `<button class="cta secondary small" type="button" data-action="notification-mark-read" data-id="${id}">
                      ${icon("check")}<span>Mark read</span>
                    </button>`
              }
            </div>
          </article>
        `;
      })
      .join("");
  };

  const loadNotificationFeed = async ({ filter = "all", unreadOnly = false, includeHidden = false, silent = false } = {}) => {
    if (!ui.notificationsItems) return;
    if (!hasFullAccount()) {
      state.notificationFeed.items = [];
      state.notificationFeed.unreadCount = 0;
      state.notificationFeed.filter = "all";
      state.notificationFeed.unreadOnly = false;
      state.notificationFeed.loading = false;
      renderNotificationFeed();
      return;
    }
    state.notificationFeed.filter = String(filter || "all").trim().toLowerCase() || "all";
    state.notificationFeed.unreadOnly = Boolean(unreadOnly);
    state.notificationFeed.loading = true;
    renderNotificationFeed();
    try {
      const headers = await buildApiAuthHeaders({ includeJson: false });
      const params = new URLSearchParams();
      if (state.notificationFeed.filter && state.notificationFeed.filter !== "all" && state.notificationFeed.filter !== "unread") {
        params.set("category", state.notificationFeed.filter);
      }
      if (state.notificationFeed.unreadOnly || state.notificationFeed.filter === "unread") {
        params.set("unread", "true");
      }
      if (includeHidden) params.set("includeHidden", "true");
      params.set("limit", "80");
      const response = await fetch(`/api/notifications/items?${params.toString()}`, {
        method: "GET",
        headers,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(String(payload?.error || "Unable to load notifications."));
      }
      state.notificationFeed.items = Array.isArray(payload?.items) ? payload.items : [];
      state.notificationFeed.unreadCount =
        Number(payload?.unreadCount || state.notificationFeed.items.filter((item) => !item?.read).length) || 0;
      state.notificationFeed.loading = false;
      renderNotificationFeed();
    } catch (error) {
      state.notificationFeed.loading = false;
      renderNotificationFeed();
      if (!silent) {
        const message = extractErrorMessage(error, "Unable to load notifications.");
        setNotificationStatus(message);
        showToast(message, "warn");
      }
    }
  };

  const markNotificationItemRead = async (itemId) => {
    const id = String(itemId || "").trim();
    if (!id) return;
    if (!hasFullAccount()) return;
    try {
      const headers = await buildApiAuthHeaders({ includeJson: true });
      const response = await fetch(`/api/notifications/items/${encodeURIComponent(id)}/read`, {
        method: "POST",
        headers,
        body: JSON.stringify({ read: true }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(String(payload?.error || "Unable to update notification."));
      state.notificationFeed.items = (state.notificationFeed.items || []).map((item) =>
        String(item?.id || "") === id ? { ...item, read: true } : item
      );
      state.notificationFeed.unreadCount = Math.max(0, Number(state.notificationFeed.unreadCount || 0) - 1);
      renderNotificationFeed();
    } catch (error) {
      showToast(extractErrorMessage(error, "Unable to mark notification as read."), "warn");
    }
  };

  const markAllNotificationsRead = async () => {
    if (!hasFullAccount()) return;
    try {
      const headers = await buildApiAuthHeaders({ includeJson: true });
      const response = await fetch("/api/notifications/items/read-all", {
        method: "POST",
        headers,
        body: JSON.stringify({ includeHidden: false }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(String(payload?.error || "Unable to mark all as read."));
      state.notificationFeed.items = (state.notificationFeed.items || []).map((item) => ({ ...item, read: true }));
      state.notificationFeed.unreadCount = 0;
      renderNotificationFeed();
      showToast("All notifications marked as read.");
    } catch (error) {
      showToast(extractErrorMessage(error, "Unable to mark all notifications as read."), "warn");
    }
  };

  const persistNotificationLog = () => {
    try {
      localStorage.setItem(FCM_LOG_CACHE_KEY, JSON.stringify((state.notificationLog || []).slice(0, 30)));
    } catch (error) {
      // Ignore storage failures.
    }
  };

  const appendNotificationLog = ({ title, body, source = "foreground", at = new Date().toISOString() }) => {
    const next = [
      {
        title: String(title || "Quantura update"),
        body: String(body || ""),
        source: String(source || "foreground"),
        at: String(at || new Date().toISOString()),
        personalized: false,
        nextSteps: [],
      },
      ...(Array.isArray(state.notificationLog) ? state.notificationLog : []),
    ].slice(0, 30);
    state.notificationLog = next;
    persistNotificationLog();
    renderNotificationLog();
  };

  const personalizeNotificationEntry = async ({ title, body, source }) => {
    const cleanTitle = String(title || "Quantura update").trim() || "Quantura update";
    const cleanBody = String(body || "").trim();
    if (!state.notificationPrivacy?.locationConsent) {
      return { title: cleanTitle, body: cleanBody, personalized: false, nextSteps: [] };
    }
    try {
      const headers = await buildApiAuthHeaders({ includeJson: true });
      const response = await fetch("/api/notifications/personalize", {
        method: "POST",
        headers,
        body: JSON.stringify({
          title: cleanTitle,
          body: cleanBody,
          source: String(source || "foreground"),
          context: {
            timezone: String(state.notificationPrivacy?.timezone || ""),
            countryCode: String(state.notificationPrivacy?.coarseLocation?.countryCode || ""),
            region: String(state.notificationPrivacy?.ipRegion || ""),
          },
        }),
      });
      if (!response.ok) {
        return { title: cleanTitle, body: cleanBody, personalized: false, nextSteps: [] };
      }
      const payload = await response.json().catch(() => ({}));
      const notification = payload?.notification && typeof payload.notification === "object" ? payload.notification : {};
      return {
        title: String(notification.title || cleanTitle).trim() || cleanTitle,
        body: String(notification.body || cleanBody).trim(),
        nextSteps: Array.isArray(notification.nextSteps) ? notification.nextSteps.slice(0, 4).map((item) => String(item)) : [],
        personalized: Boolean(notification.personalized),
      };
    } catch (error) {
      return { title: cleanTitle, body: cleanBody, personalized: false, nextSteps: [] };
    }
  };

  const appendNotificationLogPersonalized = async ({ title, body, source = "foreground", at = new Date().toISOString() }) => {
    const rewritten = await personalizeNotificationEntry({ title, body, source });
    const next = [
      {
        title: String(rewritten.title || title || "Quantura update"),
        body: String(rewritten.body || body || ""),
        source: String(source || "foreground"),
        at: String(at || new Date().toISOString()),
        personalized: Boolean(rewritten.personalized),
        nextSteps: Array.isArray(rewritten.nextSteps) ? rewritten.nextSteps.slice(0, 4) : [],
      },
      ...(Array.isArray(state.notificationLog) ? state.notificationLog : []),
    ].slice(0, 30);
    state.notificationLog = next;
    persistNotificationLog();
    renderNotificationLog();
  };

  const resolveTradingViewTheme = () => {
    if (state.tradingViewTheme === "dark" || state.tradingViewTheme === "light") {
      return state.tradingViewTheme;
    }
    return isDarkMode() ? "dark" : "light";
  };

  const normalizeTradingViewSymbol = (ticker) => {
    const clean = normalizeTicker(ticker);
    if (!clean) return "NASDAQ:AAPL";
    if (clean.includes(":")) return clean;
    return `NASDAQ:${clean}`;
  };

  const resolveTradingViewInterval = (interval, rangePreset) => {
    const base = String(interval || "").toLowerCase() === "1h" ? "60" : "D";
    if (base === "60") return "60";
    const range = String(rangePreset || "max").toLowerCase();
    if (range === "1d") return "5";
    if (range === "5d") return "30";
    if (range === "1m") return "60";
    if (range === "3m") return "240";
    if (range === "5y") return "W";
    return "D";
  };

  const resolveTradingViewStyle = () => (state.chartViewMode === "line" ? "3" : "1");

  const setTradingViewStatus = (text) => {
    if (!ui.tradingViewStatus) return;
    ui.tradingViewStatus.textContent = String(text || "");
  };

  const setTerminalChartEngineVisibility = (engine) => {
    const shell = ui.tickerChart?.closest(".chart-shell");
    if (shell) {
      shell.classList.toggle("chart-engine-tradingview", engine === "tradingview");
      shell.classList.toggle("chart-engine-legacy", engine === "legacy");
    }
    if (ui.tradingViewRoot) {
      ui.tradingViewRoot.classList.toggle("hidden", engine !== "tradingview");
    }
    if (ui.tickerChart) {
      ui.tickerChart.classList.toggle("hidden", engine === "tradingview");
    }
  };

  const buildCurrentChartOverlays = () => {
    const ticker = normalizeTicker(state.tickerContext.ticker || "");
    const forecastOverlays =
      state.tickerContext.forecastDoc && normalizeTicker(state.tickerContext.forecastDoc.ticker || "") === ticker
        ? buildForecastOverlays(state.tickerContext.forecastDoc.forecastRows || [])
        : [];
    return [...forecastOverlays, ...(state.tickerContext.indicatorOverlays || [])];
  };

  const buildTradingViewWidgetSrc = (baseUrl, config) => {
    const payload = encodeURIComponent(JSON.stringify(config || {}));
    return `${baseUrl}#${payload}`;
  };

  const mountTradingViewIframe = ({ container, src, title, onload, onerror }) => {
    if (!container) return null;
    const frame = document.createElement("iframe");
    frame.setAttribute("scrolling", "no");
    frame.setAttribute("allowtransparency", "true");
    frame.setAttribute("frameborder", "0");
    frame.setAttribute("loading", "lazy");
    frame.setAttribute("title", title || "TradingView widget");
    frame.setAttribute("lang", "en");
    frame.src = src;
    if (typeof onload === "function") frame.addEventListener("load", onload, { once: true });
    if (typeof onerror === "function") frame.addEventListener("error", onerror, { once: true });
    container.innerHTML = "";
    container.appendChild(frame);
    return frame;
  };

  const renderTradingViewTerminal = ({ ticker, interval, onFallback = null }) => {
    if (!ui.tradingViewRoot || !ui.tradingViewAdvanced) return false;
    const symbol = normalizeTradingViewSymbol(ticker);
    const theme = resolveTradingViewTheme();
    const tvInterval = resolveTradingViewInterval(interval, state.chartRangePreset);
    const style = resolveTradingViewStyle();
    const nonce = Date.now();
    state.tradingViewRenderNonce = nonce;
    state.tradingViewLoadFailed = false;
    if (state.tradingViewLoadTimer) {
      window.clearTimeout(state.tradingViewLoadTimer);
      state.tradingViewLoadTimer = null;
    }
    ui.tradingViewFallback?.classList.add("hidden");
    setTradingViewStatus(`TradingView · ${symbol}`);

    const shared = {
      symbol,
      colorTheme: theme,
      isTransparent: true,
      locale: "en",
    };
    let fallbackTriggered = false;
    const triggerFallback = (reason) => {
      if (fallbackTriggered) return;
      fallbackTriggered = true;
      if (state.tradingViewRenderNonce !== nonce) return;
      state.tradingViewLoadFailed = true;
      setTradingViewStatus(reason || "TradingView unavailable");
      ui.tradingViewFallback?.classList.remove("hidden");
      if (typeof onFallback === "function") {
        try {
          onFallback();
        } catch (error) {
          // Ignore fallback callback errors.
        }
      }
    };

    mountTradingViewIframe({
      container: ui.tradingViewTickerTape,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/ticker-tape/?locale=en", {
        symbols: [{ proName: symbol, title: symbol }],
        showSymbolLogo: true,
        displayMode: "adaptive",
        colorTheme: theme,
        isTransparent: true,
      }),
      title: `Ticker tape ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewSymbolInfo,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/symbol-info/?locale=en", {
        ...shared,
        width: "100%",
        height: 255,
      }),
      title: `Symbol info ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewAdvanced,
      src: buildTradingViewWidgetSrc("https://www.tradingview.com/widgetembed/?hideideas=1&locale=en", {
        symbol,
        interval: tvInterval,
        allow_symbol_change: "1",
        hide_side_toolbar: "0",
        save_image: "1",
        style,
        theme,
        timezone: "Etc/UTC",
        studies: ["STD;MACD"],
      }),
      title: `Advanced chart ${symbol}`,
      onload: () => {
        if (state.tradingViewRenderNonce !== nonce) return;
        state.tradingViewLoadFailed = false;
        if (state.tradingViewLoadTimer) {
          window.clearTimeout(state.tradingViewLoadTimer);
          state.tradingViewLoadTimer = null;
        }
        ui.tradingViewFallback?.classList.add("hidden");
        setTradingViewStatus(`TradingView loaded · ${symbol}`);
      },
      onerror: () => {
        triggerFallback("TradingView unavailable");
      },
    });

    mountTradingViewIframe({
      container: ui.tradingViewCompanyProfile,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/symbol-profile/?locale=en", {
        ...shared,
        width: "100%",
        height: "100%",
      }),
      title: `Company profile ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewFundamentalData,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/financials/?locale=en", {
        symbol,
        colorTheme: theme,
        isTransparent: true,
        displayMode: "regular",
        width: "100%",
        height: 775,
      }),
      title: `Fundamentals ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewTechnicalAnalysis,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/technical-analysis/?locale=en", {
        interval: "15m",
        width: "100%",
        height: "100%",
        isTransparent: true,
        symbol,
        showIntervalTabs: true,
        displayMode: "single",
        colorTheme: theme,
      }),
      title: `Technical analysis ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewTopStories,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/timeline/?locale=en", {
        symbol,
        colorTheme: theme,
        isTransparent: true,
        displayMode: "regular",
        width: "100%",
        height: 600,
      }),
      title: `Top stories ${symbol}`,
    });

    state.tradingViewLoadTimer = window.setTimeout(() => {
      triggerFallback("TradingView timeout");
    }, TRADINGVIEW_LOAD_TIMEOUT_MS);

    return true;
  };

  const applyChartControlState = () => {
    ui.chartRangeButtons.forEach((button) => {
      const preset = String(button.dataset.chartRange || "").toLowerCase();
      button.classList.toggle("active", preset === state.chartRangePreset);
      button.setAttribute("aria-pressed", preset === state.chartRangePreset ? "true" : "false");
    });
    ui.chartViewButtons.forEach((button) => {
      const mode = String(button.dataset.chartView || "").toLowerCase();
      button.classList.toggle("active", mode === state.chartViewMode);
      button.setAttribute("aria-pressed", mode === state.chartViewMode ? "true" : "false");
    });
    ui.chartThemeButtons.forEach((button) => {
      const theme = String(button.dataset.tvTheme || "").toLowerCase();
      button.classList.toggle("active", theme === state.tradingViewTheme);
      button.setAttribute("aria-pressed", theme === state.tradingViewTheme ? "true" : "false");
    });
  };

  const computeChartRange = (xValues, preset) => {
    const normalized = String(preset || "max").toLowerCase();
    if (normalized === "max") return null;
    const points = Array.isArray(xValues)
      ? xValues
          .map((value) => {
            const dt = new Date(value);
            return Number.isFinite(dt.getTime()) ? dt : null;
          })
          .filter(Boolean)
      : [];
    if (!points.length) return null;
    const first = points[0];
    const last = points[points.length - 1];
    const start = new Date(last.getTime());
    const dayMs = 24 * 60 * 60 * 1000;
    if (normalized === "1d") start.setTime(last.getTime() - dayMs);
    if (normalized === "5d") start.setTime(last.getTime() - 5 * dayMs);
    if (normalized === "1m") start.setTime(last.getTime() - 30 * dayMs);
    if (normalized === "3m") start.setTime(last.getTime() - 90 * dayMs);
    if (normalized === "1y") start.setTime(last.getTime() - 365 * dayMs);
    if (normalized === "5y") start.setTime(last.getTime() - 365 * 5 * dayMs);
    if (normalized === "ytd") {
      start.setMonth(0, 1);
      start.setHours(0, 0, 0, 0);
    }
    const clampedStart = start < first ? first : start;
    return [clampedStart.toISOString(), last.toISOString()];
  };

  const bindChartControls = () => {
    if (ui.chartRangeButtons.length) {
      ui.chartRangeButtons.forEach((button) => {
        button.addEventListener("click", async () => {
          const preset = String(button.dataset.chartRange || "").toLowerCase();
          if (!preset || preset === state.chartRangePreset) return;
          state.chartRangePreset = preset;
          safeLocalStorageSet(CHART_RANGE_CACHE_KEY, preset);
          applyChartControlState();
          if (ui.tickerChart && state.tickerContext.rows?.length) {
            await renderTickerChart(
              state.tickerContext.rows,
              state.tickerContext.ticker,
              state.tickerContext.interval,
              buildCurrentChartOverlays()
            );
          }
        });
      });
    }
    if (ui.chartViewButtons.length) {
      ui.chartViewButtons.forEach((button) => {
        button.addEventListener("click", async () => {
          const mode = String(button.dataset.chartView || "").toLowerCase();
          if (!mode || mode === state.chartViewMode) return;
          state.chartViewMode = mode === "line" ? "line" : "candlestick";
          safeLocalStorageSet(CHART_VIEW_CACHE_KEY, state.chartViewMode);
          applyChartControlState();
          if (ui.tickerChart && state.tickerContext.rows?.length) {
            await renderTickerChart(
              state.tickerContext.rows,
              state.tickerContext.ticker,
              state.tickerContext.interval,
              buildCurrentChartOverlays()
            );
          }
        });
      });
    }

    if (ui.chartThemeButtons.length) {
      ui.chartThemeButtons.forEach((button) => {
        button.addEventListener("click", () => {
          const next = String(button.dataset.tvTheme || "").toLowerCase();
          if (!next || next === state.tradingViewTheme) return;
          state.tradingViewTheme = next === "dark" || next === "light" ? next : "auto";
          safeLocalStorageSet(TRADINGVIEW_THEME_CACHE_KEY, state.tradingViewTheme);
          applyChartControlState();
          if (getActiveTicker() && isPanelVisible("ticker")) {
            renderTradingViewTerminal({
              ticker: getActiveTicker(),
              interval: state.tickerContext.interval || "1d",
            });
          }
        });
      });
    }

    setTerminalChartEngineVisibility("legacy");
    applyChartControlState();
  };

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

  const buildShareUrl = (kind, shareId) => {
    const id = encodeURIComponent(String(shareId || "").trim());
    if (!id) return "";
    const type = String(kind || "").trim().toLowerCase();
    const path = type === "forecast" ? "/forecasting" : type === "screener" ? "/screener" : "/uploads";
    return `${window.location.origin}${path}?share=${id}`;
  };

  const fetchSharedScreenerPayload = async (shareId) => {
    const cleanShareId = String(shareId || "").trim();
    if (!cleanShareId) throw new Error("Share ID is required.");
    const headers = await buildApiAuthHeaders();
    const response = await fetch(`/api/shares/${encodeURIComponent(cleanShareId)}`, {
      method: "GET",
      headers,
      credentials: "same-origin",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(String(payload?.error || "Shared screener unavailable.").trim());
    }
    if (String(payload?.kind || "").trim().toLowerCase() !== "screener" || !payload?.screener) {
      throw new Error("Shared item is not a screener run.");
    }
    return payload;
  };

  const renderSharedScreenerRun = async (shareId, { notify = false } = {}) => {
    if (window.location.pathname !== "/screener" || !ui.screenerOutput) return null;
    const cleanShareId = String(shareId || "").trim();
    if (!cleanShareId) return null;

    try {
      setOutputLoading(ui.screenerOutput, "Loading shared screener...");
      if (ui.screenerLoadStatus) ui.screenerLoadStatus.textContent = "Loading shared screener...";
      const payload = await fetchSharedScreenerPayload(cleanShareId);
      const screener = payload?.screener && typeof payload.screener === "object" ? payload.screener : {};
      const shareUrl = buildShareUrl("screener", cleanShareId);
      const runDoc = {
        ...screener,
        id: String(screener.id || payload.sourceId || "").trim(),
        __sharedMeta: {
          shareId: cleanShareId,
          shareUrl,
          readOnly: Boolean(payload.readOnly),
          canImport: Boolean(payload.canImport),
        },
      };
      state.pendingShareProcessed = true;
      setPendingShareId("");
      renderScreenerRunOutput(runDoc);
      if (ui.screenerLoadStatus) {
        ui.screenerLoadStatus.textContent = runDoc.__sharedMeta.readOnly
          ? "Viewing shared screener (read-only)."
          : "Viewing shared screener as owner.";
      }
      if (notify) showToast("Shared screener loaded.");
      return runDoc;
    } catch (error) {
      setOutputReady(ui.screenerOutput);
      if (ui.screenerLoadStatus) ui.screenerLoadStatus.textContent = "Unable to load shared screener.";
      if (ui.screenerOutput) {
        ui.screenerOutput.innerHTML = `<div class="small muted">${escapeHtml(
          extractErrorMessage(error, "Shared screener unavailable.")
        )}</div>`;
      }
      if (notify) showToast(extractErrorMessage(error, "Unable to load shared screener."), "warn");
      throw error;
    }
  };

  const importSharedItemById = async (functions, shareId, { redirect = true } = {}) => {
    if (!functions) throw new Error("Import service is unavailable.");
    const cleanShareId = String(shareId || "").trim();
    if (!cleanShareId) throw new Error("Share ID is required.");
    const importShare = functions.httpsCallable("import_shared_item");
    const result = await importShare({ shareId: cleanShareId, meta: buildMeta() });
    const kind = String(result.data?.kind || "").trim().toLowerCase();
    const importedId = String(result.data?.importedId || "").trim();
    if (!kind || !importedId) throw new Error("Shared item import did not return an ID.");
    if (redirect) {
      if (kind === "forecast") {
        window.location.href = `/forecasting?forecastId=${encodeURIComponent(importedId)}`;
      } else if (kind === "screener") {
        window.location.href = `/screener?runId=${encodeURIComponent(importedId)}`;
      } else if (kind === "upload") {
        window.location.href = `/uploads?uploadId=${encodeURIComponent(importedId)}`;
      }
    }
    return { kind, importedId };
  };

  const processPendingShareImport = async (functions) => {
    if (!functions || !state.user) return null;
    const shareId = String(getPendingShareId() || "").trim();
    if (!shareId) return null;
    if (state.pendingShareProcessed) return null;
    state.pendingShareProcessed = true;

    try {
      setPendingShareId(shareId);
      const { kind, importedId } = await importSharedItemById(functions, shareId, { redirect: false });
      setPendingShareId("");
      showToast("Shared item saved to your dashboard.");
      logEvent("shared_item_imported", { kind });

      if (kind === "forecast") {
        window.location.href = `/forecasting?forecastId=${encodeURIComponent(importedId)}`;
        return { kind, importedId };
      }
      if (kind === "screener") {
        window.location.href = `/screener?runId=${encodeURIComponent(importedId)}`;
        return { kind, importedId };
      }
      if (kind === "upload") {
        window.location.href = `/uploads?uploadId=${encodeURIComponent(importedId)}`;
        return { kind, importedId };
      }

      return { kind, importedId };
    } catch (error) {
      state.pendingShareProcessed = false;
      showToast(error.message || "Unable to import shared item.", "warn");
      return null;
    }
  };

  const TICKER_SYNC_INPUT_IDS = Object.freeze([
    "terminal-ticker",
    "forecast-ticker",
    "technicals-ticker",
    "download-ticker",
    "news-ticker",
    "intel-ticker",
    "options-ticker",
    "ticker-query-ticker",
    "watchlist-ticker",
    "alert-ticker",
    "autopilot-ticker",
    "predictions-ticker",
  ]);

  const getActiveTicker = () => normalizeTicker(state.tickerContext.activeTicker || state.tickerContext.ticker || "");

  const getTickerHistoryStorageKey = () => {
    const uid = String(state.user?.uid || "anon").trim() || "anon";
    return `${TICKER_HISTORY_KEY_PREFIX}:${uid}`;
  };

  const readTickerHistory = () => {
    try {
      const raw = String(safeLocalStorageGet(getTickerHistoryStorageKey()) || "").trim();
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      const normalized = parsed
        .map((entry) => normalizeTicker(entry))
        .filter(Boolean);
      return Array.from(new Set(normalized)).slice(0, TICKER_HISTORY_LIMIT);
    } catch (error) {
      return [];
    }
  };

  const writeTickerHistory = (entries) => {
    const normalized = Array.from(
      new Set(
        (Array.isArray(entries) ? entries : [])
          .map((entry) => normalizeTicker(entry))
          .filter(Boolean)
      )
    ).slice(0, TICKER_HISTORY_LIMIT);
    state.tickerContext.tickerHistory = normalized;
    safeLocalStorageSet(getTickerHistoryStorageKey(), JSON.stringify(normalized));
    return normalized;
  };

  const renderTickerHistory = () => {
    if (!ui.tickerHistory) return;
    const history = state.tickerContext.tickerHistory?.length ? state.tickerContext.tickerHistory : readTickerHistory();
    state.tickerContext.tickerHistory = history;
    if (!history.length) {
      ui.tickerHistory.innerHTML = `<div class="small muted">Recent tickers will appear here as you research.</div>`;
      return;
    }
    ui.tickerHistory.innerHTML = `
      <div class="ticker-history-title small muted">Recent tickers</div>
      <div class="ticker-history-list">
        ${history
          .map(
            (ticker) => `
          <span class="ticker-history-item">
            <button class="ticker-history-chip" type="button" data-action="ticker-history-select" data-ticker="${escapeHtml(ticker)}">${escapeHtml(ticker)}</button>
            <button class="ticker-history-remove" type="button" data-action="ticker-history-delete" data-ticker="${escapeHtml(ticker)}" aria-label="Delete ${escapeHtml(ticker)} from ticker history">&times;</button>
          </span>
        `
          )
          .join("")}
      </div>
    `;
  };

  const rememberTickerInHistory = (ticker) => {
    const clean = normalizeTicker(ticker);
    if (!clean) return;
    const existing = state.tickerContext.tickerHistory?.length ? state.tickerContext.tickerHistory : readTickerHistory();
    const next = [clean, ...existing.filter((entry) => entry !== clean)].slice(0, TICKER_HISTORY_LIMIT);
    writeTickerHistory(next);
    renderTickerHistory();
  };

  const removeTickerFromHistory = (ticker) => {
    const clean = normalizeTicker(ticker);
    if (!clean) return;
    const existing = state.tickerContext.tickerHistory?.length ? state.tickerContext.tickerHistory : readTickerHistory();
    writeTickerHistory(existing.filter((entry) => entry !== clean));
    renderTickerHistory();
  };

  const refreshTradingViewForTicker = (ticker) => {
    const clean = normalizeTicker(ticker);
    if (!clean || !isPanelVisible("ticker")) return;
    const interval = String(ui.terminalInterval?.value || state.tickerContext.interval || "1d");
    const rendered = renderTradingViewTerminal({ ticker: clean, interval });
    if (rendered && !(state.tickerContext.indicatorOverlays || []).length) {
      setTerminalChartEngineVisibility("tradingview");
    }
  };

  const syncTickerInputs = (ticker, { source = "sync", skipHistory = false, emitAnalytics = false } = {}) => {
    const clean = normalizeTicker(ticker);
    if (!clean) return false;
    const previous = getActiveTicker();
    state.tickerContext.activeTicker = clean;
    state.tickerContext.ticker = clean;
    safeLocalStorageSet(LAST_TICKER_KEY, clean);
    if (!skipHistory) rememberTickerInHistory(clean);

    TICKER_SYNC_INPUT_IDS.forEach((id) => {
      const el = document.getElementById(id);
      if (!el || !("value" in el)) return;
      if (String(el.value || "").trim() !== clean) {
        el.value = clean;
      }
    });

    if (previous !== clean) {
      refreshTradingViewForTicker(clean);
      scheduleSideDataRefresh(clean, { force: false });
      if (emitAnalytics) {
        logEvent("active_ticker_changed", { ticker: clean, source });
      }
    }
    return previous !== clean;
  };

  const bindTickerInputSync = () => {
    const seen = new Set();
    TICKER_SYNC_INPUT_IDS.forEach((id) => {
      const el = document.getElementById(id);
      if (!el || seen.has(el)) return;
      seen.add(el);
      const commitTicker = () => {
        const clean = normalizeTicker(el.value);
        if (!clean) return;
        syncTickerInputs(clean, { source: `input:${id}` });
      };
      el.addEventListener("change", commitTicker);
      el.addEventListener("blur", commitTicker);
    });
  };

  const formatUsd = (value, digits = 2) => {
    const num = typeof value === "number" ? value : Number(value);
    if (!Number.isFinite(num)) return "—";
    return `$${num.toFixed(digits)}`;
  };

  const formatCompactNumber = (value) => {
    const num = typeof value === "number" ? value : Number(value);
    if (!Number.isFinite(num)) return "—";
    try {
      return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(num);
    } catch (error) {
      return num.toLocaleString();
    }
  };

  const formatPercent = (value, { signed = false, digits = 2 } = {}) => {
    const num = typeof value === "number" ? value : Number(value);
    if (!Number.isFinite(num)) return "—";
    const prefix = signed && num > 0 ? "+" : "";
    return `${prefix}${num.toFixed(digits)}%`;
  };

  const formatIntelValue = (value) => {
    if (value === null || value === undefined || value === "") return "—";
    if (Array.isArray(value)) return value.map((item) => String(item)).join(", ");
    if (typeof value === "number" && Number.isFinite(value)) return value.toLocaleString();
    return String(value);
  };

  const renderTickerIntel = (payload) => {
    if (!ui.intelOutput && !ui.tickerIntelligenceOutput) return;
    const data = payload || {};
    const ticker = normalizeTicker(data.ticker || state.tickerContext.ticker || "") || "";
    const profile = data.profile || {};
    const profileDetails = data.profileDetails && typeof data.profileDetails === "object" ? data.profileDetails : {};
    const valuation = data.valuation && typeof data.valuation === "object" ? data.valuation : {};
    const trading = data.trading && typeof data.trading === "object" ? data.trading : {};
    const events = Array.isArray(data.events) ? data.events : [];
    const analyst = data.analyst || {};
    const trend = Array.isArray(data.recommendationTrend) ? data.recommendationTrend : [];
    const executiveSummary = data.executiveSummary && typeof data.executiveSummary === "object" ? data.executiveSummary : {};
    const deepDive = data.fundamentalDeepDive && typeof data.fundamentalDeepDive === "object" ? data.fundamentalDeepDive : {};
    const riskAndEsg = data.riskAndEsg && typeof data.riskAndEsg === "object" ? data.riskAndEsg : {};
    const heatmap = Array.isArray(data.balanceSheetHeatmap) ? data.balanceSheetHeatmap : [];
    const peers = Array.isArray(data.peerComparison) ? data.peerComparison : [];

    const name = escapeHtml(profile.name || ticker || "Ticker");
    const sector = escapeHtml(profile.sector || "");
    const industry = escapeHtml(profile.industry || "");
    const exchange = escapeHtml(profile.exchange || "");
    const currency = escapeHtml(profile.currency || "");
    const website = String(profile.website || "").trim();
    const websiteLink = website ? escapeHtml(website) : "";
    const summary = escapeHtml(profile.summary || "");
    const profileLongName = escapeHtml(String(profileDetails.longName || profile.name || ticker || "Ticker"));
    const profileSector = escapeHtml(String(profileDetails.sector || profile.sector || "").trim());
    const profileIndustry = escapeHtml(String(profileDetails.industry || profile.industry || "").trim());
    const profileCountry = escapeHtml(String(profileDetails.country || profile.country || "").trim());
    const profileWebsite = String(profileDetails.website || profile.website || "").trim();
    const profileWebsiteLink = profileWebsite ? escapeHtml(profileWebsite) : websiteLink;
    const profileBusinessSummary = escapeHtml(String(profileDetails.longBusinessSummary || profile.summary || "").trim());

    const valuationRows = [
      { label: "Market cap", value: toFiniteOrNull(valuation.marketCap) === null ? "—" : formatCompactNumber(valuation.marketCap) },
      { label: "Trailing P/E", value: toFiniteOrNull(valuation.trailingPE) === null ? "—" : Number(valuation.trailingPE).toFixed(2) },
      { label: "Forward P/E", value: toFiniteOrNull(valuation.forwardPE) === null ? "—" : Number(valuation.forwardPE).toFixed(2) },
      { label: "Price to book", value: toFiniteOrNull(valuation.priceToBook) === null ? "—" : Number(valuation.priceToBook).toFixed(2) },
      {
        label: "Enterprise value",
        value: toFiniteOrNull(valuation.enterpriseValue) === null ? "—" : formatCompactNumber(valuation.enterpriseValue),
      },
    ];
    const tradingRows = [
      { label: "Beta", value: toFiniteOrNull(trading.beta) === null ? "—" : Number(trading.beta).toFixed(2) },
      {
        label: "52-week range",
        value:
          toFiniteOrNull(trading.fiftyTwoWeekLow) !== null && toFiniteOrNull(trading.fiftyTwoWeekHigh) !== null
            ? `${formatUsd(trading.fiftyTwoWeekLow)} - ${formatUsd(trading.fiftyTwoWeekHigh)}`
            : "—",
      },
      { label: "Average volume", value: toFiniteOrNull(trading.avgVolume) === null ? "—" : formatCompactNumber(trading.avgVolume) },
      {
        label: "Shares outstanding",
        value: toFiniteOrNull(trading.sharesOutstanding) === null ? "—" : formatCompactNumber(trading.sharesOutstanding),
      },
    ];

    const stats = [
      { label: "Market cap", value: profile.marketCap ? formatCompactNumber(profile.marketCap) : "—" },
      { label: "52-week range", value: profile.fiftyTwoWeekLow && profile.fiftyTwoWeekHigh ? `${formatUsd(profile.fiftyTwoWeekLow)} - ${formatUsd(profile.fiftyTwoWeekHigh)}` : "—" },
      { label: "Trailing P/E", value: profile.trailingPE ? Number(profile.trailingPE).toFixed(2) : "—" },
      { label: "Forward P/E", value: profile.forwardPE ? Number(profile.forwardPE).toFixed(2) : "—" },
      { label: "Beta", value: profile.beta ? Number(profile.beta).toFixed(2) : "—" },
      { label: "Dividend yield", value: profile.dividendYield ? formatPercent(Number(profile.dividendYield) * 100.0, { signed: false, digits: 2 }) : "—" },
    ];

    const recommendationKey = analyst.recommendationKey ? String(analyst.recommendationKey).replace(/_/g, " ") : "";
    const recommendationMean = analyst.recommendationMean ? Number(analyst.recommendationMean).toFixed(2) : "";
    const analystOpinions = analyst.analystOpinions ? Number(analyst.analystOpinions).toLocaleString() : "";
    const targetLine =
      analyst.targetMeanPrice || analyst.targetLowPrice || analyst.targetHighPrice
        ? `${formatUsd(analyst.targetLowPrice)} / ${formatUsd(analyst.targetMeanPrice)} / ${formatUsd(analyst.targetHighPrice)}`
        : "";

    const eventMarkup = events.length
      ? `
        <div class="intel-list">
          ${events
            .slice(0, 8)
            .map((item) => `<div class="intel-row"><span class="intel-k">${escapeHtml(item.label || "")}</span><span class="intel-v">${escapeHtml(formatIntelValue(item.value))}</span></div>`)
            .join("")}
        </div>
      `
      : `<div class="small muted">No events returned.</div>`;

    const trendMarkup = trend.length
      ? `
        <div class="table-wrap" style="margin-top:10px;">
          <table class="data-table">
            <thead><tr><th>Period</th><th>SB</th><th>B</th><th>H</th><th>S</th><th>SS</th></tr></thead>
            <tbody>
              ${trend
                .slice(0, 5)
                .map(
                  (row) => `
                <tr>
                  <td>${escapeHtml(row.period || "")}</td>
                  <td>${Number(row.strongBuy || 0)}</td>
                  <td>${Number(row.buy || 0)}</td>
                  <td>${Number(row.hold || 0)}</td>
                  <td>${Number(row.sell || 0)}</td>
                  <td>${Number(row.strongSell || 0)}</td>
                </tr>
              `
                )
                .join("")}
            </tbody>
          </table>
        </div>
      `
      : "";

    const compactHtml = `
      <div class="intel-head">
        <div class="intel-name">${name}</div>
        <div class="small muted">${[ticker, sector, industry, exchange, currency].filter(Boolean).join(" · ")}</div>
        ${websiteLink ? `<a class="news-link" href="${websiteLink}" target="_blank" rel="noreferrer">Company site</a>` : ""}
      </div>

      <div class="intel-split">
        <div>
          <div class="small"><strong>Company / Profile</strong></div>
          <div class="intel-kv">
            <div class="intel-kv-row"><span>Name</span><span>${profileLongName || "—"}</span></div>
            <div class="intel-kv-row"><span>Sector</span><span>${profileSector || "—"}</span></div>
            <div class="intel-kv-row"><span>Industry</span><span>${profileIndustry || "—"}</span></div>
            <div class="intel-kv-row"><span>Country</span><span>${profileCountry || "—"}</span></div>
          </div>
          ${
            profileWebsiteLink
              ? `<a class="news-link" href="${profileWebsiteLink}" target="_blank" rel="noreferrer">Website</a>`
              : ""
          }
          ${profileBusinessSummary ? `<div class="intel-summary small" style="margin-top:8px;">${profileBusinessSummary}</div>` : ""}
        </div>
        <div>
          <div class="small"><strong>Valuation</strong></div>
          <div class="intel-kv">
            ${valuationRows
              .map(
                (row) => `
                <div class="intel-kv-row"><span>${escapeHtml(String(row.label || ""))}</span><span>${escapeHtml(String(row.value || "—"))}</span></div>
              `
              )
              .join("")}
          </div>
          <div class="small" style="margin-top:8px;"><strong>Trading</strong></div>
          <div class="intel-kv">
            ${tradingRows
              .map(
                (row) => `
                <div class="intel-kv-row"><span>${escapeHtml(String(row.label || ""))}</span><span>${escapeHtml(String(row.value || "—"))}</span></div>
              `
              )
              .join("")}
          </div>
        </div>
      </div>

      <div class="intel-stats">
        ${stats.map((item) => `<div class="intel-stat"><div class="small muted">${escapeHtml(item.label)}</div><div class="intel-stat-v">${escapeHtml(item.value)}</div></div>`).join("")}
      </div>

      ${summary ? `<div class="intel-summary small">${summary}</div>` : ""}

      <div class="intel-split">
        <div>
          <div class="small"><strong>Upcoming events</strong></div>
          ${eventMarkup}
        </div>
        <div>
          <div class="small"><strong>Analyst snapshot</strong></div>
          <div class="intel-analyst small">
            ${recommendationKey ? `<div><strong>Consensus</strong> ${escapeHtml(recommendationKey)}${recommendationMean ? ` (mean ${escapeHtml(recommendationMean)})` : ""}</div>` : "<div class=\"muted\">No consensus available.</div>"}
            ${analystOpinions ? `<div><strong>Analyst opinions</strong> ${escapeHtml(analystOpinions)}</div>` : ""}
            ${targetLine ? `<div><strong>Target (low / mean / high)</strong> ${escapeHtml(targetLine)}</div>` : ""}
          </div>
          ${trendMarkup}
        </div>
      </div>
    `;

    if (ui.intelOutput) {
      ui.intelOutput.innerHTML = compactHtml;
    }

    const liquidity = riskAndEsg.liquidity && typeof riskAndEsg.liquidity === "object" ? riskAndEsg.liquidity : {};
    const esg = riskAndEsg.esg && typeof riskAndEsg.esg === "object" ? riskAndEsg.esg : {};
    const revenueMechanics = deepDive.revenueMechanics && typeof deepDive.revenueMechanics === "object" ? deepDive.revenueMechanics : {};
    const profitability = deepDive.profitability && typeof deepDive.profitability === "object" ? deepDive.profitability : {};
    const capitalAllocation = deepDive.capitalAllocation && typeof deepDive.capitalAllocation === "object" ? deepDive.capitalAllocation : {};

    const toPctOrDash = (value, digits = 2) => {
      const num = toFiniteOrNull(value);
      if (num === null) return "—";
      return `${(num * 100).toFixed(digits)}%`;
    };

    const heatmapHtml = heatmap.length
      ? heatmap
          .map((cell) => {
            const score = toFiniteOrNull(cell?.score);
            const numeric = score === null ? null : Math.max(0, Math.min(100, score));
            const hue = numeric === null ? 210 : Math.round((numeric * 1.2)); // 0=red, 120=green.
            const bg = numeric === null ? "rgba(148, 163, 184, 0.14)" : `hsla(${hue}, 78%, 42%, 0.18)`;
            const border = numeric === null ? "rgba(148, 163, 184, 0.36)" : `hsla(${hue}, 82%, 36%, 0.4)`;
            return `
              <div class="intel-heat-cell" style="background:${bg}; border-color:${border};">
                <div class="intel-heat-label">${escapeHtml(String(cell?.label || "Metric"))}</div>
                <div class="intel-heat-score">${numeric === null ? "—" : `${numeric.toFixed(1)}`}</div>
                <div class="small muted">${escapeHtml(String(cell?.hint || ""))}</div>
              </div>
            `;
          })
          .join("")
      : `<div class="small muted">Balance sheet heatmap is unavailable for this ticker.</div>`;

    const peersHtml = peers.length
      ? `
        <div class="table-wrap peer-comparison-wrap">
          <table class="data-table">
            <thead>
              <tr><th>Ticker</th><th>P/E</th><th>Debt/Equity</th><th>Sharpe</th></tr>
            </thead>
            <tbody>
              ${peers
                .map(
                  (row) => `
                  <tr>
                    <td>${escapeHtml(String(row?.ticker || "—"))}</td>
                    <td>${toFiniteOrNull(row?.pe) === null ? "—" : Number(row.pe).toFixed(2)}</td>
                    <td>${toFiniteOrNull(row?.debtToEquity) === null ? "—" : Number(row.debtToEquity).toFixed(2)}</td>
                    <td>${toFiniteOrNull(row?.sharpeRatio) === null ? "—" : Number(row.sharpeRatio).toFixed(2)}</td>
                  </tr>
                `
                )
                .join("")}
            </tbody>
          </table>
        </div>
      `
      : `<div class="small muted">Peer comparison is unavailable right now.</div>`;

    const institutionalHtml = `
      <div class="intel-institutional-grid">
        <article class="intel-column-card">
          <div class="intel-subhead">Executive Summary</div>
          <div class="intel-kv">
            <div class="intel-kv-row"><span>Ticker</span><span>${escapeHtml(String(executiveSummary.ticker || ticker || "—"))}</span></div>
            <div class="intel-kv-row"><span>Exchange</span><span>${escapeHtml(String(executiveSummary.exchange || profile.exchange || "—"))}</span></div>
            <div class="intel-kv-row"><span>Sector</span><span>${escapeHtml(String(executiveSummary.sector || profile.sector || "—"))}</span></div>
            <div class="intel-kv-row"><span>Market Cap</span><span>${profile.marketCap ? escapeHtml(formatCompactNumber(profile.marketCap)) : "—"}</span></div>
            <div class="intel-kv-row"><span>12M Price Target</span><span>${toFiniteOrNull(executiveSummary.priceTarget12m) === null ? "—" : escapeHtml(formatUsd(executiveSummary.priceTarget12m))}</span></div>
          </div>
        </article>
        <article class="intel-column-card">
          <div class="intel-subhead">Fundamental Deep Dive</div>
          <div class="small"><strong>Revenue Mechanics</strong></div>
          <div class="intel-kv">
            <div class="intel-kv-row"><span>Total Revenue</span><span>${toFiniteOrNull(revenueMechanics.totalRevenue) === null ? "—" : escapeHtml(formatCompactNumber(revenueMechanics.totalRevenue))}</span></div>
            <div class="intel-kv-row"><span>Gross Profit</span><span>${toFiniteOrNull(revenueMechanics.grossProfit) === null ? "—" : escapeHtml(formatCompactNumber(revenueMechanics.grossProfit))}</span></div>
          </div>
          <div class="small muted">${escapeHtml(String(revenueMechanics.segmentBreakdown || "Segment detail is limited for this issuer."))}</div>
          <div class="small" style="margin-top:8px;"><strong>Profitability Analysis</strong></div>
          <div class="intel-kv">
            <div class="intel-kv-row"><span>Net Margin</span><span>${toPctOrDash(profitability.netMargin)}</span></div>
            <div class="intel-kv-row"><span>ROI</span><span>${toPctOrDash(profitability.roi)}</span></div>
          </div>
          <div class="small" style="margin-top:8px;"><strong>Capital Allocation</strong></div>
          <div class="small">${escapeHtml(String(capitalAllocation.dividendPolicy || "No dividend policy reported."))}</div>
          <div class="small">${escapeHtml(String(capitalAllocation.shareBuybacks || "No explicit buyback trend reported."))}</div>
        </article>
        <article class="intel-column-card">
          <div class="intel-subhead">Risk & ESG</div>
          <div class="small"><strong>Risk Mitigation</strong></div>
          <div class="small">${escapeHtml(String(riskAndEsg.riskMitigation || "Risk mitigation data is limited."))}</div>
          <div class="small" style="margin-top:8px;"><strong>Liquidity</strong></div>
          <div class="intel-kv">
            <div class="intel-kv-row"><span>Total Cash</span><span>${toFiniteOrNull(liquidity.totalCash) === null ? "—" : escapeHtml(formatCompactNumber(liquidity.totalCash))}</span></div>
            <div class="intel-kv-row"><span>Total Debt</span><span>${toFiniteOrNull(liquidity.totalDebt) === null ? "—" : escapeHtml(formatCompactNumber(liquidity.totalDebt))}</span></div>
            <div class="intel-kv-row"><span>Current Ratio</span><span>${toFiniteOrNull(liquidity.currentRatio) === null ? "—" : Number(liquidity.currentRatio).toFixed(2)}</span></div>
          </div>
          <div class="small" style="margin-top:8px;"><strong>ESG Score</strong></div>
          <div class="intel-kv">
            <div class="intel-kv-row"><span>Environmental</span><span>${toFiniteOrNull(esg.environmental) === null ? "—" : Number(esg.environmental).toFixed(1)}</span></div>
            <div class="intel-kv-row"><span>Social</span><span>${toFiniteOrNull(esg.social) === null ? "—" : Number(esg.social).toFixed(1)}</span></div>
            <div class="intel-kv-row"><span>Governance</span><span>${toFiniteOrNull(esg.governance) === null ? "—" : Number(esg.governance).toFixed(1)}</span></div>
            <div class="intel-kv-row"><span>Overall</span><span>${toFiniteOrNull(esg.overall) === null ? "—" : Number(esg.overall).toFixed(1)}</span></div>
          </div>
        </article>
      </div>
      <div style="margin-top:16px;">
        <div class="small"><strong>Balance Sheet Heatmap</strong></div>
        <div class="intel-heatmap">${heatmapHtml}</div>
      </div>
      <div style="margin-top:16px;">
        <div class="small"><strong>Peer Comparison</strong> · P/E, Debt-to-Equity, Sharpe Ratio</div>
        ${peersHtml}
      </div>
      <div class="intel-raw-shell" style="margin-top:16px;">
        <div class="card-head">
          <div class="small"><strong>Raw Yahoo Fields</strong> · full `.info` payload</div>
          <button
            class="cta secondary small"
            type="button"
            data-action="intel-load-full-info"
            data-ticker="${escapeHtml(ticker)}"
          >
            View all fields
          </button>
        </div>
        <div class="intel-raw-controls hidden" data-intel-raw-controls>
          <label class="label" for="intel-raw-filter">Search keys</label>
          <input id="intel-raw-filter" data-action="intel-filter-full-info" data-ticker="${escapeHtml(ticker)}" placeholder="marketCap, enterpriseValue, beta..." />
        </div>
        <div id="intel-raw-info-output" class="panel-output small hidden">Expand to load all fields.</div>
      </div>
    `;

    if (ui.tickerIntelligenceOutput) {
      ui.tickerIntelligenceOutput.innerHTML = institutionalHtml;
    }
  };

  const formatPredictionDate = (value) => {
    const text = String(value || "").trim();
    if (!text) return "—";
    const date = new Date(text);
    if (Number.isNaN(date.getTime())) return "—";
    return date.toLocaleString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  };

  const formatPredictionCountdown = (value) => {
    const text = String(value || "").trim();
    if (!text) return "No end date";
    const target = new Date(text);
    if (Number.isNaN(target.getTime())) return "No end date";
    const deltaMs = target.getTime() - Date.now();
    if (deltaMs <= 0) return "Ended";
    const totalHours = Math.ceil(deltaMs / (60 * 60 * 1000));
    if (totalHours >= 48) return `Ends in ${Math.ceil(totalHours / 24)}d`;
    if (totalHours >= 1) return `Ends in ${totalHours}h`;
    const minutes = Math.max(1, Math.ceil(deltaMs / (60 * 1000)));
    return `Ends in ${minutes}m`;
  };

  const parsePredictionArray = (raw) => {
    if (Array.isArray(raw)) return raw;
    if (typeof raw !== "string") return [];
    const text = raw.trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  };

  const clampPredictionPrice = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return null;
    if (num < 0) return 0;
    if (num > 1) return 1;
    return num;
  };

  const formatPredictionPercent = (value) => {
    const num = clampPredictionPrice(value);
    if (num === null) return "—";
    return `${Math.round(num * 100)}%`;
  };

  const formatPredictionCents = (value) => {
    const num = clampPredictionPrice(value);
    if (num === null) return "—";
    const cents = num * 100;
    const text = cents.toFixed(1);
    return `${text.endsWith(".0") ? text.slice(0, -2) : text}¢`;
  };

  const predictionMarketUrl = (market, event = null) => {
    const direct = String(market?.marketUrl || "").trim();
    if (direct) return direct;
    const marketSlug = String(market?.slug || "").trim().replace(/^\/+|\/+$/g, "");
    if (marketSlug) return `https://polymarket.com/event/${encodeURIComponent(marketSlug)}`;
    const eventSlug = String(event?.slug || market?.eventSlug || "").trim().replace(/^\/+|\/+$/g, "");
    return eventSlug ? `https://polymarket.com/event/${encodeURIComponent(eventSlug)}` : "";
  };

  const normalizePredictionMarket = (rawMarket) => {
    const market = rawMarket && typeof rawMarket === "object" ? rawMarket : {};
    const parsedOutcomes = parsePredictionArray(market.outcomes)
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .slice(0, 16);
    const parsedOutcomePrices = parsePredictionArray(market.outcomePrices)
      .map((item) => clampPredictionPrice(item))
      .filter((item) => item !== null)
      .slice(0, 16);
    const alignedLength = Math.min(parsedOutcomes.length, parsedOutcomePrices.length);
    const outcomes = alignedLength > 0 ? parsedOutcomes.slice(0, alignedLength) : [];
    const outcomePrices = alignedLength > 0 ? parsedOutcomePrices.slice(0, alignedLength) : [];

    const status = String(market.status || "").trim().toLowerCase();
    const closed = Boolean(market.closed) || status === "closed" || status === "resolved" || status === "ended";
    const active = typeof market.active === "boolean" ? market.active : !closed;
    const volumeUsdRaw = Number(market.volumeUsd ?? market.volume ?? 0);
    const liquidityUsdRaw = Number(market.liquidityUsd ?? market.liquidity ?? 0);
    const volumeUsd = Number.isFinite(volumeUsdRaw) ? Math.max(0, volumeUsdRaw) : 0;
    const liquidityUsd = Number.isFinite(liquidityUsdRaw) ? Math.max(0, liquidityUsdRaw) : 0;
    const topOutcomes = outcomes
      .map((label, index) => ({ label, prob: outcomePrices[index] }))
      .filter((entry) => typeof entry.label === "string" && Number.isFinite(entry.prob))
      .sort((a, b) => b.prob - a.prob)
      .slice(0, 6);
    const yesIndex = outcomes.findIndex((label) => /^yes$/i.test(label));
    const yesProb = yesIndex >= 0 ? outcomePrices[yesIndex] : null;

    return {
      id: String(market.id || market.marketId || market.conditionId || market.market_id || "").trim(),
      question: String(market.question || market.title || "").trim(),
      slug: String(market.slug || "").trim(),
      endDate: String(market.endDate || market.end_date || "").trim(),
      category: String(market.category || "").trim(),
      image: String(market.image || "").trim(),
      icon: String(market.icon || "").trim(),
      volumeUsd,
      liquidityUsd,
      outcomes,
      outcomePrices,
      isBinary: outcomes.length === 2,
      yesProb,
      topOutcomes,
      closed,
      active,
    };
  };

  const normalizePredictionEvent = (rawEvent, index = 0) => {
    const event = rawEvent && typeof rawEvent === "object" ? rawEvent : {};
    const markets = (Array.isArray(event.markets) ? event.markets : [])
      .map((item) => normalizePredictionMarket(item))
      .filter((item) => item.id && item.question);
    markets.sort((a, b) => {
      const volumeDelta = Number(b.volumeUsd || 0) - Number(a.volumeUsd || 0);
      if (volumeDelta !== 0) return volumeDelta;
      return Number(b.liquidityUsd || 0) - Number(a.liquidityUsd || 0);
    });
    if (!markets.length) return null;
    return {
      id: String(event.id || event.eventId || `event-${index + 1}`).trim(),
      title: String(event.title || event.name || event.question || "Prediction markets").trim(),
      slug: String(event.slug || "").trim(),
      ticker: String(event.ticker || "").trim(),
      markets,
    };
  };

  const normalizePredictionsPayload = (rawPayload, fallbackQuery = "") => {
    const payload = rawPayload && typeof rawPayload === "object" ? rawPayload : {};
    const eventsRaw = Array.isArray(payload.events) ? payload.events : Array.isArray(rawPayload) ? rawPayload : [];
    const events = eventsRaw
      .map((event, index) => normalizePredictionEvent(event, index))
      .filter((event) => Boolean(event));
    return {
      query: String(payload.query || fallbackQuery || "").trim(),
      fetchedAt: String(payload.fetchedAt || new Date().toISOString()),
      events,
    };
  };

  const getPredictionsPanelState = (ticker = "") => {
    const activeSymbol = normalizeTicker(ticker || state.tickerContext.predictionsTicker || state.tickerContext.ticker || "");
    const mode = state.tickerContext.predictionsMode === "topActive" ? "topActive" : "ticker";
    const rawQuery = String(state.tickerContext.predictionsQuery || "").trim().slice(0, 80);
    const query = mode === "ticker" ? rawQuery || activeSymbol : "";
    return {
      ticker: activeSymbol,
      mode,
      query,
      includeClosed: Boolean(state.tickerContext.predictionsIncludeClosed),
      expanded: Boolean(state.tickerContext.predictionsExpanded),
    };
  };

  const buildPredictionsCacheKey = ({ mode, query, includeClosed }) => {
    if (mode === "topActive") return "top-active::limit-36::offset-0";
    return `ticker::${String(query || "").trim().toLowerCase()}::${includeClosed ? "closed" : "open"}`;
  };

  const getPredictionsCache = (cacheKey) => {
    if (!cacheKey || !polymarketClientCache.has(cacheKey)) return null;
    const record = polymarketClientCache.get(cacheKey);
    const expiresAtMs = Number(record?.expiresAtMs || 0);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
      polymarketClientCache.delete(cacheKey);
      return null;
    }
    polymarketClientCache.delete(cacheKey);
    polymarketClientCache.set(cacheKey, record);
    return record.payload || null;
  };

  const setPredictionsCache = (cacheKey, payload) => {
    if (!cacheKey || !payload) return;
    polymarketClientCache.set(cacheKey, {
      expiresAtMs: Date.now() + POLYMARKET_CLIENT_CACHE_TTL_MS,
      payload,
    });
    while (polymarketClientCache.size > POLYMARKET_CLIENT_CACHE_MAX_ENTRIES) {
      const oldestKey = polymarketClientCache.keys().next().value;
      if (!oldestKey) break;
      polymarketClientCache.delete(oldestKey);
    }
  };

  const buildVisiblePredictionGroups = (events, { includeClosed = false, expanded = false } = {}) => {
    const groups = [];
    let totalMarkets = 0;
    events.forEach((event) => {
      const markets = (Array.isArray(event?.markets) ? event.markets : [])
        .filter((market) => {
          if (!includeClosed && (market.closed || market.active === false)) return false;
          return true;
        })
        .sort((a, b) => {
          const volumeDelta = Number(b.volumeUsd || 0) - Number(a.volumeUsd || 0);
          if (volumeDelta !== 0) return volumeDelta;
          return Number(b.liquidityUsd || 0) - Number(a.liquidityUsd || 0);
        });
      if (!markets.length) return;
      totalMarkets += markets.length;
      groups.push({ ...event, markets });
    });

    const limit = expanded ? Number.POSITIVE_INFINITY : POLYMARKET_DEFAULT_MARKET_LIMIT;
    let remaining = limit;
    let shownMarkets = 0;
    const visibleGroups = [];
    groups.forEach((event) => {
      if (remaining <= 0) return;
      const slice = event.markets.slice(0, remaining);
      if (!slice.length) return;
      shownMarkets += slice.length;
      remaining -= slice.length;
      visibleGroups.push({ ...event, markets: slice });
    });

    return { groups: visibleGroups, shownMarkets, totalMarkets };
  };

  const renderPredictionOutcomePill = (label, prob, variant = "neutral") => {
    const cleanLabel = String(label || "").trim();
    const cleanProb = clampPredictionPrice(prob);
    if (!cleanLabel || cleanProb === null) return "";
    const percent = Math.max(0, Math.min(100, Math.round(cleanProb * 100)));
    return `
      <button class="prediction-outcome-pill ${variant}" type="button" aria-disabled="true" tabindex="-1" aria-label="${escapeHtml(
        `${cleanLabel} ${percent}%`
      )}">
        <span class="prediction-outcome-pill-fill" style="width:${percent}%"></span>
        <span class="prediction-outcome-pill-text">${escapeHtml(cleanLabel)} ${percent}%</span>
      </button>
    `;
  };

  const renderPredictionMarketCard = (event, market) => {
    const category = String(market.category || event?.ticker || "Market").trim();
    const volumeText = Number(market.volumeUsd || 0) > 0 ? `$${formatCompactNumber(market.volumeUsd)} Vol.` : "—";
    const marketUrl = predictionMarketUrl(market, event);
    const binaryRows = (() => {
      if (!market.isBinary || !Array.isArray(market.outcomes) || !Array.isArray(market.outcomePrices)) return [];
      let yesIndex = market.outcomes.findIndex((label) => /^yes$/i.test(label));
      let noIndex = market.outcomes.findIndex((label) => /^no$/i.test(label));
      if (yesIndex < 0) yesIndex = 0;
      if (noIndex < 0) noIndex = yesIndex === 0 ? 1 : 0;
      return [yesIndex, noIndex]
        .filter((index) => index >= 0 && index < market.outcomes.length && index < market.outcomePrices.length)
        .map((index) => ({ label: market.outcomes[index], prob: market.outcomePrices[index] }));
    })();
    const yesProb = binaryRows.find((item) => /^yes$/i.test(item.label))?.prob;
    const noProb = binaryRows.find((item) => /^no$/i.test(item.label))?.prob;
    const primaryProb =
      yesProb ??
      (Array.isArray(market.topOutcomes) && market.topOutcomes.length ? market.topOutcomes[0].prob : null);
    const primaryLabel =
      yesProb !== undefined
        ? "Yes"
        : Array.isArray(market.topOutcomes) && market.topOutcomes.length
        ? String(market.topOutcomes[0].label || "Likely")
        : "Outcome";
    const pillsHtml = binaryRows.length
      ? binaryRows
          .map((item) => renderPredictionOutcomePill(item.label, item.prob, /^yes$/i.test(item.label) ? "yes" : "no"))
          .join("")
      : Array.isArray(market.topOutcomes) && market.topOutcomes.length
      ? market.topOutcomes
          .slice(0, 3)
          .map((item) => renderPredictionOutcomePill(item.label, item.prob))
          .join("")
      : "";
    return `
      <article class="prediction-market-card prediction-market-row">
        <div class="prediction-market-row-head small">
          <span class="prediction-chip">${escapeHtml(category)}</span>
          <span>${escapeHtml(formatPredictionCountdown(market.endDate))}</span>
        </div>
        <div class="prediction-market-row-grid">
          <div class="prediction-market-row-question">
            <h4 class="prediction-market-title">${escapeHtml(String(market.question || "Untitled market"))}</h4>
            <div class="small muted">${escapeHtml(volumeText)}</div>
          </div>
          <div class="prediction-market-row-prob">
            <div class="prediction-market-prob">${escapeHtml(formatPredictionPercent(primaryProb))}</div>
            <div class="small muted">${escapeHtml(primaryLabel)}</div>
          </div>
          <div class="prediction-market-row-actions">
            ${
              Number.isFinite(yesProb)
                ? `<button class="prediction-buy-btn yes" type="button" aria-disabled="true" tabindex="-1">Buy Yes ${escapeHtml(
                    formatPredictionCents(yesProb)
                  )}</button>`
                : ""
            }
            ${
              Number.isFinite(noProb)
                ? `<button class="prediction-buy-btn no" type="button" aria-disabled="true" tabindex="-1">Buy No ${escapeHtml(
                    formatPredictionCents(noProb)
                  )}</button>`
                : ""
            }
          </div>
        </div>
        <div class="prediction-pill-row">
          ${
            pillsHtml ||
            `<span class="prediction-no-price-badge small" aria-label="No price data for this market">No price data</span>`
          }
        </div>
        <div class="prediction-market-footer">
          ${
            marketUrl
              ? `<a class="news-link" href="${escapeHtml(marketUrl)}" target="_blank" rel="noreferrer" data-analytics="polymarket_market_open" data-label="${escapeHtml(String(market.question || "polymarket_market"))}" aria-label="${escapeHtml(
                  `View ${String(market.question || "market")} on Polymarket`
                )}">View on Polymarket</a>`
              : `<span class="small muted">Market link unavailable</span>`
          }
          <span class="small muted prediction-warning" aria-label="Markets can be wrong">Markets can be wrong</span>
        </div>
      </article>
    `;
  };

  const buildPredictionsSkeleton = () =>
    new Array(6)
      .fill(0)
      .map(
        () => `
      <article class="prediction-market-card prediction-skeleton-card" aria-hidden="true">
        <div class="prediction-skeleton-line short"></div>
        <div class="prediction-skeleton-line"></div>
        <div class="prediction-skeleton-line medium"></div>
        <div class="prediction-skeleton-line"></div>
      </article>
    `
      )
      .join("");

  const renderPredictionsOutput = ({ payload = null, ticker = "", loading = false, error = "" } = {}) => {
    if (!ui.tickerPredictionsOutput) return;
    const panel = getPredictionsPanelState(ticker);
    const normalized = normalizePredictionsPayload(payload || state.tickerContext.predictionsData || {}, panel.query || panel.ticker);
    const { groups, shownMarkets, totalMarkets } = buildVisiblePredictionGroups(normalized.events, {
      includeClosed: panel.includeClosed,
      expanded: panel.expanded,
    });
    const includeClosedToggle = panel.mode === "ticker";

    const resultBody = (() => {
      if (loading) {
        return `<div class="prediction-card-grid">${buildPredictionsSkeleton()}</div>`;
      }
      if (groups.length) {
        return `
          <div class="predictions-events">
            ${groups
              .map(
                (event) => `
              <section class="prediction-event-group">
                <div class="prediction-event-head">
                  <div>
                    <h3 class="prediction-event-title">${escapeHtml(String(event.title || "Prediction markets"))}</h3>
                    <span class="small muted">Prediction markets</span>
                  </div>
                  ${
                    event.slug
                      ? `<a class="news-link" href="https://polymarket.com/event/${encodeURIComponent(
                          String(event.slug)
                        )}" target="_blank" rel="noreferrer">Event</a>`
                      : ""
                  }
                </div>
                <div class="prediction-card-grid">
                  ${event.markets.map((market) => renderPredictionMarketCard(event, market)).join("")}
                </div>
              </section>
            `
              )
              .join("")}
          </div>
          ${
            totalMarkets > shownMarkets
              ? `<div class="predictions-show-more-row"><button class="cta secondary small" type="button" data-action="predictions-show-more">${
                  panel.expanded ? "Show less" : `Show more (${totalMarkets - shownMarkets})`
                }</button></div>`
              : ""
          }
        `;
      }
      if (error) {
        return `<div class="small muted">Polymarket predictions are temporarily unavailable${panel.ticker ? ` for ${escapeHtml(panel.ticker)}` : ""}.</div>`;
      }
      if (panel.mode === "ticker" && !panel.query) {
        return `<div class="small muted">Enter a ticker or keyword to search prediction markets.</div>`;
      }
      return `<div class="small muted">No active prediction markets found for ${escapeHtml(panel.query || panel.ticker || "this search")}.</div>`;
    })();

    ui.tickerPredictionsOutput.innerHTML = `
      <div class="predictions-shell">
        <div class="predictions-toolbar">
          <div class="predictions-tabs" role="tablist" aria-label="Predictions source">
            <button class="task-chip ${panel.mode === "ticker" ? "active" : ""}" type="button" role="tab" aria-selected="${
      panel.mode === "ticker" ? "true" : "false"
    }" data-action="predictions-tab" data-mode="ticker">For this ticker</button>
            <button class="task-chip ${panel.mode === "topActive" ? "active" : ""}" type="button" role="tab" aria-selected="${
      panel.mode === "topActive" ? "true" : "false"
    }" data-action="predictions-tab" data-mode="topActive">Top Active</button>
          </div>
          <div class="predictions-search-row">
            <input
              type="search"
              class="input"
              data-action="predictions-query"
              aria-label="Search prediction markets"
              placeholder="Search ticker or keyword"
              value="${escapeHtml(panel.mode === "ticker" ? panel.query : panel.ticker || "")}"
            />
            <button class="cta secondary small" type="button" data-action="predictions-search-now">Search</button>
            <label class="predictions-include-closed ${includeClosedToggle ? "" : "hidden"}">
              <input type="checkbox" data-action="predictions-include-closed" ${panel.includeClosed ? "checked" : ""} />
              <span>Include closed</span>
            </label>
          </div>
          <div class="predictions-meta small">
            <span>${loading ? "Loading markets..." : `${shownMarkets} of ${totalMarkets} markets`}</span>
            <span>${normalized.fetchedAt ? `Updated ${escapeHtml(formatPredictionDate(normalized.fetchedAt))}` : ""}</span>
          </div>
        </div>
        ${resultBody}
      </div>
    `;
  };

  const fetchTickerPredictionsPayload = async ({ mode, query, includeClosed, signal }) => {
    if (mode === "topActive") {
      const response = await fetch("/api/polymarket/active", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ limit: 36, offset: 0, sort: "volume" }),
        signal,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(String(payload?.detail || payload?.error || "Unable to load active prediction markets."));
      }
      return normalizePredictionsPayload(payload, "top-active");
    }

    const term = String(query || "").trim();
    if (!term) {
      return normalizePredictionsPayload({ query: "", events: [] }, "");
    }

    const callSearch = async (q) => {
      const response = await fetch("/api/polymarket/search", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          q,
          limitPerType: 24,
          includeClosed: Boolean(includeClosed),
          sort: "volume",
        }),
        signal,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(String(payload?.detail || payload?.error || "Unable to load prediction markets."));
      }
      return normalizePredictionsPayload(payload, q);
    };

    const primary = await callSearch(term);
    const total = primary.events.reduce((sum, event) => sum + (Array.isArray(event.markets) ? event.markets.length : 0), 0);
    if (total > 0) return primary;

    if (/^[A-Z0-9.\-]{1,12}$/i.test(term)) {
      const fallback = await callSearch(`${term} stock`);
      const fallbackTotal = fallback.events.reduce(
        (sum, event) => sum + (Array.isArray(event.markets) ? event.markets.length : 0),
        0
      );
      if (fallbackTotal > 0) return fallback;
    }
    return primary;
  };

  const loadTickerPredictions = async (
    ticker,
    { notify = false, force = false, mode = null, query = null, includeClosed = null } = {}
  ) => {
    if (!ui.tickerPredictionsOutput) return;
    const symbol = normalizeTicker(ticker || state.tickerContext.ticker || state.tickerContext.intelTicker || "");
    const previousTicker = normalizeTicker(state.tickerContext.predictionsTicker || "");
    state.tickerContext.predictionsTicker = symbol;
    if (mode === "ticker" || mode === "topActive") {
      state.tickerContext.predictionsMode = mode;
    }
    if (typeof includeClosed === "boolean") {
      state.tickerContext.predictionsIncludeClosed = includeClosed;
    }
    if (typeof query === "string") {
      state.tickerContext.predictionsQuery = query.trim().slice(0, 80);
    } else if (state.tickerContext.predictionsMode !== "topActive") {
      const currentQuery = String(state.tickerContext.predictionsQuery || "").trim();
      if (symbol && symbol !== previousTicker) {
        state.tickerContext.predictionsQuery = symbol;
      } else if (!currentQuery || currentQuery.toUpperCase() === previousTicker) {
        state.tickerContext.predictionsQuery = symbol;
      }
    }

    const panel = getPredictionsPanelState(symbol);
    const requestKey = buildPredictionsCacheKey(panel);

    if (!force && requestKey && state.tickerContext.predictionsRequestKey === requestKey && state.tickerContext.predictionsData) {
      renderPredictionsOutput({ payload: state.tickerContext.predictionsData, ticker: symbol });
      return;
    }

    const cached = !force ? getPredictionsCache(requestKey) : null;
    if (cached) {
      state.tickerContext.predictionsData = cached;
      state.tickerContext.predictionsRequestKey = requestKey;
      renderPredictionsOutput({ payload: cached, ticker: symbol });
      return;
    }

    if (panel.mode === "ticker" && !panel.query) {
      state.tickerContext.predictionsData = normalizePredictionsPayload({ query: "", events: [] }, "");
      state.tickerContext.predictionsRequestKey = requestKey;
      renderPredictionsOutput({ payload: state.tickerContext.predictionsData, ticker: symbol });
      return;
    }

    renderPredictionsOutput({
      payload: state.tickerContext.predictionsData,
      ticker: symbol,
      loading: true,
    });
    logEvent("polymarket_load_started", {
      ticker: panel.ticker || panel.query || "",
      mode: panel.mode,
      force: Boolean(force),
    });

    if (polymarketInFlightController) {
      try {
        polymarketInFlightController.abort();
      } catch (error) {
        // Best effort.
      }
      polymarketInFlightController = null;
    }

    const requestNonce = (polymarketInFlightNonce += 1);
    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    polymarketInFlightController = controller;

    try {
      const payload = await fetchTickerPredictionsPayload({
        mode: panel.mode,
        query: panel.query,
        includeClosed: panel.includeClosed,
        signal: controller?.signal,
      });
      if (requestNonce !== polymarketInFlightNonce) return;
      state.tickerContext.predictionsData = payload;
      state.tickerContext.predictionsRequestKey = requestKey;
      setPredictionsCache(requestKey, payload);
      renderPredictionsOutput({ payload, ticker: symbol });
      const marketCount = payload.events.reduce((sum, event) => sum + (Array.isArray(event.markets) ? event.markets.length : 0), 0);
      logEvent("predictions_loaded", {
        ticker: panel.ticker || panel.query || "",
        mode: panel.mode,
        markets: marketCount,
      });
    } catch (error) {
      const aborted = controller?.signal?.aborted;
      if (aborted) return;
      renderPredictionsOutput({
        payload: state.tickerContext.predictionsData,
        ticker: symbol,
        error: String(error?.message || "Unable to load predictions."),
      });
      logEvent("polymarket_load_error", {
        ticker: panel.ticker || panel.query || "",
        mode: panel.mode,
        message: String(error?.message || "load_failed").slice(0, 120),
      });
      if (notify) showToast(error.message || "Unable to load predictions.", "warn");
    } finally {
      if (requestNonce === polymarketInFlightNonce) {
        polymarketInFlightController = null;
      }
    }
  };

  const bindPredictionsPanelInteractions = () => {
    if (!ui.tickerPredictionsOutput || ui.tickerPredictionsOutput.dataset.boundPredictions === "1") return;
    ui.tickerPredictionsOutput.dataset.boundPredictions = "1";

    ui.tickerPredictionsOutput.addEventListener("click", (event) => {
      const tabButton = event.target.closest("[data-action='predictions-tab']");
      if (tabButton) {
        event.preventDefault();
        triggerSubtleHaptic();
        const nextMode = String(tabButton.dataset.mode || "ticker");
        state.tickerContext.predictionsExpanded = false;
        const activeTicker = normalizeTicker(state.tickerContext.ticker || state.tickerContext.intelTicker || "");
        if (nextMode === "ticker" && !String(state.tickerContext.predictionsQuery || "").trim()) {
          state.tickerContext.predictionsQuery = activeTicker;
        }
        loadTickerPredictions(activeTicker, { mode: nextMode, force: true, notify: false }).catch(() => {});
        return;
      }

      const showMore = event.target.closest("[data-action='predictions-show-more']");
      if (showMore) {
        event.preventDefault();
        state.tickerContext.predictionsExpanded = !state.tickerContext.predictionsExpanded;
        const activeTicker = normalizeTicker(state.tickerContext.ticker || state.tickerContext.intelTicker || "");
        renderPredictionsOutput({ payload: state.tickerContext.predictionsData, ticker: activeTicker });
        return;
      }

      const searchNow = event.target.closest("[data-action='predictions-search-now']");
      if (searchNow) {
        event.preventDefault();
        state.tickerContext.predictionsMode = "ticker";
        state.tickerContext.predictionsExpanded = false;
        const queryInput = ui.tickerPredictionsOutput.querySelector("[data-action='predictions-query']");
        const queryValue = String(queryInput?.value || "").trim().slice(0, 80);
        state.tickerContext.predictionsQuery = queryValue;
        const activeTicker = normalizeTicker(state.tickerContext.ticker || state.tickerContext.intelTicker || "");
        loadTickerPredictions(activeTicker, {
          mode: "ticker",
          query: queryValue,
          force: true,
          notify: true,
        }).catch(() => {});
      }
    });

    ui.tickerPredictionsOutput.addEventListener("input", (event) => {
      const input = event.target.closest("[data-action='predictions-query']");
      if (!input) return;
      const queryValue = String(input.value || "").trim().slice(0, 80);
      state.tickerContext.predictionsMode = "ticker";
      state.tickerContext.predictionsQuery = queryValue;
      state.tickerContext.predictionsExpanded = false;
      if (polymarketSearchDebounceTimer) window.clearTimeout(polymarketSearchDebounceTimer);
      polymarketSearchDebounceTimer = window.setTimeout(() => {
        const activeTicker = normalizeTicker(state.tickerContext.ticker || state.tickerContext.intelTicker || "");
        loadTickerPredictions(activeTicker, {
          mode: "ticker",
          query: queryValue,
          force: true,
          notify: false,
        }).catch(() => {});
      }, POLYMARKET_SEARCH_DEBOUNCE_MS);
    });

    ui.tickerPredictionsOutput.addEventListener("change", (event) => {
      const toggle = event.target.closest("[data-action='predictions-include-closed']");
      if (!toggle) return;
      const checked = Boolean(toggle.checked);
      state.tickerContext.predictionsIncludeClosed = checked;
      state.tickerContext.predictionsExpanded = false;
      const activeTicker = normalizeTicker(state.tickerContext.ticker || state.tickerContext.intelTicker || "");
      loadTickerPredictions(activeTicker, {
        includeClosed: checked,
        force: true,
        notify: false,
      }).catch(() => {});
    });
  };

  const setTickerIntelTab = (tab, { ensureLoaded = true } = {}) => {
    const next = tab === "predictions" ? "predictions" : "intelligence";
    const previous = state.intelActiveTab || "";
    state.intelActiveTab = next;

    try {
      const params = new URLSearchParams(window.location.search || "");
      if (next === "predictions") params.set("intel", "predictions");
      else params.delete("intel");
      const nextSearch = params.toString();
      const nextUrl = `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ""}`;
      if (nextUrl !== `${window.location.pathname}${window.location.search}`) {
        history.replaceState(history.state || {}, "", nextUrl);
      }
    } catch (error) {
      // ignore
    }

    if (ui.tickerIntelligenceOutput) {
      ui.tickerIntelligenceOutput.classList.toggle("hidden", next !== "intelligence");
    }
    if (ui.tickerPredictionsOutput) {
      ui.tickerPredictionsOutput.classList.toggle("hidden", next !== "predictions");
    }
    ui.tickerIntelTabs.forEach((button) => {
      const active = String(button.dataset.intelTab || "") === next;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });

    if (previous !== next) {
      logEvent("ticker_intel_tab_selected", {
        tab: next,
        previous_tab: previous || "",
        ticker: normalizeTicker(state.tickerContext.ticker || state.tickerContext.intelTicker || ""),
      });
    }

    if (next === "predictions" && ensureLoaded) {
      const activeTicker = normalizeTicker(state.tickerContext.ticker || state.tickerContext.intelTicker || "");
      if (activeTicker) {
        logEvent("polymarket_tab_opened", { ticker: activeTicker });
        loadTickerPredictions(activeTicker, { notify: false }).catch(() => {});
      } else {
        renderPredictionsOutput({ payload: state.tickerContext.predictionsData, ticker: "" });
      }
    }
  };

  const bindTickerIntelTabs = () => {
    bindPredictionsPanelInteractions();
    if (!ui.tickerIntelTabs.length) return;
    ui.tickerIntelTabs.forEach((button) => {
      if (button.dataset.bound === "1") return;
      button.dataset.bound = "1";
      button.addEventListener("click", () => {
        triggerSubtleHaptic();
        setTickerIntelTab(String(button.dataset.intelTab || "intelligence"));
      });
    });
    const initialTab = (() => {
      try {
        const params = new URLSearchParams(window.location.search || "");
        const intel = String(params.get("intel") || "").trim().toLowerCase();
        if (intel === "predictions") return "predictions";
      } catch (error) {
        // ignore
      }
      return state.intelActiveTab || "intelligence";
    })();
    setTickerIntelTab(initialTab, { ensureLoaded: false });
  };

  const fetchTickerIntelPayload = async (functions, ticker, { force = false } = {}) => {
    const symbol = normalizeTicker(ticker);
    if (!symbol) throw new Error("Ticker is required.");
    let callableError = null;
    if (functions?.httpsCallable) {
      try {
        const getIntel = functions.httpsCallable("get_ticker_intel");
        const result = await getIntel({ ticker: symbol, force: Boolean(force), meta: buildMeta() });
        const payload = result?.data && typeof result.data === "object" ? result.data : {};
        return payload;
      } catch (error) {
        callableError = error;
      }
    }

    const headers = await buildApiAuthHeaders({ includeJson: false });
    const params = new URLSearchParams();
    params.set("ticker", symbol);
    if (force) params.set("force", "1");
    const response = await fetch(`/api/ticker/intel?${params.toString()}`, {
      method: "GET",
      headers,
      credentials: "same-origin",
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      const detail = String(payload?.detail || payload?.error || "").trim();
      if (callableError?.message && detail) {
        throw new Error(`${callableError.message} (${detail})`);
      }
      if (detail) throw new Error(detail);
      if (callableError?.message) throw new Error(callableError.message);
      throw new Error("Unable to load ticker intelligence.");
    }
    const payload = await response.json().catch(() => ({}));
    return payload && typeof payload === "object" ? payload : {};
  };

  const loadTickerIntel = async (functions, ticker, { notify = false, force = false } = {}) => {
    if ((!functions && !window.fetch) || (!ui.intelOutput && !ui.tickerIntelligenceOutput)) return;
    const symbol = normalizeTicker(ticker);
    if (!symbol) {
      if (ui.intelOutput) ui.intelOutput.innerHTML = `<div class="small muted">Load a ticker to see company context.</div>`;
      if (ui.tickerIntelligenceOutput) {
        ui.tickerIntelligenceOutput.innerHTML = `<div class="small muted">Load a ticker to generate institutional intelligence.</div>`;
      }
      if (ui.tickerPredictionsOutput) {
        state.tickerContext.predictionsTicker = "";
        state.tickerContext.predictionsQuery = "";
        state.tickerContext.predictionsData = normalizePredictionsPayload({ query: "", events: [] }, "");
        state.tickerContext.predictionsRequestKey = "";
        renderPredictionsOutput({ payload: state.tickerContext.predictionsData, ticker: "" });
      }
      return;
    }
    if (!force && state.tickerContext.intelTicker === symbol) return;
    state.tickerContext.intelTicker = symbol;

    try {
      if (ui.intelOutput) setOutputLoading(ui.intelOutput, "Loading company context...");
      if (ui.tickerIntelligenceOutput) setOutputLoading(ui.tickerIntelligenceOutput, "Loading institutional intelligence...");

      const intelPayload = await fetchTickerIntelPayload(functions, symbol, { force });
      if (ui.intelOutput) setOutputReady(ui.intelOutput);
      if (ui.tickerIntelligenceOutput) setOutputReady(ui.tickerIntelligenceOutput);
      renderTickerIntel(intelPayload || {});
      if (state.intelActiveTab === "predictions") {
        loadTickerPredictions(symbol, { notify: false, force }).catch(() => {});
      }
      logEvent("ticker_intel_loaded", { ticker: symbol });
    } catch (error) {
      if (ui.intelOutput) {
        setOutputReady(ui.intelOutput);
        ui.intelOutput.innerHTML = `<div class="small muted">Unable to load ticker intelligence right now.</div>`;
      }
      if (ui.tickerIntelligenceOutput) {
        setOutputReady(ui.tickerIntelligenceOutput);
        ui.tickerIntelligenceOutput.innerHTML = `<div class="small muted">Unable to load institutional intelligence right now.</div>`;
      }
      if (ui.tickerPredictionsOutput && state.intelActiveTab === "predictions") {
        renderPredictionsOutput({
          payload: state.tickerContext.predictionsData,
          ticker: symbol,
          error: "Unable to load predictions.",
        });
      }
      if (notify) showToast(error.message || "Unable to load ticker intelligence.", "warn");
    }
  };

  const renderTickerNews = (items, ticker) => {
    if (!ui.newsOutput) return;
    const list = Array.isArray(items) ? items : [];
    const symbol = normalizeTicker(ticker) || "";

    if (!list.length) {
      ui.newsOutput.innerHTML = `
        <div class="small muted">No headlines returned for ${escapeHtml(symbol || "this ticker")}.</div>
        <div class="small" style="margin-top:10px;">Try a different symbol, or load a trending ticker and retry.</div>
      `;
      return;
    }

    ui.newsOutput.innerHTML = list
      .map((item) => {
        const title = escapeHtml(item.title || "");
        const publisher = escapeHtml(item.publisher || "");
        const published = formatEpoch(item.publishedAt);
        const summary = escapeHtml(item.summary || "");
        const link = item.link ? escapeHtml(item.link) : "";
        const thumb = item.thumbnailUrl ? escapeHtml(item.thumbnailUrl) : "";
        const meta = [publisher, published].filter(Boolean).join(" · ");
        return `
          <article class="news-card${thumb ? " news-card--with-thumb" : ""}">
            ${thumb ? `<img class="news-thumb" src="${thumb}" alt="" loading="lazy" />` : ""}
            <div class="news-body">
              <div class="news-title">${title}</div>
              <div class="news-meta small">${meta}</div>
              ${summary ? `<div class="news-summary small">${summary}</div>` : ""}
              ${link ? `<a class="news-link" href="${link}" target="_blank" rel="noreferrer">Read article</a>` : ""}
            </div>
          </article>
        `;
      })
      .join("");
  };

  const loadTickerNews = async (functions, ticker, { notify = false, force = false } = {}) => {
    if (!functions || !ui.newsOutput) return;
    const symbol = normalizeTicker(ticker);
    if (!symbol) {
      ui.newsOutput.innerHTML = `<div class="small muted">Load a ticker to see headlines.</div>`;
      return;
    }
    if (!force && state.tickerContext.newsTicker === symbol) return;
    state.tickerContext.newsTicker = symbol;

    try {
      setOutputLoading(ui.newsOutput, "Loading headlines...");
      const getNews = functions.httpsCallable("get_ticker_news");
      const result = await getNews({ ticker: symbol, meta: buildMeta() });
      const items = result.data?.news || [];
      setOutputReady(ui.newsOutput);
      renderTickerNews(items, symbol);
      logEvent("news_loaded", { ticker: symbol, count: Array.isArray(items) ? items.length : 0 });
    } catch (error) {
      setOutputReady(ui.newsOutput);
      ui.newsOutput.innerHTML = `<div class="small muted">Unable to load headlines right now.</div>`;
      if (notify) showToast(error.message || "Unable to load news.", "warn");
    }
  };

  const normalizeXSocialQuery = (raw) => String(raw || "").trim();

  const normalizeXAuthorHandle = (value) => sanitizeText(String(value || "").replace(/^@+/, ""), 80);

  const readXCount = (...values) => {
    for (const value of values) {
      const num = Number(value);
      if (Number.isFinite(num)) return Math.max(0, Math.floor(num));
    }
    return 0;
  };

  const normalizeXTrendPost = (raw) => {
    const post = raw && typeof raw === "object" ? raw : {};
    const author = post.author && typeof post.author === "object" ? post.author : {};
    const user = post.user && typeof post.user === "object" ? post.user : {};
    const account = post.account && typeof post.account === "object" ? post.account : {};
    const metricsRaw =
      (post.metrics && typeof post.metrics === "object" ? post.metrics : null) ||
      (post.public_metrics && typeof post.public_metrics === "object" ? post.public_metrics : null) ||
      (post.engagement && typeof post.engagement === "object" ? post.engagement : null) ||
      {};

    const authorUsername = normalizeXAuthorHandle(
      post.authorUsername ||
        post.author_username ||
        post.username ||
        post.screen_name ||
        post.handle ||
        author.username ||
        author.screen_name ||
        author.handle ||
        user.username ||
        user.screen_name ||
        user.handle ||
        account.username ||
        account.screen_name
    );
    const authorName =
      sanitizeText(
        post.authorName ||
          post.author_name ||
          post.authorDisplayName ||
          post.author_display_name ||
          post.name ||
          author.name ||
          author.display_name ||
          author.full_name ||
          user.name ||
          user.display_name ||
          account.name,
        120
      ) || authorUsername;

    const postId = sanitizeText(post.id || post.id_str || post.tweet_id || post.status_id, 120);
    const text = sanitizeText(post.text || post.full_text || post.content || post.body || post.title || post.snippet, 8000);
    const createdAt = post.createdAt || post.created_at || post.publishedAt || post.timestamp || post.date || null;
    const permalink =
      sanitizeText(post.permalink || post.url || post.link || post.tweetUrl || post.tweet_url, 500) ||
      (postId ? `https://x.com/i/web/status/${encodeURIComponent(postId)}` : "");

    const likes = readXCount(
      metricsRaw.like_count,
      metricsRaw.favorite_count,
      metricsRaw.likes,
      metricsRaw.likeCount,
      post.like_count,
      post.favorite_count,
      post.likes,
      post.likeCount,
      post.favoriteCount
    );
    const reposts = readXCount(
      metricsRaw.retweet_count,
      metricsRaw.repost_count,
      metricsRaw.reposts,
      metricsRaw.retweetCount,
      metricsRaw.repostCount,
      post.retweet_count,
      post.repost_count,
      post.reposts,
      post.retweetCount,
      post.repostCount
    );
    const replies = readXCount(
      metricsRaw.reply_count,
      metricsRaw.replies,
      metricsRaw.replyCount,
      post.reply_count,
      post.replies,
      post.replyCount
    );

    return {
      ...post,
      id: postId || sanitizeText(post.id, 120),
      text,
      createdAt,
      permalink,
      authorName,
      authorUsername,
      likes,
      reposts,
      replies,
      metrics: {
        ...metricsRaw,
        like_count: likes,
        retweet_count: reposts,
        reply_count: replies,
      },
    };
  };

  const uniqueSocialRows = (rows) => {
    const list = Array.isArray(rows) ? rows : [];
    const seen = new Set();
    const out = [];
    for (const row of list) {
      const key = String(row?.id || row?.permalink || `${row?.text || row?.title || ""}_${row?.createdAt || ""}`).trim();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(row);
    }
    return out;
  };

  const isTransientXTrendsError = (error) => {
    const code = String(error?.code || "").toLowerCase();
    const message = String(error?.message || "").toLowerCase();
    if (["resource-exhausted", "unavailable", "internal", "deadline-exceeded"].some((item) => code.includes(item))) return true;
    if (message.includes("429")) return true;
    if (/\b5\d\d\b/.test(message)) return true;
    if (message.includes("timeout")) return true;
    return false;
  };

  const callWithBackoffRetry = async (callable, payload, { attempts = 3, baseDelayMs = 320 } = {}) => {
    let lastError = null;
    for (let i = 0; i < Math.max(1, attempts); i += 1) {
      try {
        return await callable(payload);
      } catch (error) {
        lastError = error;
        if (!isTransientXTrendsError(error) || i >= attempts - 1) throw error;
        const jitter = Math.floor(Math.random() * 160);
        const delay = baseDelayMs * (2 ** i) + jitter;
        // Exponential backoff keeps noisy 429/5xx from surfacing as immediate hard failures.
        await new Promise((resolve) => window.setTimeout(resolve, delay));
      }
    }
    throw lastError || new Error("X trends request failed.");
  };

  const buildXVariantChips = ({ variants, activeQuery, ticker }) => {
    const symbol = normalizeTicker(ticker) || "";
    const active = normalizeXSocialQuery(activeQuery).toUpperCase();
    const chips = (Array.isArray(variants) ? variants : [])
      .map((item) => normalizeTicker(item))
      .filter(Boolean)
      .filter((item, idx, arr) => arr.indexOf(item) === idx)
      .filter((item) => item !== active)
      .slice(0, 4);
    if (!chips.length || !symbol) return "";
    return `
      <div class="x-variant-chips">
        ${chips
          .map(
            (variant) =>
              `<button class="task-chip" type="button" data-action="x-trends-variant" data-ticker="${escapeHtml(symbol)}" data-query="${escapeHtml(
                variant
              )}">Try ${escapeHtml(variant)}</button>`
          )
          .join("")}
      </div>
    `;
  };

  const renderTickerXTrends = (payload = {}) => {
    if (!ui.xTrendingOutput) return;
    const list = Array.isArray(payload.posts) ? payload.posts : [];
    const storyList = Array.isArray(payload.stories) ? payload.stories : [];
    const symbol = normalizeTicker(payload.ticker || "") || "";
    const warningText = String(payload.warning || "").trim();
    const query = normalizeXSocialQuery(payload.query || symbol);
    const fallbackUsed = Boolean(payload.fallbackUsed);
    const fallbackQuery = normalizeXSocialQuery(payload.fallbackQuery || "");
    const page = Math.max(1, Number(payload.page || 1));
    const pageSize = Math.max(1, Number(payload.pageSize || 8));
    const totalPosts = Number(payload.totalPosts || list.length || 0);
    const hasMorePosts = Boolean(payload.hasMorePosts);
    const variantChips = buildXVariantChips({
      variants: payload.queryVariants,
      activeQuery: query,
      ticker: symbol,
    });
    const warningBlock = warningText ? `<div class="small muted" style="margin-top:10px;">${escapeHtml(warningText)}</div>` : "";
    const queryBlock = query ? `<div class="small muted" style="margin-bottom:8px;">Query: ${escapeHtml(query)}</div>` : "";
    const fallbackBlock = fallbackUsed
      ? `<div class="small muted" style="margin-bottom:8px;">Fallback query used: ${escapeHtml(fallbackQuery || "$" + symbol)}</div>`
      : "";

    if (!list.length && !storyList.length) {
      ui.xTrendingOutput.innerHTML = `
        <div class="x-empty-state">
          <div class="small muted">No posts found for ${escapeHtml(symbol || "this ticker")}.</div>
          ${queryBlock}
          ${fallbackBlock}
          ${variantChips}
          <div style="margin-top:10px;">
            <button class="cta secondary small" type="button" data-action="x-trends-retry" data-ticker="${escapeHtml(symbol)}" data-query="${escapeHtml(
              query
            )}">Retry</button>
          </div>
          ${warningBlock}
        </div>
      `;
      return;
    }

    const blocks = [];
    if (queryBlock) blocks.push(queryBlock);
    if (fallbackBlock) blocks.push(fallbackBlock);

    if (list.length) {
      blocks.push(
        list
          .map((post) => {
            const authorName = escapeHtml(post.authorName || post.authorUsername || "Unknown");
            const authorHandle = escapeHtml(post.authorUsername ? `@${post.authorUsername}` : "");
            const text = escapeHtml(post.text || post.title || "");
            const created = formatEpoch(post.createdAt);
            const metrics = post.metrics && typeof post.metrics === "object" ? post.metrics : {};
            const likes = readXCount(post.likes, metrics.like_count, metrics.favorite_count, metrics.likes, metrics.likeCount);
            const reposts = readXCount(
              post.reposts,
              metrics.retweet_count,
              metrics.repost_count,
              metrics.reposts,
              metrics.retweetCount,
              metrics.repostCount
            );
            const replies = readXCount(post.replies, metrics.reply_count, metrics.replies, metrics.replyCount);
            const permalink = post.permalink ? escapeHtml(post.permalink) : "";
            return `
              <article class="x-post-card">
                <div class="x-post-top">
                  <div class="x-post-author">${authorName}</div>
                  <div class="x-post-handle small muted">${authorHandle}</div>
                </div>
                <div class="x-post-text small">${text}</div>
                <div class="x-post-meta small">
                  <span>${escapeHtml(created || "Now")}</span>
                  <span>Likes ${Number.isFinite(likes) ? likes.toLocaleString() : "0"}</span>
                  <span>Reposts ${Number.isFinite(reposts) ? reposts.toLocaleString() : "0"}</span>
                  <span>Replies ${Number.isFinite(replies) ? replies.toLocaleString() : "0"}</span>
                </div>
                ${permalink ? `<a class="x-post-link" href="${permalink}" target="_blank" rel="noreferrer">View on X</a>` : ""}
              </article>
            `;
          })
          .join("")
      );
    } else if (storyList.length) {
      blocks.push(`<div class="small muted">No matching X posts returned for ${escapeHtml(symbol || "this ticker")}.</div>`);
    }

    if (storyList.length) {
      blocks.push(`<div class="x-story-divider small muted">X News stories</div>`);
      blocks.push(
        storyList
          .map((story) => {
            const title = escapeHtml(story.name || "X News story");
            const hook = escapeHtml(story.hook || "");
            const summary = escapeHtml(story.summary || "");
            const updated = escapeHtml(formatEpoch(story.updatedAt) || "");
            const category = escapeHtml(story.category || "");
            const tickers = Array.isArray(story.tickers) ? story.tickers.filter(Boolean).slice(0, 6).join(", ") : "";
            const meta = [category, updated, tickers ? `Tickers: ${tickers}` : ""].filter(Boolean).join(" · ");
            const link = story.searchUrl ? escapeHtml(story.searchUrl) : "";
            return `
              <article class="x-story-card">
                <div class="x-story-title">${title}</div>
                ${meta ? `<div class="x-story-meta small muted">${meta}</div>` : ""}
                ${hook ? `<div class="x-story-hook small">${hook}</div>` : ""}
                ${summary && summary !== hook ? `<div class="x-story-summary small muted">${summary}</div>` : ""}
                ${link ? `<a class="x-post-link" href="${link}" target="_blank" rel="noreferrer">Open on X</a>` : ""}
              </article>
            `;
          })
          .join("")
      );
    }

    if (hasMorePosts) {
      blocks.push(`
        <div class="x-pagination">
          <button
            class="cta secondary small"
            type="button"
            data-action="x-trends-more"
            data-ticker="${escapeHtml(symbol)}"
            data-query="${escapeHtml(query)}"
            data-next-page="${page + 1}"
          >
            Load more posts
          </button>
          <span class="small muted">${Math.min(list.length, totalPosts)} / ${Number.isFinite(totalPosts) ? totalPosts : "?"}</span>
        </div>
      `);
    }

    ui.xTrendingOutput.innerHTML = blocks.join("") + variantChips + warningBlock;
  };

  const loadTickerXTrends = async (
    functions,
    ticker,
    { notify = false, force = false, page = 1, append = false, queryOverride = "", pageSize = 8 } = {}
  ) => {
    if (!functions || !ui.xTrendingOutput) return;
    const symbol = normalizeTicker(ticker);
    if (!symbol) {
      ui.xTrendingOutput.innerHTML = `<div class="small muted">Load a ticker to see live X discussion.</div>`;
      return;
    }

    const query = normalizeXSocialQuery(queryOverride);
    const normalizedPage = Math.max(1, Number(page || 1));
    const normalizedPageSize = Math.max(1, Math.min(20, Number(pageSize || 8)));
    const sameRequest = state.tickerContext.xTicker === symbol && state.tickerContext.xQuery === query;
    if (!force && !append && sameRequest && state.tickerContext.xPage === normalizedPage) return;

    if (!append) {
      state.tickerContext.xTicker = symbol;
      state.tickerContext.xQuery = query;
      state.tickerContext.xPage = 1;
      state.tickerContext.xPosts = [];
      state.tickerContext.xStories = [];
      state.tickerContext.xHasMorePosts = false;
      state.tickerContext.xHasMoreStories = false;
      state.tickerContext.xVariants = [];
      setOutputLoading(ui.xTrendingOutput, "Loading X trends...");
    }

    try {
      const getXTrends = functions.httpsCallable("get_ticker_x_trends");
      const result = await callWithBackoffRetry(getXTrends, {
        ticker: symbol,
        query,
        page: normalizedPage,
        pageSize: normalizedPageSize,
        meta: buildMeta(),
      });
      const payload = result.data || {};
      const incomingPosts = (Array.isArray(payload.posts) ? payload.posts : []).map(normalizeXTrendPost).filter(Boolean);
      const incomingStories = Array.isArray(payload.stories) ? payload.stories : [];

      const mergedPosts = append ? uniqueSocialRows([...(state.tickerContext.xPosts || []), ...incomingPosts]) : uniqueSocialRows(incomingPosts);
      const mergedStories = append
        ? uniqueSocialRows([...(state.tickerContext.xStories || []), ...incomingStories])
        : uniqueSocialRows(incomingStories);

      state.tickerContext.xTicker = symbol;
      state.tickerContext.xQuery = query;
      state.tickerContext.xPage = normalizedPage;
      state.tickerContext.xPosts = mergedPosts;
      state.tickerContext.xStories = mergedStories;
      state.tickerContext.xHasMorePosts = Boolean(payload.hasMorePosts);
      state.tickerContext.xHasMoreStories = Boolean(payload.hasMoreStories);
      state.tickerContext.xVariants = Array.isArray(payload.queryVariants) ? payload.queryVariants : [];

      setOutputReady(ui.xTrendingOutput);
      renderTickerXTrends({
        ticker: symbol,
        query: payload.query || query || symbol,
        queryVariants: state.tickerContext.xVariants,
        posts: mergedPosts,
        stories: mergedStories,
        page: normalizedPage,
        pageSize: Number(payload.pageSize || normalizedPageSize),
        hasMorePosts: state.tickerContext.xHasMorePosts,
        hasMoreStories: state.tickerContext.xHasMoreStories,
        totalPosts: Number(payload.totalPosts || mergedPosts.length || 0),
        totalStories: Number(payload.totalStories || mergedStories.length || 0),
        warning: String(payload.warning || "").trim(),
        fallbackUsed: Boolean(payload.fallbackUsed),
        fallbackQuery: String(payload.fallbackQuery || "").trim(),
      });
      logEvent("x_trends_loaded", {
        ticker: symbol,
        query: query || symbol,
        count: mergedPosts.length,
        stories: mergedStories.length,
        page: normalizedPage,
      });
    } catch (error) {
      setOutputReady(ui.xTrendingOutput);
      ui.xTrendingOutput.innerHTML = `
        <div class="small muted">Unable to load X trends right now.</div>
        <div style="margin-top:10px;">
          <button class="cta secondary small" type="button" data-action="x-trends-retry" data-ticker="${escapeHtml(symbol)}" data-query="${escapeHtml(
            query
          )}">Retry</button>
        </div>
      `;
      if (notify) showToast(error.message || "Unable to load X trends.", "warn");
    }
  };

  const normalizeTickerListInput = (raw) =>
    String(raw || "")
      .split(/[,\s]+/)
      .map((item) => normalizeTicker(item))
      .filter(Boolean)
      .slice(0, 30);

  const EARNINGS_TIMEZONE = "America/New_York";
  const EARNINGS_DAY_MS = 24 * 60 * 60 * 1000;
  const EARNINGS_PAGE_SIZE = 60;

  const toYmdUtc = (date) => {
    if (!(date instanceof Date) || !Number.isFinite(date.getTime())) return "";
    const y = date.getUTCFullYear();
    const m = String(date.getUTCMonth() + 1).padStart(2, "0");
    const d = String(date.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  };

  const parseYmdUtc = (value) => {
    const text = String(value || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
    const parsed = new Date(`${text}T00:00:00Z`);
    return Number.isFinite(parsed.getTime()) ? parsed : null;
  };

  const shiftEarningsYmd = (value, days) => {
    const base = parseYmdUtc(value);
    if (!base) return "";
    return toYmdUtc(new Date(base.getTime() + Number(days || 0) * EARNINGS_DAY_MS));
  };

  const getTimeZoneYmd = (date, timeZone) => {
    try {
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      }).formatToParts(date);
      const year = parts.find((item) => item.type === "year")?.value || "";
      const month = parts.find((item) => item.type === "month")?.value || "";
      const day = parts.find((item) => item.type === "day")?.value || "";
      if (year && month && day) return `${year}-${month}-${day}`;
    } catch (error) {
      // Ignore.
    }
    return toYmdUtc(date);
  };

  const getNyTodayYmd = () => getTimeZoneYmd(new Date(), EARNINGS_TIMEZONE);

  const getWeekStartYmd = (value) => {
    const date = parseYmdUtc(value);
    if (!date) return "";
    const day = date.getUTCDay();
    return toYmdUtc(new Date(date.getTime() - day * EARNINGS_DAY_MS));
  };

  const buildDateSpan = (from, to) => {
    const start = parseYmdUtc(from);
    const end = parseYmdUtc(to);
    if (!start || !end || start.getTime() > end.getTime()) return [];
    const out = [];
    for (let cursor = start.getTime(); cursor <= end.getTime(); cursor += EARNINGS_DAY_MS) {
      out.push(toYmdUtc(new Date(cursor)));
    }
    return out;
  };

  const formatRangeLabel = (from, to) => {
    const start = parseYmdUtc(from);
    const end = parseYmdUtc(to);
    if (!start || !end) return "—";
    const fmt = new Intl.DateTimeFormat("en-US", {
      timeZone: "UTC",
      month: "short",
      day: "numeric",
      year: "numeric",
    });
    return `${fmt.format(start)} – ${fmt.format(end)}`;
  };

  const formatDayLabel = (value) => {
    const date = parseYmdUtc(value);
    if (!date) return { weekday: "—", day: "—" };
    return {
      weekday: new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short" }).format(date),
      day: new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" }).format(date),
    };
  };

  const parseNullableNumber = (value) => {
    if (value === null || value === undefined) return null;
    const text = String(value).trim();
    if (!text || text.toLowerCase() === "null" || text === "—") return null;
    const num = Number(text.replace(/,/g, ""));
    return Number.isFinite(num) ? num : null;
  };

  const normalizeCallTime = (value) => {
    const text = sanitizeText(value, 60).toUpperCase();
    if (!text) return "—";
    if (text.includes("BMO") || text.includes("BEFORE")) return "BMO";
    if (text.includes("AMC") || text.includes("AFTER")) return "AMC";
    if (text.includes("TAS") || text.includes("TIME NOT SUPPLIED")) return "TAS";
    return text;
  };

  const computeSurprisePct = (epsActual, epsEstimate) => {
    if (!Number.isFinite(epsActual) || !Number.isFinite(epsEstimate) || Math.abs(epsEstimate) < 1e-9) return null;
    return ((epsActual - epsEstimate) / Math.abs(epsEstimate)) * 100;
  };

  const normalizeEarningsRow = (raw) => {
    const row = raw && typeof raw === "object" ? raw : {};
    const symbol = normalizeTicker(row.symbol || row.ticker);
    const dateRaw = sanitizeText(row.date || row.reportDate, 20);
    const date = /^\d{4}-\d{2}-\d{2}$/.test(dateRaw) ? dateRaw : "";
    if (!symbol || !date) return null;
    const company = sanitizeText(row.name || row.company || row.companyName, 180) || symbol;
    const epsEstimate = parseNullableNumber(row.epsEstimated ?? row.epsEstimate ?? row.estimate);
    const epsActual = parseNullableNumber(row.epsActual ?? row.eps);
    const providedSurprise = parseNullableNumber(row.epsSurprisePercentage ?? row.surprisePercent ?? row.surprise);
    const surprisePct = Number.isFinite(providedSurprise) ? providedSurprise : computeSurprisePct(epsActual, epsEstimate);
    return {
      date,
      symbol,
      company,
      eventName: sanitizeText(row.eventName || row.event || "Earnings", 120) || "Earnings",
      callTime: normalizeCallTime(row.callTime || row.time || row.hour || row.when),
      epsEstimate,
      epsActual,
      surprisePct: Number.isFinite(surprisePct) ? surprisePct : null,
      market: sanitizeText(row.market || row.exchange, 80) || null,
    };
  };

  const buildRowsByDate = (rows) => {
    const out = {};
    (Array.isArray(rows) ? rows : []).forEach((raw) => {
      const row = normalizeEarningsRow(raw);
      if (!row) return;
      if (!out[row.date]) out[row.date] = [];
      out[row.date].push(row);
    });
    Object.keys(out).forEach((date) => {
      out[date].sort((a, b) => a.symbol.localeCompare(b.symbol));
    });
    return out;
  };

  const getEarningsFollowStorageKey = () => {
    const uid = String(state.user?.uid || "anon").trim() || "anon";
    return `quantura:earnings:follows:${uid}`;
  };

  const readLocalEarningsFollows = () => {
    try {
      const raw = String(safeLocalStorageGet(getEarningsFollowStorageKey()) || "").trim();
      if (!raw) return new Set();
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return new Set();
      return new Set(parsed.map((item) => normalizeTicker(item)).filter(Boolean));
    } catch (error) {
      return new Set();
    }
  };

  const writeLocalEarningsFollows = (set) => {
    const out = Array.from(set || []).map((item) => normalizeTicker(item)).filter(Boolean);
    safeLocalStorageSet(getEarningsFollowStorageKey(), JSON.stringify(Array.from(new Set(out))));
  };

  const loadEarningsFollowSet = async ({ force = false } = {}) => {
    const db = state.clients?.db;
    const uid = String(state.user?.uid || "").trim();
    if (!db || !uid) {
      state.earningsCalendar.follows = readLocalEarningsFollows();
      state.earningsCalendar.followsUid = uid || "anon";
      return state.earningsCalendar.follows;
    }
    if (!force && state.earningsCalendar.followsUid === uid && state.earningsCalendar.follows instanceof Set) {
      return state.earningsCalendar.follows;
    }
    try {
      const snap = await db.collection("users").doc(uid).collection("earnings_follows").limit(1200).get();
      const follows = new Set(
        snap.docs
          .map((doc) => normalizeTicker(doc.id || doc.data()?.symbol))
          .filter(Boolean)
      );
      state.earningsCalendar.follows = follows;
      state.earningsCalendar.followsUid = uid;
      writeLocalEarningsFollows(follows);
      return follows;
    } catch (error) {
      const fallback = readLocalEarningsFollows();
      state.earningsCalendar.follows = fallback;
      state.earningsCalendar.followsUid = uid || "anon";
      return fallback;
    }
  };

  const resolveEarningsRangeFromPreset = (preset) => {
    const currentPreset = String(preset || "this-week").trim().toLowerCase();
    const nyToday = getNyTodayYmd();
    const thisWeekStart = getWeekStartYmd(nyToday);
    if (currentPreset === "next-week") {
      const start = shiftEarningsYmd(thisWeekStart, 7);
      return { start, end: shiftEarningsYmd(start, 6), stepDays: 7 };
    }
    if (currentPreset === "last-week") {
      const start = shiftEarningsYmd(thisWeekStart, -7);
      return { start, end: shiftEarningsYmd(start, 6), stepDays: 7 };
    }
    if (currentPreset === "next-30-days") {
      return { start: nyToday, end: shiftEarningsYmd(nyToday, 29), stepDays: 30 };
    }
    return { start: thisWeekStart, end: shiftEarningsYmd(thisWeekStart, 6), stepDays: 7 };
  };

  const ensureEarningsRangeState = () => {
    if (state.earningsCalendar.rangeStart && state.earningsCalendar.rangeEnd) {
      return {
        start: state.earningsCalendar.rangeStart,
        end: state.earningsCalendar.rangeEnd,
        stepDays: state.earningsCalendar.preset === "next-30-days" ? 30 : 7,
      };
    }
    const initial = resolveEarningsRangeFromPreset(state.earningsCalendar.preset);
    state.earningsCalendar.rangeStart = initial.start;
    state.earningsCalendar.rangeEnd = initial.end;
    return initial;
  };

  const getFilteredRowsByDate = () => {
    const query = String(state.earningsCalendar.search || "").trim().toLowerCase();
    const rowsByDate = state.earningsCalendar.rowsByDate || {};
    const out = {};
    (state.earningsCalendar.rangeDates || []).forEach((date) => {
      const rows = Array.isArray(rowsByDate[date]) ? rowsByDate[date] : [];
      if (!query) {
        out[date] = rows;
        return;
      }
      out[date] = rows.filter((row) => {
        const symbol = String(row?.symbol || "").toLowerCase();
        const company = String(row?.company || "").toLowerCase();
        return symbol.includes(query) || company.includes(query);
      });
    });
    return out;
  };

  const renderEarningsDayStrip = (filteredByDate) => {
    if (!ui.eventsCalendarDayStrip) return;
    const dates = Array.isArray(state.earningsCalendar.rangeDates) ? state.earningsCalendar.rangeDates : [];
    if (!dates.length) {
      ui.eventsCalendarDayStrip.innerHTML = `<div class="small muted">No days in selected range.</div>`;
      return;
    }
    ui.eventsCalendarDayStrip.innerHTML = dates
      .map((date) => {
        const labels = formatDayLabel(date);
        const count = Array.isArray(filteredByDate?.[date]) ? filteredByDate[date].length : 0;
        const isActive = String(state.earningsCalendar.selectedDate || "") === date;
        return `
          <button class="earnings-day-tile${isActive ? " is-active" : ""}" type="button" role="tab" aria-selected="${
            isActive ? "true" : "false"
          }" data-earnings-date="${escapeHtml(date)}">
            <span class="earnings-day-weekday">${escapeHtml(labels.weekday)}</span>
            <span class="earnings-day-date">${escapeHtml(labels.day)}</span>
            <span class="earnings-day-badge">${count} Earnings</span>
          </button>
        `;
      })
      .join("");
  };

  const renderEarningsTable = (filteredByDate) => {
    if (!ui.eventsCalendarOutput) return;
    const selectedDate = String(state.earningsCalendar.selectedDate || "");
    const selectedRowsRaw = Array.isArray(filteredByDate?.[selectedDate]) ? filteredByDate[selectedDate] : [];
    const selectedRows = selectedRowsRaw.slice(0, 500);
    const page = Math.max(1, Number(state.earningsCalendar.pageByDate?.[selectedDate] || 1));
    const visibleCount = Math.min(selectedRows.length, page * EARNINGS_PAGE_SIZE);
    const visibleRows = selectedRows.slice(0, visibleCount);

    if (ui.eventsCalendarSelectedDayTitle) {
      const label = formatDayLabel(selectedDate);
      ui.eventsCalendarSelectedDayTitle.textContent = selectedDate
        ? `Earnings on ${label.day} (${selectedRows.length})`
        : "Earnings on —";
    }

    if (!selectedRows.length) {
      ui.eventsCalendarOutput.innerHTML = `<div class="small muted">No earnings found for the selected day.</div>`;
      return;
    }

    const follows = state.earningsCalendar.follows instanceof Set ? state.earningsCalendar.follows : new Set();
    const rowsHtml = visibleRows
      .map((row) => {
        const followed = follows.has(row.symbol);
        const epsEstimate = Number.isFinite(row.epsEstimate) ? row.epsEstimate.toFixed(2) : "—";
        const epsActual = Number.isFinite(row.epsActual) ? row.epsActual.toFixed(2) : "—";
        const surprise = Number.isFinite(row.surprisePct) ? `${row.surprisePct > 0 ? "+" : ""}${row.surprisePct.toFixed(1)}%` : "—";
        const surpriseClass = Number.isFinite(row.surprisePct)
          ? row.surprisePct > 0
            ? "is-positive"
            : row.surprisePct < 0
            ? "is-negative"
            : ""
          : "";
        return `
          <tr>
            <td data-label="Follow">
              <button
                class="earnings-follow-star${followed ? " is-active" : ""}"
                type="button"
                data-earnings-follow="${escapeHtml(row.symbol)}"
                aria-label="${followed ? "Unfollow" : "Follow"} ${escapeHtml(row.symbol)}"
                title="${followed ? "Unfollow" : "Follow"} ${escapeHtml(row.symbol)}"
              >
                ${followed ? "★" : "☆"}
              </button>
            </td>
            <td data-label="Symbol"><strong>${escapeHtml(row.symbol)}</strong></td>
            <td data-label="Company">${escapeHtml(row.company || "—")}</td>
            <td data-label="Event">${escapeHtml(row.eventName || "Earnings")}</td>
            <td data-label="Call Time">${escapeHtml(row.callTime || "—")}</td>
            <td data-label="EPS Estimate">${escapeHtml(epsEstimate)}</td>
            <td data-label="Reported EPS">${escapeHtml(epsActual)}</td>
            <td data-label="Surprise %" class="earnings-surprise ${surpriseClass}">${escapeHtml(surprise)}</td>
            <td data-label="Market">${escapeHtml(row.market || "—")}</td>
          </tr>
        `;
      })
      .join("");

    const hasMore = visibleCount < selectedRows.length;
    ui.eventsCalendarOutput.innerHTML = `
      <div class="earnings-table-wrap">
        <table class="data-table earnings-data-table">
          <thead>
            <tr>
              <th>Follow</th>
              <th>Symbol</th>
              <th>Company</th>
              <th>Event Name</th>
              <th>Earnings Call Time</th>
              <th>EPS Estimate</th>
              <th>Reported EPS</th>
              <th>Surprise %</th>
              <th>Market/Exchange</th>
            </tr>
          </thead>
          <tbody>
            ${rowsHtml}
          </tbody>
        </table>
      </div>
      ${
        hasMore
          ? `<div class="earnings-table-footer"><button class="cta secondary small" type="button" data-earnings-load-more="${escapeHtml(
              selectedDate
            )}">Load more</button><span class="small muted">${visibleCount} / ${selectedRows.length}</span></div>`
          : `<div class="small muted earnings-table-footer">${selectedRows.length} row${selectedRows.length === 1 ? "" : "s"}</div>`
      }
    `;
  };

  const renderEarningsCalendar = () => {
    if (!ui.eventsCalendarOutput) return;
    const rangeStart = state.earningsCalendar.rangeStart;
    const rangeEnd = state.earningsCalendar.rangeEnd;
    const rangeDates = buildDateSpan(rangeStart, rangeEnd);
    state.earningsCalendar.rangeDates = rangeDates;
    if (ui.eventsCalendarRangeLabel) ui.eventsCalendarRangeLabel.textContent = formatRangeLabel(rangeStart, rangeEnd);

    const filteredByDate = getFilteredRowsByDate();
    const selectedDateExists = rangeDates.includes(String(state.earningsCalendar.selectedDate || ""));
    if (!selectedDateExists) {
      const firstWithRows = rangeDates.find((date) => (filteredByDate[date] || []).length > 0);
      state.earningsCalendar.selectedDate = firstWithRows || rangeDates[0] || "";
    }
    if (!state.earningsCalendar.pageByDate) state.earningsCalendar.pageByDate = {};
    if (!state.earningsCalendar.pageByDate[state.earningsCalendar.selectedDate]) {
      state.earningsCalendar.pageByDate[state.earningsCalendar.selectedDate] = 1;
    }

    renderEarningsDayStrip(filteredByDate);
    renderEarningsTable(filteredByDate);
  };

  const loadEarningsCalendar = async ({ force = false, notify = false } = {}) => {
    if (!ui.eventsCalendarOutput) return;
    const range = ensureEarningsRangeState();
    const start = range.start;
    const end = range.end;
    const cacheKey = `${start}_${end}`;

    await loadEarningsFollowSet({ force: false });

    const cached = state.earningsCalendar.requestCache.get(cacheKey);
    if (!force && cached && Array.isArray(cached.rows)) {
      state.earningsCalendar.rows = cached.rows;
      state.earningsCalendar.rowsByDate = buildRowsByDate(cached.rows);
      state.earningsCalendar.rangeStart = start;
      state.earningsCalendar.rangeEnd = end;
      if (ui.eventsCalendarStatus) {
        ui.eventsCalendarStatus.textContent = `Showing ${cached.rows.length} earnings rows (cached).`;
      }
      renderEarningsCalendar();
      return;
    }

    if (state.earningsCalendar.inFlightController) {
      try {
        state.earningsCalendar.inFlightController.abort();
      } catch (error) {
        // Ignore.
      }
    }
    const controller = new AbortController();
    state.earningsCalendar.inFlightController = controller;

    try {
      if (ui.eventsCalendarStatus) ui.eventsCalendarStatus.textContent = "Loading earnings range...";
      setOutputLoading(ui.eventsCalendarOutput, "Loading earnings calendar...");
      const headers = await buildApiAuthHeaders({ includeJson: true });
      const refreshResp = await fetch("/api/earnings/refresh", {
        method: "POST",
        headers,
        credentials: "same-origin",
        signal: controller.signal,
        body: JSON.stringify({
          start,
          end,
        }),
      });
      const refreshPayload = await refreshResp.json().catch(() => ({}));
      if (!refreshResp.ok) {
        throw new Error(String(refreshPayload?.error || "Unable to refresh earnings cache.").trim());
      }

      const normalizedRows = (Array.isArray(refreshPayload?.items) ? refreshPayload.items : [])
        .map((row) => normalizeEarningsRow(row))
        .filter(Boolean);
      state.earningsCalendar.requestCache.set(cacheKey, {
        rows: normalizedRows,
        fetchedAtMs: Number(refreshPayload?.lastFetchedAtMs || Date.now()) || Date.now(),
        lastUpdated: String(refreshPayload?.lastUpdated || "").trim(),
      });

      state.earningsCalendar.rows = normalizedRows;
      state.earningsCalendar.rowsByDate = buildRowsByDate(normalizedRows);
      state.earningsCalendar.rangeStart = start;
      state.earningsCalendar.rangeEnd = end;
      state.earningsCalendar.pageByDate = {};
      setOutputReady(ui.eventsCalendarOutput);
      renderEarningsCalendar();
      if (ui.eventsCalendarStatus) {
        ui.eventsCalendarStatus.textContent = `Loaded ${normalizedRows.length} earnings row${normalizedRows.length === 1 ? "" : "s"}.`;
      }
      logEvent("earnings_calendar_loaded", {
        rangeStart: start,
        rangeEnd: end,
        rows: normalizedRows.length,
      });
    } catch (error) {
      const aborted = String(error?.name || "").toLowerCase() === "aborterror";
      if (aborted) return;
      setOutputReady(ui.eventsCalendarOutput);
      ui.eventsCalendarOutput.innerHTML = `<div class="small muted">Unable to load earnings calendar right now.</div>`;
      if (ui.eventsCalendarStatus) ui.eventsCalendarStatus.textContent = "Unable to load earnings calendar.";
      if (notify) showToast(error.message || "Unable to load earnings calendar.", "warn");
    } finally {
      if (state.earningsCalendar.inFlightController === controller) {
        state.earningsCalendar.inFlightController = null;
      }
    }
  };

  const shiftEarningsCalendarRange = (direction = 1) => {
    const current = ensureEarningsRangeState();
    const stepDays = current.stepDays || 7;
    const delta = Math.max(1, stepDays) * (direction >= 0 ? 1 : -1);
    state.earningsCalendar.rangeStart = shiftEarningsYmd(current.start, delta);
    state.earningsCalendar.rangeEnd = shiftEarningsYmd(current.end, delta);
    state.earningsCalendar.selectedDate = "";
    state.earningsCalendar.pageByDate = {};
  };

  const setEarningsCalendarPreset = (presetValue) => {
    const preset = String(presetValue || "this-week").trim().toLowerCase();
    const allowed = new Set(["this-week", "next-week", "last-week", "next-30-days"]);
    state.earningsCalendar.preset = allowed.has(preset) ? preset : "this-week";
    const range = resolveEarningsRangeFromPreset(state.earningsCalendar.preset);
    state.earningsCalendar.rangeStart = range.start;
    state.earningsCalendar.rangeEnd = range.end;
    state.earningsCalendar.selectedDate = "";
    state.earningsCalendar.pageByDate = {};
  };

  const toggleEarningsFollow = async (symbol) => {
    const clean = normalizeTicker(symbol);
    if (!clean) return;
    if (!(state.earningsCalendar.follows instanceof Set)) {
      state.earningsCalendar.follows = new Set();
    }
    const next = new Set(state.earningsCalendar.follows);
    const willFollow = !next.has(clean);
    if (willFollow) next.add(clean);
    else next.delete(clean);
    state.earningsCalendar.follows = next;
    writeLocalEarningsFollows(next);
    renderEarningsCalendar();

    const db = state.clients?.db;
    const uid = String(state.user?.uid || "").trim();
    if (!db || !uid) return;
    const ref = db.collection("users").doc(uid).collection("earnings_follows").doc(clean);
    try {
      if (willFollow) {
        await ref.set(
          {
            symbol: clean,
            createdAt: firebase.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
      } else {
        await ref.delete();
      }
    } catch (error) {
      // Keep local optimistic state even if network write fails.
    }
  };

  const renderMarketHeadlinesFeed = (payload) => {
    if (!ui.marketHeadlinesOutput || !ui.marketSocialOutput) return;
    const headlines = Array.isArray(payload?.headlines) ? payload.headlines : [];
    const social = payload?.social && typeof payload.social === "object" ? payload.social : {};
    const warnings = Array.isArray(payload?.warnings) ? payload.warnings.filter(Boolean) : [];
    const query = String(payload?.query || "").trim();

    if (!headlines.length) {
      ui.marketHeadlinesOutput.innerHTML = `<div class="small muted">No headlines returned.</div>`;
    } else {
      ui.marketHeadlinesOutput.innerHTML = `
        ${query ? `<div class="small muted" style="margin-bottom:10px;">Query: ${escapeHtml(query)}</div>` : ""}
        ${headlines
          .map((item) => {
            const title = escapeHtml(String(item?.title || "Headline"));
            const summary = escapeHtml(String(item?.summary || "").trim());
            const publisher = escapeHtml(String(item?.publisher || "Unknown"));
            const when = escapeHtml(formatEpoch(item?.publishedAt) || "");
            const link = escapeHtml(String(item?.link || "").trim());
            const thumb = escapeHtml(String(item?.thumbnailUrl || "").trim());
            return `
              <article class="news-card${thumb ? " news-card--with-thumb" : ""}">
                ${thumb ? `<img class="news-thumb" src="${thumb}" alt="" loading="lazy" />` : ""}
                <div class="news-body">
                  <div class="news-title">${title}</div>
                  <div class="news-meta small">${publisher}${when ? ` · ${when}` : ""}</div>
                  ${summary ? `<div class="news-summary small">${summary}</div>` : ""}
                  ${link ? `<a class="news-link" href="${link}" target="_blank" rel="noopener noreferrer">Open source</a>` : ""}
                </div>
              </article>
            `;
          })
          .join("")}
      `;
    }

    const networks = [
      ["x", "X posts"],
      ["reddit", "Reddit"],
      ["facebook", "Facebook"],
      ["instagram", "Instagram"],
    ];
    ui.marketSocialOutput.innerHTML = networks
      .map(([key, label]) => {
        const rows = Array.isArray(social[key]) ? social[key] : [];
        const content = rows.length
          ? rows
              .slice(0, 8)
              .map((row) => {
                const text = escapeHtml(String(row?.text || row?.title || "").trim() || "No text");
                const link = escapeHtml(String(row?.permalink || "").trim());
                const author = escapeHtml(
                  String(row?.authorUsername || row?.author || row?.authorName || row?.subreddit || "").trim()
                );
                const metrics =
                  key === "x"
                    ? `Likes ${Number((row?.metrics || {}).like_count || 0).toLocaleString()}`
                    : key === "reddit"
                    ? `Score ${Number(row?.score || 0).toLocaleString()}`
                    : "";
                return `
                  <article class="x-post-card">
                    <div class="x-post-top">
                      <div class="x-post-author">${author || escapeHtml(label)}</div>
                    </div>
                    <div class="x-post-text small">${text}</div>
                    ${metrics ? `<div class="x-post-meta small"><span>${escapeHtml(metrics)}</span></div>` : ""}
                    ${link ? `<a class="x-post-link" href="${link}" target="_blank" rel="noopener noreferrer">Open post</a>` : ""}
                  </article>
                `;
              })
              .join("")
          : `<div class="small muted">No ${escapeHtml(label)} returned.</div>`;
        return `
          <section style="margin-bottom:14px;">
            <div class="small" style="margin-bottom:8px;"><strong>${escapeHtml(label)}</strong></div>
            ${content}
          </section>
        `;
      })
      .join("");

    if (warnings.length) {
      ui.marketSocialOutput.innerHTML += `<div class="small muted">${escapeHtml(warnings.join(" "))}</div>`;
    }
  };

  const loadMarketHeadlinesFeed = async (functions, { force = false, notify = false } = {}) => {
    if (!functions || !ui.marketHeadlinesOutput) return;
    const country = normalizeCountryCode(ui.marketHeadlinesCountry?.value || state.preferredCountry || "US");
    const limitRaw = Number(ui.marketHeadlinesLimit?.value || 18);
    const limit = Number.isFinite(limitRaw) ? Math.max(5, Math.min(40, limitRaw)) : 18;
    const requestKey = `${country}_${limit}`;
    if (!force && ui.marketHeadlinesOutput.dataset.requestKey === requestKey) return;
    ui.marketHeadlinesOutput.dataset.requestKey = requestKey;
    try {
      if (ui.marketHeadlinesStatus) ui.marketHeadlinesStatus.textContent = "Loading market feed...";
      setOutputLoading(ui.marketHeadlinesOutput, "Loading market headlines...");
      setOutputLoading(ui.marketSocialOutput, "Loading social posts...");
      const getFeed = functions.httpsCallable("get_market_headlines_feed");
      const result = await getFeed({ country, limit, meta: buildMeta() });
      setOutputReady(ui.marketHeadlinesOutput);
      setOutputReady(ui.marketSocialOutput);
      renderMarketHeadlinesFeed(result.data || {});
      if (ui.marketHeadlinesStatus) ui.marketHeadlinesStatus.textContent = `Loaded ${country} market feed.`;
      logEvent("market_headlines_loaded", { country, limit });
    } catch (error) {
      setOutputReady(ui.marketHeadlinesOutput);
      setOutputReady(ui.marketSocialOutput);
      ui.marketHeadlinesOutput.innerHTML = `<div class="small muted">Unable to load market headlines right now.</div>`;
      ui.marketSocialOutput.innerHTML = `<div class="small muted">Unable to load social feed right now.</div>`;
      if (ui.marketHeadlinesStatus) ui.marketHeadlinesStatus.textContent = "Unable to load market feed.";
      if (notify) showToast(error.message || "Unable to load market feed.", "warn");
    }
  };

  const normalizeModelCouncilProviderId = (provider) => {
    const value = String(provider || "").trim().toLowerCase();
    if (!value) return "";
    if (value === "amazon_nova") return "amazon_nova";
    if (value === "perplexity_sonar") return "perplexity";
    return value;
  };

  const modelCouncilProviderFromModel = (modelId) => {
    const id = normalizeAiModelId(modelId || "").toLowerCase();
    if (!id) return "openai";
    if (id.startsWith("amazon.nova")) return "amazon_nova";
    if (id.startsWith("gemini")) return "gemini";
    if (id.startsWith("mistral")) return "mistral";
    if (id.startsWith("sonar")) return "perplexity";
    if (id.startsWith("other/")) return "other";
    return "openai";
  };

  const tickerQueryModelGroup = (modelId, providerId = "") => {
    const provider = normalizeModelCouncilProviderId(providerId || modelCouncilProviderFromModel(modelId));
    const id = normalizeAiModelId(modelId).toLowerCase();
    if (provider === "openai") {
      if (id.includes("nano")) return "Fast";
      if (id.includes("mini")) return "Balanced";
      if (id.startsWith("gpt-5")) return "Reasoning";
    }
    if (provider === "perplexity") return "Research";
    if (provider === "other") return "Custom";
    return "Balanced";
  };

  const tickerQueryModelHint = (modelId, providerId = "") => {
    const meta = getModelMeta(modelId);
    if (meta?.helper) return String(meta.helper).trim();
    const provider = normalizeModelCouncilProviderId(providerId || modelCouncilProviderFromModel(modelId));
    const id = normalizeAiModelId(modelId).toLowerCase();
    if (provider === "openai" && id.includes("nano")) return "Lowest latency for quick scans.";
    if (provider === "openai" && id.includes("mini")) return "Best default for most questions.";
    if (provider === "gemini") return "Gemini provider path running server-side.";
    if (provider === "mistral") return "Mistral provider path running server-side.";
    if (provider === "perplexity") return "Perplexity Sonar provider path running server-side.";
    if (provider === "other") return "Custom provider routed through backend.";
    return "Higher depth reasoning with slower latency.";
  };

  const normalizeTickerQueryModules = (values) => {
    const allowed = new Set(MODEL_COUNCIL_MODULE_CATALOG.map((item) => String(item.id || "").trim()).filter(Boolean));
    const raw = Array.isArray(values)
      ? values
      : typeof values === "string"
      ? String(values)
          .split(",")
          .map((item) => item.trim())
      : [];
    const seen = new Set();
    const out = [];
    raw.forEach((item) => {
      const token = String(item || "").trim();
      if (!token || !allowed.has(token) || seen.has(token)) return;
      seen.add(token);
      out.push(token);
    });
    if (!out.length) {
      return MODEL_COUNCIL_MODULE_CATALOG.filter((item) => item.checkedByDefault).map((item) => item.id);
    }
    return out;
  };

  const getSelectedTickerQueryModules = () => {
    if (!ui.tickerQueryModulesPicker) {
      return normalizeTickerQueryModules(state.tickerContext.tickerQueryModules);
    }
    const selected = Array.from(ui.tickerQueryModulesPicker.querySelectorAll('input[type="checkbox"][data-module-id]'))
      .filter((node) => node.checked)
      .map((node) => String(node.dataset.moduleId || "").trim())
      .filter(Boolean);
    return normalizeTickerQueryModules(selected);
  };

  const setTickerQueryModulesSelection = (moduleIds, { persist = true } = {}) => {
    const normalized = normalizeTickerQueryModules(moduleIds);
    state.tickerContext.tickerQueryModules = normalized;
    if (persist) safeLocalStorageSet(TICKER_QUERY_MODULES_KEY, normalized.join(","));
    if (!ui.tickerQueryModulesPicker) return;
    const selectedSet = new Set(normalized);
    ui.tickerQueryModulesPicker.querySelectorAll('input[type="checkbox"][data-module-id]').forEach((node) => {
      const token = String(node.dataset.moduleId || "").trim();
      node.checked = selectedSet.has(token);
      const chip = node.closest(".module-picker-chip");
      if (chip && typeof chip.classList?.toggle === "function") {
        chip.classList.toggle("is-selected", node.checked);
      }
    });
  };

  const renderTickerQueryModulePicker = () => {
    if (!ui.tickerQueryModulesPicker) return;
    const stored = safeLocalStorageGet(TICKER_QUERY_MODULES_KEY) || "";
    const selected = normalizeTickerQueryModules(state.tickerContext.tickerQueryModules || stored);
    const selectedSet = new Set(selected);
    ui.tickerQueryModulesPicker.innerHTML = MODEL_COUNCIL_MODULE_CATALOG.map((module) => {
      const id = String(module.id || "").trim();
      const label = String(module.label || id);
      const selectedClass = selectedSet.has(id) ? " is-selected" : "";
      return `
        <label class="module-picker-chip${selectedClass}">
          <input type="checkbox" data-module-id="${escapeHtml(id)}" ${selectedSet.has(id) ? "checked" : ""} />
          <span>${escapeHtml(label)}</span>
        </label>
      `;
    }).join("");
    setTickerQueryModulesSelection(selected, { persist: true });
    if (ui.tickerQueryModulesPicker.dataset.bound !== "1") {
      ui.tickerQueryModulesPicker.addEventListener("change", () => {
        const next = getSelectedTickerQueryModules();
        setTickerQueryModulesSelection(next, { persist: true });
      });
      ui.tickerQueryModulesPicker.dataset.bound = "1";
    }
  };

  const createShuffledModelCouncilPromptDeck = () => {
    const deck = MODEL_COUNCIL_PROMPT_SUGGESTIONS.slice();
    for (let i = deck.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [deck[i], deck[j]] = [deck[j], deck[i]];
    }
    return deck;
  };

  const materializeModelCouncilPrompt = (template, ticker) => {
    const symbol = normalizeTicker(ticker || ui.tickerQueryTicker?.value || state.tickerContext.ticker || "") || "AAPL";
    return String(template || "").replace(/\{ticker\}/gi, symbol);
  };

  const renderModelCouncilPromptCards = ({ reshuffle = false } = {}) => {
    if (!ui.tickerQueryPromptCards) return;
    const total = MODEL_COUNCIL_PROMPT_SUGGESTIONS.length;
    if (!total) {
      ui.tickerQueryPromptCards.innerHTML = "";
      return;
    }
    if (
      reshuffle ||
      !Array.isArray(state.tickerContext.tickerQueryPromptDeck) ||
      state.tickerContext.tickerQueryPromptDeck.length !== total
    ) {
      state.tickerContext.tickerQueryPromptDeck = createShuffledModelCouncilPromptDeck();
      state.tickerContext.tickerQueryPromptCursor = 0;
    }

    const deck = state.tickerContext.tickerQueryPromptDeck;
    let cursor = Math.max(0, Number(state.tickerContext.tickerQueryPromptCursor || 0));
    if (cursor + MODEL_COUNCIL_PROMPT_VISIBLE_COUNT > deck.length) {
      state.tickerContext.tickerQueryPromptDeck = createShuffledModelCouncilPromptDeck();
      state.tickerContext.tickerQueryPromptCursor = 0;
      cursor = 0;
    }
    const visible = state.tickerContext.tickerQueryPromptDeck.slice(cursor, cursor + MODEL_COUNCIL_PROMPT_VISIBLE_COUNT);
    ui.tickerQueryPromptCards.innerHTML = visible
      .map((template) => {
        const text = materializeModelCouncilPrompt(template, ui.tickerQueryTicker?.value || state.tickerContext.ticker || "");
        return `
          <button
            class="model-council-prompt-card"
            type="button"
            data-action="model-council-prompt"
            data-template="${escapeHtml(String(template || ""))}"
            title="${escapeHtml(text)}"
          >
            ${escapeHtml(text)}
          </button>
        `;
      })
      .join("");
  };

  const renderTickerQueryModulesOutput = (moduleData, selectedModules) => {
    if (!ui.tickerQueryModulesOutput) return;
    const modules = normalizeTickerQueryModules(selectedModules);
    const data = moduleData && typeof moduleData === "object" ? moduleData : {};
    if (!modules.length) {
      ui.tickerQueryModulesOutput.classList.add("hidden");
      return;
    }
    const details = modules
      .map((moduleId) => {
        const label = MODEL_COUNCIL_MODULE_CATALOG.find((item) => item.id === moduleId)?.label || moduleId;
        const payload = data[moduleId];
        const serialized = JSON.stringify(payload ?? { message: "No data." }, null, 2);
        const clipped = serialized.length > 5000 ? `${serialized.slice(0, 5000)}\n...truncated` : serialized;
        return `
          <details class="model-council-module" open>
            <summary>${escapeHtml(label)}</summary>
            <pre class="small">${escapeHtml(clipped)}</pre>
          </details>
        `;
      })
      .join("");
    ui.tickerQueryModulesOutput.innerHTML = `
      <div class="small"><strong>Selected yfinance modules</strong></div>
      <div class="model-council-modules-stack">${details}</div>
    `;
    ui.tickerQueryModulesOutput.classList.remove("hidden");
  };

  const renderTickerQueryResult = (payload) => {
    if (!ui.tickerQueryOutput) return;
    const answer = escapeHtml(String(payload?.answer || "").trim() || "No answer returned.");
    const modelRaw = String(payload?.model || "gpt-5-mini");
    const providerRaw = normalizeModelCouncilProviderId(payload?.provider || modelCouncilProviderFromModel(modelRaw) || "openai");
    const model = escapeHtml(modelRaw);
    const provider = escapeHtml(MODEL_PROVIDER_LABEL[providerRaw] || providerRaw || "OpenAI");
    const context = payload?.context && typeof payload.context === "object" ? payload.context : {};
    const logoUrlRaw = String(context.logoUrl || context.logo_url || "").trim();
    const logoUrl = /^https?:\/\//i.test(logoUrlRaw) ? logoUrlRaw : "";
    const sector = escapeHtml(String(context.sector || "").trim());
    const industry = escapeHtml(String(context.industry || "").trim());
    const exchange = escapeHtml(String(context.exchange || "").trim());
    const headlines = Array.isArray(context.headlines) ? context.headlines : [];
    const citations = Array.isArray(payload?.citations) ? payload.citations : [];
    const responseId = String(payload?.responseId || state.tickerContext.tickerQueryLastResponseId || "").trim();
    const feedbackState = String(payload?.feedback || state.tickerContext.tickerQueryFeedback || "").trim().toLowerCase();
    const shareUrl = String(payload?.shareUrl || state.tickerContext.tickerQueryShareUrl || "").trim();

    ui.tickerQueryOutput.innerHTML = `
      <div class="small muted" style="display:flex; align-items:center; gap:10px;">
        ${logoUrl ? `<img src="${escapeHtml(logoUrl)}" alt="" loading="lazy" style="width:20px; height:20px; border-radius:50%; object-fit:cover;" />` : ""}
        <span>Provider: ${provider} · Model: ${model}</span>
      </div>
      <div class="small" style="margin-top:10px; white-space:pre-wrap;">${answer}</div>
      <div class="model-council-actions">
        <button class="task-chip${feedbackState === "like" ? " active" : ""}" type="button" data-action="model-council-like" data-response-id="${escapeHtml(responseId)}">Like</button>
        <button class="task-chip${feedbackState === "dislike" ? " active" : ""}" type="button" data-action="model-council-dislike" data-response-id="${escapeHtml(responseId)}">Dislike</button>
        <button class="task-chip" type="button" data-action="model-council-share" data-response-id="${escapeHtml(responseId)}" ${responseId ? "" : "disabled"}>${icon("share-ios")}<span>Share link</span></button>
      </div>
      ${shareUrl ? `<div class="small muted">Shared: <a href="${escapeHtml(shareUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(shareUrl)}</a></div>` : ""}
      <p class="small muted solve-now-disclaimer">${escapeHtml(MODEL_COUNCIL_OUTPUT_DISCLAIMER)}</p>
      <div class="small muted" style="margin-top:10px;">
        ${sector ? `Sector: ${sector}` : ""}${industry ? ` · Industry: ${industry}` : ""}${exchange ? ` · Exchange: ${exchange}` : ""}
      </div>
      ${
        headlines.length
          ? `<div style="margin-top:12px;">
              <div class="small"><strong>Context headlines</strong></div>
              <ul class="small" style="margin:6px 0 0 16px;">
                ${headlines
                  .slice(0, 5)
                  .map((item) => {
                    const title = escapeHtml(String(item?.title || "").trim());
                    const link = escapeHtml(String(item?.link || "").trim());
                    return `<li>${link ? `<a class="news-link" href="${link}" target="_blank" rel="noopener noreferrer">${title}</a>` : title}</li>`;
                  })
                  .join("")}
              </ul>
            </div>`
          : ""
      }
      ${
        citations.length
          ? `<div style="margin-top:12px;">
              <div class="small"><strong>Citations</strong></div>
              <ul class="small" style="margin:6px 0 0 16px;">
                ${citations
                  .slice(0, 8)
                  .map((item) => {
                    const title = escapeHtml(String(item?.title || item?.url || "Source").trim());
                    const url = escapeHtml(String(item?.url || "").trim());
                    return `<li>${url ? `<a class="news-link" href="${url}" target="_blank" rel="noopener noreferrer">${title}</a>` : title}</li>`;
                  })
                  .join("")}
              </ul>
            </div>`
          : ""
      }
    `;
  };

  const renderTickerQueryErrorState = ({ message = "", retryProvider = "", retryModel = "" } = {}) => {
    if (!ui.tickerQueryOutput) return;
    const text = escapeHtml(String(message || "Unable to complete Model Council request right now."));
    const provider = escapeHtml(String(retryProvider || "").trim());
    const model = escapeHtml(String(retryModel || "").trim());
    ui.tickerQueryOutput.innerHTML = `
      <div class="small muted">${text}</div>
      ${
        provider && model
          ? `<button class="cta secondary small" type="button" data-action="model-council-retry" data-provider="${provider}" data-model="${model}" style="margin-top:12px;">
              ${icon("refresh")}<span>Retry with ${provider}/${model}</span>
            </button>`
          : ""
      }
      <p class="small muted solve-now-disclaimer" style="margin-top:10px;">${escapeHtml(MODEL_COUNCIL_OUTPUT_DISCLAIMER)}</p>
    `;
  };

  const buildApiAuthHeaders = async ({ includeJson = false } = {}) => {
    const headers = {};
    if (includeJson) headers["Content-Type"] = "application/json";
    try {
      const auth = state.clients?.auth;
      const user = auth?.currentUser;
      if (user) {
        const token = await user.getIdToken();
        if (token) headers.Authorization = `Bearer ${token}`;
      }
    } catch (error) {
      // Ignore token read failures; endpoint can still decide if auth is required.
    }
    return headers;
  };

  const normalizeFxCode = (value, fallback = "USD") => {
    const normalized = String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z]/g, "")
      .slice(0, 6);
    if (normalized) return normalized;
    return String(fallback || "USD")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z]/g, "")
      .slice(0, 6);
  };

  const readTerminalFxRecent = () => {
    try {
      const parsed = JSON.parse(localStorage.getItem(TERMINAL_FX_RECENT_KEY) || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  };

  const writeTerminalFxRecent = (items) => {
    try {
      localStorage.setItem(TERMINAL_FX_RECENT_KEY, JSON.stringify((Array.isArray(items) ? items : []).slice(0, TERMINAL_FX_RECENT_LIMIT)));
    } catch (error) {
      // Ignore storage write failures.
    }
  };

  const formatFxNumber = (value, maxDigits = 6) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "-";
    return new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: maxDigits,
    }).format(numeric);
  };

  const setTerminalFxStatus = (message, isError = false) => {
    if (!ui.terminalFxStatus) return;
    ui.terminalFxStatus.textContent = String(message || "");
    ui.terminalFxStatus.classList.toggle("error", Boolean(isError));
  };

  const renderTerminalFxResult = (payload) => {
    if (!ui.terminalFxResult) return;
    if (!payload || typeof payload !== "object") {
      ui.terminalFxResult.innerHTML = '<div class="small muted">Run a conversion to view rate details.</div>';
      return;
    }
    ui.terminalFxResult.innerHTML = `
      <div class="profile-item"><span class="label">Amount in</span><span class="value">${formatFxNumber(payload.amountIn, 6)} ${escapeHtml(payload.base || "")}</span></div>
      <div class="profile-item"><span class="label">Rate</span><span class="value">${formatFxNumber(payload.rate, 8)}</span></div>
      <div class="profile-item"><span class="label">Amount out</span><span class="value">${formatFxNumber(payload.amountOut, 6)} ${escapeHtml(payload.quote || "")}</span></div>
      <div class="profile-item"><span class="label">Symbol</span><span class="value">${escapeHtml(payload.symbolUsed || "-")}</span></div>
      <div class="profile-item"><span class="label">Updated</span><span class="value">${
        payload.asOf ? escapeHtml(new Date(payload.asOf).toLocaleString()) : "-"
      }</span></div>
      <div class="small muted" style="margin-top: 8px;">Source: ${escapeHtml(payload.source || "yahoo_finance")}</div>
    `;
  };

  const renderTerminalFxRecent = () => {
    if (!ui.terminalFxRecent) return;
    const items = readTerminalFxRecent();
    if (!items.length) {
      ui.terminalFxRecent.innerHTML = '<div class="small muted">No recent conversions yet.</div>';
      return;
    }
    ui.terminalFxRecent.innerHTML = items
      .map((item, index) => {
        const label = `${formatFxNumber(item.amountIn, 4)} ${normalizeFxCode(item.base)} -> ${formatFxNumber(
          item.amountOut,
          4
        )} ${normalizeFxCode(item.quote)}`;
        return `<button type="button" class="task-chip" data-terminal-fx-recent-index="${index}" style="margin: 4px 6px 4px 0;">${escapeHtml(
          label
        )}</button>`;
      })
      .join("");
  };

  const pushTerminalFxRecent = (payload) => {
    const rows = readTerminalFxRecent();
    const normalizedBase = normalizeFxCode(payload.base || "USD");
    const normalizedQuote = normalizeFxCode(payload.quote || "USD");
    const normalizedAmount = Number(payload.amountIn || 0);
    const deduped = rows.filter((row) => {
      const rowBase = normalizeFxCode(row.base || "USD");
      const rowQuote = normalizeFxCode(row.quote || "USD");
      return !(rowBase === normalizedBase && rowQuote === normalizedQuote && Number(row.amountIn || 0) === normalizedAmount);
    });
    deduped.unshift({
      base: normalizedBase,
      quote: normalizedQuote,
      amountIn: normalizedAmount,
      amountOut: Number(payload.amountOut || 0),
      rate: Number(payload.rate || 0),
      asOf: String(payload.asOf || ""),
      symbolUsed: String(payload.symbolUsed || ""),
      source: String(payload.source || ""),
      createdAt: Date.now(),
    });
    writeTerminalFxRecent(deduped);
    renderTerminalFxRecent();
  };

  const runTerminalFxConvert = async () => {
    const amount = Number(ui.terminalFxAmount?.value || 0);
    const base = normalizeFxCode(ui.terminalFxBase?.value || "USD");
    const quote = normalizeFxCode(ui.terminalFxQuote?.value || "USD");

    if (!Number.isFinite(amount) || amount <= 0) {
      setTerminalFxStatus("Enter a valid amount greater than zero.", true);
      return;
    }
    if (!base || !quote) {
      setTerminalFxStatus("Select base and quote currencies.", true);
      return;
    }

    const submitButton = ui.terminalFxSubmit;
    const submitLabel = submitButton?.querySelector("span");
    if (submitButton) submitButton.disabled = true;
    if (submitLabel) submitLabel.textContent = "Converting...";
    setTerminalFxStatus("Requesting FX quote...");

    try {
      const headers = await buildApiAuthHeaders({ includeJson: true });
      const response = await fetch("/api/fx/convert", {
        method: "POST",
        headers,
        body: JSON.stringify({
          base,
          quote,
          amount,
          meta: buildMeta(),
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const detail = String(payload?.error || payload?.detail || `HTTP ${response.status}`).trim();
        throw new Error(detail || "fx_convert_failed");
      }

      const normalized = {
        base,
        quote,
        amountIn: Number(payload?.amountIn || amount),
        amountOut: Number(payload?.amountOut || 0),
        rate: Number(payload?.rate || 0),
        symbolUsed: String(payload?.symbolUsed || ""),
        source: String(payload?.source || "yahoo_finance"),
        asOf: String(payload?.asOf || ""),
      };
      renderTerminalFxResult(normalized);
      pushTerminalFxRecent(normalized);
      setTerminalFxStatus(`Converted ${base}/${quote} using ${normalized.symbolUsed || "Yahoo FX"}.`);
      logEvent("fx_convert", { base, quote });
    } catch (error) {
      const message = extractErrorMessage(error, "Currency conversion failed.");
      setTerminalFxStatus(message, true);
      renderTerminalFxResult(null);
      showToast(message, "warn");
    } finally {
      if (submitButton) submitButton.disabled = false;
      if (submitLabel) submitLabel.textContent = "Convert";
    }
  };

  const normalizeMyRequestType = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const lowered = raw.toLowerCase();
    if (lowered === "forecast" || lowered === "screener" || lowered === "indicator") return lowered;
    if (lowered === "modelcouncil" || lowered === "model_council" || lowered === "model-council") return "modelCouncil";
    return "";
  };

  const normalizeMyRequestVisibility = (value, fallback = "private") => {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === "public" || raw === "unlisted" || raw === "private") return raw;
    return fallback;
  };

  const normalizeMyRequestPublishedFilter = (value) => {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === "published") return "published";
    if (raw === "unpublished" || raw === "draft") return "unpublished";
    return "all";
  };

  const getMyRequestPanelStateKey = (panel, idx) => {
    const explicit = String(panel?.dataset?.myRequestsPanelKey || "").trim();
    if (explicit) return explicit;
    const panelName = String(panel?.closest?.("[data-panel]")?.dataset?.panel || "").trim() || `panel_${idx}`;
    panel.dataset.myRequestsPanelKey = panelName;
    return panelName;
  };

  const readMyRequestPanelState = (panel, idx = 0) => {
    const key = getMyRequestPanelStateKey(panel, idx);
    const current = state.myRequestsPanelState[key] || {};
    const defaultType = normalizeMyRequestType(panel?.dataset?.defaultType || "");
    const controls = {
      search: panel?.querySelector?.("[data-my-requests-search]"),
      type: panel?.querySelector?.("[data-my-requests-type]"),
      published: panel?.querySelector?.("[data-my-requests-published]"),
      status: panel?.querySelector?.("[data-my-requests-status]"),
      list: panel?.querySelector?.("[data-my-requests-list]"),
    };
    const next = {
      search: String(current.search || controls.search?.value || "").trim(),
      type: normalizeMyRequestType(current.type || controls.type?.value || "") || defaultType || "",
      published: normalizeMyRequestPublishedFilter(current.published || controls.published?.value || "all"),
    };
    state.myRequestsPanelState[key] = next;
    return { key, controls, filters: next };
  };

  const sortMyRequestsInState = () => {
    state.myRequests = (Array.isArray(state.myRequests) ? state.myRequests : [])
      .slice()
      .sort((a, b) => {
        const aMs = Number(a?.updatedAtMs || a?.createdAtMs || 0);
        const bMs = Number(b?.updatedAtMs || b?.createdAtMs || 0);
        return bMs - aMs;
      });
    const byId = {};
    state.myRequests.forEach((item) => {
      const id = String(item?.id || "").trim();
      if (!id) return;
      byId[id] = item;
    });
    state.myRequestsById = byId;
  };

  const upsertMyRequestInState = (request) => {
    const id = String(request?.id || "").trim();
    if (!id) return;
    const index = state.myRequests.findIndex((item) => String(item?.id || "").trim() === id);
    if (index >= 0) {
      state.myRequests[index] = { ...state.myRequests[index], ...request };
    } else {
      state.myRequests.push(request);
    }
    sortMyRequestsInState();
  };

  const removeMyRequestFromState = (requestId) => {
    const id = String(requestId || "").trim();
    if (!id) return;
    state.myRequests = state.myRequests.filter((item) => String(item?.id || "").trim() !== id);
    delete state.myRequestsById[id];
  };

  const fetchMyRequestsList = async ({ force = false, notify = false } = {}) => {
    if (!hasFullAccount()) {
      state.myRequests = [];
      state.myRequestsById = {};
      state.myRequestsLoadedAt = 0;
      return [];
    }
    if (state.myRequestsLoading) return state.myRequests;
    if (!force && state.myRequests.length && Date.now() - Number(state.myRequestsLoadedAt || 0) < 15000) {
      return state.myRequests;
    }

    state.myRequestsLoading = true;
    try {
      const headers = await buildApiAuthHeaders();
      const response = await fetch("/api/my-requests?limit=160", {
        method: "GET",
        headers,
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(String(payload?.error || "Unable to load requests.").trim());
      state.myRequests = Array.isArray(payload?.items) ? payload.items : [];
      sortMyRequestsInState();
      state.myRequestsLoadedAt = Date.now();
      return state.myRequests;
    } catch (error) {
      if (notify) showToast(error.message || "Unable to load requests.", "warn");
      return state.myRequests;
    } finally {
      state.myRequestsLoading = false;
    }
  };

  const getMyRequestById = (requestId) => {
    const id = String(requestId || "").trim();
    if (!id) return null;
    return state.myRequestsById[id] || state.myRequests.find((item) => String(item?.id || "").trim() === id) || null;
  };

  const fetchMyRequestById = async (requestId) => {
    const id = String(requestId || "").trim();
    if (!id) return null;
    const cached = getMyRequestById(id);
    if (cached?.input && cached?.outputsMeta) return cached;
    const headers = await buildApiAuthHeaders();
    const response = await fetch(`/api/my-requests/${encodeURIComponent(id)}`, {
      method: "GET",
      headers,
      credentials: "same-origin",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(String(payload?.error || "Unable to load request.").trim());
    const request = payload?.request && typeof payload.request === "object" ? payload.request : null;
    if (request) {
      upsertMyRequestInState(request);
    }
    return request;
  };

  const renderMyRequestCards = (items = []) => {
    if (!Array.isArray(items) || !items.length) return `<div class="small muted">No requests matched this filter.</div>`;
    return items
      .map((item) => {
        const id = escapeHtml(String(item?.id || ""));
        const type = normalizeMyRequestType(item?.type) || "forecast";
        const typeLabel = escapeHtml(String(item?.typeLabel || MY_REQUEST_TYPE_LABELS[type] || type));
        const title = escapeHtml(String(item?.title || "Request"));
        const ticker = escapeHtml(String(item?.ticker || "—"));
        const createdAt = escapeHtml(formatTimestamp(item?.createdAt || item?.createdAtMs));
        const updatedAt = escapeHtml(formatTimestamp(item?.updatedAt || item?.updatedAtMs));
        const published = Boolean(item?.published);
        const share = item?.share && typeof item.share === "object" ? item.share : {};
        const shareVisibility = escapeHtml(String(share?.visibility || "private").toLowerCase());
        const summary = escapeHtml(String((item?.outputsMeta || {})?.summary || ""));
        return `
          <div class="order-card" data-request-id="${id}">
            <div class="order-header">
              <div>
                <div class="order-title">${title}</div>
                <div class="small">ID: ${id}</div>
              </div>
              <span class="status ${published ? "fulfilled" : "pending"}">${published ? "published" : "unpublished"}</span>
            </div>
            <div class="order-meta">
              <div><strong>Type</strong> ${typeLabel}</div>
              <div><strong>Ticker</strong> ${ticker}</div>
              <div><strong>Created</strong> ${createdAt}</div>
              <div><strong>Updated</strong> ${updatedAt}</div>
              <div><strong>Share</strong> ${shareVisibility}</div>
              ${summary ? `<div><strong>Summary</strong> ${summary}</div>` : ""}
            </div>
            <div class="order-actions" style="display:flex;gap:10px;flex-wrap:wrap;">
              <button class="cta secondary small" type="button" data-action="my-request-load" data-request-id="${id}">${icon("play")}<span>Load</span></button>
              <button class="cta secondary small" type="button" data-action="my-request-share" data-request-id="${id}">${icon("share-ios")}<span>Share</span></button>
              <button class="cta secondary small" type="button" data-action="my-request-rename" data-request-id="${id}">${icon("edit-pencil")}<span>Rename</span></button>
              <button class="cta secondary small" type="button" data-action="my-request-duplicate" data-request-id="${id}">${icon("copy")}<span>Duplicate</span></button>
              ${published ? `<button class="cta secondary small" type="button" data-action="my-request-unpublish" data-request-id="${id}">${icon("eye-off")}<span>Unpublish</span></button>` : ""}
              <button class="cta secondary small danger" type="button" data-action="my-request-delete" data-request-id="${id}">${icon("trash")}<span>Delete</span></button>
            </div>
          </div>
        `;
      })
      .join("");
  };

  const renderMyRequestsPanels = () => {
    const panels = Array.isArray(ui.myRequestsPanels) ? ui.myRequestsPanels : [];
    panels.forEach((panel, idx) => {
      const { controls, filters } = readMyRequestPanelState(panel, idx);
      if (!controls?.list || !controls?.status) return;

      const searchText = String(filters.search || "").trim().toLowerCase();
      const typeFilter = normalizeMyRequestType(filters.type);
      const publishedFilter = normalizeMyRequestPublishedFilter(filters.published);
      const sourceRows = Array.isArray(state.myRequests) ? state.myRequests : [];
      const rows = sourceRows.filter((item) => {
        if (Boolean(item?.deleted)) return false;
        const itemType = normalizeMyRequestType(item?.type);
        if (typeFilter && itemType !== typeFilter) return false;
        if (publishedFilter === "published" && !Boolean(item?.published)) return false;
        if (publishedFilter === "unpublished" && Boolean(item?.published)) return false;
        if (!searchText) return true;
        const haystack = [
          String(item?.title || ""),
          String(item?.ticker || ""),
          String(item?.typeLabel || ""),
          String((item?.outputsMeta || {})?.summary || ""),
          String((item?.input || {})?.question || ""),
          String(item?.createdAt || ""),
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(searchText);
      });

      if (!hasFullAccount()) {
        controls.status.textContent = "Sign in to manage requests.";
        controls.list.innerHTML = `<div class="small muted">Sign in to load your requests.</div>`;
        return;
      }
      if (state.myRequestsLoading) {
        controls.status.textContent = "Loading requests...";
        controls.list.innerHTML = `<div class="small muted">${skeletonHtml(3)}</div>`;
        return;
      }
      controls.status.textContent = rows.length
        ? `${rows.length} request${rows.length === 1 ? "" : "s"}`
        : "No requests matched this filter.";
      controls.list.innerHTML = renderMyRequestCards(rows.slice(0, 60));
    });
  };

  const bindMyRequestsPanels = () => {
    const panels = Array.isArray(ui.myRequestsPanels) ? ui.myRequestsPanels : [];
    panels.forEach((panel, idx) => {
      if (!panel || panel.dataset.bound === "1") return;
      panel.dataset.bound = "1";
      const { controls, key, filters } = readMyRequestPanelState(panel, idx);
      if (controls?.type) controls.type.value = filters.type || "";
      if (controls?.published) controls.published.value = filters.published || "all";
      if (controls?.search) controls.search.value = filters.search || "";

      controls?.search?.addEventListener("input", () => {
        state.myRequestsPanelState[key] = {
          ...state.myRequestsPanelState[key],
          search: String(controls.search.value || "").trim(),
        };
        renderMyRequestsPanels();
      });
      controls?.type?.addEventListener("change", () => {
        state.myRequestsPanelState[key] = {
          ...state.myRequestsPanelState[key],
          type: normalizeMyRequestType(controls.type.value || "") || "",
        };
        renderMyRequestsPanels();
      });
      controls?.published?.addEventListener("change", () => {
        state.myRequestsPanelState[key] = {
          ...state.myRequestsPanelState[key],
          published: normalizeMyRequestPublishedFilter(controls.published.value || "all"),
        };
        renderMyRequestsPanels();
      });
      panel.querySelector('[data-action="my-requests-refresh"]')?.addEventListener("click", async () => {
        await fetchMyRequestsList({ force: true, notify: true });
        renderMyRequestsPanels();
      });
    });
  };

  const upsertMyRequest = async (payload = {}) => {
    if (!hasFullAccount()) return null;
    const type = normalizeMyRequestType(payload.type);
    if (!type || !MY_REQUEST_TYPES.has(type)) return null;
    const headers = await buildApiAuthHeaders({ includeJson: true });
    const response = await fetch("/api/my-requests", {
      method: "POST",
      headers,
      credentials: "same-origin",
      body: JSON.stringify({
        type,
        requestId: String(payload.requestId || "").trim(),
        title: String(payload.title || "").trim(),
        input: payload.input && typeof payload.input === "object" ? payload.input : {},
        outputsMeta: payload.outputsMeta && typeof payload.outputsMeta === "object" ? payload.outputsMeta : {},
        sourceRef: payload.sourceRef && typeof payload.sourceRef === "object" ? payload.sourceRef : {},
        published: Boolean(payload.published),
      }),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(String(body?.error || "Unable to save request.").trim());
    const request = body?.request && typeof body.request === "object" ? body.request : null;
    if (request) {
      upsertMyRequestInState(request);
      renderMyRequestsPanels();
    }
    return request;
  };

  const updateMyRequest = async (requestId, payload = {}, { method = "PATCH", path = "" } = {}) => {
    const id = String(requestId || "").trim();
    if (!id) throw new Error("Request ID is required.");
    const headers = await buildApiAuthHeaders({ includeJson: true });
    const target = path || `/api/my-requests/${encodeURIComponent(id)}`;
    const response = await fetch(target, {
      method,
      headers,
      credentials: "same-origin",
      body: method === "GET" || method === "DELETE" ? undefined : JSON.stringify(payload),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(String(body?.error || "Request update failed.").trim());
    const request = body?.request && typeof body.request === "object" ? body.request : null;
    if (request) {
      upsertMyRequestInState(request);
    } else if (method === "DELETE" || body?.deleted) {
      removeMyRequestFromState(id);
    }
    renderMyRequestsPanels();
    return body;
  };

  const openMyRequestShareModal = ({ request, onSaved } = {}) =>
    new Promise((resolve) => {
      const item = request && typeof request === "object" ? request : null;
      if (!item) {
        resolve(null);
        return;
      }
      const requestId = String(item.id || "").trim();
      if (!requestId) {
        resolve(null);
        return;
      }

      const modal = ensureActionModal();
      const card = modal.querySelector(".modal-card");
      if (!card) {
        resolve(null);
        return;
      }

      const currentShare = item.share && typeof item.share === "object" ? item.share : {};
      const currentVisibility = normalizeMyRequestVisibility(currentShare.visibility, "private");
      card.innerHTML = `
        <h3>Share request</h3>
        <p class="small">Choose visibility and copy a read-only link.</p>
        <label class="label" for="my-request-share-visibility">Visibility</label>
        <select id="my-request-share-visibility" class="modal-input">
          <option value="private"${currentVisibility === "private" ? " selected" : ""}>Private</option>
          <option value="unlisted"${currentVisibility === "unlisted" ? " selected" : ""}>Unlisted</option>
          <option value="public"${currentVisibility === "public" ? " selected" : ""}>Public</option>
        </select>
        <div class="modal-actions" style="margin-top:14px;">
          <button class="cta secondary" type="button" data-action="cancel">Close</button>
          <button class="cta" type="button" data-action="confirm">Save visibility</button>
          <button class="cta secondary" type="button" data-action="copy" disabled>Copy link</button>
        </div>
        <p class="small muted" style="margin-top:10px;" data-role="status"></p>
      `;

      const visibilityInput = card.querySelector("#my-request-share-visibility");
      const copyBtn = card.querySelector('[data-action="copy"]');
      const confirmBtn = card.querySelector('[data-action="confirm"]');
      const status = card.querySelector('[data-role="status"]');
      let latestShareUrl = String(currentShare.shareUrl || "").trim();
      if (latestShareUrl) copyBtn.disabled = false;
      if (status) status.textContent = latestShareUrl ? latestShareUrl : "";

      const cleanup = (result = null) => {
        modal.classList.add("hidden");
        modal.removeEventListener("click", onClick);
        window.removeEventListener("keydown", onKeyDown, true);
        resolve(result);
      };

      const onClick = async (event) => {
        const action = event.target?.dataset?.action;
        if (!action) return;
        if (action === "close" || action === "cancel") {
          cleanup(null);
          return;
        }
        if (action === "copy") {
          if (!latestShareUrl) return;
          try {
            await performShare({
              url: latestShareUrl,
              title: "Quantura request",
              text: "Shared request from Quantura",
            });
            showToast("Share link copied.");
          } catch (error) {
            showToast(error.message || "Unable to copy link.", "warn");
          }
          return;
        }
        if (action !== "confirm") return;
        const visibility = normalizeMyRequestVisibility(visibilityInput?.value || "private", "private");
        confirmBtn.disabled = true;
        if (copyBtn) copyBtn.disabled = true;
        if (status) status.textContent = "Saving...";
        try {
          const body = await updateMyRequest(requestId, { visibility }, { method: "POST", path: `/api/my-requests/${encodeURIComponent(requestId)}/share` });
          latestShareUrl = String(body?.share?.shareUrl || "").trim();
          if (status) status.textContent = latestShareUrl || "Share disabled for private visibility.";
          if (copyBtn) copyBtn.disabled = !latestShareUrl;
          const refreshed = body?.request && typeof body.request === "object" ? body.request : null;
          if (refreshed && typeof onSaved === "function") onSaved(refreshed);
          if (visibility === "private") {
            showToast("Request set to private.");
          } else {
            showToast("Share link ready.");
          }
        } catch (error) {
          if (status) status.textContent = error.message || "Unable to update share settings.";
          showToast(error.message || "Unable to update share settings.", "warn");
        } finally {
          confirmBtn.disabled = false;
        }
      };

      const onKeyDown = (event) => {
        if (event.key === "Escape") cleanup(null);
      };

      modal.addEventListener("click", onClick);
      window.addEventListener("keydown", onKeyDown, true);
      modal.classList.remove("hidden");
    });

  const applyTickerQueryModelSelection = (modelId, providerId = "") => {
    const normalizedModel = normalizeAiModelId(modelId || "") || "gpt-5-mini";
    const normalizedProvider = normalizeModelCouncilProviderId(providerId || modelCouncilProviderFromModel(normalizedModel) || "openai");
    state.tickerContext.tickerQueryModel = normalizedModel;
    state.tickerContext.tickerQueryProvider = normalizedProvider;
    safeLocalStorageSet(TICKER_QUERY_MODEL_KEY, normalizedModel);
    safeLocalStorageSet(TICKER_QUERY_PROVIDER_KEY, normalizedProvider);
    if (ui.tickerQueryModel && ui.tickerQueryModel.value !== normalizedModel) {
      ui.tickerQueryModel.value = normalizedModel;
    }
    if (ui.tickerQueryProvider && ui.tickerQueryProvider.value !== normalizedProvider) {
      ui.tickerQueryProvider.value = normalizedProvider;
    }
    if (ui.tickerQueryProviderHint) {
      ui.tickerQueryProviderHint.textContent = `${MODEL_PROVIDER_LABEL[normalizedProvider] || normalizedProvider} provider · server-side only.`;
    }
    if (ui.tickerQueryModelHint) {
      ui.tickerQueryModelHint.textContent = `${tickerQueryModelHint(normalizedModel, normalizedProvider)} Some models may cost more and run slower.`;
    }
  };

  const renderTickerQueryModels = (models, { provider = "" } = {}) => {
    if (!ui.tickerQueryModel) return;
    const list = Array.isArray(models) ? models : [];
    const providerFilter = normalizeModelCouncilProviderId(provider || ui.tickerQueryProvider?.value || state.tickerContext.tickerQueryProvider || "");
    const scoped = providerFilter ? list.filter((row) => normalizeModelCouncilProviderId(row?.provider || "") === providerFilter) : list;
    const source = scoped.length ? scoped : list;
    if (!source.length) {
      ui.tickerQueryModel.innerHTML = `<option value="gpt-5-mini">gpt-5-mini</option>`;
      applyTickerQueryModelSelection("gpt-5-mini", "openai");
      return;
    }
    const grouped = source.reduce((acc, row) => {
      const modelId = normalizeAiModelId(row?.id || row?.model || "");
      if (!modelId) return acc;
      const providerId = normalizeModelCouncilProviderId(row?.provider || modelCouncilProviderFromModel(modelId) || "");
      const group = String(row?.group || tickerQueryModelGroup(modelId, providerId) || "Balanced").trim();
      if (!acc[group]) acc[group] = [];
      acc[group].push({
        id: modelId,
        provider: providerId,
        label: String(row?.label || getModelMeta(modelId)?.label || modelId),
        hint: String(row?.hint || tickerQueryModelHint(modelId, providerId)),
      });
      return acc;
    }, {});
    const order = ["Fast", "Balanced", "Reasoning", "Research", "Custom"];
    ui.tickerQueryModel.innerHTML = order
      .filter((group) => Array.isArray(grouped[group]) && grouped[group].length)
      .map((group) => {
        const options = grouped[group]
          .map((item) => `<option value="${escapeHtml(item.id)}" data-provider="${escapeHtml(item.provider)}" title="${escapeHtml(item.hint)}">${escapeHtml(item.label)}</option>`)
          .join("");
        return `<optgroup label="${escapeHtml(group)}">${options}</optgroup>`;
      })
      .join("");
    const availableSet = new Set(source.map((row) => normalizeAiModelId(row?.id || row?.model || "")).filter(Boolean));
    let selected = normalizeAiModelId(state.tickerContext.tickerQueryModel || safeLocalStorageGet(TICKER_QUERY_MODEL_KEY) || "");
    if (!selected || !availableSet.has(selected)) {
      selected = normalizeAiModelId(source[0]?.id || source[0]?.model || "gpt-5-mini") || "gpt-5-mini";
    }
    const selectedProvider = normalizeModelCouncilProviderId(
      source.find((row) => normalizeAiModelId(row?.id || row?.model || "") === selected)?.provider ||
      providerFilter ||
      modelCouncilProviderFromModel(selected)
    );
    applyTickerQueryModelSelection(selected, selectedProvider);
  };

  const renderTickerQueryProviderOptions = (providers, models) => {
    if (!ui.tickerQueryProvider) return;
    const rows = Array.isArray(providers) ? providers : [];
    const fallbackRows = Array.isArray(models)
      ? Array.from(
          new Map(
            models
              .map((row) => normalizeModelCouncilProviderId(row?.provider || ""))
              .filter(Boolean)
              .map((providerId) => [providerId, { id: providerId, displayName: MODEL_PROVIDER_LABEL[providerId] || providerId }])
          ).values()
        )
      : [];
    const source = rows.length ? rows.filter((row) => row?.available !== false) : fallbackRows;
    if (!source.length) {
      ui.tickerQueryProvider.innerHTML = `<option value="openai">OpenAI</option>`;
      applyTickerQueryModelSelection("gpt-5-mini", "openai");
      return;
    }
    ui.tickerQueryProvider.innerHTML = source
      .map((row) => {
        const id = normalizeModelCouncilProviderId(row?.id || "");
        const label = String(row?.displayName || MODEL_PROVIDER_LABEL[id] || id || "").trim();
        return `<option value="${escapeHtml(id)}">${escapeHtml(label)}</option>`;
      })
      .join("");
    const available = new Set(source.map((row) => normalizeModelCouncilProviderId(row?.id || "")).filter(Boolean));
    let selectedProvider = normalizeModelCouncilProviderId(
      state.tickerContext.tickerQueryProvider || safeLocalStorageGet(TICKER_QUERY_PROVIDER_KEY) || ""
    );
    if (!selectedProvider || !available.has(selectedProvider)) {
      selectedProvider = normalizeModelCouncilProviderId(source[0]?.id || "openai");
    }
    ui.tickerQueryProvider.value = selectedProvider;
    safeLocalStorageSet(TICKER_QUERY_PROVIDER_KEY, selectedProvider);
    state.tickerContext.tickerQueryProvider = selectedProvider;
    renderTickerQueryModels(models, { provider: selectedProvider });
  };

  const loadTickerQueryModels = async ({ force = false } = {}) => {
    if (!ui.tickerQueryModel) return;
    if (!force && state.tickerContext.tickerQueryModelsLoaded && state.tickerContext.tickerQueryModels.length) {
      renderTickerQueryProviderOptions(state.tickerContext.tickerQueryProviders || [], state.tickerContext.tickerQueryModels);
      return;
    }

    const fallback = () => {
      const localModels = AI_MODEL_CATALOG.map((model) => ({
        id: normalizeAiModelId(model.id),
        provider: normalizeModelCouncilProviderId(model.provider || modelCouncilProviderFromModel(model.id)),
        label: String(model.label || model.id),
        group: tickerQueryModelGroup(model.id, model.provider),
        hint: tickerQueryModelHint(model.id, model.provider),
      })).filter((row) => row.id);
      state.tickerContext.tickerQueryModels = localModels;
      state.tickerContext.tickerQueryProviders = Array.from(
        new Map(
          localModels.map((row) => [row.provider, { id: row.provider, displayName: MODEL_PROVIDER_LABEL[row.provider] || row.provider, available: true }])
        ).values()
      );
      state.tickerContext.tickerQueryModelsLoaded = true;
      renderTickerQueryProviderOptions(state.tickerContext.tickerQueryProviders, localModels);
    };

    try {
      const headers = await buildApiAuthHeaders();
      const response = await fetch("/api/model-council/models", {
        method: "GET",
        headers,
        credentials: "same-origin",
      });
      if (!response.ok) throw new Error("Model list unavailable.");
      const payload = await response.json();
      const remoteModels = Array.isArray(payload?.models) ? payload.models : [];
      const models = remoteModels
        .map((row) => {
          const id = normalizeAiModelId(row?.id || row?.model || "");
          if (!id) return null;
          const provider = normalizeModelCouncilProviderId(row?.provider || modelCouncilProviderFromModel(id));
          return {
            id,
            provider,
            label: String(row?.label || getModelMeta(id)?.label || id),
            group: String(row?.group || tickerQueryModelGroup(id, provider)),
            hint: String(row?.hint || tickerQueryModelHint(id, provider)),
          };
        })
        .filter(Boolean);
      if (!models.length) throw new Error("No compatible models returned.");
      const providers = Array.isArray(payload?.providers) ? payload.providers : [];
      state.tickerContext.tickerQueryModels = models;
      state.tickerContext.tickerQueryProviders = providers;
      state.tickerContext.tickerQueryModelsLoaded = true;
      renderTickerQueryProviderOptions(providers, models);
    } catch (error) {
      fallback();
    }
  };

  const formatTokenStat = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num) || num < 0) return "—";
    return Math.round(num).toLocaleString();
  };

  const renderTickerQueryCacheStats = (usage, { visible = false } = {}) => {
    if (!ui.tickerQueryCacheStats) return;
    const promptTokens = Number(usage?.prompt_tokens || 0);
    const completionTokens = Number(usage?.completion_tokens || 0);
    const cachedTokens = Number(usage?.cached_tokens || 0);
    const totalTokens = Number(usage?.total_tokens || promptTokens + completionTokens || 0);
    ui.tickerQueryCacheStats.innerHTML = `
      <div class="small"><strong>prompt_tokens:</strong> ${escapeHtml(formatTokenStat(promptTokens))}</div>
      <div class="small"><strong>completion_tokens:</strong> ${escapeHtml(formatTokenStat(completionTokens))}</div>
      <div class="small"><strong>cached_tokens:</strong> ${escapeHtml(formatTokenStat(cachedTokens))}</div>
      <div class="small"><strong>total_tokens:</strong> ${escapeHtml(formatTokenStat(totalTokens))}</div>
    `;
    ui.tickerQueryCacheStats.classList.toggle("hidden", !visible);
  };

  const updateTickerQueryModelInfo = ({ latencyMs = null, usage = null } = {}) => {
    if (!ui.tickerQueryModelInfo) return;
    const promptTokens = Number(usage?.prompt_tokens || 0);
    const completionTokens = Number(usage?.completion_tokens || 0);
    const cachedTokens = Number(usage?.cached_tokens || 0);
    const latencyLabel = Number.isFinite(Number(latencyMs)) && Number(latencyMs) >= 0 ? `${Math.round(Number(latencyMs))}ms` : "—";
    const cacheHint = cachedTokens > 0 ? " · Cache hit: faster + cheaper." : "";
    ui.tickerQueryModelInfo.textContent =
      `Latency: ${latencyLabel} · Tokens: ${formatTokenStat(promptTokens + completionTokens)} · Cached: ${formatTokenStat(cachedTokens)}${cacheHint}`;
  };

  const fetchTickerQueryModuleData = async ({ ticker, modules = [] } = {}) => {
    const symbol = normalizeTicker(ticker || "");
    if (!symbol) return { moduleData: {}, moduleContext: {} };
    const headers = await buildApiAuthHeaders({ includeJson: true });
    const response = await fetch("/api/ticker/modules", {
      method: "POST",
      headers,
      credentials: "same-origin",
      body: JSON.stringify({
        ticker: symbol,
        modules,
      }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) return { moduleData: {}, moduleContext: {} };
    return {
      moduleData: payload?.moduleData && typeof payload.moduleData === "object" ? payload.moduleData : {},
      moduleContext: payload?.moduleContext && typeof payload.moduleContext === "object" ? payload.moduleContext : {},
    };
  };

  const buildModelCouncilSystemMessage = ({ ticker, language, moduleContext, technicalContext } = {}) => {
    const context = {
      ticker: normalizeTicker(ticker || ""),
      language: normalizeLanguageCode(language || "en"),
      moduleContext: moduleContext && typeof moduleContext === "object" ? moduleContext : {},
      technicalContext: technicalContext && typeof technicalContext === "object" ? technicalContext : {},
    };
    return [
      "You are Quantura Model Council, a multi-model equity research copilot.",
      "Use the provided structured ticker context and cite uncertainty clearly.",
      "Return a concise, structured response with thesis, risks, and next steps.",
      `Structured context JSON:\n${JSON.stringify(context)}`,
    ].join("\n\n");
  };

  const streamTickerQueryInsight = async ({ ticker, prompt, language, model, provider, modules = [], technicalContext = null } = {}) => {
    const headers = await buildApiAuthHeaders({ includeJson: true });

    // Primary path keeps response persistence + share/feedback IDs.
    try {
      const response = await fetch("/api/model-council/query", {
        method: "POST",
        headers,
        credentials: "same-origin",
        body: JSON.stringify({
          ticker,
          question: prompt,
          language,
          model,
          provider,
          modules,
          technicalContext: technicalContext && typeof technicalContext === "object" ? technicalContext : undefined,
          messages: [{ role: "user", content: prompt }],
          meta: buildMeta(),
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (response.ok) {
        return {
          answer: String(payload?.answer || "").trim(),
          model: normalizeAiModelId(payload?.model || model) || model,
          provider: normalizeModelCouncilProviderId(payload?.provider || provider || "openai"),
          usage: payload?.usage && typeof payload.usage === "object" ? payload.usage : {},
          latencyMs: Number.isFinite(Number(payload?.latencyMs)) ? Number(payload.latencyMs) : null,
          context: payload?.context && typeof payload.context === "object" ? payload.context : {},
          moduleData: payload?.moduleData && typeof payload.moduleData === "object" ? payload.moduleData : {},
          selectedModules: Array.isArray(payload?.selectedModules) ? payload.selectedModules : modules,
          responseId: String(payload?.responseId || "").trim(),
          citations: Array.isArray(payload?.citations) ? payload.citations : [],
        };
      }
    } catch (error) {
      // Fall through to /api/llm/run fallback.
    }

    // Fallback path guarantees selected provider/model produces output.
    const { moduleData, moduleContext } = await fetchTickerQueryModuleData({ ticker, modules });
    const systemPrompt = buildModelCouncilSystemMessage({
      ticker,
      language,
      moduleContext,
      technicalContext,
    });
    const fallbackResponse = await fetch("/api/llm/run", {
      method: "POST",
      headers,
      credentials: "same-origin",
      body: JSON.stringify({
        provider,
        model,
        fallbackProviders: ["openai", "gemini", "mistral", "perplexity", "other"],
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: String(prompt || "").trim() },
        ],
        params: {
          temperature: 0.2,
          maxTokens: 900,
          webSearch: true,
          stream: true,
          background: true,
        },
      }),
    });
    const payload = await fallbackResponse.json().catch(() => ({}));
    if (!fallbackResponse.ok) {
      const message = String(payload?.error || payload?.message || "Unable to complete Model Council request right now.").trim();
      const err = new Error(message);
      err.retryProvider = String(payload?.retryProvider || "").trim();
      err.retryModel = String(payload?.retryModel || "").trim();
      throw err;
    }
    return {
      answer: String(payload?.text || "").trim(),
      model: normalizeAiModelId(payload?.model || model) || model,
      provider: normalizeModelCouncilProviderId(payload?.provider || provider || "openai"),
      usage: payload?.usage && typeof payload.usage === "object" ? payload.usage : {},
      latencyMs: Number.isFinite(Number(payload?.latencyMs)) ? Number(payload.latencyMs) : null,
      context: {
        moduleContext,
      },
      moduleData,
      selectedModules: modules,
      responseId: "",
      citations: Array.isArray(payload?.citations) ? payload.citations : [],
    };
  };

  const improveTickerQueryPrompt = async ({ ticker, question, language, modules, model, provider } = {}) => {
    const headers = await buildApiAuthHeaders({ includeJson: true });
    try {
      const response = await fetch("/api/model-council/improve-prompt", {
        method: "POST",
        headers,
        credentials: "same-origin",
        body: JSON.stringify({
          ticker,
          question,
          language,
          modules,
          model,
          provider,
          meta: buildMeta(),
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (response.ok) {
        return {
          improvedPrompt: String(payload?.improvedPrompt || question || "").trim() || String(question || "").trim(),
          model: String(payload?.model || "").trim(),
          provider: String(payload?.provider || "").trim(),
        };
      }
    } catch (error) {
      // Fallback to /api/llm/run below.
    }

    const rewriteResponse = await fetch("/api/llm/run", {
      method: "POST",
      headers,
      credentials: "same-origin",
      body: JSON.stringify({
        provider: "openai",
        model: "gpt-5-mini",
        fallbackProviders: ["openai", "gemini", "mistral", "perplexity", "other"],
        messages: [
          {
            role: "system",
            content:
              "Rewrite the user prompt for clarity and specificity for financial analysis. Return plain text only. Preserve intent.",
          },
          {
            role: "user",
            content: `Ticker: ${normalizeTicker(ticker || "")}\nLanguage: ${normalizeLanguageCode(language || "en")}\nModules: ${Array.isArray(modules) ? modules.join(", ") : ""}\nPrompt:\n${String(question || "").trim()}`,
          },
        ],
        params: {
          temperature: 0.1,
          maxTokens: 420,
          webSearch: false,
          stream: false,
          background: false,
        },
      }),
    });
    const payload = await rewriteResponse.json().catch(() => ({}));
    if (!rewriteResponse.ok) {
      const message = String(payload?.error || "Unable to improve prompt right now.").trim();
      throw new Error(message);
    }
    return {
      improvedPrompt: String(payload?.text || question || "").trim() || String(question || "").trim(),
      model: String(payload?.model || "gpt-5-mini").trim(),
      provider: String(payload?.provider || "openai").trim(),
    };
  };

  const submitModelCouncilFeedback = async ({ responseId, action } = {}) => {
    const id = String(responseId || "").trim();
    const normalizedAction = String(action || "").trim().toLowerCase();
    if (!id || !normalizedAction) return;
    const headers = await buildApiAuthHeaders({ includeJson: true });
    await fetch("/api/model-council/feedback", {
      method: "POST",
      headers,
      credentials: "same-origin",
      body: JSON.stringify({
        responseId: id,
        action: normalizedAction,
        meta: buildMeta(),
      }),
    }).catch(() => {});
  };

  const createModelCouncilShareLink = async (responseId) => {
    const id = String(responseId || "").trim();
    if (!id) throw new Error("Response ID is required.");
    const headers = await buildApiAuthHeaders({ includeJson: true });
    const response = await fetch("/api/model-council/share", {
      method: "POST",
      headers,
      credentials: "same-origin",
      body: JSON.stringify({ responseId: id, meta: buildMeta() }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(String(payload?.error || "Unable to create share link.").trim());
    return String(payload?.shareUrl || "").trim();
  };

  const loadPublicModelCouncilShare = async ({ setPanel = true } = {}) => {
    if (!ui.tickerQueryOutput) return false;
    let shareId = "";
    try {
      const params = new URLSearchParams(window.location.search);
      shareId = String(params.get("publicShare") || "").trim();
    } catch (error) {
      shareId = "";
    }
    if (!shareId) return false;

    try {
      setOutputLoading(ui.tickerQueryOutput, "Loading shared Model Council response...");
      const response = await fetch(`/api/model-council/share/${encodeURIComponent(shareId)}`, {
        method: "GET",
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(String(payload?.error || "Shared response unavailable.").trim());
      }
      setOutputReady(ui.tickerQueryOutput);
      state.tickerContext.tickerQueryLastResponseId = String(payload?.responseId || "").trim();
      state.tickerContext.tickerQueryLastResponse = payload;
      state.tickerContext.tickerQueryShareUrl = `${window.location.origin}/model-council?publicShare=${encodeURIComponent(shareId)}`;
      renderTickerQueryResult({
        answer: String(payload?.answer || "").trim(),
        model: String(payload?.model || "").trim(),
        provider: String(payload?.provider || "").trim(),
        context: payload?.context || {},
        responseId: String(payload?.responseId || "").trim(),
        citations: Array.isArray(payload?.citations) ? payload.citations : [],
        shareUrl: state.tickerContext.tickerQueryShareUrl,
      });
      renderTickerQueryModulesOutput(payload?.moduleData || {}, payload?.selectedModules || []);
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Viewing shared read-only Model Council response.";
      if (setPanel && typeof window.__quanturaSetPanel === "function") {
        window.__quanturaSetPanel("ticker-query", { pushPath: false });
      }
      return true;
    } catch (error) {
      setOutputReady(ui.tickerQueryOutput);
      renderTickerQueryErrorState({ message: error.message || "Shared response unavailable." });
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Unable to load shared response.";
      return false;
    }
  };

  const syncModelCouncilSeo = () => {
    const title = "Model Council | Quantura";
    const description = "Multi-provider Model Council with structured Yahoo Finance module context for ticker analysis.";
    try {
      if (window.location.pathname === "/model-council" || window.location.pathname === "/ticker-query") {
        document.title = title;
      }
      const metaDescription = document.querySelector('meta[name="description"]');
      if (metaDescription && window.location.pathname === "/model-council") {
        metaDescription.setAttribute("content", description);
      }
    } catch (error) {
      // Best-effort only.
    }
  };

  const loadTickerQueryInsight = async (
    functions,
    { ticker, question, notify = false, skipImprove = false, providerOverride = "", modelOverride = "" } = {}
  ) => {
    if (!ui.tickerQueryOutput) return;
    const symbol = normalizeTicker(ticker || ui.tickerQueryTicker?.value || state.tickerContext.ticker || "");
    const prompt = String(question || ui.tickerQueryQuestion?.value || "").trim();
    const languageRaw = normalizeLanguageCode(ui.tickerQueryLanguage?.value || state.preferredLanguage || "en");
    const language = languageRaw === "auto" ? state.preferredLanguage || "en" : languageRaw;
    const selectedProvider = normalizeModelCouncilProviderId(
      providerOverride || ui.tickerQueryProvider?.value || state.tickerContext.tickerQueryProvider || "openai"
    );
    const selectedModel = normalizeAiModelId(
      modelOverride || ui.tickerQueryModel?.value || state.tickerContext.tickerQueryModel || "gpt-5-mini"
    ) || "gpt-5-mini";
    const selectedModules = getSelectedTickerQueryModules();

    if (!symbol) {
      showToast("Ticker is required for Model Council.", "warn");
      return;
    }
    if (!prompt) {
      showToast("Enter a question for Model Council.", "warn");
      return;
    }
    const rewardApproved = await maybeShowNativeRewardGate({
      reason: "model_council",
      title: "Watch a rewarded ad to unlock Model Council output?",
      message: "Running Model Council in the native app can require a rewarded video before generating output.",
    });
    if (!rewardApproved) return;

    const improveEnabled = Boolean(ui.tickerQueryImproveToggle?.checked);
    if (improveEnabled && !skipImprove) {
      try {
        if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Improving prompt preview...";
        const improved = await improveTickerQueryPrompt({
          ticker: symbol,
          question: prompt,
          language,
          modules: selectedModules,
          model: selectedModel,
          provider: selectedProvider,
        });
        if (ui.tickerQueryImprovePreview) ui.tickerQueryImprovePreview.value = improved.improvedPrompt || prompt;
        if (ui.tickerQueryImprovePreviewWrap) ui.tickerQueryImprovePreviewWrap.classList.remove("hidden");
        state.tickerContext.tickerQueryPendingQuestion = prompt;
        state.tickerContext.tickerQueryPendingProvider = selectedProvider;
        state.tickerContext.tickerQueryPendingModel = selectedModel;
        if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Prompt improved. Review preview, then run Model Council.";
        return;
      } catch (error) {
        if (notify) showToast(error.message || "Unable to improve prompt preview.", "warn");
      }
    }

    const finalPrompt = String(
      (skipImprove && ui.tickerQueryImprovePreview ? ui.tickerQueryImprovePreview.value : prompt) || prompt
    ).trim();
    if (!finalPrompt) {
      showToast("Prompt cannot be empty.", "warn");
      return;
    }

    try {
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Running Model Council...";
      setOutputLoading(ui.tickerQueryOutput, "Running Model Council analysis...");
      if (ui.tickerQueryModulesOutput) {
        ui.tickerQueryModulesOutput.classList.add("hidden");
      }
      updateTickerQueryModelInfo({});
      applyTickerQueryModelSelection(selectedModel, selectedProvider);
      setTickerQueryModulesSelection(selectedModules, { persist: true });
      renderTickerQueryCacheStats(null, { visible: false });
      renderTickerQueryResult({
        answer: "",
        model: selectedModel,
        provider: selectedProvider,
        context: {},
      });
      const started = Date.now();
      const responsePayload = await streamTickerQueryInsight({
        ticker: symbol,
        prompt: finalPrompt,
        language,
        model: selectedModel,
        provider: selectedProvider,
        modules: selectedModules,
      });
      const usage = responsePayload.usage && typeof responsePayload.usage === "object" ? responsePayload.usage : {};
      const latencyMs = Number.isFinite(Number(responsePayload.latencyMs)) ? Number(responsePayload.latencyMs) : Date.now() - started;
      state.tickerContext.tickerQueryLastResponseId = String(responsePayload.responseId || "").trim();
      state.tickerContext.tickerQueryLastResponse = responsePayload;
      state.tickerContext.tickerQueryFeedback = "";
      state.tickerContext.tickerQueryShareUrl = "";
      setOutputReady(ui.tickerQueryOutput);
      renderTickerQueryResult({
        answer: responsePayload.answer || "No answer returned.",
        model: responsePayload.model || selectedModel,
        provider: responsePayload.provider || selectedProvider,
        context: responsePayload.context || {},
        responseId: responsePayload.responseId || "",
        citations: responsePayload.citations || [],
      });
      renderTickerQueryModulesOutput(responsePayload.moduleData || {}, responsePayload.selectedModules || selectedModules);
      updateTickerQueryModelInfo({ latencyMs, usage });
      const showCacheStats = Boolean(ui.tickerQueryShowCacheStats?.checked);
      renderTickerQueryCacheStats(usage, { visible: showCacheStats });
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Model Council completed.";
      if (ui.tickerQueryImprovePreviewWrap && skipImprove) {
        ui.tickerQueryImprovePreviewWrap.classList.add("hidden");
      }
	      logEvent("model_council_completed", {
	        ticker: symbol,
	        language,
	        provider: responsePayload.provider || selectedProvider,
	        model: responsePayload.model || selectedModel,
	        modules_count: (responsePayload.selectedModules || selectedModules || []).length,
	        prompt_tokens: Number(usage?.prompt_tokens || 0),
	        completion_tokens: Number(usage?.completion_tokens || 0),
	        cached_tokens: Number(usage?.cached_tokens || 0),
	      });
	      upsertMyRequest({
	        type: "modelCouncil",
	        title: `${symbol} Model Council`,
	        input: {
	          ticker: symbol,
	          question: finalPrompt,
	          language,
	          provider: responsePayload.provider || selectedProvider,
	          model: responsePayload.model || selectedModel,
	          modules: Array.isArray(responsePayload.selectedModules)
	            ? responsePayload.selectedModules
	            : selectedModules,
	        },
	        outputsMeta: {
	          summary: String(responsePayload.answer || "").trim().slice(0, 480),
	          answer: String(responsePayload.answer || "").trim().slice(0, 4000),
	          provider: responsePayload.provider || selectedProvider,
	          model: responsePayload.model || selectedModel,
	          latencyMs,
	        },
	        sourceRef: {
	          collection: "model_council_responses",
	          id: String(responsePayload.responseId || "").trim(),
	        },
	      }).catch(() => {});
	    } catch (error) {
	      setOutputReady(ui.tickerQueryOutput);
	      renderTickerQueryErrorState({
	        message: error.message || "Unable to run Model Council right now.",
        retryProvider: error.retryProvider || "",
        retryModel: error.retryModel || "",
      });
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Unable to complete Model Council request.";
      if (notify) showToast(error.message || "Unable to run Model Council.", "warn");
    }
  };

  const isPanelVisible = (panelName) => {
    const panel = document.querySelector(`[data-panel="${String(panelName || "").trim()}"]`);
    return Boolean(panel && !panel.classList.contains("hidden"));
  };

  const autoloadOptionsChain = async (functions, { force = false } = {}) => {
    if (!functions || !ui.optionsForm || !ui.optionsOutput) return;
    const symbol = normalizeTicker(state.tickerContext.ticker || safeLocalStorageGet(LAST_TICKER_KEY) || "");
    if (!symbol) return;

    const tickerInput = document.getElementById("options-ticker");
    if (tickerInput && "value" in tickerInput) tickerInput.value = symbol;

    if (!hasFullAccount()) {
      if (isPanelVisible("options")) {
        setOutputReady(ui.optionsOutput);
        ui.optionsOutput.innerHTML = `<div class="small muted">Sign in to load the options chain.</div>`;
      }
      return;
    }

    if (!force && state.tickerContext.optionsTicker === symbol) return;
    state.tickerContext.optionsTicker = symbol;

    try {
      ui.optionsForm.requestSubmit?.();
    } catch (error) {
      // Ignore.
    }
  };

  const scheduleSideDataRefresh = (ticker, { force = false } = {}) => {
    const symbol = normalizeTicker(ticker) || "";
    const functions = state.clients?.functions;
    if (!functions || (!ui.newsOutput && !ui.xTrendingOutput && !ui.intelOutput && !ui.optionsOutput)) return;
    if (!symbol) return;
    if (state.sideDataRefreshTimer) window.clearTimeout(state.sideDataRefreshTimer);
    state.sideDataRefreshTimer = window.setTimeout(() => {
      loadTickerNews(functions, symbol, { force, notify: false });
      loadTickerXTrends(functions, symbol, { force, notify: false });
      loadTickerIntel(functions, symbol, { force, notify: false });
      if (state.panelAutoloaded.options || isPanelVisible("options")) {
        autoloadOptionsChain(functions, { force });
      }
    }, 220);
  };

  const normalizeTrendingLogoUrl = (raw) => {
    const value = String(raw || "").trim();
    return /^https?:\/\//i.test(value) ? value : "";
  };

  const extractHostnameForLogo = (rawUrl) => {
    const value = String(rawUrl || "").trim();
    if (!value) return "";
    try {
      const parsed = new URL(value.startsWith("http") ? value : `https://${value}`);
      const host = String(parsed.hostname || "").trim().toLowerCase();
      if (!host) return "";
      return host.replace(/^www\./, "");
    } catch (error) {
      return "";
    }
  };

  const websiteToYfinanceLogoUrl = (website) => {
    const host = extractHostnameForLogo(website);
    return host ? `https://logo.clearbit.com/${host}` : "";
  };

  const extractTrendingRowLogoUrl = (row) => {
    if (!row || typeof row !== "object") return "";
    const direct =
      normalizeTrendingLogoUrl(row.logoUrl) ||
      normalizeTrendingLogoUrl(row.logo_url) ||
      normalizeTrendingLogoUrl(row.logo);
    if (direct) return direct;
    return websiteToYfinanceLogoUrl(row.website || row.site || row.domain);
  };

  const extractIntelLogoUrl = (payload) => {
    const data = payload && typeof payload === "object" ? payload : {};
    const profile = data.profile && typeof data.profile === "object" ? data.profile : {};
    const profileDetails = data.profileDetails && typeof data.profileDetails === "object" ? data.profileDetails : {};
    return (
      normalizeTrendingLogoUrl(data.logoUrl) ||
      normalizeTrendingLogoUrl(data.logo_url) ||
      normalizeTrendingLogoUrl(profile.logoUrl) ||
      normalizeTrendingLogoUrl(profile.logo_url) ||
      normalizeTrendingLogoUrl(profileDetails.logoUrl) ||
      normalizeTrendingLogoUrl(profileDetails.logo_url) ||
      websiteToYfinanceLogoUrl(profile.website || profileDetails.website || data.website)
    );
  };

  const fetchTrendingTickerLogoUrl = async (functions, symbol) => {
    const ticker = normalizeTicker(symbol);
    if (!functions || !ticker) return "";
    if (trendingLogoCache.has(ticker)) {
      return String(trendingLogoCache.get(ticker) || "");
    }
    if (trendingLogoInFlight.has(ticker)) {
      return trendingLogoInFlight.get(ticker);
    }
    const pending = (async () => {
      try {
        const result = await fetchTickerIntelPayload(functions, ticker, { force: false });
        const logoUrl = extractIntelLogoUrl(result || {});
        trendingLogoCache.set(ticker, logoUrl || "");
        return logoUrl || "";
      } catch (error) {
        trendingLogoCache.set(ticker, "");
        return "";
      } finally {
        trendingLogoInFlight.delete(ticker);
      }
    })();
    trendingLogoInFlight.set(ticker, pending);
    return pending;
  };

  const normalizeTrendingTickerRows = (payload) => {
    const items = Array.isArray(payload?.items) ? payload.items : [];
    const tickers = Array.isArray(payload?.tickers) ? payload.tickers : [];
    const baseRows = items.length
      ? items
      : tickers.map((symbol) => ({
          symbol,
          lastClose: null,
          changePct: null,
        }));
    return baseRows
      .map((row) => {
        const symbol = normalizeTicker(row?.symbol || row?.ticker || "");
        if (!symbol) return null;
        return {
          ...row,
          symbol,
          logoUrl: extractTrendingRowLogoUrl(row),
        };
      })
      .filter(Boolean);
  };

  const renderTrendingTickerRows = (rows) => {
    if (!ui.trendingList) return;
    if (!rows.length) {
      ui.trendingList.innerHTML = `<div class="small muted">No trending tickers returned.</div>`;
      return;
    }

    ui.trendingList.innerHTML = rows
      .slice(0, 18)
      .map((row) => {
        const symbol = normalizeTicker(row.symbol || row.ticker || "") || "";
        const lastClose = row.lastClose;
        const changePct = row.changePct;
        const change = row.change;
        const logoUrl = extractTrendingRowLogoUrl(row);

        const changeNum = typeof changePct === "number" ? changePct : Number(changePct);
        const changeOk = Number.isFinite(changeNum);
        const direction = !changeOk ? "flat" : changeNum < 0 ? "down" : "up";
        const changeLabel = changeOk ? formatPercent(changeNum, { signed: true, digits: 2 }) : "Quote unavailable";
        const absChange = typeof change === "number" ? change : Number(change);
        const absChangeLabel = Number.isFinite(absChange) ? `${absChange > 0 ? "+" : ""}${absChange.toFixed(2)}` : "";

        const priceLabel = lastClose !== null && lastClose !== undefined ? formatUsd(lastClose) : "—";
        const subLabel = absChangeLabel && changeOk ? `${absChangeLabel} · ${changeLabel}` : changeLabel;

        return `
          <button class="trending-hot-chip" type="button" data-action="pick-ticker" data-ticker="${escapeHtml(symbol)}">
            <div class="trending-top">
              <div class="trending-symbol" style="display:inline-flex; align-items:center; gap:8px;">
                ${
                  logoUrl
                    ? `<img src="${escapeHtml(logoUrl)}" alt="" loading="lazy" style="width:18px; height:18px; border-radius:50%; object-fit:cover; background:rgba(255,255,255,0.9);" />`
                    : ""
                }
                <span>${escapeHtml(symbol)}</span>
              </div>
              <div class="trending-price">${escapeHtml(priceLabel)}</div>
            </div>
            <div class="trending-bottom">
              <span class="trending-chip trending-chip--${direction}">${escapeHtml(subLabel)}</span>
              <span class="small muted">Open ticker</span>
            </div>
          </button>
        `;
      })
      .join("");
  };

  const enrichTrendingTickerRowsWithLogos = async (rows, functions) => {
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length || !functions) return list;
    const updated = list.map((row) => ({ ...(row || {}) }));
    let changed = false;
    const targets = updated
      .slice(0, 18)
      .map((row, index) => ({ row, index }))
      .filter(({ row }) => !extractTrendingRowLogoUrl(row));

    await Promise.all(
      targets.map(async ({ row, index }) => {
        const symbol = normalizeTicker(row?.symbol || row?.ticker || "");
        if (!symbol) return;
        const logoUrl = await fetchTrendingTickerLogoUrl(functions, symbol);
        if (!logoUrl) return;
        updated[index].logoUrl = logoUrl;
        changed = true;
      })
    );
    return changed ? updated : list;
  };

  const loadTrendingTickers = async (functions, { notify = false, force = false } = {}) => {
    if (!functions || !ui.trendingList) return;
    try {
      setOutputLoading(ui.trendingList, "Loading trending tickers...");
      const getTrending = functions.httpsCallable("get_trending_tickers");
      const result = await getTrending({ force: Boolean(force), meta: buildMeta() });
      const rows = normalizeTrendingTickerRows(result.data || {});
      setOutputReady(ui.trendingList);
      renderTrendingTickerRows(rows);
      enrichTrendingTickerRowsWithLogos(rows, functions)
        .then((enrichedRows) => {
          if (enrichedRows !== rows) {
            renderTrendingTickerRows(enrichedRows);
          }
        })
        .catch(() => undefined);
      const count = rows.length;
      logEvent("trending_loaded", { count });
    } catch (error) {
      setOutputReady(ui.trendingList);
      ui.trendingList.innerHTML = `<div class="small muted">Unable to load trending tickers right now.</div>`;
      if (notify) showToast(error.message || "Unable to load trending tickers.", "warn");
    }
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

  const renderTickerChart = async (rows, ticker, interval, overlays = [], options = {}) => {
    if (!ui.tickerChart) return;
    const tickerPanelVisible = isPanelVisible("ticker");
    if (!tickerPanelVisible) {
      setTerminalChartEngineVisibility("legacy");
      return;
    }
    const cleanTicker = normalizeTicker(ticker || state.tickerContext.ticker || "") || "AAPL";
    const hasOverlays = Array.isArray(overlays) && overlays.length > 0;
    const skipTradingView = Boolean(options?.skipTradingView);

    if (!rows?.length) {
      ui.tickerChart.textContent = "No price data to plot.";
      return;
    }

    if (!skipTradingView && !hasOverlays) {
      const rendered = renderTradingViewTerminal({
        ticker: cleanTicker,
        interval,
        onFallback: () => {
          setTerminalStatus("TradingView unavailable. Showing Quantura chart.");
          renderTickerChart(rows, cleanTicker, interval, overlays, { skipTradingView: true }).catch(() => {});
        },
      });
      if (rendered) {
        setTerminalChartEngineVisibility("tradingview");
        return;
      }
    }

    setTerminalChartEngineVisibility("legacy");
    if (hasOverlays) {
      setTerminalStatus("Quantura overlay mode is active on the chart.");
    }

	    const Plotly = getPlotly();
	    if (!Plotly) {
	      ui.tickerChart.textContent = "Chart library not loaded.";
	      return;
	    }

      const dateKey = extractDateKey(rows);
      if (!dateKey) {
        ui.tickerChart.textContent = "Unable to find timestamp column.";
        return;
      }

    const x = rows.map((row) => row[dateKey]);
    const xTimestamps = x
      .map((value) => {
        const ts = new Date(value).getTime();
        return Number.isFinite(ts) ? ts : null;
      })
      .filter((value) => value !== null)
      .sort((a, b) => a - b);
    const hasOhlc = ["Open", "High", "Low", "Close"].every((key) => key in (rows[0] || {}));
    const drawCandles = hasOhlc && state.chartViewMode !== "line";

    const baseTraces = drawCandles
      ? [
          {
            type: "candlestick",
            name: `${cleanTicker} price`,
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
            name: `${cleanTicker} close`,
            x,
            y: rows.map((row) => row.Close ?? row.close ?? row.AdjClose ?? null),
            line: { color: "#12182a", width: 2 },
          },
        ];

      const dark = isDarkMode();
      const isMobileViewport = typeof window !== "undefined" && window.matchMedia("(max-width: 768px)").matches;
      const textColor = dark ? "rgba(246, 244, 238, 0.92)" : "#12182a";
      const gridColor = dark ? "rgba(246, 244, 238, 0.14)" : "rgba(18, 24, 42, 0.12)";
	    const layout = {
		      title: { text: `${cleanTicker} (${interval})`, font: { family: "Manrope, sans-serif", size: 16, color: textColor } },
	      font: { family: "Manrope, sans-serif", color: textColor },
	      paper_bgcolor: "rgba(0,0,0,0)",
	      plot_bgcolor: "rgba(0,0,0,0)",
	      margin: { l: 50, r: 20, t: 40, b: isMobileViewport ? 92 : 50 },
        hovermode: "x unified",
        dragmode: "pan",
        hoverlabel: { namelength: 32 },
      xaxis: {
        rangeslider: { visible: false, thickness: 0 },
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
	      legend: {
          orientation: "h",
          y: isMobileViewport ? -0.28 : 1.05,
          yanchor: "top",
          x: 0,
          xanchor: "left",
        },
    };
    const manualRange = computeChartRange(x, state.chartRangePreset);
    if (manualRange && manualRange.length === 2) {
      layout.xaxis.range = manualRange;
      layout.xaxis.autorange = false;
    } else {
      layout.xaxis.autorange = true;
    }

    await Plotly.react(ui.tickerChart, [...baseTraces, ...overlays], layout, {
      responsive: true,
      displaylogo: false,
      displayModeBar: false,
      scrollZoom: true,
    });

    if (xTimestamps.length >= 2) {
      const minMs = xTimestamps[0];
      const maxMs = xTimestamps[xTimestamps.length - 1];
      const totalSpanMs = Math.max(1, maxMs - minMs);
      const minSpanMs = interval === "1h" ? 6 * 60 * 60 * 1000 : 3 * 24 * 60 * 60 * 1000;
      const previousHandler = ui.tickerChart.__quanturaRelayoutHandler;
      if (previousHandler && typeof ui.tickerChart.removeListener === "function") {
        try {
          ui.tickerChart.removeListener("plotly_relayout", previousHandler);
        } catch (error) {
          // Ignore listener removal errors.
        }
      }

      let relayoutLock = false;
      const guardRange = (startMs, endMs) => {
        let nextStart = Math.min(startMs, endMs);
        let nextEnd = Math.max(startMs, endMs);

        if (nextStart < minMs) nextStart = minMs;
        if (nextEnd > maxMs) nextEnd = maxMs;

        let span = nextEnd - nextStart;
        if (span > totalSpanMs) {
          nextStart = minMs;
          nextEnd = maxMs;
          span = nextEnd - nextStart;
        }
        if (span < minSpanMs) {
          const center = (nextStart + nextEnd) / 2;
          nextStart = center - minSpanMs / 2;
          nextEnd = center + minSpanMs / 2;
          if (nextStart < minMs) {
            nextStart = minMs;
            nextEnd = Math.min(maxMs, minMs + minSpanMs);
          }
          if (nextEnd > maxMs) {
            nextEnd = maxMs;
            nextStart = Math.max(minMs, maxMs - minSpanMs);
          }
        }
        return [nextStart, nextEnd];
      };

      const relayoutHandler = (eventData) => {
        if (relayoutLock || !eventData || eventData["xaxis.autorange"]) return;
        let startRaw = eventData["xaxis.range[0]"];
        let endRaw = eventData["xaxis.range[1]"];
        if (Array.isArray(eventData["xaxis.range"]) && eventData["xaxis.range"].length >= 2) {
          startRaw = eventData["xaxis.range"][0];
          endRaw = eventData["xaxis.range"][1];
        }
        const startMs = new Date(startRaw).getTime();
        const endMs = new Date(endRaw).getTime();
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return;

        const [nextStart, nextEnd] = guardRange(startMs, endMs);
        if (Math.abs(nextStart - startMs) < 1 && Math.abs(nextEnd - endMs) < 1) return;

        relayoutLock = true;
        Plotly.relayout(ui.tickerChart, {
          "xaxis.range": [new Date(nextStart).toISOString(), new Date(nextEnd).toISOString()],
          "xaxis.autorange": false,
        })
          .catch(() => {})
          .finally(() => {
            relayoutLock = false;
          });
      };
      ui.tickerChart.__quanturaRelayoutHandler = relayoutHandler;
      if (typeof ui.tickerChart.on === "function") {
        ui.tickerChart.on("plotly_relayout", relayoutHandler);
      }
    }
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
      const isMobileViewport = typeof window !== "undefined" && window.matchMedia("(max-width: 768px)").matches;
      const textColor = dark ? "rgba(246, 244, 238, 0.92)" : "#12182a";
      const gridColor = dark ? "rgba(246, 244, 238, 0.14)" : "rgba(18, 24, 42, 0.12)";
	    const layout = {
	      title: { text: "Technical indicators", font: { family: "Manrope, sans-serif", size: 14, color: textColor } },
	      font: { family: "Manrope, sans-serif", color: textColor },
	      paper_bgcolor: "rgba(0,0,0,0)",
	      plot_bgcolor: "rgba(0,0,0,0)",
	      margin: { l: 50, r: 20, t: 40, b: isMobileViewport ? 88 : 50 },
	      xaxis: { showspikes: true, spikemode: "across", spikesnap: "cursor", gridcolor: gridColor, zerolinecolor: gridColor },
	      yaxis: { zeroline: false, gridcolor: gridColor },
	      legend: {
          orientation: "h",
          y: isMobileViewport ? -0.28 : 1.05,
          yanchor: "top",
          x: 0,
          xanchor: "left",
        },
	    };

    await Plotly.react(ui.indicatorChart, traces, layout, {
      responsive: true,
      displaylogo: false,
      scrollZoom: true,
    });
  };

  const parseCsvTable = (csvText, { maxRows = 5000 } = {}) => {
    const text = String(csvText || "");
    if (!text.trim()) throw new Error("CSV file is empty.");

    const delimiter = ",";
    const rows = [];
    let row = [];
    let field = "";
    let inQuotes = false;

    const pushField = () => {
      row.push(field);
      field = "";
    };
    const pushRow = () => {
      // Drop trailing completely-empty rows.
      if (row.length === 1 && !String(row[0] || "").trim()) {
        row = [];
        return;
      }
      rows.push(row);
      row = [];
    };

    for (let i = 0; i < text.length; i += 1) {
      const ch = text[i];
      const next = text[i + 1];
      if (inQuotes) {
        if (ch === "\"" && next === "\"") {
          field += "\"";
          i += 1;
          continue;
        }
        if (ch === "\"") {
          inQuotes = false;
          continue;
        }
        field += ch;
        continue;
      }

      if (ch === "\"") {
        inQuotes = true;
        continue;
      }
      if (ch === delimiter) {
        pushField();
        continue;
      }
      if (ch === "\n") {
        pushField();
        pushRow();
        if (rows.length >= maxRows + 1) break;
        continue;
      }
      if (ch === "\r") continue;
      field += ch;
    }

    if (inQuotes) {
      // Best-effort recovery for malformed CSV.
      inQuotes = false;
    }
    if (field.length || row.length) {
      pushField();
      pushRow();
    }

    if (rows.length < 2) throw new Error("CSV must include a header row and at least one data row.");
    const headers = rows[0].map((h) => String(h || "").trim());
    const data = rows.slice(1).filter((r) => Array.isArray(r) && r.some((v) => String(v || "").trim()));

    return { headers, rows: data };
  };

  const renderCsvPreview = (table, { maxCols = 8 } = {}) => {
    if (!ui.predictionsPreview) return;
    const headers = table?.headers || [];
    const rows = table?.rows || [];
    if (!headers.length || !rows.length) {
      ui.predictionsPreview.textContent = "No preview available.";
      return;
    }

    const cols = headers.slice(0, Math.max(1, Math.min(headers.length, maxCols)));
    const pageSizeRaw = Number(state.predictionsContext?.previewPageSize || 25);
    const pageSize = [25, 50, 100, 250, 500].includes(pageSizeRaw) ? pageSizeRaw : 25;
    const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
    const currentPageRaw = Number(state.predictionsContext?.previewPage || 0);
    const currentPage = Math.max(0, Math.min(totalPages - 1, Number.isFinite(currentPageRaw) ? currentPageRaw : 0));
    state.predictionsContext.previewPage = currentPage;
    state.predictionsContext.previewPageSize = pageSize;

    const start = currentPage * pageSize;
    const end = Math.min(rows.length, start + pageSize);
    const bodyRows = rows.slice(start, end);

    const controlsMarkup = (position = "top") => `
      <div class="csv-controls csv-toolbar${position === "bottom" ? " is-bottom" : ""}" aria-label="CSV preview pagination">
        <div class="csv-group">
          <span class="small csv-footnote">Rows per page</span>
          ${[25, 50, 100, 250, 500]
            .map(
              (size) =>
                `<button class="task-chip" type="button" data-action="csv-page-size" data-size="${size}" aria-pressed="${
                  size === pageSize ? "true" : "false"
                }">${size}</button>`
            )
            .join("")}
        </div>
        <div class="csv-group">
          <button class="task-chip" type="button" data-action="csv-page" data-dir="-1" ${currentPage === 0 ? "disabled" : ""}>Prev</button>
          <span class="small csv-footnote">Page ${currentPage + 1} of ${totalPages}</span>
          <button class="task-chip" type="button" data-action="csv-page" data-dir="1" ${
            currentPage >= totalPages - 1 ? "disabled" : ""
          }>Next</button>
        </div>
      </div>
    `;

    ui.predictionsPreview.innerHTML = `
      ${controlsMarkup("top")}
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>${cols.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr>
          </thead>
          <tbody>
            ${bodyRows
              .map((r) => `<tr>${cols.map((_, idx) => `<td>${escapeHtml(r[idx] ?? "")}</td>`).join("")}</tr>`)
              .join("")}
          </tbody>
        </table>
      </div>
      ${controlsMarkup("bottom")}
      <div class="small csv-footnote" style="margin-top:10px;">
        Showing rows ${start + 1}-${end} of ${rows.length} row(s) and ${cols.length} of ${headers.length} column(s).
      </div>
    `;
  };

  const isLikelyFxTicker = (symbol) => {
    const raw = String(symbol || "").toUpperCase().trim();
    if (!raw) return false;
    if (raw.endsWith("=X")) return true;
    const normalized = raw.replace(/[^A-Z]/g, "");
    return /^[A-Z]{6}X?$/.test(normalized);
  };

  const resolveDownloadPriceDigits = (ticker) => (isLikelyFxTicker(ticker) ? 6 : 2);

  const formatDownloadPriceValue = (value, digits) => {
    const raw = String(value ?? "").trim();
    if (!raw) return "—";
    const num = Number(raw.replace(/,/g, ""));
    if (!Number.isFinite(num)) return "—";
    return num.toFixed(Math.max(0, digits));
  };

  const formatDownloadVolumeValue = (value) => {
    const raw = String(value ?? "").trim();
    if (!raw) return { compact: "—", full: "" };
    const num = Number(raw.replace(/,/g, ""));
    if (!Number.isFinite(num)) return { compact: "—", full: "" };
    const rounded = Math.round(num);
    return {
      compact: formatCompactNumber(rounded),
      full: rounded.toLocaleString(),
    };
  };

  const renderDownloadHistoryPreview = (csvText, { ticker = "", maxRows = 30 } = {}) => {
    if (!ui.downloadPreview) return;

    let table;
    try {
      table = parseCsvTable(csvText, { maxRows: 25000 });
    } catch (error) {
      ui.downloadPreview.innerHTML = `<div class="small muted">${escapeHtml(error?.message || "Unable to preview downloaded CSV.")}</div>`;
      return;
    }

    const headers = Array.isArray(table?.headers) ? table.headers : [];
    const rows = Array.isArray(table?.rows) ? table.rows : [];
    if (!headers.length || !rows.length) {
      ui.downloadPreview.innerHTML = `<div class="small muted">No history rows returned for ${escapeHtml(ticker || "this ticker")}.</div>`;
      return;
    }

    const findIndex = (...names) => {
      const targets = new Set(names.map((name) => String(name || "").trim().toLowerCase()));
      return headers.findIndex((header) => targets.has(String(header || "").trim().toLowerCase()));
    };

    const idxDate = findIndex("date", "datetime");
    const idxClose = findIndex("price", "close");
    const idxOpen = findIndex("open");
    const idxHigh = findIndex("high");
    const idxLow = findIndex("low");
    const idxVolume = findIndex("volume");
    const required = [idxDate, idxClose, idxOpen, idxHigh, idxLow, idxVolume];
    if (required.some((idx) => idx < 0)) {
      ui.downloadPreview.innerHTML = `<div class="small muted">CSV preview is available, but expected OHLCV columns were not found.</div>`;
      return;
    }

    const priceDigits = resolveDownloadPriceDigits(ticker);
    const normalizedRows = rows
      .map((row) => {
        const dateText = String(row[idxDate] ?? "").trim();
        const dt = parseDateCell(dateText);
        return {
          dateText,
          ts: dt ? dt.getTime() : Number.NaN,
          close: row[idxClose],
          open: row[idxOpen],
          high: row[idxHigh],
          low: row[idxLow],
          volume: row[idxVolume],
        };
      })
      .filter((row) => row.dateText);

    if (!normalizedRows.length) {
      ui.downloadPreview.innerHTML = `<div class="small muted">No rows available for preview.</div>`;
      return;
    }

    normalizedRows.sort((a, b) => {
      const aFinite = Number.isFinite(a.ts);
      const bFinite = Number.isFinite(b.ts);
      if (aFinite && bFinite) return b.ts - a.ts;
      if (aFinite) return -1;
      if (bFinite) return 1;
      return String(b.dateText).localeCompare(String(a.dateText));
    });

    const previewRows = normalizedRows.slice(0, Math.max(1, maxRows));
    ui.downloadPreview.innerHTML = `
      <div class="small muted" style="margin-bottom:10px;">
        Showing newest ${previewRows.length} of ${normalizedRows.length.toLocaleString()} row(s).
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Close</th>
              <th>Open</th>
              <th>High</th>
              <th>Low</th>
              <th>Volume</th>
            </tr>
          </thead>
          <tbody>
            ${previewRows
              .map((row) => {
                const volume = formatDownloadVolumeValue(row.volume);
                const volumeTitle = volume.full ? ` title="${escapeHtml(volume.full)}"` : "";
                return `
                  <tr>
                    <td>${escapeHtml(row.dateText)}</td>
                    <td>${escapeHtml(formatDownloadPriceValue(row.close, priceDigits))}</td>
                    <td>${escapeHtml(formatDownloadPriceValue(row.open, priceDigits))}</td>
                    <td>${escapeHtml(formatDownloadPriceValue(row.high, priceDigits))}</td>
                    <td>${escapeHtml(formatDownloadPriceValue(row.low, priceDigits))}</td>
                    <td${volumeTitle}>${escapeHtml(volume.compact)}</td>
                  </tr>
                `;
              })
              .join("")}
          </tbody>
        </table>
      </div>
    `;
  };

  const pad2 = (value) => String(value).padStart(2, "0");

  const parseDateCell = (raw) => {
    const text = String(raw ?? "").trim();
    if (!text) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
      const parsed = new Date(`${text}T12:00:00`);
      return Number.isFinite(parsed.getTime()) ? parsed : null;
    }
    const parsed = new Date(text);
    return Number.isFinite(parsed.getTime()) ? parsed : null;
  };

  const dateToYmd = (dt) => `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())}`;

  const isWeekday = (dt) => {
    const day = dt.getDay();
    return day >= 1 && day <= 5;
  };

  const nearestWeekdayIndex = (rowsWithDate, fromIndex) => {
    if (!Array.isArray(rowsWithDate) || !rowsWithDate.length) return -1;
    const safeIndex = Math.max(0, Math.min(rowsWithDate.length - 1, fromIndex));
    if (rowsWithDate[safeIndex]?.isWeekday) return safeIndex;
    for (let distance = 1; distance < rowsWithDate.length; distance += 1) {
      const future = safeIndex + distance;
      if (future < rowsWithDate.length && rowsWithDate[future]?.isWeekday) return future;
      const past = safeIndex - distance;
      if (past >= 0 && rowsWithDate[past]?.isWeekday) return past;
    }
    return -1;
  };

  const extractQuantileColumns = (headers) => {
    if (!Array.isArray(headers)) return [];
    const cols = [];
    headers.forEach((header, idx) => {
      const raw = String(header || "").trim();
      const normalized = raw.toLowerCase().replace(/[^a-z0-9.]/g, "");
      let q = null;

      let match = normalized.match(/^(?:p|q)(\d{1,2})$/);
      if (match) q = Number(match[1]) / 100;
      if (q === null) {
        match = normalized.match(/^(?:p|q)(0?\.\d+)$/);
        if (match) q = Number(match[1]);
      }
      if (q === null) {
        match = normalized.match(/^(?:quantile)?(0?\.\d+)$/);
        if (match) q = Number(match[1]);
      }
      if (!Number.isFinite(q) || q <= 0 || q >= 1) return;

      cols.push({
        idx,
        header: raw,
        q,
        label: `p${Math.round(q * 100)}`,
      });
    });

    return cols.sort((a, b) => a.q - b.q);
  };

  const numericCell = (row, idx) => {
    const raw = row?.[idx];
    const num = Number(String(raw ?? "").trim());
    return Number.isFinite(num) ? num : null;
  };

  const shiftYmd = (ymd, deltaDays) => {
    const base = parseDateCell(ymd);
    if (!base) return ymd;
    base.setDate(base.getDate() + Number(deltaDays || 0));
    return dateToYmd(base);
  };

  const fetchTickerHighNearDate = async (functions, ticker, ymd) => {
    const getHistory = functions.httpsCallable("get_ticker_history");
    const targetDate = parseDateCell(ymd);
    if (!targetDate) throw new Error("Unable to parse target date for price lookup.");
    const start = shiftYmd(ymd, -5);
    const end = shiftYmd(ymd, 6);
    const result = await getHistory({ ticker, interval: "1d", start, end, meta: buildMeta() });
    const rows = Array.isArray(result.data?.rows) ? result.data.rows : [];
    if (!rows.length) throw new Error("No price rows found near the selected weekday.");

    const targetYmd = dateToYmd(targetDate);
    const candidates = rows
      .map((row) => {
        const rawDate = row.Date ?? row.Datetime ?? row.ds;
        const dt = parseDateCell(rawDate);
        const high = Number(row.High ?? row.high);
        if (!dt || !Number.isFinite(high)) return null;
        return {
          ymd: dateToYmd(dt),
          high,
          ts: dt.getTime(),
        };
      })
      .filter(Boolean);

    if (!candidates.length) throw new Error("No valid highs were returned for the selected window.");

    const exact = candidates.find((item) => item.ymd === targetYmd);
    if (exact) return { ...exact, exact: true, requestedYmd: targetYmd };

    const targetTs = targetDate.getTime();
    candidates.sort((a, b) => {
      const da = Math.abs(a.ts - targetTs);
      const db = Math.abs(b.ts - targetTs);
      if (da !== db) return da - db;
      return b.ts - a.ts;
    });
    return { ...candidates[0], exact: false, requestedYmd: targetYmd };
  };

  const runPredictionsQuantileMapping = async (functions) => {
    if (!hasFullAccount()) throw new Error("Sign in to run the OpenAI CSV Agent.");
    const table = state.predictionsContext.table;
    const uploadDoc = state.predictionsContext.uploadDoc;
    const uploadId = state.predictionsContext.uploadId;
    if (!table || !Array.isArray(table.rows) || table.rows.length < 2) {
      throw new Error("Load an uploaded CSV first.");
    }
    if (!uploadDoc) {
      throw new Error("Upload metadata is not loaded yet.");
    }

    let ticker = normalizeTicker(
      uploadDoc.ticker || uploadDoc.metaTicker || ui.predictionsTicker?.value || state.tickerContext?.ticker || ""
    );
    if (!ticker) {
      const prompted = await openPromptModal({
        title: "Ticker required",
        message: "Enter the ticker symbol for this uploaded predictions CSV.",
        label: "Ticker",
        placeholder: "AAPL",
        initialValue: "",
        confirmLabel: "Save ticker",
      });
      ticker = normalizeTicker(prompted || "");
      if (!ticker) {
        throw new Error("Ticker is required to run the OpenAI CSV Agent.");
      }
      if (ui.predictionsTicker) ui.predictionsTicker.value = ticker;
      if (uploadId && state.clients?.db) {
        try {
          await state.clients.db.collection("prediction_uploads").doc(uploadId).set(
            {
              ticker,
              metaTicker: ticker,
              updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
              meta: buildMeta(),
            },
            { merge: true }
          );
        } catch (error) {
          // Keep running even if persistence fails.
        }
      }
    }

    const headers = table.headers || [];
    const rows = table.rows || [];
    const norm = (h) => String(h || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    const dateCandidates = new Set(["date", "ds", "datetime", "timestamp", "time"]);
    const dateIndex = headers.findIndex((h) => dateCandidates.has(norm(h)));
    if (dateIndex < 0) throw new Error("Could not find a date column in this CSV.");

    const quantileCols = extractQuantileColumns(headers);
    if (!quantileCols.length) throw new Error("No quantile columns were detected (expected names like p10/q50/p90).");

    const quantilesForRow = (row) =>
      quantileCols
        .map((col) => ({ ...col, value: numericCell(row, col.idx) }))
        .filter((item) => Number.isFinite(item.value));

    const rowsWithDate = rows.map((row, idx) => {
      const dt = parseDateCell(row?.[dateIndex]);
      return {
        idx,
        row,
        date: dt,
        ymd: dt ? dateToYmd(dt) : "",
        isWeekday: dt ? isWeekday(dt) : false,
      };
    });
    if (!rowsWithDate[0]?.date || !rowsWithDate[rowsWithDate.length - 1]?.date) {
      throw new Error("First/last prediction rows are missing valid dates.");
    }

    const firstRaw = rowsWithDate[0];
    const lastRaw = rowsWithDate[rowsWithDate.length - 1];
    const weekdayRows = rowsWithDate.filter((item) => item.isWeekday);
    const firstUse =
      weekdayRows.find((item) => quantilesForRow(item.row).length > 0) ||
      rowsWithDate.find((item) => quantilesForRow(item.row).length > 0) ||
      rowsWithDate[0];
    const lastUse =
      [...weekdayRows].reverse().find((item) => quantilesForRow(item.row).length > 0) ||
      [...rowsWithDate].reverse().find((item) => quantilesForRow(item.row).length > 0) ||
      rowsWithDate[rowsWithDate.length - 1];

    const startQuantiles = quantilesForRow(firstUse.row);
    if (!startQuantiles.length) throw new Error("Could not find numeric quantile values on the first weekday row.");

    let highLookup = null;
    let highLookupError = "";
    let highValue = NaN;
    try {
      highLookup = await fetchTickerHighNearDate(functions, ticker, firstUse.ymd);
      highValue = Number(highLookup.high);
    } catch (error) {
      highLookupError = String(error?.message || "Unable to fetch the reference High value.");
    }

    let selected = startQuantiles.reduce((best, item) =>
      Math.abs(item.q - 0.5) < Math.abs(best.q - 0.5) ? item : best
    , startQuantiles[0]);
    if (Number.isFinite(highValue)) {
      selected = startQuantiles[0];
      for (const candidate of startQuantiles) {
        if (highValue >= candidate.value) selected = candidate;
      }
    }

    const pointForecastValue = numericCell(lastUse.row, selected.idx);
    let resolvedPointForecast = pointForecastValue;
    if (!Number.isFinite(resolvedPointForecast)) {
      const lastQuantiles = quantilesForRow(lastUse.row);
      if (lastQuantiles.length) {
        const nearest = lastQuantiles.reduce((best, item) =>
          Math.abs(item.q - selected.q) < Math.abs(best.q - selected.q) ? item : best
        , lastQuantiles[0]);
        resolvedPointForecast = nearest.value;
      }
    }
    if (!Number.isFinite(resolvedPointForecast)) {
      throw new Error(`Last usable row is missing numeric quantile values.`);
    }

    const firstRowText = firstRaw.row.map((value) => String(value ?? "")).join(" | ");
    const lastRowText = lastRaw.row.map((value) => String(value ?? "")).join(" | ");
    const warningBits = [];
    if (!firstRaw.isWeekday) warningBits.push(`First row (${firstRaw.ymd}) is not a weekday; using ${firstUse.ymd}.`);
    if (!lastRaw.isWeekday) warningBits.push(`Last row (${lastRaw.ymd}) is not a weekday; using ${lastUse.ymd}.`);

    if (highLookupError) warningBits.push(highLookupError);
    const relation = Number.isFinite(highValue)
      ? (highValue > selected.value
          ? `High ${highValue.toFixed(2)} is above ${selected.label.toUpperCase()} start value ${selected.value.toFixed(2)}.`
          : highValue < selected.value
            ? `High ${highValue.toFixed(2)} is below ${selected.label.toUpperCase()} start value ${selected.value.toFixed(2)}.`
            : `High ${highValue.toFixed(2)} is equal to ${selected.label.toUpperCase()} start value ${selected.value.toFixed(2)}.`)
      : `Reference high was unavailable, so ${selected.label.toUpperCase()} was selected from the first usable row.`;

    const mappingResult = {
      uploadId: uploadId || "",
      uploadTitle: String(uploadDoc.title || "predictions.csv"),
      ticker,
      firstRowDate: firstRaw.ymd,
      firstRowIsWeekday: firstRaw.isWeekday,
      firstWeekdayDate: firstUse.ymd,
      lastRowDate: lastRaw.ymd,
      lastRowIsWeekday: lastRaw.isWeekday,
      lastWeekdayDate: lastUse.ymd,
      warningText: warningBits.join(" "),
      referenceHigh: Number.isFinite(highValue) ? Number(highValue.toFixed(4)) : null,
      referenceHighDate: highLookup?.ymd || "",
      selectedQuantile: selected.label,
      selectedQuantileLabel: selected.label.toUpperCase(),
      selectedQuantileStartValue: Number(selected.value.toFixed(4)),
      pointForecastValue: Number(resolvedPointForecast.toFixed(4)),
      relation,
      firstRowText,
      lastRowText,
    };

    if (ui.predictionsAgentOutput) {
      ui.predictionsAgentOutput.innerHTML = `
        <div class="small"><strong>Ticker:</strong> ${escapeHtml(ticker)}</div>
        <div class="small"><strong>Upload:</strong> ${escapeHtml(uploadDoc.title || uploadId || "predictions.csv")}</div>
        <div class="small"><strong>First row date:</strong> ${escapeHtml(firstRaw.ymd)} (${firstRaw.isWeekday ? "weekday" : "weekend"})</div>
        <div class="small"><strong>Last row date:</strong> ${escapeHtml(lastRaw.ymd)} (${lastRaw.isWeekday ? "weekday" : "weekend"})</div>
        ${warningBits.length ? `<div class="small" style="margin-top:8px;"><strong>Warning:</strong> ${escapeHtml(warningBits.join(" "))}</div>` : ""}
        ${
          Number.isFinite(highValue) && highLookup
            ? `<div class="small" style="margin-top:8px;"><strong>Reference high:</strong> ${highLookup.high.toFixed(2)} on ${escapeHtml(highLookup.ymd)}${
                highLookup.exact ? "" : " (nearest trading day)"
              }</div>`
            : `<div class="small" style="margin-top:8px;"><strong>Reference high:</strong> unavailable</div>`
        }
        <div class="small" style="margin-top:8px;"><strong>Selected quantile:</strong> ${escapeHtml(selected.label.toUpperCase())}</div>
        <div class="small">${escapeHtml(relation)}</div>
        <div class="small" style="margin-top:8px;"><strong>Point forecast (last weekday, same quantile):</strong> ${resolvedPointForecast.toFixed(4)}</div>
        <div class="small" style="margin-top:12px;"><strong>First prediction row (no header):</strong></div>
        <pre class="small" style="margin:6px 0 0; white-space:pre-wrap;">${escapeHtml(firstRowText)}</pre>
        <div class="small" style="margin-top:10px;"><strong>Last prediction row (no header):</strong></div>
        <pre class="small" style="margin:6px 0 0; white-space:pre-wrap;">${escapeHtml(lastRowText)}</pre>
      `;
    }

    logEvent("predictions_quantile_mapping", {
      upload_id: uploadId || "",
      ticker,
      quantile: selected.label,
      first_weekday: firstUse.ymd,
      last_weekday: lastUse.ymd,
    });
    return mappingResult;
  };

  const inferCsvAxes = (table) => {
    const headers = table?.headers || [];
    const rows = table?.rows || [];
    const norm = (h) => String(h || "").toLowerCase().replace(/[^a-z0-9]/g, "");

    const candidates = new Set(["date", "ds", "datetime", "timestamp", "time"]);
    let xIndex = headers.findIndex((h) => candidates.has(norm(h)));
    if (xIndex < 0) {
      xIndex = 0;
    }

    const sampleN = Math.min(50, rows.length);
    const numericScore = (idx) => {
      let ok = 0;
      let total = 0;
      for (let i = 0; i < sampleN; i += 1) {
        const raw = rows[i]?.[idx];
        const val = String(raw ?? "").trim();
        if (!val) continue;
        total += 1;
        const num = Number(val);
        if (Number.isFinite(num)) ok += 1;
      }
      return total ? ok / total : 0;
    };

    const numericCols = headers
      .map((h, idx) => ({ h, idx, score: numericScore(idx) }))
      .filter((c) => c.idx !== xIndex && c.score >= 0.6);

    const quantMatches = (h) => {
      const m = norm(h).match(/^(q|p)(\d\d)$/);
      if (!m) return null;
      return { key: m[1], q: Number(m[2]) };
    };

    const quantCols = numericCols
      .map((c) => ({ ...c, qm: quantMatches(c.h) }))
      .filter((c) => c.qm && Number.isFinite(c.qm.q))
      .sort((a, b) => a.qm.q - b.qm.q);

    let yCols = [];
    if (quantCols.length >= 2) {
      yCols = quantCols.slice(0, Math.min(6, quantCols.length));
    } else {
      const preferredNames = ["yhat", "prediction", "forecast", "price", "close", "value"];
      const preferred = [];
      for (const name of preferredNames) {
        const hit = numericCols.find((c) => norm(c.h) === name);
        if (hit) preferred.push(hit);
      }
      yCols = preferred.length ? preferred.slice(0, 4) : numericCols.slice(0, 4);
    }

    if (!yCols.length) throw new Error("No numeric columns detected to plot.");
    return { xIndex, yCols };
  };

  const renderPredictionsChart = async (table, { title = "CSV plot" } = {}) => {
    if (!ui.predictionsChart) return;
    const Plotly = getPlotly();
    if (!Plotly) {
      ui.predictionsChart.textContent = "Chart library not loaded.";
      return;
    }
    const headers = table?.headers || [];
    const rows = table?.rows || [];
    if (!headers.length || !rows.length) {
      ui.predictionsChart.textContent = "No CSV data to plot.";
      return;
    }

    const { xIndex, yCols } = inferCsvAxes(table);
    const xLabel = headers[xIndex] || "x";
    const x = rows.map((r, idx) => {
      const raw = r?.[xIndex];
      const val = raw === undefined || raw === null || raw === "" ? null : raw;
      if (val === null) return idx;
      const parsed = Date.parse(String(val));
      return Number.isFinite(parsed) ? new Date(parsed) : val;
    });

    const traces = yCols.map((col) => ({
      type: "scatter",
      mode: "lines",
      name: col.h,
      x,
      y: rows.map((r) => {
        const raw = r?.[col.idx];
        const num = Number(String(raw ?? "").trim());
        return Number.isFinite(num) ? num : null;
      }),
      line: { width: 2 },
    }));

    const dark = isDarkMode();
    const plotBg = dark ? "#0b0f1a" : "#ffffff";
    const textColor = dark ? "rgba(246, 244, 238, 0.92)" : "#12182a";
    const gridColor = dark ? "rgba(246, 244, 238, 0.14)" : "rgba(18, 24, 42, 0.12)";
    const layout = {
      title: { text: title, font: { family: "Manrope, sans-serif", size: 14, color: textColor } },
      font: { family: "Manrope, sans-serif", color: textColor },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: plotBg,
      margin: { l: 50, r: 30, t: 44, b: 44 },
      xaxis: { title: { text: xLabel }, showspikes: true, spikemode: "across", spikesnap: "cursor", gridcolor: gridColor, zerolinecolor: gridColor },
      yaxis: { gridcolor: gridColor, zerolinecolor: gridColor },
      legend: { orientation: "h" },
      hovermode: "x unified",
    };

    await Plotly.react(ui.predictionsChart, traces, layout, { responsive: true, displaylogo: false });
  };

  const resolveUploadCsvUrl = async (storage, uploadDoc) => {
    if (storage && uploadDoc?.filePath) {
      try {
        return await storage.ref().child(String(uploadDoc.filePath)).getDownloadURL();
      } catch (error) {
        // Fall back.
      }
    }
    return String(uploadDoc?.fileUrl || "").trim();
  };

  const fetchUploadCsvText = async ({ uploadId, url, maxBytes = 2_000_000 }) => {
    if (!url) throw new Error("Upload is missing a downloadable URL.");
    try {
      const resp = await fetch(url, { cache: "no-store" });
      if (!resp.ok) throw new Error("Unable to download CSV.");
      const text = await resp.text();
      return { text, truncated: false, source: "direct" };
    } catch (error) {
      const functions = state.clients?.functions;
      if (!functions) throw error;
      const callable = functions.httpsCallable("get_prediction_upload_csv");
      const result = await callable({ uploadId, maxBytes, meta: buildMeta() });
      const text = String(result.data?.csv || "");
      if (!text) throw new Error("Unable to download CSV.");
      return {
        text,
        truncated: Boolean(result.data?.truncated),
        source: "function",
      };
    }
  };

  const plotPredictionUploadById = async (db, storage, uploadId) => {
    if (!db || !uploadId) throw new Error("Upload ID is required.");
    if (!ui.predictionsChart || !ui.predictionsPreview || !ui.predictionsPlotMeta) return;
    const cleanId = String(uploadId || "").trim();
    if (!cleanId) throw new Error("Upload ID is required.");

    ui.predictionsPlotMeta.textContent = "Loading CSV...";
    if (ui.predictionsChart) setOutputLoading(ui.predictionsChart, "Loading CSV plot...");
    if (ui.predictionsPreview) setOutputLoading(ui.predictionsPreview, "Loading preview...");

    const snap = await db.collection("prediction_uploads").doc(cleanId).get();
    if (!snap.exists) throw new Error("Upload not found.");
    const doc = { id: snap.id, ...(snap.data() || {}) };

    const url = await resolveUploadCsvUrl(storage, doc);
    if (!url) throw new Error("Upload is missing a downloadable URL.");
    const { text, truncated, source } = await fetchUploadCsvText({ uploadId: cleanId, url, maxBytes: 5_000_000 });

    const table = parseCsvTable(text, { maxRows: 20000 });
    const title = doc.title ? `Upload: ${doc.title}` : "CSV plot";
    state.predictionsContext.uploadId = cleanId;
    state.predictionsContext.uploadDoc = doc;
    state.predictionsContext.table = table;
    state.predictionsContext.previewPage = 0;
    if (ui.predictionsTicker && doc.ticker) ui.predictionsTicker.value = normalizeTicker(doc.ticker) || String(doc.ticker);
    ui.predictionsPlotMeta.textContent = `${doc.title || "predictions.csv"} · ${table.rows.length.toLocaleString()} rows · ${table.headers.length} cols${
      truncated ? " · truncated" : ""
    }`;
    if (ui.predictionsAgentOutput) {
      ui.predictionsAgentOutput.innerHTML =
        "Run the OpenAI CSV Agent to compute weekday-aware quantile mapping and return an analyst summary.";
    }

    renderCsvPreview(table);
    setOutputReady(ui.predictionsPreview);
    try {
      await renderPredictionsChart(table, { title });
      setOutputReady(ui.predictionsChart);
    } catch (chartError) {
      setOutputReady(ui.predictionsChart);
      if (ui.predictionsChart) {
        ui.predictionsChart.innerHTML = `
          <div class="small muted">
            CSV preview is available, but chart rendering failed: ${escapeHtml(chartError?.message || "Unknown error")}
          </div>
        `;
      }
    }
    logEvent("predictions_plotted", { upload_id: cleanId, source });
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
	    const quantKeys = extractQuantileKeys(forecastRows);
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
    if (key === "prophet") return "Quantura Horizon";
    if (key === "sagemaker_canvas") return "SageMaker Canvas";
    return raw ? String(raw) : "Forecast";
  };

  const renderForecastPicker = (items) => {
    if (!ui.forecastLoadSelect) return;
    const list = Array.isArray(items) ? items : [];
    const previous = String(ui.forecastLoadSelect.value || "").trim();

    const options = [
      `<option value="">Select a forecast</option>`,
      ...list.slice(0, 60).map((item) => {
        const id = escapeHtml(item.id || "");
        const ticker = escapeHtml(normalizeTicker(item.ticker || "") || "Ticker");
        const service = escapeHtml(labelForecastService(item.service || ""));
        const interval = escapeHtml(String(item.interval || "") || "1d");
        const when = escapeHtml(formatTimestamp(item.createdAt));
        const label = `${ticker} · ${service} · ${interval} · ${when}`;
        return `<option value="${id}">${label}</option>`;
      }),
    ];

    ui.forecastLoadSelect.innerHTML = options.join("");
    if (previous) ui.forecastLoadSelect.value = previous;
  };

  const renderScreenerRunPicker = (items) => {
    if (!ui.screenerLoadSelect) return;
    const list = Array.isArray(items) ? items : [];
    const previous = String(ui.screenerLoadSelect.value || "").trim();

    const options = [
      `<option value="">Select a run</option>`,
      ...list.slice(0, 60).map((item) => {
        const id = escapeHtml(item.id || "");
        const title = escapeHtml(String(item.title || "").trim());
        const universe = escapeHtml(String(item.universe || "trending"));
        const market = escapeHtml(String(item.market || "us"));
        const count = Array.isArray(item.results) ? item.results.length : Number(item.count || 0) || 0;
        const when = escapeHtml(formatTimestamp(item.createdAt));
        const label = `${title || universe} · ${market.toUpperCase()} · ${count} names · ${when}`;
        return `<option value="${id}">${label}</option>`;
      }),
    ];

    ui.screenerLoadSelect.innerHTML = options.join("");
    if (previous) ui.screenerLoadSelect.value = previous;
  };

  const normalizeAiModelId = (modelId) => {
    const id = String(modelId || "").trim();
    if (!id) return "";
    const lower = id.toLowerCase();
    const aliases = {
      "gpt-5-2": "gpt-5.2",
      "gpt5.2": "gpt-5.2",
      "gpt-5-1": "gpt-5.1",
      "gpt5.1": "gpt-5.1",
      "gpt5": "gpt-5",
      "gpt5-mini": "gpt-5-mini",
      "gpt5-nano": "gpt-5-nano",
      "gpt-5-thinking": "gpt-5.2",
      "nova-micro": "amazon.nova-micro-v1:0",
      "nova-lite": "amazon.nova-lite-v1:0",
      "nova-pro": "amazon.nova-pro-v1:0",
      "amazon-nova-micro": "amazon.nova-micro-v1:0",
      "amazon-nova-lite": "amazon.nova-lite-v1:0",
      "amazon-nova-pro": "amazon.nova-pro-v1:0",
      "gemini-pro": "gemini-1.5-pro",
      "gemini-flash": "gemini-2.0-flash",
      "mistral-small": "mistral-small-latest",
      "mistral-medium": "mistral-medium-latest",
      "mistral-large": "mistral-large-latest",
      "perplexity-sonar": "sonar",
      "perplexity-sonar-pro": "sonar-pro",
    };
    if (aliases[lower]) return aliases[lower];
    if (lower.startsWith("gpt-") && lower.charAt(4) === "4") {
      return "gpt-5-mini";
    }
    if (lower.startsWith("o1")) {
      return "gpt-5.1";
    }
    if (lower.startsWith("amazon.nova")) {
      return lower;
    }
    if (lower.startsWith("gemini")) {
      return lower;
    }
    if (lower.startsWith("mistral")) {
      return lower;
    }
    if (lower.startsWith("sonar") || lower.startsWith("perplexity/sonar")) {
      return lower.replace("perplexity/", "");
    }
    if (lower.startsWith("other/")) {
      return lower;
    }
    return id;
  };

  const getModelMeta = (modelId) => {
    const id = normalizeAiModelId(modelId);
    if (!id) return null;
    return AI_MODEL_CATALOG.find((item) => item.id === id) || null;
  };

  const getWeeklyUsageKey = () => {
    const now = new Date();
    const utcDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    const day = utcDate.getUTCDay() || 7;
    utcDate.setUTCDate(utcDate.getUTCDate() + 4 - day);
    const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
    const weekNumber = Math.ceil((((utcDate - yearStart) / 86400000) + 1) / 7);
    return `${utcDate.getUTCFullYear()}-W${String(weekNumber).padStart(2, "0")}`;
  };

  const getCurrentAiTierKey = () => {
    if (!hasFullAccount()) return "free";
    if (isAdminUser()) return "business";
    if (!state.userHasPaidPlan) return "free";
    const tier = normalizeSubscriptionTier(state.userSubscriptionTier);
    return tier === "free" ? "go" : tier;
  };

  const getCurrentAiTierConfig = () => {
    const key = getCurrentAiTierKey();
    const rawGlobalAllowed = Array.isArray(state.remoteFlags?.llmAllowedModels)
      ? state.remoteFlags.llmAllowedModels
      : DEFAULT_LLM_ALLOWED_MODELS;
    const globalAllowed = rawGlobalAllowed
      .map((x) => normalizeAiModelId(String(x).trim()))
      .filter((modelId) => modelId && (modelId.startsWith("gpt-5") || modelId.startsWith("amazon.nova")));
    const globalSet = new Set(globalAllowed);
    const tiers = state.remoteFlags.aiUsageTiers && typeof state.remoteFlags.aiUsageTiers === "object"
      ? state.remoteFlags.aiUsageTiers
      : AI_USAGE_TIER_DEFAULTS;
    const normalizedKey = key === "desk" ? "business" : key;
    const config =
      tiers[normalizedKey] && typeof tiers[normalizedKey] === "object"
        ? tiers[normalizedKey]
        : tiers[key] && typeof tiers[key] === "object"
        ? tiers[key]
        : AI_USAGE_TIER_DEFAULTS[normalizedKey] || AI_USAGE_TIER_DEFAULTS[key] || AI_USAGE_TIER_DEFAULTS.free;
    const rawAllowed = Array.isArray(config.allowed_models) ? config.allowed_models : [];
    const allowedModels = rawAllowed
      .map((x) => normalizeAiModelId(String(x).trim()))
      .filter((modelId) => modelId && (modelId.startsWith("gpt-5") || modelId.startsWith("amazon.nova")))
      .filter((modelId) => !globalSet.size || globalSet.has(modelId));
    const fallbackAllowed = (AI_USAGE_TIER_DEFAULTS[normalizedKey]?.allowed_models || AI_USAGE_TIER_DEFAULTS[key]?.allowed_models || AI_USAGE_TIER_DEFAULTS.free.allowed_models)
      .map((x) => normalizeAiModelId(String(x).trim()))
      .filter((modelId) => modelId && (modelId.startsWith("gpt-5") || modelId.startsWith("amazon.nova")))
      .filter((modelId) => !globalSet.size || globalSet.has(modelId));
    const weeklyLimitRaw = Number(config.weekly_limit ?? config.daily_limit ?? AI_USAGE_TIER_DEFAULTS[normalizedKey]?.weekly_limit ?? AI_USAGE_TIER_DEFAULTS[key]?.weekly_limit ?? 3);
    const weeklyLimit = Number.isFinite(weeklyLimitRaw) ? Math.max(1, weeklyLimitRaw) : 3;
    const workspaceLimitRaw = Number(config.workspace_limit ?? config.workspaceLimit ?? AI_USAGE_TIER_DEFAULTS[normalizedKey]?.workspace_limit ?? 0);
    const workspaceLimit = Number.isFinite(workspaceLimitRaw) ? Math.max(0, Math.floor(workspaceLimitRaw)) : 0;
    const finalAllowed = allowedModels.length
      ? allowedModels
      : fallbackAllowed.length
      ? fallbackAllowed
      : globalAllowed.length
      ? globalAllowed
      : DEFAULT_LLM_ALLOWED_MODELS;
    return {
      key,
      allowedModels: finalAllowed,
      weeklyLimit,
      dailyLimit: weeklyLimit, // Legacy alias for older UI helpers.
      volatilityAlerts: Boolean(config.volatility_alerts),
      adFree: Boolean(config.ad_free ?? normalizedKey !== "free"),
      workspaceLimit,
    };
  };

  const syncScreenerProviderAccent = () => {
    if (!ui.screenerForm || !ui.screenerModel) return;
    const modelId = normalizeAiModelId(ui.screenerModel.value || "");
    const meta = getModelMeta(modelId);
    const provider = meta?.provider || "openai";
    state.selectedScreenerModel = modelId || "gpt-5-mini";
    ui.screenerForm.dataset.providerAccent = provider;
    ui.screenerForm.dataset.personality = (meta?.personality || "balanced").toLowerCase();
  };

  const bindScreenerFilterTabs = () => {
    const tabs = Array.from(document.querySelectorAll("[data-screener-filter-tab]"));
    const groups = Array.from(document.querySelectorAll("[data-screener-filter-group]"));
    if (!tabs.length || !groups.length) return;

    const activate = (groupKey) => {
      const target = String(groupKey || "descriptive").trim().toLowerCase();
      const showAll = target === "all";
      tabs.forEach((tab) => {
        const key = String(tab.dataset.screenerFilterTab || "").trim().toLowerCase();
        const active = key === target;
        tab.classList.toggle("is-active", active);
        tab.setAttribute("aria-selected", active ? "true" : "false");
      });
      groups.forEach((group) => {
        const key = String(group.dataset.screenerFilterGroup || "").trim().toLowerCase();
        const visible = showAll || key === target;
        group.classList.toggle("is-active", visible);
        group.hidden = !visible;
      });
    };

    tabs.forEach((tab) => {
      tab.setAttribute("role", "tab");
      tab.addEventListener("click", () => {
        activate(tab.dataset.screenerFilterTab || "descriptive");
      });
    });
    groups.forEach((group) => {
      group.setAttribute("role", "tabpanel");
    });

    const defaultTab = tabs.find((tab) => tab.classList.contains("is-active")) || tabs[0];
    activate(defaultTab?.dataset.screenerFilterTab || "descriptive");
  };

  const EXTRA_FUNDAMENTAL_FILTER_FIELDS = [
    {
      name: "filterPriceCash",
      label: "Price/Cash",
      options: [
        { value: "", label: "Any" },
        { value: "low", label: "Low (<3)" },
        { value: "u5", label: "Under 5" },
        { value: "u10", label: "Under 10" },
        { value: "o20", label: "Over 20" },
        { value: "o50", label: "Over 50" },
      ],
    },
    {
      name: "filterPriceFcf",
      label: "Price/Free Cash Flow",
      options: [
        { value: "", label: "Any" },
        { value: "low", label: "Low (<15)" },
        { value: "u20", label: "Under 20" },
        { value: "u50", label: "Under 50" },
        { value: "o50", label: "Over 50" },
        { value: "o100", label: "Over 100" },
      ],
    },
    {
      name: "filterEvEbitda",
      label: "EV/EBITDA",
      options: [
        { value: "", label: "Any" },
        { value: "negative", label: "Negative (<0)" },
        { value: "low", label: "Low (<15)" },
        { value: "profitable", label: "Profitable (>0)" },
        { value: "high", label: "High (>50)" },
        { value: "u20", label: "Under 20" },
        { value: "o20", label: "Over 20" },
      ],
    },
    {
      name: "filterEvSales",
      label: "EV/Sales",
      options: [
        { value: "", label: "Any" },
        { value: "negative", label: "Negative (<0)" },
        { value: "low", label: "Low (<1)" },
        { value: "positive", label: "Positive (>0)" },
        { value: "high", label: "High (>10)" },
        { value: "u2", label: "Under 2" },
        { value: "o5", label: "Over 5" },
      ],
    },
    {
      name: "filterDividendGrowth",
      label: "Dividend Growth",
      options: [
        { value: "", label: "Any" },
        { value: "1ypos", label: "1 Year Positive" },
        { value: "1yo5", label: "1 Year Over 5%" },
        { value: "3ypos", label: "3 Years Positive" },
        { value: "3yo10", label: "3 Years Over 10%" },
        { value: "5ypos", label: "5 Years Positive" },
        { value: "cy5", label: "Growing 5+ Years" },
      ],
    },
    {
      name: "filterEpsGrowthThisYear",
      label: "EPS Growth This Year",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "poslow", label: "Positive Low (0-10%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterEpsGrowthNextYear",
      label: "EPS Growth Next Year",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "poslow", label: "Positive Low (0-10%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterEpsGrowthQoq",
      label: "EPS Growth Qtr/Qtr",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "poslow", label: "Positive Low (0-10%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterEpsGrowthTtm",
      label: "EPS Growth TTM",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterEpsGrowth3Years",
      label: "EPS Growth 3Y",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterEpsGrowth5Years",
      label: "EPS Growth 5Y",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterEpsGrowthNext5Years",
      label: "EPS Growth Next 5Y",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "poslow", label: "Positive Low (<10%)" },
        { value: "high", label: "High (>25%)" },
      ],
    },
    {
      name: "filterSalesGrowthQoq",
      label: "Sales Growth Qtr/Qtr",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "poslow", label: "Positive Low (0-10%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterSalesGrowthTtm",
      label: "Sales Growth TTM",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "poslow", label: "Positive Low (0-10%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterSalesGrowth3Years",
      label: "Sales Growth 3Y",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterSalesGrowth5Years",
      label: "Sales Growth 5Y",
      options: [
        { value: "", label: "Any" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o10", label: "Over 10%" },
      ],
    },
    {
      name: "filterEarningsRevenueSurprise",
      label: "Earnings & Revenue Surprise",
      options: [
        { value: "", label: "Any" },
        { value: "bp", label: "Both positive (>0%)" },
        { value: "bm", label: "Both met (0%)" },
        { value: "bn", label: "Both negative (<0%)" },
        { value: "ep", label: "EPS Positive" },
        { value: "en", label: "EPS Negative" },
        { value: "rp", label: "Revenue Positive" },
        { value: "rn", label: "Revenue Negative" },
      ],
    },
    {
      name: "filterRoi",
      label: "Return on Invested Capital",
      options: [
        { value: "", label: "Any" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "verypos", label: "Very Positive (>25%)" },
        { value: "o10", label: "Over +10%" },
        { value: "o25", label: "Over +25%" },
      ],
    },
    {
      name: "filterCurrentRatio",
      label: "Current Ratio",
      options: [
        { value: "", label: "Any" },
        { value: "high", label: "High (>3)" },
        { value: "low", label: "Low (<1)" },
        { value: "u1", label: "Under 1" },
        { value: "o2", label: "Over 2" },
      ],
    },
    {
      name: "filterQuickRatio",
      label: "Quick Ratio",
      options: [
        { value: "", label: "Any" },
        { value: "high", label: "High (>3)" },
        { value: "low", label: "Low (<0.5)" },
        { value: "u1", label: "Under 1" },
        { value: "o2", label: "Over 2" },
      ],
    },
    {
      name: "filterLtDebtEquity",
      label: "LT Debt/Equity",
      options: [
        { value: "", label: "Any" },
        { value: "high", label: "High (>0.5)" },
        { value: "low", label: "Low (<0.1)" },
        { value: "u0.5", label: "Under 0.5" },
        { value: "o1", label: "Over 1" },
      ],
    },
    {
      name: "filterGrossMargin",
      label: "Gross Margin",
      options: [
        { value: "", label: "Any" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "high", label: "High (>50%)" },
        { value: "o30", label: "Over 30%" },
        { value: "o50", label: "Over 50%" },
      ],
    },
    {
      name: "filterOperatingMargin",
      label: "Operating Margin",
      options: [
        { value: "", label: "Any" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "veryneg", label: "Very Negative (<-20%)" },
        { value: "high", label: "High (>25%)" },
        { value: "o20", label: "Over 20%" },
      ],
    },
    {
      name: "filterPayoutRatio",
      label: "Payout Ratio",
      options: [
        { value: "", label: "Any" },
        { value: "none", label: "None (0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "low", label: "Low (<20%)" },
        { value: "high", label: "High (>50%)" },
      ],
    },
    {
      name: "filterInsiderOwnership",
      label: "Insider Ownership",
      options: [
        { value: "", label: "Any" },
        { value: "low", label: "Low (<5%)" },
        { value: "high", label: "High (>30%)" },
        { value: "veryhigh", label: "Very High (>50%)" },
      ],
    },
    {
      name: "filterInsiderTransactions",
      label: "Insider Transactions",
      options: [
        { value: "", label: "Any" },
        { value: "veryneg", label: "Very Negative (<-20%)" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "verypos", label: "Very Positive (>20%)" },
      ],
    },
    {
      name: "filterInstitutionalOwnership",
      label: "Institutional Ownership",
      options: [
        { value: "", label: "Any" },
        { value: "low", label: "Low (<5%)" },
        { value: "high", label: "High (>90%)" },
        { value: "o50", label: "Over 50%" },
      ],
    },
    {
      name: "filterInstitutionalTransactions",
      label: "Institutional Transactions",
      options: [
        { value: "", label: "Any" },
        { value: "veryneg", label: "Very Negative (<-20%)" },
        { value: "neg", label: "Negative (<0%)" },
        { value: "pos", label: "Positive (>0%)" },
        { value: "verypos", label: "Very Positive (>20%)" },
      ],
    },
  ];

  const hydrateFundamentalFilterFields = () => {
    const groups = Array.from(document.querySelectorAll('[data-screener-filter-group="fundamental"] .screener-filter-grid'));
    if (!groups.length) return;

    const makeId = (name, idx) =>
      `screener-extra-${String(name || "")
        .replace(/[^a-zA-Z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "")
        .toLowerCase()}-${idx}`;

    groups.forEach((grid, groupIdx) => {
      EXTRA_FUNDAMENTAL_FILTER_FIELDS.forEach((field) => {
        if (!field?.name || !Array.isArray(field.options)) return;
        if (grid.querySelector(`[name="${field.name}"]`)) return;

        const wrapper = document.createElement("div");
        wrapper.className = "field";

        const label = document.createElement("label");
        label.className = "label";
        const id = makeId(field.name, groupIdx);
        label.setAttribute("for", id);
        label.textContent = String(field.label || field.name);

        const select = document.createElement("select");
        select.id = id;
        select.name = field.name;
        select.innerHTML = field.options
          .map((opt) => {
            const value = escapeHtml(String(opt.value ?? ""));
            const text = escapeHtml(String(opt.label ?? ""));
            return `<option value="${value}">${text}</option>`;
          })
          .join("");

        wrapper.appendChild(label);
        wrapper.appendChild(select);
        grid.appendChild(wrapper);
      });
    });
  };

  const collectScreenerFilters = (formData) => {
    const filters = {};
    if (!(formData instanceof FormData)) return filters;
    for (const [key, value] of formData.entries()) {
      const name = String(key || "").trim();
      if (!name.startsWith("filter")) continue;
      const text = String(value || "").trim();
      if (!text) continue;
      filters[name] = text;
    }
    return filters;
  };

  const refreshScreenerModelUi = () => {
    if (!ui.screenerModel) return;
    const currentValue = normalizeAiModelId(ui.screenerModel.value || state.selectedScreenerModel || "");
    const tier = getCurrentAiTierConfig();
    state.aiUsageTierKey = tier.key;
    const allowedSet = new Set(tier.allowedModels);

    const grouped = AI_MODEL_CATALOG.reduce((acc, item) => {
      const key = item.tier || "Core";
      if (!acc[key]) acc[key] = [];
      acc[key].push(item);
      return acc;
    }, {});

    const html = Object.keys(grouped)
      .map((groupKey) => {
        const options = grouped[groupKey]
          .map((item) => {
            const locked = allowedSet.size > 0 && !allowedSet.has(item.id);
            const helper = String(item.helper || "").trim();
            const lockTier = String(item.tier || "Pro");
            const label = `${item.label}${helper ? ` - ${helper}` : ""}${locked ? ` (${lockTier})` : ""}`;
            return `<option value="${escapeHtml(item.id)}" ${locked ? "disabled" : ""} data-provider="${escapeHtml(item.provider)}" data-personality="${escapeHtml(item.personality || "balanced")}">${escapeHtml(label)}</option>`;
          })
          .join("");
        return `<optgroup label="${escapeHtml(groupKey)}">${options}</optgroup>`;
      })
      .join("");

    ui.screenerModel.innerHTML = html;
    let nextValue = currentValue;
    if (!nextValue || !AI_MODEL_CATALOG.some((item) => item.id === nextValue)) {
      nextValue = normalizeAiModelId(tier.allowedModels[0] || AI_MODEL_CATALOG[0].id);
    }
    if (allowedSet.size > 0 && !allowedSet.has(nextValue)) {
      nextValue = normalizeAiModelId(tier.allowedModels[0] || AI_MODEL_CATALOG[0].id);
    }
    ui.screenerModel.value = nextValue;
    state.selectedScreenerModel = nextValue;
    syncScreenerProviderAccent();

    if (ui.screenerModelMeta) {
      const tierLabelMap = {
        free: "Free",
        go: "Go",
        plus: "Plus",
        pro: "Pro",
        business: "Business",
        desk: "Business",
      };
      const tierLabel = tierLabelMap[tier.key] || "Free";
      const hasNova = tier.allowedModels.some((modelId) => String(modelId).startsWith("amazon.nova"));
      ui.screenerModelMeta.textContent = `${tierLabel} tier · ${tier.weeklyLimit} weekly credits · ${Math.max(
        0,
        Number(tier.workspaceLimit || 0)
      )} collaborator seat${Number(tier.workspaceLimit || 0) === 1 ? "" : "s"} · ${
        hasNova ? "GPT-5 + Nova personalities" : "GPT-5 personalities"
      }`;
    }
  };

  const refreshScreenerCreditsUi = () => {
    const tier = getCurrentAiTierConfig();
    const weekKey = getWeeklyUsageKey();
    if (state.aiUsageDateKey !== weekKey) {
      state.aiUsageDateKey = weekKey;
      state.aiUsageToday = 0;
    }
    const used = Math.max(0, Number(state.aiUsageToday || 0));
    const limit = Math.max(1, Number(tier.weeklyLimit || 3));
    const pct = Math.max(0, Math.min(100, (used / limit) * 100));
    if (ui.screenerCreditsText) {
      ui.screenerCreditsText.textContent = `${used} / ${limit}`;
    }
    if (ui.screenerCreditsFill) {
      ui.screenerCreditsFill.style.width = `${pct.toFixed(1)}%`;
    }
  };

  const loadScreenerUsageToday = async (db) => {
    if (!db || !state.user) return;
    const weekKey = getWeeklyUsageKey();
    const docId = `${state.user.uid}_${weekKey}`;
    try {
      const snap = await db.collection("usage_weekly").doc(docId).get();
      const raw = snap.exists ? Number(snap.data()?.aiScreenerRuns || 0) : 0;
      state.aiUsageToday = Number.isFinite(raw) ? Math.max(0, raw) : 0;
      state.aiUsageDateKey = weekKey;
    } catch (error) {
      state.aiUsageToday = 0;
      state.aiUsageDateKey = weekKey;
    }
    refreshScreenerCreditsUi();
  };

  const showLimitReachedModal = async (message) => {
    const upgrade = await openConfirmModal({
      title: "Limit Reached",
      message:
        String(message || "").trim() ||
        "You have reached your weekly AI screener credit limit. Upgrade your plan for higher throughput and model access.",
      confirmLabel: "Upgrade Plan",
      cancelLabel: "Close",
    });
    if (upgrade) {
      window.location.href = "/pricing";
    }
  };

  const normalizeRoiHorizonKey = (raw) => {
    const key = String(raw || "").trim().toLowerCase();
    if (key === "1m" || key === "3m" || key === "6m" || key === "1y" || key === "5y" || key === "max") return key;
    return AI_LEADERBOARD_DEFAULT_HORIZON;
  };

  const toFiniteOrNull = (value) => {
    const num = typeof value === "number" ? value : Number(value);
    return Number.isFinite(num) ? num : null;
  };

  const formatRoiPercent = (value) => {
    const num = toFiniteOrNull(value);
    if (num === null) return "—";
    return formatPercent(num * 100, { signed: true, digits: 2 });
  };

  const ensureReturnsShape = (returnsRaw = {}) => {
    const base = returnsRaw && typeof returnsRaw === "object" ? returnsRaw : {};
    const oneY = toFiniteOrNull(base["1y"]);
    const normalized = {
      "1m": toFiniteOrNull(base["1m"]),
      "3m": toFiniteOrNull(base["3m"]),
      "6m": toFiniteOrNull(base["6m"]),
      "1y": oneY,
      "5y": toFiniteOrNull(base["5y"]),
      max: toFiniteOrNull(base.max),
    };
    if (normalized["1m"] === null && oneY !== null) normalized["1m"] = oneY * 0.09;
    if (normalized["3m"] === null && oneY !== null) normalized["3m"] = oneY * 0.28;
    if (normalized["6m"] === null && oneY !== null) normalized["6m"] = oneY * 0.55;
    if (normalized["5y"] === null && oneY !== null) normalized["5y"] = oneY * 4.2;
    if (normalized.max === null) normalized.max = Math.max(normalized["5y"] || -Infinity, normalized["1y"] || -Infinity);
    return normalized;
  };

  const getAgentReturn = (agent, horizonKey) => {
    const key = normalizeRoiHorizonKey(horizonKey);
    const returns = ensureReturnsShape(agent?.returns || {});
    return toFiniteOrNull(returns[key]);
  };

  const getAgentModelMeta = (agent) => {
    const modelId = normalizeAiModelId(agent?.modelId || "");
    const fromCatalog = getModelMeta(modelId);
    if (fromCatalog) return fromCatalog;
    const provider =
      String(agent?.modelProvider || "").trim().toLowerCase() ||
      (modelId.startsWith("amazon.nova") ? "amazon_nova" : "openai");
    return {
      id: modelId || "unknown",
      provider,
      tier: String(agent?.modelTier || "Standard"),
      label: modelId || "Unknown model",
    };
  };

  const renderModelBadge = (agent) => {
    const meta = getAgentModelMeta(agent);
    const label = meta.label || meta.id || "GPT-5 personality";
    const modelTag = meta.id ? ` · ${meta.id}` : "";
    const providerClass = `model-badge-${String(meta.provider || "openai").replace(/[^a-z0-9_-]/gi, "").toLowerCase() || "openai"}`;
    return `
      <span class="model-badge ${providerClass}">
        <span class="model-badge-dot" aria-hidden="true"></span>
        ${escapeHtml(`${label}${modelTag}`)}
      </span>
    `;
  };

  const getAgentOwnerUsername = (agent) => {
    const raw = String(agent?.ownerUsername || "").trim();
    if (raw) return raw;
    const email = String(agent?.ownerEmail || "").trim();
    if (email.includes("@")) return sanitizeProfileUsername(email.split("@")[0], null);
    return "quantura_member";
  };

  const renderAgentOwnerIdentity = (agent) => {
    const avatarMeta = getProfileAvatarMeta(agent?.ownerAvatar || "bull");
    const bio = normalizeProfileBio(agent?.ownerBio || "");
    return `
      <div class="small muted ai-owner-identity">
        <span aria-hidden="true">${escapeHtml(avatarMeta.emoji || "")}</span>
        <span>${escapeHtml(avatarMeta.label || "Member")}</span>
      </div>
      ${bio ? `<div class="small muted">${escapeHtml(bio)}</div>` : ""}
    `;
  };

  const renderAgentOwnerLinks = (agent) => {
    const links = normalizeProfileSocialLinks(agent?.ownerSocialLinks || {});
    const chips = [
      ["website", "Site"],
      ["x", "X"],
      ["linkedin", "LinkedIn"],
      ["github", "GitHub"],
      ["youtube", "YouTube"],
      ["tiktok", "TikTok"],
      ["facebook", "Facebook"],
      ["instagram", "Instagram"],
      ["reddit", "Reddit"],
    ]
      .map(([key, label]) => {
        const href = String(links[key] || "").trim();
        if (!href) return "";
        return `<a class="task-chip" href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
      })
      .filter(Boolean)
      .join("");
    if (!chips) return "";
    return `<div class="task-actions ai-owner-links">${chips}</div>`;
  };

  const renderAiPortfolioSummary = (runDoc) => {
    const portfolio = runDoc?.aiPortfolio && typeof runDoc.aiPortfolio === "object" ? runDoc.aiPortfolio : null;
    if (!portfolio) {
      return `<div class="small muted">Generate an AI Portfolio to score long-term growth with Quantura Horizon and publish an Explore-ready AI Agent.</div>`;
    }
    const holdings = Array.isArray(portfolio.holdings) ? portfolio.holdings : [];
    const chips = holdings
      .slice(0, 10)
      .map((item) => {
        const symbol = escapeHtml(String(item?.symbol || "").trim() || "—");
        const roi = formatRoiPercent(item?.projectedRoi);
        return `<span class="trending-chip">${symbol} · ${roi}</span>`;
      })
      .join("");
    const rationale = escapeHtml(String(portfolio.rationale || "").trim());
    const strategy = escapeHtml(String(portfolio.strategy || "Quantura Horizon long-term growth").trim());
    const updatedAt = portfolio.updatedAt ? escapeHtml(formatTimestamp(portfolio.updatedAt)) : "";
    const footer = [updatedAt ? `Updated ${updatedAt}` : "", portfolio.agentId ? `Agent ID: ${escapeHtml(portfolio.agentId)}` : ""]
      .filter(Boolean)
      .join(" · ");
    return `
      <div class="small"><strong>Strategy:</strong> ${strategy}</div>
      ${chips ? `<div class="trending-list" style="margin-top:10px;">${chips}</div>` : ""}
      ${rationale ? `<div class="small" style="margin-top:10px;"><strong>Trade rationale:</strong> ${rationale}</div>` : ""}
      ${footer ? `<div class="small muted" style="margin-top:8px;">${footer}</div>` : ""}
    `;
  };

  const buildAIAgentSparkline = (agent) => {
    const returns = ensureReturnsShape(agent?.returns || {});
    const points = [returns["1m"], returns["3m"], returns["1y"]]
      .map((value) => toFiniteOrNull(value))
      .filter((value) => value !== null);
    if (!points.length) return "";
    const min = Math.min(...points);
    const max = Math.max(...points);
    const span = Math.max(max - min, 0.0001);
    const width = 120;
    const height = 36;
    const coords = points
      .map((value, idx) => {
        const x = (idx / Math.max(1, points.length - 1)) * width;
        const y = height - ((value - min) / span) * height;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .join(" ");
    return `<svg class="agent-sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true"><polyline points="${coords}" /></svg>`;
  };

  const renderAIAgentLeaderboard = (agents = []) => {
    const container = document.getElementById("ai-agent-leaderboard");
    if (!container) return;
    const selected = normalizeRoiHorizonKey(document.getElementById("ai-leaderboard-horizon")?.value || state.aiLeaderboardHorizon);
    const modelFilterNode = document.getElementById("ai-leaderboard-model-filter");
    let selectedModelFilter = normalizeAiModelId(modelFilterNode?.value || state.aiModelFilter || "all") || "all";
    state.aiLeaderboardHorizon = selected;
    const list = Array.isArray(agents) ? agents.slice() : [];
    if (modelFilterNode) {
      const modelSet = new Set(list.map((agent) => normalizeAiModelId(agent?.modelId || "")).filter(Boolean));
      if (!modelSet.size) AI_MODEL_CATALOG.forEach((model) => modelSet.add(model.id));
      const options = [
        `<option value="all">All personalities</option>`,
        ...Array.from(modelSet)
          .sort((a, b) => a.localeCompare(b))
          .map((modelId) => {
            const meta = getModelMeta(modelId);
            return `<option value="${escapeHtml(modelId)}">${escapeHtml(meta?.label || modelId)}</option>`;
          }),
      ];
      modelFilterNode.innerHTML = options.join("");
      if (selectedModelFilter !== "all" && !modelSet.has(selectedModelFilter)) {
        selectedModelFilter = "all";
      }
      modelFilterNode.value = selectedModelFilter;
    }
    state.aiModelFilter = selectedModelFilter;
    const filtered = list.filter(
      (agent) => selectedModelFilter === "all" || normalizeAiModelId(agent?.modelId || "") === selectedModelFilter
    );
    const ranked = filtered
      .map((agent) => ({
        ...agent,
        __roi: getAgentReturn(agent, selected),
      }))
      .sort((a, b) => {
        const ax = toFiniteOrNull(a.__roi);
        const bx = toFiniteOrNull(b.__roi);
        if (ax === null && bx === null) return 0;
        if (ax === null) return 1;
        if (bx === null) return -1;
        return bx - ax;
      });

    if (!ranked.length) {
      container.innerHTML = `<div class="small muted">No AI Agents for this filter yet. Generate one from the latest screen to publish to Explore.</div>`;
      return;
    }

    const isMobile = typeof window !== "undefined" && window.matchMedia("(max-width: 768px)").matches;
    if (isMobile) {
      container.innerHTML = `
        <div class="ai-leaderboard-cards">
          ${ranked
            .map((agent, idx) => {
              const agentId = escapeHtml(String(agent.id || "").trim());
              const name = escapeHtml(String(agent.name || "Unnamed Agent").trim());
              const roi = formatRoiPercent(agent.__roi);
              const holdings = Array.isArray(agent.holdings) ? agent.holdings : [];
              const symbols = holdings
                .slice(0, 6)
                .map((item) => escapeHtml(typeof item === "string" ? item : item?.symbol || ""))
                .filter(Boolean)
                .join(", ");
              const ownerUsername = escapeHtml(getAgentOwnerUsername(agent));
              const ownerLinks = renderAgentOwnerLinks(agent);
              const ownerIdentity = renderAgentOwnerIdentity(agent);
              const likes = Number(agent.likesCount || 0);
              const follows = Number(agent.followersCount || 0);
              const liked = state.aiLikeSet.has(String(agent.id || ""));
              const followed = state.aiFollowSet.has(String(agent.id || ""));
              const creatorWorkspaceId = String(agent.ownerId || agent.workspaceId || "").trim();
              const canSupport =
                Boolean(creatorWorkspaceId) &&
                String(state.user?.uid || "").trim() !== creatorWorkspaceId &&
                creatorWorkspaceId !== "quantura";
              return `
                <article class="ai-agent-card">
                  <div class="ai-agent-head">
                    <div class="small muted">Rank #${idx + 1}</div>
                    <div class="small muted">${selected.toUpperCase()} ROI</div>
                  </div>
                  <div class="ai-agent-name">${name}</div>
                  <div class="small muted">by @${ownerUsername}</div>
                  ${ownerIdentity}
                  <div class="ai-agent-roi">${roi}</div>
                  <div class="small">${renderModelBadge(agent)}</div>
                  ${ownerLinks}
                  ${buildAIAgentSparkline(agent)}
                  <div class="small muted">${symbols || "No holdings listed."}</div>
                  <div class="ai-agent-actions">
                    <button class="task-chip${followed ? " active" : ""}" type="button" data-action="ai-agent-follow" data-agent-id="${agentId}">
                      ${followed ? "Following" : "Follow"} (${follows})
                    </button>
                    <button class="task-chip${liked ? " active" : ""}" type="button" data-action="ai-agent-like" data-agent-id="${agentId}">
                      ${liked ? "Liked" : "Like"} (${likes})
                    </button>
                    <button class="task-chip" type="button" data-action="ai-agent-share" data-agent-id="${agentId}">Share</button>
                    ${
                      canSupport
                        ? `<button class="task-chip" type="button" data-action="ai-agent-thanks" data-creator-workspace-id="${escapeHtml(
                            creatorWorkspaceId
                          )}" data-target-id="${agentId}">Send thanks</button>
                           <button class="task-chip" type="button" data-action="ai-agent-subscribe" data-creator-workspace-id="${escapeHtml(
                             creatorWorkspaceId
                           )}" data-target-id="${agentId}">Subscribe</button>`
                        : ""
                    }
                  </div>
                </article>
              `;
            })
            .join("")}
        </div>
      `;
      return;
    }

    container.innerHTML = `
      <div class="table-wrap">
        <table class="data-table ai-leaderboard-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Agent</th>
              <th>${selected.toUpperCase()} ROI</th>
              <th>Holdings</th>
              <th>Social</th>
            </tr>
          </thead>
          <tbody>
            ${ranked
              .map((agent, idx) => {
                const agentId = escapeHtml(String(agent.id || "").trim());
                const name = escapeHtml(String(agent.name || "Unnamed Agent").trim());
                const roi = formatRoiPercent(agent.__roi);
                const holdings = Array.isArray(agent.holdings) ? agent.holdings : [];
                const symbols = holdings
                  .slice(0, 6)
                  .map((item) => escapeHtml(typeof item === "string" ? item : item?.symbol || ""))
                  .filter(Boolean)
                  .join(", ");
                const ownerUsername = escapeHtml(getAgentOwnerUsername(agent));
                const ownerLinks = renderAgentOwnerLinks(agent);
                const ownerIdentity = renderAgentOwnerIdentity(agent);
                const likes = Number(agent.likesCount || 0);
                const follows = Number(agent.followersCount || 0);
                const liked = state.aiLikeSet.has(String(agent.id || ""));
                const followed = state.aiFollowSet.has(String(agent.id || ""));
                const creatorWorkspaceId = String(agent.ownerId || agent.workspaceId || "").trim();
                const canSupport =
                  Boolean(creatorWorkspaceId) &&
                  String(state.user?.uid || "").trim() !== creatorWorkspaceId &&
                  creatorWorkspaceId !== "quantura";
                return `
                  <tr>
                    <td>${idx + 1}</td>
                    <td>
                      <div><strong>${name}</strong></div>
                      <div class="small">${renderModelBadge(agent)}</div>
                      <div class="small muted">by @${ownerUsername}</div>
                      ${ownerIdentity}
                      <div class="small muted">${escapeHtml(String(agent.description || "").trim() || "AI-generated portfolio agent")}</div>
                      ${ownerLinks}
                    </td>
                    <td><strong>${roi}</strong></td>
                    <td>${symbols || "—"}</td>
                    <td>
                      <div class="task-actions">
                        <button class="task-chip${followed ? " active" : ""}" type="button" data-action="ai-agent-follow" data-agent-id="${agentId}">
                          ${followed ? "Following" : "Follow"} (${follows})
                        </button>
                        <button class="task-chip${liked ? " active" : ""}" type="button" data-action="ai-agent-like" data-agent-id="${agentId}">
                          ${liked ? "Liked" : "Like"} (${likes})
                        </button>
                        <button class="task-chip" type="button" data-action="ai-agent-share" data-agent-id="${agentId}">Share</button>
                        ${
                          canSupport
                            ? `<button class="task-chip" type="button" data-action="ai-agent-thanks" data-creator-workspace-id="${escapeHtml(
                                creatorWorkspaceId
                              )}" data-target-id="${agentId}">Send thanks</button>
                               <button class="task-chip" type="button" data-action="ai-agent-subscribe" data-creator-workspace-id="${escapeHtml(
                                 creatorWorkspaceId
                               )}" data-target-id="${agentId}">Subscribe</button>`
                            : ""
                        }
                      </div>
                    </td>
                  </tr>
                `;
              })
              .join("")}
          </tbody>
        </table>
      </div>
    `;
  };

  const bindAIAgentLeaderboardControls = () => {
    const horizonSelect = document.getElementById("ai-leaderboard-horizon");
    if (horizonSelect) {
      if (horizonSelect.dataset.bound !== "1") {
        horizonSelect.addEventListener("change", () => {
          state.aiLeaderboardHorizon = normalizeRoiHorizonKey(horizonSelect.value);
          renderAIAgentLeaderboard(state.aiAgents);
        });
        horizonSelect.dataset.bound = "1";
      }
      horizonSelect.value = state.aiLeaderboardHorizon;
    }
    const modelFilterSelect = document.getElementById("ai-leaderboard-model-filter");
    if (modelFilterSelect) {
      if (modelFilterSelect.dataset.bound !== "1") {
        modelFilterSelect.addEventListener("change", () => {
          state.aiModelFilter = String(modelFilterSelect.value || "all");
          renderAIAgentLeaderboard(state.aiAgents);
        });
        modelFilterSelect.dataset.bound = "1";
      }
      modelFilterSelect.value = state.aiModelFilter || "all";
    }
  };

  const renderScreenerRunOutput = (runDoc) => {
    if (!ui.screenerOutput) return;
    const rows = Array.isArray(runDoc?.results) ? runDoc.results : [];
    setOutputReady(ui.screenerOutput);

    if (!rows.length) {
      state.sharedScreenerView = null;
      ui.screenerOutput.innerHTML = `
        <div class="small muted">No screener rows stored for this run.</div>
      `;
      return;
    }

    const sharedMeta = runDoc && typeof runDoc.__sharedMeta === "object" ? runDoc.__sharedMeta : null;
    const sharedShareId = String(sharedMeta?.shareId || "").trim();
    const sharedShareUrl = String(sharedMeta?.shareUrl || (sharedShareId ? buildShareUrl("screener", sharedShareId) : "")).trim();
    const sharedReadOnly = Boolean(sharedMeta?.readOnly);
    const sharedCanImport = Boolean(sharedMeta?.canImport);
    const isSharedView = Boolean(sharedShareId);

    if (isSharedView) {
      state.sharedScreenerView = {
        shareId: sharedShareId,
        shareUrl: sharedShareUrl,
        readOnly: sharedReadOnly,
        canImport: sharedCanImport,
        runId: String(runDoc.id || "").trim(),
        rows,
      };
    } else {
      state.sharedScreenerView = null;
    }

    const runId = escapeHtml(runDoc.id || "");
    const createdAt = escapeHtml(formatTimestamp(runDoc.createdAt));
    const notes = String(runDoc.notes || "").trim();
    const title = String(runDoc.title || "").trim();
    const portfolioSummary = renderAiPortfolioSummary(runDoc);
    const agentId = escapeHtml(String(runDoc?.aiPortfolio?.agentId || "").trim());
    const isPublic = Boolean(runDoc?.isPublic);
    const ownerWorkspaceId = String(runDoc?.userId || "").trim();
    const canSupportOwner = Boolean(ownerWorkspaceId) && String(state.user?.uid || "").trim() !== ownerWorkspaceId;
    const ownerUsername = escapeHtml(String(runDoc?.ownerUsername || "").trim());
    const ownerBio = escapeHtml(String(runDoc?.ownerBio || "").trim());
    const ownerAvatarMeta = getProfileAvatarMeta(String(runDoc?.ownerAvatar || "bull").trim());
    const canEditRun = !sharedReadOnly;

    const actionButtons = [];
    if (canEditRun) {
      actionButtons.push(
        `<button class="cta secondary small" type="button" data-action="download-screener" data-run-id="${runId}">Download CSV</button>`,
        `<button class="cta secondary small" type="button" data-action="rename-screener" data-run-id="${runId}">Rename</button>`,
        `<button class="cta secondary small" type="button" data-action="share-screener" data-run-id="${runId}">Share link</button>`,
        `<button class="cta secondary small" type="button" data-action="toggle-screener-public" data-run-id="${runId}" data-is-public="${
          isPublic ? "1" : "0"
        }">${isPublic ? "Make private" : "Publish public"}</button>`,
        `<button class="cta secondary small danger" type="button" data-action="delete-screener" data-run-id="${runId}">Delete</button>`
      );
    } else {
      actionButtons.push(
        `<button class="cta secondary small" type="button" data-action="download-shared-screener">Download CSV</button>`,
        `<button class="cta secondary small" type="button" data-action="copy-shared-screener-link" data-share-id="${escapeHtml(
          sharedShareId
        )}">Copy share link</button>`
      );
      if (sharedShareId && (sharedCanImport || !hasFullAccount())) {
        actionButtons.push(
          `<button class="cta secondary small" type="button" data-action="import-shared-screener" data-share-id="${escapeHtml(
            sharedShareId
          )}">${hasFullAccount() ? "Save to dashboard" : "Sign in to save"}</button>`
        );
      }
    }

    if (canSupportOwner) {
      actionButtons.push(
        `<button class="cta secondary small" type="button" data-action="screener-owner-thanks" data-creator-workspace-id="${escapeHtml(
          ownerWorkspaceId
        )}" data-target-id="${runId}">Send thanks</button>`,
        `<button class="cta secondary small" type="button" data-action="screener-owner-subscribe" data-creator-workspace-id="${escapeHtml(
          ownerWorkspaceId
        )}" data-target-id="${runId}">Subscribe</button>`
      );
    }

    const sharedNotice = isSharedView
      ? `<div class="small muted"><strong>Shared:</strong> ${
          sharedReadOnly ? "Read-only view. Only the owner can edit this run." : "Owner view from shared link."
        }</div>`
      : "";

    ui.screenerOutput.innerHTML = `
      ${title ? `<div class="small"><strong>Title:</strong> ${escapeHtml(title)}</div>` : ""}
      <div class="small"><strong>Run ID:</strong> ${runId || "—"}</div>
      <div class="small"><strong>Created:</strong> ${createdAt}</div>
      <div class="small"><strong>Visibility:</strong> ${isPublic ? "Public" : "Private"}</div>
      ${sharedNotice}
      ${
        ownerUsername || ownerBio
          ? `<div class="small muted"><strong>Owner:</strong> ${escapeHtml(ownerAvatarMeta.emoji || "")} ${
              ownerUsername ? `@${ownerUsername}` : "workspace member"
            }${ownerBio ? ` · ${ownerBio}` : ""}</div>`
          : ""
      }
      ${notes ? `<div class="small" style="margin-top:10px;"><strong>Notes:</strong> ${escapeHtml(notes)}</div>` : ""}
      <div class="hero-actions" style="margin-top:12px;">
        ${actionButtons.join("")}
      </div>
      <div class="card" style="margin-top:14px;">
        <div class="card-head">
          <h3>AI Portfolio</h3>
          ${
            canEditRun
              ? `<div class="hero-actions" style="margin-top:0;">
                  <button class="cta secondary small" type="button" data-action="generate-ai-portfolio" data-run-id="${runId}">${icon(
                    "magic-wand"
                  )}<span>Generate with Quantura Horizon</span></button>
                  <button class="cta secondary small" type="button" data-action="rename-ai-agent" data-agent-id="${agentId}" ${
                    agentId ? "" : "disabled"
                  }>${icon("edit-pencil")}<span>Rename Agent</span></button>
                </div>`
              : `<div class="small muted">AI actions are disabled in read-only mode.</div>`
          }
        </div>
        <div id="ai-portfolio-summary" class="small">${portfolioSummary}</div>
      </div>
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
              <th>Projected 1Y ROI</th>
            </tr>
          </thead>
          <tbody>
            ${rows
              .map(
                (row) => {
                  const logoRaw = String(row?.logoUrl || row?.logo_url || "").trim();
                  const logoUrl = /^https?:\/\//i.test(logoRaw) ? logoRaw : "";
                  return `
                  <tr>
                    <td>
                      <button class="link-button" type="button" data-action="pick-ticker" data-ticker="${escapeHtml(row.symbol)}" style="display:inline-flex; align-items:center; gap:8px;">
                        ${logoUrl ? `<img src="${escapeHtml(logoUrl)}" alt="" loading="lazy" style="width:16px; height:16px; border-radius:50%; object-fit:cover;" />` : ""}
                        ${escapeHtml(row.symbol)}
                      </button>
                    </td>
                    <td>${row.lastClose ?? "—"}</td>
                    <td>${row.return1m ?? "—"}</td>
                    <td>${row.return3m ?? "—"}</td>
                    <td>${row.rsi14 ?? "—"}</td>
                    <td>${row.volatility ?? "—"}</td>
                    <td>${row.score ?? "—"}</td>
                    <td>${formatRoiPercent(toFiniteOrNull(row.projectedRoi))}</td>
                  </tr>
                `;
                }
              )
              .join("")}
          </tbody>
        </table>
      </div>
    `;
    bindAIAgentLeaderboardControls();
    renderAIAgentLeaderboard(state.aiAgents);
  };

  const extractCloseFromHistoryRow = (row) => {
    const keys = ["close", "Close", "adjClose", "Adj Close", "c", "last", "price"];
    for (const key of keys) {
      const value = toFiniteOrNull(row?.[key]);
      if (value !== null) return value;
    }
    return null;
  };

  const extractDateFromHistoryRow = (row) => {
    const keys = ["date", "datetime", "timestamp", "ds", "Date", "time"];
    for (const key of keys) {
      const raw = row?.[key];
      if (!raw) continue;
      const text = String(raw).trim();
      if (!text) continue;
      const parsed = new Date(text);
      if (!Number.isNaN(parsed.getTime())) return parsed;
      const match = text.match(/^(\d{4}-\d{2}-\d{2})/);
      if (match) {
        const dt = new Date(match[1]);
        if (!Number.isNaN(dt.getTime())) return dt;
      }
    }
    return null;
  };

  const toIsoTimestamp = (date, interval = "1d") => {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
    if (String(interval || "1d").toLowerCase() === "1h") {
      return date.toISOString();
    }
    return date.toISOString().slice(0, 10);
  };

  const buildForwardForecastDates = ({ lastDate, horizon = 0, interval = "1d" } = {}) => {
    const total = Math.max(0, Number(horizon) || 0);
    const out = [];
    if (!(lastDate instanceof Date) || Number.isNaN(lastDate.getTime()) || total <= 0) return out;
    let cursor = new Date(lastDate.getTime());
    const hourly = String(interval || "1d").toLowerCase() === "1h";
    while (out.length < total) {
      if (hourly) {
        cursor = new Date(cursor.getTime() + 60 * 60 * 1000);
        out.push(toIsoTimestamp(cursor, interval));
        continue;
      }
      cursor = new Date(cursor.getTime() + 24 * 60 * 60 * 1000);
      const weekday = cursor.getUTCDay();
      if (weekday === 0 || weekday === 6) continue;
      out.push(toIsoTimestamp(cursor, interval));
    }
    return out;
  };

  const alignForecastRowsWithHistory = ({ forecastRows = [], historyRows = [], interval = "1d", horizon = 0 } = {}) => {
    const normalized = normalizeForecastSeriesRows(forecastRows);
    if (!normalized.length) return [];
    const quantileKeys = Object.keys(normalized[0] || {}).filter((key) => /^q\d{1,3}$/.test(key));
    if (!quantileKeys.length) return normalized;
    const providedDates = normalized.map((row) => String(row.ds || "").trim()).filter(Boolean);
    if (providedDates.length === normalized.length) {
      return normalized;
    }
    const historyList = Array.isArray(historyRows) ? historyRows : [];
    const lastHistoryDate = extractDateFromHistoryRow(historyList[historyList.length - 1] || null);
    if (!lastHistoryDate) return normalized;
    const generatedDates = buildForwardForecastDates({
      lastDate: lastHistoryDate,
      horizon: Math.max(Number(horizon) || 0, normalized.length),
      interval,
    });
    if (!generatedDates.length) return normalized;
    return normalized.map((row, idx) => ({
      ...row,
      ds: generatedDates[idx] || row.ds,
    }));
  };

  const summarizeTickerRationale = ({ projectedRoi, q4Seasonality }) => {
    if (projectedRoi > 0 && q4Seasonality) {
      return "Quantura Horizon detects strong upward trend with recurring Q4 seasonal strength.";
    }
    if (projectedRoi > 0) {
      return "Quantura Horizon projects a positive long-term slope with supportive confidence structure.";
    }
    return "Trend is mixed and confidence is weaker versus peers.";
  };

  const averageNumber = (values = []) => {
    const nums = values.map((value) => toFiniteOrNull(value)).filter((value) => value !== null);
    if (!nums.length) return null;
    const total = nums.reduce((acc, value) => acc + value, 0);
    return total / nums.length;
  };

  const buildProphetTickerScore = async ({ functions, db, ticker, workspaceId }) => {
    const cleanTicker = normalizeTicker(ticker);
    if (!cleanTicker) return null;

    const endDate = new Date();
    const startDate = new Date(endDate);
    startDate.setFullYear(startDate.getFullYear() - 2);
    const start = startDate.toISOString().slice(0, 10);
    const end = endDate.toISOString().slice(0, 10);

    const getHistory = functions.httpsCallable("get_ticker_history");
    const historyResult = await getHistory({ ticker: cleanTicker, interval: "1d", start, end, meta: buildMeta() });
    const historyRows = Array.isArray(historyResult.data?.rows) ? historyResult.data.rows : [];
    if (!historyRows.length) return null;

    const closeSeries = historyRows.map((row) => extractCloseFromHistoryRow(row)).filter((value) => value !== null);
    if (!closeSeries.length) return null;
    const currentPrice = closeSeries[closeSeries.length - 1];
    if (currentPrice === null || currentPrice <= 0) return null;

    const runForecast = functions.httpsCallable("run_timeseries_forecast");
    const forecastResult = await runForecast({
      ticker: cleanTicker,
      horizon: 365,
      interval: "1d",
      service: "prophet",
      dailySeasonality: true,
      quantiles: [0.1, 0.5, 0.9],
      workspaceId,
      start,
      meta: buildMeta(),
    });
    const forecastData = forecastResult.data && typeof forecastResult.data === "object" ? forecastResult.data : {};
    const forecastRows = normalizeForecastSeriesRows(forecastData.forecastSeries || forecastData.forecastRows || []);
    if (!forecastRows.length) return null;

    const lastRow = forecastRows[forecastRows.length - 1] || {};
    const yhat = toFiniteOrNull(lastRow.q50 ?? lastRow.yhat ?? lastRow.median);
    const yhatLower = toFiniteOrNull(lastRow.q10 ?? lastRow.yhat_lower ?? lastRow.lower);
    if (yhat === null) return null;
    if (yhatLower !== null && yhatLower < 0) return null;

    const projectedRoi = (yhat - currentPrice) / currentPrice;

    const forecastWithDate = forecastRows
      .map((row) => {
        const ds = row?.ds ? new Date(String(row.ds)) : null;
        const value = toFiniteOrNull(row?.q50 ?? row?.yhat ?? row?.median);
        return {
          ds: ds && !Number.isNaN(ds.getTime()) ? ds : null,
          value,
        };
      })
      .filter((row) => row.ds && row.value !== null);

    const q4Values = forecastWithDate.filter((row) => row.ds.getUTCMonth() >= 9).map((row) => row.value);
    const q4Avg = averageNumber(q4Values);
    const fullAvg = averageNumber(forecastWithDate.map((row) => row.value));
    const q4Seasonality = q4Avg !== null && fullAvg !== null && q4Avg > fullAvg * 1.02;

    return {
      symbol: cleanTicker,
      currentPrice,
      projectedRoi,
      yhat,
      yhatLower,
      q4Seasonality,
      rationale: summarizeTickerRationale({ projectedRoi, q4Seasonality }),
    };
  };

  const buildPortfolioReturns = ({ holdings, screenerRows }) => {
    const bySymbol = new Map(
      (Array.isArray(screenerRows) ? screenerRows : []).map((row) => [normalizeTicker(row?.symbol || ""), row || {}])
    );
    const oneY = averageNumber((holdings || []).map((item) => item.projectedRoi));
    const oneMFromScreen = averageNumber(
      (holdings || []).map((item) => {
        const row = bySymbol.get(item.symbol);
        const val = toFiniteOrNull(row?.return1m);
        return val === null ? null : val / 100;
      })
    );
    const threeMFromScreen = averageNumber(
      (holdings || []).map((item) => {
        const row = bySymbol.get(item.symbol);
        const val = toFiniteOrNull(row?.return3m);
        return val === null ? null : val / 100;
      })
    );
    return ensureReturnsShape({
      "1m": oneMFromScreen !== null ? oneMFromScreen : oneY !== null ? oneY * 0.09 : null,
      "3m": threeMFromScreen !== null ? threeMFromScreen : oneY !== null ? oneY * 0.28 : null,
      "6m": oneY !== null ? oneY * 0.55 : null,
      "1y": oneY,
      "5y": oneY !== null ? oneY * 4.2 : null,
      max: null,
    });
  };

  const buildPortfolioRationale = (holdings = []) => {
    const strong = holdings.filter((item) => item.q4Seasonality).length;
    const avgRoi = averageNumber(holdings.map((item) => item.projectedRoi));
    if (strong > 0) {
      return "Quantura Horizon detects strong upward trend with recurring Q4 seasonal strength. The portfolio is further filtered for positive confidence structure and ranked by projected 1-year ROI.";
    }
    if (avgRoi !== null && avgRoi > 0) {
      return "This portfolio emphasizes names with positive Quantura Horizon slope and favorable 1-year risk-adjusted upside. Selections were constrained to avoid negative lower confidence outcomes.";
    }
    return "Selected for relative long-term strength versus peers while preserving diversification across sectors and factor regimes.";
  };

  const persistAIAgentSocialAction = async ({ functions, payload }) => {
    try {
      const action = functions.httpsCallable("upsert_ai_agent_social_action");
      await action(payload);
      return true;
    } catch (error) {
      return false;
    }
  };

  const startCreatorSupportCheckout = async ({
    functions,
    creatorWorkspaceId,
    mode,
    targetId = "",
    targetType = "profile",
  }) => {
    if (!hasFullAccount()) {
      showToast("Sign in to support creators.", "warn");
      return;
    }
    const creatorId = String(creatorWorkspaceId || "").trim();
    if (!creatorId) {
      showToast("Creator account is unavailable.", "warn");
      return;
    }
    if (creatorId === String(state.user.uid || "").trim()) {
      showToast("You cannot subscribe to your own profile.", "warn");
      return;
    }
    const createCheckout = functions.httpsCallable("create_creator_support_checkout");
    const payload = {
      creatorWorkspaceId: creatorId,
      mode: mode === "subscribe" ? "subscription" : "tip",
      targetType: String(targetType || "profile").trim().toLowerCase(),
      targetId: String(targetId || "").trim(),
      meta: buildMeta(),
    };
    const result = await createCheckout(payload);
    const url = String(result.data?.url || "").trim();
    if (!url) throw new Error("Stripe checkout URL is missing.");
    window.location.assign(url);
  };

  const toggleAIAgentSocial = async ({ kind, agentId, db, functions }) => {
    if (!hasFullAccount()) {
      showToast("Sign in to interact with AI Agents.", "warn");
      return;
    }
    const workspaceId = state.activeWorkspaceId || state.user.uid;
    if (!workspaceId || !agentId) return;

    const actionKey = kind === "follow" ? "follow" : "like";
    const socialCollection = actionKey === "follow" ? "ai_agent_followers" : "ai_agent_likes";
    const userDocId = `${agentId}__${state.user.uid}`;
    const socialRef = db.collection("users").doc(workspaceId).collection(socialCollection).doc(userDocId);
    const agentRef = db.collection("users").doc(workspaceId).collection("ai_agents").doc(agentId);
    const countField = actionKey === "follow" ? "followersCount" : "likesCount";

    const snap = await socialRef.get();
    const active = snap.exists;

    const persistedToServer = await persistAIAgentSocialAction({
      functions,
      payload: { workspaceId, agentId, action: actionKey, active: !active, meta: buildMeta() },
    });

    if (!persistedToServer) {
      await db.runTransaction(async (txn) => {
        const agentSnap = await txn.get(agentRef);
        const agentData = agentSnap.exists ? agentSnap.data() || {} : {};
        const previous = Number(agentData[countField] || 0);
        if (active) {
          txn.delete(socialRef);
          txn.set(
            agentRef,
            {
              [countField]: Math.max(0, previous - 1),
              updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          );
        } else {
          txn.set(socialRef, {
            agentId,
            workspaceId,
            userId: state.user.uid,
            userEmail: state.user.email || "",
            createdAt: firebase.firestore.FieldValue.serverTimestamp(),
            meta: buildMeta(),
          });
          txn.set(
            agentRef,
            {
              [countField]: previous + 1,
              updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          );
        }
      });
    }

    showToast(actionKey === "follow" ? (!active ? "Following agent." : "Unfollowed agent.") : !active ? "Agent liked." : "Like removed.");
  };

  const buildAIAgentShareUrl = (agentId) => {
    const url = new URL(window.location.origin + "/screener");
    url.searchParams.set("agentId", String(agentId || "").trim());
    return url.toString();
  };

  const upsertAIAgentFromPortfolio = async ({ db, workspaceId, runId, runDoc, portfolio }) => {
    const collection = db.collection("users").doc(workspaceId).collection("ai_agents");
    const existingId = String(runDoc?.aiPortfolio?.agentId || "").trim();
    const nextId = existingId || `agent_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const ownerUsername = sanitizeProfileUsername(state.userProfile?.username || "", state.user);
    const ownerSocialLinks = normalizeProfileSocialLinks(state.userProfile?.socialLinks || {});
    const ownerAvatar = normalizeProfileAvatar(state.userProfile?.avatar || "bull");
    const ownerBio = normalizeProfileBio(state.userProfile?.bio || "");
    const ownerPublicProfile = Boolean(state.userProfile?.publicProfile);
    const payload = {
      name: portfolio.name,
      description: portfolio.description,
      strategy: portfolio.strategy,
      holdings: portfolio.holdings,
      returns: portfolio.returns,
      rationale: portfolio.rationale,
      sourceRunId: runId,
      likesCount: Number(runDoc?.likesCount || 0),
      followersCount: Number(runDoc?.followersCount || 0),
      ownerId: state.user?.uid || "",
      ownerEmail: state.user?.email || "",
      ownerUsername,
      ownerSocialLinks,
      ownerAvatar,
      ownerBio,
      ownerPublicProfile,
      workspaceId,
      updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
      createdAt: firebase.firestore.FieldValue.serverTimestamp(),
      meta: buildMeta(),
    };
    await collection.doc(nextId).set(payload, { merge: true });
    return nextId;
  };

  const generateAIPortfolioForRun = async ({ db, functions, runId, preferredName = "", selectedModel = "" }) => {
    if (!hasFullAccount()) {
      showToast("Sign in to generate AI Portfolios.", "warn");
      return;
    }
    const workspaceId = state.activeWorkspaceId || state.user.uid;
    const runSnap = await db.collection("screener_runs").doc(runId).get();
    if (!runSnap.exists) throw new Error("Screener run not found.");
    const runDoc = { id: runSnap.id, ...(runSnap.data() || {}) };
    const modelId = normalizeAiModelId(selectedModel || runDoc.modelUsed || state.selectedScreenerModel || "gpt-5-mini");
    const modelMeta = getModelMeta(modelId) || {
      id: modelId,
      provider: "openai",
      tier: "Standard",
      label: modelId || "Model",
    };
    const rows = Array.isArray(runDoc.results) ? runDoc.results : [];
    const tickers = Array.from(
      new Set(
        rows
          .map((row) => normalizeTicker(row?.symbol || ""))
          .filter(Boolean)
          .slice(0, 20)
      )
    );
    if (!tickers.length) throw new Error("No tickers found in this run.");

    const summary = document.getElementById("ai-portfolio-summary");
    if (summary) summary.innerHTML = `<div class="small muted">Running Quantura Horizon across ${tickers.length} tickers (2-year history each)...</div>`;

    const scored = [];
    for (let idx = 0; idx < tickers.length; idx += 1) {
      const ticker = tickers[idx];
      if (summary) summary.innerHTML = `<div class="small muted">Scoring ${ticker} (${idx + 1}/${tickers.length})...</div>`;
      try {
        const score = await buildProphetTickerScore({ functions, db, ticker, workspaceId });
        if (score && score.projectedRoi > 0) scored.push(score);
      } catch (error) {
        // Skip individual symbol failures.
      }
    }

    const ranked = scored
      .filter((item) => item.yhatLower === null || item.yhatLower >= 0)
      .sort((a, b) => b.projectedRoi - a.projectedRoi);
    if (!ranked.length) throw new Error("No eligible Quantura Horizon candidates. Try broader screener criteria.");

    const topCount = Math.max(5, Math.min(10, ranked.length));
    const holdings = ranked.slice(0, topCount).map((row) => ({
      symbol: row.symbol,
      projectedRoi: row.projectedRoi,
      currentPrice: row.currentPrice,
      forecastPrice: row.yhat,
      yhatLower: row.yhatLower,
      q4Seasonality: row.q4Seasonality,
      rationale: row.rationale,
    }));

    const baseName = String(preferredName || runDoc.aiPortfolio?.name || "").trim();
    const chosenName = baseName || `Quantura Horizon ${new Date().toISOString().slice(0, 10)}`;
    const returns = buildPortfolioReturns({ holdings, screenerRows: rows });
    const rationale = buildPortfolioRationale(holdings);
    const ownerUsername = sanitizeProfileUsername(state.userProfile?.username || "", state.user);
    const ownerSocialLinks = normalizeProfileSocialLinks(state.userProfile?.socialLinks || {});
    const ownerAvatar = normalizeProfileAvatar(state.userProfile?.avatar || "bull");
    const ownerBio = normalizeProfileBio(state.userProfile?.bio || "");
    const ownerPublicProfile = Boolean(state.userProfile?.publicProfile);
    const roiBySymbol = new Map(holdings.map((item) => [item.symbol, item.projectedRoi]));
    const enrichedResults = rows.map((row) => {
      const symbol = normalizeTicker(row?.symbol || "");
      const roi = symbol ? roiBySymbol.get(symbol) : null;
      return {
        ...row,
        projectedRoi: roi !== undefined ? roi : row?.projectedRoi ?? null,
      };
    });
    const agentPayload = {
      name: chosenName,
      description: `AI Portfolio generated from screener criteria using Quantura Horizon long-term growth scoring (${modelMeta.label}).`,
      strategy: "quantura_horizon_long_term_growth",
      holdings,
      returns,
      rationale,
      modelId: modelMeta.id,
      modelProvider: modelMeta.provider,
      modelTier: modelMeta.tier,
      modelLabel: modelMeta.label,
      ownerUsername,
      ownerSocialLinks,
      ownerAvatar,
      ownerBio,
      ownerPublicProfile,
    };
    const agentId = await upsertAIAgentFromPortfolio({
      db,
      workspaceId,
      runId,
      runDoc,
      portfolio: agentPayload,
    });

    await db
      .collection("screener_runs")
      .doc(runId)
      .set(
        {
          results: enrichedResults,
          modelUsed: modelMeta.id,
          modelProvider: modelMeta.provider,
          modelTier: modelMeta.tier,
          aiPortfolio: {
            ...agentPayload,
            agentId,
            updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
          },
        },
        { merge: true }
      );

    const refreshed = await db.collection("screener_runs").doc(runId).get();
    if (refreshed.exists) {
      renderScreenerRunOutput({ id: refreshed.id, ...(refreshed.data() || {}) });
    }
    showToast("AI Portfolio generated and published to Explore.");
  };

  const startAIAgentSocial = (db, workspaceId) => {
    if (state.unsubscribeAIFollows) state.unsubscribeAIFollows();
    if (state.unsubscribeAILikes) state.unsubscribeAILikes();
    state.aiFollowSet = new Set();
    state.aiLikeSet = new Set();
    if (!workspaceId || !state.user) return;

    const userId = state.user.uid;
    state.unsubscribeAIFollows = db
      .collection("users")
      .doc(workspaceId)
      .collection("ai_agent_followers")
      .where("userId", "==", userId)
      .onSnapshot(
        (snapshot) => {
          state.aiFollowSet = new Set(snapshot.docs.map((doc) => String(doc.data()?.agentId || "").trim()).filter(Boolean));
          renderAIAgentLeaderboard(state.aiAgents);
        },
        () => {
          state.aiFollowSet = new Set();
          renderAIAgentLeaderboard(state.aiAgents);
        }
      );

    state.unsubscribeAILikes = db
      .collection("users")
      .doc(workspaceId)
      .collection("ai_agent_likes")
      .where("userId", "==", userId)
      .onSnapshot(
        (snapshot) => {
          state.aiLikeSet = new Set(snapshot.docs.map((doc) => String(doc.data()?.agentId || "").trim()).filter(Boolean));
          renderAIAgentLeaderboard(state.aiAgents);
        },
        () => {
          state.aiLikeSet = new Set();
          renderAIAgentLeaderboard(state.aiAgents);
        }
      );
  };

  const seedDefaultAIAgents = async (db, workspaceId) => {
    if (!state.user || !workspaceId) return;
    if (state.aiDefaultsSeededWorkspaceId === workspaceId) return;
    const collection = db.collection("users").doc(workspaceId).collection("ai_agents");
    const writes = DEFAULT_AI_AGENTS.map((agent) =>
      collection.doc(`default_${agent.id}`).set(
        {
          ...agent,
          isDefault: true,
          workspaceId,
          ownerId: "quantura",
          ownerUsername: "quantura",
          ownerEmail: "system@quantura.ai",
          ownerSocialLinks: {
            website: "https://quantura.studio/",
            x: "",
            linkedin: "",
            github: "",
            youtube: "",
            tiktok: "",
            facebook: "",
            instagram: "",
            reddit: "",
          },
          ownerAvatar: "bull",
          ownerBio: "Quantura system strategy templates.",
          ownerPublicProfile: true,
          updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
          createdAt: firebase.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      )
    );
    await Promise.all(writes);
    state.aiDefaultsSeededWorkspaceId = workspaceId;
  };

  const buildPresetScreenerRows = (symbols = []) =>
    symbols.slice(0, 12).map((symbol, idx) => {
      const base = Math.max(0.2, 1 - idx * 0.08);
      return {
        symbol,
        lastClose: null,
        return1m: Number((base * 3.2).toFixed(2)),
        return3m: Number((base * 8.1).toFixed(2)),
        rsi14: Number((52 + idx * 1.8).toFixed(2)),
        volatility: Number((0.22 + idx * 0.008).toFixed(4)),
        score: Number(base.toFixed(6)),
        marketCap: null,
        marketCapLabel: "—",
      };
    });

  const seedAdminPresetScreenerRuns = async (db, workspaceId) => {
    if (!state.user || !db || !workspaceId) return;
    if (!isAdminUser(state.user)) return;

    const markerRef = db.collection("users").doc(workspaceId).collection("settings").doc("admin_screener_seed");
    const marker = await markerRef.get();
    const seedVersion = "2026-02-17-social-presets-v1";
    if (marker.exists && String(marker.data()?.version || "") === seedVersion) return;

    const batch = db.batch();
    const now = firebase.firestore.FieldValue.serverTimestamp();
    ADMIN_SCREENER_PRESET_RUNS.forEach((preset, idx) => {
      const runRef = db.collection("screener_runs").doc(`admin_${preset.id}`);
      batch.set(
        runRef,
        {
          userId: workspaceId,
          userEmail: state.user.email || "",
          createdByUid: state.user.uid,
          createdByEmail: state.user.email || "",
          market: "us",
          universe: "trending",
          maxNames: 12,
          status: "completed",
          title: preset.title,
          notes: preset.notes,
          results: buildPresetScreenerRows(preset.symbols || []),
          modelUsed: preset.modelUsed,
          modelTier: "desk",
          weeklyLimit: 75,
          dailyLimit: 75,
          allowedModels: ["gpt-5-nano", "gpt-5-mini", "gpt-5", "gpt-5.1", "gpt-5.2"],
          filters: {},
          noteSignals: {
            tickers: preset.symbols || [],
            queries: [preset.notes],
            matchedHints: ["admin_seed"],
            usedWebSearch: true,
          },
          fallbackUsed: false,
          createdAt: now,
          updatedAt: now,
          meta: {
            source: "admin_seed",
            presetIndex: idx,
          },
        },
        { merge: true }
      );
    });
    batch.set(
      markerRef,
      {
        version: seedVersion,
        updatedAt: now,
      },
      { merge: true }
    );
    await batch.commit();
  };

  const startAIAgents = (db, workspaceId) => {
    if (state.unsubscribeAIAgents) state.unsubscribeAIAgents();
    state.aiAgents = [];
    if (!workspaceId) return;
    state.unsubscribeAIAgents = db
      .collection("users")
      .doc(workspaceId)
      .collection("ai_agents")
      .orderBy("updatedAt", "desc")
      .limit(120)
      .onSnapshot(
        (snapshot) => {
          state.aiAgents = snapshot.docs.map((doc) => ({ id: doc.id, ...(doc.data() || {}) }));
          bindAIAgentLeaderboardControls();
          renderAIAgentLeaderboard(state.aiAgents);
        },
        () => {
          state.aiAgents = [];
          bindAIAgentLeaderboardControls();
          renderAIAgentLeaderboard(state.aiAgents);
        }
      );
    startAIAgentSocial(db, workspaceId);
  };

  const loadScreenerRunById = async (db, runId) => {
    if (!db || !runId) throw new Error("Run ID is required.");
    if (!hasSessionUser()) {
      try {
        await ensureSessionUser({
          reason: "screener_load_requires_session",
          message: "Sign in to sync saved screener runs.",
        });
      } catch (error) {
        throw new Error("Sign in to load saved runs.");
      }
    }

    const cleanId = String(runId || "").trim();
    if (!cleanId) throw new Error("Run ID is required.");

    setOutputLoading(ui.screenerOutput, "Loading saved run...");
    const doc = await db.collection("screener_runs").doc(cleanId).get();
    if (!doc.exists) throw new Error("Run not found.");
    const data = doc.data() || {};
    state.sharedScreenerView = null;
    renderScreenerRunOutput({ id: doc.id, ...data });
    logEvent("screener_loaded_saved", { run_id: doc.id });
  };

  const extractQuantileKeys = (rows) => {
    const list = Array.isArray(rows) ? rows : [];
    const set = new Set();
    for (let i = 0; i < Math.min(list.length, 500); i += 1) {
      const row = list[i] || {};
      Object.keys(row).forEach((key) => {
        // Support q05/q50/q95 plus edge cases like q100 when users request extreme quantiles.
        if (/^q\d{1,3}$/.test(key)) set.add(key);
      });
    }
    return Array.from(set).sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
  };

  const extractForecastKeyLevels = (rows = []) => {
    const normalizedRows = normalizeForecastSeriesRows(rows);
    if (!normalizedRows.length) {
      return { support: null, median: null, resistance: null, bandWidth: null, bandWidthPct: null };
    }
    const quantileKeys = extractQuantileKeys(normalizedRows);
    if (!quantileKeys.length) {
      return { support: null, median: null, resistance: null, bandWidth: null, bandWidthPct: null };
    }
    const last = normalizedRows[normalizedRows.length - 1] || {};
    const supportKey = quantileKeys[0];
    const resistanceKey = quantileKeys[quantileKeys.length - 1];
    const medianKey =
      quantileKeys.find((key) => key === "q50") ||
      quantileKeys
        .slice()
        .sort((a, b) => Math.abs(Number(a.slice(1)) - 50) - Math.abs(Number(b.slice(1)) - 50))[0];
    const support = Number(last[supportKey]);
    const median = Number(last[medianKey]);
    const resistance = Number(last[resistanceKey]);
    const bandWidth = Number.isFinite(resistance) && Number.isFinite(support) ? resistance - support : null;
    const bandWidthPct =
      Number.isFinite(bandWidth) && Number.isFinite(median) && median > 0 ? (bandWidth / median) * 100 : null;
    return {
      support: Number.isFinite(support) ? support : null,
      median: Number.isFinite(median) ? median : null,
      resistance: Number.isFinite(resistance) ? resistance : null,
      bandWidth: Number.isFinite(bandWidth) ? bandWidth : null,
      bandWidthPct: Number.isFinite(bandWidthPct) ? bandWidthPct : null,
    };
  };

  const buildForecastAutoSummaryContext = (forecastDoc = {}) => {
    const rows = normalizeForecastSeriesRows(forecastDoc.forecastRows || []);
    const metrics = forecastDoc.metrics && typeof forecastDoc.metrics === "object" ? forecastDoc.metrics : {};
    const keyLevels = extractForecastKeyLevels(rows);
    const recentClose = Number(metrics.lastClose);
    const finalMedian = Number(keyLevels.median);
    const delta = Number.isFinite(recentClose) && Number.isFinite(finalMedian) ? finalMedian - recentClose : null;
    const deltaPct =
      Number.isFinite(delta) && Number.isFinite(recentClose) && recentClose > 0 ? (delta / recentClose) * 100 : null;
    return {
      ticker: normalizeTicker(forecastDoc.ticker || state.tickerContext.ticker || ""),
      interval: String(forecastDoc.interval || state.tickerContext.interval || "1d"),
      horizon: Number(forecastDoc.horizon || metrics.horizon || rows.length || 0) || rows.length || 0,
      service: String(forecastDoc.service || "prophet"),
      quantiles: Array.isArray(forecastDoc.quantiles) ? forecastDoc.quantiles : [],
      rowCount: rows.length,
      recentClose: Number.isFinite(recentClose) ? recentClose : null,
      finalMedian: Number.isFinite(finalMedian) ? finalMedian : null,
      support: keyLevels.support,
      resistance: keyLevels.resistance,
      bandWidth: keyLevels.bandWidth,
      bandWidthPct: keyLevels.bandWidthPct,
      medianDelta: Number.isFinite(delta) ? delta : null,
      medianDeltaPct: Number.isFinite(deltaPct) ? deltaPct : null,
      mae: Number.isFinite(Number(metrics.mae)) ? Number(metrics.mae) : null,
      coverage10_90: Number.isFinite(Number(metrics.coverage10_90)) ? Number(metrics.coverage10_90) : null,
      volatility: Number.isFinite(Number(metrics.volatility)) ? Number(metrics.volatility) : null,
      drift: Number.isFinite(Number(metrics.drift)) ? Number(metrics.drift) : null,
    };
  };

  const syncForecastAiSummaryToMyRequest = async ({ requestId = "", summary = null } = {}) => {
    if (!hasSessionUser()) return;
    const reqId = String(requestId || "").trim();
    if (!reqId) return;
    const myRequestId = `forecast__${reqId}`;
    const existing = getMyRequestById(myRequestId);
    if (!existing) return;
    const outputsMeta = existing.outputsMeta && typeof existing.outputsMeta === "object" ? existing.outputsMeta : {};
    const nextOutputsMeta = {
      ...outputsMeta,
      aiSummary: String(summary?.text || "").trim().slice(0, 1600),
      aiProvider: String(summary?.provider || "").trim(),
      aiModel: String(summary?.model || "").trim(),
      aiLatencyMs: Number(summary?.latencyMs || 0) || 0,
      aiGeneratedAt: Date.now(),
    };
    await upsertMyRequest({
      type: "forecast",
      requestId: myRequestId,
      title: String(existing.title || ""),
      input: existing.input && typeof existing.input === "object" ? existing.input : {},
      outputsMeta: nextOutputsMeta,
      sourceRef: existing.sourceRef && typeof existing.sourceRef === "object" ? existing.sourceRef : { collection: "forecast_requests", id: reqId },
      published: Boolean(existing.published),
    }).catch(() => {});
  };

  const runForecastAutoSummary = async ({ forecastDoc = null, requestId = "", notify = false } = {}) => {
    const doc = forecastDoc && typeof forecastDoc === "object" ? forecastDoc : null;
    const targetId = String(requestId || doc?.id || state.tickerContext.forecastId || "").trim();
    if (!doc || !targetId) return null;
    const rows = normalizeForecastSeriesRows(doc.forecastRows || []);
    if (!rows.length) return null;

    const requestToken = `forecast_ai_${targetId}_${Date.now()}`;
    state.tickerContext.forecastAiSummary = {
      requestId: targetId,
      loading: true,
      text: "",
      provider: normalizeModelCouncilProviderId(state.tickerContext.tickerQueryProvider || "openai"),
      model: normalizeAiModelId(state.tickerContext.tickerQueryModel || "gpt-5-mini") || "gpt-5-mini",
      latencyMs: null,
      usage: {},
      responseId: "",
      shareUrl: "",
      feedback: "",
      error: "",
      requestToken,
    };
    if (state.tickerContext.forecastDoc && String(state.tickerContext.forecastDoc.id || "") === targetId) {
      renderForecastDetails(state.tickerContext.forecastDoc);
    }

    const context = buildForecastAutoSummaryContext(doc);
    const provider = normalizeModelCouncilProviderId(state.tickerContext.tickerQueryProvider || "openai");
    const model = normalizeAiModelId(state.tickerContext.tickerQueryModel || "gpt-5-mini") || "gpt-5-mini";
    const startedAt = Date.now();
    try {
      const headers = await buildApiAuthHeaders({ includeJson: true });
      const response = await fetch("/api/llm/run", {
        method: "POST",
        headers,
        credentials: "same-origin",
        body: JSON.stringify({
          provider,
          model,
          fallbackProviders: ["openai", "gemini", "mistral", "perplexity", "other"],
          messages: [
            {
              role: "system",
              content:
                "You are Quantura Model Council. Write a concise analyst narrative with sections: Thesis, Risk frame, Key levels, Next steps. Mention uncertainty clearly.",
            },
            {
              role: "user",
              content: `Forecast context (JSON):\n${JSON.stringify(context)}\n\nWrite a practical summary in under 180 words.`,
            },
          ],
          params: {
            temperature: 0.2,
            maxTokens: 360,
            webSearch: false,
            stream: false,
            background: false,
          },
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(String(payload?.error || payload?.message || "Unable to generate AI forecast summary right now.").trim());
      }
      const nextSummary = {
        requestId: targetId,
        loading: false,
        text: String(payload?.text || "").trim() || "No summary returned.",
        provider: normalizeModelCouncilProviderId(payload?.provider || provider),
        model: normalizeAiModelId(payload?.model || model) || model,
        latencyMs: Number.isFinite(Number(payload?.latencyMs)) ? Number(payload.latencyMs) : Date.now() - startedAt,
        usage: payload?.usage && typeof payload.usage === "object" ? payload.usage : {},
        responseId: String(payload?.responseId || "").trim(),
        shareUrl: "",
        feedback: "",
        error: "",
        requestToken,
      };
      if (String(state.tickerContext.forecastAiSummary?.requestToken || "") !== requestToken) {
        return nextSummary;
      }
      state.tickerContext.forecastAiSummary = nextSummary;
      if (state.tickerContext.forecastDoc && String(state.tickerContext.forecastDoc.id || "") === targetId) {
        renderForecastDetails(state.tickerContext.forecastDoc);
      }
      syncForecastAiSummaryToMyRequest({ requestId: targetId, summary: nextSummary }).catch(() => {});
      return nextSummary;
    } catch (error) {
      const message = String(error?.message || "Unable to generate AI forecast summary right now.").trim();
      if (String(state.tickerContext.forecastAiSummary?.requestToken || "") === requestToken) {
        state.tickerContext.forecastAiSummary = {
          requestId: targetId,
          loading: false,
          text: "",
          provider,
          model,
          latencyMs: null,
          usage: {},
          responseId: "",
          shareUrl: "",
          feedback: "",
          error: message,
          requestToken,
        };
        if (state.tickerContext.forecastDoc && String(state.tickerContext.forecastDoc.id || "") === targetId) {
          renderForecastDetails(state.tickerContext.forecastDoc);
        }
      }
      if (notify) showToast(message, "warn");
      return null;
    }
  };

  const renderForecastAiSummaryMarkup = (forecastDoc) => {
    const forecastId = String(forecastDoc?.id || state.tickerContext.forecastId || "").trim();
    const summary = state.tickerContext.forecastAiSummary && typeof state.tickerContext.forecastAiSummary === "object"
      ? state.tickerContext.forecastAiSummary
      : null;
    const isCurrent = summary && String(summary.requestId || "").trim() === forecastId;
    const model = String(isCurrent ? summary?.model || "" : "").trim();
    const provider = String(isCurrent ? summary?.provider || "" : "").trim();
    const latencyMs = Number(isCurrent ? summary?.latencyMs : NaN);
    const responseId = String(isCurrent ? summary?.responseId || "" : "").trim();
    const shareUrl = String(isCurrent ? summary?.shareUrl || "" : "").trim();
    const feedback = String(isCurrent ? summary?.feedback || "" : "").trim().toLowerCase();
    const loading = Boolean(isCurrent && summary?.loading);
    const error = String(isCurrent ? summary?.error || "" : "").trim();
    const answer = String(isCurrent ? summary?.text || "" : "").trim();
    const disabled = !forecastId;
    const statusLine = loading
      ? "Generating automatic Model Council summary..."
      : error
        ? error
        : answer
          ? `${model || "Model"}${provider ? ` · ${provider}` : ""}${
              Number.isFinite(latencyMs) && latencyMs >= 0 ? ` · ${Math.round(latencyMs)}ms` : ""
            }`
          : "Run a forecast to generate an automatic AI narrative.";
    return `
      <div class="results-panel forecast-ai-summary-panel">
        <h3>Automatic AI summary</h3>
        <div class="small muted">${escapeHtml(statusLine)}</div>
        <div class="panel-output small" style="margin-top:8px;">
          ${
            loading
              ? skeletonHtml(2)
              : error
                ? `<div class="small muted">${escapeHtml(error)}</div>`
                : answer
                  ? `<div>${escapeHtml(answer).replace(/\n/g, "<br>")}</div>`
                  : `<div class="small muted">No AI narrative generated yet.</div>`
          }
        </div>
        <div class="task-chip-row" style="margin-top:10px;">
          <button class="task-chip${feedback === "like" ? " active" : ""}" type="button" data-action="forecast-ai-like" data-forecast-id="${escapeHtml(
            forecastId
          )}" ${disabled ? "disabled" : ""}>Like</button>
          <button class="task-chip${feedback === "dislike" ? " active" : ""}" type="button" data-action="forecast-ai-dislike" data-forecast-id="${escapeHtml(
            forecastId
          )}" ${disabled ? "disabled" : ""}>Dislike</button>
          <button class="task-chip" type="button" data-action="forecast-ai-share" data-forecast-id="${escapeHtml(
            forecastId
          )}" data-response-id="${escapeHtml(responseId)}" ${disabled || (!answer && !shareUrl) ? "disabled" : ""}>${icon("share-ios")}<span>Share link</span></button>
        </div>
        ${
          shareUrl
            ? `<div class="small muted" style="margin-top:8px;">Shared: <a href="${escapeHtml(shareUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(
                shareUrl
              )}</a></div>`
            : ""
        }
        <p class="small muted solve-now-disclaimer">${escapeHtml(MODEL_COUNCIL_OUTPUT_DISCLAIMER)}</p>
      </div>
    `;
  };

  const renderForecastDetails = (forecastDoc) => {
    if (!ui.forecastOutput || !forecastDoc) return;
    const rows = Array.isArray(forecastDoc.forecastRows) ? forecastDoc.forecastRows : [];
    if (!rows.length) {
      setOutputReady(ui.forecastOutput);
      const source = String(forecastDoc.chartSeriesSource || "").trim();
      const message =
        source === "missing"
          ? "No local fan-chart series found for this run on this device. Re-run the forecast to regenerate chart data."
          : source === "preview_only"
            ? "Only preview rows are available on this device. Full fan-chart series was kept client-side on the originating device."
            : "No forecast rows are available for this run.";
      ui.forecastOutput.innerHTML = `
        <div class="small muted">${escapeHtml(message)}</div>
        ${renderForecastAiSummaryMarkup(forecastDoc)}
      `;
      return;
    }

    const quantKeys = extractQuantileKeys(rows);
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
    const metrics = forecastDoc.metrics && typeof forecastDoc.metrics === "object" ? forecastDoc.metrics : {};
    const interval = String(forecastDoc.interval || state.tickerContext.interval || "1d");

    const formatFractionPercent = (value, digits = 1) => {
      const num = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(num)) return String(value ?? "—");
      const pct = num <= 1 ? num * 100 : num;
      return formatPercent(pct, { digits });
    };

    const metricChip = (label, value, iconName) => `
      <div class="metric-chip">
        ${icon(iconName)}
        <span class="metric-label">${escapeHtml(label)}</span>
        <span class="metric-value">${escapeHtml(value)}</span>
      </div>
    `;

    const chips = [];
    const displayedKeys = new Set();

    const horizonValue = metrics.horizon ?? forecastDoc.horizon;
    if (horizonValue) {
      displayedKeys.add("horizon");
      const unit = interval === "1h" ? "hours" : "days";
      chips.push(metricChip("Horizon", `${horizonValue} ${unit}`, "clock"));
    }
    if (metrics.lastClose !== null && metrics.lastClose !== undefined) {
      displayedKeys.add("lastClose");
      chips.push(metricChip("Last close", formatUsd(metrics.lastClose), "candlestick-chart"));
    }
    if (metrics.medianEnd !== null && metrics.medianEnd !== undefined) {
      displayedKeys.add("medianEnd");
      chips.push(metricChip("Median end", formatUsd(metrics.medianEnd), "graph-up"));
    }
    if (metrics.mae !== null && metrics.mae !== undefined) {
      displayedKeys.add("mae");
      chips.push(metricChip("MAE", formatUsd(metrics.mae), "ruler"));
    }
    if (metrics.rmse !== null && metrics.rmse !== undefined) {
      displayedKeys.add("rmse");
      chips.push(metricChip("RMSE", formatUsd(metrics.rmse), "ruler-arrows"));
    }
    if (metrics.mape !== null && metrics.mape !== undefined) {
      displayedKeys.add("mape");
      chips.push(metricChip("MAPE", formatFractionPercent(metrics.mape), "percentage"));
    }
    if (metrics.coverage10_90 !== null && metrics.coverage10_90 !== undefined && metrics.coverage10_90 !== "n/a") {
      displayedKeys.add("coverage10_90");
      chips.push(metricChip("Coverage (10–90)", formatFractionPercent(metrics.coverage10_90), "check-circle"));
    }
    if (metrics.historyPoints !== null && metrics.historyPoints !== undefined) {
      displayedKeys.add("historyPoints");
      chips.push(metricChip("History", formatCompactNumber(metrics.historyPoints), "database-check"));
    }
    if (metrics.drift !== null && metrics.drift !== undefined) {
      displayedKeys.add("drift");
      const num = typeof metrics.drift === "number" ? metrics.drift : Number(metrics.drift);
      const value = Number.isFinite(num) ? formatPercent(num * 100, { signed: true, digits: 2 }) : String(metrics.drift);
      chips.push(metricChip("Drift", value, "graph-up"));
    }
    if (metrics.volatility !== null && metrics.volatility !== undefined) {
      displayedKeys.add("volatility");
      const num = typeof metrics.volatility === "number" ? metrics.volatility : Number(metrics.volatility);
      const value = Number.isFinite(num) ? formatPercent(num * 100, { digits: 2 }) : String(metrics.volatility);
      chips.push(metricChip("Volatility", value, "sine-wave"));
    }

    const metricsStrip = chips.length ? `<div class="metric-strip">${chips.join("")}</div>` : "";

    const metricEntries = Object.entries(metrics || {}).filter(([, value]) => value !== null && value !== undefined && value !== "");
    const extraMetrics = metricEntries.filter(([key]) => !displayedKeys.has(key));
    const metricsTable = metricEntries.length
      ? `
        <details class="learn-more">
          <summary>All model metrics</summary>
          <div class="table-wrap" style="margin-top:10px;">
            <table class="data-table">
              <thead><tr><th>Metric</th><th>Value</th></tr></thead>
              <tbody>
                ${metricEntries
                  .map(([key, value]) => `<tr><td>${escapeHtml(key)}</td><td>${escapeHtml(value)}</td></tr>`)
                  .join("")}
              </tbody>
            </table>
          </div>
          ${extraMetrics.length ? `<div class="small muted" style="margin-top: 8px;">Tip: saved runs may include additional engine-specific diagnostics.</div>` : ""}
        </details>
      `
      : "";

    const summary = [
      `<div class="small meta-line">${icon("hashtag")}<strong>Forecast ID:</strong> ${escapeHtml(forecastDoc.id || "")}</div>`,
      `<div class="small meta-line">${icon("magic-wand")}<strong>Service:</strong> ${escapeHtml(labelForecastService(forecastDoc.service))}</div>`,
      forecastDoc.engine ? `<div class="small meta-line">${icon("electronics-chip")}<strong>Engine:</strong> ${escapeHtml(forecastDoc.engine)}</div>` : "",
      quantileLabel ? `<div class="small meta-line">${icon("percentage")}<strong>Quantiles:</strong> ${escapeHtml(quantileLabel)}</div>` : "",
    ]
      .filter(Boolean)
      .join("");

    const tradeRationale = String(forecastDoc.tradeRationale || "").trim();

    setOutputReady(ui.forecastOutput);
    ui.forecastOutput.innerHTML = `
      <div class="output-stack quantura-horizon-widget">
        ${summary}
        ${metricsStrip}
        ${
          tradeRationale
            ? `<div class="horizon-rationale"><strong>AI Trade Rationale:</strong> ${escapeHtml(tradeRationale)}</div>`
            : ""
        }
        ${metricsTable}
        <div class="table-controls">
          <button class="cta secondary small" type="button" data-action="forecast-page" data-delta="-1" ${
            page === 0 ? "disabled" : ""
          }>${icon("arrow-left")}<span>Prev</span></button>
          <div class="small muted">Rows ${start + 1}-${end} of ${total} · Page ${page + 1}/${totalPages}</div>
          <button class="cta secondary small" type="button" data-action="forecast-page" data-delta="1" ${
            page >= totalPages - 1 ? "disabled" : ""
          }>${icon("arrow-right")}<span>Next</span></button>
          <button class="cta secondary small" type="button" data-action="forecast-csv">${icon("download")}<span>Download CSV</span></button>
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
            Fan-chart series are stored client-side on this device for fast rendering. Firestore stores only request metadata and compact summaries.
          </p>
        </details>
        ${renderForecastAiSummaryMarkup(forecastDoc)}
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
    const doc = { id: snap.id, ...(snap.data() || {}) };
    const fromCache = await loadForecastSeriesFromClientCache({
      requestId: snap.id,
      ticker: doc.ticker,
      interval: doc.interval,
      horizon: doc.horizon,
      service: doc.service,
      quantiles: doc.quantiles,
      start: doc.start,
    });
    const cachedRows = normalizeForecastSeriesRows(fromCache?.forecastRows || []);
    if (cachedRows.length) {
      doc.forecastRows = cachedRows;
      doc.chartSeriesSource = "client_cache";
      doc.chartCacheKey = String(fromCache?.id || "").trim();
      return doc;
    }

    const legacyRows = normalizeForecastSeriesRows(doc.forecastRows || []);
    if (legacyRows.length) {
      doc.forecastRows = legacyRows;
      doc.chartSeriesSource = "firestore_legacy";
      saveForecastSeriesToClientCache({
        requestId: snap.id,
        ticker: doc.ticker,
        interval: doc.interval,
        horizon: doc.horizon,
        service: doc.service,
        quantiles: doc.quantiles,
        start: doc.start,
        forecastRows: legacyRows,
        metrics: doc.metrics,
      }).catch(() => {});
      return doc;
    }

    const previewRows = normalizeForecastSeriesRows(doc.forecastPreview || []);
    doc.forecastRows = previewRows;
    doc.chartSeriesSource = previewRows.length ? "preview_only" : "missing";
    return doc;
  };

  const plotForecastById = async (db, functions, forecastId, { preloadedDoc = null } = {}) => {
    if (!forecastId) return;
    const doc =
      preloadedDoc && typeof preloadedDoc === "object"
        ? { ...preloadedDoc, id: String(preloadedDoc.id || forecastId).trim() }
        : await loadForecastDoc(db, forecastId);
    const ticker = normalizeTicker(doc.ticker);
    const interval = doc.interval || state.tickerContext.interval || "1d";
    state.tickerContext.forecastId = forecastId;
    state.tickerContext.forecastDoc = doc;
    state.tickerContext.forecastTablePage = 0;
    state.tickerContext.forecastCacheMeta = {
      source: String(doc.chartSeriesSource || ""),
      cacheKey: String(doc.chartCacheKey || ""),
      forecastId,
    };
    syncTickerInputs(ticker, { source: "forecast_load" });

    if (!ticker) throw new Error("Forecast ticker is missing.");
    if (!state.tickerContext.rows.length || getActiveTicker() !== ticker || state.tickerContext.interval !== interval) {
      setTerminalStatus("Loading price history...");
      const rows = await loadTickerHistory(functions, ticker, interval);
      state.tickerContext.rows = rows;
      state.tickerContext.interval = interval;
    }

    const forecastOverlays = buildForecastOverlays(doc.forecastRows || []);
    const overlays = [...forecastOverlays, ...(state.tickerContext.indicatorOverlays || [])];
    await renderTickerChart(state.tickerContext.rows, ticker, interval, overlays);
    if (String(doc.chartSeriesSource || "") === "missing") {
      setTerminalStatus("Forecast metadata loaded. Re-run on this device to regenerate fan chart data.");
    } else if (String(doc.chartSeriesSource || "") === "preview_only") {
      setTerminalStatus("Forecast preview loaded. Full fan chart data is stored on the originating device.");
    } else {
      setTerminalStatus(`Plotted forecast ${forecastId}.`);
    }
    renderForecastDetails(doc);
    return doc;
  };

  const mapMyRequestTypeToPanel = (type) => {
    const normalized = normalizeMyRequestType(type);
    if (normalized === "screener") return "screener";
    if (normalized === "indicator") return "indicators";
    if (normalized === "modelCouncil") return "ticker-query";
    return "forecast";
  };

  const loadMyRequestIntoUi = async ({ requestId = "", request = null, db, functions, notify = true } = {}) => {
    if (!hasFullAccount()) {
      throw new Error("Sign in to load saved requests.");
    }
    const id = String(requestId || request?.id || "").trim();
    if (!id) throw new Error("Request ID is required.");
    const item = request && typeof request === "object" ? request : await fetchMyRequestById(id);
    if (!item) throw new Error("Request not found.");

    const type = normalizeMyRequestType(item.type) || "forecast";
    const panelId = mapMyRequestTypeToPanel(type);
    if (typeof window.__quanturaSetPanel === "function") {
      window.__quanturaSetPanel(panelId, { pushPath: false });
    }

    const sourceRef = item.sourceRef && typeof item.sourceRef === "object" ? item.sourceRef : {};
    const sourceId = String(sourceRef.id || "").trim();
    const input = item.input && typeof item.input === "object" ? item.input : {};
    const outputsMeta = item.outputsMeta && typeof item.outputsMeta === "object" ? item.outputsMeta : {};
    const ticker = normalizeTicker(input.ticker || item.ticker || "");
    if (ticker) syncTickerInputs(ticker, { source: "my_request_load" });

    if (type === "forecast") {
      const forecastId = sourceId || String(id.split("__").slice(1).join("__") || "").trim();
      if (!forecastId) throw new Error("Forecast source is missing.");
      await plotForecastById(db, functions, forecastId);
      if (notify) showToast("Forecast request loaded.");
      return item;
    }

    if (type === "screener") {
      const runId = sourceId || String(id.split("__").slice(1).join("__") || "").trim();
      if (!runId) throw new Error("Screener source is missing.");
      await loadScreenerRunById(db, runId);
      if (notify) showToast("Screener request loaded.");
      return item;
    }

    if (type === "indicator") {
      const tickerInput = document.getElementById("technicals-ticker");
      const intervalInput = document.getElementById("technicals-interval");
      const lookbackInput = document.getElementById("technicals-lookback");
      if (tickerInput && ticker) tickerInput.value = ticker;
      if (intervalInput && input.interval) intervalInput.value = String(input.interval);
      if (lookbackInput && Number(input.lookback)) lookbackInput.value = String(Math.max(1, Number(input.lookback)));
      const indicators = Array.isArray(input.indicators) ? input.indicators.map((entry) => String(entry || "").trim().toUpperCase()) : [];
      if (indicators.length) {
        document.querySelectorAll('#technicals-form input[name="indicators"]').forEach((checkbox) => {
          const value = String(checkbox.value || "").trim().toUpperCase();
          checkbox.checked = indicators.includes(value);
        });
      }
      if (ui.technicalsOutput) {
        setOutputReady(ui.technicalsOutput);
        const summary = String(outputsMeta.summary || "").trim();
        ui.technicalsOutput.innerHTML = `<div class="small muted">${
          summary ? escapeHtml(summary) : "Indicator inputs restored. Run indicators to refresh values."
        }</div>`;
      }
      if (notify) showToast("Indicator request loaded.");
      return item;
    }

    if (type === "modelCouncil") {
      const provider = String(input.provider || outputsMeta.provider || "").trim().toLowerCase();
      const model = normalizeAiModelId(input.model || outputsMeta.model || "");
      if (ui.tickerQueryTicker && ticker) ui.tickerQueryTicker.value = ticker;
      if (ui.tickerQueryQuestion) ui.tickerQueryQuestion.value = String(input.question || "");
      if (ui.tickerQueryLanguage && input.language) ui.tickerQueryLanguage.value = String(input.language);
      if (provider) {
        state.tickerContext.tickerQueryProvider = provider;
        if (ui.tickerQueryProvider) ui.tickerQueryProvider.value = provider;
      }
      if (model) {
        state.tickerContext.tickerQueryModel = model;
        if (ui.tickerQueryModel) ui.tickerQueryModel.value = model;
      }
      if (Array.isArray(input.modules) && input.modules.length) {
        setTickerQueryModulesSelection(input.modules, { persist: true });
      }
      applyTickerQueryModelSelection(model || state.tickerContext.tickerQueryModel || "gpt-5-mini", provider || state.tickerContext.tickerQueryProvider || "openai");
      const answer = String(outputsMeta.answer || outputsMeta.summary || "").trim();
      if (answer) {
        const responseId = sourceId || String(item.id || "").trim();
        state.tickerContext.tickerQueryLastResponseId = responseId;
        const responsePayload = {
          answer,
          model: model || state.tickerContext.tickerQueryModel || "",
          provider: provider || state.tickerContext.tickerQueryProvider || "",
          usage: {},
          context: {},
          moduleData: {},
          selectedModules: Array.isArray(input.modules) ? input.modules : [],
          responseId,
          citations: [],
        };
        state.tickerContext.tickerQueryLastResponse = responsePayload;
        renderTickerQueryResult(responsePayload);
      }
      if (notify) showToast("Model Council request loaded.");
      return item;
    }

    return item;
  };

  const loadSharedMyRequestFromUrl = async ({ setPanel = true } = {}) => {
    const shareSlug = String(getQueryParam("requestShare") || "").trim();
    if (!shareSlug) return false;
    try {
      const headers = await buildApiAuthHeaders();
      const response = await fetch(`/api/my-requests/shared/${encodeURIComponent(shareSlug)}`, {
        method: "GET",
        headers,
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(String(payload?.error || "Shared request unavailable.").trim());
      }
      const request = payload?.request && typeof payload.request === "object" ? payload.request : null;
      if (!request) throw new Error("Shared request unavailable.");
      const type = normalizeMyRequestType(request.type) || "forecast";
      const panelId = mapMyRequestTypeToPanel(type);
      if (setPanel && typeof window.__quanturaSetPanel === "function") {
        window.__quanturaSetPanel(panelId, { pushPath: false });
      }
      if (type === "modelCouncil" && ui.tickerQueryOutput) {
        const outputsMeta = request.outputsMeta && typeof request.outputsMeta === "object" ? request.outputsMeta : {};
        const answer = String(outputsMeta.answer || outputsMeta.summary || "").trim();
        renderTickerQueryResult({
          answer,
          model: String(outputsMeta.model || request.input?.model || ""),
          provider: String(outputsMeta.provider || request.input?.provider || ""),
          context: {},
          moduleData: {},
          selectedModules: Array.isArray(request.input?.modules) ? request.input.modules : [],
          responseId: String(request.sourceRef?.id || request.id || ""),
          citations: [],
          shareUrl: String(payload?.share?.shareUrl || ""),
        });
      } else if (type === "forecast" && ui.forecastOutput) {
        setOutputReady(ui.forecastOutput);
        const summary = String(request.outputsMeta?.summary || "Shared forecast request loaded.");
        ui.forecastOutput.innerHTML = `<div class="small muted">${escapeHtml(summary)}</div>`;
      } else if (type === "screener" && ui.screenerOutput) {
        setOutputReady(ui.screenerOutput);
        const summary = String(request.outputsMeta?.summary || "Shared screener request loaded.");
        ui.screenerOutput.innerHTML = `<div class="small muted">${escapeHtml(summary)}</div>`;
      } else if (type === "indicator" && ui.technicalsOutput) {
        setOutputReady(ui.technicalsOutput);
        const summary = String(request.outputsMeta?.summary || "Shared indicator request loaded.");
        ui.technicalsOutput.innerHTML = `<div class="small muted">${escapeHtml(summary)}</div>`;
      }
      showToast("Viewing shared request.");
      return true;
    } catch (error) {
      showToast(error.message || "Unable to load shared request.", "warn");
      return false;
    }
  };

  const ensureMessagingServiceWorker = async () => {
    if (!("serviceWorker" in navigator)) {
      throw new Error("Service workers are not available in this browser.");
    }
    return navigator.serviceWorker.register("/firebase-messaging-sw.js");
  };

  const loadVapidKey = async (functions) => {
    if (state.remoteFlags?.webPushVapidKey) return String(state.remoteFlags.webPushVapidKey || "").trim();
    if (window.QUANTURA_VAPID_KEY) return String(window.QUANTURA_VAPID_KEY || "").trim();
    try {
      const response = await fetch("/api/notifications/config", {
        method: "GET",
        credentials: "same-origin",
      });
      if (response.ok) {
        const payload = await response.json().catch(() => ({}));
        const vapidPublicKey = String(payload?.vapidPublicKey || "").trim();
        if (vapidPublicKey) return vapidPublicKey;
      }
    } catch (error) {
      // Fall through to legacy callable fallback if available.
    }
    if (functions?.httpsCallable) {
      const getWebPushConfig = functions.httpsCallable("get_web_push_config");
      const response = await getWebPushConfig({ meta: buildMeta() });
      return String(response.data?.vapidKey || "").trim();
    }
    return "";
  };

  const syncNotificationToken = async (functions, token, opts = {}) => {
    const cleanToken = String(token || "").trim();
    if (cleanToken.length < 20) {
      throw new Error("Valid notification token is required.");
    }
    const headers = await buildApiAuthHeaders({ includeJson: true });
    if (!headers.Authorization) {
      throw new Error("Sign in before enabling notifications.");
    }
    const response = await fetch("/api/notifications/register-token", {
      method: "POST",
      headers,
      credentials: "same-origin",
      body: JSON.stringify({
        token: cleanToken,
        platform: isNativeApp() ? getNativePlatform() || "native" : "web",
        source: String(opts.source || "messaging"),
        notificationPrivacy: {
          locationConsent: Boolean(state.notificationPrivacy?.locationConsent),
          ipRegionConsent: Boolean(state.notificationPrivacy?.ipRegionConsent),
          coarseLocation: state.notificationPrivacy?.coarseLocation || null,
          ipRegion: String(state.notificationPrivacy?.ipRegion || "").trim(),
          timezone: String(state.notificationPrivacy?.timezone || "").trim(),
        },
      }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(String(payload?.error || "Unable to register notification token."));
    }
    localStorage.setItem(FCM_TOKEN_CACHE_KEY, cleanToken);
    return cleanToken;
  };

  const pingNotificationSession = async () => {
    if (!hasSessionUser()) return false;
    const headers = await buildApiAuthHeaders({ includeJson: true });
    if (!headers.Authorization) return false;
    try {
      const response = await fetch("/api/notifications/session/ping", {
        method: "POST",
        headers,
        credentials: "same-origin",
        body: JSON.stringify({
          isAnonymous: isAnonymousUser(),
        }),
      });
      return Boolean(response.ok);
    } catch (error) {
      return false;
    }
  };

  const registerNotificationToken = async (functions, messaging, opts = {}) => {
    if (!hasSessionUser()) throw new Error("Sign in before enabling notifications.");
    await pingNotificationSession().catch(() => undefined);
    if (isNativeApp()) {
      const nativeToken = String(window.__NATIVE_FCM_TOKEN__ || "").trim();
      if (!nativeToken) throw new Error("Native push token is not available yet.");
      return syncNotificationToken(functions, nativeToken, {
        forceRefresh: Boolean(opts.forceRefresh),
        source: "native",
      });
    }
    if (!messaging) throw new Error("Messaging SDK is not available.");
    if (!isPushSupported()) throw new Error("Push notifications are not supported in this browser.");

    const permission = await Notification.requestPermission();
    if (permission !== "granted") {
      throw new Error("Notification permission was not granted.");
    }

    const cachedToken = String(safeLocalStorageGet(FCM_TOKEN_CACHE_KEY) || "").trim();
    if (cachedToken && !opts.forceRefresh) {
      try {
        return await syncNotificationToken(functions, cachedToken, { forceRefresh: false, source: "cache" });
      } catch (error) {
        // Fall through to minting a new token.
      }
    }

    let vapidKey = "";
    try {
      vapidKey = await loadVapidKey(functions);
    } catch (error) {
      if (cachedToken) {
        return syncNotificationToken(functions, cachedToken, {
          forceRefresh: false,
          source: opts.forceRefresh ? "cache_refresh" : "cache",
        });
      }
      throw error;
    }
    if (!vapidKey) {
      if (cachedToken) {
        return syncNotificationToken(functions, cachedToken, {
          forceRefresh: false,
          source: opts.forceRefresh ? "cache_refresh" : "cache",
        });
      }
      throw new Error("Web push key is missing. Configure FCM_WEB_VAPID_KEY on the server.");
    }

    const serviceWorkerRegistration = await ensureMessagingServiceWorker();
    const token = await messaging.getToken({
      vapidKey,
      serviceWorkerRegistration,
    });

    if (!token) {
      if (cachedToken) {
        return syncNotificationToken(functions, cachedToken, {
          forceRefresh: false,
          source: opts.forceRefresh ? "cache_refresh" : "cache",
        });
      }
      throw new Error("No registration token generated.");
    }

    return syncNotificationToken(functions, token, {
      forceRefresh: Boolean(opts.forceRefresh),
      source: opts.forceRefresh ? "messaging_refresh" : "messaging",
    });
  };

  const unregisterCachedNotificationToken = async (functions) => {
    const token = localStorage.getItem(FCM_TOKEN_CACHE_KEY);
    if (!token) return;
    try {
      const headers = await buildApiAuthHeaders({ includeJson: true });
      if (headers.Authorization) {
        await fetch("/api/notifications/unregister-token", {
          method: "POST",
          headers,
          credentials: "same-origin",
          body: JSON.stringify({ token }),
        });
      } else if (functions?.httpsCallable) {
        const unregisterToken = functions.httpsCallable("unregister_notification_token");
        await unregisterToken({ token, meta: buildMeta() });
      }
    } catch (error) {
      // Ignore token cleanup errors.
    }
    localStorage.removeItem(FCM_TOKEN_CACHE_KEY);
  };

  const bindForegroundPushHandler = (messaging) => {
    if (!messaging || state.messagingBound) return;
    try {
      messaging.onMessage(async (payload) => {
        const title = payload?.notification?.title || "Quantura update";
        const body = payload?.notification?.body || "You have a new dashboard update.";
        showToast(`${title}: ${body}`);
        setNotificationStatus(`Last message: ${title}`);
        await appendNotificationLogPersonalized({
          title,
          body,
          source: "foreground",
          at: new Date().toISOString(),
        });
        logEvent("push_received_foreground", { title });
      });
      if (typeof navigator !== "undefined" && navigator.serviceWorker) {
        navigator.serviceWorker.addEventListener("message", (event) => {
          const data = event?.data || {};
          if (String(data?.type || "").trim() !== "quantura_push_background") return;
          const title = String(data?.title || "Quantura update");
          const body = String(data?.body || "");
          appendNotificationLogPersonalized({
            title,
            body,
            source: "background",
            at: new Date().toISOString(),
          });
          setNotificationStatus(`Last message: ${title}`);
        });
      }
      state.messagingBound = true;
    } catch (error) {
      // Ignore foreground messaging bind errors.
    }
  };

  const isNativeIosStoreKitCheckoutOnly = () =>
    isNativeApp() && getNativePlatform() === "ios";

  const isNativeAndroidPlayBillingCheckout = () =>
    isNativeApp() &&
    getNativePlatform() === "android" &&
    Boolean(state.remoteFlags?.nativeAndroidPlayBillingEnabled ?? true);

  const nativeBillingPortalLabel = () =>
    getNativePlatform() === "ios" ? "Restore purchases" : "Manage subscriptions";

  const resolveNativeIapPlanKey = (panel) => {
    const explicit = String(panel?.dataset?.iapPlan || "").trim().toLowerCase();
    if (explicit) return explicit;
    const product = String(panel?.dataset?.product || "").trim().toLowerCase();
    if (product.includes("annual") && product.includes("business")) return "annual_business";
    if (product.includes("annual") && product.includes("plus")) return "annual_plus";
    if (product.includes("annual") && product.includes("go")) return "annual_go";
    if (product.includes("desk")) return "desk";
    if (product.includes("business")) return "business";
    if (product.includes("plus") || product.includes("premium")) return "plus";
    if (product.includes("go")) return "go";
    if (product.includes("forecast")) return "forecast";
    if (product.includes("pro")) return "pro";
    return "default";
  };

  const nativeIapMapForPlatform = (platform, configuredRaw) => {
    const targetPlatform = platform === "ios" ? "ios" : "android";
    const fallback =
      DEFAULT_NATIVE_IAP_PRODUCT_IDS[targetPlatform] || DEFAULT_NATIVE_IAP_PRODUCT_IDS.android;
    const configured =
      configuredRaw && typeof configuredRaw === "object" ? configuredRaw : {};
    const scoped =
      configured[targetPlatform] && typeof configured[targetPlatform] === "object"
        ? configured[targetPlatform]
        : configured;
    return { ...fallback, ...scoped };
  };

  const resolveNativeIapProductId = (panel) => {
    const fromPanel = String(panel?.dataset?.iapProductId || "").trim();
    if (fromPanel) return fromPanel;

    const platform = getNativePlatform() === "ios" ? "ios" : "android";
    const map = nativeIapMapForPlatform(platform, state.remoteFlags?.nativeIapProductIds);
    const plan = resolveNativeIapPlanKey(panel);
    const productId = String(
      map[plan] ||
        map.default ||
        DEFAULT_NATIVE_IAP_PRODUCT_IDS[platform]?.default ||
        DEFAULT_NATIVE_IAP_PRODUCT_IDS.android.default
    ).trim();
    return productId;
  };

  const formatUsdPriceLabel = (amount, cycle) => {
    const numeric = Number(amount);
    if (!Number.isFinite(numeric) || numeric <= 0) return cycle === "yearly" ? "$0/yr" : "$0/mo";
    const decimals = numeric % 1 === 0 ? 0 : 2;
    return cycle === "yearly" ? `$${numeric.toFixed(decimals)}/yr` : `$${numeric.toFixed(decimals)}/mo`;
  };

  const applyPricingBillingCycle = (cycle = "monthly") => {
    const normalizedCycle = cycle === "yearly" ? "yearly" : "monthly";
    document.querySelectorAll('[data-billing-cycle]').forEach((btn) => {
      const active = String(btn?.dataset?.billingCycle || "") === normalizedCycle;
      btn.setAttribute("aria-pressed", active ? "true" : "false");
      btn.classList.toggle("secondary", !active);
    });

    ui.purchasePanels.forEach((panel) => {
      if (!panel?.dataset?.monthlyPrice) return;
      const monthlyPrice = Number(panel.dataset.monthlyPrice || panel.dataset.price || 0);
      const yearlyPrice = Number(panel.dataset.yearlyPrice || monthlyPrice * 12 || 0);
      const nextPrice = normalizedCycle === "yearly" ? yearlyPrice : monthlyPrice;
      const nextProduct =
        normalizedCycle === "yearly"
          ? String(panel.dataset.yearlyProduct || panel.dataset.product || "")
          : String(panel.dataset.monthlyProduct || panel.dataset.product || "");
      const nextIapPlan =
        normalizedCycle === "yearly"
          ? String(panel.dataset.yearlyIapPlan || panel.dataset.iapPlan || "")
          : String(panel.dataset.monthlyIapPlan || panel.dataset.iapPlan || "");

      panel.dataset.price = String(nextPrice || 0);
      panel.dataset.product = nextProduct;
      panel.dataset.iapPlan = nextIapPlan;

      const card = panel.closest("[data-pricing-plan-card]");
      const priceNode = card?.querySelector("[data-pricing-price]");
      if (priceNode) {
        priceNode.textContent = formatUsdPriceLabel(nextPrice, normalizedCycle);
      }
      const copyNode = card?.querySelector("[data-pricing-cycle-copy]");
      if (copyNode) {
        const annualList = monthlyPrice * 12;
        const savings = Math.max(0, annualList - yearlyPrice);
        if (normalizedCycle === "yearly") {
          if (savings > 0) {
            copyNode.innerHTML = `You save <strong>$${savings.toFixed(0)}/yr</strong> vs monthly billing.`;
          } else {
            copyNode.innerHTML = "Annual display shown. Native yearly SKU may map to monthly where unavailable.";
          }
        } else {
          if (savings > 0) {
            copyNode.innerHTML = `Pay yearly to lock in <strong>$${yearlyPrice.toFixed(0)}/yr</strong>.`;
          } else {
            copyNode.innerHTML = "Monthly billing shown.";
          }
        }
      }
    });
  };

  const initPricingBillingToggle = () => {
    const wrap = document.getElementById("pricing-billing-toggle");
    if (!wrap) return;
    const buttons = Array.from(wrap.querySelectorAll("[data-billing-cycle]"));
    if (!buttons.length) return;

    const initialCycle = String(safeLocalStorageGet("quantura_pricing_cycle") || "monthly").trim().toLowerCase();
    applyPricingBillingCycle(initialCycle === "yearly" ? "yearly" : "monthly");

    buttons.forEach((button) => {
      if (!(button instanceof HTMLElement)) return;
      if (button.dataset.bound === "1") return;
      button.dataset.bound = "1";
      button.addEventListener("click", () => {
        const cycle = String(button.dataset.billingCycle || "monthly").trim().toLowerCase();
        const normalized = cycle === "yearly" ? "yearly" : "monthly";
        safeLocalStorageSet("quantura_pricing_cycle", normalized);
        applyPricingBillingCycle(normalized);
        logEvent("pricing_cycle_toggled", { cycle: normalized, page_path: window.location.pathname });
      });
    });
  };

  const requestNativeInAppPurchase = (panel, opts = {}) => {
    const requestId = `np_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
    const orderId = String(opts.orderId || panel?.dataset?.orderId || "").trim();
    const product = String(panel?.dataset?.product || "Quantura Pro").trim();
    const productId = resolveNativeIapProductId(panel);
    if (!productId) return false;

    const payload = {
      action: "startNativePurchase",
      requestId,
      orderId,
      source: String(opts.source || "pricing"),
      product,
      productId,
      price: Number(panel?.dataset?.price || 0) || 0,
      currency: String(panel?.dataset?.currency || "USD"),
    };
    const sent = sendNativeBridgeMessage(payload);
    if (sent) {
      logEvent("native_purchase_requested", {
        platform: getNativePlatform() || "unknown",
        product_id: productId,
        order_id: orderId,
        source: payload.source,
      });
    }
    return sent;
  };

  const requestNativeSubscriptionManager = (source = "billing_portal") => {
    const requestId = `nsm_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
    const sent = sendNativeBridgeMessage({
      action: "openNativeSubscriptionManager",
      requestId,
      source: String(source || "billing_portal"),
    });
    if (sent) {
      logEvent("native_subscription_manager_requested", {
        platform: getNativePlatform() || "unknown",
        source: String(source || "billing_portal"),
      });
    }
    return sent;
  };

  const isNativeIapRuntime = () => isNativeIosStoreKitCheckoutOnly() || isNativeAndroidPlayBillingCheckout();

  const readPendingNativeIapEvents = () => {
    try {
      const raw = String(safeLocalStorageGet(NATIVE_IAP_PENDING_EVENTS_KEY) || "").trim();
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  };

  const writePendingNativeIapEvents = (events) => {
    try {
      const list = Array.isArray(events) ? events.slice(0, 80) : [];
      safeLocalStorageSet(NATIVE_IAP_PENDING_EVENTS_KEY, JSON.stringify(list));
    } catch (error) {
      // Ignore storage write issues.
    }
  };

  const queuePendingNativeIapEvent = (payload) => {
    const event = payload && typeof payload === "object" ? payload : {};
    const next = {
      eventId: String(event.eventId || `iap_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 9)}`).trim(),
      productId: String(event.productId || "").trim(),
      orderId: String(event.orderId || "").trim(),
      status: String(event.status || "purchased").trim().toLowerCase(),
      platform: String(event.platform || getNativePlatform() || "").trim().toLowerCase(),
      source: String(event.source || "native_iap").trim().toLowerCase(),
      sourceUid: String(event.sourceUid || "").trim(),
      purchasedAtMs: Number(event.purchasedAtMs || Date.now()) || Date.now(),
      queuedAtMs: Date.now(),
    };
    if (!next.productId) return;
    const existing = readPendingNativeIapEvents();
    existing.unshift(next);
    writePendingNativeIapEvents(existing);
  };

  const flushPendingNativeIapEvents = async () => {
    if (!hasSessionUser()) return;
    const queued = readPendingNativeIapEvents();
    if (!queued.length) return;
    const headers = await buildApiAuthHeaders({ includeJson: true });
    if (!headers.Authorization) return;
    const remaining = [];
    for (const event of queued.slice(0, 40)) {
      try {
        const response = await fetch("/api/notifications/iap-event", {
          method: "POST",
          headers,
          credentials: "same-origin",
          body: JSON.stringify(event),
        });
        if (!response.ok) {
          remaining.push(event);
        }
      } catch (error) {
        remaining.push(event);
      }
    }
    writePendingNativeIapEvents(remaining);
  };

  const mergeAnonymousSessionData = async (sourceUid, targetUid) => {
    const fromUid = String(sourceUid || "").trim();
    const toUid = String(targetUid || "").trim();
    if (!fromUid || !toUid || fromUid === toUid) return;
    const headers = await buildApiAuthHeaders({ includeJson: true });
    if (!headers.Authorization) return;
    try {
      await fetch("/api/notifications/merge-anon-data", {
        method: "POST",
        headers,
        credentials: "same-origin",
        body: JSON.stringify({ sourceUid: fromUid }),
      });
    } catch (error) {
      // Merge is best-effort and retried implicitly on future sign-ins.
    }
  };

  const confirmNativePurchaseOnBackend = async ({ orderId, productId, status }) => {
    if (!orderId || !hasSessionUser()) {
      if (orderId) {
        queuePendingNativeIapEvent({
          orderId,
          productId,
          status,
          source: "native_iap_order_pending",
          sourceUid: String(state.user?.uid || "").trim(),
        });
      }
      return;
    }
    const functions = state.clients?.functions;
    if (!functions) {
      queuePendingNativeIapEvent({
        orderId,
        productId,
        status,
        source: "native_iap_order_no_functions",
        sourceUid: String(state.user?.uid || "").trim(),
      });
      return;
    }
    try {
      const confirm = functions.httpsCallable("confirm_native_iap_purchase");
      await confirm({
        orderId,
        productId: String(productId || "").trim(),
        status: String(status || "").trim().toLowerCase(),
        platform: String(getNativePlatform() || "").trim().toLowerCase(),
        tier: subscriptionTierFromOrder({ productId }),
        meta: buildMeta(),
      });
    } catch (error) {
      queuePendingNativeIapEvent({
        orderId,
        productId,
        status,
        source: "native_iap_order_retry",
        sourceUid: String(state.user?.uid || "").trim(),
      });
    }
  };

  const applyNativePurchaseResult = (detail) => {
    const payload = detail && typeof detail === "object" ? detail : {};
    const orderId = String(payload.orderId || "").trim();
    const status = String(payload.status || "").trim().toLowerCase();
    const ok = Boolean(payload.ok);
    const message = String(payload.message || "").trim();
    const productId = String(payload.productId || "").trim();
    const panel = orderId
      ? ui.purchasePanels.find((entry) => String(entry?.dataset?.orderId || "").trim() === orderId)
      : null;
    const note = panel?.querySelector(".purchase-note");
    const success = panel?.querySelector(".purchase-success");
    const stripe = panel?.querySelector('[data-action="stripe"]');

    logEvent("native_purchase_result", {
      platform: getNativePlatform() || "unknown",
      order_id: orderId,
      product_id: productId,
      status: status || (ok ? "success" : "failed"),
    });

    if (status === "restored") {
      if (note) note.textContent = message || "Purchases restored.";
      stripe?.classList.add("hidden");
      showToast(message || "Purchases restored.");
      return;
    }

    if (status === "subscriptions_opened") {
      const platform = getNativePlatform();
      const openedMessage =
        platform === "ios"
          ? "Opened App Store subscriptions."
          : "Opened Google Play subscriptions.";
      if (note) note.textContent = message || openedMessage;
      showToast(message || openedMessage);
      return;
    }

    if (status === "purchased" || (ok && (!status || status === "success"))) {
      if (success) {
        success.textContent = orderId
          ? `In-app purchase completed for order ${orderId}.`
          : "In-app purchase completed.";
        success.classList.remove("hidden");
      }
      if (note) note.textContent = "Purchase completed in native checkout.";
      stripe?.classList.add("hidden");
      showToast("In-app purchase completed.");
      if (orderId) {
        confirmNativePurchaseOnBackend({ orderId, productId, status: "purchased" });
      } else {
        queuePendingNativeIapEvent({
          productId,
          status: "purchased",
          platform: getNativePlatform() || "unknown",
          source: "native_iap_no_order",
          sourceUid: String(state.user?.uid || "").trim(),
        });
        flushPendingNativeIapEvents().catch(() => undefined);
      }
      if (orderId) {
        logEvent("purchase", {
          transaction_id: orderId,
          currency: String(panel?.dataset?.currency || "USD"),
          value: Number(panel?.dataset?.price || 0) || 0,
          source: "native_iap",
        });
      }
      return;
    }

    if (status === "cancelled") {
      if (note) note.textContent = "In-app purchase was cancelled.";
      showToast("Purchase cancelled.", "warn");
      return;
    }

    if (status === "pending") {
      if (note) note.textContent = "Purchase is pending approval.";
      showToast("Purchase pending approval.");
      return;
    }

    if (message) {
      if (note) note.textContent = message;
      showToast(message, "warn");
    } else {
      if (note) note.textContent = "Unable to complete native checkout.";
      showToast("Unable to complete native checkout.", "warn");
    }
  };

  const bindNativePurchaseResultBridge = () => {
    if (typeof window === "undefined") return;
    if (window.__QUANTURA_NATIVE_PURCHASE_BOUND__ === true) return;
    window.__QUANTURA_NATIVE_PURCHASE_BOUND__ = true;
    window.addEventListener("quantura:native-purchase-result", (event) => {
      applyNativePurchaseResult(event?.detail || {});
    });
  };

  const handlePurchase = async (panel, functions) => {
    const nativeBillingProvider = isNativeIapRuntime();
    if (nativeBillingProvider) {
      try {
        await ensureSessionUser({
          reason: "native_iap_checkout",
          message: "Initializing guest checkout session...",
        });
      } catch (error) {
        showToast(error?.message || "Unable to initialize guest checkout session.", "warn");
        return;
      }
    } else if (!requireFullAccount("Sign in to continue.", { redirect: true })) {
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
      const productId = resolveNativeIapProductId(panel);
      const subscriptionTier = resolveNativeIapPlanKey(panel);
      logEvent("begin_checkout", { currency: panel.dataset.currency || "USD", value: Number(panel.dataset.price || 349) });
      const createOrder = functions.httpsCallable("create_order");
      const result = await createOrder({
        product: panel.dataset.product || "Quantura Subscription",
        productId,
        tier: subscriptionTier,
        paymentProvider: nativeBillingProvider ? "native_iap" : "stripe",
        price: Number(panel.dataset.price || 349),
        currency: panel.dataset.currency || "USD",
        meta: {
          ...meta,
          subscriptionTier,
          productId,
          purchaseRuntime: nativeBillingProvider ? (getNativePlatform() || "native") : "web",
        },
      });
      const orderId = result.data?.orderId;
      if (orderId) {
        panel.dataset.orderId = String(orderId);
      }
      if (success) {
        success.textContent = `Order ${orderId} created. Proceed to payment to finalize.`;
        success.classList.remove("hidden");
      }
      const nativePlatform = getNativePlatform();
      if (isNativeIosStoreKitCheckoutOnly() || isNativeAndroidPlayBillingCheckout()) {
        const sent = requestNativeInAppPurchase(panel, { orderId, source: "order_created" });
        if (sent) {
          stripe?.classList.add("hidden");
          note.textContent =
            nativePlatform === "ios"
              ? "Order created. Opening App Store in-app purchase..."
              : "Order created. Opening Google Play in-app purchase...";
          showToast(
            nativePlatform === "ios"
              ? "Opening native iOS checkout..."
              : "Opening native Android checkout..."
          );
        } else {
          stripe?.classList.add("hidden");
          note.textContent =
            nativePlatform === "ios"
              ? "Native iOS checkout is required. Please reopen this page in the app."
              : "Native Android checkout is unavailable right now.";
          showToast(note.textContent, "warn");
        }
      } else {
        stripe?.classList.remove("hidden");
        note.textContent = "Order created. Proceed to payment to finalize.";
      }
      logEvent("order_created", { order_id: orderId, currency: panel.dataset.currency || "USD" });
      if (!isNativeIosStoreKitCheckoutOnly() && !isNativeAndroidPlayBillingCheckout()) {
        showToast("Order created. Proceed to payment.");
      }
    } catch (error) {
      if (nativeBillingProvider) {
        const sent = requestNativeInAppPurchase(panel, { orderId: "", source: "native_no_order_fallback" });
        if (sent) {
          if (stripe) stripe.classList.add("hidden");
          if (note) {
            note.textContent =
              "Native checkout opened without a server order. We will sync purchase details after sign-in.";
          }
          queuePendingNativeIapEvent({
            productId: resolveNativeIapProductId(panel),
            status: "pending",
            platform: getNativePlatform() || "unknown",
            source: "native_iap_no_order_fallback",
            sourceUid: String(state.user?.uid || "").trim(),
          });
          showToast("Opening native checkout in guest mode...");
          return;
        }
      }
      showToast(error.message || "Unable to create order.", "warn");
    } finally {
      button.disabled = false;
      button.textContent = button.dataset.labelAuth || "Choose plan";
    }
  };

  const loadStripeJs = async () => {
    if (typeof window === "undefined") return null;
    if (window.Stripe) return window.Stripe;
    await new Promise((resolve, reject) => {
      const existing = document.querySelector('script[src="https://js.stripe.com/v3/"]');
      if (existing) {
        existing.addEventListener("load", resolve, { once: true });
        existing.addEventListener("error", reject, { once: true });
        return;
      }
      const script = document.createElement("script");
      script.src = "https://js.stripe.com/v3/";
      script.async = true;
      script.onload = resolve;
      script.onerror = reject;
      document.head.appendChild(script);
    });
    return window.Stripe || null;
  };

  const handleStripeCheckout = async (panel, functions) => {
    if (isNativeIapRuntime()) {
      if (!hasSessionUser()) {
        try {
          await ensureSessionUser({
            reason: "native_iap_checkout",
            message: "Initializing guest checkout session...",
          });
        } catch (error) {
          showToast(error?.message || "Unable to initialize guest checkout session.", "warn");
          return;
        }
      }
      const orderId = String(panel?.dataset?.orderId || "").trim();
      const sent = requestNativeInAppPurchase(panel, { orderId, source: "stripe_button" });
      if (!sent) {
        showToast("Native in-app checkout is required in the mobile app.", "warn");
      }
      return;
    }
    if (!requireFullAccount("Sign in to continue.", { redirect: true })) return;

    if (!state.remoteFlags.stripeCheckoutEnabled) {
      showToast("Checkout is temporarily disabled.", "warn");
      return;
    }

    const stripeBtn = panel.querySelector('[data-action="stripe"]');
    const note = panel.querySelector(".purchase-note");
    const orderId = String(panel.dataset.orderId || "").trim();
    if (!orderId) {
      showToast("Create an order first.", "warn");
      return;
    }

    if (stripeBtn) {
      stripeBtn.disabled = true;
      stripeBtn.textContent = "Redirecting to payment...";
    }
    if (note) note.textContent = "Starting secure checkout...";

    try {
      logEvent("add_payment_info", { currency: panel.dataset.currency || "USD", value: Number(panel.dataset.price || 349) });
      const createSession = functions.httpsCallable("create_stripe_checkout_session");
      const result = await createSession({ orderId, meta: buildMeta() });
      const sessionId = String(result.data?.sessionId || "");
      const url = String(result.data?.url || "");

      logEvent("checkout_redirect", { order_id: orderId, mode: result.data?.mode || "" });
      if (url) {
        window.location.assign(url);
        return;
      }

      const stripeKey = String(state.remoteFlags.stripePublicKey || "").trim();
      if (stripeKey && sessionId) {
        const StripeCtor = await loadStripeJs();
        if (StripeCtor) {
          const stripe = StripeCtor(stripeKey);
          await stripe.redirectToCheckout({ sessionId });
          return;
        }
      }

      throw new Error("Checkout URL is not available.");
    } catch (error) {
      showToast(error.message || "Unable to start checkout.", "warn");
      if (note) note.textContent = "Unable to start checkout. Try again.";
    } finally {
      if (stripeBtn) {
        stripeBtn.disabled = false;
        stripeBtn.textContent = "Proceed to payment";
      }
    }
  };

  const handleBillingPortalOpen = async (event, functions) => {
    if (!ui.billingPortalLink) return;

    if (isNativeIapRuntime()) {
      event?.preventDefault?.();
      if (!hasSessionUser()) {
        try {
          await ensureSessionUser({
            reason: "native_subscription_manager",
            message: "Initializing guest session...",
          });
        } catch (error) {
          showToast(error?.message || "Unable to initialize session.", "warn");
          return;
        }
      }
      const sent = requestNativeSubscriptionManager("billing_portal");
      if (sent) {
        showToast(
          getNativePlatform() === "ios"
            ? "Restoring App Store purchases..."
            : "Restoring Google Play purchases..."
        );
      } else {
        showToast("Subscription management is only available in native app settings.", "warn");
      }
      return;
    }
    if (!hasFullAccount()) return;

    event?.preventDefault?.();
    if (ui.billingPortalLink.dataset.loading === "1") return;
    ui.billingPortalLink.dataset.loading = "1";

    const originalText = ui.billingPortalLink.textContent || "Open billing portal";
    ui.billingPortalLink.textContent = "Opening billing portal...";
    ui.billingPortalLink.setAttribute("aria-disabled", "true");

    try {
      const returnUrl = `${window.location.origin}${window.location.pathname}`;
      const email = String(state.user?.email || "").trim().toLowerCase();
      const customerId = String(state.user?.stripeCustomerId || "").trim();

      let url = "";
      try {
        const response = await fetch("/api/shop/portal", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
          },
          credentials: "same-origin",
          body: JSON.stringify({
            email,
            customerId,
            returnUrl,
          }),
        });
        const payload = await response.json().catch(() => ({}));
        if (response.ok && payload?.url) {
          url = String(payload.url || "").trim();
        }
      } catch (_error) {}

      if (!url) {
        const createPortal = functions.httpsCallable("create_stripe_billing_portal_session");
        const result = await createPortal({
          returnUrl,
          email,
          customerId,
          meta: buildMeta(),
        });
        url = String(result.data?.url || "").trim();
      }

      if (!url) throw new Error("Stripe billing portal URL is missing.");
      logEvent("billing_portal_open", { provider: "stripe" });
      window.location.assign(url);
      return;
    } catch (error) {
      showToast(error.message || "Unable to open Stripe billing portal.", "warn");
    } finally {
      ui.billingPortalLink.dataset.loading = "0";
      ui.billingPortalLink.textContent = originalText;
      ui.billingPortalLink.removeAttribute("aria-disabled");
    }
  };

  const handleCheckoutReturn = async (functions) => {
    if (isNativeIosStoreKitCheckoutOnly() || isNativeAndroidPlayBillingCheckout()) {
      try {
        const params = new URLSearchParams(window.location.search);
        if (params.has("checkout") || params.has("orderId") || params.has("session_id")) {
          params.delete("checkout");
          params.delete("orderId");
          params.delete("session_id");
          history.replaceState({}, "", `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ""}`);
        }
      } catch (error) {
        // Ignore URL cleanup errors.
      }
      return;
    }

    const checkout = String(getQueryParam("checkout") || "").trim().toLowerCase();
    if (!checkout) return;

    const orderId = String(getQueryParam("orderId") || "").trim();
    const sessionId = String(getQueryParam("session_id") || "").trim();
    if (!orderId) return;

    if (checkout === "cancel") {
      showToast("Checkout cancelled.", "warn");
      try {
        const params = new URLSearchParams(window.location.search);
        params.delete("checkout");
        params.delete("orderId");
        params.delete("session_id");
        history.replaceState({}, "", `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ""}`);
      } catch (error) {
        // Ignore.
      }
      return;
    }

    if (checkout !== "success" || !sessionId) return;
    if (!requireFullAccount("Sign in to finalize checkout.", { redirect: true })) return;

    try {
      const confirm = functions.httpsCallable("confirm_stripe_checkout");
      const result = await confirm({ orderId, sessionId });
      const paid = Boolean(result.data?.paid);
      const currency = String(result.data?.currency || "USD");
      const price = Number(result.data?.price || 0);
      const product = String(result.data?.product || "");
      showToast(paid ? "Payment confirmed." : "Payment pending review.");
      logEvent("purchase", {
        transaction_id: orderId,
        currency,
        value: Number.isFinite(price) ? price : 0,
        items: product ? [{ item_name: product, item_id: product, price }] : undefined,
      });
      if (paid) {
        try {
          history.replaceState({}, "", "/dashboard");
          window.location.assign("/dashboard");
        } catch (error) {
          // Ignore.
        }
      }
    } catch (error) {
      showToast(error.message || "Unable to confirm payment yet.", "warn");
    }
  };

		  const init = () => {
	    hydrateUnsplashGallery();
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
          const nativeAuthBridge = installNativeAuthBridge(auth);

	      state.clients = { auth, db, functions, storage, messaging };
	      hydrateUnsplashGallery(functions);
	      bindFeatureVoteForms(functions);

      window.__quanturaPanelActivated = (panel) => {
        const next = String(panel || "").trim();
        if (!next) return;
        const showTickerChart = next === "ticker";

        if (ui.studioChartShell) {
          // Keep the chart window scoped to the ticker panel only.
          ui.studioChartShell.classList.toggle("hidden", !showTickerChart);
        }
        if (ui.intelStrip) {
          // Keep the intel side-strip scoped to the ticker view.
          ui.intelStrip.classList.toggle("hidden", !showTickerChart);
        }
        if (next === "ticker") {
          state.intelActiveTab = "intelligence";
          if (ui.tickerIntelligenceOutput) {
            ui.tickerIntelligenceOutput.classList.remove("hidden");
          }
          const activeTicker = getActiveTicker() || normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (activeTicker) {
            syncTickerInputs(activeTicker, { source: "panel_open_ticker", skipHistory: true });
            refreshTradingViewForTicker(activeTicker);
          }
          if (activeTicker && Array.isArray(state.tickerContext.rows) && state.tickerContext.rows.length) {
            renderTickerChart(
              state.tickerContext.rows,
              activeTicker,
              state.tickerContext.interval || "1d",
              // TradingView does not support Quantura overlays; keep the TI view clean.
              []
            ).catch(() => {});
          } else if (activeTicker) {
            setTerminalChartEngineVisibility("tradingview");
          } else {
            setTerminalChartEngineVisibility("legacy");
          }
        } else {
          setTerminalChartEngineVisibility("legacy");
        }

        if (next === "predictions") {
          const activeTicker = getActiveTicker() || normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (activeTicker) {
            syncTickerInputs(activeTicker, { source: "panel_open_predictions", skipHistory: true });
          }
          const firstPredictionsLoad = !state.panelAutoloaded.predictions;
          state.panelAutoloaded.predictions = true;
          loadTickerPredictions(activeTicker, {
            mode: activeTicker ? "ticker" : "topActive",
            force: firstPredictionsLoad,
            notify: false,
          }).catch(() => {});
        }

        if (next === "trending") {
          if (!state.panelAutoloaded.trending) {
            state.panelAutoloaded.trending = true;
            loadTrendingTickers(functions, { notify: false });
          }
        }

        if (next === "news") {
          const ticker = getActiveTicker() || normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (!ticker) return;
          scheduleSideDataRefresh(ticker, { force: !state.panelAutoloaded.news });
          state.panelAutoloaded.news = true;
        }

        if (next === "events-calendar") {
          const first = !state.panelAutoloaded.eventsCalendar;
          state.panelAutoloaded.eventsCalendar = true;
          loadEarningsCalendar({ force: first, notify: false });
        }

        if (next === "market-headlines") {
          const first = !state.panelAutoloaded.marketHeadlines;
          state.panelAutoloaded.marketHeadlines = true;
          loadMarketHeadlinesFeed(functions, { force: first, notify: false });
        }

        if (next === "macro") {
          const firstMacro = !state.panelAutoloaded.macro;
          state.panelAutoloaded.macro = true;
          loadFiscalMacroDashboard({ force: firstMacro }).catch(() => {});
        }

        if (next === "ticker-query") {
          const ticker = getActiveTicker() || normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (ticker && ui.tickerQueryTicker && !String(ui.tickerQueryTicker.value || "").trim()) {
            ui.tickerQueryTicker.value = ticker;
          }
          syncModelCouncilSeo();
          if (ui.tickerQueryLanguage && ui.tickerQueryLanguage.value === "auto") {
            ui.tickerQueryLanguage.value = state.preferredLanguage || "en";
          }
          if (!state.tickerContext.tickerQueryModelsLoaded) {
            loadTickerQueryModels().catch(() => {});
          }
        }

        if (next === "options") {
          const ticker = getActiveTicker() || normalizeTicker(safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (!ticker) return;
          const first = !state.panelAutoloaded.options;
          state.panelAutoloaded.options = true;
          autoloadOptionsChain(functions, { force: first });
        }

        if (next === "notifications") {
          loadNotificationFeed({
            filter: state.notificationFeed?.filter || "all",
            unreadOnly: Boolean(state.notificationFeed?.unreadOnly),
            silent: true,
          }).catch(() => {});
        }

        scheduleNativeInlineAdsRefresh();
      };

      ensureThemeToggle();
      normalizeHeaderBranding();
      normalizeTopNavigation();
      normalizeFooterSocialLinks();
      normalizeFooterContactInfo();
      initPricingBillingToggle();
      bindSolveNowModalTriggers();
      removeHeaderSolveNowCta();
      ensureHeaderNotificationsCta();
      ensureSidebarCollapseToggle();
      bindMobileNav();
      bindMobileSidebarDrawer();
      bindMobileBottomNav();
      bindNativeRewardedNavigationAds();
      scheduleNativeInlineAdsRefresh();
      window.setInterval(scheduleNativeInlineAdsRefresh, 3500);
      initializeLanguageControls().catch(() => {});
      captureShareFromUrl();
      renderNotificationLog();
      renderNotificationFeed();
      ensureNotificationPrivacyControls();
      syncNotificationPrivacyControls();
      recordPromoSessionUsage();
      state.promoForecastCount = getStoredNumber(PROMO_FORECAST_COUNT_KEY, 0);
      bindChartControls();

	    if (!state.authResolved) {
	      if (ui.headerUserEmail) ui.headerUserEmail.textContent = "Restoring session...";
	      if (ui.headerUserStatus) ui.headerUserStatus.textContent = "Loading";
	    }

    if (ui.notificationsStatus) {
      const cachedToken = safeLocalStorageGet(FCM_TOKEN_CACHE_KEY) || "";
      setNotificationTokenPreview(cachedToken);
      if (!state.remoteFlags.pushEnabled) {
        setNotificationStatus("Notifications are temporarily disabled.");
        setNotificationControlsEnabled(false);
	      } else if (!isPushChannelAvailable()) {
	        setNotificationStatus("Push notifications are not supported on this device.");
	        setNotificationControlsEnabled(false);
	      } else if (!isNativeApp() && !messaging) {
	        setNotificationStatus("Messaging SDK is not loaded on this page.");
	        setNotificationControlsEnabled(false);
	      } else {
	        setNotificationStatus("Sign in and enable notifications.");
	        setNotificationControlsEnabled(true);
	      }
	    }

    bindForegroundPushHandler(messaging);
    bindNativePurchaseResultBridge();

    const nativeRuntime = isNativeApp();
    if (nativeRuntime && state.cookieConsent !== "accepted") {
      state.cookieConsent = "accepted";
      safeLocalStorageSet(COOKIE_CONSENT_KEY, "accepted");
      ensureInitialPageView();
      setUserId(state.user?.uid || null);
      const existingCookieBanner = document.getElementById("cookie-banner");
      if (existingCookieBanner) existingCookieBanner.classList.add("hidden");
    }

    if (state.cookieConsent === "accepted") {
      ensureInitialPageView();
    } else if (state.cookieConsent !== "declined") {
      ensureCookieModal().classList.remove("hidden");
	    }
      ensureProfileFeedbackButtons();
	    bindPanelNavigation();
      bindTickerIntelTabs();
      bindFaqAccordion();
	    syncStickyOffsets();
	    window.addEventListener("resize", () => window.requestAnimationFrame(syncStickyOffsets));
	    window.setTimeout(syncStickyOffsets, 280);
      useRemoteConfig(() => {
        refreshScreenerModelUi();
        refreshScreenerCreditsUi();
      });
	    loadRemoteConfig().then(() => {
        refreshScreenerModelUi();
        refreshScreenerCreditsUi();
      }).catch(() => {
        loadServerPromoStatus().catch(() => {});
      });
      handleCheckoutReturn(functions);

			    document.addEventListener("click", async (event) => {
		      const target = event.target.closest("[data-analytics]");
		      if (!target) return;
		      logEvent(target.dataset.analytics, {
	        label: target.dataset.label || target.textContent.trim(),
	        page_path: window.location.pathname,
	      });
	    });

		    document.addEventListener("click", async (event) => {
		      const social = event.target.closest(".social-link");
		      if (!social) return;
		      const href = social.getAttribute("href") || "";
		      if (!href || href === "#") {
		        event.preventDefault();
		        showToast("Social links are coming soon.");
		      }
		    });

		    document.addEventListener("click", async (event) => {
		      const copyButton = event.target.closest('[data-action="copy-bibtex"]');
		      if (!copyButton) return;
		      event.preventDefault();
		      const bibtex = String(copyButton.dataset.bibtex || "").trim();
		      if (!bibtex) {
		        showToast("Citation data is unavailable.", "warn");
		        return;
		      }
		      copyButton.disabled = true;
		      try {
		        const copied = await copyToClipboard(bibtex);
		        if (!copied) throw new Error("clipboard_unavailable");
		        showToast("BibTeX copied.");
		      } catch (error) {
		        showToast("Unable to copy BibTeX.", "warn");
		      } finally {
		        copyButton.disabled = false;
		      }
		    });

		    const pickTicker = async (rawTicker) => {
		      const ticker = normalizeTicker(rawTicker);
		      if (!ticker) return;
		      syncTickerInputs(ticker, { source: "pick_ticker", emitAnalytics: true });
		      logEvent("ticker_selected", { ticker, page_path: window.location.pathname });

			      // If we're in the terminal, load the chart immediately. Otherwise, jump to the terminal.
			      if (ui.terminalForm && ui.terminalTicker && ui.tickerChart) {
              window.__quanturaSetPanel?.("ticker");
			        ui.terminalTicker.value = ticker;
			        ui.terminalForm.requestSubmit?.();
			      } else {
		        const params = new URLSearchParams();
		        params.set("ticker", ticker);
		        window.location.href = `/ticker-intelligence?${params.toString()}`;
		      }
		    };

		    document.addEventListener("click", async (event) => {
          const historySelect = event.target.closest('[data-action="ticker-history-select"]');
          if (historySelect) {
            event.preventDefault();
            await pickTicker(historySelect.dataset.ticker || "");
            return;
          }

          const historyDelete = event.target.closest('[data-action="ticker-history-delete"]');
          if (historyDelete) {
            event.preventDefault();
            removeTickerFromHistory(historyDelete.dataset.ticker || "");
            return;
          }

		      const button = event.target.closest('[data-action="pick-ticker"]');
		      if (!button) return;
		      event.preventDefault();
		      await pickTicker(button.dataset.ticker || button.textContent);
		    });

        document.addEventListener("click", async (event) => {
          const retryBtn = event.target.closest('[data-action="x-trends-retry"]');
          if (retryBtn) {
            event.preventDefault();
            const ticker = normalizeTicker(retryBtn.dataset.ticker || state.tickerContext.xTicker || state.tickerContext.ticker || "");
            if (!ticker) return;
            const query = normalizeXSocialQuery(retryBtn.dataset.query || state.tickerContext.xQuery || "");
            await loadTickerXTrends(functions, ticker, {
              force: true,
              notify: true,
              page: 1,
              append: false,
              queryOverride: query,
            });
            return;
          }

          const variantBtn = event.target.closest('[data-action="x-trends-variant"]');
          if (variantBtn) {
            event.preventDefault();
            const ticker = normalizeTicker(variantBtn.dataset.ticker || state.tickerContext.xTicker || state.tickerContext.ticker || "");
            if (!ticker) return;
            const query = normalizeXSocialQuery(variantBtn.dataset.query || "");
            await loadTickerXTrends(functions, ticker, {
              force: true,
              notify: true,
              page: 1,
              append: false,
              queryOverride: query,
            });
            return;
          }

          const loadMoreBtn = event.target.closest('[data-action="x-trends-more"]');
          if (loadMoreBtn) {
            event.preventDefault();
            const ticker = normalizeTicker(loadMoreBtn.dataset.ticker || state.tickerContext.xTicker || state.tickerContext.ticker || "");
            if (!ticker) return;
            const query = normalizeXSocialQuery(loadMoreBtn.dataset.query || state.tickerContext.xQuery || "");
            const nextPage = Math.max(2, Number(loadMoreBtn.dataset.nextPage || state.tickerContext.xPage + 1 || 2));
            loadMoreBtn.disabled = true;
            try {
              await loadTickerXTrends(functions, ticker, {
                force: true,
                notify: false,
                page: nextPage,
                append: true,
                queryOverride: query,
              });
            } finally {
              loadMoreBtn.disabled = false;
            }
          }
        });

        document.addEventListener("click", async (event) => {
          const fullInfoBtn = event.target.closest('[data-action="intel-load-full-info"]');
          if (!fullInfoBtn) return;
          event.preventDefault();
          const symbol = normalizeTicker(fullInfoBtn.dataset.ticker || state.tickerContext.ticker || "");
          if (!symbol || !functions) return;
          const output = document.getElementById("intel-raw-info-output");
          const controls = document.querySelector("[data-intel-raw-controls]");
          const filterInput = document.querySelector('[data-action="intel-filter-full-info"]');
          if (!output) return;

          const isExpanded = fullInfoBtn.dataset.expanded === "1";
          if (isExpanded) {
            output.classList.add("hidden");
            controls?.classList.add("hidden");
            fullInfoBtn.dataset.expanded = "0";
            fullInfoBtn.innerHTML = "View all fields";
            return;
          }

          fullInfoBtn.disabled = true;
          const originalLabel = fullInfoBtn.innerHTML;
          if (output.classList.contains("hidden")) {
            output.classList.remove("hidden");
          }
          output.innerHTML = `<div data-skeleton>${skeletonHtml(5)}<div class="small muted" style="margin-top:10px;">Loading full info fields...</div></div>`;
          try {
            await loadTickerFullInfo(functions, symbol);
            controls?.classList.remove("hidden");
            const filterValue = String(filterInput?.value || "").trim();
            renderTickerFullInfoEntries(symbol, filterValue);
            fullInfoBtn.dataset.expanded = "1";
            fullInfoBtn.innerHTML = "Hide all fields";
          } catch (error) {
            output.innerHTML = `<div class="small muted">Unable to load full info fields right now.</div>`;
            fullInfoBtn.dataset.expanded = "0";
            fullInfoBtn.innerHTML = originalLabel || "View all fields";
            showToast(error.message || "Unable to load full info fields.", "warn");
          } finally {
            fullInfoBtn.disabled = false;
          }
        });

        document.addEventListener("input", (event) => {
          const filterInput = event.target.closest?.('[data-action="intel-filter-full-info"]');
          if (!filterInput) return;
          const symbol = normalizeTicker(filterInput.dataset.ticker || state.tickerContext.ticker || "");
          if (!symbol) return;
          state.tickerContext.fullInfoFilter = String(filterInput.value || "");
          renderTickerFullInfoEntries(symbol, state.tickerContext.fullInfoFilter);
        });

        document.addEventListener("click", (event) => {
          const action = event.target.closest("[data-action]")?.dataset?.action;
          if (!action) return;
          if (!ui.tasksCalendar) return;

          const cursor = state.taskCalendarCursor instanceof Date ? new Date(state.taskCalendarCursor) : new Date();
          const base = new Date(cursor.getFullYear(), cursor.getMonth(), 1);

          if (action === "calendar-prev") {
            event.preventDefault?.();
            base.setMonth(base.getMonth() - 1);
          } else if (action === "calendar-next") {
            event.preventDefault?.();
            base.setMonth(base.getMonth() + 1);
          } else if (action === "calendar-today") {
            event.preventDefault?.();
            const now = new Date();
            base.setFullYear(now.getFullYear(), now.getMonth(), 1);
          } else {
            return;
          }

          state.taskCalendarCursor = new Date(base.getFullYear(), base.getMonth(), 1);
          renderTaskCalendar(state.taskCalendarTasks);
        });

		    document.addEventListener("click", async (event) => {
		      const plotButton = event.target.closest('[data-action="plot-forecast"]');
		      if (!plotButton) return;
		      const forecastId = plotButton.dataset.forecastId;
		      if (!forecastId) return;

      try {
        await ensureSessionUser({
          reason: "forecast_plot_requires_session",
          message: "Sign in to sync saved forecast runs.",
        });
      } catch (error) {
        showToast(error?.message || "Unable to start guest session.", "warn");
        return;
      }

		      const onTerminalPage = Boolean(ui.terminalForm && ui.tickerChart);
		      if (!onTerminalPage) {
		        logEvent("forecast_plot_navigate", { forecast_id: forecastId });
		        const params = new URLSearchParams();
		        params.set("forecastId", forecastId);
		        window.location.href = `/forecasting?${params.toString()}`;
		        return;
		      }

	      try {
	        setTerminalStatus("Loading saved run...");
	        await plotForecastById(db, functions, forecastId);
	        logEvent("forecast_plotted", { forecast_id: forecastId });
          document.querySelector('[data-panel-target="forecast"]')?.click?.();
	        document.getElementById("terminal")?.scrollIntoView({ behavior: "smooth" });
	      } catch (error) {
	        setTerminalStatus(error.message || "Unable to plot forecast.");
	        showToast(error.message || "Unable to plot forecast.", "warn");
		      }
		    });

        document.addEventListener("click", async (event) => {
          const dlButton = event.target.closest('[data-action="download-forecast"]');
          if (!dlButton) return;
          event.preventDefault();
          try {
            await ensureSessionUser({
              reason: "forecast_download_requires_session",
              message: "Sign in to sync forecast downloads.",
            });
          } catch (error) {
            showToast(error?.message || "Unable to start guest session.", "warn");
            return;
          }
          const forecastId = String(dlButton.dataset.forecastId || "").trim();
          if (!forecastId) return;

          try {
            dlButton.disabled = true;
            setTerminalStatus("Preparing download...");
	            const doc = await loadForecastDoc(db, forecastId);
	            const rows = Array.isArray(doc.forecastRows) ? doc.forecastRows : [];
	            if (!rows.length) throw new Error("No forecast rows stored for this run.");
              if (String(doc.chartSeriesSource || "") === "preview_only") {
                showToast("Only preview rows are available on this device. Downloading preview CSV.", "warn");
              }

            const quantKeys = Object.keys(rows[0] || {})
              .filter((key) => /^q\d\d$/.test(key))
              .sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
            const headers = ["ds", ...quantKeys];
            const csv = buildCsv(rows, headers);
            const ticker = normalizeTicker(doc.ticker || "ticker") || "ticker";
            const service = String(doc.service || "forecast").replace(/[^a-z0-9_\\-]+/gi, "_");
            triggerDownload(`${ticker}_${service}_${doc.id || forecastId}.csv`, csv);
            showToast("CSV downloaded.");
            logEvent("forecast_csv_downloaded", { forecast_id: forecastId, ticker, service });
          } catch (error) {
            showToast(error.message || "Unable to download forecast.", "warn");
          } finally {
            dlButton.disabled = false;
            setTerminalStatus("");
          }
        });

        document.addEventListener("click", async (event) => {
          const dlButton = event.target.closest('[data-action="download-screener"]');
          const sharedDlButton = event.target.closest('[data-action="download-shared-screener"]');
          if (!dlButton && !sharedDlButton) return;
          event.preventDefault();
          if (sharedDlButton) {
            const sharedRows = Array.isArray(state.sharedScreenerView?.rows) ? state.sharedScreenerView.rows : [];
            if (!sharedRows.length) {
              showToast("No shared screener rows are loaded.", "warn");
              return;
            }
            try {
              sharedDlButton.disabled = true;
              const headers = ["symbol", "lastClose", "return1m", "return3m", "rsi14", "volatility", "score", "projectedRoi"];
              const csv = buildCsv(sharedRows, headers);
              const runId = String(state.sharedScreenerView?.runId || "shared").trim() || "shared";
              triggerDownload(`quantura_screener_${runId}.csv`, csv);
              showToast("CSV downloaded.");
              logEvent("screener_csv_downloaded_shared", { share_id: state.sharedScreenerView?.shareId || "" });
            } catch (error) {
              showToast(error.message || "Unable to download shared screener run.", "warn");
            } finally {
              sharedDlButton.disabled = false;
            }
            return;
          }

          try {
            await ensureSessionUser({
              reason: "screener_download_requires_session",
              message: "Sign in to sync screener downloads.",
            });
          } catch (error) {
            showToast(error?.message || "Unable to start guest session.", "warn");
            return;
          }
          const runId = String(dlButton.dataset.runId || "").trim();
          if (!runId) return;

          try {
            dlButton.disabled = true;
            const doc = await db.collection("screener_runs").doc(runId).get();
            if (!doc.exists) throw new Error("Screener run not found.");
            const data = doc.data() || {};
            const rows = Array.isArray(data.results) ? data.results : [];
            if (!rows.length) throw new Error("No results stored for this run.");

            const headers = ["symbol", "lastClose", "return1m", "return3m", "rsi14", "volatility", "score"];
            const csv = buildCsv(rows, headers);
            triggerDownload(`quantura_screener_${runId}.csv`, csv);
            showToast("CSV downloaded.");
            logEvent("screener_csv_downloaded", { run_id: runId });
          } catch (error) {
            showToast(error.message || "Unable to download screener run.", "warn");
          } finally {
            dlButton.disabled = false;
          }
        });

        document.addEventListener("click", async (event) => {
          const actionTarget = event.target.closest('[data-action^="my-request-"]');
          if (!actionTarget) return;
          const action = String(actionTarget.dataset.action || "").trim();
          if (!action || action === "my-requests-refresh") return;
          const requestId = String(actionTarget.dataset.requestId || "").trim();
          if (!requestId) return;
          event.preventDefault();

          try {
            await ensureSessionUser({
              reason: "my_requests_requires_session",
              message: "Sign in to sync your saved requests.",
            });
          } catch (error) {
            showToast(error?.message || "Unable to start guest session.", "warn");
            return;
          }

          const button = actionTarget.closest("button") || actionTarget;
          const previousDisabled = Boolean(button.disabled);
          button.disabled = true;
          try {
            let request = getMyRequestById(requestId);
            if (!request && action !== "my-request-delete") {
              request = await fetchMyRequestById(requestId);
            }

            if (action === "my-request-load") {
              await loadMyRequestIntoUi({ requestId, request, db, functions, notify: true });
              logEvent("my_request_loaded", {
                request_id: requestId,
                type: normalizeMyRequestType(request?.type || ""),
              });
              return;
            }

            if (action === "my-request-share") {
              if (!request) throw new Error("Request not found.");
              await openMyRequestShareModal({
                request,
                onSaved: (saved) => {
                  if (saved && typeof saved === "object") {
                    upsertMyRequestInState(saved);
                    renderMyRequestsPanels();
                  }
                },
              });
              logEvent("my_request_share_opened", {
                request_id: requestId,
                type: normalizeMyRequestType(request?.type || ""),
              });
              return;
            }

            if (action === "my-request-rename") {
              if (!request) throw new Error("Request not found.");
              const nextTitle = await openPromptModal({
                title: "Rename request",
                message: "Use a short title so you can find this request quickly later.",
                label: "Title",
                placeholder: "Request title",
                initialValue: String(request.title || ""),
                confirmLabel: "Save",
              });
              if (!nextTitle) return;
              await updateMyRequest(requestId, { title: nextTitle }, { method: "PATCH" });
              showToast("Request renamed.");
              logEvent("my_request_renamed", {
                request_id: requestId,
                type: normalizeMyRequestType(request?.type || ""),
              });
              return;
            }

            if (action === "my-request-duplicate") {
              const body = await updateMyRequest(
                requestId,
                {},
                { method: "POST", path: `/api/my-requests/${encodeURIComponent(requestId)}/duplicate` }
              );
              const duplicatedId = String(body?.request?.id || "").trim();
              showToast(duplicatedId ? `Request duplicated (${duplicatedId.slice(0, 12)}...).` : "Request duplicated.");
              logEvent("my_request_duplicated", {
                request_id: requestId,
                duplicated_request_id: duplicatedId,
                type: normalizeMyRequestType(request?.type || body?.request?.type || ""),
              });
              return;
            }

            if (action === "my-request-unpublish") {
              await updateMyRequest(
                requestId,
                {},
                { method: "POST", path: `/api/my-requests/${encodeURIComponent(requestId)}/unpublish` }
              );
              showToast("Request unpublished from Explore.");
              logEvent("my_request_unpublished", {
                request_id: requestId,
                type: normalizeMyRequestType(request?.type || ""),
              });
              return;
            }

            if (action === "my-request-delete") {
              const confirmed = await openConfirmModal({
                title: "Delete request?",
                message: "This removes the request from your list and unpublishes it from Explore.",
                confirmLabel: "Delete",
                danger: true,
              });
              if (!confirmed) return;
              await updateMyRequest(
                requestId,
                {},
                { method: "DELETE", path: `/api/my-requests/${encodeURIComponent(requestId)}` }
              );
              showToast("Request deleted.");
              logEvent("my_request_deleted", { request_id: requestId, type: normalizeMyRequestType(request?.type || "") });
            }
          } catch (error) {
            showToast(error.message || "Unable to update request.", "warn");
          } finally {
            button.disabled = previousDisabled;
          }
        });

        document.addEventListener("click", async (event) => {
          const copySharedScreener = event.target.closest('[data-action="copy-shared-screener-link"]');
          if (copySharedScreener) {
            event.preventDefault();
            const shareId = String(copySharedScreener.dataset.shareId || state.sharedScreenerView?.shareId || "").trim();
            if (!shareId) return;
            const url = buildShareUrl("screener", shareId);
            copySharedScreener.disabled = true;
            try {
              await performShare({
                url,
                title: "Quantura screener",
                text: "Shared screener run",
              });
              showToast("Share link copied.");
            } catch (error) {
              showToast(error.message || "Unable to copy share link.", "warn");
            } finally {
              copySharedScreener.disabled = false;
            }
            return;
          }

          const importSharedScreener = event.target.closest('[data-action="import-shared-screener"]');
          if (importSharedScreener) {
            event.preventDefault();
            const shareId = String(importSharedScreener.dataset.shareId || state.sharedScreenerView?.shareId || "").trim();
            if (!shareId) return;
            if (!hasFullAccount()) {
              setPendingShareId(shareId);
              window.location.href = "/account";
              return;
            }
            importSharedScreener.disabled = true;
            try {
              setPendingShareId(shareId);
              const result = await importSharedItemById(functions, shareId, { redirect: false });
              setPendingShareId("");
              showToast("Shared screener saved to your dashboard.");
              logEvent("shared_item_imported", { kind: result.kind });
              if (result.kind === "screener") {
                window.location.href = `/screener?runId=${encodeURIComponent(result.importedId)}`;
              } else if (result.kind === "forecast") {
                window.location.href = `/forecasting?forecastId=${encodeURIComponent(result.importedId)}`;
              } else if (result.kind === "upload") {
                window.location.href = `/uploads?uploadId=${encodeURIComponent(result.importedId)}`;
              }
            } catch (error) {
              setPendingShareId(shareId);
              showToast(error.message || "Unable to import shared screener.", "warn");
            } finally {
              importSharedScreener.disabled = false;
            }
            return;
          }

          const shareForecast = event.target.closest('[data-action="share-forecast"]');
          if (shareForecast) {
            event.preventDefault();
            if (!hasFullAccount()) {
              showToast("Sign in to share forecasts.", "warn");
              return;
            }
            const forecastId = String(shareForecast.dataset.forecastId || "").trim();
            if (!forecastId) return;

            shareForecast.disabled = true;
            try {
              const createShare = functions.httpsCallable("create_share_link");
              const result = await createShare({ kind: "forecast", id: forecastId, meta: buildMeta() });
              const shareId = String(result.data?.shareId || "").trim();
              const url = String(result.data?.shareUrl || "") || buildShareUrl("forecast", shareId);
              if (!shareId || !url) throw new Error("Unable to create share link.");
              await performShare({
                url,
                title: "Quantura forecast",
                text: "Forecast shared from Quantura.",
              });
              showToast("Share link copied.");
              logEvent("forecast_shared", { forecast_id: forecastId });
            } catch (error) {
              showToast(error.message || "Unable to share forecast.", "warn");
            } finally {
              shareForecast.disabled = false;
            }
            return;
          }

	          const deleteForecast = event.target.closest('[data-action="delete-forecast"]');
	          if (deleteForecast) {
	            event.preventDefault();
	            if (!hasFullAccount()) {
	              showToast("Sign in to delete forecasts.", "warn");
	              return;
	            }
	            const forecastId = String(deleteForecast.dataset.forecastId || "").trim();
	            if (!forecastId) return;
	            const ok = await openConfirmModal({
	              title: "Delete saved run?",
	              message: "This removes the saved run from your workspace. This cannot be undone.",
	              confirmLabel: "Delete",
	              danger: true,
	            });
	            if (!ok) return;

            deleteForecast.disabled = true;
            try {
              const del = functions.httpsCallable("delete_forecast_request");
              await del({ forecastId, meta: buildMeta() });
              showToast("Forecast deleted.");
              logEvent("forecast_deleted", { forecast_id: forecastId });
              if (state.tickerContext.forecastId === forecastId) {
                state.tickerContext.forecastId = "";
                state.tickerContext.forecastDoc = null;
                if (ui.forecastOutput) ui.forecastOutput.innerHTML = `<div class="small muted">Forecast deleted.</div>`;
              }
            } catch (error) {
              showToast(error.message || "Unable to delete forecast.", "warn");
            } finally {
              deleteForecast.disabled = false;
            }
            return;
          }

          const renameScreener = event.target.closest('[data-action="rename-screener"]');
          if (renameScreener) {
            event.preventDefault();
            if (!hasFullAccount()) {
              showToast("Sign in to rename screener runs.", "warn");
              return;
            }
            const runId = String(renameScreener.dataset.runId || "").trim();
            if (!runId) return;

            let currentTitle = "";
            try {
              const snap = await db.collection("screener_runs").doc(runId).get();
              if (snap.exists) currentTitle = String(snap.data()?.title || "");
            } catch (error) {
              currentTitle = "";
            }

            const nextTitle = await openPromptModal({
              title: "Rename screener run",
              message: "Update the title shown for this saved screener run.",
              label: "Title",
              placeholder: "Screener run",
              initialValue: currentTitle,
              confirmLabel: "Rename",
            });
            if (!nextTitle) return;

            renameScreener.disabled = true;
            try {
              const rename = functions.httpsCallable("rename_screener_run");
              await rename({ runId, title: nextTitle, meta: buildMeta() });
              showToast("Screener run renamed.");
              logEvent("screener_renamed", { run_id: runId });
              const fresh = await db.collection("screener_runs").doc(runId).get();
              if (fresh.exists) renderScreenerRunOutput({ id: fresh.id, ...(fresh.data() || {}) });
            } catch (error) {
              showToast(error.message || "Unable to rename screener run.", "warn");
            } finally {
              renameScreener.disabled = false;
            }
            return;
          }

          const shareScreener = event.target.closest('[data-action="share-screener"]');
          if (shareScreener) {
            event.preventDefault();
            if (!hasFullAccount()) {
              showToast("Sign in to share screener runs.", "warn");
              return;
            }
            const runId = String(shareScreener.dataset.runId || "").trim();
            if (!runId) return;

            shareScreener.disabled = true;
            try {
              const createShare = functions.httpsCallable("create_share_link");
              const result = await createShare({ kind: "screener", id: runId, meta: buildMeta() });
              const shareId = String(result.data?.shareId || "").trim();
              const url = String(result.data?.shareUrl || "") || buildShareUrl("screener", shareId);
              if (!shareId || !url) throw new Error("Unable to create share link.");
              await performShare({
                url,
                title: "Quantura screener",
                text: "Screener run shared from Quantura.",
              });
              showToast("Share link copied.");
              logEvent("screener_shared", { run_id: runId });
            } catch (error) {
              showToast(error.message || "Unable to share screener run.", "warn");
            } finally {
              shareScreener.disabled = false;
            }
            return;
          }

          const generatePortfolio = event.target.closest('[data-action="generate-ai-portfolio"]');
          if (generatePortfolio) {
            event.preventDefault();
            if (!hasFullAccount()) {
              showToast("Sign in to generate AI Portfolios.", "warn");
              return;
            }
            const runId = String(generatePortfolio.dataset.runId || "").trim();
            if (!runId) return;
            generatePortfolio.disabled = true;
            try {
              const preferredName = String(document.getElementById("screener-agent-name")?.value || "").trim();
              const selectedModel = normalizeAiModelId(ui.screenerModel?.value || state.selectedScreenerModel || "");
              await generateAIPortfolioForRun({ db, functions, runId, preferredName, selectedModel });
              logEvent("ai_portfolio_generated", { run_id: runId });
            } catch (error) {
              showToast(error.message || "Unable to generate AI Portfolio.", "warn");
            } finally {
              generatePortfolio.disabled = false;
            }
            return;
          }

          const renameAgent = event.target.closest('[data-action="rename-ai-agent"]');
          if (renameAgent) {
            event.preventDefault();
            if (!hasFullAccount()) {
              showToast("Sign in to rename AI Agents.", "warn");
              return;
            }
            const agentId = String(renameAgent.dataset.agentId || "").trim();
            if (!agentId) return;
            const workspaceId = state.activeWorkspaceId || state.user.uid;
            const current = state.aiAgents.find((agent) => String(agent.id || "") === agentId);
            const nextName = await openPromptModal({
              title: "Rename AI Agent",
              message: "Update the custom name shown in Explore.",
              label: "Agent name",
              placeholder: "Quantura Horizon",
              initialValue: String(current?.name || "").trim(),
              confirmLabel: "Rename",
            });
            if (!nextName) return;

            renameAgent.disabled = true;
            try {
              await db
                .collection("users")
                .doc(workspaceId)
                .collection("ai_agents")
                .doc(agentId)
                .set(
                  {
                    name: nextName.trim(),
                    updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
                  },
                  { merge: true }
                );
              showToast("AI Agent renamed.");
              logEvent("ai_agent_renamed", { agent_id: agentId });
            } catch (error) {
              showToast(error.message || "Unable to rename AI Agent.", "warn");
            } finally {
              renameAgent.disabled = false;
            }
            return;
          }

          const followAgent = event.target.closest('[data-action="ai-agent-follow"]');
          if (followAgent) {
            event.preventDefault();
            const agentId = String(followAgent.dataset.agentId || "").trim();
            if (!agentId) return;
            followAgent.disabled = true;
            try {
              await toggleAIAgentSocial({ kind: "follow", agentId, db, functions });
            } catch (error) {
              showToast(error.message || "Unable to update follow state.", "warn");
            } finally {
              followAgent.disabled = false;
            }
            return;
          }

          const likeAgent = event.target.closest('[data-action="ai-agent-like"]');
          if (likeAgent) {
            event.preventDefault();
            const agentId = String(likeAgent.dataset.agentId || "").trim();
            if (!agentId) return;
            likeAgent.disabled = true;
            try {
              await toggleAIAgentSocial({ kind: "like", agentId, db, functions });
            } catch (error) {
              showToast(error.message || "Unable to update like state.", "warn");
            } finally {
              likeAgent.disabled = false;
            }
            return;
          }

          const shareAgent = event.target.closest('[data-action="ai-agent-share"]');
          if (shareAgent) {
            event.preventDefault();
            const agentId = String(shareAgent.dataset.agentId || "").trim();
            if (!agentId) return;
            const url = buildAIAgentShareUrl(agentId);
            try {
              await performShare({
                url,
                title: "Quantura AI Agent",
                text: "AI Agent shared from Quantura.",
              });
              showToast("Agent link copied.");
              logEvent("ai_agent_shared", { agent_id: agentId });
            } catch (error) {
              showToast(error.message || "Unable to copy share link.", "warn");
            }
            return;
          }

          const agentThanks = event.target.closest('[data-action="ai-agent-thanks"]');
          if (agentThanks) {
            event.preventDefault();
            const creatorWorkspaceId = String(agentThanks.dataset.creatorWorkspaceId || "").trim();
            const targetId = String(agentThanks.dataset.targetId || "").trim();
            agentThanks.disabled = true;
            try {
              await startCreatorSupportCheckout({
                functions,
                creatorWorkspaceId,
                mode: "tip",
                targetId,
                targetType: "screener",
              });
            } catch (error) {
              showToast(error.message || "Unable to open support checkout.", "warn");
            } finally {
              agentThanks.disabled = false;
            }
            return;
          }

          const agentSubscribe = event.target.closest('[data-action="ai-agent-subscribe"]');
          if (agentSubscribe) {
            event.preventDefault();
            const creatorWorkspaceId = String(agentSubscribe.dataset.creatorWorkspaceId || "").trim();
            const targetId = String(agentSubscribe.dataset.targetId || "").trim();
            agentSubscribe.disabled = true;
            try {
              await startCreatorSupportCheckout({
                functions,
                creatorWorkspaceId,
                mode: "subscribe",
                targetId,
                targetType: "screener",
              });
            } catch (error) {
              showToast(error.message || "Unable to open subscription checkout.", "warn");
            } finally {
              agentSubscribe.disabled = false;
            }
            return;
          }

          const toggleScreenerPublic = event.target.closest('[data-action="toggle-screener-public"]');
          if (toggleScreenerPublic) {
            event.preventDefault();
            if (!hasFullAccount()) {
              showToast("Sign in to update screener visibility.", "warn");
              return;
            }
            const runId = String(toggleScreenerPublic.dataset.runId || "").trim();
            if (!runId) return;
            const currentlyPublic = String(toggleScreenerPublic.dataset.isPublic || "0").trim() === "1";
            const nextValue = !currentlyPublic;
            toggleScreenerPublic.disabled = true;
            try {
              const updateVisibility = functions.httpsCallable("set_screener_public_visibility");
              await updateVisibility({ runId, isPublic: nextValue, meta: buildMeta() });
              const fresh = await db.collection("screener_runs").doc(runId).get();
              if (fresh.exists) {
                renderScreenerRunOutput({ id: fresh.id, ...(fresh.data() || {}) });
              }
              showToast(nextValue ? "Screener is now public." : "Screener is now private.");
            } catch (error) {
              showToast(error.message || "Unable to update screener visibility.", "warn");
            } finally {
              toggleScreenerPublic.disabled = false;
            }
            return;
          }

          const screenerOwnerThanks = event.target.closest('[data-action="screener-owner-thanks"]');
          if (screenerOwnerThanks) {
            event.preventDefault();
            const creatorWorkspaceId = String(screenerOwnerThanks.dataset.creatorWorkspaceId || "").trim();
            const targetId = String(screenerOwnerThanks.dataset.targetId || "").trim();
            screenerOwnerThanks.disabled = true;
            try {
              await startCreatorSupportCheckout({
                functions,
                creatorWorkspaceId,
                mode: "tip",
                targetId,
              });
            } catch (error) {
              showToast(error.message || "Unable to open support checkout.", "warn");
            } finally {
              screenerOwnerThanks.disabled = false;
            }
            return;
          }

          const screenerOwnerSubscribe = event.target.closest('[data-action="screener-owner-subscribe"]');
          if (screenerOwnerSubscribe) {
            event.preventDefault();
            const creatorWorkspaceId = String(screenerOwnerSubscribe.dataset.creatorWorkspaceId || "").trim();
            const targetId = String(screenerOwnerSubscribe.dataset.targetId || "").trim();
            screenerOwnerSubscribe.disabled = true;
            try {
              await startCreatorSupportCheckout({
                functions,
                creatorWorkspaceId,
                mode: "subscribe",
                targetId,
              });
            } catch (error) {
              showToast(error.message || "Unable to open subscription checkout.", "warn");
            } finally {
              screenerOwnerSubscribe.disabled = false;
            }
            return;
          }

	          const deleteScreener = event.target.closest('[data-action="delete-screener"]');
	          if (deleteScreener) {
	            event.preventDefault();
	            if (!hasFullAccount()) {
	              showToast("Sign in to delete screener runs.", "warn");
	              return;
	            }
	            const runId = String(deleteScreener.dataset.runId || "").trim();
	            if (!runId) return;
	            const ok = await openConfirmModal({
	              title: "Delete screener run?",
	              message: "This removes the saved screener run from your workspace. This cannot be undone.",
	              confirmLabel: "Delete",
	              danger: true,
	            });
	            if (!ok) return;

            deleteScreener.disabled = true;
            try {
              const del = functions.httpsCallable("delete_screener_run");
              await del({ runId, meta: buildMeta() });
              showToast("Screener run deleted.");
              logEvent("screener_deleted", { run_id: runId });
              if (ui.screenerOutput) ui.screenerOutput.innerHTML = `<div class="small muted">Screener run deleted.</div>`;
            } catch (error) {
              showToast(error.message || "Unable to delete screener run.", "warn");
            } finally {
              deleteScreener.disabled = false;
            }
            return;
          }

	          const deleteAutopilot = event.target.closest('[data-action="delete-autopilot"]');
	          if (deleteAutopilot) {
	            event.preventDefault();
	            if (!requireAdminAccess("Autopilot queue is currently admin-only.")) return;
	            const requestId = String(deleteAutopilot.dataset.requestId || "").trim();
	            if (!requestId) return;
	            const ok = await openConfirmModal({
	              title: "Delete autopilot request?",
	              message: "This removes the queued autopilot request. This cannot be undone.",
	              confirmLabel: "Delete",
	              danger: true,
	            });
	            if (!ok) return;

	            deleteAutopilot.disabled = true;
	            try {
	              const del = functions.httpsCallable("delete_autopilot_request");
	              await del({ requestId, meta: buildMeta() });
	              showToast("Autopilot request deleted.");
	              logEvent("autopilot_deleted", { request_id: requestId });
	            } catch (error) {
	              showToast(error.message || "Unable to delete autopilot request.", "warn");
	            } finally {
	              deleteAutopilot.disabled = false;
	            }
	            return;
	          }

	          const plotUpload = event.target.closest('[data-action="plot-upload"]');
	          if (plotUpload) {
	            event.preventDefault();
	            if (!requireAdminAccess("Prediction uploads are currently admin-only.")) return;
	            const uploadId = String(plotUpload.dataset.uploadId || "").trim();
	            if (!uploadId) return;
            try {
              await plotPredictionUploadById(db, storage, uploadId);
              showToast("Upload loaded.");
            } catch (error) {
              showToast(error.message || "Unable to plot upload.", "warn");
            }
            return;
          }

	          const downloadUpload = event.target.closest('[data-action="download-upload"]');
	          if (downloadUpload) {
	            event.preventDefault();
	            if (!requireAdminAccess("Prediction uploads are currently admin-only.")) return;
	            const uploadId = String(downloadUpload.dataset.uploadId || "").trim();
	            if (!uploadId) return;

            downloadUpload.disabled = true;
            try {
              const snap = await db.collection("prediction_uploads").doc(uploadId).get();
              if (!snap.exists) throw new Error("Upload not found.");
              const doc = { id: snap.id, ...(snap.data() || {}) };
              const url = await resolveUploadCsvUrl(storage, doc);
              if (!url) throw new Error("Upload is missing a downloadable URL.");
              const { text, source } = await fetchUploadCsvText({ uploadId, url, maxBytes: 5_000_000 });
              triggerDownload(String(doc.title || "predictions.csv"), text);
              showToast("CSV downloaded.");
              logEvent("predictions_downloaded", { upload_id: uploadId, source });
            } catch (error) {
              showToast(error.message || "Unable to download upload.", "warn");
            } finally {
              downloadUpload.disabled = false;
            }
            return;
          }

	          const renameUpload = event.target.closest('[data-action="rename-upload"]');
	          if (renameUpload) {
	            event.preventDefault();
	            if (!requireAdminAccess("Prediction uploads are currently admin-only.")) return;
	            const uploadId = String(renameUpload.dataset.uploadId || "").trim();
	            if (!uploadId) return;
	            let currentTitle = "";
	            try {
	              const snap = await db.collection("prediction_uploads").doc(uploadId).get();
	              if (snap.exists) currentTitle = String(snap.data()?.title || "");
	            } catch (error) {
	              currentTitle = "";
	            }
	            const nextTitle = await openPromptModal({
	              title: "Rename upload",
	              message: "Update the filename shown in your uploads list.",
	              label: "Title",
	              placeholder: "predictions.csv",
	              initialValue: currentTitle,
	              confirmLabel: "Rename",
	            });
	            if (!nextTitle) return;

            renameUpload.disabled = true;
            try {
              const rename = functions.httpsCallable("rename_prediction_upload");
              await rename({ uploadId, title: nextTitle, meta: buildMeta() });
              showToast("Upload renamed.");
              logEvent("predictions_renamed", { upload_id: uploadId });
            } catch (error) {
              showToast(error.message || "Unable to rename upload.", "warn");
            } finally {
              renameUpload.disabled = false;
            }
            return;
          }

	          const shareUpload = event.target.closest('[data-action="share-upload"]');
	          if (shareUpload) {
	            event.preventDefault();
	            if (!requireAdminAccess("Prediction uploads are currently admin-only.")) return;
	            const uploadId = String(shareUpload.dataset.uploadId || "").trim();
	            if (!uploadId) return;

            shareUpload.disabled = true;
            try {
              const createShare = functions.httpsCallable("create_share_link");
              const result = await createShare({ kind: "upload", id: uploadId, meta: buildMeta() });
              const shareId = String(result.data?.shareId || "").trim();
              const url = String(result.data?.shareUrl || "") || buildShareUrl("upload", shareId);
              if (!shareId || !url) throw new Error("Unable to create share link.");
              await performShare({
                url,
                title: "Quantura upload",
                text: "Prediction upload shared from Quantura.",
              });
              showToast("Share link copied.");
              logEvent("upload_shared", { upload_id: uploadId });
            } catch (error) {
              showToast(error.message || "Unable to share upload.", "warn");
            } finally {
              shareUpload.disabled = false;
            }
            return;
          }

	          const deleteUpload = event.target.closest('[data-action="delete-upload"]');
	          if (deleteUpload) {
	            event.preventDefault();
	            if (!requireAdminAccess("Prediction uploads are currently admin-only.")) return;
	            const uploadId = String(deleteUpload.dataset.uploadId || "").trim();
	            if (!uploadId) return;
	            const ok = await openConfirmModal({
	              title: "Delete upload?",
	              message: "This deletes the CSV metadata and removes the file from storage.",
	              confirmLabel: "Delete",
	              danger: true,
	            });
	            if (!ok) return;

            deleteUpload.disabled = true;
            try {
              const del = functions.httpsCallable("delete_prediction_upload");
              await del({ uploadId, meta: buildMeta() });
              showToast("Upload deleted.");
              logEvent("predictions_deleted", { upload_id: uploadId });
              if (state.predictionsContext.uploadId === uploadId) {
                state.predictionsContext.uploadId = "";
                state.predictionsContext.uploadDoc = null;
                state.predictionsContext.table = null;
                state.predictionsContext.previewPage = 0;
              }
              if (ui.predictionsPlotMeta) ui.predictionsPlotMeta.textContent = "Select an upload to preview and plot it.";
              if (ui.predictionsChart) ui.predictionsChart.innerHTML = "";
              if (ui.predictionsPreview) ui.predictionsPreview.innerHTML = "Preview will appear here.";
              if (ui.predictionsAgentOutput) {
                ui.predictionsAgentOutput.innerHTML =
                  "Run the OpenAI CSV Agent to compute weekday-aware quantile mapping and return an analyst summary.";
              }
            } catch (error) {
              showToast(error.message || "Unable to delete upload.", "warn");
            } finally {
              deleteUpload.disabled = false;
            }
          }

        });

		    document.addEventListener("click", async (event) => {
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
			      if (csvBtn) {
		        event.preventDefault();
		        const doc = state.tickerContext.forecastDoc;
		        if (!doc || !Array.isArray(doc.forecastRows) || !doc.forecastRows.length) {
		          showToast("No forecast rows available to export.", "warn");
		          return;
		        }
			        const rows = doc.forecastRows;
              if (String(doc.chartSeriesSource || "") === "preview_only") {
                showToast("Only preview rows are available on this device. Downloading preview CSV.", "warn");
              }
		        const quantKeys = extractQuantileKeys(rows);
		        const headers = ["ds", ...quantKeys];
		        const csv = buildCsv(rows, headers);
		        const ticker = normalizeTicker(doc.ticker || "ticker") || "ticker";
		        const service = String(doc.service || "forecast").replace(/[^a-z0-9_\\-]+/gi, "_");
		        triggerDownload(`${ticker}_${service}_${doc.id || "run"}.csv`, csv);
		        showToast("CSV downloaded.");
		        return;
		      }

      const pageSizeBtn = event.target.closest('[data-action="csv-page-size"]');
      if (pageSizeBtn) {
        event.preventDefault();
        const nextSize = Number(pageSizeBtn.dataset.size || "25");
        if (!Number.isFinite(nextSize) || nextSize <= 0) return;
        state.predictionsContext.previewPageSize = nextSize;
        state.predictionsContext.previewPage = 0;
        if (state.predictionsContext.table) renderCsvPreview(state.predictionsContext.table);
        return;
      }

      const csvPageBtn = event.target.closest('[data-action="csv-page"]');
      if (csvPageBtn) {
        event.preventDefault();
        const dir = Number(csvPageBtn.dataset.dir || "0");
        if (!Number.isFinite(dir) || !dir) return;
        const table = state.predictionsContext.table;
        if (!table || !Array.isArray(table.rows) || table.rows.length === 0) return;
        const pageSize = Number(state.predictionsContext.previewPageSize || 25) || 25;
        const totalPages = Math.max(1, Math.ceil(table.rows.length / pageSize));
        const next = Math.max(0, Math.min(totalPages - 1, Number(state.predictionsContext.previewPage || 0) + dir));
        state.predictionsContext.previewPage = next;
        renderCsvPreview(table);
      }
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

        bindMyRequestsPanels();
        renderMyRequestsPanels();

			    ui.headerAuth?.addEventListener("click", () => {
			      window.location.href = hasFullAccount() ? "/dashboard" : "/account";
			    });

	    ui.workspaceSelect?.addEventListener("change", () => {
	      if (!hasFullAccount()) {
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
          startScreenerRuns(db, next);
		      startWorkspaceTasks(db, next);
		      startWatchlist(db, next);
		      startPriceAlerts(db, next);
          startAIAgents(db, next);
          startVolatilityMonitor(db, functions, next);
          seedDefaultAIAgents(db, next).catch(() => {});
          fetchMyRequestsList({ force: true }).then(() => {
            renderMyRequestsPanels();
          });
		      showToast("Workspace updated.");
		    });

	    ui.collabInviteForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
	      if (!hasFullAccount()) {
	        showToast("Sign in to invite collaborators.", "warn");
	        return;
	      }
	      const seatLimit = getWorkspaceSeatLimitForTier();
	      const activeCount = Math.max(0, Number(state.collaboratorCount || 0));
	      const pendingCount = Math.max(0, Number(state.pendingCollabInviteCount || 0));
	      if (seatLimit <= 0) {
	        showToast("Your current plan does not include shared workspace seats. Upgrade to invite collaborators.", "warn");
	        return;
	      }
	      if (activeCount + pendingCount >= seatLimit) {
	        showToast(`Workspace seat limit reached (${seatLimit}). Upgrade your plan for more collaborators.`, "warn");
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
	        if (!hasFullAccount()) {
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
	      if (!hasFullAccount()) {
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
	      if (!hasFullAccount()) {
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
	      if (!hasFullAccount()) {
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
          await ensureVolatilityAlertsForWatchlist({ db, workspaceId, items: [{ ticker }] });
	        if (ui.watchlistNotes) ui.watchlistNotes.value = "";
          const defaultVol = Math.round(getConfiguredVolatilityThreshold() * 100);
	        showToast(`${ticker} added to watchlist. Default ±${defaultVol}% volatility alert enabled.`);
	        logEvent("watchlist_added", { ticker, workspace_id: workspaceId });
	      } catch (error) {
	        showToast(error.message || "Unable to update watchlist.", "warn");
	      }
	    });

	    ui.alertForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
	      if (!hasFullAccount()) {
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
	      if (!hasFullAccount()) {
	        showToast("Sign in first.", "warn");
	        return;
	      }
	      if (ui.alertsStatus) ui.alertsStatus.textContent = "Checking alerts...";
	      try {
	        const workspaceId = state.activeWorkspaceId || state.user.uid;
          const vol = await runVolatilityAlertsCheck({ db, functions, workspaceId, sendPush: true });
	        const check = functions.httpsCallable("check_price_alerts");
	        const result = await check({ workspaceId, meta: buildMeta() });
	        const data = result.data || {};
	        const triggered = Number(data.triggered || 0) + Number(vol.triggered || 0);
	        const checked = Number(data.checked || 0) + Number(vol.checked || 0);
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
	        if (!hasFullAccount()) return;
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
	      if (!hasFullAccount()) return;
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
	        if (!hasFullAccount()) return;
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
            await db.collection("users").doc(workspaceId).collection("price_alerts").doc(`volatility_${ticker}`).delete().catch(() => {});
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
	        if (!hasFullAccount()) return;
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
	      if (!hasFullAccount()) return;
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
	      if (!hasFullAccount()) return;
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

      const performSignOut = async () => {
        const runtime = resolveRuntimeLabel();
        try {
          await unregisterCachedNotificationToken(functions);
        } catch (error) {
          // Ignore token cleanup failures.
        }
        if (isNativeApp()) {
          requestNativeBridgeSignOut();
          window.__NATIVE_FCM_TOKEN__ = "";
        }
        clearPendingAuthCredential();
        await auth.signOut();
        showToast("Signed out.");
        logEvent("logout", { method: "firebase", runtime });
      };

      ui.headerSignOut?.addEventListener("click", async (event) => {
        event.preventDefault?.();
        await performSignOut();
      });

      ui.dashboardAuthLink?.addEventListener("click", async (event) => {
        if (!state.user) return;
        event.preventDefault?.();
        await performSignOut();
      });

    const isAuthCollision = (code) =>
      [
        "auth/credential-already-in-use",
        "auth/email-already-in-use",
        "auth/account-exists-with-different-credential",
        "auth/provider-already-linked",
      ].includes(String(code || "").trim());

    const providerMethodLabel = (providerMethod) => {
      const id = String(providerMethod || "").trim().toLowerCase();
      if (id === "google.com" || id === "google") return "Google";
      if (id === "facebook.com" || id === "facebook") return "Facebook";
      if (id === "github.com" || id === "github") return "GitHub";
      if (id === "twitter.com" || id === "twitter") return "X";
      if (id === "yahoo.com" || id === "yahoo") return "Yahoo";
      if (id === "microsoft.com" || id === "microsoft") return "Microsoft";
      if (id === "password" || id === "emailpassword") return "Email and password";
      return id || "another provider";
    };

    const buildProviderFromMethod = (providerMethod) => {
      const id = String(providerMethod || "").trim().toLowerCase();
      if (id === "google.com" || id === "google") {
        return new firebase.auth.GoogleAuthProvider();
      }
      if (id === "facebook.com" || id === "facebook") {
        const provider = new firebase.auth.FacebookAuthProvider();
        provider.addScope("email");
        return provider;
      }
      if (id === "github.com" || id === "github") {
        const provider = new firebase.auth.GithubAuthProvider();
        provider.addScope("user:email");
        return provider;
      }
      if (id === "twitter.com" || id === "twitter") {
        return new firebase.auth.TwitterAuthProvider();
      }
      if (id === "yahoo.com" || id === "yahoo") {
        const provider = new firebase.auth.OAuthProvider("yahoo.com");
        provider.addScope("profile");
        provider.addScope("email");
        return provider;
      }
      if (id === "microsoft.com" || id === "microsoft") {
        const provider = new firebase.auth.OAuthProvider("microsoft.com");
        provider.addScope("user.read");
        return provider;
      }
      return null;
    };

    const resolveAuthErrorEmail = (error) =>
      String(error?.email || error?.customData?.email || error?.customData?._tokenResponse?.email || "").trim();

    const serializeAuthCredential = (credential) => {
      if (!credential || typeof credential !== "object") return null;
      try {
        if (typeof credential.toJSON === "function") return credential.toJSON();
      } catch (error) {
        return null;
      }
      return null;
    };

    const deserializeAuthCredential = (value) => {
      if (!value || typeof value !== "object") return null;
      try {
        if (firebase?.auth?.AuthCredential?.fromJSON) {
          return firebase.auth.AuthCredential.fromJSON(value);
        }
      } catch (error) {
        return null;
      }
      return null;
    };

    const extractAuthErrorCredential = (error, methodHint = "") => {
      if (error?.credential) return error.credential;
      const method = String(methodHint || "").trim().toLowerCase();
      try {
        if (method === "google" || method === "google.com") {
          return firebase.auth.GoogleAuthProvider.credentialFromError(error);
        }
        if (method === "facebook" || method === "facebook.com") {
          return firebase.auth.FacebookAuthProvider.credentialFromError(error);
        }
        if (method === "github" || method === "github.com") {
          return firebase.auth.GithubAuthProvider.credentialFromError(error);
        }
        if (method === "twitter" || method === "twitter.com") {
          return firebase.auth.TwitterAuthProvider.credentialFromError(error);
        }
      } catch (credentialError) {
        return null;
      }
      return null;
    };

    const clearPendingAuthCredential = () => {
      safeLocalStorageRemove(AUTH_PENDING_CREDENTIAL_KEY);
      try {
        sessionStorage.removeItem(AUTH_PENDING_CREDENTIAL_KEY);
      } catch (error) {
        // Ignore session storage failures.
      }
    };

    const savePendingAuthCredential = (credential, context = {}) => {
      const serialized = serializeAuthCredential(credential);
      if (!serialized) return false;
      const payload = {
        credential: serialized,
        email: String(context.email || "").trim().toLowerCase(),
        providerId: String(context.providerId || credential?.providerId || "").trim().toLowerCase(),
        savedAt: Date.now(),
      };
      const json = JSON.stringify(payload);
      safeLocalStorageSet(AUTH_PENDING_CREDENTIAL_KEY, json);
      try {
        sessionStorage.setItem(AUTH_PENDING_CREDENTIAL_KEY, json);
      } catch (error) {
        // Ignore session storage failures.
      }
      return true;
    };

    const readPendingAuthCredential = () => {
      const raw =
        String(safeLocalStorageGet(AUTH_PENDING_CREDENTIAL_KEY) || "").trim() ||
        (() => {
          try {
            return String(sessionStorage.getItem(AUTH_PENDING_CREDENTIAL_KEY) || "").trim();
          } catch (error) {
            return "";
          }
        })();
      if (!raw) return null;
      try {
        const parsed = JSON.parse(raw);
        const credential = deserializeAuthCredential(parsed?.credential);
        if (!credential) return null;
        return {
          credential,
          email: String(parsed?.email || "").trim().toLowerCase(),
          providerId: String(parsed?.providerId || credential?.providerId || "").trim().toLowerCase(),
        };
      } catch (error) {
        return null;
      }
    };

    const linkPendingCredentialIfPresent = async ({ silent = false } = {}) => {
      const pending = readPendingAuthCredential();
      const currentUser = auth.currentUser;
      if (!pending || !pending.credential || !currentUser) return false;
      try {
        await currentUser.linkWithCredential(pending.credential);
        clearPendingAuthCredential();
        if (!silent) {
          const providerLabel = providerMethodLabel(pending.providerId);
          const message = `Connected ${providerLabel} to your account.`;
          if (ui.emailMessage) ui.emailMessage.textContent = message;
          showToast(message);
        }
        logEvent("auth_credential_linked", { provider: pending.providerId || "unknown" });
        return true;
      } catch (error) {
        if (isAuthCollision(error?.code) || String(error?.code || "") === "auth/provider-already-linked") {
          clearPendingAuthCredential();
          if (!silent) showToast("Provider already linked.");
          return true;
        }
        if (!silent && ui.emailMessage) {
          ui.emailMessage.textContent = error?.message || "Signed in, but unable to link provider.";
        }
        return false;
      }
    };

    const recoverFromAuthCollision = async (error, { methodHint = "", preferRuntime = resolveRuntimeLabel() } = {}) => {
      if (!isAuthCollision(error?.code)) return false;
      const email = resolveAuthErrorEmail(error);
      const collisionCredential = extractAuthErrorCredential(error, methodHint);
      if (collisionCredential) {
        savePendingAuthCredential(collisionCredential, {
          email,
          providerId: collisionCredential.providerId || methodHint,
        });
      }

      let methods = [];
      if (email) {
        try {
          methods = await auth.fetchSignInMethodsForEmail(email);
        } catch (methodsError) {
          methods = [];
        }
      }
      const primaryMethod = String(methods[0] || "").trim().toLowerCase();
      if (!primaryMethod) {
        if (ui.emailMessage) {
          ui.emailMessage.textContent =
            "This email is already in use. Sign in with your existing provider, then retry linking.";
        }
        showToast("Account exists with another provider. Use your existing sign-in first.", "warn");
        logEvent("auth_collision_unresolved", { method: methodHint || "unknown" });
        return true;
      }

      if (primaryMethod === "password") {
        const message =
          "This email already exists with password sign-in. Sign in with email/password first, then provider linking will continue.";
        if (ui.emailInput && email) ui.emailInput.value = email;
        if (ui.emailMessage) ui.emailMessage.textContent = message;
        showToast("Sign in with email/password to continue linking.", "warn");
        logEvent("auth_collision_requires_password", { method: methodHint || "unknown" });
        return true;
      }

      const provider = buildProviderFromMethod(primaryMethod);
      if (!provider) {
        const message = `Account exists with ${providerMethodLabel(primaryMethod)}. Complete that sign-in first.`;
        if (ui.emailMessage) ui.emailMessage.textContent = message;
        showToast(message, "warn");
        logEvent("auth_collision_provider_unavailable", { provider: primaryMethod });
        return true;
      }

      const providerLabel = providerMethodLabel(primaryMethod);
      const proceed = window.confirm(
        `${email || "This account"} is already linked to ${providerLabel}. Sign in with ${providerLabel} now to recover and link providers?`
      );
      if (!proceed) {
        if (ui.emailMessage) {
          ui.emailMessage.textContent = `Recovery paused. Use ${providerLabel} sign-in to continue account linking.`;
        }
        return true;
      }

      try {
        if (isInstalledPwa() || isMobileBrowser()) {
          await auth.signInWithRedirect(provider);
          logEvent("auth_collision_recovery_redirect_started", {
            provider: primaryMethod,
            runtime: preferRuntime,
          });
          return true;
        }
        await auth.signInWithPopup(provider);
        await linkPendingCredentialIfPresent({ silent: false });
        if (ui.emailMessage) ui.emailMessage.textContent = `Signed in with ${providerLabel}.`;
        showToast(`Signed in with ${providerLabel}.`);
        logEvent("auth_collision_recovered", { provider: primaryMethod, runtime: preferRuntime });
        return true;
      } catch (recoverError) {
        const message = recoverError?.message || `Unable to sign in with ${providerLabel}.`;
        if (ui.emailMessage) ui.emailMessage.textContent = message;
        showToast(message, "warn");
        logEvent("auth_collision_recovery_error", { provider: primaryMethod, runtime: preferRuntime });
        return true;
      }
    };

    const linkOrSignInWithCredential = async (credential, fallbackSignIn, { methodHint = "password", email = "" } = {}) => {
      const current = auth.currentUser;
      if (current?.isAnonymous) {
        try {
          await current.linkWithCredential(credential);
          await linkPendingCredentialIfPresent({ silent: true });
          return;
        } catch (linkError) {
          if (await recoverFromAuthCollision(linkError, { methodHint })) {
            return;
          }
          if (isAuthCollision(linkError?.code)) {
            await fallbackSignIn();
            await linkPendingCredentialIfPresent({ silent: true });
            return;
          }
          throw linkError;
        }
      }
      try {
        await fallbackSignIn();
        await linkPendingCredentialIfPresent({ silent: true });
      } catch (fallbackError) {
        if (await recoverFromAuthCollision(fallbackError, { methodHint })) return;
        throw fallbackError;
      }
      if (email && ui.emailInput && !String(ui.emailInput.value || "").trim()) {
        ui.emailInput.value = email;
      }
    };

    ui.emailForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      try {
        await persistenceReady;
        const email = String(ui.emailInput?.value || "").trim();
        const password = String(ui.passwordInput?.value || "");
        const credential = firebase.auth.EmailAuthProvider.credential(email, password);
        await linkOrSignInWithCredential(credential, () => auth.signInWithEmailAndPassword(email, password), {
          methodHint: "password",
          email,
        });
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
        const email = String(ui.emailInput?.value || "").trim();
        const password = String(ui.passwordInput?.value || "");
        const credential = firebase.auth.EmailAuthProvider.credential(email, password);
        await linkOrSignInWithCredential(credential, () => auth.createUserWithEmailAndPassword(email, password), {
          methodHint: "password",
          email,
        });
        showToast("Account created.");
        logEvent("sign_up", { method: "password" });
      } catch (error) {
        if (ui.emailMessage) ui.emailMessage.textContent = error.message;
      }
    });

    ui.authForgotPassword?.addEventListener("click", async () => {
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      const email = String(ui.emailInput?.value || "").trim();
      if (!email) {
        const message = "Enter your email first, then click Forgot password.";
        if (ui.emailMessage) ui.emailMessage.textContent = message;
        showToast(message, "warn");
        return;
      }
      try {
        await persistenceReady;
        await auth.sendPasswordResetEmail(email);
        const message = `Password reset link sent to ${email}.`;
        if (ui.emailMessage) ui.emailMessage.textContent = message;
        showToast("Password reset email sent.");
      } catch (error) {
        if (ui.emailMessage) ui.emailMessage.textContent = error.message || "Unable to send password reset email.";
      }
    });

    const processRedirectSignInResult = async () => {
      if (isNativeApp()) return;
      if (!(isInstalledPwa() || isMobileBrowser())) return;
      try {
        await persistenceReady;
        const result = await auth.getRedirectResult();
        if (result?.user) {
          await linkPendingCredentialIfPresent({ silent: true });
          showToast("Signed in.");
          logEvent("login", { method: result.credential?.providerId || "redirect", runtime: resolveRuntimeLabel() });
        }
      } catch (error) {
        if (await recoverFromAuthCollision(error, { methodHint: "redirect", preferRuntime: resolveRuntimeLabel() })) {
          return;
        }
        if (ui.emailMessage) ui.emailMessage.textContent = error.message || "Redirect sign-in failed.";
        showToast(error.message || "Redirect sign-in failed.", "warn");
      }
    };

    const signInWithProvider = async (provider, successMessage, method) => {
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      if (state.authInFlight) return;
      state.authInFlight = true;
      const runtime = resolveRuntimeLabel();
      const normalizedMethod = String(method || "").trim().toLowerCase();
      const supportsNativeBridge = new Set([
        "google",
        "apple",
        "github",
        "twitter",
        "x",
        "yahoo",
        "microsoft",
      ]).has(normalizedMethod);
      try {
        await persistenceReady;
        if (isNativeApp() && supportsNativeBridge) {
          const bridge = installNativeAuthBridge(auth);
          const requested = Boolean(bridge?.requestSignIn?.(normalizedMethod));
          if (!requested) throw new Error("Native sign-in bridge is unavailable.");
          await waitForNativeAuthCompletion(auth, 90000);
          await linkPendingCredentialIfPresent({ silent: true });
          showToast(successMessage);
          logEvent("login", { method: normalizedMethod, runtime, source: "native_bridge" });
          return;
        }

        if (isInstalledPwa() || isMobileBrowser()) {
          if (auth.currentUser?.isAnonymous) {
            await auth.currentUser.linkWithRedirect(provider);
          } else {
            await auth.signInWithRedirect(provider);
          }
          logEvent("login_redirect_started", { method: normalizedMethod, runtime });
          return;
        }

        if (auth.currentUser?.isAnonymous) {
          await auth.currentUser.linkWithPopup(provider);
        } else {
          await auth.signInWithPopup(provider);
        }
        await linkPendingCredentialIfPresent({ silent: true });
        showToast(successMessage);
        logEvent("login", { method: normalizedMethod, runtime, source: "popup" });
      } catch (error) {
        if (await recoverFromAuthCollision(error, { methodHint: normalizedMethod, preferRuntime: runtime })) {
          return;
        }
        const message = error?.message || "Unable to sign in.";
        if (ui.emailMessage) ui.emailMessage.textContent = message;
        showToast(message, "warn");
        logEvent("login_error", { method: normalizedMethod, runtime });
      } finally {
        state.authInFlight = false;
      }
    };

    processRedirectSignInResult();

    ui.googleSignin?.addEventListener("click", async () => {
      await signInWithProvider(new firebase.auth.GoogleAuthProvider(), "Signed in with Google.", "google");
    });

    ui.facebookSignin?.addEventListener("click", async () => {
      const provider = new firebase.auth.FacebookAuthProvider();
      provider.addScope("email");
      await signInWithProvider(provider, "Signed in with Facebook.", "facebook");
    });

    ui.githubSignin?.addEventListener("click", async () => {
      const provider = new firebase.auth.GithubAuthProvider();
      provider.addScope("user:email");
      await signInWithProvider(provider, "Signed in with GitHub.", "github");
    });

    ui.twitterSignin?.addEventListener("click", async () => {
      const provider = new firebase.auth.TwitterAuthProvider();
      await signInWithProvider(provider, "Signed in with X.", "twitter");
    });

    ui.microsoftSignin?.addEventListener("click", async () => {
      const provider = new firebase.auth.OAuthProvider("microsoft.com");
      provider.addScope("user.read");
      await signInWithProvider(provider, "Signed in with Microsoft.", "microsoft");
    });

    ui.yahooSignin?.addEventListener("click", async () => {
      const provider = new firebase.auth.OAuthProvider("yahoo.com");
      provider.addScope("profile");
      provider.addScope("email");
      await signInWithProvider(provider, "Signed in with Yahoo.", "yahoo");
    });

		    ui.purchasePanels.forEach((panel) => {
	      const purchaseBtn = panel.querySelector('[data-action="purchase"]');
	      const stripeBtn = panel.querySelector('[data-action="stripe"]');
	      purchaseBtn?.addEventListener("click", () => handlePurchase(panel, functions));
	      stripeBtn?.addEventListener("click", () => handleStripeCheckout(panel, functions));
	    });

    if (ui.billingPortalLink && ui.billingPortalLink.dataset.bound !== "1") {
      ui.billingPortalLink.addEventListener("click", (event) => handleBillingPortalOpen(event, functions));
      ui.billingPortalLink.dataset.bound = "1";
    }

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
      state.tickerContext.interval = initialInterval;
      syncTickerInputs(initialTicker, { source: "initial_ticker" });
      bindTickerInputSync();
      renderTickerHistory();

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
	        syncTickerInputs(ticker, { source: "terminal_submit", emitAnalytics: true });
	        state.tickerContext.interval = interval;
	        state.tickerContext.forecastDoc = null;
	        state.tickerContext.forecastId = "";
        state.tickerContext.forecastAiSummary = null;
        state.tickerContext.forecastCacheMeta = null;
	        setTerminalStatus("Loading price history...");
        try {
          const rows = await loadTickerHistory(functions, ticker, interval);
          state.tickerContext.rows = rows;
	          await renderTickerChart(rows, ticker, interval, state.tickerContext.indicatorOverlays || []);
	          setTerminalStatus("Loaded.");
	          logEvent("terminal_load", { ticker, interval });
	          scheduleSideDataRefresh(ticker, { force: true });
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
	            scheduleSideDataRefresh(initialTicker, { force: true });
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
        category: formData.get("category"),
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
          let sessionUser = null;
          try {
            sessionUser = await ensureSessionUser({
              reason: "forecast_requires_session",
              message: "Sign in to sync forecast history across devices.",
            });
          } catch (error) {
            showToast(error?.message || "Unable to start guest session.", "warn");
            return;
          }
          const rewardApproved = await maybeShowNativeRewardGate({
            reason: "forecast",
            title: "Watch a rewarded ad to unlock forecast output?",
            message: "Forecast generation in native can require a rewarded video before running.",
          });
          if (!rewardApproved) return;
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
          const ticker = normalizeTicker(formData.get("ticker") || state.tickerContext.ticker || ui.terminalTicker?.value || "");
          if (!ticker) {
            showToast("Enter a ticker to forecast.", "warn");
            return;
          }
          syncTickerInputs(ticker, { source: "forecast_form" });
		      const payload = {
		        ticker,
	        horizon: Number(formData.get("horizon")),
	        interval: formData.get("interval"),
	        service: "prophet",
	        quantiles,
          workspaceId: state.activeWorkspaceId || sessionUser?.uid || state.user?.uid || "",
	        meta: buildMeta(),
	        utm: getUtm(),
		      };
          payload.start = (() => {
            const desiredInterval = String(payload.interval || state.tickerContext.interval || "1d");
            if (
              Array.isArray(state.tickerContext.rows) &&
              state.tickerContext.rows.length &&
              getActiveTicker() === ticker &&
              state.tickerContext.interval === desiredInterval
            ) {
              const dateKey = extractDateKey(state.tickerContext.rows);
              const first = dateKey ? state.tickerContext.rows[0]?.[dateKey] : null;
              if (typeof first === "string") {
                const match = first.match(/^(\\d{4}-\\d{2}-\\d{2})/);
                if (match) return match[1];
              }
              if (first) {
                const dt = new Date(first);
                if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
              }
            }
            return computeHistoryStart(desiredInterval);
          })();

			      try {
			        setOutputLoading(ui.forecastOutput, "Generating forecast...");
              state.tickerContext.forecastAiSummary = {
                requestId: "",
                loading: false,
                text: "",
                provider: "",
                model: "",
                latencyMs: null,
                usage: {},
                responseId: "",
                shareUrl: "",
                feedback: "",
                error: "",
              };
			        const runForecast = functions.httpsCallable("run_timeseries_forecast");
			        const result = await runForecast(payload);
			        const data = result.data || {};
			        const requestId = String(data.requestId || "").trim();
			        if (!requestId) {
			          throw new Error("Forecast run did not return a request ID.");
			        }
              const responseRows = normalizeForecastSeriesRows(data.forecastSeries || data.forecastRows || []);
              const historyRows =
                Array.isArray(state.tickerContext.rows) &&
                state.tickerContext.rows.length &&
                normalizeTicker(state.tickerContext.ticker || "") === ticker &&
                String(state.tickerContext.interval || "1d") === String(payload.interval || "1d")
                  ? state.tickerContext.rows
                  : await loadTickerHistory(functions, ticker, String(payload.interval || "1d"));
              const alignedRows = alignForecastRowsWithHistory({
                forecastRows: responseRows,
                historyRows,
                interval: String(payload.interval || "1d"),
                horizon: Number(payload.horizon || 0) || responseRows.length,
              });
              const cacheMeta = await saveForecastSeriesToClientCache({
                requestId,
                ticker,
                interval: String(payload.interval || "1d"),
                horizon: Number(payload.horizon || 0),
                service: String(payload.service || "prophet"),
                quantiles: Array.isArray(payload.quantiles) ? payload.quantiles : [],
                start: String(payload.start || ""),
                forecastRows: alignedRows,
                historicalRows: historyRows,
                chartConfig: {
                  quantileKeys: extractQuantileKeys(alignedRows),
                  interval: String(payload.interval || "1d"),
                },
                metrics: data.metrics && typeof data.metrics === "object" ? data.metrics : {},
              });
              state.tickerContext.forecastCacheMeta = cacheMeta;
              const localKeyLevels = extractForecastKeyLevels(alignedRows);

              const forecastDocLocal = {
                id: requestId,
                ticker,
                interval: String(payload.interval || "1d"),
                horizon: Number(payload.horizon || 0) || alignedRows.length,
                start: String(payload.start || ""),
                quantiles: Array.isArray(payload.quantiles) ? payload.quantiles : [],
                service: String(payload.service || "prophet"),
                engine: String(data.engine || ""),
                status: String(data.status || "completed"),
                serviceMessage: String(data.serviceMessage || "").trim(),
                metrics:
                  data.metrics && typeof data.metrics === "object"
                    ? data.metrics
                    : {
                        lastClose: data.lastClose,
                        mae: data.mae,
                        coverage10_90: data.coverage10_90,
                        medianEnd: localKeyLevels.median,
                      },
                forecastPreview: Array.isArray(data.forecastPreview) ? data.forecastPreview : alignedRows.slice(0, 12),
                forecastRows: alignedRows,
                forecastQuantilesEnd: data.forecastQuantilesEnd && typeof data.forecastQuantilesEnd === "object" ? data.forecastQuantilesEnd : {},
                tradeRationale: String(data.tradeRationale || "").trim(),
                chartSeriesSource: "client_cache",
                chartCacheKey: String(cacheMeta?.cacheKey || "").trim(),
              };

				        logEvent("forecast_request", { ticker: payload.ticker, interval: payload.interval, service: payload.service });
				        showToast("Forecast saved.");
	            recordPromoForecastUsage();
	            upsertMyRequest({
              type: "forecast",
              requestId: `forecast__${requestId}`,
              title: `${normalizeTicker(payload.ticker || "") || "Ticker"} forecast`,
              input: {
                ticker: normalizeTicker(payload.ticker || ""),
                interval: String(payload.interval || "1d"),
                horizon: Number(payload.horizon || 0) || null,
                service: String(payload.service || ""),
                quantiles: Array.isArray(payload.quantiles) ? payload.quantiles : [],
              },
	              outputsMeta: {
	                summary: String(data?.serviceMessage || "").trim(),
	                service: String(payload.service || ""),
	                interval: String(payload.interval || ""),
                  chartStorage: "client_only",
                  chartCacheKey: String(cacheMeta?.cacheKey || "").trim(),
                  chartParamsHash: String(cacheMeta?.paramsHash || "").trim(),
                  chartRows: Number(cacheMeta?.rowCount || 0) || 0,
	              },
	              sourceRef: {
	                collection: "forecast_requests",
	                id: requestId,
	              },
	            }).catch(() => {});

				        try {
			          if (ui.tickerChart) {
			            setTerminalStatus("Loading forecast for chart...");
			            await plotForecastById(db, functions, requestId, { preloadedDoc: forecastDocLocal });
			            document.getElementById("terminal")?.scrollIntoView({ behavior: "smooth" });
			          } else {
			            state.tickerContext.forecastDoc = forecastDocLocal;
			            state.tickerContext.forecastId = requestId;
			            state.tickerContext.forecastTablePage = 0;
                  state.tickerContext.rows = Array.isArray(historyRows) ? historyRows : [];
                  state.tickerContext.interval = String(payload.interval || "1d");
			            renderForecastDetails(forecastDocLocal);
			          }
                runForecastAutoSummary({ forecastDoc: forecastDocLocal, requestId, notify: false }).catch(() => {});
				        } catch (plotError) {
                runForecastAutoSummary({ forecastDoc: forecastDocLocal, requestId, notify: false }).catch(() => {});
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

	        ui.forecastLoadButton?.addEventListener("click", async () => {
          try {
            await ensureSessionUser({
              reason: "forecast_saved_load_requires_session",
              message: "Sign in to sync saved forecast runs.",
            });
          } catch (error) {
            showToast(error?.message || "Unable to start guest session.", "warn");
            return;
          }
          const forecastId = String(ui.forecastLoadSelect?.value || "").trim();
          if (!forecastId) {
            showToast("Select a saved run.", "warn");
            return;
          }
          if (ui.forecastLoadStatus) ui.forecastLoadStatus.textContent = "Loading...";
          try {
            setTerminalStatus("Loading saved run...");
            await plotForecastById(db, functions, forecastId);
            if (ui.forecastLoadStatus) ui.forecastLoadStatus.textContent = "";
            showToast("Forecast loaded.");
            logEvent("forecast_loaded_saved", { forecast_id: forecastId });
            document.getElementById("terminal")?.scrollIntoView({ behavior: "smooth" });
          } catch (error) {
            if (ui.forecastLoadStatus) ui.forecastLoadStatus.textContent = error.message || "Unable to load forecast.";
            showToast(error.message || "Unable to load forecast.", "warn");
	          }
	        });

        if (ui.forecastOutput && ui.forecastOutput.dataset.bound !== "1") {
          ui.forecastOutput.addEventListener("click", async (event) => {
            const likeBtn = event.target.closest('[data-action="forecast-ai-like"]');
            if (likeBtn) {
              event.preventDefault();
              const forecastId = String(likeBtn.dataset.forecastId || state.tickerContext.forecastId || "").trim();
              if (!forecastId) return;
              const summary = state.tickerContext.forecastAiSummary || {};
              state.tickerContext.forecastAiSummary = { ...summary, requestId: forecastId, feedback: "like" };
              const responseId = String(summary.responseId || "").trim();
              if (responseId) {
                await submitModelCouncilFeedback({ responseId, action: "like" });
              }
              if (state.tickerContext.forecastDoc) renderForecastDetails(state.tickerContext.forecastDoc);
              showToast("Feedback saved.");
              return;
            }

            const dislikeBtn = event.target.closest('[data-action="forecast-ai-dislike"]');
            if (dislikeBtn) {
              event.preventDefault();
              const forecastId = String(dislikeBtn.dataset.forecastId || state.tickerContext.forecastId || "").trim();
              if (!forecastId) return;
              const summary = state.tickerContext.forecastAiSummary || {};
              state.tickerContext.forecastAiSummary = { ...summary, requestId: forecastId, feedback: "dislike" };
              const responseId = String(summary.responseId || "").trim();
              if (responseId) {
                await submitModelCouncilFeedback({ responseId, action: "dislike" });
              }
              if (state.tickerContext.forecastDoc) renderForecastDetails(state.tickerContext.forecastDoc);
              showToast("Feedback saved.");
              return;
            }

            const shareBtn = event.target.closest('[data-action="forecast-ai-share"]');
            if (shareBtn) {
              event.preventDefault();
              const forecastId = String(shareBtn.dataset.forecastId || state.tickerContext.forecastId || "").trim();
              if (!forecastId) return;
              shareBtn.disabled = true;
              try {
                const summary = state.tickerContext.forecastAiSummary || {};
                const responseId = String(shareBtn.dataset.responseId || summary.responseId || "").trim();
                let shareUrl = String(summary.shareUrl || "").trim();
                if (!shareUrl && responseId) {
                  try {
                    shareUrl = await createModelCouncilShareLink(responseId);
                    await submitModelCouncilFeedback({ responseId, action: "share" });
                  } catch (error) {
                    shareUrl = "";
                  }
                }
                if (!shareUrl && hasFullAccount()) {
                  const requestDocId = `forecast__${forecastId}`;
                  try {
                    const body = await updateMyRequest(
                      requestDocId,
                      { visibility: "unlisted" },
                      { method: "POST", path: `/api/my-requests/${encodeURIComponent(requestDocId)}/share` }
                    );
                    shareUrl = String(body?.share?.shareUrl || "").trim();
                  } catch (error) {
                    shareUrl = "";
                  }
                }
                if (!shareUrl) {
                  shareUrl = `${window.location.origin}/forecasting?forecastId=${encodeURIComponent(forecastId)}`;
                }
                state.tickerContext.forecastAiSummary = {
                  ...summary,
                  requestId: forecastId,
                  shareUrl,
                  responseId: responseId || String(summary.responseId || "").trim(),
                };
                if (state.tickerContext.forecastDoc) renderForecastDetails(state.tickerContext.forecastDoc);
                await performShare({
                  url: shareUrl,
                  title: "Quantura forecast summary",
                  text: String(summary.text || "Forecast narrative generated by Model Council.").slice(0, 220),
                });
                showToast("Share link copied.");
              } catch (error) {
                showToast(error.message || "Unable to share forecast summary.", "warn");
              } finally {
                shareBtn.disabled = false;
              }
            }
          });
          ui.forecastOutput.dataset.bound = "1";
        }

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

        const rewardApproved = await maybeShowNativeRewardGate({
          reason: "indicator_llm",
          title: "Watch a rewarded ad to unlock indicator AI output?",
          message: "Indicator calculations and AI narrative can require a rewarded video in native apps.",
        });
        if (!rewardApproved) return;

	      try {
          const activeTicker = normalizeTicker(payload.ticker || state.tickerContext.ticker || "");
          if (activeTicker) {
            payload.ticker = activeTicker;
            syncTickerInputs(activeTicker, { source: "technicals_form" });
          }
	        setOutputLoading(ui.technicalsOutput, "Computing indicators...");
          const headers = await buildApiAuthHeaders({ includeJson: true });
          const response = await fetch("/api/indicators/analyze", {
            method: "POST",
            headers,
            body: JSON.stringify(payload),
          });
          const data = await response.json().catch(() => ({}));
          if (!response.ok) {
            const detail = String(data?.detail || data?.error || `HTTP ${response.status}`).trim();
            throw new Error(detail || "indicator_analysis_failed");
          }
	        const rows = data.latest || [];
          const analysis = data.analysis && typeof data.analysis === "object" ? data.analysis : {};
          const prediction = analysis.prediction && typeof analysis.prediction === "object" ? analysis.prediction : {};
	        if (ui.technicalsOutput) {
	          setOutputReady(ui.technicalsOutput);
		          if (!rows.length) {
		            ui.technicalsOutput.textContent = "No indicator data returned.";
		          } else {
                const targetPrice = Number(prediction.targetPrice);
                const lastClose = Number(data?.meta?.lastClose);
                const upsidePct =
                  Number.isFinite(targetPrice) && Number.isFinite(lastClose) && lastClose > 0
                    ? ((targetPrice - lastClose) / lastClose) * 100
                    : null;
                const keySignals = Array.isArray(analysis.keySignals) ? analysis.keySignals.slice(0, 6) : [];
                const summaryText = escapeHtml(String(analysis.summary || "").trim());
                const narrativeText = escapeHtml(String(analysis.text || "").trim());
		            ui.technicalsOutput.innerHTML = `
                  <div class="table-wrap">
		                <table class="data-table">
	                    <thead><tr><th>Indicator</th><th>Value</th></tr></thead>
	                    <tbody>
	                      ${rows
                          .map((row) => {
                            const name = escapeHtml(String(row?.name || "").trim());
                            const value = escapeHtml(String(row?.display ?? row?.value ?? "—").trim() || "—");
                            return `<tr><td>${name}</td><td>${value}</td></tr>`;
                          })
                          .join("")}
	                    </tbody>
	                  </table>
                  </div>
                  ${
                    summaryText || narrativeText
                      ? `<div class="results-panel" style="margin-top: 12px;">
                          <h3>AI indicator analysis</h3>
                          <div class="small muted">Provider: ${escapeHtml(String(analysis.provider || "openai"))} · Model: ${escapeHtml(
                          String(analysis.model || "")
                        )}</div>
                          ${summaryText ? `<div class="small" style="margin-top:8px;">${summaryText}</div>` : ""}
                          <div class="form-grid" style="margin-top:10px;">
                            <div class="profile-item"><span class="label">Direction</span><span class="value">${escapeHtml(
                              String(prediction.direction || "neutral")
                            )}</span></div>
                            <div class="profile-item"><span class="label">Target price</span><span class="value">${
                              Number.isFinite(targetPrice) ? formatUsd(targetPrice, 2) : "—"
                            }</span></div>
                            <div class="profile-item"><span class="label">Timeline</span><span class="value">${escapeHtml(
                              String(prediction.timeline || `${prediction.timelineDays || "—"} trading days`)
                            )}</span></div>
                            <div class="profile-item"><span class="label">Confidence</span><span class="value">${escapeHtml(
                              String(prediction.confidence || "medium")
                            )}</span></div>
                            <div class="profile-item"><span class="label">Implied move</span><span class="value">${
                              Number.isFinite(upsidePct) ? formatPercent(upsidePct, { signed: true, digits: 2 }) : "—"
                            }</span></div>
                          </div>
                          ${
                            keySignals.length
                              ? `<ul class="small" style="margin:10px 0 0 16px;">${keySignals
                                  .map((signal) => `<li>${escapeHtml(String(signal || "").trim())}</li>`)
                                  .join("")}</ul>`
                              : ""
                          }
                          ${narrativeText ? `<div class="small" style="margin-top:10px; white-space:pre-wrap;">${narrativeText}</div>` : ""}
                          <p class="small muted solve-now-disclaimer" style="margin-top:10px;">${escapeHtml(
                            String(analysis.disclaimer || MODEL_COUNCIL_OUTPUT_DISCLAIMER)
                          )}</p>
                        </div>`
                      : ""
                  }
	            `;
	          }
	        }

        if (includeSeries && data.series) {
          await renderIndicatorChart(data.series);
          state.tickerContext.indicatorOverlays = buildIndicatorOverlays(data.series);
          const ticker = normalizeTicker(payload.ticker);
          if (ticker && ui.tickerChart && state.tickerContext.rows.length && getActiveTicker() === ticker) {
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
        upsertMyRequest({
          type: "indicator",
          title: `${normalizeTicker(payload.ticker || "") || "Ticker"} indicators`,
          input: {
            ticker: normalizeTicker(payload.ticker || ""),
            interval: String(payload.interval || "1d"),
            lookback: Number(payload.lookback || 0) || null,
            indicators: Array.isArray(indicators) ? indicators.map((entry) => String(entry || "").trim().toUpperCase()) : [],
          },
          outputsMeta: {
            summary:
              (() => {
                const narrative = String(analysis?.summary || "").trim();
                if (narrative) return narrative.slice(0, 320);
                if (Array.isArray(rows) && rows.length) {
                  return rows
                    .slice(0, 4)
                    .map((row) => `${row?.name}: ${row?.display ?? row?.value}`)
                    .join(" • ");
                }
                return "Indicator request saved.";
              })(),
            latestCount: Array.isArray(rows) ? rows.length : 0,
            prediction: prediction?.direction ? String(prediction.direction) : "",
          },
          sourceRef: {
            collection: "indicator_requests",
            id: `ind_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
          },
        }).catch(() => {});
	      } catch (error) {
	        showToast(error.message || "Unable to run indicators.", "warn");
	      }
	    });

    ui.downloadForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        await ensureSessionUser({
          reason: "history_download_requires_session",
          message: "Sign in to sync download history.",
        });
      } catch (error) {
        showToast(error?.message || "Unable to start guest session.", "warn");
        return;
      }
      const rewardApproved = await maybeShowNativeRewardGate({
        reason: "history_download",
        title: "Watch a rewarded ad to unlock download output?",
        message: "Historical download in native can require a rewarded video before exporting CSV.",
      });
      if (!rewardApproved) return;
      const formData = new FormData(ui.downloadForm);
      const ticker =
        normalizeTicker(formData.get("ticker")) || state.tickerContext.ticker || safeLocalStorageGet(LAST_TICKER_KEY) || "";
      if (!ticker) {
        showToast("Load a ticker first.", "warn");
        return;
      }
      const interval = String(formData.get("interval") || "1d");
      const today = new Date();
      const end = String(formData.get("end") || "").trim() || today.toISOString().slice(0, 10);
      const start =
        interval === "1h"
          ? (() => {
              const dt = new Date();
              dt.setDate(dt.getDate() - 729);
              return dt.toISOString().slice(0, 10);
            })()
          : "1900-01-01";
      const payload = {
        ticker,
        start,
        end,
        interval,
        meta: buildMeta(),
      };

      try {
        ui.downloadStatus.textContent = "Fetching data...";
        if (ui.downloadPreview) {
          ui.downloadPreview.innerHTML = `<div class="small muted">Preparing preview...</div>`;
        }
        const getDownload = functions.httpsCallable("download_price_csv");
        const result = await getDownload(payload);
        const data = result.data || {};
        const csvText = String(data.csv || "");
        if (!csvText.trim()) {
          ui.downloadStatus.textContent = "No data returned.";
          if (ui.downloadPreview) {
            ui.downloadPreview.innerHTML = `<div class="small muted">No history rows returned for ${escapeHtml(ticker)}.</div>`;
          }
          return;
        }
        const filename = String(data.filename || `${ticker}_${start}_${end}.csv`);
        renderDownloadHistoryPreview(csvText, { ticker });
        triggerDownload(filename, csvText);
        const rowCount = Number(data.rowCount || 0);
        ui.downloadStatus.textContent = rowCount ? `Download ready (${rowCount} rows).` : "Download ready.";
        logEvent("download_history", { ticker, interval });
      } catch (error) {
        ui.downloadStatus.textContent = "Download failed.";
        showToast(error.message || "Unable to fetch history.", "warn");
      }
    });

		    ui.trendingButton?.addEventListener("click", async () => {
		      await loadTrendingTickers(functions, { notify: true, force: true });
		    });

    if (ui.eventsCalendarPreset && !String(ui.eventsCalendarPreset.value || "").trim()) {
      ui.eventsCalendarPreset.value = "this-week";
    }
    setEarningsCalendarPreset(ui.eventsCalendarPreset?.value || "this-week");
    ui.eventsCalendarPreset?.addEventListener("change", async () => {
      setEarningsCalendarPreset(ui.eventsCalendarPreset?.value || "this-week");
      await loadEarningsCalendar({ force: false, notify: false });
    });
    ui.eventsCalendarPrev?.addEventListener("click", async () => {
      shiftEarningsCalendarRange(-1);
      await loadEarningsCalendar({ force: false, notify: false });
    });
    ui.eventsCalendarNext?.addEventListener("click", async () => {
      shiftEarningsCalendarRange(1);
      await loadEarningsCalendar({ force: false, notify: false });
    });
    ui.eventsCalendarSearch?.addEventListener("input", () => {
      state.earningsCalendar.search = String(ui.eventsCalendarSearch?.value || "").trim();
      state.earningsCalendar.pageByDate = {};
      renderEarningsCalendar();
    });
    ui.eventsCalendarSearchClear?.addEventListener("click", () => {
      if (ui.eventsCalendarSearch) ui.eventsCalendarSearch.value = "";
      state.earningsCalendar.search = "";
      state.earningsCalendar.pageByDate = {};
      renderEarningsCalendar();
    });
    ui.eventsCalendarDayStrip?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-earnings-date]");
      if (!button) return;
      const date = String(button.getAttribute("data-earnings-date") || "").trim();
      if (!date) return;
      state.earningsCalendar.selectedDate = date;
      state.earningsCalendar.pageByDate[date] = 1;
      renderEarningsCalendar();
    });
    ui.eventsCalendarOutput?.addEventListener("click", async (event) => {
      const followButton = event.target.closest("[data-earnings-follow]");
      if (followButton) {
        const symbol = String(followButton.getAttribute("data-earnings-follow") || "").trim();
        if (!symbol) return;
        await toggleEarningsFollow(symbol);
        return;
      }
      const loadMoreButton = event.target.closest("[data-earnings-load-more]");
      if (loadMoreButton) {
        const date = String(loadMoreButton.getAttribute("data-earnings-load-more") || "").trim();
        if (!date) return;
        const current = Math.max(1, Number(state.earningsCalendar.pageByDate?.[date] || 1));
        state.earningsCalendar.pageByDate[date] = current + 1;
        renderEarningsCalendar();
      }
    });

    if (ui.terminalFxBase && !String(ui.terminalFxBase.value || "").trim()) ui.terminalFxBase.value = "USD";
    if (ui.terminalFxQuote && !String(ui.terminalFxQuote.value || "").trim()) ui.terminalFxQuote.value = "EUR";
    if (ui.terminalFxAmount && !String(ui.terminalFxAmount.value || "").trim()) ui.terminalFxAmount.value = "1";
    renderTerminalFxRecent();
    ui.terminalFxSwap?.addEventListener("click", () => {
      const currentBase = normalizeFxCode(ui.terminalFxBase?.value || "USD");
      const currentQuote = normalizeFxCode(ui.terminalFxQuote?.value || "EUR");
      if (ui.terminalFxBase) ui.terminalFxBase.value = currentQuote;
      if (ui.terminalFxQuote) ui.terminalFxQuote.value = currentBase;
    });
    ui.terminalFxForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      await runTerminalFxConvert();
    });
    ui.terminalFxRecent?.addEventListener("click", async (event) => {
      const button = event.target.closest("[data-terminal-fx-recent-index]");
      if (!button) return;
      const idx = Number(button.getAttribute("data-terminal-fx-recent-index"));
      const record = readTerminalFxRecent()[idx];
      if (!record) return;
      if (ui.terminalFxAmount) ui.terminalFxAmount.value = String(record.amountIn || 1);
      if (ui.terminalFxBase) ui.terminalFxBase.value = normalizeFxCode(record.base || "USD");
      if (ui.terminalFxQuote) ui.terminalFxQuote.value = normalizeFxCode(record.quote || "EUR");
      await runTerminalFxConvert();
    });

    if (ui.marketHeadlinesCountry && !ui.marketHeadlinesCountry.value) {
      ui.marketHeadlinesCountry.value = state.preferredCountry || "US";
    }
    ui.marketHeadlinesForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      await loadMarketHeadlinesFeed(functions, { force: true, notify: true });
    });

    if (ui.macroDashboardStatus) {
      loadFiscalMacroDashboard({ force: false }).catch((error) => {
        ui.macroDashboardStatus.textContent = extractErrorMessage(error, "Macro dashboard is unavailable.");
      });
    }
    ui.macroDashboardGroups?.addEventListener("click", async (event) => {
      const loadMoreButton = event.target.closest("[data-fiscaldata-load-more]");
      const detailsButton = event.target.closest("[data-fiscaldata-view-details]");
      if (loadMoreButton) {
        const cardId = String(loadMoreButton.getAttribute("data-fiscaldata-load-more") || "").trim();
        const entry = Array.isArray(state.fiscaldataRegistry)
          ? state.fiscaldataRegistry.find((item) => String(item?.id || "").trim() === cardId)
          : null;
        if (!entry) return;
      const cardState = state.fiscaldataMacroPages?.[cardId] || {};
      const nextPage = Number(cardState?.nextPageNumber || Number(cardState?.pageNumber || 1) + 1);
      await loadFiscalMacroCard(entry, { pageNumber: nextPage, append: true });
      renderFiscalMacroDashboard();
      return;
      }
      if (detailsButton) {
        const cardId = String(detailsButton.getAttribute("data-fiscaldata-view-details") || "").trim();
        const entry = Array.isArray(state.fiscaldataRegistry)
          ? state.fiscaldataRegistry.find((item) => String(item?.id || "").trim() === cardId)
          : null;
        if (!entry) return;
        renderFiscalMacroDetailsModal(entry, state.fiscaldataMacroPages?.[cardId] || {});
      }
    });
    document.addEventListener("click", (event) => {
      const closeButton = event.target.closest("[data-fiscaldata-close]");
      if (!closeButton) return;
      const modal = document.getElementById("fiscaldata-macro-details");
      modal?.classList.add("hidden");
    });

    if (ui.tickerQueryLanguage && !ui.tickerQueryLanguage.value) {
      ui.tickerQueryLanguage.value = state.preferredLanguage || "en";
    }
    renderTickerQueryModulePicker();
    renderModelCouncilPromptCards({ reshuffle: true });
    if (ui.tickerQueryPromptShuffle && ui.tickerQueryPromptShuffle.dataset.bound !== "1") {
      ui.tickerQueryPromptShuffle.addEventListener("click", () => {
        state.tickerContext.tickerQueryPromptDeck = createShuffledModelCouncilPromptDeck();
        state.tickerContext.tickerQueryPromptCursor = 0;
        renderModelCouncilPromptCards();
      });
      ui.tickerQueryPromptShuffle.dataset.bound = "1";
    }
    if (ui.tickerQueryPromptCards && ui.tickerQueryPromptCards.dataset.bound !== "1") {
      ui.tickerQueryPromptCards.addEventListener("click", (event) => {
        const card = event.target.closest('[data-action="model-council-prompt"]');
        if (!card) return;
        const template = String(card.dataset.template || "").trim();
        if (!template) return;
        const prompt = materializeModelCouncilPrompt(template, ui.tickerQueryTicker?.value || state.tickerContext.ticker || "");
        if (ui.tickerQueryQuestion) {
          ui.tickerQueryQuestion.value = prompt;
          ui.tickerQueryQuestion.focus();
          const len = ui.tickerQueryQuestion.value.length;
          if (typeof ui.tickerQueryQuestion.setSelectionRange === "function") {
            ui.tickerQueryQuestion.setSelectionRange(len, len);
          }
        }
      });
      ui.tickerQueryPromptCards.dataset.bound = "1";
    }
    if (ui.tickerQueryTicker && ui.tickerQueryTicker.dataset.promptBound !== "1") {
      ui.tickerQueryTicker.addEventListener("input", () => {
        renderModelCouncilPromptCards();
      });
      ui.tickerQueryTicker.dataset.promptBound = "1";
    }
    const improvePref = String(safeLocalStorageGet(TICKER_QUERY_IMPROVE_TOGGLE_KEY) || "1");
    if (ui.tickerQueryImproveToggle) {
      ui.tickerQueryImproveToggle.checked = improvePref !== "0";
      if (ui.tickerQueryImproveToggle.dataset.bound !== "1") {
        ui.tickerQueryImproveToggle.addEventListener("change", () => {
          const active = Boolean(ui.tickerQueryImproveToggle?.checked);
          safeLocalStorageSet(TICKER_QUERY_IMPROVE_TOGGLE_KEY, active ? "1" : "0");
          if (!active && ui.tickerQueryImprovePreviewWrap) {
            ui.tickerQueryImprovePreviewWrap.classList.add("hidden");
          }
        });
        ui.tickerQueryImproveToggle.dataset.bound = "1";
      }
    }
    if (ui.tickerQueryProvider && ui.tickerQueryProvider.dataset.bound !== "1") {
      ui.tickerQueryProvider.addEventListener("change", () => {
        const providerId = normalizeModelCouncilProviderId(ui.tickerQueryProvider?.value || "openai");
        state.tickerContext.tickerQueryProvider = providerId;
        safeLocalStorageSet(TICKER_QUERY_PROVIDER_KEY, providerId);
        renderTickerQueryModels(state.tickerContext.tickerQueryModels || [], { provider: providerId });
      });
      ui.tickerQueryProvider.dataset.bound = "1";
    }
    if (ui.tickerQueryModel && ui.tickerQueryModel.dataset.bound !== "1") {
      ui.tickerQueryModel.addEventListener("change", () => {
        const selectedOption = ui.tickerQueryModel?.selectedOptions?.[0];
        const providerId = normalizeModelCouncilProviderId(selectedOption?.dataset?.provider || ui.tickerQueryProvider?.value || "openai");
        applyTickerQueryModelSelection(ui.tickerQueryModel?.value || "gpt-5-mini", providerId);
      });
      ui.tickerQueryModel.dataset.bound = "1";
    }
    if (ui.tickerQueryCacheToggleWrap) {
      const host = (typeof window !== "undefined" && window.location && window.location.hostname) ? window.location.hostname : "";
      const isDevHost = host === "localhost" || host === "127.0.0.1" || host.endsWith(".localhost");
      ui.tickerQueryCacheToggleWrap.classList.toggle("hidden", !isDevHost);
    }
    if (ui.tickerQueryShowCacheStats && ui.tickerQueryShowCacheStats.dataset.bound !== "1") {
      ui.tickerQueryShowCacheStats.addEventListener("change", () => {
        const showCacheStats = Boolean(ui.tickerQueryShowCacheStats?.checked);
        if (showCacheStats) {
          renderTickerQueryCacheStats(null, { visible: true });
          return;
        }
        renderTickerQueryCacheStats(null, { visible: false });
      });
      ui.tickerQueryShowCacheStats.dataset.bound = "1";
    }
    updateTickerQueryModelInfo({});
    ui.tickerQueryForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      await loadTickerQueryInsight(functions, {
        ticker: ui.tickerQueryTicker?.value || state.tickerContext.ticker || "",
        question: ui.tickerQueryQuestion?.value || "",
        notify: true,
      });
    });
    if (ui.tickerQueryRunFinal && ui.tickerQueryRunFinal.dataset.bound !== "1") {
      ui.tickerQueryRunFinal.addEventListener("click", async () => {
        await loadTickerQueryInsight(functions, {
          ticker: ui.tickerQueryTicker?.value || state.tickerContext.ticker || "",
          question: ui.tickerQueryImprovePreview?.value || ui.tickerQueryQuestion?.value || "",
          notify: true,
          skipImprove: true,
        });
      });
      ui.tickerQueryRunFinal.dataset.bound = "1";
    }
    if (ui.tickerQueryOutput && ui.tickerQueryOutput.dataset.bound !== "1") {
      ui.tickerQueryOutput.addEventListener("click", async (event) => {
        const likeBtn = event.target.closest('[data-action="model-council-like"]');
        if (likeBtn) {
          event.preventDefault();
          const responseId = String(likeBtn.dataset.responseId || state.tickerContext.tickerQueryLastResponseId || "").trim();
          if (!responseId) return;
          state.tickerContext.tickerQueryFeedback = "like";
          await submitModelCouncilFeedback({ responseId, action: "like" });
          renderTickerQueryResult({ ...(state.tickerContext.tickerQueryLastResponse || {}), feedback: "like" });
          showToast("Feedback saved.");
          return;
        }

        const dislikeBtn = event.target.closest('[data-action="model-council-dislike"]');
        if (dislikeBtn) {
          event.preventDefault();
          const responseId = String(dislikeBtn.dataset.responseId || state.tickerContext.tickerQueryLastResponseId || "").trim();
          if (!responseId) return;
          state.tickerContext.tickerQueryFeedback = "dislike";
          await submitModelCouncilFeedback({ responseId, action: "dislike" });
          renderTickerQueryResult({ ...(state.tickerContext.tickerQueryLastResponse || {}), feedback: "dislike" });
          showToast("Feedback saved.");
          return;
        }

        const shareBtn = event.target.closest('[data-action="model-council-share"]');
        if (shareBtn) {
          event.preventDefault();
          const responseId = String(shareBtn.dataset.responseId || state.tickerContext.tickerQueryLastResponseId || "").trim();
          if (!responseId) return;
          shareBtn.disabled = true;
          try {
            const shareUrl = await createModelCouncilShareLink(responseId);
            state.tickerContext.tickerQueryShareUrl = shareUrl;
            await submitModelCouncilFeedback({ responseId, action: "share" });
            renderTickerQueryResult({ ...(state.tickerContext.tickerQueryLastResponse || {}), shareUrl });
            await performShare({
              url: shareUrl,
              title: "Quantura Model Council",
              text: "Read-only Model Council response",
            });
            showToast("Share link copied.");
          } catch (error) {
            showToast(error.message || "Unable to create share link.", "warn");
          } finally {
            shareBtn.disabled = false;
          }
          return;
        }

        const retryBtn = event.target.closest('[data-action="model-council-retry"]');
        if (retryBtn) {
          event.preventDefault();
          await loadTickerQueryInsight(functions, {
            ticker: ui.tickerQueryTicker?.value || state.tickerContext.ticker || "",
            question: ui.tickerQueryQuestion?.value || "",
            notify: true,
            skipImprove: true,
            providerOverride: retryBtn.dataset.provider || "",
            modelOverride: retryBtn.dataset.model || "",
          });
        }
      });
      ui.tickerQueryOutput.dataset.bound = "1";
    }
	    loadTickerQueryModels().catch(() => {});
	    loadPublicModelCouncilShare({ setPanel: false }).catch(() => {});
      loadSharedMyRequestFromUrl({ setPanel: false }).catch(() => {});
	    syncModelCouncilSeo();

	    ui.optionsForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
        try {
          await ensureSessionUser({
            reason: "options_requires_session",
            message: "Sign in to sync options preferences.",
          });
        } catch (error) {
          showToast(error?.message || "Unable to start guest session.", "warn");
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
          const source = String(data.source || "yfinance").trim().toLowerCase();
          const referenceOnly = Boolean(data.referenceOnly);
          const fallbackUsed = Boolean(data.fallbackUsed);
          const notice = String(data.notice || "").trim();
	        const selectedExpiration = data.selectedExpiration || payload.expiration || "";
	        const expirations = data.expirations || [];
	        const calls = data.calls || [];
	        const puts = data.puts || [];
          const sortByStrike = (rows) => {
            if (!Array.isArray(rows)) return [];
            return rows
              .slice()
              .sort((a, b) => {
                const sa = typeof a?.strike === "number" ? a.strike : Number(a?.strike);
                const sb = typeof b?.strike === "number" ? b.strike : Number(b?.strike);
                if (!Number.isFinite(sa) && !Number.isFinite(sb)) return 0;
                if (!Number.isFinite(sa)) return 1;
                if (!Number.isFinite(sb)) return -1;
                return sa - sb;
              });
          };
          const callsSorted = sortByStrike(calls);
          const putsSorted = sortByStrike(puts);

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
                  <div class="small"><strong>Source:</strong> ${escapeHtml(source === "reference" ? "Reference contract feed" : "Yahoo Finance")}</div>
	                <div class="small"><strong>RFR:</strong> ${typeof riskFreeRate === "number" ? fmt(riskFreeRate, 3) : "—"} · <strong>T:</strong> ${
	                  typeof timeToExpiryYears === "number" ? fmt(timeToExpiryYears, 3) : "—"
	                }y</div>
	              </div>
                ${
                  source === "reference" || referenceOnly || fallbackUsed
                    ? `<div class="notice" style="margin:10px 0;">${escapeHtml(
                        notice || "Reference fallback is active. Quotes are not enabled on current plan; showing reference-only contracts."
                      )}</div>`
                    : ""
                }
	              <details class="option-block" open>
	                <summary>Calls</summary>
	                ${table(callsSorted, "Calls")}
	              </details>
	              <details class="option-block">
	                <summary>Puts</summary>
	                ${table(putsSorted, "Puts")}
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
	      try {
	        ui.optionsForm.requestSubmit?.();
	      } catch (error) {
	        // Ignore.
	      }
	    });

      ui.screenerModel?.addEventListener("change", () => {
        syncScreenerProviderAccent();
        refreshScreenerCreditsUi();
      });
      refreshScreenerModelUi();
      refreshScreenerCreditsUi();
      hydrateFundamentalFilterFields();
      bindScreenerFilterTabs();
      bindAIAgentLeaderboardControls();

	    ui.screenerForm?.addEventListener("submit", async (event) => {
	      event.preventDefault();
        let sessionUser = null;
        try {
          sessionUser = await ensureSessionUser({
            reason: "screener_requires_session",
            message: "Sign in to sync screener runs across devices.",
          });
        } catch (error) {
          showToast(error?.message || "Unable to start guest session.", "warn");
          return;
        }
	      const formData = new FormData(ui.screenerForm);
      const requestedNames = Number(formData.get("maxNames"));
      const boundedNames = Number.isFinite(requestedNames) ? Math.max(5, Math.min(25, requestedNames)) : 10;
      const minCapBucket = String(formData.get("minCapBucket") || "any").trim().toLowerCase();
      const minCapAbs = minCapBucket === "any" ? null : Number(minCapBucket);
      const selectedModel = normalizeAiModelId(formData.get("model") || state.selectedScreenerModel || "gpt-5-mini");
      const selectedMeta = getModelMeta(selectedModel) || { personality: "balanced" };
      const tier = getCurrentAiTierConfig();
      if (tier.allowedModels.length && !tier.allowedModels.includes(selectedModel)) {
        await showLimitReachedModal("Selected personality is only available for Pro.");
        return;
      }
      if (Number(state.aiUsageToday || 0) >= Number(tier.weeklyLimit || 3)) {
        await showLimitReachedModal("You have reached your weekly AI screener credit limit.");
        return;
      }
      const payload = {
        universe: formData.get("universe"),
        market: formData.get("market"),
        minCap: minCapAbs,
        marketCapFilter: {
          type: minCapAbs === null ? "any" : "greater_than",
          value: minCapAbs,
        },
        maxNames: boundedNames,
        notes: formData.get("notes"),
        agentName: String(formData.get("agentName") || "").trim(),
        filters: collectScreenerFilters(formData),
        model: selectedModel,
        personality: String(selectedMeta.personality || "balanced"),
        workspaceId: state.activeWorkspaceId || sessionUser?.uid || state.user?.uid || "",
        meta: buildMeta(),
      };

	      try {
	        setOutputLoading(ui.screenerOutput, "Running screener and preparing AI Portfolio...");
	        const runScreener = functions.httpsCallable("run_quick_screener");
	        const result = await runScreener(payload);
	        const rows = result.data?.results || [];
          const runId = String(result.data?.runId || "").trim();
          const runTitle = String(result.data?.title || "").trim();
          const resultsFound = Number(result.data?.resultsFound || rows.length || 0);
          if (ui.screenerResultsCount) {
            ui.screenerResultsCount.textContent = `Results Found: ${Number.isFinite(resultsFound) ? resultsFound : rows.length}`;
          }
          renderScreenerRunOutput({
            id: runId || "—",
            title: runTitle || `${payload.universe || "AI Portfolio"} run`,
            results: rows,
            notes: payload.notes,
            modelUsed: payload.model,
            modelTier: tier.key,
            createdAt: new Date().toISOString(),
          });
          if (runId) {
            upsertMyRequest({
              type: "screener",
              requestId: `screener__${runId}`,
              title: runTitle || `${payload.universe || "AI Portfolio"} run`,
              input: {
                universe: String(payload.universe || ""),
                market: String(payload.market || ""),
                maxNames: Number(payload.maxNames || 0) || null,
                notes: String(payload.notes || ""),
                model: String(payload.model || ""),
                filters: payload.filters && typeof payload.filters === "object" ? payload.filters : {},
              },
              outputsMeta: {
                summary: String(payload.notes || "").trim(),
                resultsCount: Number.isFinite(resultsFound) ? resultsFound : rows.length,
                topSymbols: Array.isArray(rows)
                  ? rows.slice(0, 12).map((row) => normalizeTicker(row?.symbol || "")).filter(Boolean)
                  : [],
                modelUsed: String(payload.model || ""),
              },
              sourceRef: {
                collection: "screener_runs",
                id: runId,
              },
            }).catch(() => {});
          }
          state.aiUsageToday = Number(state.aiUsageToday || 0) + 1;
          refreshScreenerCreditsUi();
          if (runId) {
            try {
              await generateAIPortfolioForRun({
                db,
                functions,
                runId,
                preferredName: payload.agentName,
                selectedModel: payload.model,
              });
            } catch (portfolioError) {
              showToast(portfolioError.message || "Portfolio generated from screener, but AI ranking needs retry.", "warn");
            }
          }
        showToast("AI Portfolio generation started.");
        logEvent("screener_request", { universe: payload.universe });
      } catch (error) {
        const message = extractErrorMessage(error, "Unable to generate AI Portfolio.");
        if (ui.screenerResultsCount) ui.screenerResultsCount.textContent = "Results Found: 0";
        if (ui.screenerOutput) {
          setOutputReady(ui.screenerOutput);
          ui.screenerOutput.innerHTML = `<div class="small muted">${escapeHtml(message)}</div>`;
        }
        showToast(message, "warn");
      }
    });

    ui.screenerLoadButton?.addEventListener("click", async () => {
      try {
        await ensureSessionUser({
          reason: "screener_saved_load_requires_session",
          message: "Sign in to sync saved screener runs.",
        });
      } catch (error) {
        showToast(error?.message || "Unable to start guest session.", "warn");
        return;
      }
      const runId = String(ui.screenerLoadSelect?.value || "").trim();
      if (!runId) {
        showToast("Select a saved run.", "warn");
        return;
      }
      if (ui.screenerLoadStatus) ui.screenerLoadStatus.textContent = "Loading...";
      try {
        await loadScreenerRunById(db, runId);
        if (ui.screenerLoadStatus) ui.screenerLoadStatus.textContent = "";
        showToast("Screener run loaded.");
      } catch (error) {
        if (ui.screenerLoadStatus) ui.screenerLoadStatus.textContent = error.message || "Unable to load run.";
        showToast(error.message || "Unable to load run.", "warn");
      }
    });

    ui.predictionsForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        await ensureSessionUser({
          reason: "predictions_upload_requires_session",
          message: "Sign in to sync prediction uploads.",
        });
      } catch (error) {
        showToast(error?.message || "Unable to start guest session.", "warn");
        return;
      }
      if (!requireAdminAccess("Upload predictions is currently admin-only.")) return;
      if (!storage) {
        showToast("File uploads are not available.", "warn");
        return;
      }

      const fileInput = document.getElementById("predictions-file");
      const notesInput = document.getElementById("predictions-notes");
      const tickerInput = ui.predictionsTicker;
      if (!fileInput?.files?.length) {
        showToast("Select a predictions.csv file.", "warn");
        return;
      }
      const metaTicker = normalizeTicker(tickerInput?.value || "");
      if (!metaTicker) {
        showToast("Enter the ticker for this predictions CSV.", "warn");
        tickerInput?.focus?.();
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
          ticker: metaTicker,
          fileUrl: url,
          filePath: path,
          createdAt: firebase.firestore.FieldValue.serverTimestamp(),
          meta: buildMeta(),
        };
        await db.collection("prediction_uploads").add(doc);
        ui.predictionsStatus.textContent = "Upload complete.";
        fileInput.value = "";
        if (notesInput) notesInput.value = "";
        if (tickerInput) tickerInput.value = metaTicker;
        logEvent("predictions_upload", { file: file.name });
        showToast("Predictions uploaded.");
      } catch (error) {
        ui.predictionsStatus.textContent = "Upload failed.";
        showToast(error.message || "Upload failed.", "warn");
      }
    });

    ui.predictionsAgentButton?.addEventListener("click", async () => {
      try {
        await ensureSessionUser({
          reason: "predictions_agent_requires_session",
          message: "Sign in to sync prediction agent runs.",
        });
      } catch (error) {
        showToast(error?.message || "Unable to start guest session.", "warn");
        return;
      }
      if (!requireAdminAccess("OpenAI CSV Agent is currently admin-only.")) return;
      if (!functions) {
        showToast("Functions client is not ready.", "warn");
        return;
      }
      try {
        setOutputLoading(ui.predictionsAgentOutput, "Analyzing uploaded CSV...");
        const mappingResult = await runPredictionsQuantileMapping(functions);
        if (mappingResult?.uploadId) {
          try {
            const runAgent = functions.httpsCallable("run_prediction_upload_agent");
            const agentRes = await runAgent({
              uploadId: mappingResult.uploadId,
              ticker: mappingResult.ticker,
              mappingSummary: mappingResult,
              meta: buildMeta(),
            });
	            const agent = agentRes?.data || {};
	            const agentText = String(agent.analysis || "").trim();
	            const modelUsed = normalizeAiModelId(agent.model || "gpt-5-mini") || "gpt-5-mini";
              if (db && state.user && agentText) {
                await db.collection("agent_runs").add({
                  userId: state.user.uid,
                  uploadId: mappingResult.uploadId,
                  ticker: normalizeTicker(mappingResult.ticker || ""),
                  model: modelUsed,
                  summary: agentText,
                  createdAt: firebase.firestore.FieldValue.serverTimestamp(),
                  meta: buildMeta(),
                });
              }
	            if (agentText && ui.predictionsAgentOutput) {
	              ui.predictionsAgentOutput.innerHTML += `
	                <div class="agent-summary" style="margin-top:14px;">
	                  <div class="small"><strong>OpenAI Agent (${escapeHtml(modelUsed)}):</strong></div>
                  <div class="small" style="margin-top:6px; white-space:pre-wrap;">${escapeHtml(agentText)}</div>
                </div>
              `;
            }
          } catch (agentError) {
            if (ui.predictionsAgentOutput) {
              ui.predictionsAgentOutput.innerHTML += `
                <div class="small muted" style="margin-top:12px;">
                  OpenAI agent unavailable: ${escapeHtml(agentError.message || "Unable to generate AI commentary.")}
                </div>
              `;
            }
          }
        }
        setOutputReady(ui.predictionsAgentOutput);
        showToast("OpenAI CSV Agent completed.");
      } catch (error) {
        setOutputReady(ui.predictionsAgentOutput);
        if (ui.predictionsAgentOutput) {
          ui.predictionsAgentOutput.innerHTML = `<div class="small muted">${escapeHtml(error.message || "OpenAI CSV Agent failed.")}</div>`;
        }
        showToast(error.message || "OpenAI CSV Agent failed.", "warn");
      }
    });

    ui.autopilotForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        await ensureSessionUser({
          reason: "autopilot_requires_session",
          message: "Sign in to sync autopilot runs.",
        });
      } catch (error) {
        showToast(error?.message || "Unable to start guest session.", "warn");
        return;
      }
      if (!requireAdminAccess("Autopilot queue is currently admin-only.")) return;
      const formData = new FormData(ui.autopilotForm);
      const rawTickerInput = String(formData.get("ticker") || "").trim();
      let tickers = rawTickerInput
        .split(/[,\s]+/)
        .map((item) => normalizeTicker(item))
        .filter(Boolean);
      if (!tickers.length || /^brief$/i.test(rawTickerInput)) {
        tickers = DEFAULT_BRIEF_TICKERS.slice(0, 25);
      }
      tickers = Array.from(new Set(tickers)).slice(0, 30);

      const basePayload = {
        horizon: Number(formData.get("horizon")),
        quantiles: formData.get("quantiles"),
        interval: formData.get("interval"),
        notes: formData.get("notes"),
        briefTickers: tickers,
        briefMode: tickers.length >= 20 ? "daily_weekly_brief" : "single",
        meta: buildMeta(),
      };

      try {
        ui.autopilotStatus.textContent = `Queuing ${tickers.length} ticker${tickers.length === 1 ? "" : "s"}...`;
        const queueRun = functions.httpsCallable("queue_autopilot_run");
        const requestIds = [];
        for (const ticker of tickers) {
          const result = await queueRun({
            ...basePayload,
            ticker,
          });
          const requestId = String(result.data?.requestId || "").trim();
          if (requestId) requestIds.push(requestId);
        }
        ui.autopilotStatus.textContent = requestIds.length
          ? `Queued ${requestIds.length} run(s).`
          : `Queued ${tickers.length} run(s).`;
        logEvent("autopilot_request", { count: tickers.length, mode: basePayload.briefMode });
        showToast(`Autopilot queued for ${tickers.length} diversified stocks.`);
      } catch (error) {
        ui.autopilotStatus.textContent = "Unable to queue run.";
        showToast(error.message || "Unable to queue run.", "warn");
      }
    });

	    ui.notificationsEnable?.addEventListener("click", async () => {
	      if (!requireFullAccount("Sign in to enable notifications.", { redirect: true })) return;
	      if (!state.remoteFlags.pushEnabled) {
	        showToast("Notifications are temporarily disabled.", "warn");
	        return;
	      }
	      if (!isPushChannelAvailable()) {
	        showToast("Push notifications are not supported on this device.", "warn");
	        return;
	      }
	      if (!isNativeApp() && !messaging) {
	        showToast("Messaging SDK is unavailable on this page.", "warn");
	        return;
	      }
      try {
        setNotificationStatus("Registering notification token...");
        const token = await registerNotificationToken(functions, messaging, { forceRefresh: false });
        setNotificationTokenPreview(token);
        setNotificationStatus("Notifications are enabled for this device.");
        await appendNotificationLogPersonalized({
          title: "Notifications enabled",
          body: "Notification token registered successfully.",
          source: "system",
          at: new Date().toISOString(),
        });
        await loadNotificationFeed({ filter: state.notificationFeed?.filter || "all", unreadOnly: Boolean(state.notificationFeed?.unreadOnly), silent: true });
        logEvent("notifications_enabled", { channel: isNativeApp() ? "native" : "webpush" });
        showToast("Notifications enabled.");
      } catch (error) {
	        setNotificationStatus(error.message || "Unable to enable notifications.");
	        showToast(error.message || "Unable to enable notifications.", "warn");
      }
    });

	    ui.notificationsRefresh?.addEventListener("click", async () => {
	      if (!requireFullAccount("Sign in first.", { redirect: true })) return;
	      if (!state.remoteFlags.pushEnabled) {
	        showToast("Notifications are temporarily disabled.", "warn");
	        return;
	      }
	      if (!isPushChannelAvailable()) {
	        showToast("Push notifications are not supported on this device.", "warn");
	        return;
	      }
	      if (!isNativeApp() && !messaging) {
	        showToast("Messaging SDK is unavailable on this page.", "warn");
	        return;
	      }
      try {
        setNotificationStatus("Refreshing notification token...");
        const token = await registerNotificationToken(functions, messaging, { forceRefresh: true });
        setNotificationTokenPreview(token);
        setNotificationStatus("Notification token refreshed.");
        await appendNotificationLogPersonalized({
          title: "Notification token refreshed",
          body: "FCM token rotated and synced.",
          source: "system",
          at: new Date().toISOString(),
        });
        await loadNotificationFeed({ filter: state.notificationFeed?.filter || "all", unreadOnly: Boolean(state.notificationFeed?.unreadOnly), silent: true });
        logEvent("notifications_token_refreshed", { channel: isNativeApp() ? "native" : "webpush" });
      } catch (error) {
	        setNotificationStatus(error.message || "Unable to refresh notification token.");
	        showToast(error.message || "Unable to refresh notification token.", "warn");
      }
    });

    ui.notificationsSendTest?.addEventListener("click", async () => {
	      if (!requireFullAccount("Sign in first.", { redirect: true })) return;
	      if (!state.remoteFlags.pushEnabled) {
	        showToast("Notifications are temporarily disabled.", "warn");
	        return;
	      }
	      try {
	        setNotificationStatus("Sending test notification...");
          const cachedToken = String(safeLocalStorageGet(FCM_TOKEN_CACHE_KEY) || "").trim();
          let sent = 0;
          let attempted = 0;
          let usedFallback = false;

          try {
            const headers = await buildApiAuthHeaders({ includeJson: true });
            const response = await fetch("/api/notify/sendTest", {
              method: "POST",
              headers,
              body: JSON.stringify({
                title: "Quantura test",
                body: "Web push is active for your dashboard.",
                data: {
                  source: "dashboard_test",
                  timestamp: new Date().toISOString(),
                },
                token: cachedToken,
                meta: buildMeta(),
              }),
            });
            const apiPayload = await response.json().catch(() => ({}));
            if (!response.ok) throw new Error(String(apiPayload?.error || "Unable to send test notification."));
            sent = Number(apiPayload?.successCount || apiPayload?.delivered || 0) || 0;
            attempted = Number(apiPayload?.attemptedTokenCount || apiPayload?.attempted || sent) || sent;
          } catch (apiError) {
            const sendTestNotification = functions.httpsCallable("send_test_notification");
            const result = await sendTestNotification({
              title: "Quantura test",
              body: "Web push is active for your dashboard.",
              data: {
                source: "dashboard_test",
                timestamp: new Date().toISOString(),
              },
              token: cachedToken,
              meta: buildMeta(),
            });
            sent = Number(result.data?.successCount ?? 0) || 0;
            attempted = Number(result.data?.attemptedTokenCount ?? sent) || sent;
            usedFallback = Boolean(result.data?.usedFallbackToken);
          }
        const statusSuffix = usedFallback ? " (used local token fallback)" : "";
        setNotificationStatus(`Test sent. Delivered to ${sent} of ${attempted} token(s)${statusSuffix}.`);
        await appendNotificationLogPersonalized({
          title: "Test notification sent",
          body: `Delivered to ${sent} of ${attempted} token(s).${usedFallback ? " Local cached token was used as fallback." : ""}`,
          source: "system",
          at: new Date().toISOString(),
        });
        await loadNotificationFeed({ filter: state.notificationFeed?.filter || "all", unreadOnly: Boolean(state.notificationFeed?.unreadOnly), silent: true });
        logEvent("notifications_test_sent", { delivered: sent });
        showToast("Test notification sent.");
      } catch (error) {
        setNotificationStatus(error.message || "Unable to send test notification.");
        showToast(error.message || "Unable to send test notification.", "warn");
      }
    });

    ui.notificationsClear?.addEventListener("click", () => {
      state.notificationLog = [];
      persistNotificationLog();
      renderNotificationLog();
      setNotificationStatus("Notification log cleared.");
      showToast("Notification log cleared.");
    });

    ui.notificationsMarkAll?.addEventListener("click", async () => {
      if (!requireFullAccount("Sign in first.", { redirect: true })) return;
      await markAllNotificationsRead();
    });

    if (Array.isArray(ui.notificationFilterButtons)) {
      ui.notificationFilterButtons.forEach((button) => {
        button.addEventListener("click", async () => {
          const filter = String(button?.dataset?.notificationFilter || "all").trim().toLowerCase() || "all";
          const unreadOnly = filter === "unread";
          await loadNotificationFeed({ filter: unreadOnly ? "all" : filter, unreadOnly, silent: true });
        });
      });
    }

    document.addEventListener("click", async (event) => {
      const trigger = event.target.closest('[data-action="notification-mark-read"]');
      if (!trigger) return;
      event.preventDefault();
      const itemId = String(trigger.dataset.id || "").trim();
      if (!itemId) return;
      await markNotificationItemRead(itemId);
    });

    ui.notificationsLocationOptIn?.addEventListener("change", async () => {
      try {
        await saveNotificationPrivacySettings();
        setNotificationPrivacyStatus("Consent preference saved.");
        logEvent("notifications_location_consent_updated", {
          enabled: Boolean(ui.notificationsLocationOptIn?.checked),
        });
      } catch (error) {
        setNotificationPrivacyStatus(error.message || "Unable to save location consent.", true);
      }
    });

    ui.notificationsIpOptIn?.addEventListener("change", async () => {
      try {
        await saveNotificationPrivacySettings();
        setNotificationPrivacyStatus("IP-region preference saved.");
        logEvent("notifications_ip_region_consent_updated", {
          enabled: Boolean(ui.notificationsIpOptIn?.checked),
        });
      } catch (error) {
        setNotificationPrivacyStatus(error.message || "Unable to save IP-region preference.", true);
      }
    });

    ui.notificationsRequestLocation?.addEventListener("click", async () => {
      try {
        setNotificationPrivacyStatus("Requesting location permission...");
        await requestCoarseNotificationLocation();
        await saveNotificationPrivacySettings();
        setNotificationPrivacyStatus("Coarse location captured.");
        logEvent("notifications_location_captured", {});
      } catch (error) {
        setNotificationPrivacyStatus(error.message || "Unable to capture location.", true);
      }
    });

    if (ui.profileForm && ui.profileForm.dataset.bound !== "1") {
      ui.profileForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (!requireFullAccount("Sign in to save your profile.", { redirect: true })) {
          setProfileStatus("Sign in to save your profile.", "warn");
          return;
        }
        try {
          const nextProfile = {
            username: sanitizeProfileUsername(ui.profileUsername?.value || "", state.user),
            avatar: normalizeProfileAvatar(ui.profileAvatar?.value || state.userProfile?.avatar || "bull"),
            bio: normalizeProfileBio(ui.profileBio?.value || ""),
            publicProfile: Boolean(ui.profilePublicEnabled?.checked),
            publicScreenerSharing: Boolean(ui.profilePublicScreener?.checked),
            publicEmailOptIn: Boolean(ui.profilePublicEmail?.checked),
            stripeConnectAccountId: String(state.userProfile?.stripeConnectAccountId || "").trim(),
            socialLinks: normalizeProfileSocialLinks(
              {
                website: ui.profileWebsite?.value || "",
                x: ui.profileX?.value || "",
                linkedin: ui.profileLinkedin?.value || "",
                github: ui.profileGithub?.value || "",
                youtube: ui.profileYoutube?.value || "",
                tiktok: ui.profileTiktok?.value || "",
                facebook: ui.profileFacebook?.value || "",
                instagram: ui.profileInstagram?.value || "",
                reddit: ui.profileReddit?.value || "",
              },
              { strict: true }
            ),
          };
          await db.collection("users").doc(state.user.uid).set(
            {
              profile: nextProfile,
              profileUpdatedAt: firebase.firestore.FieldValue.serverTimestamp(),
              metadata: buildMeta(),
            },
            { merge: true }
          );
          renderProfileForm(nextProfile, state.user);
          setProfileStatus("Profile saved. New AI Agents will include this attribution.", "success");
          showToast("Profile saved.");
        } catch (error) {
          const message = extractErrorMessage(error, "Unable to save profile.");
          setProfileStatus(message, "warn");
          showToast(message, "warn");
        }
      });
      ui.profileForm.dataset.bound = "1";
    }
    if (ui.profileConnectStripe && ui.profileConnectStripe.dataset.bound !== "1") {
      ui.profileConnectStripe.addEventListener("click", async () => {
        if (!requireFullAccount("Sign in to connect Stripe.", { redirect: true })) {
          setProfileStatus("Sign in to connect Stripe.", "warn");
          return;
        }
        const workspaceId = state.activeWorkspaceId || state.user.uid;
        ui.profileConnectStripe.disabled = true;
        try {
          const onboard = functions.httpsCallable("create_stripe_connect_onboarding_link");
          const result = await onboard({ workspaceId, meta: buildMeta() });
          const accountId = String(result.data?.accountId || "").trim();
          const url = String(result.data?.url || "").trim();
          if (!url) throw new Error("Stripe onboarding URL is missing.");
          state.userProfile = {
            ...state.userProfile,
            stripeConnectAccountId: accountId,
          };
          renderProfileForm(state.userProfile, state.user);
          setProfileStatus("Redirecting to Stripe onboarding...", "success");
          window.location.assign(url);
        } catch (error) {
          const message = extractErrorMessage(error, "Unable to start Stripe onboarding.");
          setProfileStatus(message, "warn");
          showToast(message, "warn");
        } finally {
          ui.profileConnectStripe.disabled = false;
        }
      });
      ui.profileConnectStripe.dataset.bound = "1";
    }
    setProfileFormEnabled(false);
    renderProfileForm({ username: "", socialLinks: cloneDefaultProfileSocialLinks() }, null);

			    persistenceReady.finally(() => {
			      auth.onAuthStateChanged(async (user) => {
          const previousUser = state.user;
          const previousUid = String(previousUser?.uid || "").trim();
          const previousWasFull = hasFullAccount(previousUser);
          const previousWasAnonymous = isAnonymousUser(previousUser);
          const isFirstAuthEvent = !state.authStateBootstrapped;
          state.authStateBootstrapped = true;

          if (!user) {
            state.authResolved = true;
            state.user = null;
            state.earningsCalendar.followsUid = "anon";
            state.earningsCalendar.follows = readLocalEarningsFollows();
            if (isPanelVisible("events-calendar")) {
              renderEarningsCalendar();
            }
            state.tickerContext.tickerHistory = readTickerHistory();
            renderTickerHistory();
            setAuthUi(null);
            setUserId(null);
            if (!state.anonymousBootstrapInFlight) {
              state.anonymousBootstrapInFlight = true;
              try {
                await auth.signInAnonymously();
                logEvent("login", { method: "anonymous_auto", runtime: resolveRuntimeLabel() });
              } catch (anonError) {
                if (ui.emailMessage) {
                  ui.emailMessage.textContent = anonError?.message || "Unable to initialize anonymous session.";
                }
              } finally {
                state.anonymousBootstrapInFlight = false;
              }
            }
            if (isNativeApp()) {
              const bridge = installNativeAuthBridge(auth);
              if (!state.nativeAuthPromptRequested) {
                state.nativeAuthPromptRequested = true;
                bridge?.requestSignIn?.();
              }
            }
            return;
          }
			      state.authResolved = true;
			      state.user = user;
          const nextUid = String(user?.uid || "").trim();
          if (previousWasAnonymous && hasFullAccount(user) && previousUid && nextUid && previousUid !== nextUid) {
            await mergeAnonymousSessionData(previousUid, nextUid).catch(() => undefined);
          }
          await flushPendingNativeIapEvents().catch(() => undefined);
          const shouldRefreshAfterSignIn =
            !isFirstAuthEvent &&
            hasFullAccount(user) &&
            (!previousWasFull || previousUid !== nextUid) &&
            !state.postSignInReloadInFlight;
          if (shouldRefreshAfterSignIn) {
            state.postSignInReloadInFlight = true;
            safeLocalStorageSet(AUTH_POST_SIGNIN_REFRESH_KEY, `${nextUid}:${Date.now()}`);
            showToast("Signed in. Refreshing workspace...");
            window.setTimeout(() => {
              window.location.reload();
            }, 120);
            return;
          }
            await loadEarningsFollowSet({ force: true }).catch(() => {});
            if (isPanelVisible("events-calendar")) {
              renderEarningsCalendar();
            }
            await linkPendingCredentialIfPresent({ silent: true }).catch(() => {});
            state.tickerContext.tickerHistory = readTickerHistory();
            renderTickerHistory();
			      setAuthUi(user);
			      setUserId(hasFullAccount(user) ? user.uid : null);
            await pingNotificationSession().catch(() => undefined);
            if (isNativeApp()) {
              installNativeAuthBridge(auth);
              if (user.isAnonymous) {
                if (!state.nativeAuthPromptRequested) {
                  state.nativeAuthPromptRequested = true;
                  window.__quanturaAuthBridge?.requestSignIn?.();
                }
              } else {
                state.nativeAuthPromptRequested = false;
              }
            }

			      if (!hasFullAccount(user)) {
		        state.userHasPaidPlan = false;
            state.userSubscriptionTier = "free";
            state.aiUsageToday = 0;
            state.aiUsageTierKey = "free";
            state.collaboratorCount = 0;
            state.pendingCollabInviteCount = 0;
            applyAdFreeExperience();
		        renderOrderList([], ui.userOrders);
            renderRequestList([], ui.userForecasts, "No forecast requests yet.");
            renderRequestList([], ui.autopilotOutput, "No autopilot requests yet.");
            renderRequestList([], ui.predictionsOutput, "No uploads yet.");
		        if (ui.watchlistList) ui.watchlistList.textContent = "Sign in to manage your watchlist.";
		        if (ui.alertsList) ui.alertsList.textContent = "Sign in to manage your alerts.";
	        if (ui.alertsStatus) ui.alertsStatus.textContent = "";
	        if (ui.collabInvitesList) ui.collabInvitesList.textContent = "Sign in to view invites.";
	        if (ui.collabCollaboratorsList) ui.collabCollaboratorsList.textContent = "Sign in to manage collaborators.";
		        ui.adminSection?.classList.add("hidden");
		        ui.navAdmin?.classList.add("hidden");
                setFeatureVoteSummaryPolling(functions, false);
        if (ui.notificationsStatus) {
          if (!isPushChannelAvailable()) {
            setNotificationStatus("Push notifications are not supported on this device.");
          } else if (!isNativeApp() && !messaging) {
            setNotificationStatus("Messaging SDK is not loaded on this page.");
          } else {
            setNotificationControlsEnabled(true);
            const cachedToken = localStorage.getItem(FCM_TOKEN_CACHE_KEY) || "";
            setNotificationTokenPreview(cachedToken);
            if (isNativeApp()) {
              try {
                const token = await registerNotificationToken(functions, messaging, { forceRefresh: !cachedToken });
                setNotificationTokenPreview(token);
                setNotificationStatus("Guest session notifications enabled for this device.");
              } catch (error) {
                setNotificationStatus(error.message || "Unable to initialize notifications.");
              }
            } else if (messaging && Notification.permission === "granted") {
              try {
                const token = await registerNotificationToken(functions, messaging, { forceRefresh: !cachedToken });
                setNotificationTokenPreview(token);
                setNotificationStatus("Guest session notifications enabled for this device.");
              } catch (error) {
                setNotificationStatus(error.message || "Unable to initialize notifications.");
              }
            } else if (!messaging) {
              setNotificationStatus("Messaging SDK is not loaded on this page.");
            } else {
              setNotificationStatus("Enable notifications to receive guest-session alerts.");
            }
          }
        }
            state.notificationFeed.items = [];
            state.notificationFeed.unreadCount = 0;
            state.notificationFeed.filter = "all";
            state.notificationFeed.unreadOnly = false;
            renderNotificationFeed();
            syncNotificationPrivacyControls();
			        if (state.unsubscribeOrders) state.unsubscribeOrders();
			        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
				        if (state.unsubscribeForecasts) state.unsubscribeForecasts();
				        if (state.unsubscribeAutopilot) state.unsubscribeAutopilot();
				        if (state.unsubscribePredictions) state.unsubscribePredictions();
				        if (state.unsubscribeTasks) state.unsubscribeTasks();
				        if (state.unsubscribeWatchlist) state.unsubscribeWatchlist();
				        if (state.unsubscribeAlerts) state.unsubscribeAlerts();
                if (state.unsubscribeScreenerRuns) state.unsubscribeScreenerRuns();
                if (state.unsubscribeAIAgents) state.unsubscribeAIAgents();
                if (state.unsubscribeAIFollows) state.unsubscribeAIFollows();
                if (state.unsubscribeAILikes) state.unsubscribeAILikes();
		        if (state.unsubscribeSharedWorkspaces) state.unsubscribeSharedWorkspaces();
            if (state.volatilityMonitorTimer) {
              window.clearInterval(state.volatilityMonitorTimer);
              state.volatilityMonitorTimer = null;
            }
            state.aiAgents = [];
            state.aiFollowSet = new Set();
            state.aiLikeSet = new Set();
            state.aiDefaultsSeededWorkspaceId = "";
            state.myRequests = [];
            state.myRequestsById = {};
            state.myRequestsLoading = false;
            state.myRequestsLoadedAt = 0;
			        state.sharedWorkspaces = [];
	            state.sharedScreenerView = null;
            state.tickerContext.forecastDoc = null;
            state.tickerContext.forecastId = "";
            state.tickerContext.forecastTablePage = 0;
            state.tickerContext.forecastAiSummary = null;
            state.tickerContext.forecastCacheMeta = null;
			        setActiveWorkspaceId("");
            state.userProfile = {
              username: "",
              socialLinks: cloneDefaultProfileSocialLinks(),
              avatar: "bull",
              bio: "",
              publicProfile: false,
              publicScreenerSharing: false,
              publicEmailOptIn: false,
              stripeConnectAccountId: "",
            };
            renderProfileForm(state.userProfile, null);
		        renderWorkspaceSelect(null);
		        if (ui.productivityBoard) ui.productivityBoard.innerHTML = "";
		        if (ui.tasksCalendar) ui.tasksCalendar.textContent = "Tasks with due dates will appear here.";
            if (ui.screenerLoadSelect) ui.screenerLoadSelect.innerHTML = `<option value="">Select a run</option>`;
            if (ui.screenerLoadStatus) ui.screenerLoadStatus.textContent = "";
            if (ui.screenerOutput && !ui.screenerOutput.dataset.loading) ui.screenerOutput.textContent = "Sign in to generate an AI Portfolio.";
            renderMyRequestsPanels();
            refreshScreenerModelUi();
            refreshScreenerCreditsUi();
            state.predictionsContext.uploadId = "";
            state.predictionsContext.uploadDoc = null;
            state.predictionsContext.table = null;
            state.predictionsContext.previewPage = 0;
            if (ui.predictionsAgentOutput) {
              ui.predictionsAgentOutput.textContent =
                "Run the OpenAI CSV Agent to compute weekday-aware quantile mapping and return an analyst summary.";
            }

            const pendingShare = String(getPendingShareId() || "").trim();
            const onScreenerPage = window.location.pathname === "/screener";
            if (pendingShare && onScreenerPage) {
              try {
                await renderSharedScreenerRun(pendingShare);
              } catch (error) {
                if (window.location.pathname !== "/account") {
                  window.location.href = "/account";
                  return;
                }
              }
            } else if (pendingShare && window.location.pathname !== "/account") {
              window.location.href = "/account";
              return;
            }

			        return;
			      }

	      await ensureUserProfile(db, user);
        await loadUserProfile(db, user);
        setProfileStatus("Profile is used when publishing AI agents in Explore.");
	      startUserOrders(db, user);
	      subscribeSharedWorkspaces(db, user);
		      const activeWorkspaceId = resolveActiveWorkspaceId(user);
		      setActiveWorkspaceId(activeWorkspaceId);
		      renderWorkspaceSelect(user);
		      startUserForecasts(db, activeWorkspaceId);
          startScreenerRuns(db, activeWorkspaceId);
          await fetchMyRequestsList({ force: true }).catch(() => []);
          renderMyRequestsPanels();
          loadScreenerUsageToday(db);
          startWorkspaceTasks(db, activeWorkspaceId);
			      startWatchlist(db, activeWorkspaceId);
			      startPriceAlerts(db, activeWorkspaceId);
          await seedDefaultAIAgents(db, activeWorkspaceId).catch(() => {});
          await seedAdminPresetScreenerRuns(db, activeWorkspaceId).catch(() => {});
          startAIAgents(db, activeWorkspaceId);
          startVolatilityMonitor(db, functions, activeWorkspaceId);
	      startAutopilotRequests(db, user);
	      startPredictionsUploads(db, user);
	      refreshCollaboration(functions);

        const pendingShare = String(getPendingShareId() || "").trim();
        if (pendingShare && window.location.pathname === "/screener") {
          try {
            await renderSharedScreenerRun(pendingShare);
          } catch (error) {
            await processPendingShareImport(functions);
          }
        } else {
          await processPendingShareImport(functions);
        }

        if (window.location.pathname === "/account" && !String(getPendingShareId() || "").trim()) {
          window.location.href = "/dashboard";
        }

	      if (ui.notificationsStatus) {
	        if (!state.remoteFlags.pushEnabled) {
	          setNotificationControlsEnabled(false);
	          setNotificationStatus("Notifications are temporarily disabled.");
        } else if (isPushChannelAvailable()) {
          setNotificationControlsEnabled(true);
          const cachedToken = localStorage.getItem(FCM_TOKEN_CACHE_KEY) || "";
          setNotificationTokenPreview(cachedToken);
          setNotificationStatus(cachedToken ? "Notifications enabled for this device." : "Click Enable notifications.");
          if (isNativeApp()) {
            try {
              const token = await registerNotificationToken(functions, messaging, { forceRefresh: !cachedToken });
              setNotificationTokenPreview(token);
              setNotificationStatus("Notifications enabled for this device.");
            } catch (error) {
              setNotificationStatus(error.message || "Unable to initialize notifications.");
            }
          } else if (messaging && Notification.permission === "granted") {
            try {
              const token = await registerNotificationToken(functions, messaging, { forceRefresh: !cachedToken });
              setNotificationTokenPreview(token);
              setNotificationStatus("Notifications enabled for this device.");
            } catch (error) {
              setNotificationStatus(error.message || "Unable to initialize notifications.");
            }
          } else if (!messaging) {
            setNotificationStatus("Messaging SDK is not loaded on this page.");
          }
	        } else {
	          setNotificationControlsEnabled(false);
            setNotificationStatus("Push notifications are not supported on this device.");
	        }
          await loadNotificationFeed({
            filter: state.notificationFeed?.filter || "all",
            unreadOnly: Boolean(state.notificationFeed?.unreadOnly),
            silent: true,
          });
	      }
        await loadNotificationPrivacySettings().catch(() => {
          syncNotificationPrivacyControls();
        });
        renderServerPromoBanner();
        maybeShowPromoModal();

	      if (ui.terminalForm && ui.tickerChart && state.tickerContext.forecastId && !state.tickerContext.forecastDoc) {
	        try {
	          setTerminalStatus("Loading saved run...");
	          await plotForecastById(db, functions, state.tickerContext.forecastId);
	        } catch (error) {
	          setTerminalStatus(error.message || "Unable to load saved run.");
	        }
	      }

	      if (isAdminUser(user)) {
	        ui.adminSection?.classList.remove("hidden");
	        ui.navAdmin?.classList.remove("hidden");
	        startAdminOrders(db);
	        startAdminAutopilotQueue(db);
            setFeatureVoteSummaryPolling(functions, true);
          loadFiscaldataCapabilities({ force: false }).catch((error) => {
            if (ui.adminFiscaldataCapabilitiesStatus) {
              ui.adminFiscaldataCapabilitiesStatus.textContent = extractErrorMessage(error, "Unable to load endpoint check.");
            }
            if (ui.adminFiscaldataCapabilities) {
              ui.adminFiscaldataCapabilities.innerHTML = `<div class="small muted">Endpoint check unavailable.</div>`;
            }
          });
	      } else {
	        ui.adminSection?.classList.add("hidden");
	        ui.navAdmin?.classList.add("hidden");
	        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
	        if (state.unsubscribeAdminAutopilot) state.unsubscribeAdminAutopilot();
            setFeatureVoteSummaryPolling(functions, false);
          if (ui.adminFiscaldataCapabilitiesStatus) ui.adminFiscaldataCapabilitiesStatus.textContent = "Admin access required.";
          if (ui.adminFiscaldataCapabilities) ui.adminFiscaldataCapabilities.innerHTML = `<div class="small muted">Admin access required.</div>`;
	      }
    });
  });
  };

  window.addEventListener("load", init);
})();
