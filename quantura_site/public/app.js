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
  const PENDING_SHARE_KEY = "quantura_pending_share_v1";
  const HOLIDAY_PROMO_SEEN_KEY = "quantura_holiday_promo_seen_v1";
  const FCM_LOG_CACHE_KEY = "quantura_fcm_log_v1";
  const CHART_RANGE_CACHE_KEY = "quantura_chart_range_v1";
  const CHART_VIEW_CACHE_KEY = "quantura_chart_view_v1";
  const CHART_ENGINE_CACHE_KEY = "quantura_chart_engine_v1";
  const TRADINGVIEW_THEME_CACHE_KEY = "quantura_tv_theme_v1";
  const LANGUAGE_PREFERENCE_KEY = "quantura_language_v1";
  const COUNTRY_PREFERENCE_KEY = "quantura_country_v1";
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
  const DEFAULT_BRIEF_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "NFLX",
    "AMD", "AVGO", "CRM", "ORCL", "JPM", "BAC", "GS", "V", "MA",
    "UNH", "LLY", "JNJ", "XOM", "CVX", "CAT", "DE", "KO", "PEP",
    "COST", "WMT", "NKE", "PLTR",
  ];
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
  ];
  const AI_USAGE_TIER_DEFAULTS = {
    free: {
      allowed_models: ["gpt-5-nano", "gpt-5-mini"],
      weekly_limit: 3,
      daily_limit: 3,
      volatility_alerts: false,
    },
    pro: {
      allowed_models: ["gpt-5-mini", "gpt-5", "gpt-5.1"],
      weekly_limit: 25,
      daily_limit: 25,
      volatility_alerts: true,
    },
    desk: {
      allowed_models: ["gpt-5-nano", "gpt-5-mini", "gpt-5", "gpt-5.1", "gpt-5.2"],
      weekly_limit: 75,
      daily_limit: 75,
      volatility_alerts: true,
    },
  };
  const MODEL_PROVIDER_LABEL = {
    openai: "OpenAI",
  };
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
  const UNSPLASH_ACCESS_KEY = "tKqmTYXWxWdvGHHlbO8OtfdtJMYaz0KXKWKyCaG61u4";
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
  const BACKTEST_SOURCE_OPTIONS = [
    { key: "python", label: "Python (.py)", ext: "py", mimeType: "text/x-python" },
    { key: "tradingview", label: "TradingView (.pine)", ext: "pine", mimeType: "text/plain" },
    { key: "metatrader5", label: "MetaTrader 5 (.mq5)", ext: "mq5", mimeType: "text/plain" },
    { key: "tradelocker", label: "TradeLocker (JSON)", ext: "json", mimeType: "application/json" },
  ];

  const hydrateUnsplashGallery = async () => {
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
          creditLink.href = photo.photographerLink || photo.link || "https://unsplash.com/?utm_source=quantura&utm_medium=referral";
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

    if (!UNSPLASH_ACCESS_KEY) return;

    const rawQuery = String(gallery.dataset.unsplashQuery || "stock market, trading desk");
    const count = Math.max(1, Math.min(8, Number(gallery.dataset.unsplashCount || cards.length || 4)));

    try {
      const endpoint = new URL("https://api.unsplash.com/photos/random");
      endpoint.searchParams.set("query", rawQuery);
      endpoint.searchParams.set("orientation", "landscape");
      endpoint.searchParams.set("content_filter", "high");
      endpoint.searchParams.set("count", String(count));
      endpoint.searchParams.set("client_id", UNSPLASH_ACCESS_KEY);

      const response = await fetch(endpoint.toString(), {
        method: "GET",
        headers: { "Accept-Version": "v1" },
      });
      if (!response.ok) throw new Error(`Unsplash request failed (${response.status})`);

      const payload = await response.json();
      const list = Array.isArray(payload) ? payload : [payload];
      const photos = list
        .map((item) => ({
          url: item?.urls?.regular || item?.urls?.full || item?.urls?.small || "",
          alt: item?.alt_description || item?.description || "Market imagery from Unsplash",
          link: item?.links?.html
            ? `${item.links.html}${String(item.links.html).includes("?") ? "&" : "?"}utm_source=quantura&utm_medium=referral`
            : "",
          photographer: item?.user?.name || "",
          photographerLink: item?.user?.links?.html
            ? `${item.user.links.html}${String(item.user.links.html).includes("?") ? "&" : "?"}utm_source=quantura&utm_medium=referral`
            : "",
        }))
        .filter((item) => item.url);

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
    emailCreate: document.getElementById("email-create"),
    emailMessage: document.getElementById("auth-email-message"),
    googleSignin: document.getElementById("google-signin"),
    githubSignin: document.getElementById("github-signin"),
    twitterSignin: document.getElementById("twitter-signin"),
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
    contactForm: document.getElementById("contact-form"),
    navAdmin: document.getElementById("nav-admin"),
    terminalForm: document.getElementById("terminal-form"),
    terminalTicker: document.getElementById("terminal-ticker"),
    terminalInterval: document.getElementById("terminal-interval"),
    terminalStatus: document.getElementById("terminal-status"),
    tickerChart: document.getElementById("ticker-chart"),
    indicatorChart: document.getElementById("indicator-chart"),
    tickerIntelligenceOutput: document.getElementById("ticker-intelligence-output"),
    forecastForm: document.getElementById("forecast-form"),
    forecastOutput: document.getElementById("forecast-output"),
    forecastService: document.getElementById("forecast-service"),
    forecastLoadSelect: document.getElementById("forecast-load-select"),
    forecastLoadButton: document.getElementById("forecast-load-button"),
    forecastLoadStatus: document.getElementById("forecast-load-status"),
    technicalsForm: document.getElementById("technicals-form"),
    technicalsOutput: document.getElementById("technicals-output"),
    downloadForm: document.getElementById("download-form"),
    downloadStatus: document.getElementById("download-status"),
    trendingButton: document.getElementById("load-trending"),
    trendingList: document.getElementById("trending-list"),
    intelOutput: document.getElementById("intel-output"),
    newsOutput: document.getElementById("news-output"),
    xTrendingOutput: document.getElementById("x-trending-output"),
    eventsCalendarForm: document.getElementById("events-calendar-form"),
    eventsCalendarTickers: document.getElementById("events-calendar-tickers"),
    eventsCalendarCountry: document.getElementById("events-calendar-country"),
    eventsCalendarStart: document.getElementById("events-calendar-start"),
    eventsCalendarEnd: document.getElementById("events-calendar-end"),
    eventsCalendarLimit: document.getElementById("events-calendar-limit"),
    eventsCalendarStatus: document.getElementById("events-calendar-status"),
    eventsCalendarOutput: document.getElementById("events-calendar-output"),
    marketHeadlinesForm: document.getElementById("market-headlines-form"),
    marketHeadlinesCountry: document.getElementById("market-headlines-country"),
    marketHeadlinesLimit: document.getElementById("market-headlines-limit"),
    marketHeadlinesStatus: document.getElementById("market-headlines-status"),
    marketHeadlinesOutput: document.getElementById("market-headlines-output"),
    marketSocialOutput: document.getElementById("market-social-output"),
    tickerQueryForm: document.getElementById("ticker-query-form"),
    tickerQueryTicker: document.getElementById("ticker-query-ticker"),
    tickerQueryQuestion: document.getElementById("ticker-query-question"),
    tickerQueryLanguage: document.getElementById("ticker-query-language"),
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
    autopilotForm: document.getElementById("autopilot-form"),
    autopilotOutput: document.getElementById("autopilot-output"),
    autopilotStatus: document.getElementById("autopilot-status"),
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
    notificationsLog: document.getElementById("notifications-log"),
    notificationsClear: document.getElementById("notifications-clear"),
    billingPortalLink: document.getElementById("billing-portal-link"),
    chartRangeButtons: Array.from(document.querySelectorAll("[data-chart-range]")),
    chartViewButtons: Array.from(document.querySelectorAll("[data-chart-view]")),
    chartEngineButtons: Array.from(document.querySelectorAll("[data-chart-engine]")),
    chartThemeButtons: Array.from(document.querySelectorAll("[data-tv-theme]")),
    tradingViewRoot: document.getElementById("tradingview-terminal-root"),
    tradingViewStatus: document.getElementById("tv-widget-status"),
    tradingViewSymbolInfo: document.getElementById("tv-symbol-info"),
    tradingViewAdvanced: document.getElementById("tv-advanced-chart"),
    tradingViewProfile: document.getElementById("tv-symbol-profile"),
    tradingViewTechnical: document.getElementById("tv-technical-analysis"),
    tradingViewTimeline: document.getElementById("tv-timeline"),
    tradingViewFinancials: document.getElementById("tv-financials"),
    tradingViewFallback: document.getElementById("tv-widget-fallback"),
    tradingViewUseLegacy: document.getElementById("tv-use-legacy"),
    predictionsChart: document.getElementById("predictions-chart"),
    predictionsPreview: document.getElementById("predictions-preview"),
    predictionsPlotMeta: document.getElementById("predictions-plot-meta"),
    predictionsAgentButton: document.getElementById("predictions-agent-button"),
    predictionsAgentOutput: document.getElementById("predictions-agent-output"),
    backtestForm: document.getElementById("backtest-form"),
    backtestStrategy: document.getElementById("backtest-strategy"),
    backtestOutput: document.getElementById("backtest-output"),
    backtestLoadSelect: document.getElementById("backtest-load-select"),
    backtestLoadId: document.getElementById("backtest-load-id"),
    backtestLoadButton: document.getElementById("backtest-load-button"),
    backtestLoadStatus: document.getElementById("backtest-load-status"),
    savedBacktestsList: document.getElementById("saved-backtests-list"),
    toast: document.getElementById("toast"),
    purchasePanels: Array.from(document.querySelectorAll(".purchase-panel")),
  };

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
      stripeConnectAccountId: "",
    },
    preferredLanguage: "en",
    preferredCountry: "US",
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
      newsTicker: "",
      xTicker: "",
      intelTicker: "",
      optionsTicker: "",
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
    taskCalendarCursor: null,
    taskCalendarTasks: [],
	    unsubscribeOrders: null,
	    unsubscribeAdmin: null,
	    unsubscribeAdminAutopilot: null,
	    unsubscribeForecasts: null,
	    unsubscribeAutopilot: null,
	    unsubscribePredictions: null,
      unsubscribeBacktests: null,
	    unsubscribeTasks: null,
	    unsubscribeWatchlist: null,
	    unsubscribeAlerts: null,
	    unsubscribeScreenerRuns: null,
      unsubscribeAIAgents: null,
      unsubscribeAIFollows: null,
      unsubscribeAILikes: null,
	    screenerUrlRunLoaded: false,
      uploadUrlLoaded: false,
      backtestUrlLoaded: false,
	    messagingBound: false,
	    remoteConfigLoaded: false,
	    remoteFlags: {
	      watchlistEnabled: true,
	      forecastProphetEnabled: true,
	      forecastTimeMixerEnabled: true,
        enableSocialLeaderboard: true,
        forecastModelPrimary: "Quantura Horizon",
        promoBannerText: "",
        maintenanceMode: false,
	      pushEnabled: true,
	      webPushVapidKey: "",
        volatilityThreshold: DEFAULT_VOLATILITY_THRESHOLD,
        aiUsageTiers: AI_USAGE_TIER_DEFAULTS,
        stripeCheckoutEnabled: true,
        stripePublicKey: "",
        holidayPromo: false,
        backtestingEnabled: true,
        backtestingFreeDailyLimit: 1,
        backtestingProDailyLimit: 25,
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
    chartEngine: (() => {
      let raw = "tradingview";
      try {
        raw = String(localStorage.getItem(CHART_ENGINE_CACHE_KEY) || "tradingview").trim().toLowerCase();
      } catch (error) {
        raw = "tradingview";
      }
      return raw === "legacy" ? "legacy" : "tradingview";
    })(),
    tradingViewTheme: (() => {
      let raw = "auto";
      try {
        raw = String(localStorage.getItem(TRADINGVIEW_THEME_CACHE_KEY) || "auto").trim().toLowerCase();
      } catch (error) {
        raw = "auto";
      }
      return raw === "dark" || raw === "light" ? raw : "auto";
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
          }));
      } catch (error) {
        return [];
      }
    })(),
    sharedWorkspaces: [],
    unsubscribeSharedWorkspaces: null,
  };

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
              "ticker-intelligence": "/ticker-intelligence",
		          indicators: "/indicators",
              trending: "/trending",
		          news: "/news",
              "events-calendar": "/events-calendar",
              "market-headlines": "/market-headlines",
              "ticker-query": "/ticker-query",
		          options: "/options",
		          saved: "/saved-forecasts",
              backtesting: "/backtesting",
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
		      try {
		        if (typeof window !== "undefined" && typeof window.__quanturaPanelActivated === "function") {
		          window.__quanturaPanelActivated(next);
		        }
		      } catch (error) {
		        // Ignore.
		      }
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
    const links = header?.querySelector(".nav-links");
    const actions = header?.querySelector(".nav-actions");
    if (!header || !nav || !links || !actions) return;

    let toggle = header.querySelector(".mobile-nav-toggle");
    let backdrop = header.querySelector(".mobile-nav-backdrop");
    if (!toggle) {
      toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "mobile-nav-toggle";
      toggle.setAttribute("aria-label", "Toggle navigation menu");
      toggle.setAttribute("aria-expanded", "false");
      toggle.innerHTML = `<span aria-hidden="true">☰</span>`;
      nav.appendChild(toggle);
    }
    if (!backdrop) {
      backdrop = document.createElement("button");
      backdrop.type = "button";
      backdrop.className = "mobile-nav-backdrop hidden";
      backdrop.setAttribute("aria-label", "Close navigation menu");
      header.appendChild(backdrop);
    }

    const close = () => {
      header.classList.remove("nav-open");
      toggle?.setAttribute("aria-expanded", "false");
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
      toggle?.setAttribute("aria-expanded", "true");
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
	            welcome_message: "Welcome to Quantura",
	            watchlist_enabled: true,
	            forecast_prophet_enabled: true,
	            forecast_timemixer_enabled: true,
              enable_social_leaderboard: true,
              forecast_model_primary: "Quantura Horizon",
              promo_banner_text: "",
              maintenance_mode: false,
              volatility_threshold: "0.05",
              ai_usage_tiers: JSON.stringify(AI_USAGE_TIER_DEFAULTS),
	            push_notifications_enabled: true,
	            webpush_vapid_key: "",
	            stripe_checkout_enabled: true,
	            stripe_public_key: "",
              holiday_promo: false,
              backtesting_enabled: true,
              backtesting_free_daily_limit: "1",
              backtesting_pro_daily_limit: "25",
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
            welcome_message: "Welcome to Quantura",
            watchlist_enabled: true,
            forecast_prophet_enabled: true,
            forecast_timemixer_enabled: true,
            enable_social_leaderboard: true,
            forecast_model_primary: "Quantura Horizon",
            promo_banner_text: "",
            maintenance_mode: false,
            volatility_threshold: "0.05",
            ai_usage_tiers: JSON.stringify(AI_USAGE_TIER_DEFAULTS),
            webpush_vapid_key: "",
            stripe_checkout_enabled: true,
            stripe_public_key: "",
            holiday_promo: false,
            backtesting_enabled: true,
            backtesting_free_daily_limit: "1",
            backtesting_pro_daily_limit: "25",
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
		    return {
		      watchlistEnabled: getBool("watchlist_enabled", true),
		      forecastProphetEnabled: getBool("forecast_prophet_enabled", true),
		      forecastTimeMixerEnabled: getBool("forecast_timemixer_enabled", true),
          enableSocialLeaderboard: getBool("enable_social_leaderboard", true),
          forecastModelPrimary: getString("forecast_model_primary", "Quantura Horizon"),
          promoBannerText: getString("promo_banner_text", ""),
          maintenanceMode: getBool("maintenance_mode", false),
		      pushEnabled: getBool("push_notifications_enabled", true),
		      webPushVapidKey: getString("webpush_vapid_key", ""),
          volatilityThreshold: getFloat("volatility_threshold", DEFAULT_VOLATILITY_THRESHOLD),
          aiUsageTiers: getJson("ai_usage_tiers", AI_USAGE_TIER_DEFAULTS),
	        stripeCheckoutEnabled: getBool("stripe_checkout_enabled", true),
	        stripePublicKey: getString("stripe_public_key", ""),
          holidayPromo: getBool("holiday_promo", false),
          backtestingEnabled: getBool("backtesting_enabled", true),
          backtestingFreeDailyLimit: getInt("backtesting_free_daily_limit", 1),
          backtestingProDailyLimit: getInt("backtesting_pro_daily_limit", 25),
		    };
		  };

	  const applyRemoteFlags = (flags) => {
	    document.querySelectorAll('[data-panel-target="watchlist"]').forEach((el) => {
	      el.classList.toggle("hidden", !flags.watchlistEnabled);
	    });
	    document.querySelectorAll('[data-panel="watchlist"]').forEach((el) => {
	      if (!flags.watchlistEnabled) el.classList.add("hidden");
	    });

      document.querySelectorAll('[data-panel-target="backtesting"]').forEach((el) => {
        el.classList.toggle("hidden", !flags.backtestingEnabled);
      });
      document.querySelectorAll('[data-panel="backtesting"]').forEach((el) => {
        if (!flags.backtestingEnabled) el.classList.add("hidden");
      });

	    if (!flags.pushEnabled) {
	      setNotificationStatus("Notifications are temporarily disabled.");
	      setNotificationControlsEnabled(false);
	    }

      const leaderboardCard = document.getElementById("ai-agent-leaderboard")?.closest(".card");
      if (leaderboardCard) leaderboardCard.classList.toggle("hidden", !flags.enableSocialLeaderboard);

	    if (ui.forecastService) {
	      const setOptionEnabled = (value, enabled) => {
	        const option = ui.forecastService.querySelector(`option[value="${value}"]`);
	        if (!option) return;
	        option.disabled = !enabled;
	        option.hidden = !enabled;
	      };
	      setOptionEnabled("prophet", flags.forecastProphetEnabled);
	      // UI policy: hide TimeMixer from the selector and expose Quantura Horizon only.
	      setOptionEnabled("ibm_timemixer", false);

	      const available = Array.from(ui.forecastService.options).filter((opt) => !opt.disabled && !opt.hidden);
	      if (available.length === 0) {
	        ui.forecastService.disabled = true;
	      } else {
	        ui.forecastService.disabled = false;
	        const selected = ui.forecastService.selectedOptions?.[0];
	        if (!selected || selected.disabled || selected.hidden) {
	          ui.forecastService.value = available[0].value;
	        }
	      }
	    }

      updateMaintenanceModeUi(Boolean(flags.maintenanceMode));
      updateDynamicPromoBanner(String(flags.promoBannerText || "").trim());
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

    const maybeShowHolidayPromo = () => {
      if (!state.remoteFlags.holidayPromo) return;
      if (typeof window === "undefined") return;
      if (window.location.pathname !== "/") return;
      if (String(safeLocalStorageGet(HOLIDAY_PROMO_SEEN_KEY) || "") === "1") return;
      if (document.getElementById("holiday-promo")) return;

      const banner = document.createElement("section");
      banner.id = "holiday-promo";
      banner.className = "promo-banner";
      banner.innerHTML = `
        <div class="promo-inner">
          <div>
            <div class="promo-badge">Holiday promo</div>
            <div class="promo-title">Limited-time discount for new Quantura members.</div>
            <div class="small muted">Explore plans, unlock higher throughput, and ship your weekly research loop faster.</div>
          </div>
          <div class="promo-actions">
            <a class="cta small" href="/pricing" data-action="promo-cta">View plans</a>
            <button class="cta secondary small" type="button" data-action="promo-dismiss">No thanks</button>
          </div>
        </div>
      `;
      const header = document.querySelector("header.header");
      if (header && typeof header.insertAdjacentElement === "function") header.insertAdjacentElement("afterend", banner);
      else document.body.prepend(banner);

      const dismiss = banner.querySelector('[data-action="promo-dismiss"]');
      const cta = banner.querySelector('[data-action="promo-cta"]');
      dismiss?.addEventListener("click", () => {
        safeLocalStorageSet(HOLIDAY_PROMO_SEEN_KEY, "1");
        banner.remove();
        logEvent("holiday_promo_dismissed", {});
      });
      cta?.addEventListener("click", () => {
        safeLocalStorageSet(HOLIDAY_PROMO_SEEN_KEY, "1");
        logEvent("holiday_promo_clicked", {});
      });

      logEvent("holiday_promo_shown", {});
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
      maybeShowHolidayPromo();
	    subscribeRemoteConfigUpdates(rc);
	    startRemoteConfigRefreshLoop(rc);
	    logEvent("remote_config_loaded", {
	      watchlist: state.remoteFlags.watchlistEnabled,
	      prophet: state.remoteFlags.forecastProphetEnabled,
	      timemixer: state.remoteFlags.forecastTimeMixerEnabled,
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

  const buildMeta = () => ({
    sessionId: getSessionId(),
    pagePath: window.location.pathname,
    pageTitle: document.title,
    referrer: document.referrer || "",
    userAgent: navigator.userAgent,
    language: state.preferredLanguage || normalizeLanguageCode(navigator.language),
    country: state.preferredCountry || "",
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

  const setCountryControls = (countryCode) => {
    const code = normalizeCountryCode(countryCode);
    state.preferredCountry = code;
    if (ui.eventsCalendarCountry && ui.eventsCalendarCountry.value !== code) ui.eventsCalendarCountry.value = code;
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
    if (persist) safeLocalStorageSet(LANGUAGE_PREFERENCE_KEY, normalized);
  };

  const applyCountryPreference = (countryCode, { persist = true } = {}) => {
    const normalized = normalizeCountryCode(countryCode);
    setCountryControls(normalized);
    if (persist) safeLocalStorageSet(COUNTRY_PREFERENCE_KEY, normalized);
  };

  const detectCountryFromIp = async () => {
    try {
      const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      const timeout = window.setTimeout(() => controller?.abort(), 2600);
      const response = await fetch("https://ipapi.co/json/", {
        method: "GET",
        mode: "cors",
        signal: controller?.signal,
      });
      window.clearTimeout(timeout);
      if (!response.ok) return "";
      const payload = await response.json();
      const raw = String(payload?.country_code || payload?.country || "").trim();
      return normalizeCountryCode(raw);
    } catch (error) {
      return "";
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
    let country = urlCountry || (storedCountry !== "US" ? storedCountry : "");
    if (!country) {
      country = await detectCountryFromIp();
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
      state.pendingShareId = id;
      if (id) {
        safeLocalStorageSet(PENDING_SHARE_KEY, id);
        writeCookie(PENDING_SHARE_KEY, id, { days: 14 });
      } else {
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
      if (state.tradingViewTheme === "auto" && state.chartEngine === "tradingview" && state.tickerContext.ticker) {
        renderTradingViewTerminal({
          ticker: state.tickerContext.ticker,
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
      button.innerHTML = `${icon("message-text")}<span>Feedback</span>`;
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
    const authLabel = isAuthed ? "Logged In" : "Logged Out";
    ensureHeaderNotificationsCta();
    if (ui.headerAuth) {
      ui.headerAuth.innerHTML = isAuthed
        ? `${icon("dashboard")}<span>Dashboard</span>`
        : `${icon("log-in")}<span>Sign in</span>`;
      ui.headerAuth.setAttribute("aria-label", isAuthed ? "Open dashboard" : "Sign in");
      if (ui.headerAuth.tagName.toLowerCase() === "button") {
        ui.headerAuth.dataset.route = isAuthed ? "/dashboard" : "/account";
      } else {
        ui.headerAuth.setAttribute("href", isAuthed ? "/dashboard" : "/account");
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

    if (ui.userEmail) ui.userEmail.textContent = user?.email || "Not signed in";
    if (ui.userProvider) ui.userProvider.textContent = user?.providerData?.[0]?.providerId || "—";
    if (ui.userStatus) {
      ui.userStatus.textContent = authLabel;
      ui.userStatus.classList.toggle("pill", true);
    }
    if (ui.billingPortalLink) {
      ui.billingPortalLink.textContent = isAuthed ? "Open Stripe billing portal" : "Sign in to manage billing";
      ui.billingPortalLink.setAttribute("href", isAuthed ? STRIPE_URL : "/account");
      ui.billingPortalLink.setAttribute("target", isAuthed ? "_blank" : "_self");
      if (isAuthed) {
        ui.billingPortalLink.setAttribute("rel", "noopener noreferrer");
      } else {
        ui.billingPortalLink.removeAttribute("rel");
      }
    }
    ui.dashboardCta?.classList.toggle("hidden", isAuthed);
    setProfileFormEnabled(isAuthed);
    if (!isAuthed) {
      setProfileStatus("Sign in to set your public leaderboard profile.");
    }

    if (ui.pricingAuthCta) {
      ui.pricingAuthCta.innerHTML = isAuthed
        ? `${icon("dashboard")}<span>Open dashboard</span>`
        : `${icon("log-in")}<span>Sign in</span>`;
      ui.pricingAuthCta.setAttribute("href", isAuthed ? "/dashboard" : "/account");
    }

    if (ui.pricingStarterCta) {
      ui.pricingStarterCta.innerHTML = isAuthed
        ? `${icon("dashboard")}<span>Go to dashboard</span>`
        : `${icon("user-plus")}<span>Start free</span>`;
      ui.pricingStarterCta.setAttribute("href", isAuthed ? "/dashboard" : "/account");
    }

    if (ui.dashboardAuthLink) {
      ui.dashboardAuthLink.innerHTML = isAuthed
        ? `${icon("log-out")}<span>Sign out</span>`
        : `${icon("log-in")}<span>Sign in</span>`;
      ui.dashboardAuthLink.setAttribute("href", isAuthed ? "#" : "/account");
      ui.dashboardAuthLink.setAttribute("aria-label", isAuthed ? "Sign out" : "Sign in");
    }

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
      container.innerHTML = "<p class=\"small\">No orders yet. Your Deep Forecast request will appear here.</p>";
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
            <div class="order-title">${order.product || "Deep Forecast"}</div>
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
            <button class="cta secondary small" type="button" data-action="plot-forecast" data-forecast-id="${escapeHtml(item.id)}" data-ticker="${escapeHtml(item.ticker)}">
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
        const paidOrders = orders.filter((order) => {
          const status = String(order?.paymentStatus || "").trim().toLowerCase();
          return status === "paid" || status === "succeeded";
        });
        state.userHasPaidPlan = paidOrders.length > 0;
        state.userSubscriptionTier = paidOrders.some((order) =>
          String(order?.product || "").toLowerCase().includes("desk")
        )
          ? "desk"
          : paidOrders.some((order) => String(order?.product || "").toLowerCase().includes("pro"))
          ? "pro"
          : state.userHasPaidPlan
          ? "pro"
          : "free";
        renderOrderList(orders, ui.userOrders);
        refreshScreenerModelUi();
        refreshScreenerCreditsUi();
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
          renderForecastPicker(items);
	      });
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

        const params = new URLSearchParams(window.location.search);
        const urlUploadId = String(params.get("uploadId") || "").trim();
        if (urlUploadId && !state.uploadUrlLoaded) {
          state.uploadUrlLoaded = true;
          plotPredictionUploadById(db, state.clients?.storage, urlUploadId).catch(() => {});
        }
      });
  };

  const syncBacktestStrategyFields = () => {
    if (!ui.backtestStrategy) return;
    const selected = String(ui.backtestStrategy.value || "").trim();
    document.querySelectorAll("[data-backtest-strategy]").forEach((el) => {
      const key = String(el.dataset.backtestStrategy || "").trim();
      el.classList.toggle("hidden", key && key !== selected);
    });
  };

  const renderBacktestSourceControls = (backtestId, selectedKey = "python") => {
    const options = BACKTEST_SOURCE_OPTIONS.map(
      (item) =>
        `<option value="${item.key}" ${item.key === selectedKey ? "selected" : ""}>${item.label}</option>`
    ).join("");
    return `
      <div class="backtest-source-controls">
        <label class="label" for="backtest-source-${escapeHtml(backtestId)}">Download source</label>
        <div class="backtest-source-row">
          <select id="backtest-source-${escapeHtml(backtestId)}" data-backtest-source-format data-backtest-id="${escapeHtml(backtestId)}">
            ${options}
          </select>
          <button class="cta secondary small" type="button" data-action="download-backtest-source" data-backtest-id="${escapeHtml(
            backtestId
          )}">${icon("download")}<span>Download Source</span></button>
        </div>
      </div>
    `;
  };

  const resolveBacktestSourceExport = (doc, selectedKey) => {
    const key = String(selectedKey || "python").trim().toLowerCase();
    const exports = doc?.exportSources && typeof doc.exportSources === "object" ? doc.exportSources : {};
    const candidate = exports?.[key] && typeof exports[key] === "object" ? exports[key] : null;
    if (candidate?.content) {
      return {
        content: String(candidate.content),
        filename: String(candidate.filename || ""),
        mimeType: String(candidate.mimeType || ""),
      };
    }
    if (key === "python") {
      const fallback = String(doc?.code || "").trim();
      if (fallback) {
        return {
          content: fallback,
          filename: "",
          mimeType: "text/x-python",
        };
      }
    }
    return null;
  };

  const renderBacktestPicker = (items) => {
    if (!ui.backtestLoadSelect) return;
    const list = Array.isArray(items) ? items : [];
    ui.backtestLoadSelect.innerHTML = `<option value="">Select a backtest</option>`;
    list.slice(0, 60).forEach((item) => {
      const opt = document.createElement("option");
      opt.value = item.id;
      const createdLabel = item.createdAt ? formatTimestamp(item.createdAt) : "";
      opt.textContent = `${item.title || item.ticker || "Backtest"}${createdLabel ? ` · ${createdLabel}` : ""}`;
      ui.backtestLoadSelect.appendChild(opt);
    });
  };

  const renderBacktestList = (items) => {
    if (!ui.savedBacktestsList) return;
    const list = Array.isArray(items) ? items : [];
    ui.savedBacktestsList.innerHTML = "";
    if (!list.length) {
      ui.savedBacktestsList.innerHTML = `<div class="small muted">No backtests yet.</div>`;
      return;
    }

    list.slice(0, 30).forEach((item) => {
      const card = document.createElement("div");
      card.className = "order-card";
      const title = escapeHtml(item.title || item.ticker || "Backtest");
      const strategy = escapeHtml(item.strategy || "");
      const interval = escapeHtml(item.interval || "");
      const created = item.createdAt ? formatTimestamp(item.createdAt) : "";
      const metrics = item.metrics || {};
      const returnPct = typeof metrics.ReturnPct === "number" ? `${metrics.ReturnPct.toFixed(2)}%` : "—";
      const sharpe = typeof metrics.Sharpe === "number" ? metrics.Sharpe.toFixed(2) : "—";
      const maxDd = typeof metrics.MaxDrawdownPct === "number" ? `${metrics.MaxDrawdownPct.toFixed(2)}%` : "—";
      const trades = typeof metrics.Trades === "number" ? String(metrics.Trades) : "—";

      card.innerHTML = `
        <div class="order-header">
          <div>
            <div class="order-title">${title}</div>
            <div class="small">ID: ${escapeHtml(item.id)}</div>
          </div>
          <span class="status completed">saved</span>
        </div>
        <div class="order-meta">
          ${created ? `<div><strong>Created</strong> ${escapeHtml(created)}</div>` : ""}
          ${item.ticker ? `<div><strong>Ticker</strong> ${escapeHtml(item.ticker)}</div>` : ""}
          ${interval ? `<div><strong>Interval</strong> ${interval}</div>` : ""}
          ${strategy ? `<div><strong>Strategy</strong> ${strategy}</div>` : ""}
          <div><strong>Return</strong> ${escapeHtml(returnPct)} · <strong>Sharpe</strong> ${escapeHtml(sharpe)} · <strong>Max DD</strong> ${escapeHtml(
            maxDd
          )} · <strong>Trades</strong> ${escapeHtml(trades)}</div>
        </div>
        <div class="order-actions" style="display:flex; gap:10px; flex-wrap:wrap;">
          <button class="cta secondary small" type="button" data-action="plot-backtest" data-backtest-id="${escapeHtml(item.id)}">Load</button>
          <button class="cta secondary small" type="button" data-action="rename-backtest" data-backtest-id="${escapeHtml(item.id)}">Rename</button>
          <button class="cta secondary small danger" type="button" data-action="delete-backtest" data-backtest-id="${escapeHtml(item.id)}">Delete</button>
        </div>
        ${renderBacktestSourceControls(item.id, "python")}
      `;
      ui.savedBacktestsList.appendChild(card);
    });
  };

  const renderBacktestDetails = async (doc, { imageUrl }) => {
    if (!ui.backtestOutput) return;
    const metrics = doc.metrics || {};
    const ret = typeof metrics.ReturnPct === "number" ? `${metrics.ReturnPct.toFixed(2)}%` : "—";
    const sharpe = typeof metrics.Sharpe === "number" ? metrics.Sharpe.toFixed(2) : "—";
    const maxDd = typeof metrics.MaxDrawdownPct === "number" ? `${metrics.MaxDrawdownPct.toFixed(2)}%` : "—";
    const trades = typeof metrics.Trades === "number" ? String(metrics.Trades) : "—";
    const winRate = typeof metrics.WinRatePct === "number" ? `${metrics.WinRatePct.toFixed(1)}%` : "—";

    const title = escapeHtml(doc.title || doc.ticker || "Backtest");
    const code = String(doc.code || "").trim();
    const codeMarkup = code
      ? `
        <details class="learn-more">
          <summary>Source code (generated)</summary>
          <pre class="code-block">${escapeHtml(code)}</pre>
        </details>
      `
      : "";

    ui.backtestOutput.innerHTML = `
      <div class="card">
        <div class="card-head">
          <h3>${title}</h3>
        </div>
        <div class="small muted" style="margin-bottom: 12px;">
          ${escapeHtml(doc.ticker || "")}${doc.interval ? ` · ${escapeHtml(doc.interval)}` : ""}${doc.strategy ? ` · ${escapeHtml(doc.strategy)}` : ""}
        </div>
        <div class="metrics metrics-tight">
          <div class="stat-card"><strong>${escapeHtml(ret)}</strong><span class="small">Return</span></div>
          <div class="stat-card"><strong>${escapeHtml(sharpe)}</strong><span class="small">Sharpe</span></div>
          <div class="stat-card"><strong>${escapeHtml(maxDd)}</strong><span class="small">Max drawdown</span></div>
          <div class="stat-card"><strong>${escapeHtml(trades)}</strong><span class="small">Trades</span></div>
          <div class="stat-card"><strong>${escapeHtml(winRate)}</strong><span class="small">Win rate</span></div>
        </div>
        ${
          imageUrl
            ? `<div style="margin-top: 14px;"><img class="backtest-image" src="${escapeHtml(imageUrl)}" alt="Backtest equity curve" loading="lazy" /></div>`
            : `<div class="small muted" style="margin-top: 14px;">Chart unavailable.</div>`
        }
        <div style="margin-top: 14px;">${renderBacktestSourceControls(doc.id, "python")}</div>
        ${codeMarkup}
      </div>
    `;
  };

  const loadBacktestById = async (db, storage, backtestId) => {
    if (!db || !backtestId) throw new Error("Backtest ID is required.");
    if (!ui.backtestOutput) return;
    const cleanId = String(backtestId || "").trim();
    if (!cleanId) throw new Error("Backtest ID is required.");
    setOutputLoading(ui.backtestOutput, "Loading backtest...");

    const snap = await db.collection("backtests").doc(cleanId).get();
    if (!snap.exists) throw new Error("Backtest not found.");
    const doc = { id: snap.id, ...(snap.data() || {}) };

    let imageUrl = "";
    if (storage && doc.imagePath) {
      try {
        imageUrl = await storage.ref().child(String(doc.imagePath)).getDownloadURL();
      } catch (error) {
        imageUrl = "";
      }
    }

    await renderBacktestDetails(doc, { imageUrl });
    setOutputReady(ui.backtestOutput);
    logEvent("backtest_loaded", { backtest_id: cleanId });
  };

  const startBacktests = (db, user) => {
    if (state.unsubscribeBacktests) state.unsubscribeBacktests();
    if (!user || !ui.savedBacktestsList || !ui.backtestLoadSelect) return;
    state.unsubscribeBacktests = db
      .collection("backtests")
      .where("userId", "==", user.uid)
      .orderBy("createdAt", "desc")
      .limit(60)
      .onSnapshot(
        (snapshot) => {
          const items = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
          renderBacktestList(items);
          renderBacktestPicker(items);

          const params = new URLSearchParams(window.location.search);
          const urlBacktestId = String(params.get("backtestId") || params.get("id") || "").trim();
          if (urlBacktestId && !state.backtestUrlLoaded) {
            state.backtestUrlLoaded = true;
            loadBacktestById(db, state.clients?.storage, urlBacktestId).catch(() => {});
          }
        },
        () => {
          renderBacktestList([]);
          renderBacktestPicker([]);
        }
      );
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
    const stripeConnectAccountId = String(safeProfile.stripeConnectAccountId || "").trim();
    state.userProfile = { username, socialLinks, avatar, bio, publicProfile, publicScreenerSharing, stripeConnectAccountId };

    if (ui.profileUsername) ui.profileUsername.value = username;
    if (ui.profileAvatar) ui.profileAvatar.value = avatar;
    if (ui.profileBio) ui.profileBio.value = bio;
    if (ui.profilePublicEnabled) ui.profilePublicEnabled.checked = publicProfile;
    if (ui.profilePublicScreener) ui.profilePublicScreener.checked = publicScreenerSharing;
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
    const stripeConnectAccountId = String(
      existingProfile.stripeConnectAccountId || existing?.stripeConnectAccountId || ""
    ).trim();

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
          stripeConnectAccountId,
        },
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

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");

  const icon = (name) => `<i class="iconoir-${name}" aria-hidden="true"></i>`;

  const toPrettyJson = (value) => `<pre class="small">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;

  const normalizeTopNavigation = () => {
    const navs = Array.from(document.querySelectorAll(".header .nav-links"));
    if (!navs.length) return;
    navs.forEach((nav) => {
      nav.innerHTML = `
        <a href="/forecasting" data-analytics="nav_terminal">${icon("candlestick-chart")}<span>Terminal</span></a>
        <a href="/research" data-analytics="nav_research">${icon("bookmark-book")}<span>Research</span></a>
        <a href="/blog" data-analytics="nav_blog">${icon("page")}<span>Blog</span></a>
        <a href="/pricing" data-analytics="nav_pricing">${icon("wallet")}<span>Pricing</span></a>
        <a href="/contact" data-analytics="nav_contact">${icon("mail")}<span>Contact</span></a>
      `;
    });
  };

  const ensureHeaderNotificationsCta = () => {
    const actions = document.querySelector(".header .nav-actions");
    if (!actions) return;
    let link = document.getElementById("header-notifications");
    if (!link) {
      link = document.createElement("a");
      link.id = "header-notifications";
      link.className = "cta secondary";
      link.setAttribute("data-analytics", "nav_notifications");
      const authButton = actions.querySelector("#header-auth");
      if (authButton?.parentElement === actions) {
        actions.insertBefore(link, authButton);
      } else {
        actions.appendChild(link);
      }
      ui.headerNotifications = link;
    }
    const authed = Boolean(state.user);
    link.href = authed ? "/notifications" : "/account";
    link.innerHTML = `${icon("bell-notification")}<span>Notifications</span>`;
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
        return `
          <article class="notification-log-item">
            <div class="notification-log-head">
              <strong>${title}</strong>
              <span class="small muted">${at}</span>
            </div>
            <p class="small">${body || "No message body provided."}</p>
            <div class="small muted">Source: ${source}</div>
          </article>
        `;
      })
      .join("");
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

  const setChartEngine = (engine, { persist = true, rerender = true } = {}) => {
    const next = engine === "legacy" ? "legacy" : "tradingview";
    state.chartEngine = next;
    if (persist) safeLocalStorageSet(CHART_ENGINE_CACHE_KEY, next);
    setTerminalChartEngineVisibility(next);
    applyChartControlState();
    if (
      rerender &&
      state.tickerContext.rows?.length &&
      state.tickerContext.ticker &&
      ui.tickerChart
    ) {
      renderTickerChart(
        state.tickerContext.rows,
        state.tickerContext.ticker,
        state.tickerContext.interval,
        buildCurrentChartOverlays()
      ).catch(() => {});
    }
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

  const renderTradingViewTerminal = ({ ticker, interval }) => {
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
      container: ui.tradingViewProfile,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/symbol-profile/?locale=en", {
        ...shared,
        width: "100%",
        height: "100%",
      }),
      title: `Company profile ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewTechnical,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/technical-analysis/?locale=en", {
        ...shared,
        interval: interval === "1h" ? "60m" : "1D",
        width: "100%",
        height: "100%",
        showIntervalTabs: true,
        displayMode: "single",
      }),
      title: `Technical analysis ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewTimeline,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/timeline/?locale=en", {
        ...shared,
        width: "100%",
        height: 600,
        displayMode: "regular",
      }),
      title: `Timeline ${symbol}`,
    });

    mountTradingViewIframe({
      container: ui.tradingViewFinancials,
      src: buildTradingViewWidgetSrc("https://www.tradingview-widget.com/embed-widget/financials/?locale=en", {
        ...shared,
        width: "100%",
        height: 775,
        displayMode: "regular",
      }),
      title: `Financials ${symbol}`,
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
        if (state.tradingViewRenderNonce !== nonce) return;
        state.tradingViewLoadFailed = true;
        setTradingViewStatus("TradingView unavailable");
        ui.tradingViewFallback?.classList.remove("hidden");
      },
    });

    state.tradingViewLoadTimer = window.setTimeout(() => {
      if (state.tradingViewRenderNonce !== nonce) return;
      state.tradingViewLoadFailed = true;
      setTradingViewStatus("TradingView timeout · fallback available");
      ui.tradingViewFallback?.classList.remove("hidden");
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
    ui.chartEngineButtons.forEach((button) => {
      const engine = String(button.dataset.chartEngine || "").toLowerCase();
      button.classList.toggle("active", engine === state.chartEngine);
      button.setAttribute("aria-pressed", engine === state.chartEngine ? "true" : "false");
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

    if (ui.chartEngineButtons.length) {
      ui.chartEngineButtons.forEach((button) => {
        button.addEventListener("click", () => {
          const next = String(button.dataset.chartEngine || "").toLowerCase();
          if (!next || next === state.chartEngine) return;
          setChartEngine(next, { persist: true, rerender: true });
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
          if (state.chartEngine === "tradingview" && state.tickerContext.ticker) {
            renderTradingViewTerminal({
              ticker: state.tickerContext.ticker,
              interval: state.tickerContext.interval || "1d",
            });
          }
        });
      });
    }

    if (ui.tradingViewUseLegacy) {
      ui.tradingViewUseLegacy.addEventListener("click", () => {
        setChartEngine("legacy", { persist: true, rerender: true });
        showToast("Switched to legacy chart.");
      });
    }

    setTerminalChartEngineVisibility(state.chartEngine);
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

  const processPendingShareImport = async (functions) => {
    if (!functions || !state.user) return null;
    const shareId = String(getPendingShareId() || "").trim();
    if (!shareId) return null;
    if (state.pendingShareProcessed) return null;
    state.pendingShareProcessed = true;

    try {
      setPendingShareId(shareId);
      const importShare = functions.httpsCallable("import_shared_item");
      const result = await importShare({ shareId, meta: buildMeta() });
      const kind = String(result.data?.kind || "").trim().toLowerCase();
      const importedId = String(result.data?.importedId || "").trim();
      if (!kind || !importedId) throw new Error("Shared item import did not return an ID.");

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

  const syncTickerInputs = (ticker) => {
    const t = normalizeTicker(ticker);
    if (!t) return;
    const ids = [
      "forecast-ticker",
      "technicals-ticker",
      "download-ticker",
      "news-ticker",
      "intel-ticker",
      "options-ticker",
      "events-calendar-tickers",
      "ticker-query-ticker",
      "watchlist-ticker",
      "alert-ticker",
      "autopilot-ticker",
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
    `;

    if (ui.tickerIntelligenceOutput) {
      ui.tickerIntelligenceOutput.innerHTML = institutionalHtml;
    }
  };

  const loadTickerIntel = async (functions, ticker, { notify = false, force = false } = {}) => {
    if (!functions || (!ui.intelOutput && !ui.tickerIntelligenceOutput)) return;
    const symbol = normalizeTicker(ticker);
    if (!symbol) {
      if (ui.intelOutput) ui.intelOutput.innerHTML = `<div class="small muted">Load a ticker to see company context.</div>`;
      if (ui.tickerIntelligenceOutput) {
        ui.tickerIntelligenceOutput.innerHTML = `<div class="small muted">Load a ticker to generate institutional intelligence.</div>`;
      }
      return;
    }
    if (!force && state.tickerContext.intelTicker === symbol) return;
    state.tickerContext.intelTicker = symbol;

    try {
      if (ui.intelOutput) setOutputLoading(ui.intelOutput, "Loading company context...");
      if (ui.tickerIntelligenceOutput) setOutputLoading(ui.tickerIntelligenceOutput, "Loading institutional intelligence...");
      const getIntel = functions.httpsCallable("get_ticker_intel");
      const result = await getIntel({ ticker: symbol, meta: buildMeta() });
      if (ui.intelOutput) setOutputReady(ui.intelOutput);
      if (ui.tickerIntelligenceOutput) setOutputReady(ui.tickerIntelligenceOutput);
      renderTickerIntel(result.data || {});
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

  const renderTickerXTrends = (posts, ticker, warning = "") => {
    if (!ui.xTrendingOutput) return;
    const list = Array.isArray(posts) ? posts : [];
    const symbol = normalizeTicker(ticker) || "";
    const warningText = String(warning || "").trim();

    if (!list.length) {
      ui.xTrendingOutput.innerHTML = `
        <div class="small muted">No X posts returned for ${escapeHtml(symbol || "this ticker")}.</div>
        ${warningText ? `<div class="small muted" style="margin-top:8px;">${escapeHtml(warningText)}</div>` : ""}
      `;
      return;
    }

    ui.xTrendingOutput.innerHTML = list
      .slice(0, 8)
      .map((post) => {
        const authorName = escapeHtml(post.authorName || post.authorUsername || "Unknown");
        const authorHandle = escapeHtml(post.authorUsername ? `@${post.authorUsername}` : "");
        const text = escapeHtml(post.text || "");
        const created = formatEpoch(post.createdAt);
        const metrics = post.metrics && typeof post.metrics === "object" ? post.metrics : {};
        const likes = Number(metrics.like_count || 0);
        const reposts = Number(metrics.retweet_count || 0);
        const replies = Number(metrics.reply_count || 0);
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
      .join("");
  };

  const loadTickerXTrends = async (functions, ticker, { notify = false, force = false } = {}) => {
    if (!functions || !ui.xTrendingOutput) return;
    const symbol = normalizeTicker(ticker);
    if (!symbol) {
      ui.xTrendingOutput.innerHTML = `<div class="small muted">Load a ticker to see live X discussion.</div>`;
      return;
    }
    if (!force && state.tickerContext.xTicker === symbol) return;
    state.tickerContext.xTicker = symbol;

    try {
      setOutputLoading(ui.xTrendingOutput, "Loading X trends...");
      const getXTrends = functions.httpsCallable("get_ticker_x_trends");
      const result = await getXTrends({ ticker: symbol, meta: buildMeta() });
      const payload = result.data || {};
      const posts = Array.isArray(payload.posts) ? payload.posts : [];
      const warning = String(payload.warning || "").trim();
      setOutputReady(ui.xTrendingOutput);
      renderTickerXTrends(posts, symbol, warning);
      logEvent("x_trends_loaded", { ticker: symbol, count: posts.length });
    } catch (error) {
      setOutputReady(ui.xTrendingOutput);
      ui.xTrendingOutput.innerHTML = `<div class="small muted">Unable to load X trends right now.</div>`;
      if (notify) showToast(error.message || "Unable to load X trends.", "warn");
    }
  };

  const normalizeTickerListInput = (raw) =>
    String(raw || "")
      .split(/[,\s]+/)
      .map((item) => normalizeTicker(item))
      .filter(Boolean)
      .slice(0, 30);

  const renderCorporateEventsCalendar = (payload) => {
    if (!ui.eventsCalendarOutput) return;
    const events = Array.isArray(payload?.events) ? payload.events : [];
    const source = String(payload?.source || "unknown").trim();
    const warnings = Array.isArray(payload?.warnings) ? payload.warnings.filter(Boolean) : [];
    if (!events.length) {
      ui.eventsCalendarOutput.innerHTML = `
        <div class="small muted">No events returned for this window.</div>
        ${warnings.length ? `<div class="small muted" style="margin-top:8px;">${escapeHtml(warnings.join(" "))}</div>` : ""}
      `;
      return;
    }
    ui.eventsCalendarOutput.innerHTML = `
      <div class="small muted" style="margin-bottom:10px;">Source: ${escapeHtml(source)} · Events: ${events.length}</div>
      ${
        warnings.length
          ? `<div class="small muted" style="margin-bottom:10px;">${escapeHtml(warnings.join(" "))}</div>`
          : ""
      }
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Ticker</th>
              <th>Event</th>
              <th>Type</th>
              <th>Status</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody>
            ${events
              .map((row) => {
                const rowDate = escapeHtml(String(row?.date || "—"));
                const rowTicker = escapeHtml(normalizeTicker(row?.ticker || "") || "—");
                const rowName = escapeHtml(String(row?.name || row?.companyName || "Corporate event"));
                const rowType = escapeHtml(String(row?.type || "—"));
                const rowStatus = escapeHtml(String(row?.status || "scheduled"));
                const rowSource = escapeHtml(String(row?.source || source || "—"));
                return `
                  <tr>
                    <td>${rowDate}</td>
                    <td>${rowTicker}</td>
                    <td>${rowName}</td>
                    <td>${rowType}</td>
                    <td>${rowStatus}</td>
                    <td>${rowSource}</td>
                  </tr>
                `;
              })
              .join("")}
          </tbody>
        </table>
      </div>
    `;
  };

  const loadCorporateEventsCalendar = async (functions, { force = false, notify = false } = {}) => {
    if (!functions || !ui.eventsCalendarOutput) return;

    const tickerSeed = normalizeTicker(state.tickerContext.ticker || safeLocalStorageGet(LAST_TICKER_KEY) || "");
    const inputTickers = normalizeTickerListInput(ui.eventsCalendarTickers?.value || "");
    const tickers = inputTickers.length ? inputTickers : tickerSeed ? [tickerSeed] : [];
    const country = normalizeCountryCode(ui.eventsCalendarCountry?.value || state.preferredCountry || "US");
    const startDate = String(ui.eventsCalendarStart?.value || "").trim();
    const endDate = String(ui.eventsCalendarEnd?.value || "").trim();
    const limitRaw = Number(ui.eventsCalendarLimit?.value || 120);
    const limit = Number.isFinite(limitRaw) ? Math.max(10, Math.min(500, limitRaw)) : 120;
    const requestKey = JSON.stringify({ tickers, country, startDate, endDate, limit });
    if (!force && ui.eventsCalendarOutput.dataset.requestKey === requestKey) return;
    ui.eventsCalendarOutput.dataset.requestKey = requestKey;

    try {
      if (ui.eventsCalendarStatus) ui.eventsCalendarStatus.textContent = "Loading calendar...";
      setOutputLoading(ui.eventsCalendarOutput, "Loading corporate events...");
      const getEvents = functions.httpsCallable("get_corporate_events_calendar");
      const result = await getEvents({
        tickers,
        ticker: tickers[0] || "",
        country,
        startDate,
        endDate,
        limit,
        meta: buildMeta(),
      });
      setOutputReady(ui.eventsCalendarOutput);
      renderCorporateEventsCalendar(result.data || {});
      if (ui.eventsCalendarStatus) {
        const source = String(result.data?.source || "unknown");
        const fallback = Boolean(result.data?.fallbackUsed);
        ui.eventsCalendarStatus.textContent = fallback
          ? `Loaded from ${source} (fallback mode).`
          : `Loaded from ${source}.`;
      }
      logEvent("corporate_events_loaded", {
        country,
        tickers_count: tickers.length,
        source: String(result.data?.source || ""),
      });
    } catch (error) {
      setOutputReady(ui.eventsCalendarOutput);
      ui.eventsCalendarOutput.innerHTML = `<div class="small muted">Unable to load corporate events right now.</div>`;
      if (ui.eventsCalendarStatus) ui.eventsCalendarStatus.textContent = "Unable to load corporate events.";
      if (notify) showToast(error.message || "Unable to load corporate events.", "warn");
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

  const renderTickerQueryResult = (payload) => {
    if (!ui.tickerQueryOutput) return;
    const answer = escapeHtml(String(payload?.answer || "").trim() || "No answer returned.");
    const model = escapeHtml(String(payload?.model || "gpt-5"));
    const provider = escapeHtml(String(payload?.provider || "openai"));
    const context = payload?.context && typeof payload.context === "object" ? payload.context : {};
    const sector = escapeHtml(String(context.sector || "").trim());
    const industry = escapeHtml(String(context.industry || "").trim());
    const exchange = escapeHtml(String(context.exchange || "").trim());
    const headlines = Array.isArray(context.headlines) ? context.headlines : [];

    ui.tickerQueryOutput.innerHTML = `
      <div class="small muted">Provider: ${provider} · Model: ${model}</div>
      <div class="small" style="margin-top:10px; white-space:pre-wrap;">${answer}</div>
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
    `;
  };

  const loadTickerQueryInsight = async (functions, { ticker, question, notify = false } = {}) => {
    if (!functions || !ui.tickerQueryOutput) return;
    const symbol = normalizeTicker(ticker || ui.tickerQueryTicker?.value || state.tickerContext.ticker || "");
    const prompt = String(question || ui.tickerQueryQuestion?.value || "").trim();
    const languageRaw = normalizeLanguageCode(ui.tickerQueryLanguage?.value || state.preferredLanguage || "en");
    const language = languageRaw === "auto" ? state.preferredLanguage || "en" : languageRaw;
    if (!symbol) {
      showToast("Ticker is required for GPT-5 query.", "warn");
      return;
    }
    if (!prompt) {
      showToast("Enter a question for GPT-5.", "warn");
      return;
    }
    try {
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Querying GPT-5...";
      setOutputLoading(ui.tickerQueryOutput, "Running GPT-5 ticker query...");
      const queryInsight = functions.httpsCallable("query_ticker_insight");
      const result = await queryInsight({ ticker: symbol, question: prompt, language, meta: buildMeta() });
      setOutputReady(ui.tickerQueryOutput);
      renderTickerQueryResult(result.data || {});
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Completed.";
      logEvent("ticker_query_completed", { ticker: symbol, language });
    } catch (error) {
      setOutputReady(ui.tickerQueryOutput);
      ui.tickerQueryOutput.innerHTML = `<div class="small muted">Unable to complete ticker query right now.</div>`;
      if (ui.tickerQueryStatus) ui.tickerQueryStatus.textContent = "Unable to complete query.";
      if (notify) showToast(error.message || "Unable to run ticker query.", "warn");
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

    if (!state.user) {
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

  const renderTrendingTickers = (payload) => {
    if (!ui.trendingList) return;
    const items = Array.isArray(payload?.items) ? payload.items : [];
    const tickers = Array.isArray(payload?.tickers) ? payload.tickers : [];

    const rows = items.length
      ? items
      : tickers.map((symbol) => ({
          symbol,
          lastClose: null,
          changePct: null,
        }));

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

        const changeNum = typeof changePct === "number" ? changePct : Number(changePct);
        const changeOk = Number.isFinite(changeNum);
        const direction = !changeOk ? "flat" : changeNum < 0 ? "down" : "up";
        const changeLabel = changeOk ? formatPercent(changeNum, { signed: true, digits: 2 }) : "Quote unavailable";
        const absChange = typeof change === "number" ? change : Number(change);
        const absChangeLabel = Number.isFinite(absChange) ? `${absChange > 0 ? "+" : ""}${absChange.toFixed(2)}` : "";

        const priceLabel = lastClose !== null && lastClose !== undefined ? formatUsd(lastClose) : "—";
        const subLabel = absChangeLabel && changeOk ? `${absChangeLabel} · ${changeLabel}` : changeLabel;

        return `
          <button class="trending-card" type="button" data-action="pick-ticker" data-ticker="${escapeHtml(symbol)}">
            <div class="trending-top">
              <div class="trending-symbol">${escapeHtml(symbol)}</div>
              <div class="trending-price">${escapeHtml(priceLabel)}</div>
            </div>
            <div class="trending-bottom">
              <span class="trending-chip trending-chip--${direction}">${escapeHtml(subLabel)}</span>
              <span class="small muted">Tap to load</span>
            </div>
          </button>
        `;
      })
      .join("");
  };

  const loadTrendingTickers = async (functions, { notify = false, force = false } = {}) => {
    if (!functions || !ui.trendingList) return;
    try {
      setOutputLoading(ui.trendingList, "Loading trending tickers...");
      const getTrending = functions.httpsCallable("get_trending_tickers");
      const result = await getTrending({ force: Boolean(force), meta: buildMeta() });
      setOutputReady(ui.trendingList);
      renderTrendingTickers(result.data || {});
      const count = Array.isArray(result.data?.tickers) ? result.data.tickers.length : 0;
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

	  const renderTickerChart = async (rows, ticker, interval, overlays = []) => {
	    if (!ui.tickerChart) return;
      const cleanTicker = normalizeTicker(ticker || state.tickerContext.ticker || "") || "AAPL";
      const hasOverlays = Array.isArray(overlays) && overlays.length > 0;

      if (!rows?.length) {
        ui.tickerChart.textContent = "No price data to plot.";
        return;
      }

      if (state.chartEngine === "tradingview" && !hasOverlays) {
        const rendered = renderTradingViewTerminal({ ticker: cleanTicker, interval });
        if (rendered) {
          setTerminalChartEngineVisibility("tradingview");
          return;
        }
      }

      if (state.chartEngine === "tradingview" && hasOverlays) {
        setChartEngine("legacy", { persist: false, rerender: false });
        setTerminalStatus("Showing legacy chart for Quantura overlays. Switch back to TradingView anytime.");
      } else {
        setTerminalChartEngineVisibility("legacy");
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
    if (!state.user) throw new Error("Sign in to run the OpenAI CSV Agent.");
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
    if (key === "ibm_timemixer") return "IBM TimeMixer";
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
    };
    if (aliases[lower]) return aliases[lower];
    if (lower.startsWith("gpt-") && lower.charAt(4) === "4") {
      return "gpt-5-mini";
    }
    if (lower.startsWith("o1")) {
      return "gpt-5.1";
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
    if (!state.user) return "free";
    if (String(state.user.email || "").toLowerCase() === String(ADMIN_EMAIL).toLowerCase()) return "desk";
    if (!state.userHasPaidPlan) return "free";
    return state.userSubscriptionTier || "pro";
  };

  const getCurrentAiTierConfig = () => {
    const key = getCurrentAiTierKey();
    const tiers = state.remoteFlags.aiUsageTiers && typeof state.remoteFlags.aiUsageTiers === "object"
      ? state.remoteFlags.aiUsageTiers
      : AI_USAGE_TIER_DEFAULTS;
    const config =
      tiers[key] && typeof tiers[key] === "object"
        ? tiers[key]
        : AI_USAGE_TIER_DEFAULTS[key] || AI_USAGE_TIER_DEFAULTS.free;
    const rawAllowed = Array.isArray(config.allowed_models) ? config.allowed_models : [];
    const allowedModels = rawAllowed
      .map((x) => normalizeAiModelId(String(x).trim()))
      .filter((modelId) => modelId && modelId.toLowerCase().startsWith("gpt-5"));
    const fallbackAllowed = (AI_USAGE_TIER_DEFAULTS[key]?.allowed_models || AI_USAGE_TIER_DEFAULTS.free.allowed_models)
      .map((x) => normalizeAiModelId(String(x).trim()))
      .filter(Boolean);
    const weeklyLimitRaw = Number(config.weekly_limit ?? config.daily_limit ?? AI_USAGE_TIER_DEFAULTS[key]?.weekly_limit ?? 3);
    const weeklyLimit = Number.isFinite(weeklyLimitRaw) ? Math.max(1, weeklyLimitRaw) : 3;
    return {
      key,
      allowedModels: allowedModels.length ? allowedModels : fallbackAllowed,
      weeklyLimit,
      dailyLimit: weeklyLimit, // Legacy alias for older UI helpers.
      volatilityAlerts: Boolean(config.volatility_alerts),
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
      const tierLabel = tier.key === "desk" ? "Desk" : tier.key === "pro" ? "Pro" : "Free";
      ui.screenerModelMeta.textContent = `${tierLabel} tier · ${tier.weeklyLimit} weekly credits · GPT-5 personalities`;
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
    const provider = String(agent?.modelProvider || "openai").trim().toLowerCase();
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
    return `
      <span class="model-badge model-badge-openai">
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
      return `<div class="small muted">Generate an AI Portfolio to score long-term growth with Quantura Horizon and publish a leaderboard-ready AI Agent.</div>`;
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
      container.innerHTML = `<div class="small muted">No AI Agents for this filter yet. Generate one from the latest screen to publish into the leaderboard.</div>`;
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
      ui.screenerOutput.innerHTML = `
        <div class="small muted">No screener rows stored for this run.</div>
      `;
      return;
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

    ui.screenerOutput.innerHTML = `
      ${title ? `<div class="small"><strong>Title:</strong> ${escapeHtml(title)}</div>` : ""}
      <div class="small"><strong>Run ID:</strong> ${runId || "—"}</div>
      <div class="small"><strong>Created:</strong> ${createdAt}</div>
      <div class="small"><strong>Visibility:</strong> ${isPublic ? "Public" : "Private"}</div>
      ${
        ownerUsername || ownerBio
          ? `<div class="small muted"><strong>Owner:</strong> ${escapeHtml(ownerAvatarMeta.emoji || "")} ${
              ownerUsername ? `@${ownerUsername}` : "workspace member"
            }${ownerBio ? ` · ${ownerBio}` : ""}</div>`
          : ""
      }
      ${notes ? `<div class="small" style="margin-top:10px;"><strong>Notes:</strong> ${escapeHtml(notes)}</div>` : ""}
      <div class="hero-actions" style="margin-top:12px;">
        <button class="cta secondary small" type="button" data-action="download-screener" data-run-id="${runId}">Download CSV</button>
        <button class="cta secondary small" type="button" data-action="rename-screener" data-run-id="${runId}">Rename</button>
        <button class="cta secondary small" type="button" data-action="share-screener" data-run-id="${runId}">Share link</button>
        <button class="cta secondary small" type="button" data-action="toggle-screener-public" data-run-id="${runId}" data-is-public="${
          isPublic ? "1" : "0"
        }">${isPublic ? "Make private" : "Publish public"}</button>
        ${
          canSupportOwner
            ? `<button class="cta secondary small" type="button" data-action="screener-owner-thanks" data-creator-workspace-id="${escapeHtml(
                ownerWorkspaceId
              )}" data-target-id="${runId}">Send thanks</button>
               <button class="cta secondary small" type="button" data-action="screener-owner-subscribe" data-creator-workspace-id="${escapeHtml(
                 ownerWorkspaceId
               )}" data-target-id="${runId}">Subscribe</button>`
            : ""
        }
        <button class="cta secondary small danger" type="button" data-action="delete-screener" data-run-id="${runId}">Delete</button>
      </div>
      <div class="card" style="margin-top:14px;">
        <div class="card-head">
          <h3>AI Portfolio</h3>
          <div class="hero-actions" style="margin-top:0;">
            <button class="cta secondary small" type="button" data-action="generate-ai-portfolio" data-run-id="${runId}">${icon("magic-wand")}<span>Generate with Quantura Horizon</span></button>
            <button class="cta secondary small" type="button" data-action="rename-ai-agent" data-agent-id="${agentId}" ${agentId ? "" : "disabled"}>${icon("edit-pencil")}<span>Rename Agent</span></button>
          </div>
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
                    <td>${formatRoiPercent(toFiniteOrNull(row.projectedRoi))}</td>
                  </tr>
                `
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
    const requestId = String(forecastResult.data?.requestId || "").trim();
    if (!requestId) return null;

    const forecastSnap = await db.collection("forecast_requests").doc(requestId).get();
    if (!forecastSnap.exists) return null;
    const forecastDoc = forecastSnap.data() || {};
    const forecastRows = Array.isArray(forecastDoc.forecastRows) ? forecastDoc.forecastRows : [];
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
    if (!state.user) {
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
    if (!state.user) {
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
    if (!state.user) {
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
    showToast("AI Portfolio generated and published to leaderboard.");
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
      .onSnapshot((snapshot) => {
        state.aiFollowSet = new Set(snapshot.docs.map((doc) => String(doc.data()?.agentId || "").trim()).filter(Boolean));
        renderAIAgentLeaderboard(state.aiAgents);
      });

    state.unsubscribeAILikes = db
      .collection("users")
      .doc(workspaceId)
      .collection("ai_agent_likes")
      .where("userId", "==", userId)
      .onSnapshot((snapshot) => {
        state.aiLikeSet = new Set(snapshot.docs.map((doc) => String(doc.data()?.agentId || "").trim()).filter(Boolean));
        renderAIAgentLeaderboard(state.aiAgents);
      });
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
            website: "https://quantura-e2e3d.web.app/",
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
    const email = String(state.user.email || "").trim().toLowerCase();
    if (email !== String(ADMIN_EMAIL).trim().toLowerCase()) return;

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
    if (!state.user) throw new Error("Sign in to load saved runs.");

    const cleanId = String(runId || "").trim();
    if (!cleanId) throw new Error("Run ID is required.");

    setOutputLoading(ui.screenerOutput, "Loading saved run...");
    const doc = await db.collection("screener_runs").doc(cleanId).get();
    if (!doc.exists) throw new Error("Run not found.");
    const data = doc.data() || {};
    renderScreenerRunOutput({ id: doc.id, ...data });
    logEvent("screener_loaded_saved", { run_id: doc.id });
  };

  const extractQuantileKeys = (rows) => {
    const list = Array.isArray(rows) ? rows : [];
    const set = new Set();
    for (let i = 0; i < Math.min(list.length, 500); i += 1) {
      const row = list[i] || {};
      Object.keys(row).forEach((key) => {
        if (/^q\\d\\d$/.test(key)) set.add(key);
      });
    }
    return Array.from(set).sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
  };

  const renderForecastDetails = (forecastDoc) => {
    if (!ui.forecastOutput || !forecastDoc) return;
    const rows = Array.isArray(forecastDoc.forecastRows) ? forecastDoc.forecastRows : [];
    if (!rows.length) {
      setOutputReady(ui.forecastOutput);
      ui.forecastOutput.innerHTML = `<div class="small muted">No forecast rows were stored for this run.</div>`;
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

    const reportStatus = String(forecastDoc.reportStatus || "").trim().toLowerCase();
    const pdfPath = getForecastReportPath(forecastDoc, "pdf");
    const pptxPath = getForecastReportPath(forecastDoc, "pptx");
    const tradeRationale = String(forecastDoc.tradeRationale || "").trim();
    const reportHint =
      reportStatus === "ready"
        ? "Report Agent outputs are ready."
        : reportStatus === "generating" || reportStatus === "queued"
          ? "Report Agent is generating PDF/PPT assets..."
          : reportStatus === "failed"
            ? "Report Agent failed. Retry generation."
            : "Generate Executive Brief and Slide Deck for this forecast.";

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
          <button class="cta secondary small" type="button" data-action="forecast-report-pdf" data-forecast-id="${escapeHtml(
            forecastDoc.id
          )}" ${pdfPath ? "" : 'data-needs-report="1"'}>${icon("download")}<span>Download Executive Brief</span></button>
          <button class="cta secondary small" type="button" data-action="forecast-report-pptx" data-forecast-id="${escapeHtml(
            forecastDoc.id
          )}" ${pptxPath ? "" : 'data-needs-report="1"'}>${icon("download")}<span>Download Slide Deck</span></button>
        </div>
        <div class="small muted">${escapeHtml(reportHint)}</div>
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

  const getForecastReportPath = (doc, kind) => {
    const assets = doc?.reportAssets && typeof doc.reportAssets === "object" ? doc.reportAssets : {};
    if (kind === "pdf") return String(assets.pdfPath || "").trim();
    if (kind === "pptx") return String(assets.pptxPath || "").trim();
    if (kind === "chart") return String(assets.chartPath || "").trim();
    return "";
  };

  const ensureForecastReportsReady = async ({ db, functions, forecastId, workspaceId, force = false }) => {
    if (!functions || !forecastId) return null;
    const action = functions.httpsCallable("generate_forecast_report_assets");
    const result = await action({
      forecastId,
      workspaceId,
      force,
      meta: buildMeta(),
    });
    const payload = result.data || {};
    const reportAssets = payload.reportAssets && typeof payload.reportAssets === "object" ? payload.reportAssets : {};
    const reportStatus = String(payload.reportStatus || "").trim();
    const tradeRationale = String(payload.tradeRationale || "").trim();

    const ref = db.collection("forecast_requests").doc(forecastId);
    await ref.set(
      {
        reportAssets,
        reportStatus: reportStatus || "ready",
        tradeRationale: tradeRationale || firebase.firestore.FieldValue.delete(),
        updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    return { reportAssets, reportStatus, tradeRationale };
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
	  };

  const bindForegroundPushHandler = (messaging) => {
    if (!messaging || state.messagingBound) return;
    try {
      messaging.onMessage((payload) => {
        const title = payload?.notification?.title || "Quantura update";
        const body = payload?.notification?.body || "You have a new dashboard update.";
        showToast(`${title}: ${body}`);
        setNotificationStatus(`Last message: ${title}`);
        appendNotificationLog({
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
          appendNotificationLog({
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
      if (orderId) {
        panel.dataset.orderId = String(orderId);
      }
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
    if (!state.user) {
      showToast("Sign in to continue.", "warn");
      return;
    }
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

  const handleCheckoutReturn = async (functions) => {
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
    if (!state.user) {
      showToast("Sign in to finalize checkout.", "warn");
      return;
    }

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

      state.clients = { auth, db, functions, storage, messaging };

      window.__quanturaPanelActivated = (panel) => {
        const next = String(panel || "").trim();
        if (!next) return;

        if (next === "trending") {
          if (!state.panelAutoloaded.trending) {
            state.panelAutoloaded.trending = true;
            loadTrendingTickers(functions, { notify: false });
          }
        }

        if (next === "news") {
          const ticker = normalizeTicker(state.tickerContext.ticker || safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (!ticker) return;
          scheduleSideDataRefresh(ticker, { force: !state.panelAutoloaded.news });
          state.panelAutoloaded.news = true;
        }

        if (next === "events-calendar") {
          const first = !state.panelAutoloaded.eventsCalendar;
          state.panelAutoloaded.eventsCalendar = true;
          loadCorporateEventsCalendar(functions, { force: first, notify: false });
        }

        if (next === "market-headlines") {
          const first = !state.panelAutoloaded.marketHeadlines;
          state.panelAutoloaded.marketHeadlines = true;
          loadMarketHeadlinesFeed(functions, { force: first, notify: false });
        }

        if (next === "ticker-query") {
          const ticker = normalizeTicker(state.tickerContext.ticker || safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (ticker && ui.tickerQueryTicker && !String(ui.tickerQueryTicker.value || "").trim()) {
            ui.tickerQueryTicker.value = ticker;
          }
          if (ui.tickerQueryLanguage && ui.tickerQueryLanguage.value === "auto") {
            ui.tickerQueryLanguage.value = state.preferredLanguage || "en";
          }
        }

        if (next === "options") {
          const ticker = normalizeTicker(state.tickerContext.ticker || safeLocalStorageGet(LAST_TICKER_KEY) || "");
          if (!ticker) return;
          const first = !state.panelAutoloaded.options;
          state.panelAutoloaded.options = true;
          autoloadOptionsChain(functions, { force: first });
        }
      };

      ensureThemeToggle();
      normalizeTopNavigation();
      ensureHeaderNotificationsCta();
      bindMobileNav();
      initializeLanguageControls().catch(() => {});
      captureShareFromUrl();
      renderNotificationLog();
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
	      } else if (!isPushSupported()) {
	        setNotificationStatus("Push notifications are not supported in this browser.");
	        setNotificationControlsEnabled(false);
	      } else if (!messaging) {
	        setNotificationStatus("Messaging SDK is not loaded on this page.");
	        setNotificationControlsEnabled(false);
	      } else {
	        setNotificationStatus("Sign in and enable notifications.");
	        setNotificationControlsEnabled(true);
	      }
	    }

    bindForegroundPushHandler(messaging);

    if (state.cookieConsent === "accepted") {
      ensureInitialPageView();
    } else if (state.cookieConsent !== "declined") {
      ensureCookieModal().classList.remove("hidden");
	    }
	    window.setTimeout(() => ensureFeedbackPrompt(), 1400);
	    bindPanelNavigation();
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
          if (!state.user) {
            showToast("Sign in to download forecasts.", "warn");
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
          if (!dlButton) return;
          event.preventDefault();
          if (!state.user) {
            showToast("Sign in to download screener runs.", "warn");
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
          const shareForecast = event.target.closest('[data-action="share-forecast"]');
          if (shareForecast) {
            event.preventDefault();
            if (!state.user) {
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
              await copyToClipboard(url);
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
	            if (!state.user) {
	              showToast("Sign in to delete forecasts.", "warn");
	              return;
	            }
	            const forecastId = String(deleteForecast.dataset.forecastId || "").trim();
	            if (!forecastId) return;
	            const ok = await openConfirmModal({
	              title: "Delete saved forecast?",
	              message: "This removes the saved forecast from your workspace. This cannot be undone.",
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
            if (!state.user) {
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
            if (!state.user) {
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
              await copyToClipboard(url);
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
            if (!state.user) {
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
            if (!state.user) {
              showToast("Sign in to rename AI Agents.", "warn");
              return;
            }
            const agentId = String(renameAgent.dataset.agentId || "").trim();
            if (!agentId) return;
            const workspaceId = state.activeWorkspaceId || state.user.uid;
            const current = state.aiAgents.find((agent) => String(agent.id || "") === agentId);
            const nextName = await openPromptModal({
              title: "Rename AI Agent",
              message: "Update the custom name shown in the leaderboard.",
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
              await copyToClipboard(url);
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
            if (!state.user) {
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
	            if (!state.user) {
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
	            if (!state.user) {
	              showToast("Sign in to delete autopilot requests.", "warn");
	              return;
	            }
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
	            if (!state.user) {
	              showToast("Sign in to view uploads.", "warn");
              return;
            }
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
            if (!state.user) {
              showToast("Sign in to download uploads.", "warn");
              return;
            }
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
	            if (!state.user) {
	              showToast("Sign in to rename uploads.", "warn");
	              return;
	            }
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
            if (!state.user) {
              showToast("Sign in to share uploads.", "warn");
              return;
            }
            const uploadId = String(shareUpload.dataset.uploadId || "").trim();
            if (!uploadId) return;

            shareUpload.disabled = true;
            try {
              const createShare = functions.httpsCallable("create_share_link");
              const result = await createShare({ kind: "upload", id: uploadId, meta: buildMeta() });
              const shareId = String(result.data?.shareId || "").trim();
              const url = String(result.data?.shareUrl || "") || buildShareUrl("upload", shareId);
              if (!shareId || !url) throw new Error("Unable to create share link.");
              await copyToClipboard(url);
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
            if (!state.user) {
              showToast("Sign in to delete uploads.", "warn");
	              return;
	            }
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

          const plotBacktest = event.target.closest('[data-action="plot-backtest"]');
          if (plotBacktest) {
            event.preventDefault();
            if (!state.user) {
              showToast("Sign in to load backtests.", "warn");
              return;
            }
            const backtestId = String(plotBacktest.dataset.backtestId || "").trim();
            if (!backtestId) return;
            try {
              await loadBacktestById(db, storage, backtestId);
              showToast("Backtest loaded.");
            } catch (error) {
              showToast(error.message || "Unable to load backtest.", "warn");
            }
            return;
          }

          const downloadBacktestCode =
            event.target.closest('[data-action="download-backtest-source"]') ||
            event.target.closest('[data-action="download-backtest-code"]');
          if (downloadBacktestCode) {
            event.preventDefault();
            if (!state.user) {
              showToast("Sign in to download backtest code.", "warn");
              return;
            }
            const backtestId = String(downloadBacktestCode.dataset.backtestId || "").trim();
            if (!backtestId) return;
            downloadBacktestCode.disabled = true;
            try {
              const snap = await db.collection("backtests").doc(backtestId).get();
              if (!snap.exists) throw new Error("Backtest not found.");
              const doc = { id: snap.id, ...(snap.data() || {}) };
              const sourceSelect = downloadBacktestCode
                .closest(".backtest-source-controls")
                ?.querySelector('[data-backtest-source-format]');
              const sourceKey = String(sourceSelect?.value || "python").trim().toLowerCase();
              const source = resolveBacktestSourceExport(doc, sourceKey);
              if (!source || !String(source.content || "").trim()) {
                throw new Error("No exported source available for the selected format.");
              }
              const safeTicker = normalizeTicker(doc.ticker || "backtest") || "backtest";
              const optionMeta =
                BACKTEST_SOURCE_OPTIONS.find((item) => item.key === sourceKey) || BACKTEST_SOURCE_OPTIONS[0];
              const suggestedFilename = source.filename
                ? String(source.filename)
                : `${safeTicker}_${backtestId}.${optionMeta.ext}`;
              triggerDownload(suggestedFilename, source.content, {
                mimeType: source.mimeType || optionMeta.mimeType,
              });
              showToast("Source downloaded.");
              logEvent("backtest_code_downloaded", { backtest_id: backtestId, source_format: sourceKey });
            } catch (error) {
              showToast(error.message || "Unable to download code.", "warn");
            } finally {
              downloadBacktestCode.disabled = false;
            }
            return;
          }

          const renameBacktest = event.target.closest('[data-action="rename-backtest"]');
          if (renameBacktest) {
            event.preventDefault();
            if (!state.user) {
              showToast("Sign in to rename backtests.", "warn");
              return;
            }
            const backtestId = String(renameBacktest.dataset.backtestId || "").trim();
            if (!backtestId) return;
            let currentTitle = "";
            try {
              const snap = await db.collection("backtests").doc(backtestId).get();
              if (snap.exists) currentTitle = String(snap.data()?.title || "");
            } catch (error) {
              currentTitle = "";
            }
            const nextTitle = await openPromptModal({
              title: "Rename backtest",
              message: "Update the label shown in your saved list.",
              label: "Title",
              placeholder: "Backtest",
              initialValue: currentTitle,
              confirmLabel: "Rename",
            });
            if (!nextTitle) return;
            renameBacktest.disabled = true;
            try {
              const rename = functions.httpsCallable("rename_backtest");
              await rename({ backtestId, title: nextTitle, meta: buildMeta() });
              showToast("Backtest renamed.");
              logEvent("backtest_renamed", { backtest_id: backtestId });
            } catch (error) {
              showToast(error.message || "Unable to rename backtest.", "warn");
            } finally {
              renameBacktest.disabled = false;
            }
            return;
          }

          const deleteBacktest = event.target.closest('[data-action="delete-backtest"]');
          if (deleteBacktest) {
            event.preventDefault();
            if (!state.user) {
              showToast("Sign in to delete backtests.", "warn");
              return;
            }
            const backtestId = String(deleteBacktest.dataset.backtestId || "").trim();
            if (!backtestId) return;
            const ok = await openConfirmModal({
              title: "Delete backtest?",
              message: "This deletes the saved backtest and removes its chart from storage. This cannot be undone.",
              confirmLabel: "Delete",
              danger: true,
            });
            if (!ok) return;
            deleteBacktest.disabled = true;
            try {
              const del = functions.httpsCallable("delete_backtest");
              await del({ backtestId, meta: buildMeta() });
              showToast("Backtest deleted.");
              logEvent("backtest_deleted", { backtest_id: backtestId });
              if (ui.backtestOutput) ui.backtestOutput.innerHTML = `<div class="small muted">Backtest deleted.</div>`;
            } catch (error) {
              showToast(error.message || "Unable to delete backtest.", "warn");
            } finally {
              deleteBacktest.disabled = false;
            }
            return;
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
		        const quantKeys = extractQuantileKeys(rows);
		        const headers = ["ds", ...quantKeys];
		        const csv = buildCsv(rows, headers);
		        const ticker = normalizeTicker(doc.ticker || "ticker") || "ticker";
		        const service = String(doc.service || "forecast").replace(/[^a-z0-9_\\-]+/gi, "_");
		        triggerDownload(`${ticker}_${service}_${doc.id || "run"}.csv`, csv);
		        showToast("CSV downloaded.");
		        return;
		      }

          const reportPdfBtn = event.target.closest('[data-action="forecast-report-pdf"]');
          const reportPptxBtn = event.target.closest('[data-action="forecast-report-pptx"]');
          if (reportPdfBtn || reportPptxBtn) {
            event.preventDefault();
            if (!state.user) {
              showToast("Sign in to download forecast reports.", "warn");
              return;
            }
            const button = reportPdfBtn || reportPptxBtn;
            const kind = reportPdfBtn ? "pdf" : "pptx";
            const forecastId = String(button.dataset.forecastId || state.tickerContext.forecastId || "").trim();
            if (!forecastId) {
              showToast("Forecast ID is missing.", "warn");
              return;
            }
            button.disabled = true;
            try {
              let doc =
                state.tickerContext.forecastDoc && String(state.tickerContext.forecastDoc.id || "") === forecastId
                  ? state.tickerContext.forecastDoc
                  : await loadForecastDoc(db, forecastId);

              let path = getForecastReportPath(doc, kind);
              const reportStatus = String(doc.reportStatus || "").trim().toLowerCase();
              if (!path || reportStatus !== "ready" || button.dataset.needsReport === "1") {
                showToast("Generating report assets...");
                await ensureForecastReportsReady({
                  db,
                  functions,
                  forecastId,
                  workspaceId: state.activeWorkspaceId || state.user.uid,
                  force: reportStatus === "failed",
                });
                doc = await loadForecastDoc(db, forecastId);
                if (state.tickerContext.forecastDoc && String(state.tickerContext.forecastDoc.id || "") === forecastId) {
                  state.tickerContext.forecastDoc = doc;
                  renderForecastDetails(doc);
                }
                path = getForecastReportPath(doc, kind);
              }

              if (!path) throw new Error("Report file is not available yet.");
              if (!storage) throw new Error("Storage client is unavailable.");
              const url = await storage.ref().child(path).getDownloadURL();
              const ticker = normalizeTicker(doc.ticker || "ticker") || "ticker";
              const service = String(doc.service || "forecast").replace(/[^a-z0-9_\-]+/gi, "_");
              const filename =
                kind === "pdf"
                  ? `${ticker}_${service}_${forecastId}_executive_brief.pdf`
                  : `${ticker}_${service}_${forecastId}_slide_deck.pptx`;
              await triggerDownloadFromUrl(url, filename);
              showToast(kind === "pdf" ? "Executive Brief downloaded." : "Slide Deck downloaded.");
              logEvent(kind === "pdf" ? "forecast_report_pdf_downloaded" : "forecast_report_pptx_downloaded", {
                forecast_id: forecastId,
              });
            } catch (error) {
              showToast(extractErrorMessage(error, "Unable to download report."), "warn");
            } finally {
              button.disabled = false;
            }
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

			    ui.headerAuth?.addEventListener("click", () => {
			      window.location.href = state.user ? "/dashboard" : "/account";
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
          startAIAgents(db, next);
          startVolatilityMonitor(db, functions, next);
          seedDefaultAIAgents(db, next).catch(() => {});
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

      const performSignOut = async () => {
        try {
          await unregisterCachedNotificationToken(functions);
        } catch (error) {
          // Ignore token cleanup failures.
        }
        await auth.signOut();
        showToast("Signed out.");
        logEvent("logout", { method: "firebase" });
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

    const signInWithProvider = async (provider, successMessage, method) => {
      if (ui.emailMessage) ui.emailMessage.textContent = "";
      try {
        await persistenceReady;
        await auth.signInWithPopup(provider);
        showToast(successMessage);
        logEvent("login", { method });
      } catch (error) {
        if (ui.emailMessage) ui.emailMessage.textContent = error.message;
      }
    };

    ui.googleSignin?.addEventListener("click", async () => {
      await signInWithProvider(new firebase.auth.GoogleAuthProvider(), "Signed in with Google.", "google");
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

	    ui.purchasePanels.forEach((panel) => {
	      const purchaseBtn = panel.querySelector('[data-action="purchase"]');
	      const stripeBtn = panel.querySelector('[data-action="stripe"]');
	      purchaseBtn?.addEventListener("click", () => handlePurchase(panel, functions));
	      stripeBtn?.addEventListener("click", () => handleStripeCheckout(panel, functions));
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
          const ticker = normalizeTicker(state.tickerContext.ticker || ui.terminalTicker?.value || "");
          if (!ticker) {
            showToast("Load a ticker in the chart before forecasting.", "warn");
            return;
          }
		      const payload = {
		        ticker,
	        horizon: Number(formData.get("horizon")),
	        interval: formData.get("interval"),
	        service: formData.get("service") || ui.forecastService?.value || "prophet",
	        quantiles,
          workspaceId: state.activeWorkspaceId || state.user.uid,
	        meta: buildMeta(),
	        utm: getUtm(),
		      };
          payload.start = (() => {
            const desiredInterval = String(payload.interval || state.tickerContext.interval || "1d");
            if (
              Array.isArray(state.tickerContext.rows) &&
              state.tickerContext.rows.length &&
              state.tickerContext.ticker === ticker &&
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
		        const runForecast = functions.httpsCallable("run_timeseries_forecast");
		        const result = await runForecast(payload);
		        const data = result.data || {};
		        const requestId = String(data.requestId || "").trim();
		        if (!requestId) {
		          throw new Error("Forecast run did not return a request ID.");
		        }

		        logEvent("forecast_request", { ticker: payload.ticker, interval: payload.interval, service: payload.service });
		        showToast("Forecast saved.");
            ensureForecastReportsReady({
              db,
              functions,
              forecastId: requestId,
              workspaceId: payload.workspaceId,
              force: false,
            }).catch(() => {});

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

        ui.forecastLoadButton?.addEventListener("click", async () => {
          if (!state.user) {
            showToast("Sign in to load saved forecasts.", "warn");
            return;
          }
          const forecastId = String(ui.forecastLoadSelect?.value || "").trim();
          if (!forecastId) {
            showToast("Select a saved forecast.", "warn");
            return;
          }
          if (ui.forecastLoadStatus) ui.forecastLoadStatus.textContent = "Loading...";
          try {
            setTerminalStatus("Loading saved forecast...");
            await plotForecastById(db, functions, forecastId);
            if (ui.forecastLoadStatus) ui.forecastLoadStatus.textContent = "";
            showToast("Forecast loaded.");
            logEvent("forecast_loaded_saved", { forecast_id: forecastId });
            ensureForecastReportsReady({
              db,
              functions,
              forecastId,
              workspaceId: state.activeWorkspaceId || state.user.uid,
              force: false,
            }).catch(() => {});
            document.getElementById("terminal")?.scrollIntoView({ behavior: "smooth" });
          } catch (error) {
            if (ui.forecastLoadStatus) ui.forecastLoadStatus.textContent = error.message || "Unable to load forecast.";
            showToast(error.message || "Unable to load forecast.", "warn");
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
      if (!state.user) {
        showToast("Sign in to download price history.", "warn");
        return;
      }
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
        const getDownload = functions.httpsCallable("download_price_csv");
        const result = await getDownload(payload);
        const data = result.data || {};
        const csvText = String(data.csv || "");
        if (!csvText.trim()) {
          ui.downloadStatus.textContent = "No data returned.";
          return;
        }
        const filename = String(data.filename || `${ticker}_${start}_${end}.csv`);
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

    if (ui.eventsCalendarStart && ui.eventsCalendarEnd) {
      const today = new Date();
      const end = new Date(today);
      end.setDate(end.getDate() + 90);
      if (!ui.eventsCalendarStart.value) ui.eventsCalendarStart.value = today.toISOString().slice(0, 10);
      if (!ui.eventsCalendarEnd.value) ui.eventsCalendarEnd.value = end.toISOString().slice(0, 10);
    }
    if (ui.eventsCalendarCountry && !ui.eventsCalendarCountry.value) {
      ui.eventsCalendarCountry.value = state.preferredCountry || "US";
    }
    ui.eventsCalendarForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      await loadCorporateEventsCalendar(functions, { force: true, notify: true });
    });

    if (ui.marketHeadlinesCountry && !ui.marketHeadlinesCountry.value) {
      ui.marketHeadlinesCountry.value = state.preferredCountry || "US";
    }
    ui.marketHeadlinesForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      await loadMarketHeadlinesFeed(functions, { force: true, notify: true });
    });

    if (ui.tickerQueryLanguage && !ui.tickerQueryLanguage.value) {
      ui.tickerQueryLanguage.value = state.preferredLanguage || "en";
    }
    ui.tickerQueryForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      await loadTickerQueryInsight(functions, {
        ticker: ui.tickerQueryTicker?.value || state.tickerContext.ticker || "",
        question: ui.tickerQueryQuestion?.value || "",
        notify: true,
      });
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
	                <div class="small"><strong>RFR:</strong> ${typeof riskFreeRate === "number" ? fmt(riskFreeRate, 3) : "—"} · <strong>T:</strong> ${
	                  typeof timeToExpiryYears === "number" ? fmt(timeToExpiryYears, 3) : "—"
	                }y</div>
	              </div>
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
	      if (!state.user) {
	        showToast("Sign in to generate an AI Portfolio.", "warn");
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
        workspaceId: state.activeWorkspaceId || state.user.uid,
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
      if (!state.user) {
        showToast("Sign in to load saved screener runs.", "warn");
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
      if (!state.user) {
        showToast("Sign in to run the OpenAI CSV Agent.", "warn");
        return;
      }
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

    ui.backtestStrategy?.addEventListener("change", () => {
      syncBacktestStrategyFields();
      logEvent("backtest_strategy_changed", { strategy: String(ui.backtestStrategy?.value || "") });
    });
    syncBacktestStrategyFields();

    ui.backtestLoadSelect?.addEventListener("change", () => {
      if (!ui.backtestLoadId || !ui.backtestLoadSelect) return;
      const value = String(ui.backtestLoadSelect.value || "").trim();
      if (value) ui.backtestLoadId.value = value;
    });

    ui.backtestLoadButton?.addEventListener("click", async () => {
      if (!state.user) {
        showToast("Sign in to load saved backtests.", "warn");
        return;
      }
      const backtestId = String(ui.backtestLoadId?.value || "").trim() || String(ui.backtestLoadSelect?.value || "").trim();
      if (!backtestId) {
        showToast("Select a backtest or paste an ID.", "warn");
        return;
      }
      if (ui.backtestLoadStatus) ui.backtestLoadStatus.textContent = "Loading...";
      try {
        await loadBacktestById(db, storage, backtestId);
        if (ui.backtestLoadStatus) ui.backtestLoadStatus.textContent = "";
        showToast("Backtest loaded.");
      } catch (error) {
        if (ui.backtestLoadStatus) ui.backtestLoadStatus.textContent = error.message || "Unable to load backtest.";
        showToast(error.message || "Unable to load backtest.", "warn");
      }
    });

    ui.backtestForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.user) {
        showToast("Sign in to run backtests.", "warn");
        return;
      }
      if (!state.remoteFlags.backtestingEnabled) {
        showToast("Backtesting is temporarily disabled.", "warn");
        return;
      }
      const ticker = normalizeTicker(state.tickerContext.ticker || ui.terminalTicker?.value || "");
      if (!ticker) {
        showToast("Load a ticker in the chart before backtesting.", "warn");
        return;
      }
      const interval = String(ui.terminalInterval?.value || state.tickerContext.interval || "1d");
      const formData = new FormData(ui.backtestForm);
      const strategy = String(formData.get("strategy") || "sma_cross").trim();
      const lookbackDays = Number(formData.get("lookback") || 730);
      const cash = Number(formData.get("cash") || 10000);
      const commission = Number(formData.get("commission") || 0.0);

      const params = {};
      if (strategy === "sma_cross") {
        params.fast = Number(formData.get("fast") || 20);
        params.slow = Number(formData.get("slow") || 50);
      } else {
        params.rsiPeriod = Number(formData.get("rsiPeriod") || 14);
        params.oversold = Number(formData.get("oversold") || 30);
        params.exitAbove = Number(formData.get("exitAbove") || 55);
      }
      params.slPct = Number(formData.get("slPct") || 0);
      params.tpPct = Number(formData.get("tpPct") || 0);

      const payload = {
        ticker,
        interval,
        strategy,
        lookbackDays,
        cash,
        commission,
        params,
        meta: buildMeta(),
      };

      try {
        setOutputLoading(ui.backtestOutput, "Running backtest...");
        const run = functions.httpsCallable("run_backtest");
        const result = await run(payload);
        const backtestId = String(result.data?.backtestId || "").trim();
        showToast("Backtest saved.");
        logEvent("backtest_run", { ticker, interval, strategy });
        if (backtestId) {
          await loadBacktestById(db, storage, backtestId);
        }
      } catch (error) {
        showToast(error.message || "Unable to run backtest.", "warn");
      }
    });

    ui.autopilotForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.user) {
        showToast("Sign in to queue autopilot runs.", "warn");
        return;
      }
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
	      if (!state.user) {
	        showToast("Sign in to enable notifications.", "warn");
	        return;
	      }
	      if (!state.remoteFlags.pushEnabled) {
	        showToast("Notifications are temporarily disabled.", "warn");
	        return;
	      }
	      if (!messaging) {
	        showToast("Messaging SDK is unavailable on this page.", "warn");
	        return;
	      }
      try {
        setNotificationStatus("Registering notification token...");
        const token = await registerNotificationToken(functions, messaging, { forceRefresh: false });
        setNotificationTokenPreview(token);
        setNotificationStatus("Notifications are enabled for this browser.");
        appendNotificationLog({
          title: "Notifications enabled",
          body: "Browser token registered successfully.",
          source: "system",
          at: new Date().toISOString(),
        });
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
	      if (!state.remoteFlags.pushEnabled) {
	        showToast("Notifications are temporarily disabled.", "warn");
	        return;
	      }
	      if (!messaging) {
	        showToast("Messaging SDK is unavailable on this page.", "warn");
	        return;
	      }
      try {
        setNotificationStatus("Refreshing notification token...");
        const token = await registerNotificationToken(functions, messaging, { forceRefresh: true });
        setNotificationTokenPreview(token);
        setNotificationStatus("Notification token refreshed.");
        appendNotificationLog({
          title: "Notification token refreshed",
          body: "FCM token rotated and synced.",
          source: "system",
          at: new Date().toISOString(),
        });
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
	      if (!state.remoteFlags.pushEnabled) {
	        showToast("Notifications are temporarily disabled.", "warn");
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
        appendNotificationLog({
          title: "Test notification sent",
          body: `Delivered to ${sent} registered token(s).`,
          source: "system",
          at: new Date().toISOString(),
        });
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

    if (ui.profileForm && ui.profileForm.dataset.bound !== "1") {
      ui.profileForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (!state.user) {
          setProfileStatus("Sign in to save your profile.", "warn");
          showToast("Sign in to save your profile.", "warn");
          return;
        }
        try {
          const nextProfile = {
            username: sanitizeProfileUsername(ui.profileUsername?.value || "", state.user),
            avatar: normalizeProfileAvatar(ui.profileAvatar?.value || state.userProfile?.avatar || "bull"),
            bio: normalizeProfileBio(ui.profileBio?.value || ""),
            publicProfile: Boolean(ui.profilePublicEnabled?.checked),
            publicScreenerSharing: Boolean(ui.profilePublicScreener?.checked),
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
        if (!state.user) {
          setProfileStatus("Sign in to connect Stripe.", "warn");
          showToast("Sign in to connect Stripe.", "warn");
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
		      state.authResolved = true;
		      state.user = user;
		      setAuthUi(user);
		      setUserId(user?.uid || null);

		      if (!user) {
		        state.userHasPaidPlan = false;
            state.userSubscriptionTier = "free";
            state.aiUsageToday = 0;
            state.aiUsageTierKey = "free";
		        renderOrderList([], ui.userOrders);
            renderRequestList([], ui.userForecasts, "No forecast requests yet.");
            renderRequestList([], ui.autopilotOutput, "No autopilot requests yet.");
            renderRequestList([], ui.predictionsOutput, "No uploads yet.");
            if (ui.backtestOutput) ui.backtestOutput.textContent = "Sign in to run backtests.";
            if (ui.savedBacktestsList) ui.savedBacktestsList.textContent = "Sign in to view backtests.";
            if (ui.backtestLoadSelect) ui.backtestLoadSelect.innerHTML = `<option value="">Select a backtest</option>`;
            if (ui.backtestLoadId) ui.backtestLoadId.value = "";
            if (ui.backtestLoadStatus) ui.backtestLoadStatus.textContent = "";
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
          setNotificationTokenPreview("");
          setNotificationControlsEnabled(false);
        }
			        if (state.unsubscribeOrders) state.unsubscribeOrders();
			        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
				        if (state.unsubscribeForecasts) state.unsubscribeForecasts();
				        if (state.unsubscribeAutopilot) state.unsubscribeAutopilot();
				        if (state.unsubscribePredictions) state.unsubscribePredictions();
                if (state.unsubscribeBacktests) state.unsubscribeBacktests();
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
		        state.sharedWorkspaces = [];
		        setActiveWorkspaceId("");
            state.userProfile = {
              username: "",
              socialLinks: cloneDefaultProfileSocialLinks(),
              avatar: "bull",
              bio: "",
              publicProfile: false,
              publicScreenerSharing: false,
              stripeConnectAccountId: "",
            };
            renderProfileForm(state.userProfile, null);
		        renderWorkspaceSelect(null);
		        if (ui.productivityBoard) ui.productivityBoard.innerHTML = "";
		        if (ui.tasksCalendar) ui.tasksCalendar.textContent = "Tasks with due dates will appear here.";
            if (ui.screenerLoadSelect) ui.screenerLoadSelect.innerHTML = `<option value="">Select a run</option>`;
            if (ui.screenerLoadStatus) ui.screenerLoadStatus.textContent = "";
            if (ui.screenerOutput && !ui.screenerOutput.dataset.loading) ui.screenerOutput.textContent = "Sign in to generate an AI Portfolio.";
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
            if (pendingShare && window.location.pathname !== "/account") {
              window.location.href = "/account";
              return;
            }

				        const gated = new Set([
				          "/dashboard",
				          "/watchlist",
				          "/productivity",
				          "/collaboration",
				          "/uploads",
				          "/autopilot",
				          "/notifications",
				        ]);
			        if (gated.has(window.location.pathname) && window.location.pathname !== "/account") {
			          window.location.href = "/account";
			        }
			        return;
			      }

	      await ensureUserProfile(db, user);
        await loadUserProfile(db, user);
        setProfileStatus("Profile is used when publishing AI Agents to the leaderboard.");
	      startUserOrders(db, user);
	      subscribeSharedWorkspaces(db, user);
		      const activeWorkspaceId = resolveActiveWorkspaceId(user);
		      setActiveWorkspaceId(activeWorkspaceId);
		      renderWorkspaceSelect(user);
		      startUserForecasts(db, activeWorkspaceId);
          startScreenerRuns(db, activeWorkspaceId);
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
        startBacktests(db, user);
	      refreshCollaboration(functions);

        await processPendingShareImport(functions);

        if (window.location.pathname === "/account" && !String(getPendingShareId() || "").trim()) {
          window.location.href = "/dashboard";
        }

	      if (ui.notificationsStatus) {
	        if (!state.remoteFlags.pushEnabled) {
	          setNotificationControlsEnabled(false);
	          setNotificationStatus("Notifications are temporarily disabled.");
        } else if (messaging && isPushSupported()) {
          setNotificationControlsEnabled(true);
          const cachedToken = localStorage.getItem(FCM_TOKEN_CACHE_KEY) || "";
          setNotificationTokenPreview(cachedToken);
          setNotificationStatus(cachedToken ? "Notifications enabled for this browser." : "Click Enable notifications.");
          if (Notification.permission === "granted") {
            try {
              const token = await registerNotificationToken(functions, messaging, { forceRefresh: !cachedToken });
              setNotificationTokenPreview(token);
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
        startAdminAutopilotQueue(db);
      } else {
        ui.adminSection?.classList.add("hidden");
        ui.navAdmin?.classList.add("hidden");
        if (state.unsubscribeAdmin) state.unsubscribeAdmin();
        if (state.unsubscribeAdminAutopilot) state.unsubscribeAdminAutopilot();
      }
    });
  });
  };

  window.addEventListener("load", init);
})();
