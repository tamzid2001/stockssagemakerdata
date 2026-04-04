(() => {
  const GISCUS_ORIGIN = "https://giscus.app";
  const GISCUS_CLIENT_URL = "https://giscus.app/client.js";
  const DEFAULT_CONFIG = Object.freeze({
    repo: "tamzid2001/stockssagemakerdata",
    repoId: "R_kgDOREYQXg",
    category: "General",
    categoryId: "DIC_kwDOREYQXs4C4zbs",
    mapping: "specific",
    strict: "1",
    reactionsEnabled: "1",
    emitMetadata: "1",
    inputPosition: "top",
    lang: "en",
    loading: "lazy",
  });

  const hostStates = new WeakMap();
  const frameStateByWindow = new Map();
  const activeStates = new Set();

  let messageListenerBound = false;
  let themeObserverBound = false;

  const normalizeText = (value, maxLength = 300) => {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (!text) return "";
    return text.length > maxLength ? `${text.slice(0, maxLength - 1).trimEnd()}…` : text;
  };

  const resolveTheme = () => {
    const theme = String(document?.documentElement?.dataset?.theme || "").trim().toLowerCase();
    return theme === "dark" ? "dark" : "light";
  };

  const setStatus = (state, message, variant = "") => {
    if (!state?.statusNode) return;
    state.statusNode.textContent = String(message || "");
    if (variant) state.statusNode.dataset.variant = variant;
    else delete state.statusNode.dataset.variant;
  };

  const buildKey = (config) =>
    JSON.stringify([
      config.term,
      config.backLink,
      config.description,
      config.inputPosition,
      config.lang,
    ]);

  const postConfig = (state, config) => {
    const frameWindow = state?.iframe?.contentWindow;
    if (!frameWindow || !config) return false;
    frameWindow.postMessage(
      {
        giscus: {
          setConfig: {
            repo: DEFAULT_CONFIG.repo,
            repoId: DEFAULT_CONFIG.repoId,
            category: DEFAULT_CONFIG.category,
            categoryId: DEFAULT_CONFIG.categoryId,
            term: config.term,
            description: config.description,
            backLink: config.backLink,
            strict: true,
            reactionsEnabled: true,
            emitMetadata: true,
            inputPosition: config.inputPosition,
            lang: config.lang,
            theme: resolveTheme(),
          },
        },
      },
      GISCUS_ORIGIN
    );
    return true;
  };

  const waitForFrame = (host) =>
    new Promise((resolve, reject) => {
      if (!host) {
        reject(new Error("GitHub discussion host is unavailable."));
        return;
      }
      const existing = host.querySelector("iframe.giscus-frame");
      if (existing) {
        resolve(existing);
        return;
      }
      const observer = new MutationObserver(() => {
        const iframe = host.querySelector("iframe.giscus-frame");
        if (!iframe) return;
        observer.disconnect();
        resolve(iframe);
      });
      observer.observe(host, { childList: true, subtree: true });
      window.setTimeout(() => {
        observer.disconnect();
        reject(new Error("GitHub discussion widget did not finish loading."));
      }, 15000);
    });

  const buildScript = (config) => {
    const script = document.createElement("script");
    script.src = GISCUS_CLIENT_URL;
    script.async = true;
    script.crossOrigin = "anonymous";
    script.setAttribute("data-repo", DEFAULT_CONFIG.repo);
    script.setAttribute("data-repo-id", DEFAULT_CONFIG.repoId);
    script.setAttribute("data-category", DEFAULT_CONFIG.category);
    script.setAttribute("data-category-id", DEFAULT_CONFIG.categoryId);
    script.setAttribute("data-mapping", DEFAULT_CONFIG.mapping);
    script.setAttribute("data-term", config.term);
    script.setAttribute("data-strict", DEFAULT_CONFIG.strict);
    script.setAttribute("data-reactions-enabled", DEFAULT_CONFIG.reactionsEnabled);
    script.setAttribute("data-emit-metadata", DEFAULT_CONFIG.emitMetadata);
    script.setAttribute("data-input-position", config.inputPosition);
    script.setAttribute("data-theme", resolveTheme());
    script.setAttribute("data-lang", config.lang);
    script.setAttribute("data-loading", DEFAULT_CONFIG.loading);
    return script;
  };

  const handleMessage = (event) => {
    if (event.origin !== GISCUS_ORIGIN) return;
    if (!event.data || typeof event.data !== "object" || !event.data.giscus) return;
    const state = frameStateByWindow.get(event.source);
    if (!state) return;
    const payload = event.data.giscus;
    if (payload.error) {
      setStatus(state, `GitHub discussion unavailable: ${normalizeText(payload.error, 180)}`, "warn");
      return;
    }
    if (payload.discussion && typeof payload.discussion === "object") {
      const count = Math.max(0, Number(payload.discussion.totalCommentCount) || 0);
      if (state.countNode) state.countNode.textContent = String(count);
      setStatus(
        state,
        count
          ? `${count} comment${count === 1 ? "" : "s"} on GitHub Discussions.`
          : "Comments powered by GitHub Discussions. Sign in with GitHub in the widget to join.",
        ""
      );
    }
  };

  const refreshAllThemes = () => {
    activeStates.forEach((state) => {
      if (state?.currentConfig) postConfig(state, state.currentConfig);
    });
  };

  const bindThemeObserver = () => {
    if (themeObserverBound) return;
    themeObserverBound = true;
    if (document?.documentElement) {
      const observer = new MutationObserver(() => {
        refreshAllThemes();
      });
      observer.observe(document.documentElement, {
        attributes: true,
        attributeFilter: ["data-theme"],
      });
    }
    try {
      const media = window.matchMedia?.("(prefers-color-scheme: dark)");
      media?.addEventListener?.("change", refreshAllThemes);
    } catch (_error) {
      // Best effort only.
    }
  };

  const ensureListeners = () => {
    if (!messageListenerBound) {
      messageListenerBound = true;
      window.addEventListener("message", handleMessage);
    }
    bindThemeObserver();
  };

  const mount = async (host, options = {}) => {
    if (!host) return null;
    ensureListeners();

    let state = hostStates.get(host);
    if (!state) {
      state = {
        host,
        statusNode: null,
        countNode: null,
        iframe: null,
        iframeWindow: null,
        currentKey: "",
        currentConfig: null,
      };
      hostStates.set(host, state);
      activeStates.add(state);
    }

    state.statusNode = options.statusNode || state.statusNode || null;
    state.countNode = options.countNode || state.countNode || null;

    const config = {
      term: normalizeText(options.term, 220),
      description: normalizeText(options.description || document.title || "Quantura discussion", 260),
      backLink: normalizeText(options.backLink || window.location.href, 600),
      inputPosition: String(options.inputPosition || DEFAULT_CONFIG.inputPosition).trim().toLowerCase() === "bottom" ? "bottom" : "top",
      lang: normalizeText(options.lang || DEFAULT_CONFIG.lang, 12) || DEFAULT_CONFIG.lang,
    };

    if (!config.term) {
      host.innerHTML = "";
      setStatus(state, "GitHub discussion is unavailable for this item.", "warn");
      return null;
    }

    host.classList.add("quantura-comments-host");
    host.dataset.giscusTerm = config.term;

    const nextKey = buildKey(config);
    state.currentConfig = config;

    if (state.currentKey !== nextKey) {
      state.currentKey = nextKey;
      if (state.iframeWindow) frameStateByWindow.delete(state.iframeWindow);
      state.iframe = null;
      state.iframeWindow = null;

      host.innerHTML = `
        <div class="quantura-comments-shell">
          <div class="quantura-comments-loading">Loading GitHub discussion...</div>
        </div>
      `;
      const shell = host.querySelector(".quantura-comments-shell");
      const script = buildScript(config);
      shell?.appendChild(script);
      setStatus(state, "Comments powered by GitHub Discussions. Sign in with GitHub in the widget to join.");

      try {
        const iframe = await waitForFrame(host);
        state.iframe = iframe;
        state.iframeWindow = iframe.contentWindow;
        frameStateByWindow.set(state.iframeWindow, state);
        const loading = host.querySelector(".quantura-comments-loading");
        if (loading) loading.remove();
        postConfig(state, config);
      } catch (error) {
        setStatus(state, error?.message || "Unable to load GitHub discussion.", "warn");
      }
      return state;
    }

    if (state.iframe) {
      postConfig(state, config);
      return state;
    }

    try {
      const iframe = await waitForFrame(host);
      state.iframe = iframe;
      state.iframeWindow = iframe.contentWindow;
      frameStateByWindow.set(state.iframeWindow, state);
      postConfig(state, config);
    } catch (error) {
      setStatus(state, error?.message || "Unable to load GitHub discussion.", "warn");
    }
    return state;
  };

  window.QuanturaGiscus = {
    mount,
    refreshTheme: refreshAllThemes,
    defaults: { ...DEFAULT_CONFIG },
  };
})();
