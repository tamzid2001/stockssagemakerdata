(() => {
  const GISCUS_ORIGIN = "https://giscus.app";
  const GISCUS_CLIENT_URL = "https://giscus.app/client.js";
  const GITHUB_DISCUSSION_LOOKUP_URL = "/api/explore/discussions/lookup";
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

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const normalizeText = (value, maxLength = 300) => {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (!text) return "";
    return text.length > maxLength ? `${text.slice(0, maxLength - 1).trimEnd()}...` : text;
  };

  const normalizeRichText = (value, maxLength = 5000) => {
    const text = String(value || "").replace(/\r\n?/g, "\n").replace(/\u0000/g, "").trim();
    if (!text) return "";
    return text.length > maxLength ? `${text.slice(0, maxLength - 1).trimEnd()}...` : text;
  };

  const buildRepoDiscussionsUrl = () => `https://github.com/${DEFAULT_CONFIG.repo}/discussions`;

  const buildCreateDiscussionUrl = (term = "") => {
    const cleanTerm = normalizeText(term, 220);
    const baseUrl = `${buildRepoDiscussionsUrl()}/new?category=general`;
    return cleanTerm ? `${baseUrl}&title=${encodeURIComponent(cleanTerm)}` : baseUrl;
  };

  const formatDateTime = (value) => {
    const clean = normalizeText(value, 80);
    if (!clean) return "";
    const date = new Date(clean);
    if (Number.isNaN(date.getTime())) return "";
    try {
      return new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(date);
    } catch (_error) {
      return clean;
    }
  };

  const formatCommentBody = (value) => {
    const clean = normalizeRichText(value, 4000);
    if (!clean) return "";
    return escapeHtml(clean).replace(/\n{2,}/g, "</p><p>").replace(/\n/g, "<br>");
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
    if (state?.fallbackActive) return false;
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

  const fetchFallbackDiscussionPayload = async (config) => {
    const params = new URLSearchParams();
    params.set("term", config.term);
    const response = await fetch(`${GITHUB_DISCUSSION_LOOKUP_URL}?${params.toString()}`, {
      headers: {
        Accept: "application/json",
      },
      credentials: "same-origin",
    });
    const payload = await response.json().catch(() => ({}));
    return payload && typeof payload === "object" ? payload : {};
  };

  const renderFallbackDiscussion = (state, payload = {}, reason = "") => {
    if (!state?.host || !state?.currentConfig) return null;

    const discussion = payload.discussion && typeof payload.discussion === "object" ? payload.discussion : null;
    const discussionUrl = normalizeText(discussion?.url || payload.repoUrl || buildRepoDiscussionsUrl(), 1200);
    const createUrl = normalizeText(payload.createUrl || buildCreateDiscussionUrl(state.currentConfig.term), 1200);
    const commentCount = Math.max(0, Number(discussion?.commentCount) || 0);
    const commentItems = Array.isArray(discussion?.comments) ? discussion.comments : [];
    const summaryText =
      normalizeRichText(discussion?.body || "", 2400) || normalizeText(state.currentConfig.description, 260);
    const prettyUpdatedAt = formatDateTime(discussion?.updatedAt || discussion?.createdAt);
    const normalizedReason = normalizeText(reason, 220).toLowerCase();
    const installBlocked = normalizedReason.includes("giscus is not installed");
    const fallbackHint = installBlocked
      ? "GitHub Discussions is available, but the embedded Giscus app is not installed on this repository."
      : "Showing a GitHub Discussions fallback for this page.";

    state.fallbackActive = true;
    if (state.iframeWindow) frameStateByWindow.delete(state.iframeWindow);
    state.iframe = null;
    state.iframeWindow = null;

    if (discussion && normalizeText(discussion.title, 260)) {
      const commentsMarkup = commentItems.length
        ? `
          <div class="quantura-comments-fallback-list">
            ${commentItems
              .map((item) => {
                const authorLogin = normalizeText(item?.authorLogin || "github", 120) || "github";
                const authorUrl = normalizeText(item?.authorUrl || discussionUrl, 1200) || discussionUrl;
                const authorAvatarUrl = normalizeText(item?.authorAvatarUrl || "", 1200);
                const createdAt = formatDateTime(item?.createdAt);
                const bodyHtml = formatCommentBody(item?.body || "");
                return `
                  <article class="quantura-comments-fallback-item">
                    <div class="quantura-comments-fallback-author">
                      ${
                        authorAvatarUrl
                          ? `<img class="quantura-comments-fallback-avatar" src="${escapeHtml(authorAvatarUrl)}" alt="" loading="lazy" />`
                          : `<span class="quantura-comments-fallback-avatar quantura-comments-fallback-avatar--placeholder">${escapeHtml(authorLogin.slice(0, 1).toUpperCase())}</span>`
                      }
                      <div>
                        <a href="${escapeHtml(authorUrl)}" target="_blank" rel="noreferrer">${escapeHtml(authorLogin)}</a>
                        ${createdAt ? `<div class="quantura-comments-fallback-meta">${escapeHtml(createdAt)}</div>` : ""}
                      </div>
                    </div>
                    ${bodyHtml ? `<div class="quantura-comments-fallback-body"><p>${bodyHtml}</p></div>` : ""}
                  </article>
                `;
              })
              .join("")}
          </div>
        `
        : `<div class="quantura-comments-fallback-empty">No comments yet. Open the thread on GitHub to start the conversation.</div>`;

      state.host.innerHTML = `
        <div class="quantura-comments-shell quantura-comments-fallback">
          <div class="quantura-comments-fallback-head">
            <div>
              <div class="quantura-comments-fallback-eyebrow">GitHub Discussion</div>
              <h4><a href="${escapeHtml(discussionUrl)}" target="_blank" rel="noreferrer">${escapeHtml(
                normalizeText(discussion.title, 260)
              )}</a></h4>
              <p>${escapeHtml(fallbackHint)}</p>
            </div>
            <a class="quantura-comments-fallback-link" href="${escapeHtml(discussionUrl)}" target="_blank" rel="noreferrer">Open on GitHub</a>
          </div>
          <div class="quantura-comments-fallback-summary">
            <span>${commentCount} comment${commentCount === 1 ? "" : "s"}</span>
            ${prettyUpdatedAt ? `<span>Updated ${escapeHtml(prettyUpdatedAt)}</span>` : ""}
            ${
              normalizeText(discussion?.categoryName, 80)
                ? `<span>${escapeHtml(normalizeText(discussion.categoryName, 80))}</span>`
                : ""
            }
          </div>
          ${summaryText ? `<div class="quantura-comments-fallback-body"><p>${formatCommentBody(summaryText)}</p></div>` : ""}
          ${commentsMarkup}
        </div>
      `;
      if (state.countNode) state.countNode.textContent = String(commentCount);
      setStatus(
        state,
        commentCount
          ? `${commentCount} comment${commentCount === 1 ? "" : "s"} on GitHub Discussions.`
          : "Discussion thread found on GitHub. Open it to leave the first comment.",
        installBlocked ? "warn" : ""
      );
      return true;
    }

    state.host.innerHTML = `
      <div class="quantura-comments-shell quantura-comments-fallback quantura-comments-fallback--empty">
        <div class="quantura-comments-fallback-head">
          <div>
            <div class="quantura-comments-fallback-eyebrow">GitHub Discussions</div>
            <h4>No discussion thread yet</h4>
            <p>${escapeHtml(fallbackHint)}</p>
          </div>
          <a class="quantura-comments-fallback-link" href="${escapeHtml(createUrl)}" target="_blank" rel="noreferrer">Start on GitHub</a>
        </div>
        <div class="quantura-comments-fallback-empty">
          Start a thread on GitHub to discuss this Quantura item. New comments will appear here automatically once a matching discussion exists.
        </div>
      </div>
    `;
    if (state.countNode) state.countNode.textContent = "0";
    setStatus(state, "No GitHub discussion thread exists yet. Start one on GitHub.", installBlocked ? "warn" : "");
    return true;
  };

  const activateFallbackDiscussion = async (state, reason = "") => {
    if (!state?.currentConfig || !state?.host) return null;
    if (state.fallbackPromise) return state.fallbackPromise;
    state.fallbackPromise = (async () => {
      let payload = {};
      try {
        payload = await fetchFallbackDiscussionPayload(state.currentConfig);
      } catch (_error) {
        payload = {};
      }
      return renderFallbackDiscussion(state, payload, reason);
    })().finally(() => {
      state.fallbackPromise = null;
    });
    return state.fallbackPromise;
  };

  const handleMessage = (event) => {
    if (event.origin !== GISCUS_ORIGIN) return;
    if (!event.data || typeof event.data !== "object" || !event.data.giscus) return;
    const state = frameStateByWindow.get(event.source);
    if (!state) return;
    const payload = event.data.giscus;
    if (payload.error) {
      activateFallbackDiscussion(state, normalizeText(payload.error, 220));
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
      if (state?.currentConfig && !state.fallbackActive) postConfig(state, state.currentConfig);
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
        fallbackActive: false,
        fallbackPromise: null,
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
      inputPosition:
        String(options.inputPosition || DEFAULT_CONFIG.inputPosition).trim().toLowerCase() === "bottom" ? "bottom" : "top",
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
      state.fallbackActive = false;
      state.fallbackPromise = null;

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
        await activateFallbackDiscussion(state, error?.message || "Unable to load GitHub discussion.");
      }
      return state;
    }

    if (state.fallbackActive) {
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
      await activateFallbackDiscussion(state, error?.message || "Unable to load GitHub discussion.");
    }
    return state;
  };

  window.QuanturaGiscus = {
    mount,
    refreshTheme: refreshAllThemes,
    defaults: { ...DEFAULT_CONFIG },
  };
})();
