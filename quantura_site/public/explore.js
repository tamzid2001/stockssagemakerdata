import {
  createApiClient,
  escapeHtml,
  formatRelativeTime,
  getNativeFeedAdRules,
  initAuth,
  isNativeRuntime,
  normalizeTicker,
  reportNativeFeedAdClick,
  reportNativeFeedAdImpression,
  requestNativeFeedAd,
  track,
} from "./explore-common.js";

const refs = {
  authToggle: document.getElementById("auth-toggle"),
  searchForm: document.getElementById("explore-search-form"),
  searchInput: document.getElementById("explore-search"),
  tabs: Array.from(document.querySelectorAll(".tab[data-mode]")),
  tickerWrap: document.getElementById("ticker-filter-wrap"),
  tickerInput: document.getElementById("ticker-filter-input"),
  tickerApply: document.getElementById("ticker-filter-apply"),
  tickerSuggestions: document.getElementById("ticker-suggestions"),
  grid: document.getElementById("explore-grid"),
  loading: document.getElementById("explore-loading"),
  empty: document.getElementById("explore-empty"),
  sentinel: document.getElementById("explore-sentinel"),
  postModal: document.getElementById("post-modal"),
  postModalBody: document.getElementById("post-modal-body"),
  modalClose: document.getElementById("modal-close"),
  reportModal: document.getElementById("report-modal"),
  reportModalClose: document.getElementById("report-modal-close"),
  reportForm: document.getElementById("report-form"),
  reportReason: document.getElementById("report-reason"),
  reportDetails: document.getElementById("report-details"),
  cardTemplate: document.getElementById("post-card-template"),
  skeletonTemplate: document.getElementById("skeleton-template"),
};

const state = {
  mode: "trending",
  q: "",
  ticker: "",
  cursor: null,
  loading: false,
  done: false,
  initialized: false,
  user: null,
  authClient: null,
  api: null,
  observer: null,
  postsById: new Map(),
  activePostId: "",
  reportPostId: "",
  isPremium: false,
  adImpressionObserver: null,
  adImpressionSeen: new Set(),
};

const MARKED_SCRIPT_URL = "https://cdn.jsdelivr.net/npm/marked/marked.min.js";
const GISCUS_RUNTIME_URL = "/giscus-comments.js?v=20260405a";
const MARKDOWN_ALLOWED_TAGS = new Set([
  "p",
  "br",
  "hr",
  "strong",
  "em",
  "b",
  "i",
  "u",
  "s",
  "ul",
  "ol",
  "li",
  "blockquote",
  "code",
  "pre",
  "a",
  "table",
  "thead",
  "tbody",
  "tr",
  "th",
  "td",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
]);
const MARKDOWN_ALLOWED_ATTRS = new Set(["href", "target", "rel", "title", "colspan", "rowspan"]);
let markedLoaderPromise = null;
let giscusRuntimePromise = null;

function ensureMarkedLibrary() {
  if (window.marked?.parse) return Promise.resolve(window.marked);
  if (markedLoaderPromise) return markedLoaderPromise;
  markedLoaderPromise = new Promise((resolve) => {
    const existing = document.querySelector('script[data-quantura-marked="1"]');
    if (existing) {
      existing.addEventListener("load", () => resolve(window.marked || null), { once: true });
      existing.addEventListener("error", () => resolve(null), { once: true });
      return;
    }
    const script = document.createElement("script");
    script.src = MARKED_SCRIPT_URL;
    script.async = true;
    script.defer = true;
    script.dataset.quanturaMarked = "1";
    script.addEventListener("load", () => resolve(window.marked || null), { once: true });
    script.addEventListener("error", () => resolve(null), { once: true });
    document.head.appendChild(script);
  });
  return markedLoaderPromise;
}

function loadGiscusRuntime() {
  if (window.QuanturaGiscus?.mount) return Promise.resolve(window.QuanturaGiscus);
  if (giscusRuntimePromise) return giscusRuntimePromise;
  giscusRuntimePromise = new Promise((resolve, reject) => {
    const existing = document.querySelector('script[data-quantura-giscus-runtime="1"]');
    if (existing) {
      existing.addEventListener("load", () => resolve(window.QuanturaGiscus || null), { once: true });
      existing.addEventListener("error", () => reject(new Error("Unable to load the GitHub discussion runtime.")), { once: true });
      return;
    }
    const script = document.createElement("script");
    script.src = GISCUS_RUNTIME_URL;
    script.async = true;
    script.defer = true;
    script.dataset.quanturaGiscusRuntime = "1";
    script.addEventListener("load", () => {
      if (window.QuanturaGiscus?.mount) {
        resolve(window.QuanturaGiscus);
        return;
      }
      reject(new Error("GitHub discussion runtime is unavailable."));
    }, { once: true });
    script.addEventListener("error", () => reject(new Error("Unable to load the GitHub discussion runtime.")), { once: true });
    document.head.appendChild(script);
  });
  return giscusRuntimePromise;
}

function buildAbsoluteExploreUrl(path = "/explore") {
  try {
    return new URL(String(path || "/explore"), window.location.origin).toString();
  } catch {
    return `${window.location.origin}/explore`;
  }
}

function buildDiscussionTerm(scope, id) {
  const cleanScope = String(scope || "Quantura Discussion").trim();
  const cleanId = String(id || "general").trim();
  return `${cleanScope} · ${cleanId}`;
}

async function mountExplorePostComments(post) {
  if (!post) return;
  const host = document.getElementById("post-comments-host");
  const statusNode = document.getElementById("post-comments-status");
  const countNode = document.getElementById("post-comments-count");
  if (!host) return;
  try {
    const runtime = await loadGiscusRuntime();
    if (!runtime?.mount) throw new Error("GitHub discussion runtime is unavailable.");
    await runtime.mount(host, {
      statusNode,
      countNode,
      term: buildDiscussionTerm("Quantura Explore Post", post.id || post.slug || post.title || "general"),
      description: `Discussion for "${String(post.title || "Quantura Explore post").trim()}".`,
      backLink: buildAbsoluteExploreUrl(`/explore?post=${encodeURIComponent(String(post.id || "").trim())}`),
    });
  } catch (error) {
    if (statusNode) statusNode.textContent = error?.message || "Unable to load GitHub discussion.";
  }
}

function sanitizeMarkdownHtml(rawHtml) {
  const html = String(rawHtml || "").trim();
  if (!html) return "";
  const template = document.createElement("template");
  template.innerHTML = html;
  Array.from(template.content.querySelectorAll("*")).forEach((node) => {
    const tag = String(node.tagName || "").toLowerCase();
    if (!MARKDOWN_ALLOWED_TAGS.has(tag)) {
      node.replaceWith(document.createTextNode(node.textContent || ""));
      return;
    }
    Array.from(node.attributes || []).forEach((attr) => {
      const attrName = String(attr.name || "").toLowerCase();
      const attrValue = String(attr.value || "");
      if (!MARKDOWN_ALLOWED_ATTRS.has(attrName)) {
        node.removeAttribute(attr.name);
        return;
      }
      if (attrName === "href" && !/^(https?:|mailto:|tel:)/i.test(attrValue.trim())) {
        node.removeAttribute(attr.name);
      }
    });
    if (tag === "a") {
      node.setAttribute("target", "_blank");
      node.setAttribute("rel", "noopener noreferrer");
    }
  });
  return template.innerHTML;
}

function normalizeRichTextSource(value = "") {
  let source = String(value || "").replace(/\r\n?/g, "\n").trim();
  if (!source) return "";
  source = source
    .replace(/([^\n])\s+(\*\*[A-Z][^*\n]{1,80}:\*\*)/g, "$1\n\n$2")
    .replace(/([^\n])\s+(\*\s+\*\*[A-Z][^*\n]{1,80}:\*\*)/g, "$1\n$2")
    .replace(/([^\n])\s+(#{1,6}\s)/g, "$1\n\n$2");
  return source;
}

function renderRichText(text, { fallback = "No output available." } = {}) {
  const source = normalizeRichTextSource(text);
  if (!source) return `<p>${escapeHtml(fallback)}</p>`;
  try {
    if (window.marked?.parse) {
      const parsed = window.marked.parse(source, {
        gfm: true,
        breaks: true,
        mangle: false,
        headerIds: false,
      });
      return sanitizeMarkdownHtml(parsed);
    }
  } catch {
    // Fall back to escaped text.
  }
  return `<p>${escapeHtml(source).replace(/\n/g, "<br>")}</p>`;
}

function toPlainTextPreview(text, maxChars = 260) {
  const normalized = normalizeRichTextSource(text)
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[(.*?)\]\((.*?)\)/g, "$1")
    .replace(/[*_~>#]/g, "")
    .replace(/\$([^$]+)\$/g, "$1")
    .replace(/\n+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return { text: "", truncated: false };
  if (normalized.length <= maxChars) return { text: normalized, truncated: false };
  return {
    text: `${normalized.slice(0, maxChars - 1).trimEnd()}…`,
    truncated: true,
  };
}

function setAuthButton() {
  if (!refs.authToggle) return;
  const user = state.user;
  if (!user || user.isAnonymous) {
    refs.authToggle.textContent = "Sign in";
    return;
  }
  refs.authToggle.textContent = "Sign out";
}

function showLoading(visible) {
  refs.loading?.classList.toggle("hidden", !visible);
}

function showEmpty(visible) {
  refs.empty?.classList.toggle("hidden", !visible);
}

function setTabState() {
  refs.tabs.forEach((tab) => {
    const active = tab.dataset.mode === state.mode;
    tab.classList.toggle("is-active", active);
    tab.setAttribute("aria-selected", active ? "true" : "false");
  });
  refs.tickerWrap?.classList.toggle("hidden", state.mode !== "tickers");
}

function canRenderNativeFeedAds() {
  return isNativeRuntime() && !state.isPremium;
}

function ensureNativeAdImpressionObserver() {
  if (state.adImpressionObserver || typeof IntersectionObserver !== "function") return;
  state.adImpressionObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting || entry.intersectionRatio < 0.45) return;
        const target = entry.target;
        const slotId = String(target.dataset.nativeAdSlotId || "").trim();
        if (!slotId || state.adImpressionSeen.has(slotId)) return;
        state.adImpressionSeen.add(slotId);
        const placement = String(target.dataset.placement || "explore_feed").trim();
        const adUnitId = String(target.dataset.adUnitId || "").trim();
        track("ad_impression", { placement, slot_id: slotId, ad_unit_id: adUnitId });
        reportNativeFeedAdImpression({ slotId, placement, adUnitId });
      });
    },
    { threshold: [0.45] }
  );
}

function shouldInsertNativeFeedAd(position, config, totalCount) {
  if (!Number.isFinite(position) || position <= 0) return false;
  if (!Number.isFinite(totalCount) || totalCount < 2) return false;
  return position < totalCount;
}

function createNativeAdSlotNode(slotId, placement) {
  const card = document.createElement("article");
  card.className = "post-card native-ad-slot native-ad-loading";
  card.dataset.nativeAdSlotId = slotId;
  card.dataset.placement = placement;
  card.setAttribute("aria-live", "polite");
  card.innerHTML = `
    <div class="native-ad-skeleton">
      <div class="skeleton native-ad-line w50"></div>
      <div class="skeleton native-ad-line w85"></div>
      <div class="skeleton native-ad-line w70"></div>
      <div class="skeleton native-ad-media"></div>
    </div>
  `;
  return card;
}

function toInlineDataImage(value = "") {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^data:image\//i.test(raw)) return raw;
  if (/^https?:\/\//i.test(raw)) return raw;
  return `data:image/png;base64,${raw}`;
}

function hydrateNativeAdCard(slotNode, detail) {
  const ad = detail?.ad && typeof detail.ad === "object" ? detail.ad : detail;
  const slotId = String(detail?.slotId || slotNode.dataset.nativeAdSlotId || "").trim();
  const placement = String(detail?.placement || slotNode.dataset.placement || "explore_feed").trim();
  const adUnitId = String(detail?.adUnitId || ad?.adUnitId || "").trim();
  const headline = String(ad?.headline || "").trim();
  const body = String(ad?.body || "").trim();
  const cta = String(ad?.callToAction || "Learn more").trim();
  const advertiser = String(ad?.advertiser || ad?.store || "").trim();
  const iconUrl = toInlineDataImage(ad?.iconDataUrl || ad?.iconUrl || "");
  const mediaUrl = toInlineDataImage(ad?.mediaDataUrl || ad?.mediaUrl || "");
  const destinationUrl = /^https?:\/\//i.test(String(ad?.destinationUrl || "").trim()) ? String(ad.destinationUrl).trim() : "";

  slotNode.classList.remove("native-ad-loading");
  slotNode.classList.add("native-ad-ready");
  slotNode.dataset.adUnitId = adUnitId;
  slotNode.innerHTML = `
    <div class="native-ad-inner">
      <div class="native-ad-top">
        <span class="native-ad-badge">Ad</span>
        <span class="native-ad-choices">AdChoices</span>
      </div>
      <div class="native-ad-main">
        <div class="native-ad-copy">
          <h3 class="native-ad-headline">${escapeHtml(headline || "Sponsored insight")}</h3>
          <p class="native-ad-body">${escapeHtml(body || "Quantura partner message.")}</p>
          <div class="native-ad-meta">${escapeHtml(advertiser || "Sponsored")}</div>
        </div>
        ${
          iconUrl
            ? `<img class="native-ad-icon" src="${escapeHtml(iconUrl)}" alt="" loading="lazy" />`
            : `<div class="native-ad-icon native-ad-icon-fallback" aria-hidden="true">Q</div>`
        }
      </div>
      ${
        mediaUrl
          ? `<img class="native-ad-media" src="${escapeHtml(mediaUrl)}" alt="" loading="lazy" />`
          : `<div class="native-ad-media native-ad-media-fallback" aria-hidden="true"></div>`
      }
      <button type="button" class="native-ad-cta" data-native-ad-click="true">${escapeHtml(cta || "Learn more")}</button>
    </div>
  `;

  const clickHandler = (event) => {
    if (!event.target.closest("[data-native-ad-click='true']")) return;
    track("ad_click", { placement, slot_id: slotId, ad_unit_id: adUnitId });
    reportNativeFeedAdClick({ slotId, placement, adUnitId });
    if (destinationUrl) {
      window.open(destinationUrl, "_blank", "noopener,noreferrer");
    }
  };
  slotNode.addEventListener("click", clickHandler, { once: false });
  ensureNativeAdImpressionObserver();
  state.adImpressionObserver?.observe(slotNode);
}

async function requestNativeAdForSlot(slotNode) {
  if (!slotNode || !canRenderNativeFeedAds()) {
    slotNode?.remove();
    return;
  }
  const slotId = String(slotNode.dataset.nativeAdSlotId || "").trim();
  const placement = String(slotNode.dataset.placement || "explore_feed").trim();
  track("ad_request", { placement, slot_id: slotId });
  try {
    const detail = await requestNativeFeedAd({
      slotId,
      placement,
      variant: "nativeAdvanced",
      timeoutMs: 12000,
    });
    hydrateNativeAdCard(slotNode, detail || {});
    const adUnitId = String(detail?.adUnitId || detail?.ad?.adUnitId || "").trim();
    track("ad_loaded", { placement, slot_id: slotId, ad_unit_id: adUnitId });
  } catch (error) {
    track("ad_failed", { placement, slot_id: slotId, reason: String(error?.message || "load_failed").slice(0, 120) });
    slotNode.remove();
  }
}

function getPreviewMarkup(post) {
  const preview = post.preview || {};
  if (preview.kind === "image" && preview.imageUrl) {
    return `<img src="${escapeHtml(preview.imageUrl)}" alt="${escapeHtml(post.title)}" loading="lazy" />`;
  }

  const metrics = preview.metrics || {};
  const entries = Object.entries(metrics).slice(0, 6);
  if (!entries.length) {
    return `
      <div class="preview-summary">
        <div class="metric-pill"><span>Type</span><b>${escapeHtml(post.type)}</b></div>
        <div class="metric-pill"><span>Ticker</span><b>${escapeHtml((post.tickers || [])[0] || "N/A")}</b></div>
      </div>
    `;
  }

  return `
    <div class="preview-summary">
      ${entries
        .map(
          ([key, value]) =>
            `<div class="metric-pill"><span>${escapeHtml(key)}</span><b>${escapeHtml(String(value))}</b></div>`
        )
        .join("")}
    </div>
  `;
}

function getEngagementMarkup(post) {
  const counts = post.counts || {};
  const viewer = post.viewer || {};
  return `
    <button type="button" data-action="like" data-post-id="${escapeHtml(post.id)}" class="${viewer.liked ? "active" : ""}">♥ ${Number(counts.likes || 0)}</button>
    <button type="button" data-action="comment" data-post-id="${escapeHtml(post.id)}">💬 Discuss</button>
    <button type="button" data-action="repost" data-post-id="${escapeHtml(post.id)}" class="${viewer.reposted ? "active" : ""}">↻ ${Number(counts.reposts || 0)}</button>
    <button type="button" data-action="share" data-post-id="${escapeHtml(post.id)}">↗ ${Number(counts.shares || 0)}</button>
    <button type="button" data-action="save" data-post-id="${escapeHtml(post.id)}" class="${viewer.saved ? "active" : ""}">★ Save</button>
    <button type="button" data-action="report" data-post-id="${escapeHtml(post.id)}">⚑</button>
  `;
}

function renderCard(post) {
  const fragment = refs.cardTemplate?.content?.firstElementChild?.cloneNode(true);
  if (!fragment) return null;

  fragment.dataset.postId = post.id;
  fragment.querySelector(".post-preview").innerHTML = getPreviewMarkup(post);
  fragment.querySelector(".post-title").textContent = post.title || "Untitled";
  const captionPreview = toPlainTextPreview(post.caption || "", 260);
  const captionNode = fragment.querySelector(".post-caption");
  if (captionNode) {
    captionNode.textContent = captionPreview.text || "";
    captionNode.classList.toggle("hidden", !captionPreview.text);
  }
  fragment.querySelector(".chip-row").innerHTML = (post.tickers || [])
    .slice(0, 4)
    .map((ticker) => `<span class="chip">${escapeHtml(ticker)}</span>`)
    .join("");
  fragment.querySelector(".post-meta").textContent = `@${post.authorHandle || "quantura"} • ${formatRelativeTime(post.createdAtMs)}`;
  fragment.querySelector(".engagement-row").innerHTML = getEngagementMarkup(post);
  if (post.hasBody || captionPreview.truncated) {
    fragment.querySelector(".post-body")?.insertAdjacentHTML(
      "beforeend",
      `<div class="post-expand-row"><button type="button" class="see-more-btn" data-action="open" data-post-id="${escapeHtml(
        post.id
      )}">See more</button></div>`
    );
  }

  return fragment;
}

function renderSkeleton(count = 8) {
  if (!refs.grid || !refs.skeletonTemplate) return;
  refs.grid.innerHTML = "";
  for (let i = 0; i < count; i += 1) {
    const node = refs.skeletonTemplate.content?.firstElementChild?.cloneNode(true);
    if (node) refs.grid.appendChild(node);
  }
}

function upsertPost(post) {
  state.postsById.set(post.id, post);
  const existing = refs.grid?.querySelector(`[data-post-id="${CSS.escape(post.id)}"]`);
  const next = renderCard(post);
  if (!next) return;
  if (existing) existing.replaceWith(next);
  else refs.grid?.appendChild(next);
}

function appendPosts(posts, reset = false) {
  if (!refs.grid) return;
  if (reset) {
    refs.grid.innerHTML = "";
    state.adImpressionSeen.clear();
  }

  const canShowAds = canRenderNativeFeedAds();
  const rules = getNativeFeedAdRules();
  const existingPostCount = reset ? 0 : refs.grid.querySelectorAll("[data-post-id]").length;
  const totalPostCount = existingPostCount + posts.length;
  const fragment = document.createDocumentFragment();
  const pendingAdSlots = [];
  let renderedPostIndex = existingPostCount;

  posts.forEach((post) => {
    state.postsById.set(post.id, post);
    const node = renderCard(post);
    if (!node) return;
    fragment.appendChild(node);
    renderedPostIndex += 1;

    if (!canShowAds) return;
    if (!shouldInsertNativeFeedAd(renderedPostIndex, rules, totalPostCount)) return;
    const slotId = `explore-feed-${renderedPostIndex}`;
    const slotNode = createNativeAdSlotNode(slotId, "explore_feed");
    fragment.appendChild(slotNode);
    pendingAdSlots.push(slotNode);
  });

  refs.grid.appendChild(fragment);
  pendingAdSlots.forEach((slotNode) => {
    requestNativeAdForSlot(slotNode).catch(() => undefined);
  });

  showEmpty(state.postsById.size === 0);
}

async function refreshPremiumStatus() {
  state.isPremium = false;
  if (!state.api) return;
  if (!state.user || state.user.isAnonymous) return;
  try {
    const profile = await state.api.get("/me/profile");
    state.isPremium = Boolean(profile?.premium);
  } catch {
    state.isPremium = false;
  }
}

async function loadSuggestions() {
  if (!state.api || !refs.tickerSuggestions) return;
  try {
    const response = await state.api.get(`/explore/suggestions?query=${encodeURIComponent(refs.tickerInput?.value || "")}`);
    const suggestions = Array.isArray(response.suggestions) ? response.suggestions : [];
    refs.tickerSuggestions.innerHTML = suggestions.map((item) => `<option value="${escapeHtml(item)}"></option>`).join("");
  } catch {
    // Ignore suggestion errors.
  }
}

async function loadPosts(reset = false) {
  if (!state.api || state.loading || (state.done && !reset)) return;
  state.loading = true;

  if (reset) {
    state.cursor = null;
    state.done = false;
    state.postsById.clear();
    renderSkeleton(8);
  }

  showLoading(true);

  try {
    const params = new URLSearchParams();
    params.set("mode", state.mode);
    params.set("limit", "20");
    if (state.cursor) params.set("cursor", state.cursor);
    if (state.q) params.set("q", state.q);
    if (state.ticker) params.set("ticker", state.ticker);

    const response = await state.api.get(`/explore?${params.toString()}`);
    const posts = Array.isArray(response.posts) ? response.posts : [];

    appendPosts(posts, reset);
    state.cursor = response.cursor || null;
    state.done = !state.cursor;

    track("explore_feed_loaded", {
      mode: state.mode,
      count: posts.length,
      has_cursor: Boolean(state.cursor),
    });
  } catch (error) {
    if (refs.grid) {
      refs.grid.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "Unable to load Explore feed.")}</div>`;
    }
  } finally {
    state.loading = false;
    showLoading(false);
    showEmpty(!state.postsById.size);
  }
}

async function refreshPost(postId) {
  if (!postId || !state.api) return;
  try {
    const detail = await state.api.get(`/posts/${encodeURIComponent(postId)}`);
    if (detail?.post) {
      state.postsById.set(postId, detail.post);
      upsertPost(detail.post);
      if (state.activePostId === postId) {
        renderModal(detail.post);
        await mountExplorePostComments(detail.post);
      }
    }
  } catch {
    // Ignore refresh failures.
  }
}

async function doPostAction(action, postId) {
  if (!state.api || !postId) return;
  if (action === "open") {
    await openPostModal(postId);
    return;
  }
  if (action === "comment") {
    await openPostModal(postId);
    return;
  }

  if (action === "share") {
    const shareUrl = `${window.location.origin}/explore?post=${encodeURIComponent(postId)}`;
    const post = state.postsById.get(postId);
    try {
      if (navigator.share) {
        await navigator.share({
          title: post?.title || "Quantura Explore",
          text: post?.caption || "",
          url: shareUrl,
        });
      } else if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(shareUrl);
      }
    } catch {
      // User cancelled share.
    }

    await state.api.post(`/posts/${encodeURIComponent(postId)}/share`, { source: "web_share" });
    await refreshPost(postId);
    track("explore_post_shared", { post_id: postId });
    return;
  }

  if (action === "report") {
    openReportModal(postId);
    return;
  }

  if (action === "save") {
    const response = await state.api.post(`/posts/${encodeURIComponent(postId)}/save`, {});
    await refreshPost(postId);
    track("explore_post_saved", { post_id: postId, saved: Boolean(response.saved) });
    return;
  }

  if (action === "like") {
    await state.api.post(`/posts/${encodeURIComponent(postId)}/like`, {});
    await refreshPost(postId);
    track("explore_post_liked", { post_id: postId });
    return;
  }

  if (action === "repost") {
    await state.api.post(`/posts/${encodeURIComponent(postId)}/repost`, {});
    await refreshPost(postId);
    track("explore_post_reposted", { post_id: postId });
  }
}

function closeModal() {
  refs.postModal?.classList.add("hidden");
  refs.postModal?.setAttribute("aria-hidden", "true");
  state.activePostId = "";
}

function closeReportModal() {
  refs.reportModal?.classList.add("hidden");
  refs.reportModal?.setAttribute("aria-hidden", "true");
  state.reportPostId = "";
  if (refs.reportForm) refs.reportForm.reset();
}

function openReportModal(postId) {
  state.reportPostId = String(postId || "");
  if (refs.reportReason) refs.reportReason.value = "spam";
  if (refs.reportDetails) refs.reportDetails.value = "";
  refs.reportModal?.classList.remove("hidden");
  refs.reportModal?.setAttribute("aria-hidden", "false");
}

function renderModal(post) {
  if (!refs.postModalBody) return;
  const summaryText = normalizeRichTextSource(post.caption || "");
  const fullBody = normalizeRichTextSource(post.body || post.caption || "");

  refs.postModalBody.innerHTML = `
    <div class="post-preview post-modal-preview">${getPreviewMarkup(post)}</div>
    <div class="post-modal-head">
      <h2 id="post-modal-title">${escapeHtml(post.title || "")}</h2>
      <p class="muted">@${escapeHtml(post.authorHandle || "quantura")} • ${formatRelativeTime(post.createdAtMs)}</p>
      <div class="chip-row">${(post.tickers || []).map((ticker) => `<span class="chip">${escapeHtml(ticker)}</span>`).join("")}</div>
      <div class="engagement-row">${getEngagementMarkup(post)}</div>
    </div>

    ${
      summaryText && fullBody && summaryText !== fullBody
        ? `<section class="post-detail-section"><div class="post-detail-label">Summary</div><div class="post-rich-text markdown-output">${renderRichText(
            summaryText,
            {
              fallback: "No summary available.",
            }
          )}</div></section>`
        : ""
    }

    <section class="post-detail-section">
      <div class="post-detail-label">Full response</div>
      <div class="post-rich-text markdown-output">${renderRichText(fullBody, { fallback: "No response body available." })}</div>
    </section>

    <section class="comments-panel comments-panel--giscus">
      <div class="comments-header">
        <div>
          <h3>GitHub Discussion</h3>
          <p class="small muted" id="post-comments-status">Comments powered by GitHub Discussions.</p>
        </div>
        <span class="comments-count" id="post-comments-count">${Number(post?.counts?.comments || 0)}</span>
      </div>
      <p class="small muted">Sign in with GitHub inside the discussion widget to join the thread for this Explore post.</p>
      <div id="post-comments-host" class="quantura-comments-host"></div>
    </section>
  `;
}

async function openPostModal(postId) {
  if (!state.api || !postId) return;
  try {
    const detail = await state.api.get(`/posts/${encodeURIComponent(postId)}`);
    if (!detail?.post) return;

    state.activePostId = postId;
    state.postsById.set(postId, detail.post);
    upsertPost(detail.post);
    await ensureMarkedLibrary().catch(() => null);
    renderModal(detail.post);
    await mountExplorePostComments(detail.post);
    refs.postModal?.classList.remove("hidden");
    refs.postModal?.setAttribute("aria-hidden", "false");
    track("explore_post_opened", { post_id: postId });
  } catch (error) {
    window.alert(error.message || "Unable to load post details.");
  }
}

function bindEvents() {
  refs.authToggle?.addEventListener("click", async () => {
    if (!state.authClient) return;
    try {
      if (!state.user || state.user.isAnonymous) {
        await state.authClient.signInWithGoogle();
      } else {
        await state.authClient.signOut();
      }
    } catch (error) {
      window.alert(error.message || "Authentication action failed.");
    }
  });

  refs.tabs.forEach((tab) => {
    tab.addEventListener("click", async () => {
      const mode = tab.dataset.mode || "trending";
      state.mode = mode;
      if (mode !== "tickers") state.ticker = "";
      setTabState();
      await loadPosts(true);
    });
  });

  refs.tickerInput?.addEventListener("input", () => {
    loadSuggestions().catch(() => undefined);
  });

  refs.tickerApply?.addEventListener("click", async () => {
    state.ticker = normalizeTicker(refs.tickerInput?.value || "");
    state.mode = "tickers";
    setTabState();
    await loadPosts(true);
  });

  refs.grid?.addEventListener("click", async (event) => {
    const actionBtn = event.target.closest("[data-action][data-post-id]");
    if (actionBtn) {
      event.preventDefault();
      event.stopPropagation();
      await doPostAction(actionBtn.dataset.action, actionBtn.dataset.postId);
      return;
    }

    const card = event.target.closest("[data-post-id]");
    if (!card) return;
    await openPostModal(card.dataset.postId);
  });

  refs.grid?.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter") return;
    const card = event.target.closest("[data-post-id]");
    if (!card) return;
    await openPostModal(card.dataset.postId);
  });

  refs.modalClose?.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    closeModal();
  });
  refs.postModal?.addEventListener("click", (event) => {
    const closeTrigger = event.target.closest("[data-close-modal='true']");
    if (closeTrigger || event.target === refs.postModal) {
      event.preventDefault();
      closeModal();
    }
  });
  refs.postModal?.querySelector(".modal-panel")?.addEventListener("click", (event) => {
    event.stopPropagation();
  });
  refs.reportModalClose?.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    closeReportModal();
  });
  refs.reportModal?.addEventListener("click", (event) => {
    const closeTrigger = event.target.closest("[data-report-close='true']");
    if (closeTrigger || event.target === refs.reportModal) {
      event.preventDefault();
      closeReportModal();
    }
  });
  refs.reportModal?.querySelector(".modal-panel")?.addEventListener("click", (event) => {
    event.stopPropagation();
  });
  window.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (refs.reportModal && !refs.reportModal.classList.contains("hidden")) {
      closeReportModal();
      return;
    }
    if (refs.postModal && !refs.postModal.classList.contains("hidden")) {
      closeModal();
    }
  });

  refs.postModalBody?.addEventListener("click", async (event) => {
    const actionBtn = event.target.closest("[data-action][data-post-id]");
    if (actionBtn) {
      await doPostAction(actionBtn.dataset.action, actionBtn.dataset.postId);
      return;
    }

    const deleteBtn = event.target.closest("[data-comment-delete][data-post-id]");
    if (!deleteBtn || !state.api) return;
    try {
      await state.api.delete(
        `/posts/${encodeURIComponent(deleteBtn.dataset.postId)}/comment/${encodeURIComponent(deleteBtn.dataset.commentDelete)}`
      );
      await refreshPost(deleteBtn.dataset.postId);
    } catch (error) {
      window.alert(error.message || "Unable to delete comment.");
    }
  });

  refs.postModalBody?.addEventListener("submit", async (event) => {
    const form = event.target.closest("[data-comment-form]");
    if (!form || !state.api) return;
    event.preventDefault();

    const postId = form.dataset.commentForm;
    const textarea = form.querySelector("textarea[name='comment']");
    const text = String(textarea?.value || "").trim();
    if (!text) return;

    try {
      await state.api.post(`/posts/${encodeURIComponent(postId)}/comment`, { text });
      textarea.value = "";
      await refreshPost(postId);
      track("explore_comment_created", { post_id: postId });
    } catch (error) {
      window.alert(error.message || "Unable to add comment.");
    }
  });

  refs.reportForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!state.api || !state.reportPostId) return;
    const reason = String(refs.reportReason?.value || "").trim().toLowerCase();
    const details = String(refs.reportDetails?.value || "").trim();
    if (!reason) return;
    try {
      await state.api.post(`/posts/${encodeURIComponent(state.reportPostId)}/report`, { reason, details });
      closeReportModal();
      window.alert("Thanks. The report has been submitted.");
      track("explore_post_reported", { post_id: state.reportPostId, reason });
      await refreshPost(state.reportPostId);
    } catch (error) {
      window.alert(error.message || "Unable to submit report.");
    }
  });

  state.observer = new IntersectionObserver(
    (entries) => {
      const first = entries[0];
      if (!first?.isIntersecting) return;
      loadPosts(false).catch(() => undefined);
    },
    {
      rootMargin: "200px",
    }
  );

  if (refs.sentinel) state.observer.observe(refs.sentinel);
}

function bindMobileBottomNav() {
  const normalizePath = (value) => {
    const cleaned = String(value || "/").split("?")[0].split("#")[0].trim() || "/";
    const normalized = cleaned.startsWith("/") ? cleaned : `/${cleaned}`;
    return normalized.length > 1 ? normalized.replace(/\/+$/, "") : normalized;
  };
  const path = normalizePath(window.location.pathname || "/explore");
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

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  state.mode = params.get("mode") || "trending";
  if (!["trending", "latest", "following", "tickers"].includes(state.mode)) state.mode = "trending";
  state.q = String(params.get("q") || "").trim();
  state.ticker = normalizeTicker(params.get("ticker") || "");

  setTabState();
  if (refs.tickerInput) refs.tickerInput.value = state.ticker;

  state.authClient = await initAuth((user) => {
    state.user = user;
    setAuthButton();
    refreshPremiumStatus()
      .catch(() => undefined)
      .finally(() => {
        if (state.initialized) {
          loadPosts(true).catch(() => undefined);
        }
      });
  });

  state.api = await createApiClient(() => state.authClient.getAuthToken());
  ensureMarkedLibrary().catch(() => null);
  bindEvents();
  bindMobileBottomNav();
  await refreshPremiumStatus();
  await loadSuggestions();
  await loadPosts(true);
  state.initialized = true;

  const deepLinkPost = params.get("post");
  if (deepLinkPost) {
    openPostModal(deepLinkPost).catch(() => undefined);
  }
}

bootstrap().catch((error) => {
  if (refs.grid) {
    refs.grid.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "Unable to initialize Explore.")}</div>`;
  }
});
