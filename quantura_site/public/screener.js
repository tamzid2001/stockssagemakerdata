(function () {
  "use strict";

  const root = document.getElementById("qs-filters");
  if (!root) return;

  const refs = {
    search: document.getElementById("qs-search"),
    universe: document.getElementById("qs-universe"),
    marketCap: document.getElementById("qs-market-cap"),
    bias: document.getElementById("qs-bias"),
    earnings: document.getElementById("qs-earnings"),
    sort: document.getElementById("qs-sort"),
    direction: document.getElementById("qs-direction"),
    specialP10: document.getElementById("qs-special-p10"),
    clear: document.getElementById("qs-clear"),
    emptyClear: document.getElementById("qs-empty-clear"),
    refresh: document.getElementById("qs-refresh"),
    retry: document.getElementById("qs-retry"),
    export: document.getElementById("qs-export"),
    status: document.getElementById("qs-status"),
    freshness: document.getElementById("qs-freshness"),
    loading: document.getElementById("qs-loading"),
    error: document.getElementById("qs-error"),
    errorMessage: document.getElementById("qs-error-message"),
    empty: document.getElementById("qs-empty"),
    tableWrap: document.getElementById("qs-table-wrap"),
    tableBody: document.getElementById("qs-table-body"),
    pagination: document.getElementById("qs-pagination"),
    previous: document.getElementById("qs-previous"),
    next: document.getElementById("qs-next"),
    pageLabel: document.getElementById("qs-page-label"),
    filterCount: document.getElementById("qs-filter-count"),
    metricMatches: document.getElementById("qs-metric-matches"),
    metricTotal: document.getElementById("qs-metric-total"),
    metricCoverage: document.getElementById("qs-metric-coverage"),
    metricProcessed: document.getElementById("qs-metric-processed"),
    metricDate: document.getElementById("qs-metric-date"),
    metricFreshness: document.getElementById("qs-metric-freshness"),
  };

  const defaults = Object.freeze({
    search: "",
    universe: "all",
    marketCap: "all",
    bias: "all",
    earnings: "all",
    sort: "ticker",
    direction: "asc",
    specialP10: false,
    positions: [],
    page: 1,
    pageSize: 50,
  });
  let current = { ...defaults, positions: [] };
  let activeRequest = null;
  let searchTimer = null;
  let lastPersistedUrl = `${window.location.pathname}${window.location.search}`;

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function finite(value) {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function formatNumber(value, digits) {
    const parsed = finite(value);
    if (parsed === null) return "Unavailable";
    return parsed.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
  }

  function formatPrice(value) {
    const parsed = finite(value);
    if (parsed === null) return "Unavailable";
    const digits = Math.abs(parsed) < 10 ? 4 : 2;
    return `$${formatNumber(parsed, digits)}`;
  }

  function formatCap(value, isEtf) {
    if (isEtf) return "N/A — ETF";
    const parsed = finite(value);
    if (parsed === null) return "Unavailable";
    if (parsed >= 1e12) return `$${(parsed / 1e12).toFixed(2)}T`;
    if (parsed >= 1e9) return `$${(parsed / 1e9).toFixed(parsed >= 1e11 ? 0 : 1)}B`;
    if (parsed >= 1e6) return `$${(parsed / 1e6).toFixed(0)}M`;
    return `$${parsed.toLocaleString()}`;
  }

  function formatPercent(value) {
    const parsed = finite(value);
    if (parsed === null) return "—";
    const sign = parsed > 0 ? "+" : "";
    return `${sign}${parsed.toFixed(1)}%`;
  }

  function formatDate(value, includeTime) {
    const raw = String(value || "").trim();
    if (!raw) return "Unavailable";
    if (raw.startsWith("N/A")) return raw;
    const parsed = new Date(raw.length === 10 ? `${raw}T00:00:00Z` : raw);
    if (Number.isNaN(parsed.getTime())) return "Unavailable";
    return new Intl.DateTimeFormat(undefined, includeTime
      ? { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }
      : { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" }
    ).format(parsed);
  }

  function freshnessLabel(value) {
    const parsed = new Date(String(value || ""));
    if (Number.isNaN(parsed.getTime())) return "Freshness unavailable";
    const elapsed = Math.max(0, Date.now() - parsed.getTime());
    const hours = Math.floor(elapsed / 3600000);
    if (hours < 1) return "Updated less than an hour ago";
    if (hours < 24) return `Updated ${hours} hour${hours === 1 ? "" : "s"} ago`;
    const days = Math.floor(hours / 24);
    return `${days} day${days === 1 ? "" : "s"} old${days > 2 ? " · stale" : ""}`;
  }

  function checkedPositions() {
    return Array.from(root.querySelectorAll('input[name="position"]:checked')).map((input) => input.value);
  }

  function readControls() {
    return {
      ...current,
      search: String(refs.search.value || "").trim(),
      universe: refs.universe.value || "all",
      marketCap: refs.marketCap.value || "all",
      bias: refs.bias.value || "all",
      earnings: refs.earnings.value || "all",
      sort: refs.sort.value || "ticker",
      direction: refs.direction.value === "desc" ? "desc" : "asc",
      specialP10: Boolean(refs.specialP10.checked),
      positions: checkedPositions(),
    };
  }

  function writeControls(state) {
    refs.search.value = state.search || "";
    refs.universe.value = state.universe || "all";
    refs.marketCap.value = state.marketCap || "all";
    refs.bias.value = state.bias || "all";
    refs.earnings.value = state.earnings || "all";
    refs.sort.value = state.sort || "ticker";
    refs.direction.value = state.direction || "asc";
    refs.specialP10.checked = Boolean(state.specialP10);
    root.querySelectorAll('input[name="position"]').forEach((input) => {
      input.checked = state.positions.includes(input.value);
    });
  }

  function parseUrl() {
    const params = new URLSearchParams(window.location.search);
    const allowed = {
      universe: ["all", "sp500", "nasdaq", "etf"],
      marketCap: ["all", "mega", "large", "mid", "small", "micro"],
      bias: ["all", "buying", "selling", "neutral"],
      earnings: ["all", "today", "7", "14", "30", "unknown"],
      direction: ["asc", "desc"],
    };
    const state = { ...defaults, positions: [] };
    state.search = String(params.get("search") || "").slice(0, 80);
    Object.keys(allowed).forEach((key) => {
      const value = params.get(key);
      if (allowed[key].includes(value)) state[key] = value;
    });
    const sort = params.get("sort");
    if (Array.from(refs.sort.options).some((option) => option.value === sort)) state.sort = sort;
    state.specialP10 = params.get("specialP10") === "true";
    const validPositions = new Set(["below-p10", "above-p10", "below-p50", "above-p50", "below-p90", "above-p90"]);
    state.positions = String(params.get("position") || "").split(",").filter((value) => validPositions.has(value));
    const page = Number(params.get("page"));
    state.page = Number.isInteger(page) && page > 0 ? page : 1;
    return state;
  }

  function buildParams(state) {
    const params = new URLSearchParams();
    if (state.search) params.set("search", state.search);
    if (state.universe !== "all") params.set("universe", state.universe);
    if (state.marketCap !== "all") params.set("marketCap", state.marketCap);
    if (state.bias !== "all") params.set("bias", state.bias);
    if (state.earnings !== "all") params.set("earnings", state.earnings);
    if (state.sort !== "ticker") params.set("sort", state.sort);
    if (state.direction !== "asc") params.set("direction", state.direction);
    if (state.specialP10) params.set("specialP10", "true");
    if (state.positions.length) params.set("position", state.positions.join(","));
    if (state.page > 1) params.set("page", String(state.page));
    params.set("pageSize", String(state.pageSize));
    return params;
  }

  function persistUrl(state) {
    const params = buildParams(state);
    params.delete("pageSize");
    const query = params.toString();
    const nextUrl = `${window.location.pathname}${query ? `?${query}` : ""}`;
    if (nextUrl === lastPersistedUrl) return;
    window.history.pushState({ screener: true }, "", nextUrl);
    lastPersistedUrl = nextUrl;
  }

  function updateFilterCount(state) {
    const active = [state.search, state.universe !== "all", state.marketCap !== "all", state.bias !== "all", state.earnings !== "all", state.specialP10]
      .filter(Boolean).length + state.positions.length;
    refs.filterCount.textContent = `${active} active`;
  }

  function positionView(value) {
    const map = {
      below_p10: ["Below P10", "qs-position-below"],
      between_p10_p50: ["P10 → P50", "qs-position-between"],
      between_p50_p90: ["P50 → P90", "qs-position-between"],
      above_p90: ["Above P90", "qs-position-above"],
    };
    return map[value] || ["Unavailable", "qs-muted-cell"];
  }

  function distanceView(value) {
    const parsed = finite(value);
    const cls = parsed === null ? "qs-muted-cell" : parsed > 0 ? "qs-positive" : parsed < 0 ? "qs-negative" : "";
    return `<span class="${cls}">${escapeHtml(formatPercent(parsed))}</span>`;
  }

  function signalView(row) {
    const bias = String(row.general_bias || "Unavailable");
    const biasClass = bias === "Buying Bias" ? "qs-position-below" : bias === "Selling Bias" ? "qs-position-above" : "";
    return `<div class="qs-signal-stack"><span class="qs-badge ${biasClass}">${escapeHtml(bias)}</span>${row.p10_signal_active ? '<span class="qs-badge qs-special-badge">2W P10 Buy Bias</span>' : ""}<small>${escapeHtml(String(row.unusual_p50_count ?? 0))} unusual P50</small></div>`;
  }

  function rowHtml(row) {
    const memberships = Array.isArray(row.universe_memberships) ? row.universe_memberships : [];
    const position = positionView(row.quantile_position);
    const suppliedUrl = String(row.analysis_url || "");
    const analysisUrl = suppliedUrl.startsWith("/") && !suppliedUrl.startsWith("//")
      ? suppliedUrl
      : `/forecasting?ticker=${encodeURIComponent(String(row.ticker || ""))}`;
    const actualStamp = row.actual_price_timestamp ? ` title="Actual close ${escapeHtml(formatDate(row.actual_price_timestamp, true))}"` : "";
    return `<tr>
      <td data-label="Security"><div class="qs-security"><a href="${escapeHtml(analysisUrl)}" aria-label="Open ${escapeHtml(row.ticker)} forecast analysis">${escapeHtml(row.ticker)}</a><span class="qs-universe-tags">${memberships.map((item) => `<span>${escapeHtml(item)}</span>`).join("")}</span><span class="qs-company" title="${escapeHtml(row.company_name || "")}">${escapeHtml(row.company_name || "Company name unavailable")}</span></div></td>
      <td data-label="Actual" class="qs-mono"${actualStamp}>${escapeHtml(formatPrice(row.actual_price))}</td>
      <td data-label="P10" class="qs-mono">${escapeHtml(formatPrice(row.p10))}</td>
      <td data-label="P50" class="qs-mono">${escapeHtml(formatPrice(row.p50))}</td>
      <td data-label="P90" class="qs-mono">${escapeHtml(formatPrice(row.p90))}</td>
      <td data-label="Position"><span class="${position[1]}">${escapeHtml(position[0])}</span></td>
      <td data-label="Distance P10 / P50 / P90"><div class="qs-distance-stack">${distanceView(row.distance_p10_pct)}${distanceView(row.distance_p50_pct)}${distanceView(row.distance_p90_pct)}</div></td>
      <td data-label="Market cap" class="qs-mono">${escapeHtml(formatCap(row.market_cap, row.is_etf))}</td>
      <td data-label="Next earnings">${escapeHtml(formatDate(row.next_earnings_date, false))}</td>
      <td data-label="Model signal">${signalView(row)}</td>
      <td data-label="Updated" title="Forecast horizon ends ${escapeHtml(formatDate(row.forecast_date, false))}">${escapeHtml(formatDate(row.last_forecast_update, true))}</td>
    </tr>`;
  }

  function setView(name) {
    refs.loading.hidden = name !== "loading";
    refs.error.hidden = name !== "error";
    refs.empty.hidden = name !== "empty";
    refs.tableWrap.hidden = name !== "table";
    refs.pagination.hidden = name !== "table";
  }

  function disableExport(disabled) {
    if (disabled) {
      refs.export.removeAttribute("href");
      refs.export.setAttribute("aria-disabled", "true");
      refs.export.classList.add("disabled");
    } else {
      refs.export.href = "/api/screener/export.csv";
      refs.export.removeAttribute("aria-disabled");
      refs.export.classList.remove("disabled");
    }
  }

  function render(payload) {
    const items = Array.isArray(payload.items) ? payload.items : [];
    const manifest = payload.manifest || {};
    refs.metricMatches.textContent = Number(payload.total || 0).toLocaleString();
    refs.metricTotal.textContent = `of ${Number(payload.universeCount || 0).toLocaleString()} in universe`;
    const coverage = finite(manifest.coverage_percentage);
    refs.metricCoverage.textContent = coverage === null ? "Unavailable" : `${coverage.toFixed(1)}%`;
    refs.metricProcessed.textContent = `${Number(manifest.successfully_processed || 0).toLocaleString()} evaluated · ${Number(manifest.failed || 0).toLocaleString()} failed`;
    refs.metricDate.textContent = formatDate(payload.generatedAt || payload.scanDate, false);
    refs.metricFreshness.textContent = freshnessLabel(payload.generatedAt);
    refs.freshness.textContent = `Scan ${formatDate(payload.generatedAt, true)} · actual price is the most recent adjusted daily bar close · forecast timestamp varies by row.`;
    refs.status.textContent = `${Number(payload.total || 0).toLocaleString()} of ${Number(payload.universeCount || 0).toLocaleString()} securities match the active research filters.`;
    current.page = Number(payload.page || current.page || 1);
    refs.pageLabel.textContent = `Page ${current.page.toLocaleString()} of ${Number(payload.pageCount || 1).toLocaleString()}`;
    refs.previous.disabled = current.page <= 1;
    refs.next.disabled = current.page >= Number(payload.pageCount || 1);
    refs.tableBody.textContent = "";
    const sanitizedHtml = DOMPurify.sanitize(items.map(rowHtml).join(""));
    refs.tableBody.insertAdjacentHTML("beforeend", sanitizedHtml);
    disableExport(false);
    setView(items.length ? "table" : "empty");
  }

  async function load(options) {
    const settings = options || {};
    if (activeRequest) activeRequest.abort();
    activeRequest = new AbortController();
    current = readControls();
    if (settings.resetPage) current.page = 1;
    writeControls(current);
    updateFilterCount(current);
    persistUrl(current);
    setView("loading");
    disableExport(true);
    refs.status.textContent = "Scanning market data…";
    try {
      const response = await fetch(`/api/screener/data?${buildParams(current).toString()}`, {
        method: "GET",
        headers: { Accept: "application/json" },
        cache: settings.force ? "reload" : "default",
        signal: activeRequest.signal,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.detail || "The screener service returned an unavailable response.");
      render(payload);
    } catch (error) {
      if (error && error.name === "AbortError") return;
      const message = String(error && error.message ? error.message : "").trim();
      refs.errorMessage.textContent = !message || message === "Failed to fetch"
        ? "Market data could not be reached. Check your connection and retry; the last validated scan has not been changed."
        : message;
      refs.status.textContent = "The validated screener dataset is currently unavailable.";
      setView("error");
    }
  }

  function clearFilters() {
    current = { ...defaults, positions: [] };
    writeControls(current);
    load({ resetPage: true });
  }

  root.addEventListener("submit", (event) => {
    event.preventDefault();
    load({ resetPage: true });
  });
  root.addEventListener("change", () => load({ resetPage: true }));
  refs.search.addEventListener("input", () => {
    window.clearTimeout(searchTimer);
    searchTimer = window.setTimeout(() => load({ resetPage: true }), 250);
  });
  refs.clear.addEventListener("click", clearFilters);
  refs.emptyClear.addEventListener("click", clearFilters);
  refs.refresh.addEventListener("click", () => load({ force: true }));
  refs.retry.addEventListener("click", () => load({ force: true }));
  refs.previous.addEventListener("click", () => {
    if (current.page <= 1) return;
    current.page -= 1;
    load();
  });
  refs.next.addEventListener("click", () => {
    current.page += 1;
    load();
  });
  refs.export.addEventListener("click", (event) => {
    if (refs.export.getAttribute("aria-disabled") === "true") event.preventDefault();
  });
  window.addEventListener("popstate", () => {
    lastPersistedUrl = `${window.location.pathname}${window.location.search}`;
    current = parseUrl();
    writeControls(current);
    load();
  });

  current = parseUrl();
  writeControls(current);
  disableExport(true);
  load();
})();
