(function () {
  "use strict";

  const root = document.getElementById("qs-filters");
  if (!root) return;

  const refs = {
    source: document.getElementById("qs-source"),
    search: document.getElementById("qs-search"),
    universe: document.getElementById("qs-universe"),
    marketCap: document.getElementById("qs-market-cap"),
    signal: document.getElementById("qs-signal"),
    statistic: document.getElementById("qs-statistic"),
    rules: document.getElementById("qs-rules"),
    addRule: document.getElementById("qs-add-rule"),
    sort: document.getElementById("qs-sort"),
    direction: document.getElementById("qs-direction"),
    signalChanged: document.getElementById("qs-signal-changed"),
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
    source: "stocks",
    search: "",
    universe: "all",
    marketCap: "all",
    signal: "all",
    statistic: "row",
    quantileRules: [],
    sort: "ticker",
    direction: "asc",
    signalChanged: false,
    positions: [],
    page: 1,
    pageSize: 50,
  });
  let current = { ...defaults, positions: [] };
  let activeRequest = null;
  let searchTimer = null;
  let lastPersistedUrl = `${window.location.pathname}${window.location.search}`;
  let savedAlerts = [];
  let savedAlertsMeta = {};
  let alertPanelLoading = null;

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
    const absolute = Math.abs(parsed);
    const digits = absolute >= 10 ? 2 : absolute >= 1 ? 4 : absolute >= 0.01 ? 6 : 8;
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
      source: refs.source?.value || "stocks",
      search: String(refs.search.value || "").trim(),
      universe: refs.universe.value || "all",
      marketCap: refs.marketCap.value || "all",
      signal: refs.signal.value || "all",
      statistic: refs.statistic.value || "row",
      quantileRules: Array.from(refs.rules.querySelectorAll(".qs-rule")).map(rule => ({
        quantile:rule.querySelector('[data-rule="quantile"]').value,statistic:rule.querySelector('[data-rule="statistic"]').value,
        operator:rule.querySelector('[data-rule="operator"]').value,percent:Number(rule.querySelector('[data-rule="percent"]').value),
      })),
      sort: refs.sort.value || "ticker",
      direction: refs.direction.value === "desc" ? "desc" : "asc",
      signalChanged: Boolean(refs.signalChanged.checked),
      positions: checkedPositions(),
    };
  }

  function writeControls(state) {
    if(refs.source)refs.source.value=state.source || "stocks";
    refs.search.value = state.search || "";
    refs.universe.value = state.universe || "all";
    refs.marketCap.value = state.marketCap || "all";
    refs.signal.value = state.signal || "all";
    refs.statistic.value = state.statistic || "row";
    renderRules(state.quantileRules || []);
    refs.sort.value = state.sort || "ticker";
    refs.direction.value = state.direction || "asc";
    refs.signalChanged.checked = Boolean(state.signalChanged);
    const perps=state.source==="kalshi_perps";
    for(const control of [refs.universe,refs.marketCap,refs.signal,refs.signalChanged,refs.statistic,refs.addRule])control.disabled=perps;
    root.querySelectorAll('input[name="position"]').forEach((input) => {
      input.checked = state.positions.includes(input.value);
      input.disabled = perps;
    });
  }

  function parseUrl() {
    const params = new URLSearchParams(window.location.search);
    const allowed = {
      source: ["stocks","kalshi_perps"],
      universe: ["all", "sp500", "nasdaq", "etf"],
      marketCap: ["all", "mega", "large", "mid", "small", "micro"],
      signal: ["all", "buy", "sell", "neutral", "unavailable"],
      statistic: ["row", "min", "max", "avg"],
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
    state.signalChanged = params.get("signalChanged") === "true";
    try {
      const rules=JSON.parse(params.get("quantileRules") || "[]");
      if(Array.isArray(rules)) state.quantileRules=rules.slice(0,12).filter(rule => rule && ["p01","p10","p25","p50","p75","p90","p99"].includes(rule.quantile) && ["min","max","avg"].includes(rule.statistic) && ["gt","gte","lt","lte"].includes(rule.operator) && Number.isFinite(rule.percent));
    } catch { state.quantileRules=[]; }
    const validPositions = new Set(["below-p10", "above-p10", "below-p50", "above-p50", "below-p90", "above-p90"]);
    state.positions = String(params.get("position") || "").split(",").filter((value) => validPositions.has(value));
    const page = Number(params.get("page"));
    state.page = Number.isInteger(page) && page > 0 ? page : 1;
    return state;
  }

  function buildParams(state) {
    const params = new URLSearchParams();
    if(state.source!=="stocks")params.set("source",state.source);
    if (window.location.pathname === "/forecasting") params.set("panel", "screener");
    if (state.search) params.set("search", state.search);
    if (state.universe !== "all") params.set("universe", state.universe);
    if (state.marketCap !== "all") params.set("marketCap", state.marketCap);
    if (state.signal !== "all") params.set("signal", state.signal);
    if (state.statistic !== "row") params.set("statistic", state.statistic);
    if (state.quantileRules.length) params.set("quantileRules", JSON.stringify(state.quantileRules));
    if (state.sort !== "ticker") params.set("sort", state.sort);
    if (state.direction !== "asc") params.set("direction", state.direction);
    if (state.signalChanged) params.set("signalChanged", "true");
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
    const active = [state.search, state.universe !== "all", state.marketCap !== "all", state.signal !== "all", state.signalChanged]
      .filter(Boolean).length + state.positions.length + state.quantileRules.length;
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
    const signal = row.current_signal;
    if (!signal) return `<span class="qs-muted-cell">Unavailable</span><small>${escapeHtml(row.signal_status || "No comparable forecast row")}</small>`;
    const cls = signal.value === "buy" ? "qs-position-below" : signal.value === "sell" ? "qs-position-above" : "";
    return `<div class="qs-signal-stack"><span class="qs-badge ${cls}">${escapeHtml(signal.value)} · current</span><small>Forecast ${escapeHtml(formatDate(signal.forecast_date,false))}</small>${row.signal_comparison === "before_first_forecast_session" ? '<small>First forecast session not started</small>' : ""}</div>`;
  }

  function savedSignalView(row) {
    const last=row.last_non_neutral_signal;
    if(!last)return '<span class="qs-muted-cell">None saved yet</span>';
    const previous=row.previous_non_neutral_signal;
    return `<div class="qs-signal-stack"><span class="qs-badge">${escapeHtml(last.value)}</span><small>${escapeHtml(formatDate(last.forecast_date,false))} close</small>${previous?`<small>Prior: ${escapeHtml(previous.value)} · ${escapeHtml(formatDate(previous.forecast_date,false))}</small>`:""}</div>`;
  }

  function renderRules(rules) {
    const options=(values,selected)=>values.map(([value,label])=>`<option value="${value}"${value===selected?" selected":""}>${label}</option>`).join("");
    refs.rules.innerHTML=rules.map((rule,i)=>`<div class="qs-rule">
      <select data-rule="statistic" aria-label="Rule ${i+1} statistic">${options([["avg","Average"],["min","Minimum"],["max","Maximum"]],rule.statistic)}</select>
      <select data-rule="quantile" aria-label="Rule ${i+1} quantile">${options(["p01","p10","p25","p50","p75","p90","p99"].map(q=>[q,q.toUpperCase()]),rule.quantile)}</select>
      <select data-rule="operator" aria-label="Rule ${i+1} comparison">${options([["gt","> price by"],["gte","≥ price by"],["lt","< price by"],["lte","≤ price by"]],rule.operator)}</select>
      <label><input data-rule="percent" aria-label="Rule ${i+1} percent difference" type="number" min="-100000" max="100000" step="any" value="${escapeHtml(rule.percent)}"> %</label>
      <button type="button" class="cta secondary small" data-remove-rule="${i}" aria-label="Remove filter ${i+1}"><i class="iconoir-cancel" aria-hidden="true"></i></button>
    </div>`).join("");
    refs.addRule.disabled=rules.length>=12;
  }

  function rowHtml(row) {
    const memberships = Array.isArray(row.universe_memberships) ? row.universe_memberships : [];
    const position = positionView(row.quantile_position);
    const suppliedUrl = String(row.analysis_url || row.forecast_view_url || "");
    const analysisUrl = suppliedUrl.startsWith("/") && !suppliedUrl.startsWith("//")
      ? suppliedUrl
      : `/forecasting?ticker=${encodeURIComponent(String(row.ticker || ""))}`;
    const actualStamp = row.actual_price_timestamp ? ` title="Observed price ${escapeHtml(formatDate(row.actual_price_timestamp, true))}"` : "";
    return `<tr>
      <td data-label="Security"><div class="qs-security"><a href="${escapeHtml(analysisUrl)}" aria-label="Open ${escapeHtml(row.ticker)} forecast analysis">${escapeHtml(row.ticker)}</a><span class="qs-universe-tags">${memberships.map((item) => `<span>${escapeHtml(item)}</span>`).join("")}</span><span class="qs-company" title="${escapeHtml(row.company_name || "")}">${escapeHtml(row.company_name || "Company name unavailable")}</span>${row.forecast_view_url?.startsWith("/forecasting?")?`<a class="cta secondary small qs-view-forecast" href="${escapeHtml(row.forecast_view_url)}"><i class="iconoir-graph-up" aria-hidden="true"></i>${row.forecast_action === "create" ? "Forecast" : "View forecast"}</a>`:'<small>Weekly forecast not published</small>'}<button class="qs-row-toggle" type="button" data-row-toggle aria-expanded="false" aria-label="Show more metrics for ${escapeHtml(row.ticker)}"><span>More metrics</span><i class="iconoir-nav-arrow-down" aria-hidden="true"></i></button></div></td>
      <td data-label="Actual" class="qs-mono qs-mobile-core"${actualStamp}>${escapeHtml(formatPrice(row.actual_price))}<small>${escapeHtml(formatDate(row.actual_price_timestamp,true))}</small><small>${escapeHtml(String(row.quote_session || "historical").replace(/_/g," "))} · ${escapeHtml(String(row.quote_source || row.data_source || "historical").replace(/_/g," "))}</small></td>
      ${["p01","p10","p25","p50","p75","p90","p99"].map(q=>`<td data-label="${q.toUpperCase()}" class="qs-mono ${["p10","p50","p90"].includes(q)?"qs-mobile-core":"qs-mobile-detail"}" title="${escapeHtml(current.statistic === "row" ? "Comparison session" : `Horizon ${current.statistic}`)}">${escapeHtml(formatPrice(current.statistic === "row" ? row[q] : row.quantile_stats?.[q]?.[current.statistic]))}</td>`).join("")}
      <td data-label="Position" class="qs-mobile-detail"><span class="${position[1]}">${escapeHtml(position[0])}</span></td>
      <td data-label="Distance P10 / P50 / P90" class="qs-mobile-detail"><div class="qs-distance-stack">${distanceView(row.distance_p10_pct)}${distanceView(row.distance_p50_pct)}${distanceView(row.distance_p90_pct)}</div></td>
      <td data-label="Market cap" class="qs-mono qs-mobile-detail">${escapeHtml(formatCap(row.market_cap, row.is_etf))}</td>
      <td data-label="Current signal" class="qs-mobile-core">${signalView(row)}</td>
      <td data-label="Last saved buy/sell" class="qs-mobile-detail">${savedSignalView(row)}</td>
      <td data-label="Updated" class="qs-mobile-detail" title="Forecast horizon ends ${escapeHtml(formatDate(row.forecast_date, false))}">${escapeHtml(formatDate(row.last_forecast_update, true))}</td>
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
      refs.export.href = `/api/screener/export.csv?${buildParams(current)}`;
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
    const weekly=payload.schemaVersion === "quantura-screener-v3";
    const perps=payload.dataSource==="kalshi_perps";
    document.getElementById("qs-engine").textContent=perps ? "Forecast on demand" : weekly ? "Five-model weekly ensemble" : "Prior validated scan";
    document.getElementById("qs-engine-detail").textContent=perps ? "USD per underlying unit · reference spot scale" : weekly ? "1 session withheld · 7 NYSE sessions · Toto 4M" : "Weekly ensemble scan not published yet";
    if(perps)refs.metricProcessed.textContent=`${manifest.successfully_processed} spot references available · ${manifest.failed} unavailable`;
    refs.freshness.textContent = perps
      ? `Scan ${formatDate(payload.generatedAt, true)} · Kalshi reference prices normalized by contract exposure, with normalized completed trades as fallback. Quotes are not real-time ticks. ${(payload.warnings || []).join(" ")}`
      : `Scan ${formatDate(payload.generatedAt, true)} · latest completed minute close or historical fallback · quotes are not real-time ticks. ${(payload.warnings || []).join(" ")}`;
    refs.status.textContent = `${Number(payload.total || 0).toLocaleString()} of ${Number(payload.universeCount || 0).toLocaleString()} ${perps ? "markets" : "securities"} match the active research filters.`;
    current.page = Number(payload.page || current.page || 1);
    refs.pageLabel.textContent = `Page ${current.page.toLocaleString()} of ${Number(payload.pageCount || 1).toLocaleString()}`;
    refs.previous.disabled = current.page <= 1;
    refs.next.disabled = current.page >= Number(payload.pageCount || 1);
    refs.tableWrap.dataset.source = perps ? "kalshi_perps" : "stocks";
    refs.tableBody.innerHTML = items.map(rowHtml).join("");
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
  root.addEventListener("change", event => {
    if(event.target===refs.source){current={...defaults,source:refs.source.value,positions:[],quantileRules:[]};writeControls(current);}
    const perps=refs.source?.value==="kalshi_perps";
    for(const control of [refs.universe,refs.marketCap,refs.signal,refs.signalChanged,refs.statistic,refs.addRule])control.disabled=perps;
    root.querySelectorAll('input[name="position"]').forEach(control=>{control.disabled=perps;});
    load({resetPage:true});
  });
  refs.addRule.addEventListener("click",()=>{
    current=readControls();if(current.quantileRules.length>=12)return;
    current.quantileRules=[...current.quantileRules,{quantile:"p50",statistic:"avg",operator:"gt",percent:10}];writeControls(current);load({resetPage:true});
  });
  refs.rules.addEventListener("click",event=>{
    const button=event.target.closest("[data-remove-rule]");if(!button)return;
    current=readControls();current.quantileRules.splice(Number(button.dataset.removeRule),1);writeControls(current);load({resetPage:true});
  });
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
  refs.tableBody.addEventListener("click", event=>{
    const button=event.target.closest("[data-row-toggle]");if(!button)return;
    const row=button.closest("tr"),expanded=!row.classList.contains("is-expanded");
    row.classList.toggle("is-expanded",expanded);button.setAttribute("aria-expanded",String(expanded));
    button.querySelector("span").textContent=expanded?"Fewer metrics":"More metrics";
    button.querySelector("i")?.classList.toggle("iconoir-nav-arrow-up",expanded);
    button.querySelector("i")?.classList.toggle("iconoir-nav-arrow-down",!expanded);
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

  const alertStatus=document.getElementById("qs-alert-status");
  const alertSummary=document.getElementById("qs-alert-summary");
  const savedAlertPanel=document.getElementById("saved-alerts");
  const accountRequest=(path,options)=>{
    if(!window.QuanturaScreenerAccount)throw new Error("Account tools are still loading. Try again shortly.");
    return window.QuanturaScreenerAccount.request(path,options);
  };
  const evaluationStatus=(meta)=>{
    const evaluation=meta?.last_evaluation;
    if(!evaluation?.date)return "Active filters are evaluated after the next finalized stock-market close.";
    const day=formatDate(evaluation.date,false);
    const matches=Number(evaluation.matched_rows||0);
    const email=String(evaluation.email_status||"");
    const delivery=email==="accepted"?" Daily email accepted by the provider.":email.startsWith("email_")||email==="not_configured"?" Inbox delivery completed; email delivery needs attention.":"";
    return matches?`${matches.toLocaleString()} matching securities on ${day}.${delivery}`:`No matches on ${day}; the filters remain active.`;
  };
  const renderSavedAlerts=()=>{
    document.getElementById("qs-saved-list").innerHTML=savedAlerts.length?savedAlerts.map(a=>`<li><i class="iconoir-bell-notification qs-alert-icon" aria-hidden="true"></i><span><strong>${escapeHtml(a.name)}</strong><small>Active · ${a.email?"Inbox + email":"Inbox only"}</small></span><button type="button" class="cta secondary" data-apply-alert="${escapeHtml(a.id)}"><i class="iconoir-filter-list" aria-hidden="true"></i>Apply</button><button type="button" class="cta secondary" data-remove-alert="${escapeHtml(a.id)}" aria-label="Remove ${escapeHtml(a.name)} and stop its alerts"><i class="iconoir-trash" aria-hidden="true"></i>Remove</button></li>`).join(""):"<li><i class=\"iconoir-bell-off qs-alert-icon\" aria-hidden=\"true\"></i><span><strong>No saved filters yet.</strong><small>Configure the screener, then save the current filters.</small></span></li>";
    if(alertSummary)alertSummary.textContent=`${savedAlerts.length} of ${Number(savedAlertsMeta.maximum||10)} active`;
  };
  async function loadSavedAlerts(){
    const payload=await accountRequest("/api/v1/me/screener-alerts");savedAlerts=payload.data||[];savedAlertsMeta=payload.meta||{};renderSavedAlerts();
  }
  async function loadAlertInbox(){
    const inbox=await accountRequest("/api/notifications/items?category=screener&limit=20");
    document.getElementById("qs-alert-inbox").innerHTML=inbox.items?.length?inbox.items.map(item=>`<li><i class="iconoir-inbox qs-alert-icon" aria-hidden="true"></i><span><strong>${escapeHtml(item.title)}</strong><small>${escapeHtml(item.body)}</small><small>${escapeHtml(new Date(item.createdAtMs).toLocaleString())}</small></span></li>`).join(""):`<li><i class="iconoir-inbox qs-alert-icon" aria-hidden="true"></i><span><strong>No matching notifications yet.</strong><small>${escapeHtml(evaluationStatus(savedAlertsMeta))}</small></span></li>`;
  }
  async function refreshAlertPanel({announce=true}={}){
    if(alertPanelLoading)return alertPanelLoading;
    alertPanelLoading=(async()=>{await loadSavedAlerts();await loadAlertInbox();if(announce)alertStatus.textContent=`Saved filters and inbox updated. ${evaluationStatus(savedAlertsMeta)}`;})().finally(()=>{alertPanelLoading=null;});
    return alertPanelLoading;
  }
  document.getElementById("qs-save-alert").addEventListener("click",async event=>{
    const button=event.currentTarget;button.disabled=true;alertStatus.textContent="Saving…";
    try{const state=readControls();
      if(state.source==="kalshi_perps")throw new Error("Daily closing-signal alerts apply to the stock scan. Perpetual forecasts are currently on demand.");
      // Saved responses also carry legacy normalized query fields. Do not send
      // them back as new editable filters when applying and saving a preset.
      const filters=Object.fromEntries(["search","universe","marketCap","minMarketCap","maxMarketCap","signal","signalChanged","quantileRules","positions","sort","direction","statistic"].filter(key=>state[key]!==undefined).map(key=>[key,state[key]]));
      await accountRequest("/api/v1/me/screener-alerts",{method:"POST",body:{name:document.getElementById("qs-alert-name").value,filters,email:document.getElementById("qs-alert-email").checked}});
      await refreshAlertPanel({announce:false});alertStatus.textContent="Saved. Matching finalized closing results will appear once per trading day.";
    }catch(error){alertStatus.textContent=error.message;}finally{button.disabled=false;}
  });
  document.getElementById("qs-load-alerts").addEventListener("click",async event=>{
    const button=event.currentTarget;button.disabled=true;alertStatus.textContent="Loading…";
    try{await refreshAlertPanel();
    }catch(error){alertStatus.textContent=error.message;}finally{button.disabled=false;}
  });
  document.getElementById("qs-saved-list").addEventListener("click",async event=>{
    const apply=event.target.closest("[data-apply-alert]");const remove=event.target.closest("[data-remove-alert]");
    if(apply){const a=savedAlerts.find(s=>s.id===apply.dataset.applyAlert);if(a){current={...defaults,...a.filters,positions:[...a.filters.positions],page:1};writeControls(current);load();alertStatus.textContent=`Applied ${a.name}.`;}}
    if(remove){remove.disabled=true;try{await accountRequest(`/api/v1/me/screener-alerts/${remove.dataset.removeAlert}`,{method:"DELETE"});await loadSavedAlerts();alertStatus.textContent="Removed. This filter will no longer send notifications.";}catch(error){alertStatus.textContent=error.message;remove.disabled=false;}}
  });
  const openSavedAlerts=()=>{savedAlertPanel.open=true;refreshAlertPanel({announce:false}).catch(error=>{alertStatus.textContent=error.message;if(alertSummary)alertSummary.textContent="Sign in to load";});};
  if(window.location.hash==="#saved-alerts")openSavedAlerts();
  window.addEventListener("hashchange",()=>{if(window.location.hash==="#saved-alerts")openSavedAlerts();});
  savedAlertPanel.addEventListener("toggle",()=>{if(savedAlertPanel.open&&!savedAlerts.length)refreshAlertPanel({announce:false}).catch(error=>{alertStatus.textContent=error.message;if(alertSummary)alertSummary.textContent="Sign in to load";});});
  window.addEventListener("load",()=>{
    const auth=window.firebase?.auth?.();
    auth?.onAuthStateChanged(user=>{if(user&&!user.isAnonymous)window.setTimeout(()=>refreshAlertPanel({announce:false}).catch(()=>{}),150);else if(alertSummary)alertSummary.textContent="Sign in to activate";});
  },{once:true});
})();
