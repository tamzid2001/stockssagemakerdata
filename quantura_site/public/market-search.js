(() => {
  "use strict";

  const form = document.getElementById("market-search-form");
  const queryInput = document.getElementById("market-search-query");
  const sourceInput = document.getElementById("market-search-source");
  const status = document.getElementById("market-search-status");
  const results = document.getElementById("market-search-results");
  if (!form || !queryInput || !sourceInput || !status || !results) return;
  let timer;
  let controller;
  let requestSequence = 0;
  let mode = "open";
  const resources = new Map();
  const cache = new Map();
  queryInput.setAttribute("aria-controls", "market-search-results");
  queryInput.setAttribute("aria-describedby", "market-search-status");
  queryInput.maxLength = 2048;

  const escapeHtml = (value) => String(value ?? "").replace(/[&<>'"]/g, (character) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
  })[character]);
  const titleCase = (value) => String(value || "").replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
  const providerLabel = (value) => ({ alpaca: "Alpaca", yahoo: "Yahoo Finance", polymarket_us: "Polymarket US", kalshi: "Kalshi" })[value] || value;

  function setPanel(panel) {
    if (typeof window.__quanturaSetPanel === "function") window.__quanturaSetPanel(panel);
    else window.location.href = `/forecasting?panel=${encodeURIComponent(panel)}`;
  }

  function render(groups, errors) {
    resources.clear();
    const sections = ["alpaca", "yahoo", "polymarket_us", "kalshi"].flatMap((source) => {
      const rows = Array.isArray(groups?.[source]) ? groups[source] : [];
      if (!rows.length && !errors?.[source]) return [];
      const cards = rows.length
        ? rows.map((row) => {
            const prediction = row.resource_type === "prediction_market_contract";
            resources.set(row.resource_id, row);
            return `<article class="market-search-result" data-market-resource="${escapeHtml(row.resource_id)}">
              <div class="market-search-result-main">
                <div class="market-search-result-symbol">${escapeHtml(prediction ? row.outcome || row.side : row.symbol || "Market")}</div>
                <div><strong title="${escapeHtml(row.name || row.symbol || "Supported market")}">${escapeHtml(row.name || row.symbol || "Supported market")}</strong><div class="small muted">${escapeHtml(prediction ? [row.contract?.sport, row.contract?.league].filter(Boolean).join(" · ") || "Prediction market" : titleCase(row.asset_class))} · ${escapeHtml(providerLabel(row.source))}${row.exchange && row.exchange !== providerLabel(row.source) ? ` · ${escapeHtml(row.exchange)}` : ""}${row.currency ? ` · ${escapeHtml(row.currency)}` : ""}${row.status ? ` · ${escapeHtml(titleCase(row.status))}` : ""}</div>${row.unit ? `<div class="small muted">${escapeHtml(row.unit)}</div>` : ""}</div>
              </div>
              <div class="hero-actions market-search-result-actions">
                ${prediction
                  ? `<span class="market-timing small">${escapeHtml(row.timing === "live" ? "LIVE" : row.timing === "in_progress" ? "Started · open" : titleCase(row.timing || row.status))}</span>${!["closed", "settled"].includes(row.status) ? '<button class="cta small" type="button" data-market-action="prediction-forecast">Select for forecast</button>' : ""}<button class="cta secondary small" type="button" data-market-action="prediction-download">Download history</button>`
                  : `${row.forecast_available ? `<button class="cta small" type="button" data-market-action="forecast" data-symbol="${escapeHtml(row.symbol)}" data-source="${escapeHtml(row.source)}" data-asset-class="${escapeHtml(row.asset_class)}">Forecast</button>` : ""}<button class="cta secondary small" type="button" data-market-action="history" data-symbol="${escapeHtml(row.symbol)}" data-source="${escapeHtml(row.source)}">Historical data</button>`}
              </div>
            </article>`;
          }).join("")
        : `<div class="notice small">${escapeHtml(providerLabel(source))} is ${escapeHtml(String(errors[source]).replaceAll("_", " "))}.</div>`;
      return [`<section class="market-search-group"><h3>${escapeHtml(providerLabel(source))}</h3>${cards}</section>`];
    });
    results.hidden = false;
    results.innerHTML = sections.length ? sections.join("") : '<div class="empty-state">No supported market matched this search.</div>';
  }

  async function search() {
    clearTimeout(timer);
    controller?.abort();
    const sequence = ++requestSequence;
    const query = String(queryInput.value || "").trim();
    if (query.length < 2 && mode !== "live") {
      results.hidden = true;
      status.textContent = "Enter at least two characters to search markets.";
      results.removeAttribute("aria-busy");
      return;
    }
    controller = new AbortController();
    status.textContent = "Searching configured providers…";
    results.setAttribute("aria-busy", "true");
    try {
      const link = /^https?:\/\//i.test(query);
      const params = new URLSearchParams(link ? { url: query } : { q: query, source: String(sourceInput.value || "auto"), limit: "20", mode });
      const endpoint = link ? "/api/market-search/resolve" : "/api/market-search";
      const key = `${endpoint}?${params.toString()}`;
      const cached = cache.get(key);
      let payload = cached && Date.now() - cached.time < 60000 ? cached.payload : null;
      if (!payload) {
        const response = await fetch(key, { headers: { Accept: "application/json" }, signal: controller.signal });
        payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.message || "Market search is temporarily unavailable. Try again.");
        cache.set(key, { time: Date.now(), payload });
        if (cache.size > 20) cache.delete(cache.keys().next().value);
      }
      if (sequence !== requestSequence) return;
      render(payload.groups || {}, payload.errors || {});
      status.textContent = `${Number(payload.count || 0).toLocaleString()} results · Choose a team/side. ${payload.coverage || "↓ to explore, Escape to close."}`;
    } catch (error) {
      if (error.name === "AbortError" || sequence !== requestSequence) return;
      results.hidden = false;
      results.innerHTML = '<div class="empty-state">Search is temporarily unavailable. Try a provider-specific workflow.</div>';
      status.textContent = error?.message || "Market search failed.";
    } finally {
      if (sequence === requestSequence) results.removeAttribute("aria-busy");
    }
  }

  form.addEventListener("submit", (event) => { event.preventDefault(); void search(); });
  queryInput.addEventListener("input", () => {
    controller?.abort(); ++requestSequence; clearTimeout(timer);
    timer = setTimeout(search, 300);
  });
  sourceInput.addEventListener("change", () => void search());
  document.querySelectorAll("[data-market-mode]").forEach(button => button.addEventListener("click", () => {
    mode = button.dataset.marketMode;
    document.querySelectorAll("[data-market-mode]").forEach(item => item.setAttribute("aria-pressed", String(item === button)));
    queryInput.required = mode !== "live";
    if (mode === "live" && ["alpaca", "yahoo"].includes(sourceInput.value)) sourceInput.value = "auto";
    void search();
  }));
  queryInput.addEventListener("keydown", event => {
    if (event.key === "ArrowDown" && !results.hidden) {
      const first = results.querySelector("[data-market-action]");
      if (first) { event.preventDefault(); first.focus(); }
    }
    if (event.key === "Escape") { results.hidden = true; controller?.abort(); ++requestSequence; clearTimeout(timer); results.removeAttribute("aria-busy"); }
  });
  results.addEventListener("keydown", event => {
    if (event.key === "Escape") { results.hidden = true; queryInput.focus(); return; }
    if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) return;
    const buttons = [...results.querySelectorAll("[data-market-action]")];
    const index = buttons.indexOf(document.activeElement);
    if (index < 0) return;
    event.preventDefault();
    if (event.key === "ArrowUp" && index === 0) { queryInput.focus(); return; }
    const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1 : Math.max(0, Math.min(buttons.length - 1, index + (event.key === "ArrowDown" ? 1 : -1)));
    buttons[next]?.focus();
  });

  results.addEventListener("click", (event) => {
    const button = event.target.closest("[data-market-action]");
    if (!button) return;
    const action = button.dataset.marketAction;
    if (action === "prediction-forecast" || action === "prediction-download") {
      const row = resources.get(button.closest("[data-market-resource]")?.dataset.marketResource);
      if (!row?.contract) return;
      const download = action === "prediction-download";
      if (!download) window.QuanturaMarketSelection = row;
      setPanel(download ? "sports-autopilot" : "forecast");
      window.dispatchEvent(new CustomEvent("quantura:market-selected", { detail: { resource: row, intent: download ? "download" : "forecast" } }));
      status.textContent = `Selected ${row.outcome} · ${row.contract.eventTitle || row.name} · ${providerLabel(row.source)}.`;
      results.hidden = true;
      document.getElementById(download ? "prediction-market-hub" : "ensemble-forecast-form")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    const symbol = String(button.dataset.symbol || "").trim();
    const source = String(button.dataset.source || "auto").trim();
    if (action === "forecast") {
      window.QuanturaMarketSelection = null;
      const ticker = document.getElementById("ensemble-ticker") || document.getElementById("forecast-ticker");
      const forecastSource = document.getElementById("ensemble-provider") || document.getElementById("forecast-source");
      const assetClass = document.getElementById("forecast-asset-class");
      if (ticker) ticker.value = symbol;
      if (forecastSource) forecastSource.value = source === "alpaca" ? "alpaca" : "yahoo";
      if (assetClass) assetClass.value = button.dataset.assetClass || "equity";
      const sourceType = document.getElementById("ensemble-source-type");
      if (sourceType) { sourceType.value = "ticker"; sourceType.dispatchEvent(new Event("change", { bubbles: true })); }
      setPanel("forecast");
      (document.getElementById("ensemble-forecast-form") || document.getElementById("forecast-form"))?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    if (action === "history") {
      const historySymbol = document.getElementById("alpaca-symbol");
      const historySource = document.getElementById("market-history-source");
      if (historySymbol) historySymbol.value = symbol;
      if (historySource) historySource.value = source === "alpaca" ? "alpaca" : "yahoo";
      setPanel("news");
      document.getElementById("alpaca-history-form")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    if (action === "prediction") {
      const provider = source === "kalshi" ? "kalshi" : "polymarket_us";
      const radio = document.querySelector(`input[name="pm-source"][value="${provider}"]`);
      if (radio) {
        radio.checked = true;
        radio.dispatchEvent(new Event("change", { bubbles: true }));
      }
      const search = document.getElementById("pm-search");
      if (search) search.value = button.dataset.query || "";
      setPanel("sports-autopilot");
      document.getElementById("prediction-market-hub")?.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });
})();
