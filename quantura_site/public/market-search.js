(() => {
  "use strict";

  const form = document.getElementById("market-search-form");
  const queryInput = document.getElementById("market-search-query");
  const sourceInput = document.getElementById("market-search-source");
  const status = document.getElementById("market-search-status");
  const results = document.getElementById("market-search-results");
  if (!form || !queryInput || !sourceInput || !status || !results) return;

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
    const sections = ["alpaca", "yahoo", "polymarket_us", "kalshi"].flatMap((source) => {
      const rows = Array.isArray(groups?.[source]) ? groups[source] : [];
      if (!rows.length && !errors?.[source]) return [];
      const cards = rows.length
        ? rows.map((row) => {
            const prediction = row.resource_type === "prediction_market_contract";
            return `<article class="market-search-result" data-market-resource="${escapeHtml(row.resource_id)}">
              <div class="market-search-result-main">
                <div class="market-search-result-symbol">${escapeHtml(row.symbol || row.contract_id || "Market")}</div>
                <div><strong>${escapeHtml(row.name || row.symbol || "Supported market")}</strong><div class="small muted">${escapeHtml(titleCase(row.asset_class))} · ${escapeHtml(providerLabel(row.source))}${row.exchange ? ` · ${escapeHtml(row.exchange)}` : ""}${row.currency ? ` · ${escapeHtml(row.currency)}` : ""}</div>${row.unit ? `<div class="small muted">Unit: ${escapeHtml(row.unit)}</div>` : ""}</div>
              </div>
              <div class="hero-actions market-search-result-actions">
                ${prediction
                  ? `<button class="cta secondary small" type="button" data-market-action="prediction" data-source="${escapeHtml(row.source)}" data-query="${escapeHtml(row.symbol || row.name)}">Open market data</button>`
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

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const query = String(queryInput.value || "").trim();
    if (query.length < 2) return;
    status.textContent = "Searching configured providers…";
    results.hidden = true;
    try {
      const params = new URLSearchParams({ q: query, source: String(sourceInput.value || "auto"), limit: "8" });
      const response = await fetch(`/api/market-search?${params.toString()}`, { headers: { Accept: "application/json" } });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload?.message || "Market search failed.");
      render(payload.groups || {}, payload.errors || {});
      status.textContent = `${Number(payload.count || 0).toLocaleString()} supported result${Number(payload.count || 0) === 1 ? "" : "s"}. Availability reflects the configured provider response.`;
    } catch (error) {
      results.hidden = false;
      results.innerHTML = '<div class="empty-state">Search is temporarily unavailable. Try a provider-specific workflow.</div>';
      status.textContent = error?.message || "Market search failed.";
    }
  });

  results.addEventListener("click", (event) => {
    const button = event.target.closest("[data-market-action]");
    if (!button) return;
    const action = button.dataset.marketAction;
    const symbol = String(button.dataset.symbol || "").trim();
    const source = String(button.dataset.source || "auto").trim();
    if (action === "forecast") {
      const ticker = document.getElementById("forecast-ticker");
      const forecastSource = document.getElementById("forecast-source");
      const assetClass = document.getElementById("forecast-asset-class");
      if (ticker) ticker.value = symbol;
      if (forecastSource) forecastSource.value = source === "alpaca" ? "alpaca" : "yahoo";
      if (assetClass) assetClass.value = button.dataset.assetClass || "equity";
      setPanel("forecast");
      document.getElementById("forecast-form")?.scrollIntoView({ behavior: "smooth", block: "start" });
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
