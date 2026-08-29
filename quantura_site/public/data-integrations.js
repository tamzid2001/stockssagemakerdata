(() => {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const html = (value) => String(value ?? "").replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
  const number = (value, digits = 4) => Number.isFinite(Number(value)) ? Number(value).toLocaleString(undefined, { maximumFractionDigits: digits }) : "—";
  const isoDate = (date) => date.toISOString().slice(0, 10);
  const status = (element, message, tone = "") => {
    if (!element) return;
    element.textContent = message;
    element.dataset.tone = tone;
  };
  const disable = (button, loading, label) => {
    if (!button) return;
    if (!button.dataset.defaultLabel) button.dataset.defaultLabel = button.textContent.trim();
    button.disabled = loading;
    button.setAttribute("aria-busy", String(loading));
    button.textContent = loading ? label : button.dataset.defaultLabel;
  };
  async function jsonRequest(url, options = {}) {
    let response;
    try {
      response = await fetch(url, { ...options, headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}), ...(options.headers || {}) } });
    } catch (_error) {
      throw new Error("The backend could not be reached. Check your connection and try again.");
    }
    const contentType = response.headers.get("content-type") || "";
    const payload = contentType.includes("application/json") ? await response.json() : {};
    if (!response.ok) throw new Error(payload.message || payload.detail || String(payload.error || "").replaceAll("_", " ") || "The request could not be completed.");
    return payload;
  }
  async function downloadCsv(url, body, fallbackName) {
    let response;
    try {
      response = await fetch(url, { method: "POST", headers: { Accept: "text/csv", "Content-Type": "application/json" }, body: JSON.stringify({ ...body, format: "csv" }) });
    } catch (_error) {
      throw new Error("The download service could not be reached.");
    }
    const contentType = response.headers.get("content-type") || "";
    if (!response.ok || !contentType.includes("text/csv")) {
      const payload = contentType.includes("application/json") ? await response.json() : {};
      throw new Error(payload.message || "No valid CSV file was generated.");
    }
    const blob = await response.blob();
    if (!blob.size) throw new Error("The generated CSV was empty.");
    const disposition = response.headers.get("content-disposition") || "";
    const match = disposition.match(/filename="?([^";]+)"?/i);
    const anchor = document.createElement("a");
    anchor.href = URL.createObjectURL(blob);
    anchor.download = match?.[1] || fallbackName;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(anchor.href), 1000);
  }
  async function downloadFile(url, body, format, fallbackName) {
    const expectedType = format === "json" ? "application/json" : "text/csv";
    let response;
    try {
      response = await fetch(url, { method: "POST", headers: { Accept: expectedType, "Content-Type": "application/json" }, body: JSON.stringify({ ...body, format }) });
    } catch (_error) {
      throw new Error("The download service could not be reached.");
    }
    const contentType = response.headers.get("content-type") || "";
    if (!response.ok || !contentType.includes(expectedType)) {
      const payload = contentType.includes("application/json") ? await response.json().catch(() => ({})) : {};
      throw new Error(payload.message || `No valid ${format.toUpperCase()} file was generated.`);
    }
    const blob = await response.blob();
    if (!blob.size) throw new Error(`The generated ${format.toUpperCase()} file was empty.`);
    const disposition = response.headers.get("content-disposition") || "";
    const match = disposition.match(/filename="?([^";]+)"?/i);
    const anchor = document.createElement("a");
    anchor.href = URL.createObjectURL(blob);
    anchor.download = match?.[1] || fallbackName;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(anchor.href), 1000);
  }
  function table(container, columns, rows, limit = 100) {
    if (!container) return;
    if (!rows.length) {
      container.innerHTML = '<div class="empty-state">No observations were returned.</div>';
      return;
    }
    const displayed = rows.slice(0, limit);
    container.innerHTML = `<table class="data-table"><thead><tr>${columns.map((column) => `<th scope="col">${html(column.label)}</th>`).join("")}</tr></thead><tbody>${displayed.map((row) => `<tr>${columns.map((column) => `<td>${html(column.render ? column.render(row) : row[column.key])}</td>`).join("")}</tr>`).join("")}</tbody></table>${rows.length > displayed.length ? `<p class="table-note">Showing ${displayed.length.toLocaleString()} of ${rows.length.toLocaleString()} loaded rows. The download contains the full loaded selection.</p>` : ""}`;
  }
  function plot(element, traces, layout = {}) {
    if (!element || !window.Plotly) return;
    window.Plotly.react(element, traces, {
      paper_bgcolor: "transparent", plot_bgcolor: "transparent", font: { color: "#cbd5e1", family: "Inter, sans-serif", size: 12 },
      margin: { l: 56, r: 24, t: 24, b: 48 }, xaxis: { gridcolor: "rgba(148,163,184,.16)", rangeslider: { visible: false } },
      yaxis: { gridcolor: "rgba(148,163,184,.16)" }, hovermode: "x unified", showlegend: false, ...layout,
    }, { responsive: true, displaylogo: false });
  }

  function stockHistoryBody() {
    return {
      source: byId("market-history-source")?.value || "auto",
      symbol: byId("alpaca-symbol")?.value.trim().toUpperCase(), timeframe: byId("alpaca-timeframe")?.value,
      end: byId("alpaca-end")?.value, session: byId("alpaca-session")?.value,
      adjustment: byId("alpaca-adjustment")?.value, feed: byId("alpaca-feed")?.value, limit: byId("alpaca-row-limit")?.value,
    };
  }
  function initHistoricalData() {
    const form = byId("alpaca-history-form");
    if (!form) return;
    const end = new Date();
    byId("alpaca-end").value = isoDate(end);
    const historyStatus = byId("alpaca-history-status");
    const loadButton = form.querySelector('button[type="submit"]');
    const downloadButton = byId("alpaca-download");
    const sourceSelect = byId("market-history-source");
    const feedSelect = byId("alpaca-feed");
    const syncSourceControls = () => {
      const yahooOnly = sourceSelect?.value === "yahoo";
      if (feedSelect) {
        feedSelect.disabled = yahooOnly;
        feedSelect.title = yahooOnly ? "Yahoo Finance selects its own public chart feed." : "Choose an Alpaca market-data feed.";
      }
    };
    sourceSelect?.addEventListener("change", syncSourceControls);
    syncSourceControls();
    jsonRequest("/api/market-data/history/status")
      .then((payload) => {
        const alpaca = payload?.sources?.alpaca?.available ? "Alpaca connected" : "Alpaca unavailable";
        const yahoo = payload?.sources?.yahoo?.available ? "Yahoo Finance available" : "Yahoo Finance unavailable";
        status(historyStatus, `${alpaca} · ${yahoo}. Automatic mode uses the first available source.`, payload?.sources?.yahoo?.available ? "success" : "warning");
      })
      .catch((error) => status(historyStatus, error.message, "error"));
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const body = stockHistoryBody();
      disable(loadButton, true, "Loading data…");
      downloadButton.disabled = true;
      status(historyStatus, "Requesting chronological market bars…");
      try {
        const payload = await jsonRequest("/api/market-data/stocks/history", { method: "POST", body: JSON.stringify(body) });
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        table(byId("alpaca-history-preview"), [
          { key: "timestamp", label: "Timestamp" }, { key: "open", label: "Open", render: (row) => number(row.open) },
          { key: "high", label: "High", render: (row) => number(row.high) }, { key: "low", label: "Low", render: (row) => number(row.low) },
          { key: "close", label: "Close", render: (row) => number(row.close) }, { key: "volume", label: "Volume", render: (row) => number(row.volume, 0) },
          { key: "tradeCount", label: "Trades", render: (row) => number(row.tradeCount, 0) }, { key: "vwap", label: "VWAP", render: (row) => number(row.vwap) },
          { key: "session", label: "Session", render: (row) => String(row.session || "").replaceAll("_", " ") },
        ], rows);
        const counts = rows.reduce((result, row) => ({ ...result, [row.session]: (result[row.session] || 0) + 1 }), {});
        byId("alpaca-session-summary").innerHTML = Object.entries(counts).map(([key, value]) => `<div><span>${html(key.replaceAll("_", " "))}</span><strong>${Number(value).toLocaleString()}</strong></div>`).join("") || '<p class="small muted">No session observations.</p>';
        const provider = String(payload.provider || payload.source || "market data");
        const feed = payload.feed ? ` · ${String(payload.feed).toUpperCase()}` : "";
        const fallback = payload.fallbackUsed ? " · automatic fallback used" : "";
        byId("alpaca-preview-summary").textContent = `${Number(payload.count || rows.length).toLocaleString()} ${payload.timeframe} observations · ${provider}${feed} · ${payload.adjustment || "raw"}${fallback} · oldest to newest`;
        status(historyStatus, `Loaded ${Number(payload.count || rows.length).toLocaleString()} observations from ${provider}.`, "success");
        downloadButton.disabled = false;
      } catch (error) {
        byId("alpaca-history-preview").innerHTML = `<div class="error-state">${html(error.message)}</div>`;
        status(historyStatus, error.message, "error");
      } finally { disable(loadButton, false); }
    });
    downloadButton.addEventListener("click", async () => {
      const body = stockHistoryBody();
      disable(downloadButton, true, "Generating CSV…");
      try {
        await downloadCsv("/api/market-data/stocks/history", body, `${body.symbol}-${body.timeframe}.csv`);
        status(historyStatus, "CSV generated and downloaded successfully.", "success");
      } catch (error) { status(historyStatus, error.message, "error"); }
      finally { disable(downloadButton, false); }
    });
  }

  function optionHistoryBody() {
    return { source: byId("options-market-source")?.value || "auto", contractSymbol: byId("alpaca-option-contract")?.value, start: byId("alpaca-option-start")?.value, end: byId("alpaca-option-end")?.value, timeframe: byId("alpaca-option-timeframe")?.value, feed: byId("alpaca-options-feed")?.value, limit: byId("alpaca-option-row-limit")?.value };
  }
  function initOptions() {
    const form = byId("alpaca-options-form");
    if (!form) return;
    const underlying = byId("alpaca-options-underlying");
    const expiration = byId("alpaca-options-expiration");
    const statusNode = byId("alpaca-options-status");
    const expirationButton = byId("alpaca-options-expirations");
    const loadButton = form.querySelector('button[type="submit"]');
    const historyForm = byId("alpaca-option-history-form");
    const historyLoad = byId("alpaca-option-load-history");
    const historyDownload = byId("alpaca-option-download");
    const historyStatus = byId("alpaca-option-history-status");
    const sourceSelect = byId("options-market-source");
    const feedSelect = byId("alpaca-options-feed");
    const end = new Date();
    byId("alpaca-option-end").value = isoDate(end);
    byId("alpaca-option-start").value = isoDate(new Date(end.getTime() - 7 * 86400000));
    const syncOptionSource = () => {
      const yahooOnly = sourceSelect?.value === "yahoo";
      if (feedSelect) {
        feedSelect.disabled = yahooOnly;
        feedSelect.title = yahooOnly ? "Yahoo Finance selects its own public options feed." : "Choose an Alpaca options feed.";
      }
      expiration.innerHTML = '<option value="">Load expirations first</option>';
      byId("alpaca-option-contract").value = "";
      historyLoad.disabled = true;
      historyDownload.disabled = true;
    };
    sourceSelect?.addEventListener("change", syncOptionSource);
    syncOptionSource();
    async function loadExpirations() {
      const symbol = underlying.value.trim().toUpperCase();
      if (!/^[A-Z][A-Z0-9.\-]{0,14}$/.test(symbol)) { status(statusNode, "Enter a valid underlying ticker.", "error"); return; }
      disable(expirationButton, true, "Loading expirations…");
      try {
        const payload = await jsonRequest(`/api/market-data/options/expirations?underlying=${encodeURIComponent(symbol)}&source=${encodeURIComponent(sourceSelect?.value || "auto")}`);
        expiration.innerHTML = payload.expirations.map((date) => `<option value="${html(date)}">${html(date)}</option>`).join("");
        status(statusNode, `Loaded ${payload.expirations.length} active expirations from ${payload.provider || "market data"}${payload.fallbackUsed ? " using automatic fallback" : ""}. Choose one and load the chain.`, "success");
      } catch (error) { expiration.innerHTML = '<option value="">No expirations available</option>'; status(statusNode, error.message, "error"); }
      finally { disable(expirationButton, false); }
    }
    expirationButton.addEventListener("click", loadExpirations);
    underlying.addEventListener("change", () => { expiration.innerHTML = '<option value="">Load expirations first</option>'; });
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      disable(loadButton, true, "Loading chain…");
      status(statusNode, "Loading contract metadata and Alpaca snapshots…");
      try {
        const query = new URLSearchParams({ underlying: underlying.value.trim().toUpperCase(), expiration: expiration.value, type: byId("alpaca-options-type").value, feed: byId("alpaca-options-feed").value, source: sourceSelect?.value || "auto" });
        const payload = await jsonRequest(`/api/market-data/options/chain?${query}`);
        const rows = Array.isArray(payload.contracts) ? payload.contracts : [];
        table(byId("alpaca-options-chain"), [
          { key: "select", label: "Select", render: (row) => `SELECT::${row.symbol}` }, { key: "symbol", label: "Contract" },
          { key: "type", label: "Type" }, { key: "strike", label: "Strike", render: (row) => number(row.strike, 2) },
          { key: "bid", label: "Bid", render: (row) => number(row.bid) }, { key: "ask", label: "Ask", render: (row) => number(row.ask) },
          { key: "last", label: "Last", render: (row) => number(row.last) }, { key: "volume", label: "Volume", render: (row) => number(row.volume, 0) },
          { key: "openInterest", label: "Open interest", render: (row) => number(row.openInterest, 0) }, { key: "impliedVolatility", label: "IV", render: (row) => number(row.impliedVolatility) },
          { key: "delta", label: "Delta", render: (row) => number(row.delta) }, { key: "gamma", label: "Gamma", render: (row) => number(row.gamma) },
          { key: "theta", label: "Theta", render: (row) => number(row.theta) }, { key: "vega", label: "Vega", render: (row) => number(row.vega) },
        ], rows, 250);
        byId("alpaca-options-chain").querySelectorAll("td:first-child").forEach((cell) => {
          const marker = cell.textContent || "";
          if (!marker.startsWith("SELECT::")) return;
          const symbol = marker.slice(8);
          const contract = rows.find((row) => row.symbol === symbol);
          cell.innerHTML = `<button class="task-chip" type="button" data-contract="${html(symbol)}">Select</button>`;
          cell.querySelector("button").addEventListener("click", () => {
            byId("alpaca-option-contract").value = symbol;
            byId("alpaca-selected-contract").innerHTML = `<strong>${html(symbol)}</strong><span>${html(contract.type)} · $${html(number(contract.strike, 2))} · expires ${html(contract.expiration)}</span>`;
            historyLoad.disabled = false;
            historyDownload.disabled = true;
            status(historyStatus, "Contract selected. Choose a range and load history.");
          });
        });
        status(statusNode, `Loaded ${rows.length.toLocaleString()} ${byId("alpaca-options-type").value} contracts from ${payload.provider || "market data"}${payload.fallbackUsed ? " using automatic fallback" : ""}.`, "success");
      } catch (error) { byId("alpaca-options-chain").innerHTML = `<div class="error-state">${html(error.message)}</div>`; status(statusNode, error.message, "error"); }
      finally { disable(loadButton, false); }
    });
    historyForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!historyForm.reportValidity() || !byId("alpaca-option-contract").value) return;
      const body = optionHistoryBody();
      disable(historyLoad, true, "Loading history…"); historyDownload.disabled = true; status(historyStatus, "Loading historical option bars…");
      try {
        const payload = await jsonRequest("/api/market-data/options/history", { method: "POST", body: JSON.stringify(body) });
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        table(byId("alpaca-option-history-preview"), [
          { key: "timestamp", label: "Timestamp" }, { key: "open", label: "Open", render: (row) => number(row.open) }, { key: "high", label: "High", render: (row) => number(row.high) },
          { key: "low", label: "Low", render: (row) => number(row.low) }, { key: "close", label: "Close", render: (row) => number(row.close) }, { key: "volume", label: "Volume", render: (row) => number(row.volume, 0) },
          { key: "tradeCount", label: "Trades", render: (row) => number(row.tradeCount, 0) }, { key: "vwap", label: "VWAP", render: (row) => number(row.vwap) },
        ], rows);
        plot(byId("alpaca-option-history-chart"), [{ type: "scatter", mode: "lines", x: rows.map((row) => row.timestamp), y: rows.map((row) => row.close), line: { color: "#60a5fa", width: 2 }, name: "Close" }], { yaxis: { title: "Option price", gridcolor: "rgba(148,163,184,.16)" } });
        status(historyStatus, `Loaded ${payload.count.toLocaleString()} historical observations from ${payload.provider || "market data"}${payload.fallbackUsed ? " using automatic fallback" : ""}.`, "success"); historyDownload.disabled = false;
      } catch (error) { byId("alpaca-option-history-preview").innerHTML = `<div class="error-state">${html(error.message)}</div>`; status(historyStatus, error.message, "error"); }
      finally { disable(historyLoad, false); }
    });
    historyDownload.addEventListener("click", async () => {
      const body = optionHistoryBody(); disable(historyDownload, true, "Generating CSV…");
      try { await downloadCsv("/api/market-data/options/history", body, `${body.contractSymbol}-${body.timeframe}.csv`); status(historyStatus, "Valid options-history CSV generated and downloaded.", "success"); }
      catch (error) { status(historyStatus, error.message, "error"); }
      finally { disable(historyDownload, false); }
    });
  }

  function initPredictionMarketHub() {
    const form = byId("prediction-market-form");
    if (!form) return;
    const providerStatus = byId("pm-provider-status");
    const category = byId("pm-category");
    const marketList = byId("pm-market-list");
    const previewButton = byId("pm-preview-button");
    const csvButton = byId("pm-download-csv");
    const jsonButton = byId("pm-download-json");
    const selected = new Map();
    let markets = [];
    let capabilities = {};
    let page = 1;
    let total = 0;
    const pageSize = 30;
    const source = () => form.querySelector('input[name="pm-source"]:checked')?.value || "polymarket_us";
    const mode = () => form.querySelector('input[name="pm-mode"]:checked')?.value || "normalized";
    const localDateTime = (date) => {
      const adjusted = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
      return adjusted.toISOString().slice(0, 16);
    };
    const now = new Date();
    byId("pm-history-end").value = localDateTime(now);
    byId("pm-history-start").value = localDateTime(new Date(now.getTime() - 7 * 86400000));

    function updateSelection() {
      byId("pm-selected-count").textContent = `${selected.size} selected`;
      csvButton.disabled = true;
      jsonButton.disabled = true;
    }
    function updateCapabilities() {
      const provider = capabilities[source()] || {};
      const allowedTargets = new Set(provider.targets || ["price"]);
      [...byId("pm-target").options].forEach((option) => { option.disabled = !allowedTargets.has(option.value); });
      if (!allowedTargets.has(byId("pm-target").value)) byId("pm-target").value = "price";
      [...form.querySelectorAll('input[name="pm-feature"]')].forEach((input) => {
        if (["bid", "ask", "spread", "volume", "open_interest"].includes(input.value)) {
          const capabilityKey = input.value === "open_interest" ? "openInterestHistory" : input.value === "volume" ? "volumeHistory" : "bidAskHistory";
          input.disabled = provider[capabilityKey] === false;
          if (input.disabled) input.checked = false;
        }
      });
    }
    function renderProviderCard(id, label, info) {
      const card = byId(id);
      if (!card) return;
      const publicText = info?.publicConnection === "connected" ? `${Number(info.categoryCount || 0).toLocaleString()} sports categories` : String(info?.publicConnection || "unavailable").replaceAll("_", " ");
      const authText = String(info?.authentication || "not checked").replaceAll("_", " ");
      const historyText = String(info?.historicalConnection?.status || "not checked").replaceAll("_", " ");
      card.dataset.tone = info?.publicConnection === "connected" ? "success" : "warning";
      card.innerHTML = `<span>${html(label)}</span><strong>${html(publicText)}</strong><small>History: ${html(historyText)} · server authentication: ${html(authText)}</small>`;
    }
    async function loadProviderStatus(deep = false) {
      const button = byId("pm-test-sources");
      disable(button, true, deep ? "Testing securely…" : "Checking…");
      try {
        const payload = await jsonRequest(`/api/sports/prediction-markets/status${deep ? "?deep=1" : ""}`);
        capabilities = payload.capabilities || {};
        renderProviderCard("pm-status-polymarket", "Polymarket US", payload.providers?.polymarket_us);
        renderProviderCard("pm-status-kalshi", "Kalshi", payload.providers?.kalshi);
        updateCapabilities();
        status(providerStatus, deep ? "Connection checks completed. Credentials remain server-side and were not returned to this page." : "Provider discovery is ready. Choose a sport to find current markets.", "success");
      } catch (error) { status(providerStatus, error.message, "error"); }
      finally { disable(button, false); }
    }
    async function loadCategories(loadMarketsAfter = false) {
      category.disabled = true;
      category.innerHTML = '<option value="">Loading categories…</option>';
      marketList.innerHTML = '<div class="loading-state">Loading supported sports from the provider…</div>';
      status(providerStatus, `Loading ${source() === "kalshi" ? "Kalshi" : "Polymarket US"} sports categories…`);
      try {
        const payload = await jsonRequest(`/api/sports/prediction-markets/categories?source=${encodeURIComponent(source())}`);
        capabilities[source()] = payload.capabilities || capabilities[source()] || {};
        const categories = Array.isArray(payload.categories) ? payload.categories : [];
        const preferred = categories.find((item) => /(^|\s)mlb($|\s)|baseball/i.test(`${item.id} ${item.label}`)) || categories[0];
        category.innerHTML = categories.length ? categories.map((item) => `<option value="${html(item.id)}"${item.id === preferred?.id ? " selected" : ""}>${html(item.label)}${item.seriesCount ? ` · ${Number(item.seriesCount).toLocaleString()} series` : ""}</option>`).join("") : '<option value="">No supported sports available</option>';
        category.disabled = !categories.length;
        updateCapabilities();
        status(providerStatus, categories.length ? `Loaded ${categories.length.toLocaleString()} real provider categories.` : "No supported sports categories are available right now.", categories.length ? "success" : "warning");
        marketList.innerHTML = '<div class="empty-state">Choose filters, then find available contracts.</div>';
        if (loadMarketsAfter && categories.length) await loadMarkets();
      } catch (error) {
        category.innerHTML = '<option value="">Categories unavailable</option>';
        marketList.innerHTML = `<div class="error-state">${html(error.message)}</div>`;
        status(providerStatus, error.message, "error");
      } finally { category.disabled = false; }
    }
    function marketKey(contract) { return `${contract.source}:${contract.contractId}`; }
    function formatProbability(value) { return Number.isFinite(Number(value)) ? `${(Number(value) * 100).toFixed(1)}%` : "Unavailable"; }
    function renderMarkets() {
      byId("pm-market-count").textContent = `${markets.length.toLocaleString()} shown · ${total.toLocaleString()} matching`;
      byId("pm-page-label").textContent = `Page ${page}`;
      byId("pm-previous-page").disabled = page <= 1;
      byId("pm-next-page").disabled = page * pageSize >= total || markets.length < pageSize;
      if (!markets.length) {
        marketList.innerHTML = '<div class="empty-state">No markets match these filters. Broaden the date, status, or search query.</div>';
        return;
      }
      marketList.innerHTML = `<table class="prediction-market-table"><thead><tr><th scope="col">Select</th><th scope="col">Event / market</th><th scope="col">Outcome</th><th scope="col">Start</th><th scope="col">Status</th><th scope="col">Price</th></tr></thead><tbody>${markets.map((contract, index) => {
        const key = marketKey(contract);
        const checked = selected.has(key) ? " checked" : "";
        return `<tr><td data-label="Select"><input type="checkbox" data-market-index="${index}" aria-label="Select ${html(contract.eventTitle)} ${html(contract.outcome)}"${checked} /></td><td data-label="Event"><strong>${html(contract.eventTitle)}</strong><small>${html(contract.marketTitle)} · ${html(contract.providerSymbol)}</small></td><td data-label="Outcome">${html(contract.outcome)}</td><td data-label="Start">${html(contract.eventStart ? new Date(contract.eventStart).toLocaleString() : "Unavailable")}</td><td data-label="Status"><span class="market-status-pill" data-status="${html(contract.status)}">${html(contract.status)}</span></td><td data-label="Price">${html(formatProbability(contract.currentPrice))}</td></tr>`;
      }).join("")}</tbody></table>`;
      marketList.querySelectorAll("input[data-market-index]").forEach((input) => input.addEventListener("change", () => {
        const contract = markets[Number(input.dataset.marketIndex)];
        const key = marketKey(contract);
        if (input.checked && selected.size >= 25) {
          input.checked = false;
          status(providerStatus, "Select no more than 25 contracts per dataset.", "warning");
          return;
        }
        if (input.checked) {
          selected.set(key, contract);
          const eventEnd = contract.eventStart ? new Date(Math.min(Date.now(), Date.parse(contract.eventStart))) : now;
          if (Number.isFinite(eventEnd.getTime())) {
            byId("pm-history-end").value = localDateTime(eventEnd);
            byId("pm-history-start").value = localDateTime(new Date(eventEnd.getTime() - 7 * 86400000));
          }
        } else selected.delete(key);
        updateSelection();
      }));
    }
    async function loadMarkets() {
      if (!category.value) { status(providerStatus, "Choose a sport or league first.", "warning"); return; }
      const button = byId("pm-find-markets");
      disable(button, true, "Finding markets…");
      marketList.innerHTML = '<div class="loading-state">Discovering provider markets and contracts…</div>';
      const params = new URLSearchParams({ source: source(), category: category.value, status: byId("pm-status").value, search: byId("pm-search").value.trim(), dateFrom: byId("pm-event-from").value, dateTo: byId("pm-event-to").value, page: String(page), pageSize: String(pageSize) });
      try {
        const payload = await jsonRequest(`/api/sports/prediction-markets/markets?${params}`);
        markets = Array.isArray(payload.items) ? payload.items : [];
        total = Number(payload.total || markets.length);
        renderMarkets();
        const coverage = payload.scanLimited ? ` Scanned ${Number(payload.seriesScanned || 0).toLocaleString()} of ${Number(payload.seriesAvailable || 0).toLocaleString()} provider series; refine search or filters for a narrower result.` : "";
        status(providerStatus, markets.length ? `Found ${total.toLocaleString()} matching contracts.${coverage} Select one or more outcomes.` : "No contracts match the current filters.", markets.length ? (payload.providerFailures ? "warning" : "success") : "warning");
      } catch (error) {
        markets = []; total = 0;
        marketList.innerHTML = `<div class="error-state">${html(error.message)}</div>`;
        status(providerStatus, error.message, "error");
      } finally { disable(button, false); }
    }
    function requestBody() {
      if (!selected.size) throw new Error("Select at least one market contract before previewing data.");
      const startValue = byId("pm-history-start").value;
      const endValue = byId("pm-history-end").value;
      if (!startValue || !endValue) throw new Error("Choose a valid historical start and end time.");
      return {
        source: source(), contracts: [...selected.values()], start: new Date(startValue).toISOString(), end: new Date(endValue).toISOString(),
        frequency: byId("pm-frequency").value, pregameOnly: byId("pm-pregame").checked, missing: byId("pm-missing").value,
        mode: mode(), target: byId("pm-target").value, features: [...form.querySelectorAll('input[name="pm-feature"]:checked')].map((input) => input.value),
      };
    }
    function renderValidation(payload) {
      const validation = payload.validation || {};
      const targetMin = validation.targetRange?.min;
      const targetMax = validation.targetRange?.max;
      byId("pm-preview-summary").innerHTML = [
        ["Markets", validation.markets], ["Observations", validation.observations], ["Frequency", payload.frequency], ["Target", payload.target],
        ["Date range", payload.dateRange?.start && payload.dateRange?.end ? `${new Date(payload.dateRange.start).toLocaleString()} → ${new Date(payload.dateRange.end).toLocaleString()}` : "Unavailable"],
        ["Target range", targetMin !== null && targetMin !== undefined ? `${formatProbability(targetMin)}–${formatProbability(targetMax)}` : "Not applicable"],
      ].map(([label, value]) => `<div><span>${html(label)}</span><strong>${html(value)}</strong></div>`).join("");
      const checks = [
        ["Missing target rows removed", validation.missingTargetRowsRemoved || 0], ["Duplicate timestamps resolved", validation.duplicateTimestampsResolved || 0],
        ["Invalid timestamps removed", validation.invalidTimestampsRemoved || 0], ["Invalid probability rows removed", validation.invalidProbabilityRowsRemoved || 0],
        ["Post-start rows excluded", validation.postStartRowsRemoved || 0], ["Timezone", validation.timezone || "UTC"],
      ];
      const node = byId("pm-validation");
      node.classList.remove("hidden");
      node.dataset.tone = validation.ready ? "success" : "warning";
      node.innerHTML = `<div class="validation-heading"><strong>${validation.ready ? "Dataset ready" : "Dataset needs review"}</strong><span>${html(payload.mode === "canvas" ? "SageMaker Canvas validation" : "Historical data validation")}</span></div><div class="validation-checks">${checks.map(([label, value]) => `<div><span>${html(label)}</span><strong>${html(value)}</strong></div>`).join("")}</div>${Array.isArray(validation.messages) ? `<ul>${validation.messages.map((message) => `<li>${html(message)}</li>`).join("")}</ul>` : ""}`;
    }
    async function preview(event) {
      event.preventDefault();
      let body;
      try { body = requestBody(); } catch (error) { status(providerStatus, error.message, "warning"); return; }
      disable(previewButton, true, "Preparing preview…");
      csvButton.disabled = true; jsonButton.disabled = true;
      status(providerStatus, "Fetching and validating historical market observations…");
      try {
        const payload = await jsonRequest("/api/sports/prediction-markets/preview", { method: "POST", body: JSON.stringify(body) });
        renderValidation(payload);
        const headers = Array.isArray(payload.headers) ? payload.headers : [];
        const rows = Array.isArray(payload.previewRows) ? payload.previewRows : [];
        table(byId("pm-preview-table"), headers.map((key) => ({ key, label: key.replaceAll("_", " "), render: (row) => typeof row[key] === "number" ? number(row[key], 6) : row[key] })), rows, 100);
        csvButton.disabled = !payload.validation?.ready;
        jsonButton.disabled = !payload.validation?.ready;
        status(providerStatus, `Previewed ${Number(payload.rowCount || 0).toLocaleString()} validated observations.`, payload.validation?.ready ? "success" : "warning");
      } catch (error) {
        byId("pm-preview-table").innerHTML = `<div class="error-state">${html(error.message)}</div>`;
        status(providerStatus, error.message, "error");
      } finally { disable(previewButton, false); }
    }
    async function download(format) {
      let body;
      try { body = requestBody(); } catch (error) { status(providerStatus, error.message, "warning"); return; }
      const button = format === "json" ? jsonButton : csvButton;
      disable(button, true, `Generating ${format.toUpperCase()}…`);
      try {
        await downloadFile("/api/sports/prediction-markets/export", body, format, `${source()}-${mode()}-sports.${format}`);
        status(providerStatus, `${format.toUpperCase()} generated and downloaded successfully.`, "success");
      } catch (error) { status(providerStatus, error.message, "error"); }
      finally { disable(button, false); }
    }

    form.querySelectorAll('input[name="pm-source"]').forEach((input) => input.addEventListener("change", async () => {
      selected.clear(); markets = []; total = 0; page = 1; updateSelection(); updateCapabilities(); await loadCategories(true);
    }));
    form.querySelectorAll('input[name="pm-mode"]').forEach((input) => input.addEventListener("change", () => {
      byId("pm-canvas-options").classList.toggle("hidden", mode() !== "canvas");
      csvButton.disabled = true; jsonButton.disabled = true;
    }));
    [byId("pm-frequency"), byId("pm-missing"), byId("pm-pregame"), byId("pm-target")].forEach((control) => control.addEventListener("change", () => { csvButton.disabled = true; jsonButton.disabled = true; }));
    byId("pm-find-markets").addEventListener("click", () => { page = 1; loadMarkets(); });
    byId("pm-search").addEventListener("keydown", (event) => { if (event.key === "Enter") { event.preventDefault(); page = 1; loadMarkets(); } });
    byId("pm-previous-page").addEventListener("click", () => { if (page > 1) { page -= 1; loadMarkets(); } });
    byId("pm-next-page").addEventListener("click", () => { page += 1; loadMarkets(); });
    byId("pm-test-sources").addEventListener("click", () => loadProviderStatus(true));
    form.addEventListener("submit", preview);
    csvButton.addEventListener("click", () => download("csv"));
    jsonButton.addEventListener("click", () => download("json"));
    updateSelection();
    loadProviderStatus(false).finally(() => loadCategories(true));
  }

  async function firebaseToken() {
    for (let attempt = 0; attempt < 40; attempt += 1) {
      if (window.firebase?.apps?.length && window.firebase.auth) {
        const user = window.firebase.auth().currentUser;
        if (user && !user.isAnonymous) return user.getIdToken();
        if (user?.isAnonymous) throw new Error("Sign in with a full account to manage private integrations.");
      }
      await new Promise((resolve) => setTimeout(resolve, 150));
    }
    throw new Error("Sign in to manage private integrations.");
  }
  function awsBody() {
    return { accountId: byId("aws-account-id")?.value, region: byId("aws-region")?.value, roleArn: byId("aws-role-arn")?.value, executionRoleArn: byId("aws-execution-role-arn")?.value, s3Bucket: byId("aws-s3-bucket")?.value };
  }
  function initAws() {
    const form = byId("aws-integration-form"); if (!form) return;
    const awsStatus = byId("aws-integration-status"); const test = byId("aws-test-connection"); const disconnect = byId("aws-disconnect");
    const editable = [...form.querySelectorAll("input, select, button[type='submit']")];
    const setAccess = (user) => {
      const allowed = Boolean(user && !user.isAnonymous);
      editable.forEach((control) => { control.disabled = !allowed; });
      if (!allowed) {
        test.disabled = true;
        disconnect.disabled = true;
        status(awsStatus, "Sign in with a full account to configure a private AWS / SageMaker integration.");
      }
      return allowed;
    };
    async function authRequest(url, options = {}) { const token = await firebaseToken(); return jsonRequest(url, { ...options, headers: { Authorization: `Bearer ${token}`, ...(options.headers || {}) } }); }
    async function load() {
      try {
        const payload = await authRequest("/api/me/aws-integration"); const config = payload.config;
        if (config) { byId("aws-account-id").value = config.accountId || ""; byId("aws-region").value = config.region || "us-east-1"; byId("aws-role-arn").value = config.roleArn || ""; byId("aws-execution-role-arn").value = config.executionRoleArn || ""; byId("aws-s3-bucket").value = config.s3Bucket || ""; test.disabled = false; disconnect.disabled = false; status(awsStatus, `${config.status.replaceAll("_", " ")}: ${config.lastTestMessage || "Configuration saved."}`, config.status === "connected" ? "success" : "warning"); }
        else status(awsStatus, "No AWS role is connected. Save a role configuration to begin.");
      } catch (error) { status(awsStatus, error.message, "error"); }
    }
    form.addEventListener("submit", async (event) => { event.preventDefault(); if (!form.reportValidity()) return; const submit = form.querySelector('button[type="submit"]'); disable(submit, true, "Saving…"); try { await authRequest("/api/me/aws-integration", { method: "PUT", body: JSON.stringify(awsBody()) }); status(awsStatus, "Role configuration saved privately. Test the connection before launching SageMaker.", "success"); test.disabled = false; disconnect.disabled = false; } catch (error) { status(awsStatus, error.message, "error"); } finally { disable(submit, false); } });
    test.addEventListener("click", async () => { disable(test, true, "Testing OIDC role…"); status(awsStatus, "Assuming your AWS role and checking SageMaker authorization…"); try { const payload = await authRequest("/api/me/aws-integration/test", { method: "POST", body: "{}" }); status(awsStatus, `Connected to AWS account ${payload.accountId}. SageMaker access verified.`, "success"); } catch (error) { status(awsStatus, error.message, "error"); } finally { disable(test, false); } });
    disconnect.addEventListener("click", async () => { if (!window.confirm("Disconnect this AWS role from your Quantura account? Existing AWS resources are not deleted.")) return; disable(disconnect, true, "Disconnecting…"); try { await authRequest("/api/me/aws-integration", { method: "DELETE" }); form.reset(); byId("aws-region").value = "us-east-1"; test.disabled = true; disconnect.disabled = true; status(awsStatus, "AWS integration disconnected. Existing resources in AWS were not changed.", "success"); } catch (error) { status(awsStatus, error.message, "error"); } finally { disable(disconnect, false); } });
    const firebaseAuth = window.firebase?.auth?.();
    if (firebaseAuth?.onAuthStateChanged) {
      setAccess(firebaseAuth.currentUser);
      firebaseAuth.onAuthStateChanged((user) => {
        if (setAccess(user)) load();
      });
    } else {
      setAccess(null);
    }
  }

  function initForecastAlerts() {
    const form = byId("forecast-alert-settings-form");
    if (!form) return;
    const settingsStatus = byId("forecast-alert-settings-status");
    const list = byId("forecast-active-alerts");
    const save = byId("forecast-alert-settings-save");
    const refresh = byId("forecast-alert-settings-refresh");
    const boundaryInputs = [...form.querySelectorAll("[data-profile-alert-boundary]")];
    const editable = [...form.querySelectorAll("input:not([readonly]), select, button")];

    async function authRequest(url, options = {}) {
      const token = await firebaseToken();
      return jsonRequest(url, { ...options, headers: { Authorization: `Bearer ${token}`, ...(options.headers || {}) } });
    }

    const formatWhen = (value) => {
      const parsed = Date.parse(String(value || ""));
      return Number.isFinite(parsed) ? new Date(parsed).toLocaleString() : "Not yet";
    };

    function renderAlertList(alerts) {
      if (!Array.isArray(alerts) || !alerts.length) {
        list.innerHTML = '<div class="empty-state">Prediction CSV uploads will create alert configurations here.</div>';
        return;
      }
      list.innerHTML = alerts.map((alert) => {
        const monitored = Array.isArray(alert.monitoredBoundaries) ? alert.monitoredBoundaries : [];
        const available = Array.isArray(alert.availableBoundaries) ? alert.availableBoundaries : [];
        const expired = alert.status === "expired";
        return `<article class="forecast-alert-list-item" data-forecast-alert-id="${html(alert.id)}" data-forecast-alert-enabled="${alert.enabled ? "true" : "false"}">
          <div class="forecast-alert-list-head"><div><strong>${html(alert.ticker || "Forecast")}</strong><span>${html(alert.horizonStart || "N/A")} → ${html(alert.horizonEnd || "N/A")}</span></div><span class="pill" data-alert-state>${html(String(alert.status || "disabled").replaceAll("_", " "))}</span></div>
          <div class="forecast-alert-row-controls">
            <div class="forecast-boundary-choices">${available.map((boundary) => `<label><input type="checkbox" value="${html(boundary)}" data-alert-row-boundary ${monitored.includes(boundary) ? "checked" : ""} ${expired ? "disabled" : ""}/> ${html(boundary)}</label>`).join("")}</div>
            <select data-alert-row-session aria-label="Market session" ${expired ? "disabled" : ""}><option value="regular" ${alert.sessionMode === "regular" ? "selected" : ""}>Regular hours</option><option value="extended" ${alert.sessionMode === "extended" ? "selected" : ""}>Extended hours</option></select>
          </div>
          <dl class="forecast-alert-list-meta"><div><dt>Last checked</dt><dd>${html(formatWhen(alert.lastCheckedAt))}</dd></div><div><dt>Last crossing</dt><dd>${html(alert.lastCrossing ? `${alert.lastCrossing.boundary} ${alert.lastCrossing.direction}` : "None")}</dd></div><div><dt>Last email</dt><dd>${html(formatWhen(alert.lastNotificationAt))}</dd></div></dl>
          ${alert.lastError?.message ? `<p class="small forecast-alert-error">${html(alert.lastError.message)}</p>` : ""}
          <div class="hero-actions"><button class="cta secondary small" type="button" data-alert-row-save ${expired ? "disabled" : ""}>Save</button><button class="cta ${alert.enabled ? "danger" : "secondary"} small" type="button" data-alert-row-toggle ${expired ? "disabled" : ""}>${alert.enabled ? "Disable" : "Enable"}</button><a class="cta secondary small" href="/autopilot?runId=${encodeURIComponent(alert.runId || alert.id)}">Open analysis</a></div>
        </article>`;
      }).join("");
    }

    function renderSettings(payload) {
      const settings = payload?.settings || {};
      byId("forecast-alerts-email-enabled").checked = settings.emailEnabled !== false;
      byId("forecast-alert-email").value = settings.alertEmail || "";
      byId("forecast-alert-default-session").value = settings.defaultSessionMode === "extended" ? "extended" : "regular";
      const defaults = Array.isArray(settings.defaultBoundaries) ? settings.defaultBoundaries : ["P10", "P50", "P90"];
      boundaryInputs.forEach((input) => { input.checked = defaults.includes(input.value); });
      byId("forecast-alert-email-help").textContent = settings.emailVerified
        ? "Alerts are delivered only to this verified Firebase email."
        : "Verify your Firebase email before enabling forecast alert delivery.";
      renderAlertList(payload?.alerts || []);
      status(settingsStatus, settings.emailVerified ? "Forecast alert settings loaded." : "Email verification is required before alerts can be enabled.", settings.emailVerified ? "success" : "warning");
    }

    async function load() {
      disable(refresh, true, "Refreshing…");
      try { renderSettings(await authRequest("/api/me/forecast-alert-settings")); }
      catch (error) { status(settingsStatus, error.message, "error"); }
      finally { disable(refresh, false); }
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const defaults = boundaryInputs.filter((input) => input.checked).map((input) => input.value);
      if (!defaults.length) { status(settingsStatus, "Select at least one default boundary.", "error"); return; }
      disable(save, true, "Saving…");
      try {
        const payload = await authRequest("/api/me/forecast-alert-settings", { method: "PUT", body: JSON.stringify({ emailEnabled: byId("forecast-alerts-email-enabled").checked, defaultBoundaries: defaults, defaultSessionMode: byId("forecast-alert-default-session").value }) });
        renderSettings(payload);
        status(settingsStatus, "Forecast alert settings saved.", "success");
      } catch (error) { status(settingsStatus, error.message, "error"); }
      finally { disable(save, false); }
    });
    refresh.addEventListener("click", load);
    list.addEventListener("click", async (event) => {
      const button = event.target.closest("[data-alert-row-save], [data-alert-row-toggle]");
      if (!button) return;
      const row = button.closest("[data-forecast-alert-id]");
      const id = row?.dataset?.forecastAlertId;
      if (!id) return;
      const boundaries = [...row.querySelectorAll("[data-alert-row-boundary]:checked")].map((input) => input.value);
      if (!boundaries.length) { status(settingsStatus, "Select at least one boundary for this alert.", "error"); return; }
      const toggle = button.hasAttribute("data-alert-row-toggle");
      const currentlyEnabled = row.dataset.forecastAlertEnabled === "true";
      disable(button, true, toggle ? (currentlyEnabled ? "Disabling…" : "Enabling…") : "Saving…");
      try {
        await authRequest(`/api/autopilot/runs/${encodeURIComponent(id)}/price-alert`, { method: "PUT", body: JSON.stringify({ enabled: toggle ? !currentlyEnabled : currentlyEnabled, boundaries, sessionMode: row.querySelector("[data-alert-row-session]").value }) });
        await load();
        status(settingsStatus, toggle ? "Alert status updated." : "Alert boundaries updated.", "success");
      } catch (error) { status(settingsStatus, error.message, "error"); }
      finally { disable(button, false); }
    });

    const firebaseAuth = window.firebase?.auth?.();
    if (firebaseAuth?.onAuthStateChanged) {
      const setAccess = (user) => {
        const allowed = Boolean(user && !user.isAnonymous);
        editable.forEach((control) => { control.disabled = !allowed; });
        if (allowed) load();
        else {
          list.innerHTML = '<div class="empty-state">Sign in with a full account to manage private forecast alerts.</div>';
          status(settingsStatus, "Sign in with a full account to manage forecast alerts.");
        }
      };
      setAccess(firebaseAuth.currentUser);
      firebaseAuth.onAuthStateChanged(setAccess);
    }
  }

  window.addEventListener("DOMContentLoaded", () => { initHistoricalData(); initOptions(); initPredictionMarketHub(); initAws(); initForecastAlerts(); });
})();
