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

  function initMlb() {
    const form = byId("mlb-market-form");
    if (!form) return;
    const game = byId("mlb-game"); const outcome = byId("mlb-outcome"); const sourceStatus = byId("mlb-source-status"); const download = byId("mlb-download");
    let markets = [];
    async function loadGames() {
      const button = byId("mlb-refresh-games"); disable(button, true, "Refreshing…"); status(sourceStatus, "Loading genuine MLB moneyline markets from Polymarket US…");
      try {
        const payload = await jsonRequest(`/api/sports/mlb/games?scope=${encodeURIComponent(byId("mlb-scope").value)}`);
        markets = Array.isArray(payload.markets) ? payload.markets : [];
        game.innerHTML = markets.length ? `<option value="">Choose a game</option>${markets.map((market) => `<option value="${html(market.marketSlug)}">${html(market.eventTitle)} · ${html(new Date(market.gameStart).toLocaleString())}</option>`).join("")}` : '<option value="">No MLB markets available</option>';
        outcome.innerHTML = '<option value="">Choose a game first</option>';
        status(sourceStatus, markets.length ? `Loaded ${markets.length} MLB moneyline markets.` : "No MLB moneyline markets are available right now.", markets.length ? "success" : "warning");
      } catch (error) { game.innerHTML = '<option value="">Markets unavailable</option>'; status(sourceStatus, error.message, "error"); }
      finally { disable(button, false); }
    }
    game.addEventListener("change", () => {
      const market = markets.find((item) => item.marketSlug === game.value);
      outcome.innerHTML = market ? `<option value="">Choose an outcome</option>${market.sides.map((side) => `<option value="${html(side.itemId)}">${html(side.team)} · ${html(side.position)}</option>`).join("")}` : '<option value="">Choose a game first</option>';
      download.disabled = true;
    });
    byId("mlb-scope").addEventListener("change", loadGames);
    byId("mlb-refresh-games").addEventListener("click", loadGames);
    form.addEventListener("submit", async (event) => {
      event.preventDefault(); if (!form.reportValidity()) return;
      const submit = form.querySelector('button[type="submit"]'); const body = { marketSlug: game.value, itemId: outcome.value, limit: byId("mlb-row-limit").value };
      disable(submit, true, "Loading history…"); download.disabled = true; status(sourceStatus, "Requesting and normalizing 1-minute pregame observations…");
      try {
        const payload = await jsonRequest("/api/sports/mlb/history", { method: "POST", body: JSON.stringify(body) }); const rows = Array.isArray(payload.rows) ? payload.rows : []; const meta = payload.metadata || {};
        byId("mlb-metadata").innerHTML = [["Teams / game", meta.event_title], ["Market ID", meta.market_id], ["Outcome", `${meta.team} (${meta.position})`], ["Item ID", meta.item_id], ["Game start", new Date(meta.game_start).toLocaleString()], ["Data freshness", new Date(meta.data_freshness).toLocaleString()], ["Loaded / available", `${meta.output_rows.toLocaleString()} / ${meta.available_rows.toLocaleString()}`], ["Observed / forward-filled", `${meta.observed_minutes.toLocaleString()} / ${meta.forward_filled_minutes.toLocaleString()}`]].map(([label, value]) => `<div><span>${html(label)}</span><strong>${html(value)}</strong></div>`).join("");
        byId("mlb-history-summary").textContent = `${meta.team}: ${meta.output_rows.toLocaleString()} pregame one-minute rows, oldest to newest.`;
        table(byId("mlb-history-preview"), [{ key: "datetime", label: "Timestamp (UTC)" }, { key: "price", label: "Probability", render: (row) => `${(Number(row.price) * 100).toFixed(2)}%` }, { key: "minutes_before_start", label: "Minutes before game", render: (row) => number(row.minutes_before_start, 0) }, { key: "item_id", label: "Outcome item ID" }], rows);
        plot(byId("mlb-history-chart"), [{ type: "scatter", mode: "lines", x: rows.map((row) => row.datetime), y: rows.map((row) => Number(row.price) * 100), line: { color: "#34d399", width: 2 }, name: "Pregame probability" }], { yaxis: { title: "Probability (%)", range: [0, 100], gridcolor: "rgba(148,163,184,.16)" } });
        status(sourceStatus, "Pregame dataset loaded successfully.", "success"); download.disabled = false;
      } catch (error) { byId("mlb-history-preview").innerHTML = `<div class="error-state">${html(error.message)}</div>`; status(sourceStatus, error.message, "error"); }
      finally { disable(submit, false); }
    });
    download.addEventListener("click", async () => {
      const body = { marketSlug: game.value, itemId: outcome.value, limit: byId("mlb-row-limit").value }; disable(download, true, "Generating CSV…");
      try { await downloadCsv("/api/sports/mlb/history", body, "polymarket-us-mlb-pregame-1m.csv"); status(sourceStatus, "SageMaker-ready CSV generated and downloaded.", "success"); }
      catch (error) { status(sourceStatus, error.message, "error"); }
      finally { disable(download, false); }
    });
    loadGames();
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

  window.addEventListener("DOMContentLoaded", () => { initHistoricalData(); initOptions(); initMlb(); initAws(); initForecastAlerts(); });
})();
