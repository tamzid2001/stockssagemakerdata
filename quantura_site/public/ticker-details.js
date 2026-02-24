(() => {
  const root = document.getElementById("ticker-details-root");
  if (!root) return;

  const STORAGE_BASE_KEY = "quantura_market_data_base_v1";
  const STORAGE_LAST_SYMBOL = "quantura_ticker_details_symbol_v1";

  const form = document.getElementById("ticker-form");
  const symbolInput = document.getElementById("ticker-symbol-input");
  const apiBaseInput = document.getElementById("ticker-api-base");
  const loadButton = document.getElementById("ticker-load-btn");
  const copyJsonButton = document.getElementById("ticker-copy-json");
  const statusNode = document.getElementById("ticker-load-status");
  const rawSearchInput = document.getElementById("ticker-raw-search");
  const rawOutput = document.getElementById("ticker-raw-output");

  const overviewPanel = root.querySelector('[data-tab-panel="overview"]');
  const fundamentalsPanel = root.querySelector('[data-tab-panel="fundamentals"]');
  const valuationPanel = root.querySelector('[data-tab-panel="valuation"]');

  let currentPayload = null;
  let currentRawInfo = {};

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#039;");

  const numberFmt = (value, digits = 2) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: digits,
    }).format(num);
  };

  const percentFmt = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    const scaled = Math.abs(num) <= 1 ? num * 100 : num;
    return `${numberFmt(scaled, 2)}%`;
  };

  const currencyFmt = (value, currency = "USD") => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    try {
      return new Intl.NumberFormat(undefined, {
        style: "currency",
        currency,
        maximumFractionDigits: 2,
      }).format(num);
    } catch {
      return numberFmt(num, 2);
    }
  };

  const compactFmt = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return new Intl.NumberFormat(undefined, {
      notation: "compact",
      maximumFractionDigits: 2,
    }).format(num);
  };

  const getApiBase = () => {
    const explicit = String(window.__QUANTURA_MARKET_DATA_BASE__ || "").trim();
    const saved = String(localStorage.getItem(STORAGE_BASE_KEY) || "").trim();
    const fallback = "http://127.0.0.1:8090";
    return (explicit || saved || fallback).replace(/\/$/, "");
  };

  const normalizeSymbol = (input) => String(input || "").trim().toUpperCase().replace(/[^A-Z0-9.^\-=]/g, "");

  const parseSymbolFromUrl = () => {
    const pathParts = window.location.pathname.split("/").filter(Boolean);
    if (pathParts.length >= 2 && pathParts[0] === "ticker") {
      return normalizeSymbol(decodeURIComponent(pathParts[1] || ""));
    }
    const querySymbol = new URL(window.location.href).searchParams.get("symbol");
    return normalizeSymbol(querySymbol || "");
  };

  const setStatus = (text, isError = false) => {
    statusNode.textContent = text;
    statusNode.classList.toggle("error", Boolean(isError));
  };

  const setLoading = (loading) => {
    loadButton.disabled = loading;
    const span = loadButton.querySelector("span");
    if (span) span.textContent = loading ? "Loading..." : "Load ticker";
  };

  const renderKvGrid = (title, rows) => {
    return `
      <div class="card" style="margin-top: 10px;">
        <h3>${escapeHtml(title)}</h3>
        <div class="help-kv-grid small">
          ${rows
            .map(
              (row) => `<div><strong>${escapeHtml(row.label)}</strong> ${escapeHtml(row.value)}</div>`
            )
            .join("")}
        </div>
      </div>
    `;
  };

  const classifyRawGroup = (key) => {
    const lower = String(key || "").toLowerCase();
    if (/(name|sector|industry|country|website|summary|business|address|phone)/.test(lower)) return "profile";
    if (/(marketcap|enterprise|ratio|pe|book|ev|valuation|price)/.test(lower)) return "valuation";
    if (/(revenue|income|margin|cash|ebitda|eps|operating|profit|asset|equity)/.test(lower)) return "fundamentals";
    if (/(beta|risk|volatil|short|debt|drawdown|quickratio|currentratio)/.test(lower)) return "risk";
    if (/(dividend|yield|payout|exdividend)/.test(lower)) return "dividends";
    return "other";
  };

  const safeJsonStringify = (value) => {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  };

  const renderRawValue = (value) => {
    if (value === null || value === undefined) {
      return '<span class="muted">null</span>';
    }
    if (typeof value === "string") return escapeHtml(value);
    if (typeof value === "number" || typeof value === "boolean") return escapeHtml(String(value));

    if (Array.isArray(value) || typeof value === "object") {
      const typeLabel = Array.isArray(value) ? `array(${value.length})` : "object";
      return `
        <details>
          <summary>${typeLabel}</summary>
          <pre style="white-space: pre-wrap; word-break: break-word;">${escapeHtml(safeJsonStringify(value))}</pre>
        </details>
      `;
    }

    return escapeHtml(String(value));
  };

  const renderAllFields = (rawInfo, queryText = "") => {
    const filterText = String(queryText || "").trim().toLowerCase();
    const entries = Object.entries(rawInfo || {});
    const filtered = filterText
      ? entries.filter(([key]) => String(key).toLowerCase().includes(filterText))
      : entries;

    if (!filtered.length) {
      rawOutput.innerHTML = '<div class="small muted">No fields match this search.</div>';
      return;
    }

    const grouped = {
      profile: [],
      valuation: [],
      fundamentals: [],
      risk: [],
      dividends: [],
      other: [],
    };

    for (const [key, value] of filtered) {
      const group = classifyRawGroup(key);
      grouped[group].push([key, value]);
    }

    const sections = Object.entries(grouped)
      .map(([groupName, rows]) => {
        if (!rows.length) return "";
        const content = rows
          .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
          .map(
            ([key, value]) => `
              <div class="card" style="margin: 6px 0;">
                <div class="small"><strong>${escapeHtml(String(key))}</strong> <span class="muted">(${escapeHtml(typeof value)})</span></div>
                <div class="small" style="margin-top: 6px;">${renderRawValue(value)}</div>
              </div>
            `
          )
          .join("");
        return `
          <details open style="margin-bottom: 10px;">
            <summary><strong>${escapeHtml(groupName)}</strong> (${rows.length})</summary>
            ${content}
          </details>
        `;
      })
      .join("");

    rawOutput.innerHTML = sections;
  };

  const renderQuotePayload = (record) => {
    const price = record.price || {};
    const profile = record.profile || {};
    const valuation = record.valuation || {};
    const fundamentals = record.fundamentals || {};
    const risk = record.risk || {};
    const dividends = record.dividends || {};
    const currency = String(price.currency || "USD");

    overviewPanel.innerHTML = [
      renderKvGrid("Company / Profile", [
        { label: "Name", value: profile.longName || record.symbol || "-" },
        { label: "Sector", value: profile.sector || "-" },
        { label: "Industry", value: profile.industry || "-" },
        { label: "Country", value: profile.country || "-" },
        { label: "Website", value: profile.website || "-" },
        { label: "Price", value: currencyFmt(price.last, currency) },
        { label: "Prev Close", value: currencyFmt(price.prevClose, currency) },
        { label: "Day Range", value: `${currencyFmt(price.dayLow, currency)} - ${currencyFmt(price.dayHigh, currency)}` },
        { label: "Volume", value: compactFmt(price.volume) },
      ]),
      renderKvGrid("Risk and Dividends", [
        { label: "Beta", value: numberFmt(risk.beta, 3) },
        { label: "Short Ratio", value: numberFmt(risk.shortRatio, 3) },
        { label: "Dividend Rate", value: numberFmt(dividends.dividendRate, 4) },
        { label: "Dividend Yield", value: percentFmt(dividends.dividendYield) },
        { label: "Payout Ratio", value: percentFmt(dividends.payoutRatio) },
        { label: "Ex-Dividend", value: dividends.exDividendDate ? String(dividends.exDividendDate) : "-" },
      ]),
      `
        <div class="card" style="margin-top: 10px;">
          <h3>Business Summary</h3>
          <p class="small">${escapeHtml(profile.longBusinessSummary || "No long business summary available.")}</p>
        </div>
      `,
    ].join("");

    fundamentalsPanel.innerHTML = renderKvGrid("Fundamentals", [
      { label: "Revenue TTM", value: compactFmt(fundamentals.revenueTTM) },
      { label: "Gross Margins", value: percentFmt(fundamentals.grossMargins) },
      { label: "Profit Margins", value: percentFmt(fundamentals.profitMargins) },
      { label: "Operating Margins", value: percentFmt(fundamentals.operatingMargins) },
      { label: "EBITDA Margins", value: percentFmt(fundamentals.ebitdaMargins) },
      { label: "Return on Assets", value: percentFmt(fundamentals.returnOnAssets) },
      { label: "Return on Equity", value: percentFmt(fundamentals.returnOnEquity) },
    ]);

    valuationPanel.innerHTML = renderKvGrid("Valuation", [
      { label: "Market Cap", value: compactFmt(valuation.marketCap) },
      { label: "Trailing PE", value: numberFmt(valuation.trailingPE, 3) },
      { label: "Forward PE", value: numberFmt(valuation.forwardPE, 3) },
      { label: "Price to Book", value: numberFmt(valuation.priceToBook, 3) },
      { label: "Enterprise Value", value: compactFmt(valuation.enterpriseValue) },
      { label: "Shares Outstanding", value: compactFmt(valuation.sharesOutstanding) },
    ]);

    currentRawInfo = record.raw && typeof record.raw === "object" ? record.raw : {};
    renderAllFields(currentRawInfo, rawSearchInput.value || "");
  };

  const fetchTicker = async (symbol) => {
    const base = getApiBase();
    const url = new URL(`${base}/stocks/quote`);
    url.searchParams.set("tickers", symbol);
    url.searchParams.set("mode", "full");

    const response = await fetch(url.toString(), {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(String(payload?.detail || `HTTP ${response.status}`));
    }
    const record = Array.isArray(payload?.items) ? payload.items[0] : null;
    if (!record || typeof record !== "object") {
      throw new Error("No quote data returned for symbol.");
    }
    return { payload, record };
  };

  const activateTab = (tabName) => {
    const buttons = Array.from(root.querySelectorAll("[data-tab-target]"));
    const panels = Array.from(root.querySelectorAll("[data-tab-panel]"));

    for (const button of buttons) {
      const isActive = String(button.getAttribute("data-tab-target")) === tabName;
      button.classList.toggle("active", isActive);
    }

    for (const panel of panels) {
      const matches = String(panel.getAttribute("data-tab-panel")) === tabName;
      panel.classList.toggle("hidden", !matches);
    }
  };

  const loadSymbol = async (symbol) => {
    const cleanSymbol = normalizeSymbol(symbol);
    if (!cleanSymbol) {
      setStatus("Enter a valid ticker symbol.", true);
      return;
    }

    setLoading(true);
    setStatus(`Loading ${cleanSymbol}...`);
    try {
      const { payload, record } = await fetchTicker(cleanSymbol);
      currentPayload = payload;
      renderQuotePayload(record);

      localStorage.setItem(STORAGE_LAST_SYMBOL, cleanSymbol);
      const nextPath = `/ticker/${encodeURIComponent(cleanSymbol)}`;
      if (window.location.pathname !== nextPath) {
        history.replaceState({}, "", nextPath);
      }
      setStatus(`Loaded ${cleanSymbol} · ${record.asOf ? new Date(record.asOf).toLocaleString() : "latest"}`);
    } catch (error) {
      setStatus(error?.message || "Failed to load ticker details.", true);
    } finally {
      setLoading(false);
    }
  };

  root.querySelector("#ticker-tabs")?.addEventListener("click", (event) => {
    const button = event.target.closest("[data-tab-target]");
    if (!button) return;
    activateTab(String(button.getAttribute("data-tab-target") || "overview"));
  });

  rawSearchInput?.addEventListener("input", () => {
    renderAllFields(currentRawInfo, rawSearchInput.value || "");
  });

  copyJsonButton?.addEventListener("click", async () => {
    const data = currentRawInfo && Object.keys(currentRawInfo).length ? currentRawInfo : currentPayload || {};
    const text = safeJsonStringify(data);
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        setStatus("Raw JSON copied to clipboard.");
      } else {
        throw new Error("Clipboard not available");
      }
    } catch {
      setStatus("Unable to copy JSON on this device/browser.", true);
    }
  });

  apiBaseInput.value = getApiBase();
  apiBaseInput.addEventListener("change", () => {
    const value = String(apiBaseInput.value || "").trim().replace(/\/$/, "");
    if (value) {
      localStorage.setItem(STORAGE_BASE_KEY, value);
    } else {
      localStorage.removeItem(STORAGE_BASE_KEY);
      apiBaseInput.value = getApiBase();
    }
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    await loadSymbol(symbolInput.value);
  });

  activateTab("overview");

  const initial = parseSymbolFromUrl() || normalizeSymbol(localStorage.getItem(STORAGE_LAST_SYMBOL) || "") || "AAPL";
  symbolInput.value = initial;
  loadSymbol(initial);
})();
