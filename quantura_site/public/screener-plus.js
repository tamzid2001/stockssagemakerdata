(() => {
  if (window.location.pathname !== "/screener") return;

  const screenerRoot = document.getElementById("screener");
  if (!screenerRoot) return;

  const STORAGE_BASE_KEY = "quantura_market_data_base_v1";
  const STORAGE_SCREENER_STATE = "quantura_screener_plus_state_v1";

  const getApiBase = () => {
    const explicit = String(window.__QUANTURA_MARKET_DATA_BASE__ || "").trim();
    const saved = String(localStorage.getItem(STORAGE_BASE_KEY) || "").trim();
    const fallback = "http://127.0.0.1:8090";
    return (explicit || saved || fallback).replace(/\/$/, "");
  };

  const host = screenerRoot.querySelector(".content-grid");
  if (!host) return;

  const card = document.createElement("section");
  card.className = "card";
  card.id = "market-data-screener-card";
  card.innerHTML = `
    <h3>Market Data Screener (YFinance)</h3>
    <p class="small muted">Preset mode or custom query mode. Results are deterministic from one market-data backend schema.</p>

    <form id="market-data-screener-form" class="form-grid" style="margin-top: 10px;">
      <div class="field">
        <label class="label" for="mds-preset">Preset</label>
        <select id="mds-preset" class="input">
          <option value="most_actives" selected>most_actives</option>
          <option value="day_gainers">day_gainers</option>
          <option value="day_losers">day_losers</option>
          <option value="aggressive_small_caps">aggressive_small_caps</option>
          <option value="undervalued_large_caps">undervalued_large_caps</option>
          <option value="">Use custom filters only</option>
        </select>
      </div>
      <div class="field">
        <label class="label" for="mds-size">Size</label>
        <input id="mds-size" class="input" type="number" min="1" max="250" value="25" />
      </div>
      <div class="field">
        <label class="label" for="mds-offset">Offset</label>
        <input id="mds-offset" class="input" type="number" min="0" max="5000" value="0" />
      </div>
      <div class="field">
        <label class="label" for="mds-sort">Sort results</label>
        <select id="mds-sort" class="input">
          <option value="changePercent_desc" selected>Change % (desc)</option>
          <option value="changePercent_asc">Change % (asc)</option>
          <option value="marketCap_desc">Market Cap (desc)</option>
          <option value="marketCap_asc">Market Cap (asc)</option>
          <option value="volume_desc">Volume (desc)</option>
          <option value="volume_asc">Volume (asc)</option>
          <option value="symbol_asc">Symbol (A-Z)</option>
          <option value="symbol_desc">Symbol (Z-A)</option>
        </select>
      </div>
      <div class="field">
        <label class="label" for="mds-region">Region</label>
        <select id="mds-region" class="input">
          <option value="">Any</option>
          <option value="us">US</option>
          <option value="ca">CA</option>
          <option value="gb">GB</option>
          <option value="de">DE</option>
          <option value="fr">FR</option>
          <option value="jp">JP</option>
          <option value="in">IN</option>
        </select>
      </div>
      <div class="field">
        <label class="label" for="mds-exchange">Exchange</label>
        <select id="mds-exchange" class="input">
          <option value="">Any</option>
          <option value="NMS">NASDAQ (NMS)</option>
          <option value="NYQ">NYSE (NYQ)</option>
          <option value="ASE">AMEX (ASE)</option>
        </select>
      </div>
      <div class="field">
        <label class="label" for="mds-marketcap-min">Min Market Cap</label>
        <input id="mds-marketcap-min" class="input" type="number" min="0" step="1000000" placeholder="2000000000" />
      </div>
      <div class="field">
        <label class="label" for="mds-pe-max">Max Trailing PE</label>
        <input id="mds-pe-max" class="input" type="number" min="0" step="0.1" placeholder="25" />
      </div>
      <div class="field">
        <label class="label" for="mds-volume-min">Min Daily Volume</label>
        <input id="mds-volume-min" class="input" type="number" min="0" step="1000" placeholder="1000000" />
      </div>
    </form>

    <details style="margin-top: 10px;">
      <summary class="small">Custom query JSON (optional override)</summary>
      <textarea id="mds-query-json" class="input" style="min-height: 140px; font-family: monospace; margin-top: 8px;" placeholder='{"operator":"and","operands":[{"operator":"eq","operands":["region","us"]}]}'></textarea>
      <p class="small muted" style="margin-top: 8px;">When provided, JSON override is used instead of preset/field inputs.</p>
    </details>

    <div class="hero-actions" style="margin-top: 12px;">
      <button id="mds-run" class="cta" type="button"><i class="iconoir-search" aria-hidden="true"></i><span>Run market screener</span></button>
      <button id="mds-clear" class="cta secondary" type="button"><i class="iconoir-erase" aria-hidden="true"></i><span>Clear output</span></button>
    </div>

    <p id="mds-status" class="small muted" style="margin-top: 10px;">Ready.</p>
    <div id="mds-output" class="panel-output" style="margin-top: 10px;"></div>
  `;

  host.prepend(card);

  const form = document.getElementById("market-data-screener-form");
  const presetInput = document.getElementById("mds-preset");
  const sizeInput = document.getElementById("mds-size");
  const offsetInput = document.getElementById("mds-offset");
  const sortInput = document.getElementById("mds-sort");
  const regionInput = document.getElementById("mds-region");
  const exchangeInput = document.getElementById("mds-exchange");
  const marketCapMinInput = document.getElementById("mds-marketcap-min");
  const peMaxInput = document.getElementById("mds-pe-max");
  const volumeMinInput = document.getElementById("mds-volume-min");
  const jsonInput = document.getElementById("mds-query-json");
  const runBtn = document.getElementById("mds-run");
  const clearBtn = document.getElementById("mds-clear");
  const statusNode = document.getElementById("mds-status");
  const outputNode = document.getElementById("mds-output");

  let currentRows = [];

  const setStatus = (text, isError = false) => {
    statusNode.textContent = text;
    statusNode.classList.toggle("error", Boolean(isError));
  };

  const setLoading = (loading) => {
    runBtn.disabled = loading;
    const span = runBtn.querySelector("span");
    if (span) span.textContent = loading ? "Running..." : "Run market screener";
  };

  const numberFmt = (value, digits = 2) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: digits,
    }).format(num);
  };

  const compactFmt = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 2 }).format(num);
  };

  const saveState = () => {
    const state = {
      preset: presetInput.value,
      size: sizeInput.value,
      offset: offsetInput.value,
      sort: sortInput.value,
      region: regionInput.value,
      exchange: exchangeInput.value,
      marketCapMin: marketCapMinInput.value,
      peMax: peMaxInput.value,
      volumeMin: volumeMinInput.value,
      queryJson: jsonInput.value,
    };
    localStorage.setItem(STORAGE_SCREENER_STATE, JSON.stringify(state));
  };

  const loadState = () => {
    try {
      const state = JSON.parse(localStorage.getItem(STORAGE_SCREENER_STATE) || "{}");
      if (!state || typeof state !== "object") return;
      presetInput.value = String(state.preset || presetInput.value || "most_actives");
      sizeInput.value = String(state.size || sizeInput.value || "25");
      offsetInput.value = String(state.offset || offsetInput.value || "0");
      sortInput.value = String(state.sort || sortInput.value || "changePercent_desc");
      regionInput.value = String(state.region || "");
      exchangeInput.value = String(state.exchange || "");
      marketCapMinInput.value = String(state.marketCapMin || "");
      peMaxInput.value = String(state.peMax || "");
      volumeMinInput.value = String(state.volumeMin || "");
      jsonInput.value = String(state.queryJson || "");
    } catch {
      // Ignore malformed local state.
    }
  };

  const buildFilterQuery = () => {
    const operands = [];

    const region = String(regionInput.value || "").trim();
    if (region) operands.push({ operator: "eq", operands: ["region", region] });

    const exchange = String(exchangeInput.value || "").trim();
    if (exchange) operands.push({ operator: "eq", operands: ["exchange", exchange] });

    const marketCapMin = Number(marketCapMinInput.value);
    if (Number.isFinite(marketCapMin) && marketCapMin > 0) {
      operands.push({ operator: "gte", operands: ["intradaymarketcap", marketCapMin] });
    }

    const peMax = Number(peMaxInput.value);
    if (Number.isFinite(peMax) && peMax > 0) {
      operands.push({ operator: "lte", operands: ["peratio.lasttwelvemonths", peMax] });
    }

    const volumeMin = Number(volumeMinInput.value);
    if (Number.isFinite(volumeMin) && volumeMin > 0) {
      operands.push({ operator: "gte", operands: ["dayvolume", volumeMin] });
    }

    if (!operands.length) return null;
    if (operands.length === 1) return operands[0];
    return { operator: "and", operands };
  };

  const parsePayload = () => {
    const explicitJson = String(jsonInput.value || "").trim();
    if (explicitJson) {
      try {
        return {
          query: JSON.parse(explicitJson),
          size: Number(sizeInput.value || 25),
          offset: Number(offsetInput.value || 0),
        };
      } catch (error) {
        throw new Error(`Invalid custom JSON: ${error.message || error}`);
      }
    }

    const filterQuery = buildFilterQuery();
    if (filterQuery) {
      return {
        query: filterQuery,
        size: Number(sizeInput.value || 25),
        offset: Number(offsetInput.value || 0),
      };
    }

    const preset = String(presetInput.value || "").trim();
    return {
      preset: preset || "most_actives",
      size: Number(sizeInput.value || 25),
      offset: Number(offsetInput.value || 0),
    };
  };

  const sortRows = (rows) => {
    const [field, direction] = String(sortInput.value || "changePercent_desc").split("_");
    const sign = direction === "asc" ? 1 : -1;

    return [...rows].sort((a, b) => {
      const left = a?.[field];
      const right = b?.[field];

      const leftNum = Number(left);
      const rightNum = Number(right);
      if (Number.isFinite(leftNum) && Number.isFinite(rightNum)) {
        return (leftNum - rightNum) * sign;
      }
      return String(left || "").localeCompare(String(right || "")) * sign;
    });
  };

  const renderRows = () => {
    if (!currentRows.length) {
      outputNode.innerHTML = '<div class="small muted">No results returned.</div>';
      return;
    }

    const sorted = sortRows(currentRows);
    outputNode.innerHTML = `
      <div class="small muted" style="margin-bottom:8px;">${sorted.length} symbols</div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Name</th>
              <th>Price</th>
              <th>Change %</th>
              <th>Volume</th>
              <th>Market Cap</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            ${sorted
              .map(
                (row) => `
                  <tr>
                    <td><strong>${row.symbol || "-"}</strong></td>
                    <td>${row.longName || row.shortName || "-"}</td>
                    <td>${numberFmt(row.price, 4)}</td>
                    <td>${numberFmt(row.changePercent, 2)}</td>
                    <td>${compactFmt(row.volume)}</td>
                    <td>${compactFmt(row.marketCap)}</td>
                    <td>
                      <div class="hero-actions" style="gap:6px;">
                        <button type="button" class="task-chip" data-action="mds-watchlist" data-symbol="${row.symbol}">Add to Watchlist</button>
                        <button type="button" class="task-chip" data-action="mds-analyze" data-symbol="${row.symbol}">Analyze</button>
                        <button type="button" class="task-chip" data-action="mds-open" data-symbol="${row.symbol}">Open Details</button>
                      </div>
                    </td>
                  </tr>
                `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    `;
  };

  const addToWatchlist = async (symbol) => {
    try {
      if (!(window.firebase && firebase.auth && firebase.firestore)) {
        throw new Error("Firebase SDK unavailable on this page.");
      }
      const user = firebase.auth().currentUser;
      if (!user) {
        throw new Error("Sign in first to add watchlist symbols.");
      }

      const clean = String(symbol || "").trim().toUpperCase();
      if (!clean) {
        throw new Error("Invalid symbol.");
      }

      await firebase
        .firestore()
        .collection("users")
        .doc(user.uid)
        .collection("watchlist")
        .doc(clean)
        .set(
          {
            ticker: clean,
            notes: "Added from market-data screener",
            createdAt: firebase.firestore.FieldValue.serverTimestamp(),
            updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );

      setStatus(`${clean} added to watchlist.`);
    } catch (error) {
      setStatus(error?.message || "Unable to add watchlist item.", true);
    }
  };

  const runScreener = async () => {
    let payload;
    try {
      payload = parsePayload();
    } catch (error) {
      setStatus(error?.message || "Invalid screener request.", true);
      return;
    }

    const base = getApiBase();
    const url = `${base}/stocks/screener`;

    setLoading(true);
    outputNode.innerHTML = '<div class="small muted">Loading screener results...</div>';
    setStatus("Running market-data screener...");
    saveState();

    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(String(body?.detail || `HTTP ${response.status}`));
      }

      currentRows = Array.isArray(body?.items) ? body.items : [];
      renderRows();
      setStatus(`Loaded ${currentRows.length} rows (${body.mode || "preset"} mode).`);
    } catch (error) {
      currentRows = [];
      outputNode.innerHTML = '<div class="small muted">Unable to load screener results.</div>';
      setStatus(error?.message || "Screener request failed.", true);
    } finally {
      setLoading(false);
    }
  };

  outputNode.addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) return;

    const action = String(button.getAttribute("data-action") || "");
    const symbol = String(button.getAttribute("data-symbol") || "").toUpperCase();
    if (!symbol) return;

    if (action === "mds-watchlist") {
      await addToWatchlist(symbol);
      return;
    }

    if (action === "mds-analyze") {
      window.location.href = `/ticker/${encodeURIComponent(symbol)}?analysis=1`;
      return;
    }

    if (action === "mds-open") {
      window.location.href = `/ticker/${encodeURIComponent(symbol)}`;
    }
  });

  runBtn.addEventListener("click", runScreener);
  clearBtn.addEventListener("click", () => {
    currentRows = [];
    outputNode.innerHTML = '<div class="small muted">Output cleared.</div>';
    setStatus("Cleared.");
  });
  sortInput.addEventListener("change", renderRows);
  form.addEventListener("change", saveState);

  loadState();
})();
