(() => {
  if (window.location.pathname !== "/screener") return;

  const screenerForm = document.getElementById("screener-form");
  if (!screenerForm) return;

  const STORAGE_BASE_KEY = "quantura_market_data_base_v1";
  const STORAGE_SCREENER_STATE = "quantura_screener_plus_state_v2";

  const getApiBase = () => {
    const explicit = String(window.__QUANTURA_MARKET_DATA_BASE__ || "").trim();
    const saved = String(localStorage.getItem(STORAGE_BASE_KEY) || "").trim();
    const fallback = "http://127.0.0.1:8090";
    return (explicit || saved || fallback).replace(/\/$/, "");
  };

  const mount = document.createElement("section");
  mount.className = "field";
  mount.id = "market-data-screener-bridge";
  mount.innerHTML = `
    <details class="learn-more" open>
      <summary><i class="iconoir-database" aria-hidden="true"></i><span>Market Data Screener bridge (YFinance)</span></summary>
      <p class="small muted" style="margin-top:8px;">
        This is merged into the main screener flow. Pull deterministic market-data rows, then feed symbols into your AI portfolio notes.
      </p>
      <div class="form-grid" style="margin-top:10px;">
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
          <label class="label" for="mds-size">Rows</label>
          <input id="mds-size" class="input" type="number" min="1" max="150" value="20" />
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
        <div class="field">
          <label class="label" for="mds-sort">Sort</label>
          <select id="mds-sort" class="input">
            <option value="changePercent_desc" selected>Change % (desc)</option>
            <option value="changePercent_asc">Change % (asc)</option>
            <option value="marketCap_desc">Market Cap (desc)</option>
            <option value="volume_desc">Volume (desc)</option>
            <option value="symbol_asc">Symbol (A-Z)</option>
          </select>
        </div>
      </div>
      <details style="margin-top:8px;">
        <summary class="small">Custom query JSON override (optional)</summary>
        <textarea id="mds-query-json" class="input" style="min-height:120px;font-family:monospace;margin-top:8px;" placeholder='{"operator":"and","operands":[{"operator":"eq","operands":["region","us"]}]}'></textarea>
      </details>
      <div class="hero-actions" style="margin-top:10px;">
        <button id="mds-run" class="cta secondary" type="button"><i class="iconoir-search" aria-hidden="true"></i><span>Run market data query</span></button>
        <button id="mds-append-top" class="cta secondary" type="button"><i class="iconoir-edit-pencil" aria-hidden="true"></i><span>Add top symbols to criteria</span></button>
        <button id="mds-clear" class="cta secondary" type="button"><i class="iconoir-erase" aria-hidden="true"></i><span>Clear</span></button>
      </div>
      <p id="mds-status" class="small muted" style="margin-top:8px;">Ready.</p>
      <div id="mds-output" class="panel-output" style="margin-top:8px;"></div>
    </details>
  `;

  const generateBtn = document.getElementById("screener-generate-button");
  if (generateBtn?.parentElement === screenerForm) {
    screenerForm.insertBefore(mount, generateBtn);
  } else {
    screenerForm.appendChild(mount);
  }

  const notesInput = document.getElementById("screener-notes");
  const presetInput = document.getElementById("mds-preset");
  const sizeInput = document.getElementById("mds-size");
  const regionInput = document.getElementById("mds-region");
  const exchangeInput = document.getElementById("mds-exchange");
  const marketCapMinInput = document.getElementById("mds-marketcap-min");
  const peMaxInput = document.getElementById("mds-pe-max");
  const volumeMinInput = document.getElementById("mds-volume-min");
  const sortInput = document.getElementById("mds-sort");
  const jsonInput = document.getElementById("mds-query-json");
  const runBtn = document.getElementById("mds-run");
  const appendBtn = document.getElementById("mds-append-top");
  const clearBtn = document.getElementById("mds-clear");
  const statusNode = document.getElementById("mds-status");
  const outputNode = document.getElementById("mds-output");

  let currentRows = [];

  const setStatus = (text, isError = false) => {
    statusNode.textContent = text;
    statusNode.style.color = isError ? "#d83446" : "";
  };

  const compactFmt = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 2 }).format(num);
  };

  const numberFmt = (value, digits = 2) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: digits,
    }).format(num);
  };

  const saveState = () => {
    const state = {
      preset: presetInput.value,
      size: sizeInput.value,
      region: regionInput.value,
      exchange: exchangeInput.value,
      marketCapMin: marketCapMinInput.value,
      peMax: peMaxInput.value,
      volumeMin: volumeMinInput.value,
      sort: sortInput.value,
      queryJson: jsonInput.value,
    };
    localStorage.setItem(STORAGE_SCREENER_STATE, JSON.stringify(state));
  };

  const loadState = () => {
    try {
      const saved = JSON.parse(localStorage.getItem(STORAGE_SCREENER_STATE) || "{}");
      if (!saved || typeof saved !== "object") return;
      if (typeof saved.preset === "string") presetInput.value = saved.preset;
      if (typeof saved.size === "string") sizeInput.value = saved.size;
      if (typeof saved.region === "string") regionInput.value = saved.region;
      if (typeof saved.exchange === "string") exchangeInput.value = saved.exchange;
      if (typeof saved.marketCapMin === "string") marketCapMinInput.value = saved.marketCapMin;
      if (typeof saved.peMax === "string") peMaxInput.value = saved.peMax;
      if (typeof saved.volumeMin === "string") volumeMinInput.value = saved.volumeMin;
      if (typeof saved.sort === "string") sortInput.value = saved.sort;
      if (typeof saved.queryJson === "string") jsonInput.value = saved.queryJson;
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
    return operands.length === 1 ? operands[0] : { operator: "and", operands };
  };

  const parsePayload = () => {
    const explicitJson = String(jsonInput.value || "").trim();
    if (explicitJson) {
      try {
        return {
          query: JSON.parse(explicitJson),
          size: Number(sizeInput.value || 20),
          offset: 0,
        };
      } catch (error) {
        throw new Error(`Invalid custom JSON: ${error.message || error}`);
      }
    }

    const filterQuery = buildFilterQuery();
    if (filterQuery) {
      return {
        query: filterQuery,
        size: Number(sizeInput.value || 20),
        offset: 0,
      };
    }

    return {
      preset: String(presetInput.value || "most_actives") || "most_actives",
      size: Number(sizeInput.value || 20),
      offset: 0,
    };
  };

  const sortRows = (rows) => {
    const [field, direction] = String(sortInput.value || "changePercent_desc").split("_");
    const sign = direction === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const leftNum = Number(a?.[field]);
      const rightNum = Number(b?.[field]);
      if (Number.isFinite(leftNum) && Number.isFinite(rightNum)) {
        return (leftNum - rightNum) * sign;
      }
      return String(a?.[field] || "").localeCompare(String(b?.[field] || "")) * sign;
    });
  };

  const renderRows = () => {
    if (!currentRows.length) {
      outputNode.innerHTML = '<div class="small muted">No results returned.</div>';
      return;
    }

    const rows = sortRows(currentRows).slice(0, 80);
    outputNode.innerHTML = `
      <div class="small muted" style="margin-bottom:8px;">${rows.length} symbols loaded</div>
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
            ${rows
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
                        <button type="button" class="task-chip" data-action="mds-add" data-symbol="${row.symbol}">Add</button>
                        <a class="task-chip" href="/ticker/${encodeURIComponent(row.symbol || "")}" data-action="mds-open">Open</a>
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

  const appendTopSymbolsToCriteria = () => {
    const top = sortRows(currentRows)
      .slice(0, 12)
      .map((row) => String(row?.symbol || "").trim().toUpperCase())
      .filter(Boolean);
    if (!top.length) {
      setStatus("Run market data query first.", true);
      return;
    }
    const previous = String(notesInput?.value || "").trim();
    const chip = `Top YFinance symbols: ${top.join(", ")}`;
    const next = previous ? `${previous}\n${chip}` : chip;
    if (notesInput) notesInput.value = next;
    setStatus(`Added ${top.length} symbols to screener criteria.`);
  };

  const runScreener = async () => {
    let payload;
    try {
      payload = parsePayload();
    } catch (error) {
      setStatus(error?.message || "Invalid screener request.", true);
      return;
    }

    const url = `${getApiBase()}/stocks/screener`;
    runBtn.disabled = true;
    outputNode.innerHTML = '<div class="small muted">Loading market data...</div>';
    setStatus("Running market-data query...");
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
      outputNode.innerHTML = '<div class="small muted">Unable to load market data right now.</div>';
      setStatus(error?.message || "Market data request failed.", true);
    } finally {
      runBtn.disabled = false;
    }
  };

  outputNode.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action='mds-add']");
    if (!button) return;
    const symbol = String(button.getAttribute("data-symbol") || "").trim().toUpperCase();
    if (!symbol) return;
    const previous = String(notesInput?.value || "").trim();
    const next = previous ? `${previous}, ${symbol}` : symbol;
    if (notesInput) notesInput.value = next;
    setStatus(`${symbol} added to criteria notes.`);
  });

  runBtn.addEventListener("click", runScreener);
  appendBtn.addEventListener("click", appendTopSymbolsToCriteria);
  clearBtn.addEventListener("click", () => {
    currentRows = [];
    outputNode.innerHTML = '<div class="small muted">Output cleared.</div>';
    setStatus("Cleared.");
  });
  [presetInput, sizeInput, regionInput, exchangeInput, marketCapMinInput, peMaxInput, volumeMinInput, sortInput, jsonInput].forEach((el) => {
    el?.addEventListener("change", saveState);
  });

  loadState();
})();
