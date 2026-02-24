(() => {
  const root = document.getElementById("fx-tool-root");
  if (!root) return;

  const STORAGE_BASE_KEY = "quantura_market_data_base_v1";
  const STORAGE_RECENT_KEY = "quantura_fx_recent_v1";
  const MAX_RECENT = 8;

  const form = document.getElementById("fx-convert-form");
  const amountInput = document.getElementById("fx-amount");
  const fromSelect = document.getElementById("fx-from");
  const toSelect = document.getElementById("fx-to");
  const swapButton = document.getElementById("fx-swap");
  const submitButton = document.getElementById("fx-submit");
  const statusNode = document.getElementById("fx-status");
  const resultNode = document.getElementById("fx-result");
  const recentNode = document.getElementById("fx-recent");
  const apiBaseInput = document.getElementById("fx-api-base");

  const readRecent = () => {
    try {
      const parsed = JSON.parse(localStorage.getItem(STORAGE_RECENT_KEY) || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  };

  const writeRecent = (items) => {
    try {
      localStorage.setItem(STORAGE_RECENT_KEY, JSON.stringify(items.slice(0, MAX_RECENT)));
    } catch {
      // Ignore storage failures.
    }
  };

  const numberFmt = (value, maxDigits = 6) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "-";
    return new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: maxDigits,
    }).format(numeric);
  };

  const renderRecent = () => {
    const items = readRecent();
    if (!items.length) {
      recentNode.innerHTML = '<div class="small muted">No recent conversions yet.</div>';
      return;
    }
    recentNode.innerHTML = items
      .map((item, index) => {
        const label = `${numberFmt(item.amountIn, 4)} ${item.from} -> ${numberFmt(item.amountOut, 4)} ${item.to}`;
        return `
          <button type="button" class="task-chip" data-fx-recent-index="${index}" style="margin: 4px 6px 4px 0;">
            ${label}
          </button>
        `;
      })
      .join("");
  };

  const getApiBase = () => {
    const explicit = String(window.__QUANTURA_MARKET_DATA_BASE__ || "").trim();
    const saved = String(localStorage.getItem(STORAGE_BASE_KEY) || "").trim();
    const fallback = "http://127.0.0.1:8090";
    return (explicit || saved || fallback).replace(/\/$/, "");
  };

  const setStatus = (text, isError = false) => {
    statusNode.textContent = text;
    statusNode.classList.toggle("error", Boolean(isError));
  };

  const setLoading = (loading) => {
    submitButton.disabled = loading;
    submitButton.querySelector("span").textContent = loading ? "Converting..." : "Convert";
  };

  const renderResult = (payload) => {
    if (!payload || typeof payload !== "object") {
      resultNode.innerHTML = '<div class="small muted">No result.</div>';
      return;
    }

    resultNode.innerHTML = `
      <div class="profile-item"><span class="label">Amount in</span><span class="value">${numberFmt(payload.amountIn, 6)} ${payload.from || ""}</span></div>
      <div class="profile-item"><span class="label">Rate</span><span class="value">${numberFmt(payload.rate, 8)}</span></div>
      <div class="profile-item"><span class="label">Amount out</span><span class="value">${numberFmt(payload.amountOut, 6)} ${payload.to || ""}</span></div>
      <div class="profile-item"><span class="label">Symbol</span><span class="value">${payload.symbolUsed || "-"}</span></div>
      <div class="profile-item"><span class="label">Updated</span><span class="value">${payload.asOf ? new Date(payload.asOf).toLocaleString() : "-"}</span></div>
      <div class="small muted" style="margin-top:8px;">Source: ${payload.source || "yfinance"}</div>
    `;
  };

  const pushRecent = (payload) => {
    const existing = readRecent();
    const filtered = existing.filter(
      (row) =>
        !(
          String(row.from || "") === String(payload.from || "") &&
          String(row.to || "") === String(payload.to || "") &&
          Number(row.amountIn || 0) === Number(payload.amountIn || 0)
        )
    );
    filtered.unshift(payload);
    writeRecent(filtered);
    renderRecent();
  };

  const runConversion = async () => {
    const amount = Number(amountInput.value);
    const from = String(fromSelect.value || "").trim().toUpperCase();
    const to = String(toSelect.value || "").trim().toUpperCase();

    if (!Number.isFinite(amount) || amount < 0) {
      setStatus("Enter a valid amount.", true);
      return;
    }
    if (!from || !to) {
      setStatus("Select both currencies.", true);
      return;
    }

    const base = getApiBase();
    const url = new URL(`${base}/fx/convert`);
    url.searchParams.set("amount", String(amount));
    url.searchParams.set("from", from);
    url.searchParams.set("to", to);

    setLoading(true);
    setStatus("Requesting latest FX quote...");
    try {
      const response = await fetch(url.toString(), {
        method: "GET",
        headers: { Accept: "application/json" },
      });

      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const detail = String(payload?.detail || `HTTP ${response.status}`);
        throw new Error(detail);
      }

      renderResult(payload);
      pushRecent(payload);
      setStatus(`Converted via ${payload.symbolUsed || "Yahoo FX"}.`);
    } catch (error) {
      setStatus(error?.message || "Conversion failed.", true);
      resultNode.innerHTML = '<div class="small muted">Unable to fetch conversion right now. Check endpoint and retry.</div>';
    } finally {
      setLoading(false);
    }
  };

  if (apiBaseInput) {
    apiBaseInput.value = getApiBase();
    apiBaseInput.addEventListener("change", () => {
      const raw = String(apiBaseInput.value || "").trim().replace(/\/$/, "");
      if (!raw) {
        localStorage.removeItem(STORAGE_BASE_KEY);
        apiBaseInput.value = getApiBase();
        return;
      }
      localStorage.setItem(STORAGE_BASE_KEY, raw);
    });
  }

  swapButton?.addEventListener("click", () => {
    const from = fromSelect.value;
    fromSelect.value = toSelect.value;
    toSelect.value = from;
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    await runConversion();
  });

  recentNode?.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-fx-recent-index]");
    if (!button) return;
    const index = Number(button.getAttribute("data-fx-recent-index"));
    const item = readRecent()[index];
    if (!item) return;
    amountInput.value = String(item.amountIn || 1);
    fromSelect.value = String(item.from || "USD");
    toSelect.value = String(item.to || "USD");
    await runConversion();
  });

  renderRecent();
})();
