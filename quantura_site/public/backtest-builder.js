/** Loaded only when the forecast page's Backtest button is opened. */
let initialized = false;
let currentResult = null;
const byId = id => document.getElementById(id);
const safe = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const money = value => new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(Number(value) || 0);
const percent = value => value == null ? "—" : `${Number(value).toFixed(2)}%`;

function updateSource() {
  const source = window.QuanturaBacktestBridge?.source();
  const summary = byId("backtest-source-summary");
  const button = byId("backtest-run");
  const phase = byId("backtest-history-phase");
  if (!summary || !button) return;
  if (!source) {
    summary.textContent = "Select a stock ticker or an individual Kalshi / Polymarket US contract in Q Search first. Uploaded CSVs are not supported by this historical provider backtest.";
    button.disabled = true;
    return;
  }
  summary.textContent = source.type === "ticker" ? `${source.symbol} · ${source.provider || "auto"} market history` : `${source.symbol} · ${source.provider} · selected contract ${source.contract_id}`;
  phase.closest(".field").hidden = source.type !== "prediction_market";
  button.disabled = false;
}

function chart(points) {
  const host = byId("backtest-equity");
  if (!host || !points?.length) return;
  const values = points.map(point => Number(point.equity));
  const low = Math.min(...values);
  const high = Math.max(...values);
  const span = Math.max(high - low, Math.abs(high) * .005, 1);
  const path = values.map((value, index) => `${index ? "L" : "M"}${(index * 800 / Math.max(values.length - 1, 1)).toFixed(2)},${(170 - (value - low) / span * 140).toFixed(2)}`).join(" ");
  host.innerHTML = `<div class="small muted">Equity · ${money(values[0])} → ${money(values.at(-1))}</div><svg viewBox="0 0 800 190" role="img" aria-label="Simulated equity curve from ${safe(money(values[0]))} to ${safe(money(values.at(-1)))}" preserveAspectRatio="none"><path d="${path}" fill="none" stroke="currentColor" stroke-width="3" vector-effect="non-scaling-stroke" /></svg>`;
  host.hidden = false;
}

function render(result) {
  currentResult = result;
  const metrics = result.metrics || {};
  byId("backtest-result-id").textContent = result.id || "";
  byId("backtest-status").textContent = `${metrics.observed_bars ?? 0} observed bars · ${result.provider} · ${result.fill_model === "next_observed_bar_open" ? "next-bar-open model" : "display-quote proxy (not executable)"}. Historical simulation only.`;
  const entries = [
    ["Trades", metrics.trades], ["Wins / losses", `${metrics.wins} / ${metrics.losses}`],
    ["Win rate", percent(metrics.win_rate_pct)], ["Net P&L", money(metrics.net_pnl)],
    ["Return", percent(metrics.return_pct)], ["Max drawdown", money(metrics.max_drawdown)],
    ["Fees", money(metrics.fees)], ["Win / loss streak", `${metrics.longest_winning_streak} / ${metrics.longest_losing_streak}`],
  ];
  byId("backtest-metrics").innerHTML = entries.map(([label, value]) => `<div><span>${safe(label)}</span><strong>${safe(value)}</strong></div>`).join("");
  byId("backtest-metrics").hidden = false;
  chart(result.equity_curve);
  const trades = Array.isArray(result.trades) ? result.trades : [];
  byId("backtest-trades").innerHTML = trades.length
    ? `<div class="small muted">${trades.length} completed trade${trades.length === 1 ? "" : "s"}</div><div class="backtest-table-wrap"><table><thead><tr><th>Entry</th><th>Exit</th><th>Shares</th><th>P&L</th><th>Reason</th></tr></thead><tbody>${trades.slice(0, 50).map(trade => `<tr><td>${safe(new Date(trade.entry_at).toLocaleString())}</td><td>${safe(new Date(trade.exit_at).toLocaleString())}</td><td>${safe(trade.shares)}</td><td>${safe(money(trade.pnl))}</td><td>${safe(trade.reason)}</td></tr>`).join("")}</tbody></table></div>${trades.length > 50 ? '<p class="small muted">Showing first 50 trades. Download JSON for all trades.</p>' : ""}`
    : '<p class="small muted">No entry met the strategy rules in this history window. No trades were invented.</p>';
  byId("backtest-trades").hidden = false;
  byId("backtest-download").hidden = false;
  byId("backtest-strategy-download").hidden = false;
}

function download(name, value) {
  const link = document.createElement("a");
  const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: "application/json" }));
  link.href = url;
  link.download = name;
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 2000);
}

function init() {
  if (initialized) return;
  initialized = true;
  const dialog = byId("backtest-dialog");
  byId("backtest-close")?.addEventListener("click", () => dialog.close());
  dialog?.addEventListener("click", event => { if (event.target === dialog) dialog.close(); });
  window.addEventListener("quantura:market-selected", updateSource);
  byId("backtest-reset")?.addEventListener("click", () => {
    for (const [id, value] of Object.entries({ "backtest-fast": 10, "backtest-slow": 30, "backtest-capital": 1000,
      "backtest-fraction": 100, "backtest-commission": 10, "backtest-slippage": 5 })) byId(id).value = value;
    byId("backtest-end").value = "";
    byId("backtest-history-phase").value = "both";
  });
  byId("backtest-download")?.addEventListener("click", () => { if (currentResult) download(`${currentResult.id}.json`, currentResult); });
  byId("backtest-strategy-download")?.addEventListener("click", async () => {
    if (!currentResult) return;
    try {
      const response = await window.QuanturaBacktestBridge.request(`/api/v1/backtests/${encodeURIComponent(currentResult.id)}/strategy`);
      download(`${currentResult.id}-strategy.json`, response.data);
    } catch (error) { byId("backtest-status").textContent = error.message; }
  });
  byId("backtest-form")?.addEventListener("submit", async event => {
    event.preventDefault();
    const bridge = window.QuanturaBacktestBridge;
    const button = byId("backtest-run");
    const status = byId("backtest-status");
    button.disabled = true;
    status.textContent = "Loading observed history → Simulating completed-bar decisions → Saving result…";
    try {
      await bridge.ensureSession();
      const source = bridge.source();
      if (!source) throw new Error("Select a supported market in Q Search first.");
      const frequency = byId("backtest-frequency").value;
      source.frequency = source.type === "ticker" ? frequency : ({ "1Day": "1D", "1Hour": "1h", "1Min": "1min" })[frequency];
      if (source.type === "prediction_market") source.history_phase = byId("backtest-history-phase").value;
      const end = byId("backtest-end").value;
      if (end) {
        const time = new Date(end);
        if (!Number.isFinite(time.getTime()) || time.getTime() > Date.now()) throw new Error("Choose a valid past cutoff in your local timezone.");
        source.end = time.toISOString();
      }
      const body = { workspace_id: bridge.workspaceId(), source,
        strategy: { schema_version: 1, type: "sma_crossover", fast_period: Number(byId("backtest-fast").value), slow_period: Number(byId("backtest-slow").value) },
        execution: { starting_capital: Number(byId("backtest-capital").value), position_fraction: Number(byId("backtest-fraction").value) / 100,
          commission_bps: Number(byId("backtest-commission").value), slippage_bps: Number(byId("backtest-slippage").value) } };
      const response = await bridge.request("/api/v1/backtests", { method: "POST", body });
      render(response.data);
    } catch (error) {
      status.textContent = `${error.message || "Backtest failed."}${error.code ? ` (${error.code})` : ""}${error.requestId ? ` · Reference ${error.requestId}` : ""}`;
    } finally { button.disabled = !bridge.source(); }
  });
}

export function openBacktest() {
  init();
  const source = window.QuanturaBacktestBridge?.source();
  const mainFrequency = source?.type === "prediction_market" ? byId("ensemble-market-frequency")?.value : byId("ensemble-ticker-frequency")?.value;
  byId("backtest-frequency").value = ({ "1D": "1Day", "1h": "1Hour", "1min": "1Min" })[mainFrequency] || mainFrequency || "1Day";
  updateSource();
  byId("backtest-dialog")?.showModal();
  byId("backtest-close")?.focus();
}
