/** Quantile-rule research builder. Loaded only when the Backtest dialog opens. */
let initialized = false;
let capabilities = null;
let currentResult = null;
let currentJobId = "";
let pollTimer = 0;
let ruleSequence = 4;
let rules = [];
const byId = id => document.getElementById(id);
const bridge = () => window.QuanturaBacktestBridge;
const safe = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const money = value => new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(Number(value) || 0);
const percent = value => value == null ? "—" : `${Number(value).toFixed(2)}%`;
const qLabel = value => Math.abs(Number(value) * 100 - Math.round(Number(value) * 100)) < 1e-9 ? `P${Math.round(Number(value) * 100).toString().padStart(2, "0")}` : `Q${Number(value)}`;

function selectedQuantiles() {
  const raw = String(byId("backtest-quantiles")?.value || "").split(/[\s,;]+/).filter(Boolean);
  if (!raw.length) throw new Error("Choose at least one forecast quantile.");
  const values = [...new Set(raw.map(value => Number(value)))].sort((a, b) => a - b);
  if (values.length > 21 || values.some(value => !Number.isFinite(value) || value <= 0 || value >= 1)) {
    throw new Error("Use at most 21 unique decimal quantiles strictly between 0 and 1.");
  }
  return values;
}

function quantileOptions(selected) {
  let levels = [];
  try { levels = selectedQuantiles(); } catch { levels = []; }
  if (Number.isFinite(selected) && !levels.some(level => Math.abs(level - selected) < 1e-12)) {
    return `<option value="${safe(selected)}" selected disabled>${safe(qLabel(selected))} · add to forecast quantiles</option>` +
      levels.map(level => `<option value="${safe(level)}">${safe(qLabel(level))}</option>`).join("");
  }
  return levels.map(level => `<option value="${safe(level)}" ${Math.abs(level - selected) < 1e-12 ? "selected" : ""}>${safe(qLabel(level))}</option>`).join("");
}

function defaultRules() {
  rules = [
    { id: "entry_p10", kind: "entry", condition: "crosses_above", quantile: .1 },
    { id: "take_p50", kind: "take_profit", target_mode: "quantile", quantile: .5 },
    { id: "stop_p01", kind: "stop_loss", target_mode: "quantile", quantile: .01 },
  ];
}

function renderRules() {
  const host = byId("backtest-rules");
  if (!host) return;
  host.innerHTML = rules.map((rule, index) => {
    const title = { entry: "Entry", take_profit: "Take-profit", stop_loss: "Stop-loss", trailing_stop: "Trailing stop" }[rule.kind];
    const remove = `<button type="button" class="task-chip" data-remove-rule="${index}" aria-label="Remove ${title} block">Remove</button>`;
    if (rule.kind === "entry") return `<div class="backtest-rule" data-rule-index="${index}"><strong>${title}</strong>
      <div class="field"><label for="bt-condition-${index}">When observed close</label><select id="bt-condition-${index}" data-rule-field="condition"><option value="crosses_above" ${rule.condition === "crosses_above" ? "selected" : ""}>Crosses upward into</option><option value="crosses_below" ${rule.condition === "crosses_below" ? "selected" : ""}>Crosses downward into</option><option value="at_or_above" ${rule.condition === "at_or_above" ? "selected" : ""}>At or above</option><option value="at_or_below" ${rule.condition === "at_or_below" ? "selected" : ""}>At or below</option></select></div>
      <div class="field"><label for="bt-quantile-${index}">Forecast quantile</label><select id="bt-quantile-${index}" data-rule-field="quantile">${quantileOptions(rule.quantile)}</select></div>${remove}</div>`;
    if (rule.kind === "trailing_stop") return `<div class="backtest-rule" data-rule-index="${index}"><strong>${title}</strong><div class="field"><label for="bt-trail-${index}">Below highest observed close (%)</label><input id="bt-trail-${index}" data-rule-field="percent" type="number" min="0.1" max="90" step="0.1" value="${safe(rule.percent)}" /></div><span class="small muted">Checked at completed closes</span>${remove}</div>`;
    const mode = `<div class="field"><label for="bt-mode-${index}">Target type</label><select id="bt-mode-${index}" data-rule-field="target_mode"><option value="quantile" ${rule.target_mode === "quantile" ? "selected" : ""}>Forecast quantile</option><option value="percent" ${rule.target_mode === "percent" ? "selected" : ""}>From entry (%)</option></select></div>`;
    const target = rule.target_mode === "quantile"
      ? `<div class="field"><label for="bt-target-${index}">Forecast quantile</label><select id="bt-target-${index}" data-rule-field="quantile">${quantileOptions(rule.quantile)}</select></div>`
      : `<div class="field"><label for="bt-target-${index}">From entry (%)</label><input id="bt-target-${index}" data-rule-field="percent" type="number" min="0.1" max="100" step="0.1" value="${safe(rule.percent)}" /></div>`;
    return `<div class="backtest-rule" data-rule-index="${index}"><strong>${title}</strong>${mode}${target}${remove}</div>`;
  }).join("");
  validateUi(false);
}

function addRule(kind) {
  if (rules.length >= 10) { byId("backtest-rule-status").textContent = "At most 10 rule blocks can be stacked."; return; }
  const id = `rule_${ruleSequence++}`;
  if (kind === "entry") rules.push({ id, kind, condition: "at_or_below", quantile: .25 });
  else if (kind === "trailing_stop") rules.push({ id, kind, percent: 10 });
  else if (kind === "take_profit" || kind === "stop_loss") {
    const levels = selectedQuantiles();
    rules.push({ id, kind, target_mode: "quantile", quantile: kind === "take_profit" ? levels.at(-1) : levels[0] });
  }
  renderRules();
}

function selectedModels() {
  const output = {};
  for (const model of capabilities?.models || []) {
    const card = byId(`backtest-model-${model.id}`);
    const enabled = Boolean(card?.querySelector('[data-model-enabled]')?.checked) && Boolean(model.available);
    const weight = Number(card?.querySelector('[data-model-weight]')?.value || 0);
    output[model.id] = { enabled, weight: enabled ? weight : 0 };
  }
  return output;
}

function validateUi(show = true) {
  try {
    const quantiles = selectedQuantiles();
    const models = selectedModels();
    const enabled = (capabilities?.models || []).filter(model => models[model.id]?.enabled && models[model.id]?.weight > 0);
    if (!enabled.length) throw new Error("Enable at least one available model with positive weight.");
    if (Object.values(models).some(model => !Number.isFinite(model.weight) || model.weight < 0)) throw new Error("Model weights must be finite and nonnegative.");
    for (const q of quantiles) if (!enabled.some(model => {
      const support = model.quantile_support || {};
      return !support.minimum || (q >= Number(support.minimum) && q <= Number(support.maximum));
    })) throw new Error(`${qLabel(q)} is unavailable from the enabled models.`);
    const context = Number(byId("backtest-context")?.value);
    const windows = Number(byId("backtest-windows")?.value);
    const horizon = Number(byId("backtest-horizon")?.value);
    if (!Number.isInteger(context) || context < 2 || context > 500 || !Number.isInteger(windows) || windows < 1 || windows > 8 ||
        !Number.isInteger(horizon) || horizon < 1 || horizon > 60 || windows * horizon > 240 || windows * enabled.length > 20) {
      throw new Error("Use 2–500 input bars, 1–8 windows, 1–60 steps, and no more than 20 model-window runs or 240 evaluated steps.");
    }
    if (enabled.some(model => context < Number(model.minimum_observed_context || 2))) throw new Error("Increase observed input bars to meet the enabled models' minimum history.");
    const entries = rules.filter(rule => rule.kind === "entry");
    const exits = rules.filter(rule => rule.kind !== "entry");
    if (!entries.length || entries.length > 4 || !exits.length || exits.length > 6) throw new Error("Stack 1–4 entry and 1–6 exit blocks.");
    if (exits.filter(rule => rule.kind === "trailing_stop").length > 1) throw new Error("Only one trailing stop may be active.");
    if (rules.some(rule => "quantile" in rule && !quantiles.some(q => Math.abs(q - rule.quantile) < 1e-12))) throw new Error("Add each block's quantile to the forecast quantiles or change the block.");
    if (byId("backtest-entry-logic").value === "all") {
      const upper = entries.filter(rule => ["crosses_above", "at_or_above"].includes(rule.condition));
      const lower = entries.filter(rule => ["crosses_below", "at_or_below"].includes(rule.condition));
      if (upper.some(a => lower.some(b => a.quantile >= b.quantile))) throw new Error("ALL entry blocks contain incompatible upper and lower quantile conditions.");
    }
    const lowest = Math.min(...entries.map(rule => rule.quantile)), highest = Math.max(...entries.map(rule => rule.quantile));
    if (exits.some(rule => rule.target_mode === "quantile" && (rule.kind === "take_profit" ? rule.quantile <= highest : rule.quantile >= lowest))) {
      throw new Error("For a long position, quantile take-profits must exceed every entry quantile and quantile stops must be below every entry quantile.");
    }
    if (rules.some(rule => "percent" in rule && (!Number.isFinite(rule.percent) || rule.percent < .1 || rule.percent > (rule.kind === "trailing_stop" ? 90 : 100)))) throw new Error("Exit percentages must be positive and within their allowed range.");
    const signatures = new Set(rules.map(rule => JSON.stringify({ ...rule, id: undefined })));
    if (signatures.size !== rules.length) throw new Error("Remove duplicate rule blocks.");
    if (show) byId("backtest-rule-status").textContent = "Rules are compatible. The backend validates them again before compute.";
    return { quantiles, models, context, windows, horizon };
  } catch (error) {
    if (show) byId("backtest-rule-status").textContent = error.message;
    return null;
  }
}

function selectedSource() {
  const symbol = String(byId("backtest-ticker")?.value || "").trim().toUpperCase();
  if (symbol) {
    if (!/^[A-Z0-9.^=-]{1,20}$/.test(symbol)) throw new Error("Enter a valid stock ticker (up to 20 letters, numbers, or ticker symbols).");
    const pageSource = bridge()?.source();
    return { type: "ticker", symbol, provider: pageSource?.type === "ticker" && pageSource.symbol === symbol ? pageSource.provider || "auto" : "auto" };
  }
  const pageSource = bridge()?.source();
  return pageSource?.type === "prediction_market" ? pageSource : null;
}

function updateSource() {
  const summary = byId("backtest-source-summary"), button = byId("backtest-run"), phase = byId("backtest-history-phase");
  if (!summary || !button) return;
  let source;
  try { source = selectedSource(); }
  catch (error) {
    summary.textContent = error.message;
    button.disabled = true;
    return;
  }
  if (!source) {
    summary.textContent = "Enter a stock ticker above or select an individual Kalshi / Polymarket US contract in Q Search. Uploaded CSVs are not supported here.";
    button.disabled = true;
    return;
  }
  summary.textContent = source.type === "ticker" ? `${source.symbol} · ${source.provider || "auto"} market history` :
    `${source.symbol} · ${source.provider} · selected contract ${source.contract_id}`;
  phase.closest(".field").hidden = source.type !== "prediction_market";
  button.disabled = false;
}

function syncSelectedMarket() {
  const source = bridge()?.source();
  byId("backtest-ticker").value = source?.type === "ticker" ? source.symbol : "";
  updateSource();
}

function renderModels(defaults = {}) {
  const host = byId("backtest-models");
  host.innerHTML = (capabilities?.models || []).map(model => {
    const selected = defaults.models?.[model.id];
    const enabled = model.available && (selected ? Boolean(selected.enabled) : model.id === "prophet");
    const weight = Number(selected?.weight ?? model.default_weight ?? .2);
    const availability = model.available ? "Available" : String(model.unavailable_reason || "Unavailable").replaceAll("_", " ");
    return `<div class="backtest-model" id="backtest-model-${safe(model.id)}" aria-disabled="${!model.available}"><label><input type="checkbox" data-model-enabled ${enabled ? "checked" : ""} ${model.available ? "" : "disabled"}/><span>${safe(model.name)}</span></label><span class="small muted">${safe(availability)} · ${model.quantile_support?.minimum ? "P10–P90" : "requested quantiles"}</span><label class="small">Weight<input type="number" data-model-weight min="0" step="0.01" value="${safe(Number.isFinite(weight) ? weight : 0)}" ${model.available ? "" : "disabled"}/></label></div>`;
  }).join("");
  const toto = (capabilities?.models || []).find(model => model.id === "toto");
  const variant = byId("backtest-toto-variant");
  variant.innerHTML = (toto?.variants || []).map(item => `<option value="${safe(item.id)}" ${item.id === "4m" ? "selected" : ""}>${safe(item.name)}</option>`).join("");
  variant.disabled = !toto?.available;
  byId("backtest-capabilities").textContent = "Weights are normalized per quantile. Toto and TimesFM do not contribute outside P10–P90. The 4M Toto checkpoint is the backtest default; larger sizes take longer.";
  validateUi(false);
}

async function loadCapabilities() {
  const session = bridge();
  await session.ensureSession();
  const response = await session.request(`/api/v1/ensemble-forecasts/models?workspace_id=${encodeURIComponent(session.workspaceId())}`);
  capabilities = response.data;
  renderModels(session.forecastDefaults?.() || {});
}

function chart(points) {
  const host = byId("backtest-equity");
  if (!host || !points?.length) return;
  const values = points.map(point => Number(point.equity));
  const low = Math.min(...values), high = Math.max(...values);
  const span = Math.max(high - low, Math.abs(high) * .005, 1);
  const path = values.map((value, index) => `${index ? "L" : "M"}${(index * 800 / Math.max(values.length - 1, 1)).toFixed(2)},${(170 - (value - low) / span * 140).toFixed(2)}`).join(" ");
  host.innerHTML = `<div class="small muted">Equity · ${safe(money(values[0]))} → ${safe(money(values.at(-1)))}</div><svg viewBox="0 0 800 190" role="img" aria-label="Simulated equity curve from ${safe(money(values[0]))} to ${safe(money(values.at(-1)))}" preserveAspectRatio="none"><path d="${path}" fill="none" stroke="currentColor" stroke-width="3" vector-effect="non-scaling-stroke" /></svg>`;
  host.hidden = false;
}

function render(result) {
  currentResult = result;
  const metrics = result.metrics || {};
  byId("backtest-result-id").textContent = result.id || result.backtest_id || "";
  byId("backtest-status").textContent = `${metrics.forecast_windows ?? 0} walk-forward forecasts · ${metrics.matched_forecast_bars ?? 0} timestamp-matched later bars · ${result.provider} · ${result.fill_model === "next_observed_bar_open" ? "next-bar-open model" : "display-quote proxy (not executable)"}. Historical simulation only.`;
  const entries = [["Trades", metrics.trades], ["Wins / losses", `${metrics.wins} / ${metrics.losses}`], ["Win rate", percent(metrics.win_rate_pct)],
    ["Net P&L", money(metrics.net_pnl)], ["Return", percent(metrics.return_pct)], ["Max drawdown", money(metrics.max_drawdown)],
    ["Fees", money(metrics.fees)], ["Win / loss streak", `${metrics.longest_winning_streak} / ${metrics.longest_losing_streak}`]];
  byId("backtest-metrics").innerHTML = entries.map(([label, value]) => `<div><span>${safe(label)}</span><strong>${safe(value)}</strong></div>`).join("");
  byId("backtest-metrics").hidden = false;
  chart(result.equity_curve);
  const windows = Array.isArray(result.forecast_windows) ? result.forecast_windows : [];
  byId("backtest-forecast-windows").innerHTML = `<h4>Produced ensemble forecasts</h4><div class="backtest-table-wrap"><table><thead><tr><th>Window</th><th>History cutoff</th><th>Input bars</th><th>Matched outcomes</th><th>Models</th><th>First forecast quantiles</th></tr></thead><tbody>${windows.map(window => {
    const first = window.predictions?.[0]?.quantiles || {};
    const levels = Object.entries(first).map(([q, value]) => `${qLabel(q)} ${Number(value).toFixed(4)}`).join(" · ");
    const models = (window.participating_models || []).filter(model => model.status === "completed").map(model => model.id).join(", ");
    return `<tr><td>${safe(window.number)}</td><td>${safe(new Date(window.cutoff_at).toLocaleString())}</td><td>${safe(window.training_rows)}</td><td>${safe(window.matched_bars)}</td><td>${safe(models)}</td><td>${safe(levels)}</td></tr>`;
  }).join("")}</tbody></table></div>`;
  byId("backtest-forecast-windows").hidden = !windows.length;
  const trades = Array.isArray(result.trades) ? result.trades : [];
  byId("backtest-trades").innerHTML = trades.length
    ? `<h4>Simulated trades</h4><div class="backtest-table-wrap"><table><thead><tr><th>Entry</th><th>Exit</th><th>Shares</th><th>P&L</th><th>Rule</th></tr></thead><tbody>${trades.slice(0, 50).map(trade => `<tr><td>${safe(new Date(trade.entry_at).toLocaleString())}</td><td>${safe(new Date(trade.exit_at).toLocaleString())}</td><td>${safe(trade.shares)}</td><td>${safe(money(trade.pnl))}</td><td>${safe(trade.exit_rule_id)}</td></tr>`).join("")}</tbody></table></div>${trades.length > 50 ? '<p class="small muted">Showing first 50 trades. Download JSON for all trades.</p>' : ""}`
    : '<p class="small muted">No rule produced a completed trade in the matched forecast windows. No trades were invented.</p>';
  byId("backtest-trades").hidden = false;
  byId("backtest-download").hidden = false;
  byId("backtest-strategy-download").hidden = false;
}

function download(name, value) {
  const link = document.createElement("a");
  const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: "application/json" }));
  link.href = url; link.download = name; document.body.append(link); link.click(); link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 2000);
}

async function poll() {
  if (!currentJobId) return;
  try {
    const response = await bridge().request(`/api/v1/backtests/${encodeURIComponent(currentJobId)}`);
    const data = response.data || {};
    byId("backtest-result-id").textContent = currentJobId;
    if (data.status === "completed") { render(data); return; }
    if (data.status === "failed") {
      byId("backtest-status").textContent = `Backtest failed (${data.error?.code || "BACKTEST_WORKER_FAILED"}). No trades or returns are shown from an incomplete run.`;
      return;
    }
    const progress = data.progress || {};
    byId("backtest-status").textContent = `${data.status === "queued" ? "Queued for the forecast worker" : "Running walk-forward forecasts"} · ${progress.completed_windows ?? 0}/${progress.total_windows ?? "?"} windows completed${progress.current_window ? ` · window ${progress.current_window}` : ""}. This can take minutes; you may reopen this backtest later.`;
  } catch (error) {
    byId("backtest-status").textContent = `Status temporarily unavailable: ${error.message}. Retrying…`;
  }
  clearTimeout(pollTimer);
  pollTimer = setTimeout(poll, 5000);
}

function init() {
  if (initialized) return;
  initialized = true;
  defaultRules();
  renderRules();
  const dialog = byId("backtest-dialog");
  byId("backtest-close")?.addEventListener("click", () => dialog.close());
  dialog?.addEventListener("click", event => { if (event.target === dialog) dialog.close(); });
  window.addEventListener("quantura:market-selected", syncSelectedMarket);
  byId("backtest-ticker")?.addEventListener("input", updateSource);
  byId("backtest-rules")?.addEventListener("click", event => {
    const remove = event.target.closest("[data-remove-rule]");
    if (!remove) return;
    rules.splice(Number(remove.dataset.removeRule), 1);
    renderRules();
  });
  byId("backtest-rules")?.addEventListener("change", event => {
    const field = event.target.dataset.ruleField;
    const index = Number(event.target.closest("[data-rule-index]")?.dataset.ruleIndex);
    if (!field || !rules[index]) return;
    const value = field === "quantile" || field === "percent" ? Number(event.target.value) : event.target.value;
    if (field === "target_mode") {
      const old = rules[index];
      rules[index] = value === "percent" ? { id: old.id, kind: old.kind, target_mode: "percent", percent: 10 } :
        { id: old.id, kind: old.kind, target_mode: "quantile", quantile: old.kind === "take_profit" ? .5 : .01 };
      renderRules();
    } else { rules[index][field] = value; validateUi(true); }
  });
  byId("backtest-rules")?.addEventListener("input", event => {
    if (event.target.dataset.ruleField !== "percent") return;
    const index = Number(event.target.closest("[data-rule-index]")?.dataset.ruleIndex);
    if (rules[index]) { rules[index].percent = Number(event.target.value); validateUi(true); }
  });
  document.querySelectorAll("[data-add-backtest-rule]").forEach(button => button.addEventListener("click", () => addRule(button.dataset.addBacktestRule)));
  byId("backtest-quantiles")?.addEventListener("change", renderRules);
  ["backtest-context", "backtest-horizon", "backtest-windows", "backtest-entry-logic"].forEach(id => byId(id)?.addEventListener("change", () => validateUi(true)));
  byId("backtest-models")?.addEventListener("input", () => validateUi(true));
  byId("backtest-models")?.addEventListener("change", () => validateUi(true));
  byId("backtest-reset")?.addEventListener("click", () => {
    for (const [id, value] of Object.entries({ "backtest-horizon": 30, "backtest-windows": 2, "backtest-context": 128,
      "backtest-capital": 1000, "backtest-fraction": 100, "backtest-commission": 10, "backtest-slippage": 5 })) byId(id).value = value;
    byId("backtest-end").value = "";
    byId("backtest-history-phase").value = "both";
    byId("backtest-entry-logic").value = "all";
    byId("backtest-failure-policy").value = "fail";
    byId("backtest-quantiles").value = "0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99";
    defaultRules(); renderRules();
    if (capabilities) renderModels({});
  });
  byId("backtest-download")?.addEventListener("click", () => { if (currentResult) download(`${currentJobId}.json`, currentResult); });
  byId("backtest-strategy-download")?.addEventListener("click", async () => {
    if (!currentResult) return;
    try {
      const response = await bridge().request(`/api/v1/backtests/${encodeURIComponent(currentJobId)}/strategy`);
      download(`${currentJobId}-strategy.json`, response.data);
    } catch (error) { byId("backtest-status").textContent = error.message; }
  });
  byId("backtest-form")?.addEventListener("submit", async event => {
    event.preventDefault();
    const button = byId("backtest-run"), status = byId("backtest-status"), session = bridge();
    const validated = validateUi(true);
    if (!validated) return;
    button.disabled = true;
    status.textContent = "Validating source and observed bars → Creating durable quantile backtest…";
    try {
      await session.ensureSession();
      const source = selectedSource();
      if (!source) throw new Error("Enter a stock ticker or select a supported market in Q Search first.");
      const frequency = byId("backtest-frequency").value;
      source.frequency = source.type === "ticker" ? frequency : ({ "1Day": "1D", "1Hour": "1h", "1Min": "1min" })[frequency];
      if (source.type === "prediction_market") source.history_phase = byId("backtest-history-phase").value;
      const end = byId("backtest-end").value;
      if (end) {
        const time = new Date(end);
        if (!Number.isFinite(time.getTime()) || time.getTime() > Date.now()) throw new Error("Choose a valid past cutoff in your local timezone.");
        source.end = time.toISOString();
      }
      const body = { workspace_id: session.workspaceId(), source,
        forecast: { prediction_length: validated.horizon, quantiles: validated.quantiles, models: validated.models,
          toto_variant: byId("backtest-toto-variant").value || "4m", failure_policy: byId("backtest-failure-policy").value },
        replay: { context_rows: validated.context, evaluation_windows: validated.windows },
        strategy: { schema_version: 2, type: "quantile_rules", entry_logic: byId("backtest-entry-logic").value,
          rules: rules.map(rule => ({ ...rule })) },
        execution: { starting_capital: Number(byId("backtest-capital").value), position_fraction: Number(byId("backtest-fraction").value) / 100,
          commission_bps: Number(byId("backtest-commission").value), slippage_bps: Number(byId("backtest-slippage").value) } };
      const response = await session.request("/api/v1/backtests", { method: "POST", body });
      currentJobId = response.data.backtest_id;
      currentResult = null;
      ["backtest-metrics", "backtest-equity", "backtest-forecast-windows", "backtest-trades", "backtest-download", "backtest-strategy-download"].forEach(id => { byId(id).hidden = true; });
      await poll();
    } catch (error) {
      status.textContent = `${error.message || "Backtest could not be started."}${error.code ? ` (${error.code})` : ""}${error.requestId ? ` · Reference ${error.requestId}` : ""}`;
    } finally { updateSource(); }
  });
}

export function openBacktest() {
  init();
  const source = bridge()?.source();
  const mainFrequency = source?.type === "prediction_market" ? byId("ensemble-market-frequency")?.value : byId("ensemble-ticker-frequency")?.value;
  byId("backtest-frequency").value = ({ "1D": "1Day", "1h": "1Hour", "1min": "1Min" })[mainFrequency] || mainFrequency || "1Day";
  syncSelectedMarket();
  byId("backtest-dialog")?.showModal();
  byId("backtest-close")?.focus();
  if (!capabilities) loadCapabilities().catch(error => { byId("backtest-capabilities").textContent = error.message || "Model capabilities are unavailable."; });
  if (currentJobId && !currentResult) void poll();
}
