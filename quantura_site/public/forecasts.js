(() => {
  "use strict";

  const escapeHtml = (value) => String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");

  const formatPercent = (value, digits = 0) => {
    const number = Number(value);
    return Number.isFinite(number) ? `${(number * 100).toFixed(digits)}%` : "Unavailable";
  };

  const formatDelta = (value) => {
    const number = Number(value);
    if (!Number.isFinite(number) || Math.abs(number) < 0.00005) return "No published change";
    const points = number * 100;
    return `${points > 0 ? "↑" : "↓"} ${Math.abs(points).toFixed(1)} percentage points`;
  };

  const formatDate = (value, withTime = false) => {
    const date = new Date(String(value || ""));
    if (Number.isNaN(date.getTime())) return "Unavailable";
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      ...(withTime ? { hour: "numeric", minute: "2-digit", timeZoneName: "short" } : {}),
    }).format(date);
  };

  const label = (value) => String(value || "").replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());

  const api = async (path) => {
    const response = await fetch(path, { headers: { Accept: "application/json" }, credentials: "same-origin" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.error?.message || "Forecast data is temporarily unavailable.");
    return payload;
  };

  const statusLabel = (status) => ({
    pending: "Unresolved",
    resolved_yes: "Resolved yes",
    resolved_no: "Resolved no",
    resolved_partial: "Resolved partial",
    void: "Void",
    expired: "Expired",
    disputed: "Disputed",
  }[status] || label(status));

  const forecastCard = (item, compact = false) => {
    const unresolved = item.status === "pending";
    const entity = item.entity || {};
    return `
      <article class="forecast-card ${unresolved ? "is-unresolved" : "is-resolved"}">
        <div class="forecast-card-topline">
          <span class="forecast-category">${escapeHtml(label(item.category))}</span>
          <span class="forecast-status status-${escapeHtml(item.status)}">${escapeHtml(statusLabel(item.status))}</span>
        </div>
        <div class="forecast-card-probability" aria-label="Forecast probability ${escapeHtml(formatPercent(item.probability))}">${escapeHtml(formatPercent(item.probability))}</div>
        <div class="forecast-label-row"><span class="forecast-label">FORECAST</span>${unresolved ? '<span class="forecast-not-occurred">THIS EVENT HAS NOT OCCURRED</span>' : ""}</div>
        <h3>${escapeHtml(item.possible_future_headline)}</h3>
        ${compact ? "" : `<p class="forecast-question">${escapeHtml(item.question)}</p>`}
        <div class="forecast-card-meta">
          <span>${escapeHtml(entity.ticker || entity.name || "Quantura event")}</span>
          <span>Resolves ${escapeHtml(formatDate(item.resolution_deadline))}</span>
        </div>
        <div class="forecast-change ${Number(item.probability_delta) >= 0 ? "positive" : "negative"}">${escapeHtml(formatDelta(item.probability_delta))}</div>
        <a class="forecast-card-link" href="/forecasts/${encodeURIComponent(item.slug)}">View forecast <span aria-hidden="true">→</span></a>
      </article>`;
  };

  const derivePathFilters = () => {
    const segments = window.location.pathname.split("/").filter(Boolean);
    if (segments[0] !== "forecasts" || segments.length < 2) return {};
    const categoryMap = {
      markets: "markets",
      earnings: "earnings",
      corporate: "corporate",
      technology: "products_technology",
      politics: "politics_policy",
      economics: "economics",
      sports: "sports",
    };
    if (segments[1] === "stocks" && segments[2]) return { ticker: segments[2].toUpperCase() };
    if (["companies", "politics", "sports"].includes(segments[1]) && segments[2]) return { entity: segments[2] };
    if (categoryMap[segments[1]]) return { category: categoryMap[segments[1]] };
    return {};
  };

  const initFeed = () => {
    const feed = document.getElementById("forecast-feed");
    if (!feed) return;
    const form = document.getElementById("forecast-filter-form");
    const state = document.getElementById("forecast-feed-state");
    const count = document.getElementById("forecast-count");
    const loadMore = document.getElementById("forecast-load-more");
    const search = document.getElementById("forecast-search");
    const category = document.getElementById("forecast-category");
    const status = document.getElementById("forecast-status");
    const sort = document.getElementById("forecast-sort");
    const pathFilters = derivePathFilters();
    const initial = new URLSearchParams(window.location.search);
    if (pathFilters.category) category.value = pathFilters.category;
    else if (initial.get("category")) category.value = initial.get("category");
    if (initial.has("status")) status.value = initial.get("status");
    if (initial.get("search")) search.value = initial.get("search");
    if (initial.get("sort")) sort.value = initial.get("sort");
    let nextCursor = null;
    let rows = [];
    let requestSequence = 0;

    const render = () => {
      const sorted = [...rows];
      if (sort.value === "confidence") sorted.sort((a, b) => Math.abs(Number(b.probability) - 0.5) - Math.abs(Number(a.probability) - 0.5));
      if (sort.value === "changed") sorted.sort((a, b) => Math.abs(Number(b.probability_delta)) - Math.abs(Number(a.probability_delta)));
      if (sort.value === "resolving") sorted.sort((a, b) => Date.parse(a.resolution_deadline) - Date.parse(b.resolution_deadline));
      feed.innerHTML = sorted.map((item) => forecastCard(item)).join("");
      feed.hidden = sorted.length === 0;
      state.hidden = sorted.length > 0;
      state.textContent = sorted.length ? "" : "No public forecasts match these filters. Published forecasts will appear only after formal review.";
      count.textContent = `${sorted.length} forecast${sorted.length === 1 ? "" : "s"} shown`;
      loadMore.hidden = !nextCursor;
    };

    const load = async ({ append = false } = {}) => {
      const sequence = ++requestSequence;
      state.hidden = false;
      state.textContent = "Loading timestamped forecasts…";
      if (!append) {
        rows = [];
        nextCursor = null;
        feed.hidden = true;
      }
      const params = new URLSearchParams({ limit: "50" });
      const values = {
        search: search.value.trim(),
        category: category.value,
        status: status.value,
        ticker: pathFilters.ticker || "",
        entity: pathFilters.entity || "",
      };
      Object.entries(values).forEach(([key, value]) => value && params.set(key, value));
      if (append && nextCursor) params.set("cursor", nextCursor);
      try {
        const payload = await api(`/api/forecasts/public/feed?${params}`);
        if (sequence !== requestSequence) return;
        rows = append ? [...rows, ...(payload.data || [])] : (payload.data || []);
        nextCursor = payload.meta?.next_cursor || null;
        render();
        const shareParams = new URLSearchParams();
        ["search", "category", "status", "sort"].forEach((key) => {
          const value = ({ search: search.value.trim(), category: category.value, status: status.value, sort: sort.value })[key];
          if (value && !(key === "status" && value === "pending") && !(key === "sort" && value === "recent")) shareParams.set(key, value);
        });
        history.replaceState({}, "", `${window.location.pathname}${shareParams.size ? `?${shareParams}` : ""}`);
      } catch (error) {
        state.hidden = false;
        state.textContent = error.message || "Forecast data is temporarily unavailable.";
        count.textContent = "Forecast feed unavailable";
      }
    };

    let searchTimer;
    search.addEventListener("input", () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => load(), 250);
    });
    form.addEventListener("change", () => load());
    form.addEventListener("submit", (event) => { event.preventDefault(); load(); });
    document.getElementById("forecast-clear")?.addEventListener("click", () => {
      form.reset();
      status.value = "pending";
      load();
    });
    loadMore.addEventListener("click", () => load({ append: true }));
    load();
  };

  const initCalibration = async () => {
    const table = document.getElementById("forecast-calibration-table");
    const summary = document.getElementById("forecast-performance-summary");
    if (!table || !summary) return;
    try {
      const payload = await api("/api/forecasts/public/calibration");
      const data = payload.data || {};
      summary.innerHTML = `
        <div class="forecast-metric"><span>Resolved predictions</span><strong>${escapeHtml(data.resolved_count ?? 0)}</strong></div>
        <div class="forecast-metric"><span>Pending predictions</span><strong>${escapeHtml(data.pending_count ?? "—")}</strong></div>
        <div class="forecast-metric"><span>Average Brier score</span><strong>${Number.isFinite(Number(data.average_brier_score)) ? Number(data.average_brier_score).toFixed(4) : "Not available"}</strong></div>
        <div class="forecast-metric"><span>Last recalculated</span><strong>${escapeHtml(data.updated_at ? formatDate(data.updated_at, true) : "Awaiting resolved forecasts")}</strong></div>`;
      const rows = Array.isArray(data.rows) ? data.rows.filter((row) => row.forecast_count > 0) : [];
      table.querySelector("tbody").innerHTML = rows.length ? rows.map((row) => `<tr><td>${escapeHtml(row.bucket)}</td><td>${escapeHtml(row.forecast_count)}</td><td>${escapeHtml(formatPercent(row.predicted_average_probability, 1))}</td><td>${escapeHtml(formatPercent(row.actual_event_frequency, 1))}</td><td>${Number(row.average_brier_score).toFixed(4)}</td></tr>`).join("") : '<tr><td colspan="5">Calibration data will appear after reviewed forecasts resolve.</td></tr>';
    } catch (error) {
      summary.innerHTML = `<div class="forecast-state">${escapeHtml(error.message)}</div>`;
    }
  };

  const historyChart = (rows) => {
    if (!rows.length) return "";
    const width = 820;
    const height = 220;
    const points = rows.map((row, index) => {
      const x = rows.length === 1 ? width / 2 : 20 + index * ((width - 40) / (rows.length - 1));
      const y = 20 + (1 - Number(row.probability)) * (height - 40);
      return { x, y, probability: row.probability };
    });
    const path = points.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
    return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Forecast probability history"><line x1="20" y1="20" x2="20" y2="${height - 20}" class="forecast-chart-axis"/><line x1="20" y1="${height - 20}" x2="${width - 20}" y2="${height - 20}" class="forecast-chart-axis"/><path d="${path}" class="forecast-history-line"/>${points.map((point) => `<circle cx="${point.x}" cy="${point.y}" r="5" class="forecast-history-point"><title>${formatPercent(point.probability, 1)}</title></circle>`).join("")}</svg>`;
  };

  const initDetail = async () => {
    const host = document.getElementById("forecast-detail");
    if (!host) return;
    const state = document.getElementById("forecast-detail-state");
    const slug = document.body.dataset.forecastSlug || window.location.pathname.split("/").filter(Boolean).at(-1);
    try {
      const payload = await api(`/api/forecasts/public/${encodeURIComponent(slug)}`);
      const item = payload.data;
      const unresolved = item.status === "pending";
      const amendments = Array.isArray(item.amendments) ? item.amendments : [];
      host.innerHTML = `
        <div class="forecast-detail-heading">
          <div><div class="forecast-label-row"><span class="forecast-label">QUANTURA FORECAST</span>${unresolved ? '<span class="forecast-not-occurred">THIS EVENT HAS NOT OCCURRED</span>' : `<span class="forecast-status status-${escapeHtml(item.status)}">${escapeHtml(statusLabel(item.status))}</span>`}</div><div class="eyebrow">${escapeHtml(label(item.category))} · ${escapeHtml(item.entity?.name || "Event forecast")}</div><h1>${escapeHtml(item.possible_future_headline)}</h1><p class="forecast-detail-summary">${escapeHtml(item.short_summary)}</p></div>
          <div class="forecast-detail-probability"><span>Current probability</span><strong>${escapeHtml(formatPercent(item.probability))}</strong><small>${escapeHtml(formatDelta(item.probability_delta))}</small></div>
        </div>
        <div class="forecast-detail-meta"><span><strong>Created</strong>${escapeHtml(formatDate(item.published_at || item.created_at, true))}</span><span><strong>Input cutoff</strong>${escapeHtml(formatDate(item.input_cutoff_at, true))}</span><span><strong>Resolves</strong>${escapeHtml(formatDate(item.resolution_deadline, true))}</span><span><strong>Status</strong>${escapeHtml(statusLabel(item.status))}</span></div>
        <section class="forecast-detail-block formal-question"><div class="eyebrow">Formal proposition</div><h2>${escapeHtml(item.question)}</h2>${unresolved ? '<p class="forecast-disclosure">This is a prediction about an unresolved event. It is not a report that the event occurred.</p>' : ""}</section>
        <section class="forecast-detail-block"><div class="eyebrow">Quantura analysis</div><h2>Structured assessment</h2><p>${escapeHtml(item.reasoning_summary)}</p><div class="forecast-scenario-grid"><article><span>Bear case</span><p>${escapeHtml(item.bear_case)}</p></article><article><span>Base case</span><p>${escapeHtml(item.base_case)}</p></article><article><span>Bull case</span><p>${escapeHtml(item.bull_case)}</p></article></div></section>
        <section class="forecast-detail-grid"><div class="forecast-detail-block"><div class="eyebrow">Evidence available at forecast time</div><h2>Source record</h2><ul class="forecast-evidence-list">${(item.evidence || []).map((source) => `<li><strong>${escapeHtml(source.source)}</strong><span>${escapeHtml(source.description || source.title || source.data_class || "Structured evidence")}</span><small>Observed ${escapeHtml(formatDate(source.observed_at, true))}</small>${source.url ? `<a href="${escapeHtml(source.url)}" target="_blank" rel="noopener noreferrer nofollow">Open source</a>` : ""}</li>`).join("") || "<li>No redistributable source detail is available.</li>"}</ul></div><div class="forecast-detail-block"><div class="eyebrow">Resolution</div><h2>Rule fixed before publication</h2><p>${escapeHtml(item.resolution?.rule)}</p><p class="small"><strong>Authoritative source:</strong> ${escapeHtml(item.resolution?.source)}</p>${item.resolution?.actual_outcome ? `<div class="forecast-resolution-result"><strong>${escapeHtml(statusLabel(item.status))}</strong><span>${escapeHtml(item.resolution.actual_outcome)}</span><span>Brier score: ${Number.isFinite(Number(item.resolution.brier_score)) ? Number(item.resolution.brier_score).toFixed(4) : "Not scored"}</span></div>` : ""}<hr/><div class="small"><strong>Model:</strong> ${escapeHtml(item.model?.name)} · ${escapeHtml(item.model?.version)}<br/><strong>Method:</strong> ${escapeHtml(item.model?.method)}</div></div></section>
        ${amendments.length ? `<section class="forecast-detail-block forecast-amendments"><div class="eyebrow">Transparent corrections</div><h2>Published amendments</h2><p class="small">These display corrections are separate records. They do not alter the original question, probability, evidence cutoff, or resolution rule.</p><ol>${amendments.map((amendment) => `<li><strong>${escapeHtml(amendment.field)}</strong><span>${escapeHtml(amendment.corrected_display_value || amendment.note)}</span><small>${escapeHtml(amendment.reason)} · ${escapeHtml(formatDate(amendment.created_at, true))}</small></li>`).join("")}</ol></section>` : ""}`;
      host.hidden = false;
      state.hidden = true;
      const history = Array.isArray(item.probability_history) ? item.probability_history : [];
      if (history.length) {
        const section = document.getElementById("forecast-probability-history");
        section.hidden = false;
        document.getElementById("forecast-history-chart").innerHTML = historyChart(history);
        document.getElementById("forecast-history-body").innerHTML = history.map((row) => `<tr><td>${escapeHtml(row.revision)}</td><td>${escapeHtml(formatPercent(row.probability, 1))}</td><td>${escapeHtml(formatDelta(row.probability_delta))}</td><td>${escapeHtml(formatDate(row.input_cutoff_at, true))}</td><td>${escapeHtml(formatDate(row.created_at, true))}</td><td>${escapeHtml(row.reasoning_delta || "Initial published forecast")}</td></tr>`).join("");
      }
    } catch (error) {
      state.textContent = error.message || "Forecast not found.";
    }
  };

  const initHomePreview = async () => {
    const host = document.getElementById("home-forecast-preview");
    if (!host) return;
    try {
      const payload = await api("/api/forecasts/public/feed?status=pending&limit=3");
      const rows = payload.data || [];
      host.innerHTML = rows.length ? rows.map((item) => forecastCard(item, true)).join("") : '<div class="forecast-state">The public prediction record is awaiting its first reviewed forecast. Quantura does not publish placeholder events.</div>';
    } catch (error) {
      host.innerHTML = '<div class="forecast-state">The forecast preview is temporarily unavailable.</div>';
    }
  };

  document.addEventListener("DOMContentLoaded", () => {
    initFeed();
    initCalibration();
    initDetail();
    initHomePreview();
  });
})();
