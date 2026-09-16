/* Pure forecast form helpers; timestamps travel over the API as absolute UTC. */
((root) => {
  "use strict";
  const localValue = (value = Date.now()) => {
    const date = new Date(value);
    if (!Number.isFinite(date.getTime())) return "";
    return new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
  };
  const localInstant = value => {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(value || "")) throw new Error("Choose a date and time.");
    const instant = new Date(value);
    // Reject nonexistent local times during the spring DST transition.
    if (!Number.isFinite(instant.getTime()) || localValue(instant) !== value) throw new Error("This time does not exist in your timezone. Choose another time.");
    return instant.toISOString();
  };
  const cutoffInstant = (value, now = Date.now()) => {
    const instant = localInstant(value);
    if (Date.parse(instant) > now) throw new Error("The data cutoff cannot be in the future.");
    return instant;
  };
  const stockSessionFormatters = new Map();
  function stockChartTimestamp(row, job) {
    if (job.source?.type !== "ticker" || job.frequency !== "1D") return row.timestamp;
    // Daily predictions use session-date labels, not the provider's midnight
    // or opening-time bar label. Preserve real timestamps outside the plot.
    const timeZone = job.source.exchange_timezone || "America/New_York";
    if (!stockSessionFormatters.has(timeZone)) stockSessionFormatters.set(timeZone, new Intl.DateTimeFormat("en-CA", {
      timeZone, year: "numeric", month: "2-digit", day: "2-digit",
    }));
    const date = row.session_date || stockSessionFormatters.get(timeZone).format(new Date(row.timestamp));
    return `${date}T00:00:00.000Z`;
  }
  function parseCsv(text, maxRows = 10000) {
    text = String(text).replace(/^\uFEFF/, "");
    const records = []; let record = [], field = "", quoted = false, closed = false;
    const cell = () => { record.push(field); field = ""; closed = false; };
    const line = () => { cell(); if (record.some(value => value.trim())) records.push(record); record = []; if (records.length > maxRows + 1) throw new Error(`CSV is limited to ${maxRows} rows.`); };
    for (let i = 0; i < text.length; i++) {
      const char = text[i];
      if (quoted) {
        if (char === '"' && text[i+1] === '"') { field += '"'; i++; }
        else if (char === '"') { quoted = false; closed = true; }
        else field += char;
      } else if (char === '"') {
        if (field || closed) throw new Error("Invalid CSV quoting.");
        quoted = true;
      } else if (char === ',') cell();
      else if (char === '\n') line();
      else if (char !== '\r') { if (closed) throw new Error("Invalid CSV quoting."); field += char; }
    }
    if (quoted) throw new Error("CSV contains an unclosed quoted field.");
    if (field || record.length) line();
    const headers = (records.shift() || []).map(value => value.trim());
    if (!headers.length || headers.some(value => !value) || new Set(headers).size !== headers.length) throw new Error("CSV needs unique, nonempty column headers.");
    if (records.length < 40) throw new Error("Upload at least 40 time-series rows.");
    if (records.some(row => row.length !== headers.length)) throw new Error("Every CSV row must have the same number of columns.");
    return { headers, rows: records };
  }
  function csvSeries(table, timestampColumn, targetColumn) {
    const t = table.headers.indexOf(timestampColumn), y = table.headers.indexOf(targetColumn);
    if (t < 0 || y < 0 || t === y) throw new Error("Choose different timestamp and numeric value columns.");
    return table.rows.map((row, index) => {
      const raw = row[t].trim(); let timestamp;
      if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
        timestamp = `${raw}T00:00:00.000Z`;
        if (!Number.isFinite(Date.parse(timestamp)) || new Date(timestamp).toISOString() !== timestamp) throw new Error(`Row ${index+2}: invalid calendar date.`);
      }
      else if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}$/.test(raw)) timestamp = localInstant(raw.replace(" ", "T"));
      else if (/(?:Z|[+-]\d{2}:?\d{2})$/i.test(raw) && Number.isFinite(Date.parse(raw))) timestamp = new Date(raw).toISOString();
      else throw new Error(`Row ${index+2}: use an ISO date/time, with an explicit offset for seconds or ambiguous DST times.`);
      const target = Number(row[y]);
      if (!row[y].trim() || !Number.isFinite(target)) throw new Error(`Row ${index+2}: target must be a finite number.`);
      return { timestamp, target };
    });
  }
  function firstRowSignal(job, now = Date.now()) {
    const first = job.predictions?.[0];
    const timestamp = Date.parse(first?.timestamp);
    if (job.frequency !== "1min" || !Number.isFinite(timestamp) || timestamp % 60000) return {status: "unavailable", reason: "Requires a one-minute forecast."};
    const levels = first.quantiles || {}, lower = levels["0.1"], upper = levels["0.9"];
    if (![lower, upper].every(v => typeof v === "number" && Number.isFinite(v)) || lower > upper) return {status: "unavailable", reason: "Request both P10 and P90."};
    const published = Date.parse(job.completed_at);
    const replay = job.source?.analysis_mode === "historical_replay";
    // Match the first predicted interval after the immutable input cutoff.
    // Worker latency must not move the signal to a different market minute.
    const quoteTimestamp = timestamp;
    const observation = (job.observations || []).find(row => Date.parse(row.timestamp) === quoteTimestamp && row.is_forward_filled !== true && row.observed !== false);
    // Use the same selected-side target/closing price as the history and chart,
    // not an ask from a different price series. This is not an execution fill.
    const price = observation?.target;
    if (quoteTimestamp > now || typeof price !== "number" || !Number.isFinite(price)) return {status: "waiting", timestamp, reason: "Waiting for the completed one-minute quote matching the first prediction row, immediately after the downloaded history. Later quotes cannot replace it."};
    const prospective = !replay && Number.isFinite(published) && published < timestamp;
    return {status: "observed", timestamp, quoteTimestamp, price, lower, upper, signal: price < lower ? "buy" : price > upper ? "sell" : "none", prospective,
      timing: prospective ? "First predicted minute after downloaded history" : "First predicted minute after downloaded history · retrospective comparison, not a backdated live entry"};
  }
  const helpers = Object.freeze({ localValue, localInstant, cutoffInstant, stockChartTimestamp, parseCsv, csvSeries, firstRowSignal });
  if (typeof module !== "undefined" && module.exports) module.exports = helpers;
  else root.QuanturaForecastControls = helpers;
})(typeof window === "undefined" ? globalThis : window);
