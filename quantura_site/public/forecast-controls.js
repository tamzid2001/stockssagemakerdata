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
    // Replay has no historical publication clock. Keep that retrospective
    // comparison explicitly separate from the live post-completion signal.
    const quoteTimestamp = replay ? timestamp : Math.floor(published / 60000) * 60000 + 60000;
    const observation = (job.observations || []).find(row => Date.parse(row.timestamp) === quoteTimestamp && row.is_forward_filled !== true && row.observed !== false);
    // Frozen FIRST forecast row versus the first completed minute after
    // publication. These timestamps need not match; neither is backdated.
    const price = job.source?.type === "prediction_market" ? (observation?.ask ?? observation?.target) : observation?.target;
    if (quoteTimestamp > now || !Number.isFinite(quoteTimestamp) || typeof price !== "number" || !Number.isFinite(price)) return {status: "waiting", timestamp, reason: "Waiting for the first completed one-minute quote after forecast completion; comparing it with the first predicted row."};
    return {status: "observed", timestamp, quoteTimestamp, price, lower, upper, signal: price < lower ? "buy" : price > upper ? "sell" : "none", prospective: !replay,
      timing: replay ? "Historical replay, not a live publication" : "First completed minute after publication versus frozen first-row thresholds"};
  }
  const helpers = Object.freeze({ localValue, localInstant, parseCsv, csvSeries, firstRowSignal });
  if (typeof module !== "undefined" && module.exports) module.exports = helpers;
  else root.QuanturaForecastControls = helpers;
})(typeof window === "undefined" ? globalThis : window);
