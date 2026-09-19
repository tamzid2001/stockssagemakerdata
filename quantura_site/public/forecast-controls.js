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
    if (!row.timestamp || !Number.isFinite(Date.parse(row.timestamp))) return row.timestamp;
    if (job.source?.type !== "ticker" || job.frequency !== "1D") return row.timestamp;
    // Daily predictions use session-date labels, not the provider's midnight
    // or opening-time bar label. Preserve real timestamps outside the plot.
    const timeZone = job.source.exchange_timezone || "America/New_York";
    if (!stockSessionFormatters.has(timeZone)) stockSessionFormatters.set(timeZone, new Intl.DateTimeFormat("en-CA", {
      timeZone, year: "numeric", month: "2-digit", day: "2-digit",
    }));
    const date = row.session_date || (job.source?.daily_timestamp_convention === "session_date" && (!row.interval || row.interval === "1D") ? String(row.timestamp).slice(0,10) : stockSessionFormatters.get(timeZone).format(new Date(row.timestamp)));
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
    if (!Number.isFinite(timestamp)) return {status: "unavailable", reason: "Waiting for the first prediction row."};
    const dailyStock = job.source?.type === "ticker" && job.frequency === "1D";
    const intervalLabel = job.frequency === "1D" ? "day" : job.frequency === "1h" ? "hour" : job.frequency === "1min" ? "minute" : "interval";
    const levels = first.quantiles || {}, lower = levels["0.1"], upper = levels["0.9"];
    if (![lower, upper].every(v => typeof v === "number" && Number.isFinite(v)) || lower > upper) return {status: "unavailable", reason: "Request both P10 and P90."};
    const published = Date.parse(job.completed_at);
    const replay = job.source?.analysis_mode === "historical_replay";
    // Match the first predicted interval after the immutable input cutoff.
    // Worker latency must not move the signal to a different market minute.
    const observation = (job.observations || []).find(row => {
      if (row.is_forward_filled === true || row.observed === false || row.is_complete === false) return false;
      if (row.interval && row.interval !== job.frequency) return false;
      // Daily equity bars are labeled by exchange session, not UTC midnight.
      // Never substitute a provisional minute quote for that day's close.
      if (dailyStock) return row.interval === "1D" && stockChartTimestamp(row, job).slice(0, 10) === first.timestamp.slice(0, 10);
      return Date.parse(row.timestamp) === timestamp;
    });
    const quoteTimestamp = observation ? Date.parse(observation.timestamp) : timestamp;
    // Use the same selected-side target/closing price as the history and chart,
    // not an ask from a different price series. This is not an execution fill.
    const price = observation?.target;
    if (quoteTimestamp > now || typeof price !== "number" || !Number.isFinite(price)) return {status: "waiting", timestamp, reason: `Waiting for the completed ${intervalLabel} closing value matching the first prediction row, immediately after the downloaded history. Later or shorter-interval quotes cannot replace it.`};
    const prospective = !replay && Number.isFinite(published) && published < timestamp;
    return {status: "observed", timestamp, quoteTimestamp, price, lower, upper, signal: price < lower ? "buy" : price > upper ? "sell" : "none", prospective,
      timing: `First predicted ${intervalLabel} after downloaded history${prospective ? "" : " · retrospective comparison, not a backdated live entry"}`};
  }
  function forecastChartRange(job) {
    const lastInput=Date.parse(stockChartTimestamp(job.history?.at(-1) || {},job));
    const candidates=[lastInput,...(job.observations || []).map(row=>Date.parse(stockChartTimestamp(row,job)))].filter(Number.isFinite);
    const latest=candidates.length ? Math.max(...candidates) : Date.parse(job.predictions?.[0]?.timestamp);
    const end=Math.max(latest,Date.parse(job.predictions?.at(-1)?.timestamp));
    if(!Number.isFinite(latest)||!Number.isFinite(end))return null;
    const frequency=String(job.frequency || "1D").toLowerCase();
    const amount=Number.parseInt(frequency,10)||1;
    const unit=/min|^\d+m$/.test(frequency)?60_000:/hour|^\d+h$/.test(frequency)?3600_000:/week|^\d+w$/.test(frequency)?7*86400_000:86400_000;
    const firstPrediction=Date.parse(job.predictions?.[0]?.timestamp);
    // Later overlays must not push the saved forecast's first rows offscreen.
    const anchor=Number.isFinite(firstPrediction)?Math.min(latest,firstPrediction):latest;
    let start=anchor-amount*unit;
    if(job.source?.type==="ticker" && frequency==="1d" && job.chart_calendar?.sessions) {
      const date=new Date(anchor).toISOString().slice(0,10);
      const previous=job.chart_calendar.sessions.filter(day=>day<date).at(-1);
      if(previous)start=Date.parse(previous+"T00:00:00Z");
    }
    return [start,end];
  }
  // Plotly emits UTC chart coordinates without a timezone on relayout. Native
  // Date.parse would reinterpret those as the visitor's local clock.
  function chartInstant(value) {
    if (typeof value === "number") return value;
    const text = String(value || "");
    return Date.parse(/^\d{4}-\d{2}-\d{2}[T ]/.test(text) && !/(?:Z|[+-]\d{2}:?\d{2})$/i.test(text) ? text + "Z" : text);
  }
  function visibleForecastYRange(traces,range) {
    if(!range)return null;
    let lo=Infinity,hi=-Infinity;
    const add=value=>{if(typeof value==="number"&&Number.isFinite(value)){lo=Math.min(lo,value);hi=Math.max(hi,value);}};
    for(const trace of traces) {
      if(trace.visible===false || trace.visible==="legendonly")continue;
      for(let i=0;i<(trace.x || []).length;i++) {
        const x=typeof trace.x[i]==="number"?trace.x[i]:Date.parse(trace.x[i]); const y=trace.y[i];
        if(x>=range[0]&&x<=range[1])add(y);
        if(i>0 && /lines/.test(trace.mode || "")) {
          const before=typeof trace.x[i-1]==="number"?trace.x[i-1]:Date.parse(trace.x[i-1]);const prev=trace.y[i-1];
          if(typeof y==="number" && typeof prev==="number" && x>before)for(const edge of range)
            if(before<edge&&x>edge)add(prev+(y-prev)*(edge-before)/(x-before));
        }
      }
    }
    if(!Number.isFinite(lo)||!Number.isFinite(hi))return null;
    const pad=Math.max((hi-lo)*0.08,Math.abs(hi)*0.001,1e-6);
    return [lo-pad,hi+pad];
  }
  function exchangeDateBreaks(job) {
    const calendar=job.chart_calendar;
    if(job.source?.type!=="ticker" || job.frequency!=="1D" || calendar?.exchange!=="NYSE")return [];
    const times=[...(job.history || []).map(r=>stockChartTimestamp(r,job)),...(job.predictions || []).map(r=>r.timestamp),...(job.observations || []).map(r=>stockChartTimestamp(r,job))].map(Date.parse).filter(Number.isFinite);
    if(!times.length)return [];
    const start=Math.min(...times),end=Math.max(...times);
    if(start<Date.parse(calendar.start)||end>=Date.parse(calendar.end)+86400_000)return [];
    const sessions=new Set(calendar.sessions);const closed=[];
    for(let time=Math.floor(start/86400_000)*86400_000;time<=end;time+=86400_000) {
      const day=new Date(time).toISOString().slice(0,10);
      if(!sessions.has(day))closed.push(day);
    }
    return closed.length?[{values:closed,dvalue:86400_000}]:[];
  }
  const helpers = Object.freeze({ localValue, localInstant, cutoffInstant, stockChartTimestamp, parseCsv, csvSeries, firstRowSignal, forecastChartRange, chartInstant, visibleForecastYRange, exchangeDateBreaks });
  if (typeof module !== "undefined" && module.exports) module.exports = helpers;
  else root.QuanturaForecastControls = helpers;
})(typeof window === "undefined" ? globalThis : window);
