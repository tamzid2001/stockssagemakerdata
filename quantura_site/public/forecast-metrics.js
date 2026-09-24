/* Scores for this forecast's timestamp-matched prices. No extra inference. */
((root) => {
  "use strict";
  const controls = typeof module !== "undefined" && module.exports ? require("./forecast-controls.js") : root.QuanturaForecastControls;
  const finite = value => typeof value === "number" && Number.isFinite(value);
  const mean = values => values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  function summarize(matches, levels) {
    const point = matches.filter(row => finite(row.quantiles["0.5"]));
    const errors = point.map(row => row.quantiles["0.5"] - row.actual);
    const absolute = errors.map(Math.abs), squared = errors.map(value => value*value);
    const quantiles = levels.map(q => {
      const valid = matches.filter(row => finite(row.quantiles[String(q)]));
      const losses = valid.map(row => { const error = row.actual-row.quantiles[String(q)]; return error >= 0 ? q*error : (q-1)*error; });
      const denom = valid.reduce((sum,row) => sum + Math.abs(row.actual),0);
      return {q, count:valid.length, wql:denom > 0 ? 2*losses.reduce((a,b)=>a+b,0)/denom : null};
    });
    return {count:matches.length,point_count:point.length,mae:mean(absolute),rmse:squared.length ? Math.sqrt(mean(squared)) : null,
      smape:mean(point.map(row=>{const denom=Math.abs(row.actual)+Math.abs(row.quantiles["0.5"]);return denom ? 2*Math.abs(row.quantiles["0.5"]-row.actual)/denom : 0;})),
      quantiles,
      average_wql:mean(quantiles.map(row=>row.wql).filter(finite))};
  }

  function compute(job, now = Date.now()) {
    const predictions = job.predictions || [];
    const levels = [...new Set((job.quantiles || []).map(Number))].filter(q=>q>0 && q<1).sort((a,b)=>a-b);
    const byTime = new Map(predictions.map((row,index)=>[Date.parse(row.timestamp),{...row,index}]));
    const actuals = new Map();
    const selectedTarget = !job.source?.field || job.source.field === "close";
    if (selectedTarget) for (const row of job.observations || []) {
      const instant = Date.parse(row.timestamp);
      if (!finite(row.target) || !Number.isFinite(instant) || instant>now || row.is_complete === false || row.is_forward_filled === true || row.observed === false) continue;
      if (row.interval && row.interval !== job.frequency) continue;
      if (job.source?.type === "ticker" && job.frequency === "1D" && row.interval !== "1D") continue;
      const time = Date.parse(controls.stockChartTimestamp(row,job));
      if (!byTime.has(time)) continue;
      actuals.set(time,row.target);
    }
    const published = Date.parse(job.completed_at);
    const prospective = [],retrospective = [];
    for (const [time,actual] of [...actuals].sort((a,b)=>a[0]-b[0])) {
      const row = byTime.get(time), match = {actual,index:row.index,quantiles:row.quantiles || {}};
      let outcomeTime = time;
      const calendar = job.chart_calendar;
      if(job.source?.type === "ticker" && job.frequency === "1D" && calendar?.exchange === "NYSE") {
        const index = calendar.sessions.indexOf(new Date(time).toISOString().slice(0,10));
        const closeMinute = calendar.close_minute_utc?.[index];
        if(finite(closeMinute)) outcomeTime = time + closeMinute*60000;
      }
      if(outcomeTime>now)continue;
      // Conservatively exclude any target timestamp already reached at publication.
      // Daily session-date labels are not close times: use actual exchange closes,
      // including early closes and DST. Without a calendar retain conservative labeling.
      // A historical replay is descriptive only, never a walk-forward backtest.
      (job.source?.analysis_mode !== "historical_replay" && Number.isFinite(published) && outcomeTime>published ? prospective : retrospective).push(match);
    }
    return {expected:predictions.length,levels,target_supported:selectedTarget,
      prospective:summarize(prospective,levels),retrospective:summarize(retrospective,levels)};
  }

  function render(host, job) {
    if (!host) return;
    const report = compute(job), doc = host.ownerDocument;
    const openDetails = [...host.querySelectorAll("details")].map(el=>el.open);
    const oldButtons = [...host.querySelectorAll("button")];
    const expandedHelp = oldButtons.map(el=>el.getAttribute("aria-expanded")==="true");
    const focusedHelp = oldButtons.indexOf(doc.activeElement);
    const element = (tag,text,className) => {const el=doc.createElement(tag);if(text!==undefined)el.textContent=text;if(className)el.className=className;return el;};
    const format = (value, percent = false) => finite(value) ? (percent ? `${(100*value).toFixed(2)}%` : new Intl.NumberFormat(undefined,{maximumSignificantDigits:5}).format(value)) : "—";
    const cards = [
      ["MAE","mae",false,"Mean absolute difference between predicted P50 and the actual completed price at the same time. It cannot be measured before that price exists."],
      ["RMSE","rmse",false,"Root mean squared P50 error; larger misses count more. It requires completed prices after publication."],
      ["sMAPE","smape",true,"Symmetric percentage error between P50 and completed prices after publication. Near-zero values can make it unstable."],
      ["Weighted quantile loss","average_wql",false,"Average quantile error against completed prices after publication. It cannot be inferred from the forecast distribution alone."],
    ];
    const section = (metrics, title, description) => {
      const box=element("section",undefined,"forecast-metrics-section");box.append(element("h4",title));
      box.append(element("p",`${description || `${metrics.count} / ${report.expected} timestamp-matched completed outcomes · ${metrics.point_count} P50 comparisons`}${metrics.count && metrics.count<30 ? " · Small outcome sample." : ""}`,"small muted"));
      const grid=element("dl",undefined,"forecast-metrics-grid");
      for(const [name,key,percent,help] of cards){const cell=element("div"),term=element("dt",name),hint=element("button","ⓘ","forecast-metric-help");hint.type="button";hint.title=help;hint.setAttribute("aria-label",`${name}: ${help}`);hint.addEventListener("click",()=>{hint.nextElementSibling.hidden=!hint.nextElementSibling.hidden;hint.setAttribute("aria-expanded",String(!hint.nextElementSibling.hidden));});hint.setAttribute("aria-expanded","false");const explanation=element("p",help,"small muted");explanation.hidden=true;term.append(hint,explanation);cell.append(term,element("dd",format(metrics[key],percent)));grid.append(cell);}box.append(grid);
      if(metrics.count && !metrics.point_count) box.append(element("p","P50 was not requested, so the three point-error metrics are undefined. Weighted quantile loss uses the requested quantiles.","small muted"));
      if(metrics.count && metrics.average_wql === null) box.append(element("p","Weighted quantile loss is undefined when matched actual values are all zero.","small muted"));
      return box;
    };
    const fragment=doc.createDocumentFragment(),heading=element("h3","Forecast quality");heading.id="ensemble-quality-title";fragment.append(heading);
    if(report.prospective.count) {
      fragment.append(section(report.prospective,"Observed after this forecast was published"));
    } else {
      fragment.append(section({count:0,point_count:0},"This forecast",
        "No completed post-publication prices match the prediction timestamps yet. MAE, RMSE, sMAPE and weighted quantile loss require actual outcomes; a forecast alone cannot produce honest error scores. No historical validation or second model run is performed."));
    }
    host.replaceChildren(fragment);
    [...host.querySelectorAll("details")].forEach((el,i)=>{el.open=Boolean(openDetails[i]);});
    [...host.querySelectorAll("button")].forEach((el,i)=>{if(expandedHelp[i]){el.setAttribute("aria-expanded","true");el.nextElementSibling.hidden=false;}if(i===focusedHelp)el.focus({preventScroll:true});});
  }
  const api = Object.freeze({compute,render});
  if(typeof module!=="undefined" && module.exports)module.exports=api;else root.QuanturaForecastMetrics=api;
})(typeof window === "undefined" ? globalThis : window);
