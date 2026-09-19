/* Final-ensemble diagnostics only. Never score training fit as future accuracy. */
((root) => {
  "use strict";
  const controls = typeof module !== "undefined" && module.exports ? require("./forecast-controls.js") : root.QuanturaForecastControls;
  const finite = value => typeof value === "number" && Number.isFinite(value);
  const mean = values => values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  const median = values => { const sorted = [...values].sort((a,b) => a-b); return sorted.length ? (sorted[Math.floor(sorted.length/2)] + sorted[Math.ceil(sorted.length/2)-1])/2 : null; };

  function summarize(matches, levels, history) {
    const point = matches.filter(row => finite(row.quantiles["0.5"]));
    const errors = point.map(row => row.quantiles["0.5"] - row.actual);
    const absolute = errors.map(Math.abs), squared = errors.map(value => value*value);
    const denominator = point.reduce((sum,row) => sum + Math.abs(row.actual), 0);
    const actualMean = mean(point.map(row => row.actual));
    const variance = point.reduce((sum,row) => sum + (row.actual-actualMean)**2, 0);
    const nonzero = point.filter(row => row.actual !== 0);
    const naive = history.slice(1).flatMap((row,i) => finite(row.target) && finite(history[i].target) ? [Math.abs(row.target-history[i].target)] : []);
    const naiveMae = mean(naive);
    const directions = point.flatMap((row,i) => {
      const prev = point[i-1];
      if (prev && prev.index === row.index-1) return [Number(Math.sign(row.actual-prev.actual) === Math.sign(row.quantiles["0.5"]-prev.quantiles["0.5"]))];
      const anchor = history.at(-1)?.target;
      return row.index === 0 && finite(anchor) ? [Number(Math.sign(row.actual-anchor) === Math.sign(row.quantiles["0.5"]-anchor))] : [];
    });
    const quantiles = levels.map(q => {
      const valid = matches.filter(row => finite(row.quantiles[String(q)]));
      const losses = valid.map(row => { const error = row.actual-row.quantiles[String(q)]; return error >= 0 ? q*error : (q-1)*error; });
      const denom = valid.reduce((sum,row) => sum + Math.abs(row.actual),0);
      return {q, count:valid.length, pinball:mean(losses), wql:denom > 0 ? 2*losses.reduce((a,b)=>a+b,0)/denom : null,
        coverage:mean(valid.map(row => Number(row.actual <= row.quantiles[String(q)])))};
    });
    const intervals = levels.filter(q => q < 0.5 && levels.some(upper => Math.abs(upper-(1-q)) < 1e-10)).map(lower => {
      const upper = levels.find(q => Math.abs(q-(1-lower)) < 1e-10);
      const valid = matches.filter(row => finite(row.quantiles[String(lower)]) && finite(row.quantiles[String(upper)]));
      return {lower,upper,count:valid.length,nominal:upper-lower,coverage:mean(valid.map(row => Number(row.actual >= row.quantiles[String(lower)] && row.actual <= row.quantiles[String(upper)])))};
    });
    return {count:matches.length,point_count:point.length,mae:mean(absolute),rmse:squared.length ? Math.sqrt(mean(squared)) : null,mse:mean(squared),median_ae:median(absolute),bias:mean(errors),
      mape:mean(nonzero.map(row=>Math.abs((row.quantiles["0.5"]-row.actual)/row.actual))),mape_count:nonzero.length,
      smape:mean(point.map(row=>{const denom=Math.abs(row.actual)+Math.abs(row.quantiles["0.5"]);return denom ? 2*Math.abs(row.quantiles["0.5"]-row.actual)/denom : 0;})),
      wape:denominator > 0 ? absolute.reduce((a,b)=>a+b,0)/denominator : null,
      mase:naiveMae > 0 && errors.length ? mean(absolute)/naiveMae : null,
      r2:point.length >= 2 && variance > 0 ? 1-squared.reduce((a,b)=>a+b,0)/variance : null,
      directional_accuracy:mean(directions),direction_count:directions.length,quantiles,intervals,
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
      // Conservatively exclude any target timestamp already reached at publication.
      // A historical replay is descriptive only, never a walk-forward backtest.
      (job.source?.analysis_mode !== "historical_replay" && Number.isFinite(published) && time>published ? prospective : retrospective).push(match);
    }
    return {expected:predictions.length,levels,target_supported:selectedTarget,
      prospective:summarize(prospective,levels,job.history || []),retrospective:summarize(retrospective,levels,job.history || [])};
  }

  function render(host, job) {
    if (!host) return;
    const report = compute(job), doc = host.ownerDocument;
    const openDetails = [...host.querySelectorAll("details")].map(el=>el.open);
    const oldButtons = [...host.querySelectorAll("button")];
    const expandedHelp = oldButtons.map(el=>el.getAttribute("aria-expanded")==="true");
    const focusedHelp = oldButtons.indexOf(doc.activeElement);
    const element = (tag,text,className) => {const el=doc.createElement(tag);if(text!==undefined)el.textContent=text;if(className)el.className=className;return el;};
    const format = (value, percent = false) => finite(value) ? (percent ? `${(100*value).toFixed(2)}%` : new Intl.NumberFormat(undefined,{maximumSignificantDigits:5}).format(value)) : "Not available";
    const label = q => `P${Number((q*100).toPrecision(10))}`;
    const cards = [
      ["MAE","mae",false,"Mean absolute P50 error; lower is better. In the target's units."],
      ["RMSE","rmse",false,"Root mean squared P50 error; penalizes large misses. In the target's units."],
      ["Median absolute error","median_ae",false,"Middle absolute P50 error; less sensitive to outliers."],
      ["Forecast bias","bias",false,"Mean forecast minus actual. Positive means overprediction."],
      ["sMAPE","smape",true,"Symmetric percentage error; zero/zero contributes zero. Unstable near zero."],
      ["WAPE","wape",true,"Absolute error divided by total absolute actual values. Undefined when all actuals are zero."],
      ["Directional accuracy","directional_accuracy",true,"Correct changes between adjacent matched forecast steps; first step uses the last historical target. Not a trading win rate."],
      ["Average wQL","average_wql",false,"Mean weighted quantile loss over available quantiles; lower is better. Undefined for zero actual magnitude."],
    ];
    const table = (headers, rows) => {
      const wrap=element("div",undefined,"forecast-metrics-table"),t=element("table"),thead=element("thead"),hr=element("tr");
      for(const text of headers){const th=element("th",text);th.scope="col";hr.append(th);}thead.append(hr);t.append(thead);
      const body=element("tbody");for(const row of rows){const tr=element("tr");row.forEach(text=>tr.append(element("td",text)));body.append(tr);}t.append(body);wrap.append(t);return wrap;
    };
    const section = (metrics, title) => {
      const box=element("section",undefined,"forecast-metrics-section");box.append(element("h4",title));
      box.append(element("p",`${metrics.count} / ${report.expected} timestamp-matched completed outcomes · ${metrics.point_count} P50 comparisons${metrics.count && metrics.count<30 ? " · Small sample; not evidence of reliable accuracy." : ""}`,"small muted"));
      const grid=element("dl",undefined,"forecast-metrics-grid");
      for(const [name,key,percent,help] of cards){const cell=element("div"),term=element("dt",name),hint=element("button","ⓘ","forecast-metric-help");hint.type="button";hint.title=help;hint.setAttribute("aria-label",`${name}: ${help}`);hint.addEventListener("click",()=>{hint.nextElementSibling.hidden=!hint.nextElementSibling.hidden;hint.setAttribute("aria-expanded",String(!hint.nextElementSibling.hidden));});hint.setAttribute("aria-expanded","false");const explanation=element("p",help,"small muted");explanation.hidden=true;term.append(hint,explanation);cell.append(term,element("dd",format(metrics[key],percent)));grid.append(cell);}box.append(grid);
      if(!metrics.count) box.append(element("p",report.target_supported ? "Waiting for completed, timestamp-matched outcomes. Future forecast accuracy cannot be known when the models finish. Values update with Refresh quotes." : "Accuracy metrics are unavailable: the selected forecast target is not close, but this provider's actual-price overlay contains closing prices.","small muted"));
      const details=element("details");details.append(element("summary","Quantile calibration & additional metrics"));
      details.append(table(["Metric","Value"],[
        ["MSE",format(metrics.mse)],["MAPE (nonzero actuals only)",`${format(metrics.mape,true)} · n=${metrics.mape_count}`],
        ["MASE (successive historical observations baseline)",format(metrics.mase)],["R² (nonconstant actuals only)",format(metrics.r2)],
        ["Directional comparisons",String(metrics.direction_count)],
      ]));
      details.append(element("p","Calibration: the observed share below each quantile should approach its nominal level across many independent forecasts. These values describe this forecast only; they are not confidence in a trading signal.","small muted"));
      details.append(table(["Quantile","n","Pinball loss ↓","wQL ↓","Share at/below","Nominal"],metrics.quantiles.map(row=>[label(row.q),String(row.count),format(row.pinball),format(row.wql),format(row.coverage,true),format(row.q,true)])));
      if(metrics.intervals.length)details.append(table(["Interval","n","Observed coverage","Nominal"],metrics.intervals.map(row=>[`${label(row.lower)}–${label(row.upper)}`,String(row.count),format(row.coverage,true),format(row.nominal,true)])));
      box.append(details);return box;
    };
    const fragment=doc.createDocumentFragment(),heading=element("h3","Forecast quality");heading.id="ensemble-quality-title";fragment.append(heading);
    fragment.append(element("p","Final-ensemble metrics on original-scale outcomes. Not training-fit scores or a walk-forward backtest. P50 is required for point-error metrics; only the requested quantiles are evaluated.","small muted"));
    fragment.append(section(report.prospective,"Observed after publication"));
    if(report.retrospective.count) {const detail=element("details");detail.append(element("summary",`Retrospective comparison · ${report.retrospective.count} outcomes (not live validation)`));detail.append(section(report.retrospective,"Historical/replay outcomes"));fragment.append(detail);}
    host.replaceChildren(fragment);
    [...host.querySelectorAll("details")].forEach((el,i)=>{el.open=Boolean(openDetails[i]);});
    [...host.querySelectorAll("button")].forEach((el,i)=>{if(expandedHelp[i]){el.setAttribute("aria-expanded","true");el.nextElementSibling.hidden=false;}if(i===focusedHelp)el.focus({preventScroll:true});});
  }
  const api = Object.freeze({compute,render});
  if(typeof module!=="undefined" && module.exports)module.exports=api;else root.QuanturaForecastMetrics=api;
})(typeof window === "undefined" ? globalThis : window);
