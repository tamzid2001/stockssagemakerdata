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

  function availableReports(job,now=Date.now()) {
    const result=[],validation=job.historical_validation;
    if(validation?.status==='completed' && validation.metrics)result.push({metrics:validation.metrics,title:'Historical validation',description:`${validation.metrics.count} held-out observations`});
    if(job.ml_metrics?.metrics)result.push({metrics:job.ml_metrics.metrics,title:'Model quality',description:job.ml_metrics.basis || 'Admin supplied metrics'});
    const observed=compute(job,now);
    if(observed.prospective.count)result.push({metrics:observed.prospective,title:'Observed outcomes',description:`${observed.prospective.count} completed observations`});
    return result.filter(report=>['mae','rmse','smape','average_wql','mape','wape','mase'].some(k=>finite(report.metrics[k])));
  }
  function render(host, job) {
    if (!host) return;
    const reports=availableReports(job),doc=host.ownerDocument;
    host.hidden=!reports.length;host.replaceChildren();
    const el=(tag,text,cls)=>{const node=doc.createElement(tag);if(text!==undefined)node.textContent=text;if(cls)node.className=cls;return node;};
    const fields=[['MAE','mae',false],['RMSE','rmse',false],['sMAPE','smape',true],['Weighted quantile loss','average_wql',false],['MAPE','mape',true],['WAPE','wape',true],['MASE','mase',false]];
    for(const report of reports){
      const section=el('section',undefined,'forecast-metrics-section');section.append(el('h4',report.title),el('p',report.description,'small muted'));
      const grid=el('dl',undefined,'forecast-metrics-grid');
      for(const [label,key,percent] of fields){const value=report.metrics[key];if(!finite(value))continue;const cell=el('div');cell.append(el('dt',label),el('dd',percent?`${(100*value).toFixed(2)}%`:new Intl.NumberFormat(undefined,{maximumSignificantDigits:5}).format(value)));grid.append(cell);}
      section.append(grid);host.append(section);
    }
  }
  const api = Object.freeze({compute,render,availableReports});
  if(typeof module!=="undefined" && module.exports)module.exports=api;else root.QuanturaForecastMetrics=api;
})(typeof window === "undefined" ? globalThis : window);
