const test=require('node:test');
const assert=require('node:assert/strict');
const h=require('../../public/forecast-controls.js');
const calendar=require('../../public/market-calendars/nyse.json');
const stamp=day=>`${day}T00:00:00Z`;
const job={source:{type:'ticker',daily_timestamp_convention:'session_date'},frequency:'1D',chart_calendar:calendar,
  history:[{timestamp:stamp('2026-09-03'),target:500},{timestamp:stamp('2026-09-04'),target:740}],
  predictions:[{timestamp:stamp('2026-09-08'),quantiles:{'0.1':730,'0.9':791}}]};
test('default daily view contains one prior trading interval and forecast, not full history',()=>{
  assert.deepEqual(h.forecastChartRange(job),[Date.parse(stamp('2026-09-03')),Date.parse(stamp('2026-09-08'))]);
  const monday={...job,history:[{timestamp:stamp('2026-09-08'),target:741}],predictions:[{timestamp:stamp('2026-09-09')}]};
  assert.equal(h.forecastChartRange(monday)[0],Date.parse(stamp('2026-09-04'))); // Labor Day and weekend excluded.
});
test('minute/hour views use the selected interval, not a fixed sixty minutes',()=>{
  for(const [frequency,unit] of [['1min',60000],['1h',3600000]]) {
    const j={...job,source:{type:'prediction_market'},frequency,history:[{timestamp:'2026-09-18T14:00:00Z',target:.4}],predictions:[{timestamp:'2026-09-18T14:30:00Z'}]};
    assert.equal(h.forecastChartRange(j)[0],Date.parse(j.history[0].timestamp)-unit);
  }
});
test('visible Y scale excludes old 500 price but includes every visible tail and close',()=>{
  const t=(x,y)=>({x,y,mode:'lines'});
  const range=h.visibleForecastYRange([t([0,1,2],[500,740,745]),t([2,3],[730,750]),t([2,3],[791,780])],[1,3]);
  assert.ok(range[0]>720 && range[0]<730);assert.ok(range[1]>791 && range[1]<805);
  assert.deepEqual(h.visibleForecastYRange([t([1],[0.5])],[0,2]),[0.4995,0.5005]);
});
test('only exchange closures collapse, never a missing session or prediction-market gap',()=>{
  const breaks=h.exchangeDateBreaks(job);assert.deepEqual(breaks[0].values,['2026-09-05','2026-09-06','2026-09-07']);
  assert.ok(!breaks[0].values.includes('2026-09-04'));
  assert.deepEqual(h.exchangeDateBreaks({...job,source:{type:'prediction_market'}}),[]);
  assert.deepEqual(h.exchangeDateBreaks({...job,chart_calendar:null}),[]);
});
test('session-date history is not moved to the preceding New York date',()=>{
  assert.equal(h.stockChartTimestamp(job.history[1],job),'2026-09-04T00:00:00.000Z');
  assert.equal(h.stockChartTimestamp({timestamp:'2026-09-05T00:00:00Z',interval:'1min'},job),'2026-09-04T00:00:00.000Z');
});
test('Plotly relayout coordinates without timezone remain UTC, independent of visitor timezone',()=>{
  assert.equal(h.chartInstant('2026-09-18 14:30:00'),Date.parse('2026-09-18T14:30:00Z'));
  assert.equal(h.chartInstant('2026-09-18T14:30:00-04:00'),Date.parse('2026-09-18T18:30:00Z'));
});
test('later quote overlays keep all saved forecast rows inside the initial window',()=>{
  const j={...job,observations:[{timestamp:'2026-09-18T20:00:00Z',interval:'1min',target:760}]};
  const range=h.forecastChartRange(j);
  assert.ok(range[0]<Date.parse(j.predictions[0].timestamp));
  assert.ok(range[1]>=Date.parse(j.predictions.at(-1).timestamp));
});
