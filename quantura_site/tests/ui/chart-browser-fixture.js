/* Browser-only visual fixture. Evaluates the production renderer, never model
 * inference or trading; synthetic prices are confined to this test file. */
(async () => {
  const source = await (await fetch('/app.js')).text();
  const host = document.getElementById('ensemble-forecast-chart');
  for (let el = host; el; el = el.parentElement) el.hidden = false;
  document.querySelector('[data-cookie-essential]')?.click();
  const declarations = source.slice(source.indexOf('  const ensembleChartDefaultRange ='), source.indexOf('  const ensembleDatasetFrequency ='));
  const renderer = source.slice(source.indexOf('  const renderEnsembleChart ='), source.indexOf('  const startEnsembleObservations ='));
  const run = new Function('host', `
    const ui = {ensembleForecastChart:host};
    const ensembleUiState = {chartWindowId:'',chartWindow:null};
    const getPlotly = () => window.QuanturaUI.loadPlotly();
    const isDarkMode = () => document.documentElement.dataset.theme === 'dark';
    const ensembleTimeZone = () => 'America/New_York';
    const ensembleQuantileKey = String;
    const ensembleQuantileLabel = q => 'P'+Math.round(q*100);
    const ensembleChartTime = (time,timeZone) => new Intl.DateTimeFormat('en-US',{timeZone,dateStyle:'medium',timeStyle:'short'}).format(new Date(time));
    const ensembleMarketIdentity = () => ({title:'TEST FIXTURE · chart layout verification'});
    const escapeHtml = value => value;
    const renderEnsembleSignals = () => {};
    ${declarations}
    ${renderer}
    return {render:renderEnsembleChart,state:ensembleUiState};
  `)(host);
  const levels = [0.01,0.1,0.25,0.5,0.75,0.9,0.99];
  const fixture = {
    forecast_id:'chart-fixture',frequency:'1D',calendar:'NYSE',source:{type:'ticker',symbol:'TEST',daily_timestamp_convention:'session_date'},
    history:[['2026-08-03',500],['2026-09-03',740],['2026-09-04',748]].map(([date,target])=>({timestamp:date+'T00:00:00Z',target})),
    quantiles:levels,
    predictions:['2026-09-08','2026-09-09','2026-09-10','2026-09-11','2026-09-14','2026-09-15','2026-09-16'].map((date,i)=>({timestamp:date+'T00:00:00Z',quantiles:Object.fromEntries(levels.map((q,k)=>[String(q),730+k*9+i*1.1]))})),
    observations:[{timestamp:'2026-09-08T20:00:00Z',interval:'1min',target:759}]
  };
  window.chartFixture = { ...run, job:fixture, host };
  host.scrollIntoView({block:'start'});
  await run.render(fixture);
  host.scrollIntoView({block:'start'});
  return {range:host.layout.xaxis.range,y:host.layout.yaxis.range,breaks:host.layout.xaxis.rangebreaks,plotly:window.Plotly.version};
})();
