const test=require('node:test');
const assert=require('node:assert/strict');
const controls=require('../../public/forecast-controls.js');
const market=require('../../public/q-market.js');
test('forecast durations preserve calendar months and convert fixed intervals exactly',()=>{
  assert.equal(controls.durationBars(3,'months','1Month'),3);
  assert.equal(controls.durationBars(2,'weeks','1Week'),2);
  assert.equal(controls.durationBars(2,'hours','5m'),24);
  assert.equal(controls.durationBars(12,'hours','4h'),3);
  assert.throws(()=>controls.durationBars(30,'days','1MS'),/calendar date/);
  assert.equal(controls.frequencyMeta('1W-MON').timeframe,'1Week');
  assert.deepEqual(controls.forecastChartRange({frequency:'1MS',history:[{timestamp:'2024-03-01T00:00:00Z'}],predictions:[{timestamp:'2024-04-01T00:00:00Z'}]}),[Date.parse('2024-02-01'),Date.parse('2024-04-01')]);
});
test('downloads submit the selected interval rather than defaulting to daily or hourly',()=>{
  const row={resource_type:'instrument',resource_id:'kalshi_perps:KXBTCPERP',symbol:'KXBTCPERP',source:'kalshi_perps',asset_class:'perpetual',name:'BTC'};
  for(const [input,expected] of [['5m','5min'],['15m','15min'],['30m','30min'],['4h','4h'],['1Week','1W-MON'],['1Month','1MS']]) {
    const request=market.requestFor(row,{frequency:input,limit:500,kind:'stocks'});
    assert.equal(request.body.frequency,expected);
  }
});
