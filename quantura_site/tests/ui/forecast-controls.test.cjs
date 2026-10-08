const test=require('node:test');
const assert=require('node:assert/strict');
test('overlays and default chart range stop at the saved forecast end',()=>{
 const {forecastObservations,forecastChartRange}=require('../../public/forecast-controls.js');
 const job={frequency:'1h',source:{type:'ticker'},history:[{timestamp:'2026-01-01T00:00:00Z',target:1}],predictions:[{timestamp:'2026-01-01T01:00:00Z'},{timestamp:'2026-01-01T02:00:00Z'}],observations:[{timestamp:'2026-01-01T01:00:00Z',target:2},{timestamp:'2026-01-01T02:00:00Z',target:3},{timestamp:'2026-01-10T00:00:00Z',target:9}]};
 assert.equal(forecastObservations(job).length,2);assert.equal(forecastChartRange(job)[1],Date.parse('2026-01-01T02:00:00Z'));
 job.frequency='1D';job.predictions=[{timestamp:'2026-01-01T00:00:00Z'}];job.observations=[{timestamp:'2026-01-01T20:00:00Z',target:3,interval:'1D'},{timestamp:'2026-01-02T20:00:00Z',target:4,interval:'1D'}];assert.equal(forecastObservations(job).length,1);
});
