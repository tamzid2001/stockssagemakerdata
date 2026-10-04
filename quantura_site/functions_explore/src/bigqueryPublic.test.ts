import assert from 'node:assert/strict';
import test from 'node:test';
import {createBigQueryPublicClient,buildBigQueryTimeSeries,validateBigQuerySource} from './bigqueryPublic';
import {validateEconomicSource} from './economicData';
const schema=[{name:'date',type:'DATE'},{name:'value',type:'FLOAT'},{name:'station',type:'STRING'}];
const source=()=>validateBigQuerySource({type:'economic_series',provider:'bigquery',dataset_id:'weather',table_id:'daily',time_field:'date',value_field:'value',dimensions:{station:"a' OR true"},limit:500});
const row=(time:string,value:number)=>({f:[{v:time},{v:String(value)}]});
function backend({estimated=1000,duplicate=false}={}){
 const calls:any[]=[],reservations:any[]=[];
 const client=createBigQueryPublicClient({projectId:'billing',maxBytes:50000,reserve:async(...args)=>{reservations.push(args);},transport:async(path,method,body)=>{
  calls.push({path,method,body});
  if(path.endsWith('/datasets/weather'))return {location:'EU'};
  if(path.endsWith('/tables/daily'))return {schema:{fields:schema}};
  if(body?.dryRun)return {totalBytesProcessed:String(estimated)};
  if(method==='POST')return {jobReference:{jobId:'job'},jobComplete:true,cacheHit:true,pageToken:'next',rows:[row('2025-01-02T00:00:00Z',2)]};
  return {jobComplete:true,rows:[row(duplicate?'2025-01-02T00:00:00Z':'2025-01-01T00:00:00Z',1)]};
 }});return {client,calls,reservations};
}
test('BigQuery source rejects private projects, injected identifiers, invalid dates and unspecified interval changes',()=>{
 assert.equal(validateEconomicSource(source()).provider,'bigquery');
 for(const extra of [{project_id:'private'},{dataset_id:'a.b'},{time_field:'date` WHERE true'},{start:'2025-02-30'},{start:'2025-01-01T00:00:00'},{frequency:'tick'},{limit:100000}])assert.throws(()=>validateBigQuerySource({...source(),...extra}));
});
test('query is parameterized, exact historical cutoff is retained, and weekly/monthly aggregations include completed intervals only',()=>{
 const sql=buildBigQueryTimeSeries({...source(),end:'2025-01-02'},schema,Date.parse('2025-01-03'));
 assert.ok(!sql.query.includes("a' OR true"));assert.equal(sql.queryParameters.find(p=>p.name==='d0')?.parameterValue.value,"a' OR true");
 assert.equal(sql.queryParameters[0].parameterValue.value,'2025-01-02T23:59:59.999Z');
 const month=buildBigQueryTimeSeries({...source(),aggregation:'avg',frequency:'1MS'},schema);
 assert.match(month.query,/HAVING time <= @end/);assert.match(month.query,/DATE_ADD/);
 const split=buildBigQueryTimeSeries({...source(),time_field:'__year_month_day'},[...schema,...['year','mo','da'].map(name=>({name,type:'STRING'}))]);assert.match(split.query,/SAFE.PARSE_DATE/);
 assert.throws(()=>buildBigQueryTimeSeries({...source(),dimensions:{absent:'1'}},schema));
});
test('dry run refuses oversized scans before reservations or a billable query',async()=>{
 const {client,calls,reservations}=backend({estimated:50001});await assert.rejects(client.history(source(),undefined,'user'),/budget/);
 assert.equal(calls.filter(c=>c.method==='POST').length,1);assert.equal(reservations.length,0);
});
test('signed-in queries preserve location, follow pagination, sort observed rows, and coalesce concurrent repeats without new budget writes',async()=>{
 const {client,calls,reservations}=backend();
 await assert.rejects(client.history(source()),/Sign in/);assert.equal(calls.length,0);
 const [a,b]=await Promise.all([client.history(source(),undefined,'user'),client.history(source(),undefined,'user')]);
 assert.deepEqual(a.rows.map(r=>r.target),[1,2]);assert.deepEqual(a,b);assert.equal(a.metadata.cache_hit,true);assert.equal(reservations.length,1);
 const dry=calls.find(c=>c.body?.dryRun),query=calls.find(c=>c.method==='POST'&&!c.body.dryRun);assert.equal(dry.body.location,'EU');assert.equal(query.body.maximumBytesBilled,'50000');
 assert.ok(calls.at(-1).path.includes('location=EU'));await client.history(source(),undefined,'user');assert.equal(reservations.length,1);
 await client.history(source(),Date.parse('2024-01-01'),'user');assert.equal(reservations.length,2,'a different historical cutoff cannot reuse a future snapshot');
});
test('ambiguous series are rejected rather than silently merged',async()=>{await assert.rejects(backend({duplicate:true}).client.history(source(),undefined,'user'),/same timestamp/);});
