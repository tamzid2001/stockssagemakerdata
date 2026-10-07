// Shared production validation; no Firebase connection or public database writes.
const fs=require('node:fs');
const path=require('node:path');
const {gzipSync}=require('node:zlib');
const {ScreenerArtifactReader,validateSnapshot}=require('../dist/screenerArtifacts');
const {decorateScreenerRow,advanceClosingSignal}=require('../dist/screenerSignals');
async function main(){
  const [input,output]=process.argv.slice(2);
  if(!input||!output)throw Error('stock_publication_paths_required');
  const data=JSON.parse(fs.readFileSync(input,'utf8'));
  if(data.schema_version!=='quantura-screener-v3'||!data.manifest?.coverage_ok||!/^\d{4}-\d{2}-\d{2}$/.test(data.scan_date))throw Error('stock_publication_not_validated');
  const reader=new ScreenerArtifactReader(fetch,'tamzid2001','stockssagemakerdata',{get:async()=>null,set:async()=>{}});
  const previous=(await reader.read('stocks')).data;
  if(previous.scan_date>data.scan_date)throw Error('stock_publication_date_regression');
  const signals=previous.signals||{};
  for(const row of data.items){
    const signal=decorateScreenerRow(row).cutoff_p99_signal;
    if(signal)signals[row.ticker]=advanceClosingSignal(signals[row.ticker]||{},signal);
  }
  data.signals=signals;
  const cutoff=new Date(Date.parse(data.scan_date+'T00:00:00Z')-13*86400000).toISOString().slice(0,10);
  data.archive_dates=[...new Set([...(previous.archive_dates||[]),previous.scan_date,data.scan_date])].filter(d=>d>=cutoff&&d<=data.scan_date).sort().reverse();
  const snapshot=validateSnapshot({schema_version:'quantura-public-screener-v1',feed:'stocks',published_at:new Date().toISOString(),data},'stocks');
  const raw=Buffer.from(JSON.stringify(snapshot));if(raw.length>96*1024*1024)throw Error('stock_publication_too_large');
  const packed=gzipSync(raw,{level:9});if(packed.length>24*1024*1024)throw Error('stock_publication_too_large');
  fs.mkdirSync(output,{recursive:true});fs.writeFileSync(path.join(output,'snapshot.json.gz'),packed);
  console.log(JSON.stringify({source:'github_actions_artifacts',scan_date:data.scan_date,items:data.items.length,bytes:packed.length,public_firestore_writes:0}));
}
main().catch(error=>{console.error(error.message);process.exitCode=1;});
