import {createHash} from "node:crypto";
import type {Router} from "express";
import type admin from "firebase-admin";
import {authenticatePlatformRequest} from "./apiAccess";
import bundledCatalog from "./sagemakerCatalog.json";

type Metrics = Partial<Record<"mae"|"rmse"|"smape"|"average_wql"|"mape"|"wape"|"mase",number>>;
export type CanvasItem = {id:string;path:string;aliases:string[];name:string;ticker:string;kind:string;frequency:string;quantiles:number[];row_count:number;start_at:string|null;end_at:string|null;metrics:Metrics|null;metrics_basis?:string;warnings:string[];uploaded_at?:string};
type Catalog = {schema_version:string;items:CanvasItem[]};
const digest=(s:string|Buffer)=>createHash("sha256").update(s).digest("hex");
const sensitive=/-----BEGIN (?:[A-Z ]+)?PRIVATE KEY-----|(?:sk_live_|sk-svcacct-|ghp_)[A-Za-z0-9_-]{16,}/;
const safePath=(p:string)=>/^sagemaker\//.test(p) && !p.split('/').some(s=>!s || s==='.' || s==='..') && !/[\\\x00-\x1f]/.test(p);
const idPattern=/^[a-f0-9]{64}$/;
export function csvTable(text:string):string[][] {
  if(!text || Buffer.byteLength(text)>8_000_000 || sensitive.test(text) || text.replace(/^\uFEFF/,'').startsWith('{\\rtf'))throw new Error('canvas_csv_invalid');
  const rows:string[][]=[];let row:string[]=[],field='',quoted=false,closed=false;
  const input=text.replace(/^\uFEFF/,'');
  for(let i=0;i<input.length;i++){
    const c=input[i];
    if(quoted){if(c==='"'){if(input[i+1]==='"'){field+='"';i++;}else{quoted=false;closed=true;}}else field+=c;continue;}
    if(c==='"'){if(field || closed)throw new Error('canvas_csv_invalid');quoted=true;}
    else if(c===',' || c==='\n' || c==='\r'){
      row.push(field.trim());field='';closed=false;
      if(c!==','){if(row.some(Boolean))rows.push(row);row=[];if(c==='\r'&&input[i+1]==='\n')i++;}
    }else {if(closed && !/\s/.test(c))throw new Error('canvas_csv_invalid');if(!closed)field+=c;}
    if(rows.length>100_000)throw new Error('canvas_csv_too_large');
  }
  if(quoted)throw new Error('canvas_csv_invalid');
  row.push(field.trim());if(row.some(Boolean))rows.push(row);
  if(rows.length<2)throw new Error('canvas_csv_invalid');return rows;
}
export function quantileColumn(name:string):number|null {
  const m=name.trim().match(/^(?:P(\d+(?:\.\d+)?)|(?:q|quantile[_ -]?)(0?\.\d+))$/i);
  if(!m)return null;const q=m[1]?Number(m[1])/100:Number(m[2]);return q>0&&q<1?q:null;
}
function timestamp(v:string):string {
  // Canvas exports use ISO dates. A date/time without an offset is UTC.
  if(!/^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/.test(v))throw new Error('canvas_timestamp_invalid');
  const dateOnly=new Date(v.slice(0,10)+'T00:00:00Z');
  if(!Number.isFinite(dateOnly.getTime()) || dateOnly.toISOString().slice(0,10)!==v.slice(0,10) || v.length>10 && Number(v.slice(11,13))>23)throw new Error('canvas_timestamp_invalid');
  const iso=v.length===10?v+'T00:00:00Z':/(?:Z|[+-]\d{2}:?\d{2})$/i.test(v)?v:v+'Z';
  const date=new Date(iso);if(!Number.isFinite(date.getTime()) || date.toISOString().slice(0,10)!==v.slice(0,10) && v.length===10)throw new Error('canvas_timestamp_invalid');return date.toISOString();
}
export function parseCanvas(text:string){
  const table=csvTable(text),headers=table[0];
  const ti=headers.findIndex(h=>/^(date|datetime|timestamp|time)$/i.test(h));
  const qs=headers.flatMap((h,i)=>{const q=quantileColumn(h);return q===null?[]:[{q,i}];}).sort((a,b)=>a.q-b.q);
  if(ti<0 || new Set(qs.map(x=>x.q)).size!==qs.length || qs.length>21)throw new Error('canvas_columns_invalid');
  const vi=headers.findIndex(h=>/^(target|price|close)$/i.test(h));
  if(!qs.length && vi<0)throw new Error('canvas_columns_invalid');
  const numeric=(v:string)=>{if(!v || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(v))throw new Error('canvas_value_invalid');const n=Number(v);if(!Number.isFinite(n))throw new Error('canvas_value_invalid');return n;};
  const predictions:Array<{timestamp:string;quantiles:Record<string,number>}>=[],history:Array<{timestamp:string;target:number}>=[];
  let previous=-Infinity;const warnings:string[]=[];
  for(const cells of table.slice(1)){
    if(cells.length!==headers.length)throw new Error('canvas_row_invalid');
    const stamp=timestamp(cells[ti]),time=Date.parse(stamp);if(time<=previous)throw new Error('canvas_timestamps_not_increasing');previous=time;
    if(qs.length){const values=qs.map(x=>numeric(cells[x.i]));if(values.some((v,i)=>i>0 && v<values[i-1]))throw new Error('canvas_quantiles_crossed');if(values.some(v=>v<0)&&!warnings.length)warnings.push('Source includes negative predictions; original values are preserved.');predictions.push({timestamp:stamp,quantiles:Object.fromEntries(qs.map((x,i)=>[String(x.q),values[i]]))});}
    else history.push({timestamp:stamp,target:numeric(cells[vi])});
  }
  const rows=predictions.length?predictions:history;
  const gaps=rows.slice(1).map((r,i)=>Date.parse(r.timestamp)-Date.parse(rows[i].timestamp)).sort((a,b)=>a-b);
  const interval=gaps[Math.floor(gaps.length/2)] || 86400_000;
  const frequency=interval>=86400_000?'1D':interval===3600_000?'1h':`${Math.max(1,Math.round(interval/60000))}min`;
  return {predictions,history,headers,preview:table.slice(1,101),quantiles:qs.map(x=>x.q),frequency,warnings,row_count:rows.length,start_at:rows[0].timestamp,end_at:rows.at(-1)!.timestamp,kind:qs.length?'forecast':'history'};
}
function settings(){return {owner:process.env.GITHUB_REPO_OWNER || 'tamzid2001',repo:process.env.GITHUB_REPO_NAME || 'stockssagemakerdata',branch:process.env.GITHUB_ACTIONS_BRANCH || 'main'};}
const token=()=>process.env.GITHUB_SAGEMAKER_TOKEN || process.env.GITHUB_ACTIONS_TOKEN || process.env.GITHUB_TOKEN || process.env.GH_TOKEN || '';
export class CanvasRepository {
  private cached:{until:number;value:Promise<Catalog>}|null=null;
  private details=new Map<string,Promise<any>>();
  constructor(private request:typeof fetch=fetch){}
  private async api(path:string,body?:any){
    if(!token())throw new Error('canvas_github_not_configured');const {owner,repo}=settings();
    const r=await this.request(`https://api.github.com/repos/${owner}/${repo}/${path}`,{method:body?'POST':'GET',headers:{Authorization:`Bearer ${token()}`,Accept:'application/vnd.github+json','Content-Type':'application/json','X-GitHub-Api-Version':'2022-11-28'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(20_000)});
    if(!r.ok)throw new Error(r.status===409||r.status===422?'canvas_commit_conflict':'canvas_github_unavailable');return r.json() as Promise<any>;
  }
  private async raw(path:string,ref?:string){
    if(!safePath(path))throw new Error('canvas_path_invalid');const {owner,repo,branch}=settings();
    const r=await this.request(`https://raw.githubusercontent.com/${owner}/${repo}/${ref || branch}/${path.split('/').map(encodeURIComponent).join('/')}`,{signal:AbortSignal.timeout(20_000)});
    if(!r.ok)throw new Error('canvas_not_found');const text=Buffer.from(await r.arrayBuffer()).toString("utf8");if(Buffer.byteLength(text)>8_000_000)throw new Error('canvas_csv_too_large');return text;
  }
  async catalog():Promise<Catalog>{
    if(!this.cached || this.cached.until<Date.now())this.cached={until:Date.now()+60_000,value:this.raw('sagemaker/catalog.json').then(s=>{const c=JSON.parse(s);if(!Array.isArray(c.items))throw new Error('canvas_catalog_invalid');return c;}).catch(()=>bundledCatalog as Catalog)};
    return this.cached.value;
  }
  async detail(id:string){
    if(!idPattern.test(id))throw new Error('canvas_not_found');
    const catalog=await this.catalog(),item=catalog.items.find(x=>x.id===id);if(!item)throw new Error('canvas_not_found');
    const key=id+':'+digest(JSON.stringify(item));
    if(!this.details.has(key))this.details.set(key,this.raw(item.path).then(text=>{
      if(digest(text)!==id)throw new Error('canvas_integrity_failed');
      let parsed;try{parsed=parseCanvas(text);}catch{const table=csvTable(text);return {item,headers:table[0],preview:table.slice(1,101),job:null};}
      const job={forecast_id:id,title:item.ticker?`${item.ticker} · ${item.name}`:item.name,source:{type:'sagemaker',provider:'sagemaker_canvas',symbol:item.ticker,name:item.name},frequency:item.frequency || parsed.frequency,quantiles:parsed.quantiles,predictions:parsed.predictions,history:parsed.history.slice(-500),input_row_count:parsed.history.length,observations:[],models:[],warnings:item.warnings,ml_metrics:item.metrics?{metrics:item.metrics,basis:item.metrics_basis || 'SageMaker Canvas · admin supplied',origin:'admin_supplied'}:null};
      return {item,headers:parsed.headers,preview:parsed.preview,job};
    }).catch(e=>{this.details.delete(key);throw e;}));
    while(this.details.size>60)this.details.delete(this.details.keys().next().value!);return this.details.get(key)!;
  }
  async publish(body:any){
    if(typeof body?.csv!=='string' || Buffer.byteLength(body.csv)>3_000_000)throw new Error('canvas_csv_too_large');
    const parsed=parseCanvas(body.csv),id=digest(body.csv);
    const name=String(body.name || '').trim(),ticker=String(body.ticker || '').trim().toUpperCase();
    if(!name || name.length>120 || !/^[A-Z0-9.^=_/-]{1,40}$/.test(ticker))throw new Error('canvas_metadata_invalid');
    const metrics:Metrics={};
    if(body.metrics){if(typeof body.metrics!=='object' || Array.isArray(body.metrics))throw new Error('canvas_metrics_invalid');for(const [k,v]of Object.entries(body.metrics)){
      if(!['mae','rmse','smape','average_wql','mape','wape','mase'].includes(k)||typeof v!=='number'||!Number.isFinite(v)||v<0||k==='smape'&&v>2)throw new Error('canvas_metrics_invalid');(metrics as any)[k]=v;
    }}
    const basis=String(body.metrics_basis || 'SageMaker Canvas · admin supplied').trim();if(basis.length>240)throw new Error('canvas_metadata_invalid');
    const item:CanvasItem={id,path:`sagemaker/uploads/${id}.csv`,aliases:[],name,ticker,kind:parsed.kind,frequency:parsed.frequency,quantiles:parsed.quantiles,row_count:parsed.row_count,start_at:parsed.start_at,end_at:parsed.end_at,metrics:Object.keys(metrics).length?metrics:null,metrics_basis:basis,warnings:parsed.warnings,uploaded_at:new Date().toISOString()};
    for(let attempt=0;attempt<3;attempt++){
      const {branch}=settings();const ref=await this.api(`git/ref/heads/${encodeURIComponent(branch)}`),base=await this.api(`git/commits/${ref.object.sha}`);
      const catalog=JSON.parse(await this.raw('sagemaker/catalog.json',ref.object.sha)) as Catalog;
      const old=catalog.items.find(x=>x.id===id);if(old){item.path=old.path;item.aliases=old.aliases;}
      catalog.items=[...catalog.items.filter(x=>x.id!==id),item];if(catalog.items.length>2000)throw new Error('canvas_catalog_full');
      const blob=await this.api('git/blobs',{content:Buffer.from(body.csv).toString('base64'),encoding:'base64'});
      const tree=await this.api('git/trees',{base_tree:base.tree.sha,tree:[{path:item.path,mode:'100644',type:'blob',sha:blob.sha},{path:'sagemaker/catalog.json',mode:'100644',type:'blob',content:JSON.stringify(catalog,null,2)+'\n'}]});
      const commit=await this.api('git/commits',{message:`Add SageMaker Canvas forecast: ${ticker}`,tree:tree.sha,parents:[ref.object.sha]});
      const {owner,repo}=settings();const update=await this.request(`https://api.github.com/repos/${owner}/${repo}/git/refs/heads/${encodeURIComponent(branch)}`,{method:'PATCH',headers:{Authorization:`Bearer ${token()}`,Accept:'application/vnd.github+json','Content-Type':'application/json'},body:JSON.stringify({sha:commit.sha,force:false}),signal:AbortSignal.timeout(20_000)});
      if(update.ok){this.cached=null;this.details.clear();return {item,commit:commit.sha,url:`/forecasting?panel=forecast&sagemakerForecastId=${id}`};}
      if(![409,422].includes(update.status))throw new Error('canvas_github_unavailable');
    }throw new Error('canvas_commit_conflict');
  }
}
export const canvasRepository=new CanvasRepository();
export async function searchCanvas(query:string,limit=8){
  const words=query.toLowerCase().replace(/^(?:sagemaker|canvas)\s*/,'').split(/\s+/).filter(Boolean);
  return (await canvasRepository.catalog()).items.filter(i=>words.every(w=>`${i.ticker} ${i.name} ${i.path}`.toLowerCase().includes(w))).slice(0,limit).map(i=>({resource_type:'sagemaker_forecast',resource_id:'sagemaker:'+i.id,id:i.id,source:'sagemaker',symbol:i.ticker || i.name,name:i.name,asset_class:'saved_forecast',forecast_available:i.kind==='forecast',history_available:i.kind==='history',quantiles:i.quantiles,forecast_url:`/forecasting?panel=forecast&sagemakerForecastId=${i.id}`}));
}
export function registerCanvasRoutes(router:Router,options:{db:FirebaseFirestore.Firestore;auth:admin.auth.Auth;adminEmails:readonly string[]}){
  const fail=(res:any,e:unknown)=>{const code=e instanceof Error?e.message:'canvas_unavailable';res.status(code==='canvas_not_found'?404:code.startsWith('canvas_github')?503:422).json({ok:false,error:code,message:'Unable to load or publish this Canvas file. Check the CSV and retry.'});};
  router.get('/sagemaker/admin/access',async(req,res)=>{res.set('Cache-Control','private, no-store');try{const p=await authenticatePlatformRequest(req,options);res.status(p.platformAdmin && !p.guest?200:403).json({ok:!!p.platformAdmin && !p.guest});}catch{res.status(401).json({ok:false,error:'sign_in_required'});}});
  router.get('/sagemaker',async(req,res)=>{try{const c=await canvasRepository.catalog();const q=String(req.query.q || '').toLowerCase();res.set('Cache-Control','public, max-age=30, stale-while-revalidate=60').json({ok:true,items:c.items.filter(i=>`${i.name} ${i.ticker} ${i.path}`.toLowerCase().includes(q))});}catch(e){fail(res,e);}});
  router.get('/sagemaker/:id',async(req,res)=>{try{res.set('Cache-Control','public, max-age=60').json({ok:true,...await canvasRepository.detail(req.params.id)});}catch(e){fail(res,e);}});
  router.post('/sagemaker',async(req,res)=>{res.set('Cache-Control','private, no-store');try{const p=await authenticatePlatformRequest(req,options);if(p.guest||!p.platformAdmin||!['clerk_session','firebase_session'].includes(p.authMethod)){res.status(403).json({ok:false,error:'admin_required'});return;}res.json({ok:true,...await canvasRepository.publish(req.body)});}catch(e){fail(res,e);}});
}
