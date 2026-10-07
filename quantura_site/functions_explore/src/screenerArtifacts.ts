import {createHash} from "node:crypto";
import {gunzipSync} from "node:zlib";
import {getCache} from "@vercel/functions";

const Zip = require("adm-zip");
const SCHEMA="quantura-public-screener-v1";
const WORKFLOWS:Record<string,string>={stocks:"stock-screener.yml",games:"hourly-game-screener.yml",perps:"hourly-perpetual-screener.yml"};
const MAX_ZIP=24*1024*1024, MAX_JSON=96*1024*1024;
export type PublicSnapshot={schema_version:string;feed:string;published_at:string;data:Record<string,any>};
export type ArtifactSummary={id:number;name:string;created_at:string;digest?:string;size_in_bytes:number;expired:boolean;workflow_run:{id:number;head_branch:string;head_sha:string;head_repository_id:number;repository_id:number}};
const runtime=getCache({namespace:"quantura-public-artifacts-v1"});

export function validateSnapshot(value:any,feed:string):PublicSnapshot {
  if(value?.schema_version!==SCHEMA || value.feed!==feed || !Number.isFinite(Date.parse(value.published_at)) || Date.parse(value.published_at)>Date.now()+60_000 ||
    !value.data || typeof value.data!=="object" || Array.isArray(value.data) || !Array.isArray(value.data.items) || value.data.items.length>10000)throw Error("screener_artifact_invalid");
  return value;
}
export function decodeSnapshotZip(bytes:Buffer,feed:string,digest?:string):PublicSnapshot {
  if(bytes.length>MAX_ZIP || (digest && digest!==`sha256:${createHash("sha256").update(bytes).digest("hex")}`))throw Error("screener_artifact_digest_invalid");
  const entries=new Zip(bytes).getEntries();
  if(entries.length!==1 || entries[0].entryName!=="snapshot.json.gz" || entries[0].isDirectory || entries[0].header.size>MAX_ZIP)throw Error("screener_artifact_archive_invalid");
  const raw=gunzipSync(entries[0].getData(),{maxOutputLength:MAX_JSON});
  return validateSnapshot(JSON.parse(raw.toString("utf8")),feed);
}
export class ScreenerArtifactReader {
  private cached=new Map<string,{until:number;value:Promise<PublicSnapshot>}>();
  constructor(private request:typeof fetch=fetch,private owner="tamzid2001",private repo="stockssagemakerdata",private shared=runtime){}
  private headers(){const token=process.env.GITHUB_ACTIONS_TOKEN || process.env.GITHUB_TOKEN;return {Accept:"application/vnd.github+json","X-GitHub-Api-Version":"2026-03-10","User-Agent":"quantura-public-screener",...(token?{Authorization:`Bearer ${token}`}:{})};}
  private root(){return `https://api.github.com/repos/${encodeURIComponent(this.owner)}/${encodeURIComponent(this.repo)}`;}
  private async json(path:string){const r=await this.request(this.root()+path,{headers:this.headers(),signal:AbortSignal.timeout(15_000),redirect:"error"});if(!r.ok)throw Error("screener_artifact_provider_unavailable");return r.json();}
  async list(feed:string,date?:string):Promise<ArtifactSummary[]> {
    this.checkFeed(feed);
    if(date && (feed!=="stocks" || !/^\d{4}-\d{2}-\d{2}$/.test(date)))throw Error("screener_artifact_date_invalid");
    const name=`quantura-public-${feed}${date?`-${date}`:""}`;
    const key=`${this.owner}/${this.repo}/index/${name}`;
    let rows:any=await this.shared.get(key).catch(()=>null);
    if(!rows){const body=await this.json(`/actions/artifacts?name=${encodeURIComponent(name)}&per_page=100`);rows=body.artifacts;
      if(!Array.isArray(rows))throw Error("screener_artifact_index_invalid");
      rows=rows.filter((a:any)=>!a.expired && a.name===name && a.workflow_run?.head_branch==="main" && a.workflow_run.head_repository_id===a.workflow_run.repository_id && Number.isSafeInteger(a.id) && a.size_in_bytes<=MAX_ZIP);
      await this.shared.set(key,rows,{ttl:60}).catch(()=>{});
    }
    // Artifact IDs are allocated across partitions and are not chronological.
    return rows.sort((a:ArtifactSummary,b:ArtifactSummary)=>Date.parse(b.created_at)-Date.parse(a.created_at)||b.id-a.id);
  }
  private checkFeed(feed:string){if(!/^(stocks|games-(kalshi|polymarket_us)-[01]|perps-[0-3])$/.test(feed))throw Error("screener_artifact_feed_invalid");}
  async read(feed:string,date?:string):Promise<PublicSnapshot>{
    this.checkFeed(feed);const key=`${feed}/${date || "latest"}`;
    const hit=this.cached.get(key);if(hit && hit.until>Date.now())return hit.value;
    const value=this.load(feed,date);const entry={until:Date.now()+60_000,value};this.cached.set(key,entry);
    value.catch(()=>{if(this.cached.get(key)===entry)this.cached.delete(key);});return value;
  }
  private async download(a:ArtifactSummary,feed:string):Promise<PublicSnapshot>{
    const key=`${this.owner}/${this.repo}/${a.id}`;
    const count=await this.shared.get(`${key}/count`).catch(()=>null);
    if(typeof count==="number" && count>0 && count<=32){
      const chunks=await Promise.all(Array.from({length:count},(_,i)=>this.shared.get(`${key}/${i}`).catch(()=>null)));
      if(chunks.every(c=>typeof c==="string")){try{return validateSnapshot(JSON.parse(gunzipSync(Buffer.from(chunks.join(""),"base64"),{maxOutputLength:MAX_JSON}).toString()),feed);}catch{/* immutable cache can be refetched */}}
    }
    const run=await this.json(`/actions/runs/${a.workflow_run.id}`);
    const expected=WORKFLOWS[feed.split("-")[0]];
    if(![`.github/workflows/${expected}`,".github/workflows/screener-artifact-bootstrap.yml"].includes(run.path) || run.head_branch!=="main" || run.head_sha!==a.workflow_run.head_sha || !["schedule","workflow_dispatch"].includes(run.event) || run.repository?.id!==a.workflow_run.repository_id || run.head_repository?.id!==run.repository?.id)throw Error("screener_artifact_provenance_invalid");
    const redirect=await this.request(`${this.root()}/actions/artifacts/${a.id}/zip`,{headers:this.headers(),redirect:"manual",signal:AbortSignal.timeout(15_000)});
    if(redirect.status!==302)throw Error("screener_artifact_download_unavailable");
    const url=new URL(redirect.headers.get("location") || "https://invalid.invalid");
    if(url.protocol!=="https:" || url.username || url.password || ![".blob.core.windows.net",".githubusercontent.com"].some(domain=>url.hostname.endsWith(domain)))throw Error("screener_artifact_redirect_invalid");
    // The authenticated GitHub headers never follow the signed storage redirect.
    const r=await this.request(url,{redirect:"error",signal:AbortSignal.timeout(30_000)});
    if(!r.ok || Number(r.headers.get("content-length"))>MAX_ZIP)throw Error("screener_artifact_download_invalid");
    const chunks:Uint8Array[]=[];let size=0;const reader=r.body?.getReader();if(!reader)throw Error("screener_artifact_download_invalid");
    for(;;){const part=await reader.read();if(part.done)break;size+=part.value.length;if(size>MAX_ZIP){await reader.cancel();throw Error("screener_artifact_too_large");}chunks.push(part.value);}
    const value=decodeSnapshotZip(Buffer.concat(chunks),feed,a.digest);
    const {gzipSync}=await import("node:zlib");const packed=gzipSync(JSON.stringify(value)).toString("base64"), parts=packed.match(/.{1,1000000}/g) || [];
    if(parts.length<=32){try{await Promise.all(parts.map((part,i)=>this.shared.set(`${key}/${i}`,part,{ttl:86400})));await this.shared.set(`${key}/count`,parts.length,{ttl:86400});}catch{/* cache failure does not discard a verified publication */}}
    return value;
  }
  private async load(feed:string,date?:string){
    const dated=date?await this.list(feed,date):[];
    const artifacts=dated.length?dated:await this.list(feed);
    // Archive dates refer to scan_date, never the Actions upload date. Walk a
    // bounded retained index because retries can upload several scans per day.
    for(const artifact of artifacts.slice(0,date?40:5)){
      try{const value=await this.download(artifact,feed);if(!date || value.data.scan_date===date)return value;}catch(error){if(date)continue;if(artifact===artifacts.slice(0,5).at(-1))throw error;}
    }
    throw Error(date?"screener_snapshot_not_found":"screener_artifact_not_published");
  }
  clear(){this.cached.clear();}
}
export const screenerArtifacts=new ScreenerArtifactReader();
export async function publicGameCatalog(){
  const feeds=["kalshi","polymarket_us"].flatMap(provider=>[0,1].map(shard=>`games-${provider}-${shard}`));
  // Do not turn missing shards into an apparently complete, empty screener.
  const snapshots=await Promise.all(feeds.map(feed=>screenerArtifacts.read(feed)));
  return {items:snapshots.flatMap(s=>s.data.items as Record<string,unknown>[]),statuses:snapshots.map(s=>s.data.status || {}),source:"github_actions_artifacts"};
}
export async function publicGameById(id:string){return (await publicGameCatalog()).items.find(row=>row.id===id);}
