import { createHash } from "node:crypto";
import type { Firestore } from "firebase-admin/firestore";
import { AlpacaClient } from "./alpacaClient";
import type { QuantScreenerDataset, QuantScreenerRow } from "./quantScreener";
import { advanceClosingSignal, decorateScreenerRow, finalizedClosingSignal, forecastRows, newYorkDate, SavedScreenerSignal, ScreenerQuote, ScreenerSignal } from "./screenerSignals";

// A bounded number of state documents, not a write per ticker per minute.
const bucketId = (symbol: string) => String(createHash("sha256").update(symbol).digest()[0] % 32).padStart(2,"0");
export class ScreenerSignalStore {
  private cache: {until:number;states:Map<string,SavedScreenerSignal>} | undefined;
  constructor(private db: Firestore) {}
  async read(): Promise<Map<string,SavedScreenerSignal>> {
    if (this.cache && this.cache.until > Date.now()) return this.cache.states;
    const docs = await this.db.getAll(...Array.from({length:32},(_,i)=>this.db.collection("screener_signal_state").doc(String(i).padStart(2,"0"))));
    const states = new Map<string,SavedScreenerSignal>();
    for (const doc of docs) for (const [ticker,state] of Object.entries(doc.data()?.tickers || {})) states.set(ticker,state as SavedScreenerSignal);
    this.cache={until:Date.now()+600_000,states};return states;
  }
  async save(signals: Map<string,ScreenerSignal>): Promise<number> {
    const groups = new Map<string,Map<string,ScreenerSignal>>();
    for (const [ticker,signal] of signals) { const id=bucketId(ticker); if(!groups.has(id))groups.set(id,new Map()); groups.get(id)!.set(ticker,signal); }
    let changed=0;
    for(const [id,entries] of groups) {
      const ref=this.db.collection("screener_signal_state").doc(id);
      changed += await this.db.runTransaction(async tx=>{
        const snap=await tx.get(ref);const tickers=snap.data()?.tickers || {};let count=0;
        for(const [ticker,signal] of entries) {const previous=tickers[ticker] || {}; const next=advanceClosingSignal(previous,signal);if(next!==previous){tickers[ticker]=next;count++;}}
        if(count)tx.set(ref,{tickers,updated_at:new Date().toISOString()});return count;
      });
    }
    this.cache=undefined;return changed;
  }
}

export class ScreenerMarketService {
  private cached?: {until:number;scan:string;items:QuantScreenerRow[];warnings:string[]};
  private loading?: {scan:string;promise:Promise<{items:QuantScreenerRow[];warnings:string[]}>};
  // Keep constructor compatibility; publication snapshots need no minute-price calls.
  constructor(_alpaca: Pick<AlpacaClient,"getLatestStockPrices"|"getStockMinuteCloses"|"getStockSplits">, private store: Pick<ScreenerSignalStore,"read"|"save">, _feed="iex") {}
  async current(dataset:QuantScreenerDataset):Promise<{items:QuantScreenerRow[];warnings:string[]}> {
    if(this.cached && this.cached.until>Date.now() && this.cached.scan===dataset.scan_id)return this.cached;
    if(this.loading?.scan===dataset.scan_id)return this.loading.promise;
    const promise=this.refresh(dataset).finally(()=>{if(this.loading?.promise===promise)this.loading=undefined;});
    this.loading={scan:dataset.scan_id,promise};return promise;
  }
  archived(dataset:QuantScreenerDataset):{items:QuantScreenerRow[];warnings:string[]} {
    // Archived scans must not inherit Buy history written after their date.
    const publishedAt=Date.parse(dataset.generated_at);
    const now=Number.isFinite(publishedAt)?publishedAt:Date.parse(`${dataset.scan_date}T23:59:59Z`);
    return {items:dataset.items.map(row=>{
      const decorated=decorateScreenerRow(row,undefined,{},now);
      const signal=decorated.cutoff_p99_signal as ScreenerSignal|null;
      if(signal?.value==="buy")decorated.last_buy_signal=signal;
      decorated.archived_scan=true;
      return decorated;
    }),warnings:[]};
  }
  private async refresh(dataset:QuantScreenerDataset) {
    let states=new Map<string,SavedScreenerSignal>();const warnings:string[]=[];
    try{states=await this.store.read();}catch{warnings.push("Archived comparison history is temporarily unavailable.");}
    const items=dataset.items.map(row=>decorateScreenerRow(row,undefined,states.get(row.ticker)));
    if(items.some(r=>r.status==="success" && r.signal_status==="daily_scan_requires_refresh"))warnings.push("The previous publication remains readable while the latest-close daily scan completes.");
    this.cached={until:Date.now()+300_000,scan:dataset.scan_id,items,warnings};return this.cached;
  }
  async close(dataset:QuantScreenerDataset,now=Date.now()):Promise<{saved:number;unavailable:number}> {
    const signals=new Map<string,ScreenerSignal>();let unavailable=0;
    for(const row of dataset.items){
      const decorated=decorateScreenerRow(row,undefined,{},now);
      const signal=decorated.cutoff_p99_signal as ScreenerSignal|null;
      if(signal)signals.set(row.ticker,signal);else unavailable++;
    }
    const saved=await this.store.save(signals);this.cached=undefined;return {saved,unavailable};
  }
}
