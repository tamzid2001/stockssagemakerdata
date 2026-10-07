import { screenerArtifacts } from "./screenerArtifacts";
import { AlpacaClient } from "./alpacaClient";
import type { QuantScreenerDataset, QuantScreenerRow } from "./quantScreener";
import { advanceClosingSignal, decorateScreenerRow, finalizedClosingSignal, forecastRows, newYorkDate, SavedScreenerSignal, ScreenerQuote, ScreenerSignal } from "./screenerSignals";

/** Public comparison history travels with the exact publication it describes. */
export class ScreenerSignalStore {
  constructor(_db?:unknown) {}
  async read(): Promise<Map<string,SavedScreenerSignal>> {
    const data=(await screenerArtifacts.read("stocks")).data;
    return new Map(Object.entries(data.signals || {}) as [string,SavedScreenerSignal][]);
  }
  // Closing history is computed by the artifact publisher, never by a web route.
  async save(_signals: Map<string,ScreenerSignal>): Promise<number> {return 0;}
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
    try{states="signals" in dataset?new Map(Object.entries((dataset as any).signals || {}) as [string,SavedScreenerSignal][]):await this.store.read();}catch{warnings.push("Archived comparison history is temporarily unavailable.");}
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
