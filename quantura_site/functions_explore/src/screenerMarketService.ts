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
  private quotes = new Map<string,ScreenerQuote>();
  private splits?: {until:number;start:string;values:Array<{symbol:string;ex_date:string}>};
  constructor(private alpaca: Pick<AlpacaClient,"getLatestStockPrices"|"getStockMinuteCloses"|"getStockSplits">, private store: Pick<ScreenerSignalStore,"read"|"save">, private feed="iex") {}
  async current(dataset: QuantScreenerDataset): Promise<{items:QuantScreenerRow[];warnings:string[]}> {
    if(this.cached && this.cached.until>Date.now() && this.cached.scan===dataset.scan_id)return this.cached;
    if(this.loading?.scan===dataset.scan_id)return this.loading.promise;
    const promise=this.refresh(dataset).finally(()=>{if(this.loading?.promise===promise)this.loading=undefined;});
    this.loading={scan:dataset.scan_id,promise};return promise;
  }
  private async refresh(dataset: QuantScreenerDataset): Promise<{items:QuantScreenerRow[];warnings:string[]}> {
    const now=Date.now();const warnings:string[]=[];
    // Older publications remain readable, but never masquerade as a weekly ensemble.
    const symbols=dataset.items.filter(r=>forecastRows(r).length).map(r=>r.ticker);
    try {
      if(symbols.length)for(const [ticker,q] of await this.alpaca.getLatestStockPrices(symbols,this.feed))
        if(Date.parse(q.timestamp)+60_000<=now && (!this.quotes.has(ticker)||Date.parse(q.timestamp)>=Date.parse(this.quotes.get(ticker)!.timestamp)))
          this.quotes.set(ticker,{...q,source:`alpaca_${this.feed}_minute_close`});
    } catch {warnings.push("Latest minute bars unavailable; retained completed quote or historical close is shown with its timestamp.");}
    let states=new Map<string,SavedScreenerSignal>();
    try {states=await this.store.read();}catch{warnings.push("Saved closing signals temporarily unavailable.");}
    let actions:Array<{symbol:string;ex_date:string}>|undefined;
    // Corporate-action process dates can precede the split's effective date.
    const start=new Date(Date.parse(dataset.generated_at)-90*86400_000).toISOString().slice(0,10);
    if(symbols.length)try {
      if(!this.splits || this.splits.until<now || this.splits.start!==start)this.splits={until:now+1_800_000,start,values:await this.alpaca.getStockSplits(start,newYorkDate(new Date(now).toISOString()))};
      actions=this.splits.values;
    }catch {warnings.push("Split check unavailable; newer cross-session quotes are withheld until their price basis can be checked.");}
    const items=dataset.items.map(row=>{
      let quote=this.quotes.get(row.ticker);
      const basis=String(row.price_basis_date || row.last_forecast_update || dataset.generated_at).slice(0,10);
      const changed=actions?.some(a=>a.symbol===row.ticker && a.ex_date>basis && a.ex_date<=newYorkDate(new Date(now).toISOString()));
      // Keep the safe historical close if a newer day's split status is unknown.
      if(!actions && quote && newYorkDate(quote.timestamp)>basis)quote=undefined;
      return decorateScreenerRow({...row,corporate_action_check:actions?"checked":"unavailable",...(changed?{split_status:"requires_refresh"}:{} )},quote,states.get(row.ticker),now);
    });
    // Keep the cache bounded by the current published universe.
    const active=new Set(symbols);for(const key of this.quotes.keys())if(!active.has(key))this.quotes.delete(key);
    this.cached={until:now+60_000,scan:dataset.scan_id,items,warnings};return this.cached;
  }
  async close(dataset: QuantScreenerDataset, now=Date.now()): Promise<{saved:number;unavailable:number}> {
    const current=await this.current(dataset);const states=await this.store.read();
    const groups=new Map<string,QuantScreenerRow[]>();
    for(const row of current.items)for(const session of forecastRows(row)) {
      if(row.corporate_action_check!=="checked")continue;
      const end=Date.parse(session.session_close);
      if(end<=now && end>now-86400_000 && (!states.get(row.ticker)?.closing_signal || states.get(row.ticker)!.closing_signal!.forecast_date<session.date)) {
        if(!groups.has(session.session_close))groups.set(session.session_close,[]);groups.get(session.session_close)!.push(row);
      }
    }
    const signals=new Map<string,ScreenerSignal>();let unavailable=0;
    for(const [end,rows] of groups) {
      const quotes=await this.alpaca.getStockMinuteCloses(rows.map(r=>r.ticker),new Date(Date.parse(end)-60_000).toISOString(),this.feed);
      for(const row of rows){const quote=quotes.get(row.ticker);const signal=quote ? finalizedClosingSignal(row,{...quote,source:`alpaca_${this.feed}_minute_close`},now):null;if(signal)signals.set(row.ticker,signal);else unavailable++;}
    }
    const saved=await this.store.save(signals);this.cached=undefined;return {saved,unavailable};
  }
}
