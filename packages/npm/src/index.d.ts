export type Json = Record<string, any>;
export class QuanturaError extends Error {status:number;code:string;requestId:string|null;}
export const DEFAULT_BASE_URL:string;
export function apiBase(value?:string):string;
export class Quantura {
  constructor(options?:{token?:string|(()=>string|Promise<string>);baseUrl?:string;fetch?:typeof fetch;timeoutMs?:number});
  request(path:string,options?:{method?:string;body?:unknown;query?:Json;idempotencyKey?:string;raw?:false}):Promise<Json>;
  request(path:string,options:{method?:string;body?:unknown;query?:Json;idempotencyKey?:string;raw:true}):Promise<string>;
  search(q:string,options?:{source?:string;limit?:number;mode?:'open'|'live'|'any'}):Promise<Json>;
  resolve(url:string):Promise<Json>;models():Promise<Json>;access():Promise<Json>;
  createForecast(request:Json,options?:{idempotencyKey?:string}):Promise<Json>;
  getForecast(id:string):Promise<Json>;downloadForecast(id:string):Promise<string>;
  history(request:Json):Promise<Json|string>;
  historyPages(request:Json,options?:{maxPages?:number}):AsyncGenerator<Json>;
  askScout(context:Json,question:string,options?:{conversationId?:string;turnId?:string}):Promise<Json>;
  waitForForecast(id:string,options?:{intervalMs?:number;timeoutMs?:number}):Promise<Json>;
}
