export const DEFAULT_BASE_URL = 'https://quantura.studio/api/v1';

export class QuanturaError extends Error {
  constructor(message, {status=0, code='REQUEST_FAILED', requestId=null}={}) {
    super(message); this.name='QuanturaError'; this.status=status; this.code=code; this.requestId=requestId;
  }
}

export function apiBase(value=DEFAULT_BASE_URL) {
  const url=new URL(value);
  if (url.username || url.password || url.search || url.hash
      || url.protocol!=='https:' && !(url.protocol==='http:' && ['localhost','127.0.0.1','[::1]'].includes(url.hostname))) {
    throw new TypeError('Use an HTTPS API URL, or a loopback HTTP URL for development.');
  }
  return url.href.replace(/\/$/,'')+(url.pathname==='/'?'/api/v1':'');
}

export class Quantura {
  constructor({token,baseUrl=DEFAULT_BASE_URL,fetch:transport=globalThis.fetch,timeoutMs=60000}={}) {
    this.baseUrl=apiBase(baseUrl); this.transport=transport; this.timeoutMs=timeoutMs;
    this.token=token ?? (()=>typeof process!=='undefined'?(process.env.QUANTURA_API_KEY || process.env.QUANTURA_ACCESS_TOKEN || ''):'');
  }
  async request(path,{method='GET',body,query={},idempotencyKey,raw=false}={}) {
    if (!path || path.startsWith('/') || /(^|\/)\.\.?($|\/)|:/.test(path)) throw new TypeError('Use a relative Quantura API path.');
    const url=new URL(`${this.baseUrl}/${path}`);
    if(url.origin!==new URL(this.baseUrl).origin || !url.pathname.startsWith(new URL(this.baseUrl).pathname+'/'))throw new TypeError('Use a relative Quantura API path.');
    for (const [key,value] of Object.entries(query)) if(value!==undefined && value!==null) url.searchParams.set(key,String(value));
    const token=typeof this.token==='function'?await this.token():this.token;
    if (!token) throw new QuanturaError('Run quantura login or set QUANTURA_API_KEY.',{code:'AUTH_REQUIRED'});
    // Mutations are never automatically retried. Forecast callers can reuse
    // an explicit idempotency key after an ambiguous transport failure.
    const response=await this.transport(url,{method,redirect:'error',signal:AbortSignal.timeout(this.timeoutMs),
      headers:{Authorization:`Bearer ${token}`,Accept:raw?'text/csv':'application/json',
        ...(body!==undefined?{'Content-Type':'application/json'}:{}),...(idempotencyKey?{'Idempotency-Key':idempotencyKey}:{})},
      ...(body!==undefined?{body:JSON.stringify(body)}:{})});
    const text=await response.text();
    let result; try { result=JSON.parse(text); } catch {
      if(raw && response.ok)return text;
      if(response.ok)throw new QuanturaError('The API did not return JSON.',{code:'INVALID_RESPONSE'});
      result={};
    }
    if(!response.ok) {
      const error=typeof result.error==='object'?result.error:{code:result.error,message:result.message};
      throw new QuanturaError(error?.message || `Quantura request failed (${response.status}).`,
        {status:response.status,code:error?.code || 'REQUEST_FAILED',requestId:error?.request_id || response.headers.get('x-request-id')});
    }
    if(raw)return text;
    if(result===undefined)throw new QuanturaError('The API did not return JSON.',{code:'INVALID_RESPONSE'});
    return result;
  }
  search(q,{source='auto',limit=8,mode='open'}={}) {return this.request('market-search',{query:{q,source,limit,mode}});}
  resolve(url) {return this.request('market-search/resolve',{query:{url}});}
  models() {return this.request('forecast/models');}
  access() {return this.request('me/access');}
  createForecast(request,{idempotencyKey=globalThis.crypto.randomUUID()}={}) {
    return this.request('ensemble-forecasts',{method:'POST',body:request,idempotencyKey});
  }
  getForecast(id) {return this.request(`ensemble-forecasts/${encodeURIComponent(id)}`);}
  downloadForecast(id) {return this.request(`ensemble-forecasts/${encodeURIComponent(id)}/download`,{raw:true});}
  stampForecast(id) {return this.request(`ensemble-forecasts/${encodeURIComponent(id)}/proof`,{method:'POST',body:{}});}
  verifyForecast(id) {return this.request(`ensemble-forecasts/${encodeURIComponent(id)}/proof`,{query:{verify:true}});}
  downloadForecastProof(id) {return this.request(`ensemble-forecasts/${encodeURIComponent(id)}/proof`,{raw:true});}
  history(request) {return this.request('market-data/stocks/history',{method:'POST',body:request,raw:request.format==='csv'});}
  async *historyPages(request,{maxPages=1000}={}) {
    const seen=new Set(); let cursor=request.cursor;
    for(let i=0;i<maxPages;i++) {
      const page=await this.history({...request,format:'json',page_mode:true,...(cursor?{cursor}:{})});
      yield page; const next=page.next_cursor ?? page.data?.next_cursor;
      if(!next)return;
      if(typeof next!=='string' || seen.has(next) || next===cursor)throw new QuanturaError('History pagination stalled.',{code:'PAGINATION_STALLED'});
      seen.add(next); cursor=next;
    }
    throw new QuanturaError('History exceeds maxPages. Continue from the last cursor.',{code:'PAGE_LIMIT'});
  }
  askScout(context,question,{conversationId,turnId=globalThis.crypto.randomUUID()}={}) {
    return this.request('jev/forecast-questions',{method:'POST',body:{context,question,turn_id:turnId,...(conversationId?{conversation_id:conversationId}:{})}});
  }
  async waitForForecast(id,{intervalMs=5000,timeoutMs=900000}={}) {
    if(intervalMs<1000 || timeoutMs<=0)throw new TypeError('Use intervalMs >= 1000 and a positive timeout.');
    const deadline=Date.now()+timeoutMs;
    do {
      const value=await this.getForecast(id); const job=value.data ?? value;
      if(job.status==='completed')return value;
      if(['failed','cancelled','canceled'].includes(job.status))throw new QuanturaError('Forecast failed. Inspect its error and reference.',{code:job.error?.code || 'FORECAST_FAILED'});
      if(Date.now()+intervalMs>=deadline)break;
      await new Promise(resolve=>setTimeout(resolve,intervalMs));
    }while(Date.now()<deadline);
    throw new QuanturaError('Forecast is still running. Poll its ID again.',{code:'FORECAST_TIMEOUT'});
  }
}
