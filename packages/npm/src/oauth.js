import {randomBytes,createHash} from 'node:crypto';
import {createServer} from 'node:http';
import {spawn} from 'node:child_process';
import {promises as fs} from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const ISSUER='https://clerk.quantura.studio';
const CLIENT_ID='pOqhsrxJxggZ8m8a';
const REDIRECT='http://127.0.0.1:8766/callback';
const RESOURCE='https://quantura.studio';
export const credentialFile=()=>path.join(process.env.QUANTURA_CONFIG_HOME || path.join(os.homedir(),'.config','quantura'),'credentials.json');
export function pkce() { const verifier=randomBytes(32).toString('base64url');return {verifier,challenge:createHash('sha256').update(verifier).digest('base64url'),state:randomBytes(32).toString('base64url')}; }
export function validCallback(url,state) { return url.pathname==='/callback' && url.searchParams.get('state')===state && url.searchParams.get('iss')===ISSUER; }
async function save(value) {
  const target=credentialFile();await fs.mkdir(path.dirname(target),{recursive:true,mode:0o700});
  const temp=target+'.'+randomBytes(12).toString('hex');
  try {await fs.writeFile(temp,JSON.stringify(value),{mode:0o600,flag:'wx'});await fs.rename(temp,target);}
  finally {await fs.rm(temp,{force:true});}
}
async function load() {
  const target=credentialFile();const stat=await fs.lstat(target);
  if(stat.isSymbolicLink())throw new Error('Refusing a symlinked credentials file.');
  return JSON.parse(await fs.readFile(target,'utf8'));
}
async function metadata() {
  const response=await fetch(`${ISSUER}/.well-known/oauth-authorization-server`,{redirect:'error',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw new Error('OAuth discovery is unavailable.');
  const value=await response.json();
  if(value.issuer!==ISSUER || !value.code_challenge_methods_supported?.includes('S256'))throw new Error('OAuth discovery is invalid.');
  for(const key of ['authorization_endpoint','token_endpoint','revocation_endpoint']) {
    if(new URL(value[key]).origin!==ISSUER)throw new Error('OAuth endpoint is invalid.');
  }
  return value;
}
async function exchange(endpoint,body) {
  const response=await fetch(endpoint,{method:'POST',redirect:'error',signal:AbortSignal.timeout(20000),
    headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams(body)});
  if(!response.ok)throw new Error('OAuth token exchange failed. Run quantura login again.');
  const value=await response.json();
  if(!value.access_token || String(value.token_type).toLowerCase()!=='bearer')throw new Error('OAuth token response is invalid.');
  return value;
}
export async function login({noBrowser=false}={}) {
  const info=await metadata(),proof=pkce();
  const authorize=new URL(info.authorization_endpoint);
  for(const [k,v] of Object.entries({client_id:CLIENT_ID,redirect_uri:REDIRECT,response_type:'code',scope:'openid profile email offline_access',
    resource:RESOURCE,state:proof.state,code_challenge:proof.challenge,code_challenge_method:'S256'}))authorize.searchParams.set(k,v);
  let resolve,reject; const completion=new Promise((a,b)=>{resolve=a;reject=b;});
  let consumed=false;
  const server=createServer(async(req,res)=>{
    const callback=new URL(req.url,REDIRECT);
    if(req.method!=='GET' || !validCallback(callback,proof.state)) {res.writeHead(400);res.end('Invalid OAuth callback.');return;}
    if(consumed){res.writeHead(409);res.end('Callback already used.');return;}
    consumed=true;
    if(callback.searchParams.get('error') || !callback.searchParams.get('code')) {res.writeHead(400);res.end('Sign-in was canceled.');reject(new Error('Sign-in was canceled.'));return;}
    try {
      const value=await exchange(info.token_endpoint,{grant_type:'authorization_code',client_id:CLIENT_ID,redirect_uri:REDIRECT,
        resource:RESOURCE,code:callback.searchParams.get('code'),code_verifier:proof.verifier});
      await save({...value,client_id:CLIENT_ID,resource:RESOURCE,expires_at:Date.now()/1000+Number(value.expires_in || 3600)});
      res.setHeader('Content-Type','text/html; charset=utf-8');res.end('<h1>Signed in to Quantura</h1><p>You can close this window and return to your terminal.</p>');resolve();
    }catch {res.writeHead(400);res.end('Sign-in could not complete. Try again from your terminal.');reject(new Error('OAuth token exchange failed.'));}
  });
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(8766,'127.0.0.1',resolve);});
  const timer=setTimeout(()=>reject(new Error('Sign-in timed out after five minutes.')),300000);
  try {
    console.error('Sign in and approve Quantura access in your browser:\n'+authorize.href);
    if(!noBrowser) {
      const [command,args]=process.platform==='darwin'?['open',[authorize.href]]:process.platform==='win32'?['rundll32',['url.dll,FileProtocolHandler',authorize.href]]:['xdg-open',[authorize.href]];
      const child=spawn(command,args,{stdio:'ignore'});child.on('error',()=>{});child.unref();
    }
    await completion;
  }finally {clearTimeout(timer);server.close();}
}
let refreshing;
export async function accessToken(baseUrl=RESOURCE) {
  if(process.env.QUANTURA_API_KEY || process.env.QUANTURA_ACCESS_TOKEN)return process.env.QUANTURA_API_KEY || process.env.QUANTURA_ACCESS_TOKEN;
  let value;try{value=await load();}catch{throw new Error('Run quantura login or set QUANTURA_API_KEY.');}
  if(new URL(baseUrl).origin!==value.resource || value.resource!==RESOURCE || value.client_id!==CLIENT_ID)throw new Error('Saved OAuth credentials belong to another API origin.');
  if(Number(value.expires_at)>Date.now()/1000+60)return value.access_token;
  if(!value.refresh_token)throw new Error('Sign-in expired. Run quantura login.');
  if(!refreshing)refreshing=(async()=>{
    const info=await metadata();
    try {
      const fresh=await exchange(info.token_endpoint,{grant_type:'refresh_token',client_id:CLIENT_ID,resource:RESOURCE,refresh_token:value.refresh_token});
      const next={...value,...fresh,expires_at:Date.now()/1000+Number(fresh.expires_in || 3600)};
      await save(next);return next.access_token;
    }catch(error){const reread=await load();if(reread.access_token!==value.access_token && reread.expires_at>Date.now()/1000+60)return reread.access_token;throw error;}
  })().finally(()=>{refreshing=undefined;});
  return refreshing;
}
export async function logout() {
  let revoked=false;
  try {const value=await load(),info=await metadata();const response=await fetch(info.revocation_endpoint,{method:'POST',redirect:'error',signal:AbortSignal.timeout(15000),
    headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({client_id:CLIENT_ID,token:value.refresh_token || value.access_token})});revoked=response.ok;}catch{}
  await fs.rm(credentialFile(),{force:true});return revoked;
}
