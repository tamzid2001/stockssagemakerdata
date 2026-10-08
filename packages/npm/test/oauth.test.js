import {test} from 'node:test';
import assert from 'node:assert/strict';
import {promises as fs} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {login,accessToken,logout,credentialFile} from '../src/oauth.js';

test('loopback OAuth exchanges PKCE, refreshes once and revokes an empty-response grant',async()=>{
  const directory=await fs.mkdtemp(path.join(os.tmpdir(),'quantura-oauth-test-'));
  const oldHome=process.env.QUANTURA_CONFIG_HOME;
  const oldKey=process.env.QUANTURA_API_KEY,oldToken=process.env.QUANTURA_ACCESS_TOKEN;
  process.env.QUANTURA_CONFIG_HOME=directory;
  delete process.env.QUANTURA_API_KEY;delete process.env.QUANTURA_ACCESS_TOKEN;
  const originalFetch=globalThis.fetch,originalError=console.error;
  const issuer='https://clerk.quantura.studio';let authorize,notify;
  const ready=new Promise(resolve=>{notify=resolve;});
  let exchanges=0,refreshes=0;
  globalThis.fetch=async(url,options)=>{
    if(String(url).startsWith('http://127.0.0.1'))return originalFetch(url,options);
    if(String(url).endsWith('oauth-authorization-server'))return Response.json({issuer,code_challenge_methods_supported:['S256'],authorization_endpoint:issuer+'/oauth/authorize',token_endpoint:issuer+'/oauth/token',revocation_endpoint:issuer+'/oauth/token/revoke'});
    const form=new URLSearchParams(options.body);
    assert.equal(form.get('client_id'),'pOqhsrxJxggZ8m8a');
    if(String(url).endsWith('/revoke'))return new Response(null,{status:200});
    assert.equal(form.get('resource'),'https://quantura.studio');
    if(form.get('grant_type')==='authorization_code'){
      exchanges++;assert.equal(form.get('code'),'fake-code');
      const {createHash}=await import('node:crypto');
      assert.equal(createHash('sha256').update(form.get('code_verifier')).digest('base64url'),authorize.searchParams.get('code_challenge'));
      return Response.json({access_token:'fake-original',refresh_token:'fake-refresh',token_type:'Bearer',expires_in:3600});
    }
    refreshes++;return Response.json({access_token:'fake-refreshed',refresh_token:'fake-rotated',token_type:'Bearer',expires_in:3600});
  };
  console.error=message=>{authorize=new URL(String(message).split('\n').at(-1));notify();};
  try {
    const pending=login({noBrowser:true});await ready;
    const callback=new URL('http://127.0.0.1:8766/callback');
    callback.searchParams.set('state','wrong');callback.searchParams.set('iss',issuer);callback.searchParams.set('code','fake-code');
    assert.equal((await originalFetch(callback)).status,400);assert.equal(exchanges,0);
    callback.searchParams.set('state',authorize.searchParams.get('state'));
    assert.equal((await originalFetch(callback)).status,200);await pending;assert.equal(exchanges,1);
    assert.equal((await fs.stat(credentialFile())).mode & 0o777,0o600);
    assert.equal(await accessToken(),'fake-original');
    await assert.rejects(accessToken('https://untrusted.test'),/another API origin/);
    const value=JSON.parse(await fs.readFile(credentialFile(),'utf8'));value.expires_at=0;
    await fs.writeFile(credentialFile(),JSON.stringify(value));
    assert.deepEqual(await Promise.all([accessToken(),accessToken()]),['fake-refreshed','fake-refreshed']);assert.equal(refreshes,1);
    assert.equal(await logout(),true);await assert.rejects(fs.stat(credentialFile()),{code:'ENOENT'});
  }finally{
    globalThis.fetch=originalFetch;console.error=originalError;
    for(const [key,value] of Object.entries({QUANTURA_CONFIG_HOME:oldHome,QUANTURA_API_KEY:oldKey,QUANTURA_ACCESS_TOKEN:oldToken}))value===undefined?delete process.env[key]:process.env[key]=value;
    await fs.rm(directory,{recursive:true,force:true});
  }
});
