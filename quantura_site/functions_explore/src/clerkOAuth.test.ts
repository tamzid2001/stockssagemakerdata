import assert from "node:assert/strict";
import { test } from "node:test";
import { isClerkOAuthToken, verifyQuanturaOAuth, QUANTURA_OAUTH_RESOURCE } from "./clerkOAuth";

const user:any={id:"user_Test1",externalId:"legacy_uid",banned:false,locked:false};
const token={subject:user.id,clientId:"public-client",id:"oat_Test",scopes:["openid","profile","email"],
  revoked:false,expired:false,expiration:200,aud:[QUANTURA_OAUTH_RESOURCE]};
const deps=(change:any={})=>({verify:async(_value:string,audience:string)=>{
  assert.equal(audience,QUANTURA_OAUTH_RESOURCE);return {...token,...change};},user:async()=>user,now:()=>100000}) as any;

test("OAuth verification maps the authoritative migrated UID and checks resource",async()=>{
  const result=await verifyQuanturaOAuth("opaque-token",deps());
  assert.equal(result.userId,"legacy_uid");assert.equal(result.access.clientId,"public-client");
});
for(const [name,change] of Object.entries({revoked:{revoked:true},expired:{expired:true},seconds_expired:{expiration:99},
  no_audience:{aud:undefined},wrong_audience:{aud:["https://another-service.test"]},wrong_subject:{subject:"org_Test1"},
  missing_scope:{scopes:["openid","profile"]},missing_client:{clientId:""}})) {
  test(`OAuth rejects ${name}`,async()=>assert.rejects(verifyQuanturaOAuth("opaque-token",deps(change)),/api_key_invalid/));
}
test("banned/locked users and subject substitution cannot authenticate",async()=>{
  for(const patch of [{banned:true},{locked:true},{id:"user_Other"}]) {
    await assert.rejects(verifyQuanturaOAuth("token",{...deps(),user:async()=>({...user,...patch})}),/api_key_invalid/);
  }
});
test("unverified parsing only identifies OAuth access-token formats",()=>{
  const jwt=(header:any,payload:any)=>[header,payload].map(x=>Buffer.from(JSON.stringify(x)).toString("base64url")).join(".")+".fake";
  assert.equal(isClerkOAuthToken("oat_test"),true);
  assert.equal(isClerkOAuthToken(jwt({typ:"at+jwt"},{sub:"user_Test1"})),true);
  assert.equal(isClerkOAuthToken(jwt({typ:"JWT"},{iss:"https://clerk.quantura.studio",sid:"sess_Test1"})),false);
  assert.equal(isClerkOAuthToken("not-a-jwt"),false);
});
