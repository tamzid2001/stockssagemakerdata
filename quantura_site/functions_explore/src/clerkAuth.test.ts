import assert from "node:assert/strict";
import { after, test } from "node:test";
import { generateKeyPairSync, sign } from "node:crypto";

const {privateKey,publicKey}=generateKeyPairSync("rsa",{modulusLength:2048});
const previous={key:process.env.CLERK_JWT_KEY,secret:process.env.CLERK_SECRET_KEY,fetch:globalThis.fetch};
process.env.CLERK_JWT_KEY=publicKey.export({type:"spki",format:"pem"}).toString();
process.env.CLERK_SECRET_KEY="sk_test_unit_test_only";
let member=true,legacyCalls=0;
globalThis.fetch=async(input:any)=>{
  const url=String(input.url||input);
  assert.match(url,/^https:\/\/api\.clerk\.com\/v1\//);
  if(url.includes("/users/user_TestMigration"))return Response.json({object:"user",id:"user_TestMigration",external_id:"legacy_uid",first_name:"Test",last_name:null,primary_email_address_id:"email_test",email_addresses:[{object:"email_address",id:"email_test",email_address:"test@example.com",linked_to:[],verification:{status:"verified"}}],phone_numbers:[],web3_wallets:[],external_accounts:[],public_metadata:{admin:true},private_metadata:{},unsafe_metadata:{uid:"victim"},banned:false,locked:false});
  if(url.includes("/organizations/org_TestTeam/memberships"))return Response.json({data:member?[{object:"organization_membership",id:"orgmem_test",role:"org:member",organization:{object:"organization",id:"org_TestTeam",name:"Team",slug:"team",created_by:"user_Other",public_metadata:{},private_metadata:{}},public_user_data:{user_id:"user_TestMigration",identifier:"test@example.com"},public_metadata:{},private_metadata:{}}]:[],total_count:member?1:0});
  if(url.includes("/billing/subscription"))return Response.json({errors:[]},{status:404});
  throw Error("Unexpected network request in auth test");
};
const {CLERK_ISSUER,createQuanturaAuth,isClerkToken}:typeof import("./clerkAuth")=require("./clerkAuth");
const {resolveWorkspaceAccess,verifiedPlatformAdmin}:typeof import("./apiAccess")=require("./apiAccess");
const {subscriptionEntitlements}:typeof import("./clerkBilling")=require("./clerkBilling");
after(()=>{
  if(previous.key===undefined)delete process.env.CLERK_JWT_KEY;else process.env.CLERK_JWT_KEY=previous.key;
  if(previous.secret===undefined)delete process.env.CLERK_SECRET_KEY;else process.env.CLERK_SECRET_KEY=previous.secret;
  globalThis.fetch=previous.fetch;
});
function token(extra:Record<string,unknown>={}) {
  const now=Math.floor(Date.now()/1000);
  const header=Buffer.from(JSON.stringify({alg:"RS256",typ:"JWT",kid:"unit-test"})).toString("base64url");
  const payload=Buffer.from(JSON.stringify({iss:CLERK_ISSUER,azp:"https://quantura.studio",sub:"user_TestMigration",sid:"sess_TestActive",iat:now,nbf:now-5,exp:now+60,...extra})).toString("base64url");
  const data=header+"."+payload;return data+"."+sign("RSA-SHA256",Buffer.from(data),privateKey).toString("base64url");
}
const legacy={verifyIdToken:async(value:string,revoked:boolean)=>{legacyCalls++;return {uid:"native_uid",token:value,revoked};}} as any;

test("Clerk JWTs retain imported UID and use verified email, ignoring client metadata and admin claims",async()=>{
  const identity=await createQuanturaAuth(legacy).verifyIdToken(token({admin:true}));
  assert.equal(identity.uid,"legacy_uid");assert.equal(identity.clerk_user_id,"user_TestMigration");
  assert.equal(identity.email_verified,true);assert.equal(identity.admin,false);assert.equal(legacyCalls,0);
});
test("wrong origin, absent origin, wrong issuer, expired and tampered Clerk JWTs cannot fall back to Firebase",async()=>{
  const auth=createQuanturaAuth(legacy);
  for(const bad of [token({azp:"https://attacker.example"}),token({azp:undefined}),token({iss:"https://other.clerk.accounts.dev"}),token({exp:Math.floor(Date.now()/1000)-100}),token().slice(0,-12)+"AAAAAAAAAAAA"]){await assert.rejects(auth.verifyIdToken(bad));}
  assert.equal(legacyCalls,0);
});
test("administrator access follows the verified Clerk email and retained UID instead of legacy role metadata",async()=>{
  const noLegacyLookup={getUser:async()=>{throw Error("A Clerk administrator must not depend on Firebase account fields");}} as any;
  assert.equal(await verifiedPlatformAdmin(noLegacyLookup,"legacy_uid",["test@example.com"],"user_TestMigration"),true);
  assert.equal(await verifiedPlatformAdmin(noLegacyLookup,"legacy_uid",["other@example.com"],"user_TestMigration"),false);
  assert.equal(await verifiedPlatformAdmin(noLegacyLookup,"different_uid",["test@example.com"],"user_TestMigration"),false);
});
test("native Firebase credentials keep their verifier and revocation option",async()=>{
  const native=await createQuanturaAuth(legacy).verifyIdToken("native-test-token",true);
  assert.equal(native.uid,"native_uid");assert.equal(native.revoked,true);
  const sdkToken=token({iss:"https://securetoken.google.com/quantura-e2e3d"});assert.equal(isClerkToken(sdkToken),false);
});
test("organization removal denies the next request despite a cached user or stale admin claims",async()=>{
  const principal={userId:"legacy_uid",clerkUserId:"user_TestMigration",organizationId:"org_TestTeam",tokenId:null,tokenName:"Web",tokenScopes:[],plan:"free",authMethod:"clerk_session"} as any;
  const db={collection:()=>{throw Error("Organization access must not use legacy membership fallback");}} as any;
  const access=await resolveWorkspaceAccess(db,principal,"org_TestTeam");
  assert.equal(access.role,"analyst");assert.ok(access.permissions.includes("forecast.create"));
  member=false;await assert.rejects(resolveWorkspaceAccess(db,principal,"org_TestTeam"),/workspace_forbidden/);
});
test("billing permits active or cancelling paid periods and trials, while expired/past-due/other plans are denied",()=>{
  const now=Date.now(),item={plan:{slug:"pro"},status:"active",periodStart:now-1000,periodEnd:now+1000,isFreeTrial:false};
  const access=(extra:any={})=>subscriptionEntitlements({subscriptionItems:[{...item,...extra}],eligibleForFreeTrial:false} as any,now);
  assert.equal(access().docs_available,true);assert.equal(access({status:"canceled"}).docs_available,true);
  assert.equal(access({isFreeTrial:true}).subscription_status,"trialing");
  for(const extra of [{status:"past_due"},{status:"ended"},{periodEnd:now},{periodStart:now+1},{plan:{slug:"free_user"}}])assert.equal(access(extra).docs_available,false);
});
