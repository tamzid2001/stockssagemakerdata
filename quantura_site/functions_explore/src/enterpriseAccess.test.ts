import test from "node:test";
import assert from "node:assert/strict";
import { enterpriseGrant, requirePaidApiAccess, paidApiEntitlement } from "./enterpriseAccess";
import { verifiedQuanturaAdmin } from "./clerkAuth";
test("enterprise API grants require an active server-owned agreement and enforce expiry",async()=>{
  const now=Date.parse("2026-10-03T00:00Z");
  assert.equal(enterpriseGrant({tier:"enterprise",status:"active"},now),true);
  for(const value of [undefined,{tier:"pro",status:"active"},{tier:"enterprise",status:"revoked"},{tier:"enterprise",status:"active",expires_at:"invalid"},{tier:"enterprise",status:"active",expires_at:"2026-10-02"}])assert.equal(enterpriseGrant(value,now),false);
  const db={collection:(name:string)=>({doc:(id:string)=>({get:async()=>({data:()=>name==="enterprise_api_accounts"&&id==="enterprise"?{tier:"enterprise",status:"active"}:name==="billing_accounts"&&id==="paid"?{subscriptionStatus:"active"}:{plan:"enterprise"}})})})} as any;
  await requirePaidApiAccess(db,"enterprise");
  await requirePaidApiAccess(db,"paid");
  await assert.rejects(requirePaidApiAccess(db,"pro"),/paid_api_required/);
});

test("complimentary administrator Pro access requires the authoritative primary verified email and original UID",()=>{
 const user={id:"user_admin",externalId:"legacy",primaryEmailAddressId:"primary",emailAddresses:[{id:"primary",emailAddress:"tamzid257@gmail.com",verification:{status:"verified"}}],banned:false,locked:false};
 assert.equal(verifiedQuanturaAdmin(user,"legacy"),true);
 for(const change of [{banned:true},{locked:true},{primaryEmailAddressId:"other"},{externalId:"other"},{emailAddresses:[{...user.emailAddresses[0],verification:{status:"unverified"}}]}])assert.equal(verifiedQuanturaAdmin({...user,...change},"legacy"),false);
});

test("Pro API access excludes trials, past-due and expired periods, but includes paid cancellation periods",()=>{
 const paid={docs_available:true,subscription_status:"active",trial_ends_at:null};
 assert.equal(paidApiEntitlement(paid),true);assert.equal(paidApiEntitlement({...paid,subscription_status:"canceled"}),true);
 for(const extra of [{subscription_status:"trialing"},{subscription_status:"past_due"},{docs_available:false},{trial_ends_at:"2027-01-01"}])assert.equal(paidApiEntitlement({...paid,...extra}),false);
});
