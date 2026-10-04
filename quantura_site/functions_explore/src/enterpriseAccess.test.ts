import test from "node:test";
import assert from "node:assert/strict";
import { enterpriseGrant, requireEnterpriseApiAccess } from "./enterpriseAccess";
test("enterprise API grants require an active server-owned agreement and enforce expiry",async()=>{
  const now=Date.parse("2026-10-03T00:00Z");
  assert.equal(enterpriseGrant({tier:"enterprise",status:"active"},now),true);
  for(const value of [undefined,{tier:"pro",status:"active"},{tier:"enterprise",status:"revoked"},{tier:"enterprise",status:"active",expires_at:"invalid"},{tier:"enterprise",status:"active",expires_at:"2026-10-02"}])assert.equal(enterpriseGrant(value,now),false);
  const db={collection:(name:string)=>({doc:(id:string)=>({get:async()=>({data:()=>name==="enterprise_api_accounts"&&id==="enterprise"?{tier:"enterprise",status:"active"}:{plan:"enterprise"}})})})} as any;
  await requireEnterpriseApiAccess(db,"enterprise");
  await assert.rejects(requireEnterpriseApiAccess(db,"pro"),/enterprise_upgrade_required/);
});
