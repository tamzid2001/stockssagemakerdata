import { PLAN_ENTITLEMENTS, type PlanKey } from "./planEntitlements";

export type Admission = { tokens:number; refreshedAt:number; leases:Record<string,number> };
export const ADMISSION_LEASE_MS=11*60*60*1000; // 6h queue timeout + 5h worker lease

export function reserveForecast(current:Partial<Admission>,plan:PlanKey,jobId:string,dailyCount:number,now=Date.now()):Admission {
  const limit=PLAN_ENTITLEMENTS[plan].forecastComputePerDay;
  if(limit>=0 && dailyCount>=limit)throw Error("forecast_daily_quota_exceeded");
  const capacity=plan==="research"?5:["pro","quant"].includes(plan)?3:1;
  const leases=Object.fromEntries(Object.entries(current.leases||{}).filter(([,expires])=>Number(expires)>now));
  if(Object.keys(leases).length>=capacity)throw Error("forecast_concurrent_limit_exceeded");
  const elapsed=Math.max(0,now-Number(current.refreshedAt ?? now));
  const tokens=Math.min(capacity,Number(current.tokens ?? capacity)+elapsed*capacity/60000);
  if(tokens<1)throw Error("forecast_rate_limit_exceeded");
  leases[jobId]=now+ADMISSION_LEASE_MS;
  return {tokens:tokens-1,refreshedAt:now,leases};
}

export function releaseForecast(current:Partial<Admission>,jobId:string):Record<string,number> {
  const leases={...(current.leases||{})};delete leases[jobId];return leases;
}
