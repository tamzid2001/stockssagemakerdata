import crypto from "node:crypto";
import type { Router } from "express";

const repository = "tamzid2001/stockssagemakerdata";
const workflows = {paper:"polymarket-live-paper.yml",screener:"hourly-game-screener.yml",recovery:"polymarket-paper-watchdog.yml"};
async function github(path: string, body?: unknown): Promise<any> {
  const token = process.env.GITHUB_ACTIONS_TOKEN;
  if (!token) throw new Error("WATCHDOG_UNCONFIGURED");
  const response = await fetch(`https://api.github.com/repos/${repository}${path}`,{method:body?"POST":"GET",headers:{Authorization:`Bearer ${token}`,Accept:"application/vnd.github+json","Content-Type":"application/json","X-GitHub-Api-Version":"2022-11-28"},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(10_000)});
  if (!response.ok) throw new Error("GITHUB_UNAVAILABLE");
  return response.status===204?null:response.json();
}
export function watchdogAuthorized(header: unknown, secret: string): boolean {
  if (typeof header!=="string" || !/^Bearer\s+\S+$/i.test(header)) return false;
  const supplied = Buffer.from(typeof header==="string"?header.replace(/^Bearer\s+/i,""):"");
  const expected = Buffer.from(secret);
  return expected.length>=32 && supplied.length===expected.length && crypto.timingSafeEqual(supplied,expected);
}
export function shouldRecoverScreener(runs: any[], now=Date.now()): boolean {
  if (runs.some(r=>r.status!=="completed")) return false;
  return !runs.length || now-Date.parse(runs[0].created_at)>65*60_000;
}
export function registerMarketResearchWatchdog(router: Router) {
  router.get("/health/market-research",async(_req,res)=>{
    res.setHeader("Cache-Control","no-store");
    try {
      const rows=await Promise.all(Object.entries(workflows).filter(([name])=>name!=="recovery").map(async([name,file])=>{
        const {workflow_runs:runs=[]}=await github(`/actions/workflows/${file}/runs?per_page=10&branch=main`);
        const active=runs.find((r:any)=>r.status!=="completed");
        const latest=runs[0];
        const healthy=name==="paper"?Boolean(active):Boolean(latest && Date.now()-Date.parse(latest.created_at)<90*60_000 && (active||latest.conclusion==="success"));
        return [name,{healthy,status:active?.status||latest?.conclusion||"not_started",run_url:active?.html_url||latest?.html_url||null,created_at:latest?.created_at||null}];
      }));
      const status=Object.fromEntries(rows);
      const ok=Object.values(status).every((value:any)=>value.healthy);
      res.status(ok?200:503).json({ok,paper_only:true,checked_at:new Date().toISOString(),workflows:status,disclosure:"Workflow liveness only; not proof of complete market coverage or uninterrupted quote monitoring."});
    }catch {res.status(503).json({ok:false,code:"RESEARCH_STATUS_UNAVAILABLE"});}
  });
  router.post("/internal/market-research/watchdog",async(req,res)=>{
    res.setHeader("Cache-Control","no-store");
    if (!watchdogAuthorized(req.headers.authorization,process.env.QUANTURA_RESEARCH_WATCHDOG_TOKEN||"")) {res.status(401).json({error:{code:"UNAUTHORIZED"}});return;}
    try {
      await github(`/actions/workflows/${workflows.recovery}/dispatches`,{ref:"main"});
      const {workflow_runs:runs=[]}=await github(`/actions/workflows/${workflows.screener}/runs?per_page=20&branch=main`);
      const recovered=shouldRecoverScreener(runs);
      if(recovered) await github(`/actions/workflows/${workflows.screener}/dispatches`,{ref:"main",inputs:{max_contracts:500}});
      res.status(202).json({data:{paper_recovery_requested:true,screener_recovery_requested:recovered,paper_only:true}});
    }catch {res.status(503).json({error:{code:"RESEARCH_RECOVERY_UNAVAILABLE"}});}
  });
}
