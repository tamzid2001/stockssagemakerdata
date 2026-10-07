import { createHash, randomUUID } from "node:crypto";
import type { Router, Response } from "express";
import type admin from "firebase-admin";
import { authenticatePlatformRequest, authorizeWorkspaceAction, requireScope, requireWorkspacePermission, resolveWorkspaceAccess, type ApiPrincipal } from "./apiAccess";
import { loadInputRows, loadPublishedForecast, publicEnsembleJob, validateWorkerResult } from "./ensembleForecastRoutes";
import { publicGameById } from "./screenerArtifacts";
import { publicGameForecast } from "./gameForecasts";
import { askForecastQuestion, contextHash, normalizeQuestionContext, parseForecastQuestion, type ForecastContext } from "./forecastQuestions";

type Options = {db:FirebaseFirestore.Firestore;auth:admin.auth.Auth;adminEmails?:readonly string[];ask?:typeof askForecastQuestion};
type Reference = {kind:"ensemble"|"screener"|"game"|"saved_game"|"preview";id?:string;symbol?:string;scan_id?:string;name?:string;frequency?:string;rows?:Array<{timestamp:string;target:number}>};
const uuid=(v:unknown):v is string=>typeof v==="string" && /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(v);
export function parseContextReference(raw:any): Reference {
  if(!raw || typeof raw!=="object" || Array.isArray(raw))throw new Error("question_reference_invalid");
  const keys:Record<string,string[]>={ensemble:["kind","id"],screener:["kind","symbol","scan_id"],game:["kind","id"],saved_game:["kind","id"],preview:["kind","name","frequency","rows"]};
  if(!keys[raw.kind] || Object.keys(raw).some(k=>!keys[raw.kind].includes(k)))throw new Error("question_reference_invalid");
  if(raw.kind==="preview"){
    if(typeof raw.name!=="string" || !raw.name.trim() || raw.name.length>120 || typeof raw.frequency!=="string" || raw.frequency.length>40 || !Array.isArray(raw.rows) || raw.rows.length<2 || raw.rows.length>500)throw new Error("question_reference_invalid");
    return {kind:"preview",name:raw.name.trim(),frequency:raw.frequency,rows:raw.rows.map((r:any)=>({timestamp:r?.timestamp,target:r?.target}))};
  }
  if(raw.kind==="screener"){
    if(!/^[A-Za-z0-9._-]{1,40}$/.test(raw.symbol || "") || !/^[A-Za-z0-9._-]{1,160}$/.test(raw.scan_id || ""))throw new Error("question_reference_invalid");
    return {kind:"screener",symbol:raw.symbol.toUpperCase(),scan_id:raw.scan_id};
  }
  if(typeof raw.id!=="string" || !/^[A-Za-z0-9_-]{1,220}$/.test(raw.id))throw new Error("question_reference_invalid");
  return {kind:raw.kind,id:raw.id};
}
export async function loadQuestionContext(db:FirebaseFirestore.Firestore, principal:ApiPrincipal, reference:Reference):Promise<ForecastContext> {
  requireScope(principal,"forecasts:read");
  if(principal.guest)throw new Error("question_sign_in_required");
  if(reference.kind==="preview")return normalizeQuestionContext({title:reference.name,history:reference.rows,frequency:reference.frequency,source:{type:"csv_preview",provider:"user_csv"}});
  if(reference.kind==="screener"){
    const s=await loadPublishedForecast(db,reference.symbol!,reference.scan_id!);validateWorkerResult(s.result,s.job);
    return normalizeQuestionContext({...publicEnsembleJob("published",s.job,s.result),history:s.history});
  }
  if(reference.kind==="ensemble"){
    const doc=await db.collection("ensemble_forecast_jobs").doc(reference.id!).get();
    if(!doc.exists)throw new Error("question_context_not_found");
    const job=doc.data()!;const access=await resolveWorkspaceAccess(db,principal,job.workspace_id);
    authorizeWorkspaceAction(principal,access,"forecasts:read","read");requireWorkspacePermission(access,"forecast.read",reference.id);
    if(job.status!=="completed")throw new Error("question_context_not_ready");
    const result=(await db.collection("ensemble_forecast_results").doc(reference.id!).get()).data();
    if(!result)throw new Error("question_context_not_found");
    return normalizeQuestionContext({...publicEnsembleJob(reference.id!,job,result),history:Array.isArray(result.selected_history)?result.selected_history:(await loadInputRows(doc.ref,2)).slice(-500)});
  }
  let item:any;
  if(reference.kind==="saved_game"){
    const doc=await db.collection("user_game_forecasts").doc(reference.id!).get();
    if(!doc.exists)throw new Error("question_context_not_found");
    if(doc.data()?.ownerUid!==principal.userId)throw new Error("workspace_forbidden");
    item=doc.data()?.forecast;
  }else{
    const raw=await publicGameById(reference.id!);
    item=raw?publicGameForecast(raw,Date.now(),true,true):null;
  }
  if(!item)throw new Error("question_context_not_found");
  return normalizeQuestionContext({title:`${item.event_title} · ${item.outcome}`,source:{type:"prediction_market",provider:item.provider,symbol:item.symbol,outcome:item.outcome,input_cutoff_at:item.input_cutoff},
    frequency:"1h",history:item.observations,predictions:item.predictions,models:item.models,warnings:item.warnings,input_row_count:item.history_count,generated_at:item.recomputed_at || item.generated_at});
}
export function validateNotes(raw:any, context:ForecastContext) {
  if(!Array.isArray(raw) || raw.length>20)throw new Error("question_notes_invalid");
  const ids=new Set<string>();
  return raw.map((n:any)=>{
    if(!uuid(n?.id) || ids.has(n.id) || typeof n.text!=="string" || !n.text.trim() || n.text.length>240 || typeof n.timestamp!=="string" || typeof n.series!=="string" || Object.keys(n).some(k=>!["id","text","timestamp","series"].includes(k)))throw new Error("question_notes_invalid");
    ids.add(n.id);
    const value=n.series==="history"?context.history.find(r=>r.timestamp===n.timestamp)?.target:context.predictions.find(r=>r.timestamp===n.timestamp)?.quantiles[n.series];
    if(typeof value!=="number" || !Number.isFinite(value))throw new Error("question_notes_invalid");
    return {id:n.id,text:n.text.trim(),timestamp:n.timestamp,series:n.series,value};
  });
}
function failure(res:Response,error:any,requestId:string) {
  const message=String(error?.message || "");
  const status=/api_key_(missing|invalid|expired|revoked)|question_sign_in/.test(message)?401:/forbidden|scope_required|permission|plan_upgrade|required_entitlement/.test(message)?403:/not_found/.test(message)?404:/rate_limited/.test(message)?429:/busy|context_changed|not_ready|conversation_full/.test(message)?409:/question_.*invalid/.test(message)?422:503;
  if(status===429)res.setHeader("Retry-After","60");
  const details:Record<number,[string,string]>={401:["SIGN_IN_REQUIRED","Sign in to ask Scout about this forecast."],403:["ACCESS_DENIED","You no longer have access to this forecast or this API operation."],404:["CONTEXT_NOT_FOUND","This saved forecast is unavailable. Reopen a published or completed forecast."],409:["CONTEXT_CONFLICT","The forecast changed or this conversation is busy or full. Reopen the forecast or start a new conversation."],422:["INVALID_QUESTION","Use a short question without secrets and a valid forecast reference."],429:["RATE_LIMITED","Scout’s request limit has been reached. Try again later."],503:["JEV_UNAVAILABLE","Scout is temporarily unavailable. Your question was not answered; please retry."]};
  res.status(status).json({error:{code:details[status][0],message:details[status][1],request_id:requestId}});
}
export function registerForecastQuestionRoutes(router:Router,options:Options) {
  const authenticate=(req:any)=>authenticatePlatformRequest(req,options);
  router.post("/v1/jev/forecast-questions",async(req,res)=>{
    const requestId=randomUUID();res.set({"Cache-Control":"private, no-store","X-Request-ID":requestId});
    let conversationRef:FirebaseFirestore.DocumentReference|undefined,turnId="",leased=false;
    try {
      const principal=await authenticate(req);
      if(Object.keys(req.body || {}).some(k=>!["context","question","conversation_id","turn_id"].includes(k)))throw new Error("question_request_invalid");
      const reference=parseContextReference(req.body?.context),question=parseForecastQuestion(req.body?.question);
      const context=await loadQuestionContext(options.db,principal,reference),hash=contextHash(context);
      const conversationId=req.body?.conversation_id || randomUUID();turnId=req.body?.turn_id;
      if(!uuid(conversationId) || !uuid(turnId))throw new Error("question_request_invalid");
      conversationRef=options.db.collection("jev_forecast_conversations").doc(conversationId);
      const now=Date.now(),day=Math.floor(now/86400000),minute=Math.floor(now/60000);
      const userHash=createHash("sha256").update(principal.userId).digest("hex");
      const quota=options.db.collection("quantura_api_rate_windows").doc(`jev_user_${userHash}`),global=options.db.collection("quantura_api_rate_windows").doc(`jev_global_${day}`);
      const claim=await options.db.runTransaction(async tx=>{
        const [snapshot,userBudget,globalBudget]=await Promise.all([tx.get(conversationRef!),tx.get(quota),tx.get(global)]);
        const saved=snapshot.data(),messages=saved?.messages || [];
        if(snapshot.exists && (saved?.owner_uid!==principal.userId || saved?.context_hash!==hash))throw new Error("question_context_changed");
        const replay=messages.find((m:any)=>m.turn_id===turnId);
        if(replay){if(replay.question!==question)throw new Error("question_request_invalid");return {replay,previous:messages};}
        if(saved?.pending?.until>now || messages.length>=30)throw new Error("question_busy");
        const b=userBudget.data() || {},g=globalBudget.data() || {};
        const mc=b.minute===minute?Number(b.minute_count || 0):0,dc=b.day===day?Number(b.day_count || 0):0;
        if(mc>=6 || dc>=(principal.platformAdmin || principal.plan!=="free"?100:30) || Number(g.count || 0)>=1000)throw new Error("question_rate_limited");
        tx.set(quota,{minute,minute_count:mc+1,day,day_count:dc+1,expires_at:new Date((day+2)*86400000)});
        tx.set(global,{count:Number(g.count || 0)+1,expires_at:new Date((day+2)*86400000)});
        tx.set(conversationRef!,{owner_uid:principal.userId,context_hash:hash,context_reference:reference,title:context.title,messages,created_at:saved?.created_at || new Date(now).toISOString(),pending:{turn_id:turnId,until:now+45000}},{merge:true});
        return {previous:messages};
      });
      if(claim.replay){res.json({data:{conversation_id:conversationId,...claim.replay},meta:{request_id:requestId,replayed:true}});return;}
      leased=true;
      const answer=await (options.ask || askForecastQuestion)(context,question,claim.previous.map((m:any)=>({question:m.question,topic:m.response.topic})));
      const message={turn_id:turnId,question,response:answer,created_at:new Date().toISOString()};
      const requestRef=options.db.collection("users").doc(principal.userId).collection("requests").doc(`jev__${conversationId}`);
      await options.db.runTransaction(async tx=>{
        const [snapshot,existingRequest]=await Promise.all([tx.get(conversationRef!),tx.get(requestRef)]),saved=snapshot.data();
        if(saved?.owner_uid!==principal.userId || saved?.context_hash!==hash || saved?.pending?.turn_id!==turnId)throw new Error("question_context_changed");
        tx.update(conversationRef!,{messages:[...(saved.messages || []),message],pending:null,updated_at:message.created_at});
        const outputsMeta={conversation_id:conversationId,summary:answer.heading};
        tx.set(requestRef,existingRequest.exists?{outputsMeta,updatedAt:new Date(message.created_at)}:{
          type:"jev",title:`${context.title} · Scout`,ownerUid:principal.userId,workspaceId:principal.userId,createdAt:new Date(saved.created_at),updatedAt:new Date(message.created_at),
          input:{panel:"forecast",question,context_reference:reference.kind==="preview"?{kind:"preview",name:reference.name}:reference},
          outputsMeta,sourceRef:{collection:"jev_forecast_conversations",id:conversationId},status:"completed",
          published:false,deleted:false,visibility:"private",share:{visibility:"private",slug:""},searchText:`${context.title} Scout`.toLowerCase(),
        },{merge:true});
      });
      res.json({data:{conversation_id:conversationId,...message},meta:{request_id:requestId}});
    }catch(error){
      if(leased && conversationRef && turnId)await options.db.runTransaction(async tx=>{const s=await tx.get(conversationRef!);if(s.data()?.pending?.turn_id===turnId)tx.update(conversationRef!,{pending:null});}).catch(()=>undefined);
      failure(res,error,requestId);
    }
  });
  router.get("/v1/jev/conversations/:id",async(req,res)=>{
    const requestId=randomUUID();res.setHeader("Cache-Control","private, no-store");
    try{const principal=await authenticate(req);if(!uuid(req.params.id))throw new Error("question_request_invalid");
      const saved=(await options.db.collection("jev_forecast_conversations").doc(req.params.id).get()).data();
      if(!saved)throw new Error("question_context_not_found");if(saved.owner_uid!==principal.userId)throw new Error("workspace_forbidden");
      await loadQuestionContext(options.db,principal,parseContextReference(saved.context_reference));
      res.json({data:{conversation_id:req.params.id,title:saved.title,context_reference:saved.context_reference,messages:saved.messages || []}});
    }catch(error){failure(res,error,requestId);}
  });
  for(const action of ["read","save"] as const)router.post(`/v1/jev/annotations/${action}`,async(req,res)=>{
    const requestId=randomUUID();res.setHeader("Cache-Control","private, no-store");
    try{const principal=await authenticate(req);if(Object.keys(req.body || {}).some(k=>!["context","notes"].includes(k)))throw new Error("question_request_invalid");
      const reference=parseContextReference(req.body?.context),context=await loadQuestionContext(options.db,principal,reference),hash=contextHash(context);
      const id=createHash("sha256").update(`${principal.userId}:${hash}`).digest("hex"),ref=options.db.collection("forecast_annotations").doc(id);
      if(action==="read"){const s=(await ref.get()).data();res.json({data:{notes:s?.notes || [],context_hash:hash}});return;}
      const notes=validateNotes(req.body?.notes,context);
      await ref.set({owner_uid:principal.userId,context_hash:hash,notes,updated_at:new Date().toISOString()});
      res.json({data:{notes,context_hash:hash}});
    }catch(error){failure(res,error,requestId);}
  });
}
