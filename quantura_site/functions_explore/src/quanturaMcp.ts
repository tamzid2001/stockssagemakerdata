import type { Router, Request } from "express";
import rateLimit from "express-rate-limit";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import type { ApiPrincipal } from "./apiAccess";
import { CLERK_ISSUER } from "./clerkAuth";
import { QUANTURA_OAUTH_RESOURCE as ORIGIN, QUANTURA_OAUTH_SCOPES as SCOPES,
  QUANTURA_CLI_CLIENT_ID, QUANTURA_CLI_REDIRECT } from "./clerkOAuth";

const METADATA = `${ORIGIN}/.well-known/oauth-protected-resource/mcp`;
const CHALLENGE = `Bearer resource_metadata="${METADATA}", scope="${SCOPES.join(" ")}"`;
type Options = { authenticate: (req: Request) => Promise<ApiPrincipal>; fetch?: typeof fetch };
const record = z.record(z.unknown());
const id = z.string().min(1).max(220).regex(/^[A-Za-z0-9_-]+$/);

export function protectedResourceMetadata() {
  return { resource: ORIGIN, resource_name: "Quantura API and MCP", authorization_servers: [CLERK_ISSUER],
    scopes_supported: [...SCOPES], bearer_methods_supported: ["header"],
    resource_documentation: `${ORIGIN}/developers/api`, resource_policy_uri: `${ORIGIN}/privacy` };
}

export function registerQuanturaMcpRoutes(router: Router, options: Options) {
  router.get(["/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"], (_req,res) => {
    res.set({"Cache-Control":"public, max-age=300", "Access-Control-Allow-Origin":"*"}).json(protectedResourceMetadata());
  });
  router.options(["/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"], (_req,res) => {
    res.set({"Access-Control-Allow-Origin":"*", "Access-Control-Allow-Methods":"GET, OPTIONS"}).sendStatus(204);
  });
  router.get("/oauth/client", (_req,res) => {
    res.set("Cache-Control","public, max-age=300").json({client_id:QUANTURA_CLI_CLIENT_ID,
      issuer:CLERK_ISSUER, resource:ORIGIN, redirect_uri:QUANTURA_CLI_REDIRECT,
      scopes:[...SCOPES,"offline_access"], token_endpoint_auth_method:"none", code_challenge_method:"S256"});
  });
  router.use("/mcp", rateLimit({windowMs:60_000, limit:60, standardHeaders:true, legacyHeaders:false}));
  router.get("/mcp", (_req,res) => res.set("Allow","POST, OPTIONS").sendStatus(405));
  router.options("/mcp", (_req,res) => res.set({"Allow":"POST, OPTIONS",
    "Access-Control-Allow-Headers":"Authorization, Content-Type, MCP-Protocol-Version",
    "Access-Control-Allow-Methods":"POST, OPTIONS"}).sendStatus(204));
  router.post("/mcp", async (req,res) => {
    res.set("Cache-Control","private, no-store");
    const origin = req.get("origin");
    if (origin && ![ORIGIN,"https://www.quantura.studio","https://chatgpt.com","https://chat.openai.com"].includes(origin)) {
      res.status(403).json({error:"origin_forbidden"}); return;
    }
    if (!req.body || Array.isArray(req.body) || JSON.stringify(req.body).length > 1_000_000) {
      res.status(400).json({error:"invalid_mcp_request"}); return;
    }
    const bearer = String(req.headers.authorization || "").match(/^Bearer\s+(.+)$/i)?.[1] || "";
    // Protocol discovery contains no user data. Every tool call authenticates
    // and rechecks paid access through the same verifier as the HTTP API.
    if (req.body.method === "tools/call") {
      try {
        const principal = await options.authenticate(req);
        if (!["api_key","clerk_oauth"].includes(principal.authMethod)) throw new Error("api_key_invalid");
      } catch (error) {
        const paid = String((error as Error).message)==="paid_api_required";
        res.status(paid?403:401).set("WWW-Authenticate",CHALLENGE).json({jsonrpc:"2.0",id:req.body.id??null,
          error:{code:paid?-32003:-32001,message:paid?"Paid Pro or administrator API access is required.":"Sign in with Quantura OAuth."}});
        return;
      }
    }
    const server = new McpServer({name:"Quantura", version:"1.0.0"});
    const call = async (path:string, method="GET", body?:unknown, key?:string) => {
      const response = await (options.fetch || fetch)(`${ORIGIN}/api/v1/${path}`, {
        method, redirect:"error", signal:AbortSignal.timeout(60_000),
        headers:{Authorization:`Bearer ${bearer}`,Accept:"application/json",
          ...(body?{"Content-Type":"application/json"}:{}),...(key?{"Idempotency-Key":key}:{})},
        ...(body?{body:JSON.stringify(body)}:{})});
      const text = await response.text();
      if (text.length > 8_000_000) throw new Error("API response too large; use a smaller page.");
      let value; try { value=JSON.parse(text); } catch { value={error:"upstream_response_invalid",status:response.status}; }
      const structured = value && typeof value==="object" && !Array.isArray(value)?value:{data:value};
      return {content:[{type:"text" as const,text:JSON.stringify(structured)}],structuredContent:structured,isError:!response.ok,
        ...(response.status===401?{_meta:{"mcp/www_authenticate":[CHALLENGE]}}:{})};
    };
    const security = {securitySchemes:[{type:"oauth2",scopes:[...SCOPES]}]};
    function tool(name:string,title:string,description:string,schema:Record<string,z.ZodTypeAny>,readOnly:boolean,handler:any) {
      // Dynamic raw shapes remain runtime-validated by Zod. Avoid the SDK's
      // recursive generic inference at this small registration boundary.
      const register = server.registerTool as (...args:any[])=>unknown;
      register.call(server,name,{title,description,inputSchema:schema,
        annotations:{readOnlyHint:readOnly,destructiveHint:false,idempotentHint:readOnly,openWorldHint:true},_meta:security},handler);
    }
    tool("quantura_search_markets","Search Quantura","Search stocks, FX, prediction markets, economic databases and SageMaker forecasts.",
      {q:z.string().min(2).max(100),source:z.string().max(40).optional(),limit:z.number().int().min(1).max(20).optional()},true,
      ({q,source="auto",limit=8}:any)=>call(`market-search?${new URLSearchParams({q,source,limit:String(limit)})}`));
    tool("quantura_forecast_models","Forecast capabilities","Get currently available models, frequencies and minimum history requirements.",{},true,()=>call("forecast/models"));
    tool("quantura_get_forecast","Read a forecast","Read a forecast you have permission to access, including genuine historical context and quantiles.",
      {forecast_id:id},true,({forecast_id}:any)=>call(`ensemble-forecasts/${encodeURIComponent(forecast_id)}`));
    tool("quantura_create_forecast","Request a forecast","Create an asynchronous forecast job. Uses compute and saves a request in your account. Inspect capabilities first, then poll the returned ID. This does not trade.",
      {request:record,idempotency_key:z.string().min(1).max(160)},false,
      ({request,idempotency_key}:any)=>call("ensemble-forecasts","POST",request,idempotency_key));
    tool("quantura_download_history","Download market history","Get provider history as JSON. For Dukascopy paged requests, follow next_cursor without changing the other settings.",
      {request:record},true,({request}:any)=>call("market-data/stocks/history","POST",{...request,format:"json"}));
    tool("quantura_ask_scout","Ask Scout","Ask a structured question grounded in a completed forecast, game, CSV preview or SageMaker forecast. Saves the question in your account.",
      {context:record,question:z.string().min(1).max(2000),turn_id:z.string().uuid(),conversation_id:z.string().uuid().optional()},false,
      (body:any)=>call("jev/forecast-questions","POST",body));
    tool("quantura_my_access","Account API access","Read your current API access and permissions.",{},true,()=>call("me/access"));
    const transport = new StreamableHTTPServerTransport({sessionIdGenerator:undefined,enableJsonResponse:true});
    res.on("close",()=>{void transport.close();void server.close();});
    try {
      await server.connect(transport);
      await transport.handleRequest(req,res,req.body);
    } catch {
      if(!res.headersSent)res.status(500).json({jsonrpc:"2.0",id:req.body.id??null,error:{code:-32603,message:"MCP request failed. Retry the request."}});
    }
  });
}
