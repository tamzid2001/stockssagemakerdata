import {createHash, randomBytes, randomUUID} from "node:crypto";

type RecordValue = Record<string, any>;
type KeyProvider = () => Promise<string>;
const BASE = "https://app.vbase.com/api/v1/";
const CID = /^0x[0-9a-f]{64}$/i;
const plain = (v: any): RecordValue => v && typeof v === "object" && !Array.isArray(v) ? v : {};

export class ForecastProofError extends Error {
  constructor(public code: string) { super(code); }
}

/** Exact UTF-8 bytes are the commitment. Never normalize a downloaded proof. */
export function canonicalJson(value: any): string {
  if (value === null || typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonicalJson(value[k])}`).join(",")}}`;
  throw new ForecastProofError("forecast_proof_content_invalid");
}

export function proofSeed() {
  return {schema_version:"quantura_forecast_proof_v1", nonce:randomBytes(32).toString("hex"), status:"pending"};
}

export function proofManifest(id: string, job: RecordValue, result: RecordValue): string {
  const proof = plain(job.provenance);
  if (proof.schema_version !== "quantura_forecast_proof_v1" || !/^[a-f0-9]{64}$/.test(proof.nonce || "") || job.status !== "completed")
    throw new ForecastProofError("forecast_proof_not_available");
  // Exclude identity, CSV contents, credentials, mutable progress and profile metadata.
  const source = Object.fromEntries(["type","provider","symbol","dataset_id","side","contract_id","price_side","session","adjustment"].filter(k=>job.source?.[k] !== undefined).map(k=>[k,job.source[k]]));
  const configuration = Object.fromEntries(["models","quantiles","frequency","calendar","prediction_length","horizon_mode","transform","context_length","failure_policy","toto_variant","analysis_mode","search_signal_rule"].filter(k=>job.request?.[k] !== undefined).map(k=>[k,job.request[k]]));
  const output = Object.fromEntries(["dataset_hash","prepared_series_hash","result_hash","predictions","quantiles","effective_weights_by_quantile","historical_validation"].map(k=>[k,result[k] ?? null]));
  return canonicalJson({schema_version:proof.schema_version,nonce:proof.nonce,forecast_id:id,
    generated_at:result.created_at, input_cutoff_at:job.analysis_cutoff_at || job.input_cutoff_at || null,
    source,configuration,model_checkpoints:job.model_checkpoints || {},model_revisions:job.model_revisions || {},output});
}

export const contentCid = (bytes: string | Buffer) => `0x${createHash("sha3-256").update(bytes).digest("hex")}`;

export function normalizeReceipt(value: any, cid: string, collectionCid: string): RecordValue {
  const r = plain(value);
  if (!CID.test(cid) || !CID.test(collectionCid) || String(r.object_cid).toLowerCase() !== cid.toLowerCase() ||
      String(r.set_cid).toLowerCase() !== collectionCid.toLowerCase() || !/^0x[0-9a-f]{64}$/i.test(r.transaction_hash || "") ||
      !/^0x[0-9a-f]{40}$/i.test(r.user_address || "") || !Number.isSafeInteger(r.chain_id) || r.chain_id < 1 ||
      typeof r.timestamp !== "string" || !Number.isFinite(Date.parse(r.timestamp)))
    throw new ForecastProofError("forecast_proof_receipt_invalid");
  return {object_cid:cid.toLowerCase(),set_cid:collectionCid.toLowerCase(),transaction_hash:r.transaction_hash.toLowerCase(),
    user_address:r.user_address.toLowerCase(),chain_id:r.chain_id,timestamp:new Date(r.timestamp).toISOString()};
}

export class VBaseClient {
  constructor(private key: string, private collectionCid: string, private transport: typeof fetch = fetch) {
    if (!key || !CID.test(collectionCid)) throw new ForecastProofError("forecast_proof_not_configured");
  }
  private async request(path: string, body: URLSearchParams | object): Promise<RecordValue> {
    try {
      const form = body instanceof URLSearchParams;
      const response = await this.transport(BASE + path, {method:"POST",redirect:"error",signal:AbortSignal.timeout(15_000),
        headers:{Authorization:`Bearer ${this.key}`,"Content-Type":form ? "application/x-www-form-urlencoded" : "application/json",Accept:"application/json"},
        body:form ? body.toString() : JSON.stringify(body)});
      if (!response.ok) throw new ForecastProofError("forecast_proof_unavailable");
      const raw = await response.text();
      if (raw.length > 100_000) throw new ForecastProofError("forecast_proof_receipt_invalid");
      return plain(JSON.parse(raw));
    } catch (e) {
      // Never log upstream bodies or bearer credentials.
      if (e instanceof ForecastProofError) throw e;
      throw new ForecastProofError("forecast_proof_unavailable");
    }
  }
  async verify(cid: string): Promise<RecordValue | null> {
    if (!CID.test(cid)) throw new ForecastProofError("forecast_proof_content_invalid");
    const value = await this.request("stamps/verify", {cids:[cid],filter_by_user:true});
    if (!Array.isArray(value.stamp_list)) throw new ForecastProofError("forecast_proof_receipt_invalid");
    const matches = value.stamp_list.filter((r:any)=>String(r.object_cid).toLowerCase()===cid.toLowerCase() && String(r.set_cid).toLowerCase()===this.collectionCid.toLowerCase());
    return matches.length ? normalizeReceipt(matches[0],cid,this.collectionCid) : null;
  }
  async stamp(cid: string): Promise<RecordValue> {
    // Lookup first also recovers a committed request whose HTTP response was lost,
    // including retries outside the API's one-hour idempotency window.
    const existing = await this.verify(cid);
    if (existing) return existing;
    const value = await this.request("stamps", new URLSearchParams({data_cid:cid,collection_cid:this.collectionCid,
      store_stamped_file:"false",idempotent:"true",idempotency_window:"3600"}));
    return normalizeReceipt(value.commitment_receipt,cid,this.collectionCid);
  }
}

export function proofEnabled(): boolean {
  return process.env.VBASE_ENABLED === "true" && CID.test(process.env.VBASE_COLLECTION_CID || "");
}

export function publicProof(value: any): RecordValue | null {
  const p = plain(value);
  if (p.schema_version !== "quantura_forecast_proof_v1") return null;
  return {provider:"vbase",schema_version:p.schema_version,status:p.status,
    content_cid:p.content_cid || null,receipt:p.receipt || null,error_code:p.error_code || null,
    meaning:"Content and timestamp receipt; not a forecast accuracy score."};
}

export async function ensureForecastProof(db: FirebaseFirestore.Firestore, ref: FirebaseFirestore.DocumentReference,
  keyProvider: KeyProvider, transport: typeof fetch = fetch): Promise<RecordValue | null> {
  if (!proofEnabled()) throw new ForecastProofError("forecast_proof_not_configured");
  const key = await keyProvider();
  const client = new VBaseClient(key,process.env.VBASE_COLLECTION_CID!,transport);
  const token = randomUUID();
  const claim = await db.runTransaction(async tx => {
    const snap = await tx.get(ref), job = plain(snap.data());
    if (!snap.exists || job.status !== "completed") throw new ForecastProofError("forecast_proof_not_available");
    const p = plain(job.provenance);
    if (p.status === "stamped" || Number(p.lease_until) > Date.now()) return {job,acquired:false};
    if (Number(p.retry_after) > Date.now()) return {job,acquired:false};
    const next = {...(p.schema_version ? p : proofSeed()),status:"stamping",lease_token:token,lease_until:Date.now()+90_000,error_code:null};
    tx.update(ref,{provenance:next});
    return {job:{...job,provenance:next},acquired:true};
  });
  if (!claim.acquired) return publicProof(claim.job.provenance);
  let patch: RecordValue;
  try {
    const result = await db.collection("ensemble_forecast_results").doc(ref.id).get();
    if (!result.exists) throw new ForecastProofError("forecast_proof_not_available");
    const cid = contentCid(proofManifest(ref.id,claim.job,plain(result.data())));
    const receipt = await client.stamp(cid);
    patch = {status:"stamped",content_cid:cid,receipt,error_code:null,retry_after:0};
  } catch (e) {
    const code = e instanceof ForecastProofError ? e.code : "forecast_proof_unavailable";
    patch = {status:"unavailable",error_code:code,retry_after:Date.now()+60_000};
    console.warn(JSON.stringify({event:"forecast_proof_unavailable",forecast_id:ref.id,code}));
  }
  return db.runTransaction(async tx => {
    const snap = await tx.get(ref), p = plain(snap.data()?.provenance);
    if (p.lease_token !== token) return publicProof(p);
    const next = {...p,...patch,lease_token:null,lease_until:0};
    tx.update(ref,{provenance:next});
    return publicProof(next);
  });
}
