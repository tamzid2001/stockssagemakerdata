import { gunzipSync } from "node:zlib";
import { createHash } from "node:crypto";

/** Immutable model input, not the current/withheld market quote. */
export function screenerHistory(encoded: unknown): Array<{timestamp: string; target: number}> {
  if (typeof encoded !== "string" || encoded.length > 140_000) throw new Error("screener_history_invalid");
  const decoded = JSON.parse(gunzipSync(Buffer.from(encoded, "base64"), {maxOutputLength: 100_000}).toString("utf8"));
  if (!Array.isArray(decoded) || decoded.length < 2 || decoded.length > 512) throw new Error("screener_history_invalid");
  let prior = -Infinity;
  return decoded.map((r: unknown[]) => {
    if (!Array.isArray(r) || r.length !== 2 || typeof r[0] !== "string" || !Number.isFinite(Date.parse(r[0])) || Date.parse(r[0]) <= prior || typeof r[1] !== "number" || !Number.isFinite(r[1]) || r[1] <= 0) throw new Error("screener_history_invalid");
    prior = Date.parse(r[0]);
    return {timestamp: r[0], target: r[1]};
  });
}

// Cache only the tiny cutoff, not thousands of full history arrays. Bound both keys and entries.
const cutoffs = new Map<string, {timestamp: string; target: number} | null>();
export function screenerCutoff(encoded: unknown) {
  if (typeof encoded !== "string" || encoded.length > 140_000) return null;
  const key = createHash("sha256").update(encoded).digest("hex");
  if (cutoffs.has(key)) return cutoffs.get(key)!;
  let cutoff = null;
  try { cutoff = screenerHistory(encoded).at(-1)!; } catch { /* No signal without verifiable input. */ }
  if (cutoffs.size >= 4096) cutoffs.delete(cutoffs.keys().next().value!);
  cutoffs.set(key, cutoff);
  return cutoff;
}
