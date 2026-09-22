/** Server-only typed decisions. No generated prose or caller-supplied endpoint. */
export const JEV_MODEL = "jev-1.13.0";
export async function requestJev(body: unknown, options: { request?: typeof fetch; key?: string; timeout?: number } = {}) {
  const key = options.key ?? process.env.TYPESAFE_API_KEY;
  if (!key) throw new Error("support_configuration_unavailable");
  const response = await (options.request || fetch)("https://api.typesafe.ai/v1/systemone", {
    method: "POST", headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify(body), signal: AbortSignal.timeout(options.timeout ?? 15_000), redirect: "error",
  });
  if (!response.ok) throw new Error("support_provider_unavailable");
  return response.json();
}

export function verifiedChoice(value: any, question: string, ids: string[]): string | null {
  const answer = value?.answers?.[question], probabilities = answer?.probabilities;
  if (value?.model !== JEV_MODEL || answer?.type !== "choice" || !ids.includes(answer.choice) ||
      typeof answer.confidence !== "number" || !Number.isFinite(answer.confidence) || answer.confidence < .7 || answer.confidence > 1 ||
      !probabilities || Object.keys(probabilities).length !== ids.length ||
      ids.some(id => typeof probabilities[id] !== "number" || !Number.isFinite(probabilities[id]) || probabilities[id] < 0 || probabilities[id] > 1)) return null;
  const values = ids.map(id => probabilities[id]);
  if (Math.abs(values.reduce((a,b)=>a+b,0)-1) > .02 || probabilities[answer.choice] < Math.max(...values) || probabilities[answer.choice] < .7) return null;
  return answer.choice;
}
