/** One event-history policy for forecasts and dataset exports; never fabricate bars. */
export type HistoryPhase = "both" | "pregame" | "in_game";
export type HistorySelection = { history_phase: HistoryPhase; history_lookback_minutes: number };

export function historySelection(input: Record<string, unknown>, legacyPregameDefault = false): HistorySelection {
  const phase = input.history_phase ?? (input.pregameOnly === true || (input.pregameOnly === undefined && legacyPregameDefault) ? "pregame" : "both");
  const lookback = input.history_lookback_minutes ?? 0;
  if (!["both", "pregame", "in_game"].includes(String(phase))) throw new Error("history_phase_invalid");
  if (typeof lookback !== "number" || !Number.isInteger(lookback) || lookback < 0 || lookback > 129600) throw new Error("history_lookback_invalid");
  return { history_phase: phase as HistoryPhase, history_lookback_minutes: lookback };
}

export function eventHistoryRange(start: number, end: number, eventStart: number, selection: HistorySelection) {
  if (selection.history_phase !== "both" && !Number.isFinite(eventStart)) throw new Error("event_start_unavailable");
  const selectedEnd = selection.history_phase === "pregame" ? Math.min(end, eventStart) : end;
  let selectedStart = selection.history_phase === "in_game" ? Math.max(start, eventStart) : start;
  if (selection.history_lookback_minutes) selectedStart = Math.max(selectedStart, selectedEnd - selection.history_lookback_minutes * 60000);
  return { start: selectedStart, end: selectedEnd };
}

export function quoteHistoryQuality(rows: Array<{ timestamp: string; target: number }>, eventStart = NaN) {
  let changes = 0, runStart = 0, longest = 0;
  for (let i = 1; i < rows.length; i++) {
    if (Math.abs(rows[i].target - rows[i - 1].target) > 1e-9) { changes++; runStart = i; }
    longest = Math.max(longest, Date.parse(rows[i].timestamp) - Date.parse(rows[runStart].timestamp));
  }
  const tailMinutes = rows.length ? (Date.parse(rows.at(-1)!.timestamp) - Date.parse(rows[runStart].timestamp)) / 60000 : 0;
  const pregame = Number.isFinite(eventStart) ? rows.filter(r => Date.parse(r.timestamp) <= eventStart).length : null;
  return { methodology_version: "event_history_v2", observations: rows.length, price_changes: changes,
    pregame_observations: pregame, in_game_observations: pregame === null ? null : rows.length - pregame,
    longest_unchanged_minutes: longest / 60000, trailing_unchanged_minutes: tailMinutes,
    forecast_blocked: rows.length >= 2 && (changes === 0 || (rows.length - runStart >= 30 && tailMinutes >= 120)),
    repeated_prices_preserved: true, synthetic_observations: 0 };
}
