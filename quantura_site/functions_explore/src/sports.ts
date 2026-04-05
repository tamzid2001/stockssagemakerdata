export type SportsLeagueKey = "nba" | "nfl" | "mlb";

export type SportsLeagueInfo = {
  key: SportsLeagueKey;
  label: string;
  sportSlug: string;
  leagueSlug: string;
  defaultTeamSeasonOffset: number;
};

export type NormalizedSportsTeam = {
  id: string;
  abbreviation: string;
  displayName: string;
  shortDisplayName: string;
  location: string;
  slug: string;
  color: string;
  logoUrl: string;
};

export type NormalizedSportsPlayer = {
  id: string;
  displayName: string;
  shortName: string;
  firstName: string;
  lastName: string;
  jersey: string;
  position: string;
  positionAbbreviation: string;
  headshotUrl: string;
  teamId: string;
  teamAbbreviation: string;
  active: boolean;
};

export type NormalizedSportsGame = {
  id: string;
  date: string;
  displayDate: string;
  opponentTeamId: string;
  opponentAbbreviation: string;
  opponentDisplayName: string;
  homeAway: "home" | "away";
  label: string;
  status: string;
  venue: string;
  teamAbbreviation: string;
};

export type NormalizedSportsStat = {
  key: string;
  label: string;
  description: string;
  category: string;
  sourceKey: string;
  count: number;
  average: number | null;
  latest: number | null;
};

export type NormalizedSportsHistoryRow = {
  gameId: string;
  gameDate: string;
  displayDate: string;
  seasonLabel: string;
  teamId: string;
  teamAbbreviation: string;
  opponentTeamId: string;
  opponentAbbreviation: string;
  opponentDisplayName: string;
  homeAway: "home" | "away";
  result: string;
  score: string;
  teamScore: number | null;
  opponentScore: number | null;
  metrics: Record<string, number | null>;
  rawStats: Record<string, string>;
};

export type SportsPlayerContext = {
  league: SportsLeagueInfo;
  team: NormalizedSportsTeam;
  player: NormalizedSportsPlayer;
  statCatalog: NormalizedSportsStat[];
  historicalRows: NormalizedSportsHistoryRow[];
  futureGames: NormalizedSportsGame[];
  seasonsUsed: number[];
  defaultStatKey: string;
};

export type SportsTeamTotalFilter = "any" | "home" | "away";

export type SportsTeamGameTotalRow = {
  league: string;
  team: string;
  teamDisplayName: string;
  gameId: string;
  gameDate: string;
  displayDate: string;
  homeAway: "home" | "away";
  opponentTeamId: string;
  opponentAbbreviation: string;
  opponentDisplayName: string;
  status: string;
  result: string;
  score: string;
  teamTotalPoints: number | null;
  opponentTotalPoints: number | null;
  venue: string;
};

export type SportsTeamGameTotalsSnapshot = {
  league: SportsLeagueInfo;
  team: NormalizedSportsTeam;
  filters: {
    gameDate: string;
    homeAway: SportsTeamTotalFilter;
    scope: "all_history" | "single_day";
    timeZone: string;
  };
  headers: string[];
  rows: SportsTeamGameTotalRow[];
  csvText: string;
};

export type SportsPredictionAnalysisResult = {
  kind: "prediction_output";
  status: "ok" | "error";
  ticker: string;
  rowCount: number;
  columns: string[];
  summary: string;
  markdown: string;
  metrics: Record<string, number | string | null>;
  analysis: Record<string, unknown>;
  previewRows: Array<Record<string, string>>;
};

type CsvTable = {
  headers: string[];
  rows: string[][];
};

type CacheEntry<T> = {
  expiresAt: number;
  value: Promise<T>;
};

type ParsedEventAccumulator = {
  gameId: string;
  gameDate: string;
  displayDate: string;
  seasonLabel: string;
  teamId: string;
  teamAbbreviation: string;
  opponentTeamId: string;
  opponentAbbreviation: string;
  opponentDisplayName: string;
  homeAway: "home" | "away";
  result: string;
  score: string;
  teamScore: number | null;
  opponentScore: number | null;
  metrics: Record<string, number | null>;
  rawStats: Record<string, string>;
};

type StatDescriptor = {
  key: string;
  sourceKey: string;
  label: string;
  category: string;
  index: number;
};

const ESPN_BASE = "https://site.api.espn.com/apis/site/v2";
const ESPN_COMMON_BASE = "https://site.web.api.espn.com/apis/common/v3";
const DAY_MS = 24 * 60 * 60 * 1000;
const NOW_BUFFER_MS = 6 * 60 * 60 * 1000;
const CACHE = new Map<string, CacheEntry<unknown>>();

export const SPORTS_LEAGUES: Record<SportsLeagueKey, SportsLeagueInfo> = {
  nba: {
    key: "nba",
    label: "NBA",
    sportSlug: "basketball",
    leagueSlug: "nba",
    defaultTeamSeasonOffset: 1,
  },
  nfl: {
    key: "nfl",
    label: "NFL",
    sportSlug: "football",
    leagueSlug: "nfl",
    defaultTeamSeasonOffset: 0,
  },
  mlb: {
    key: "mlb",
    label: "MLB",
    sportSlug: "baseball",
    leagueSlug: "mlb",
    defaultTeamSeasonOffset: 0,
  },
};

const NBA_STAT_PRIORITY = [
  "points",
  "totalRebounds",
  "assists",
  "steals",
  "blocks",
  "threePointersMade",
  "minutes",
  "fieldGoalsMade",
  "fieldGoalsAttempted",
  "freeThrowsMade",
  "freeThrowsAttempted",
  "turnovers",
  "fieldGoalPct",
  "threePointPct",
  "freeThrowPct",
  "fouls",
];

const NFL_STAT_PRIORITY = [
  "passingYards",
  "passingTouchdowns",
  "completions",
  "passingAttempts",
  "completionPct",
  "interceptions",
  "rushingYards",
  "rushingTouchdowns",
  "rushingAttempts",
  "receivingYards",
  "receptions",
  "receivingTouchdowns",
  "receivingTargets",
  "yardsPerReception",
  "tackles",
  "soloTackles",
  "assistedTackles",
  "sacks",
  "interceptions",
  "passesDefended",
  "fieldGoalsMade",
  "extraPointsMade",
];

const MLB_STAT_PRIORITY = [
  "batting_hits",
  "batting_homeRuns",
  "batting_RBIs",
  "batting_totalBases",
  "batting_runs",
  "batting_walks",
  "batting_strikeouts",
  "batting_stolenBases",
  "batting_atBats",
  "pitching_innings",
  "pitching_strikeouts",
  "pitching_earnedRuns",
  "pitching_hits",
  "pitching_walks",
  "pitching_battersFaced",
  "pitching_pitches",
  "pitching_homeRuns",
];

function asString(value: unknown, fallback = ""): string {
  if (typeof value === "string") return value;
  if (value === null || value === undefined) return fallback;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return fallback;
}

function asFinite(value: unknown, fallback = Number.NaN): number {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function asPlainObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function sanitizeText(value: unknown, maxLen = 240): string {
  return asString(value)
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLen);
}

function normalizeLeagueKey(value: unknown): SportsLeagueKey {
  const raw = sanitizeText(value, 20).toLowerCase();
  if (raw === "nba" || raw === "nfl" || raw === "mlb") return raw;
  throw new Error("invalid_sports_league");
}

function safeNumber(raw: unknown): number | null {
  if (raw && typeof raw === "object") {
    const record = raw as Record<string, unknown>;
    const nestedCandidates = [record.value, record.displayValue, record.score];
    for (const candidate of nestedCandidates) {
      if (candidate === raw) continue;
      const parsed = safeNumber(candidate);
      if (parsed !== null) return parsed;
    }
  }
  const text = sanitizeText(raw, 60);
  if (!text || text === "-" || text === "--") return null;
  const numeric = Number(text);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatColor(raw: unknown): string {
  const clean = sanitizeText(raw, 12).replace(/[^A-Fa-f0-9]/g, "");
  if (!clean) return "";
  return `#${clean.slice(0, 6)}`;
}

function formatDateYmd(value: Date, timeZone = "UTC"): string {
  try {
    const formatter = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    const parts = formatter.formatToParts(value);
    const year = parts.find((part) => part.type === "year")?.value || "";
    const month = parts.find((part) => part.type === "month")?.value || "";
    const day = parts.find((part) => part.type === "day")?.value || "";
    if (year && month && day) return `${year}-${month}-${day}`;
  } catch {
    // Fall back to UTC formatting below.
  }
  return value.toISOString().slice(0, 10);
}

function formatDateLabel(value: string, timeZone = ""): string {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return sanitizeText(value, 40);
  const options: Intl.DateTimeFormatOptions = {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  };
  if (sanitizeText(timeZone, 80)) options.timeZone = timeZone;
  return new Intl.DateTimeFormat("en-US", options).format(date);
}

function normalizeSportsTimeZone(value: unknown): string {
  const clean = sanitizeText(value, 80).replace(/[^A-Za-z0-9/_+-]/g, "");
  if (!clean) return "UTC";
  try {
    Intl.DateTimeFormat("en-US", { timeZone: clean }).format(new Date());
    return clean;
  } catch {
    return "UTC";
  }
}

function escapeCsvCell(value: unknown): string {
  const text = asString(value, "");
  if (!/[",\n]/.test(text)) return text;
  return `"${text.replace(/"/g, "\"\"")}"`;
}

function buildCsv(headers: string[], rows: Array<Record<string, unknown>>): string {
  const lines = [headers.map((header) => escapeCsvCell(header)).join(",")];
  rows.forEach((row) => {
    lines.push(headers.map((header) => escapeCsvCell(row?.[header] ?? "")).join(","));
  });
  return lines.join("\n");
}

function normalizeCsvHeader(value: unknown): string {
  return sanitizeText(value, 80).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function parseCsvTable(csvText: string): CsvTable {
  const rows: string[][] = [];
  let current = "";
  let row: string[] = [];
  let inQuotes = false;
  for (let i = 0; i < csvText.length; i += 1) {
    const char = csvText[i];
    if (char === "\"") {
      if (inQuotes && csvText[i + 1] === "\"") {
        current += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === "," && !inQuotes) {
      row.push(current);
      current = "";
      continue;
    }
    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && csvText[i + 1] === "\n") i += 1;
      row.push(current);
      current = "";
      if (row.some((cell) => String(cell || "").trim())) rows.push(row);
      row = [];
      continue;
    }
    current += char;
  }
  row.push(current);
  if (row.some((cell) => String(cell || "").trim())) rows.push(row);
  const headers = (rows.shift() || []).map((header) => sanitizeText(header, 120));
  return { headers, rows };
}

function previewRowsFromTable(table: CsvTable, limit = 12): Array<Record<string, string>> {
  return table.rows.slice(0, limit).map((row) => {
    const out: Record<string, string> = {};
    table.headers.forEach((header, index) => {
      out[header] = asString(row[index], "");
    });
    return out;
  });
}

function extractQuantileColumns(headers: string[]): Array<{ key: string; label: string; quantile: number; index: number }> {
  return headers
    .map((header, index) => {
      const clean = sanitizeText(header, 80).toLowerCase();
      const match = clean.match(/^(?:p|q)\s*0*([0-9]{1,3})$/i);
      if (!match) return null;
      const raw = Number(match[1]);
      if (!Number.isFinite(raw)) return null;
      const quantile = raw > 1 ? raw / 100 : raw;
      if (!(quantile >= 0 && quantile <= 1)) return null;
      return {
        key: clean.replace(/\s+/g, ""),
        label: sanitizeText(header, 80),
        quantile,
        index,
      };
    })
    .filter((entry): entry is { key: string; label: string; quantile: number; index: number } => Boolean(entry))
    .sort((left, right) => left.quantile - right.quantile);
}

function pickMedianQuantile(columns: Array<{ key: string; label: string; quantile: number; index: number }>) {
  if (!columns.length) return null;
  const explicit = columns.find((column) => Math.abs(column.quantile - 0.5) < 0.001);
  if (explicit) return explicit;
  return columns[Math.floor(columns.length / 2)] || null;
}

function withCategoryPrefix(category: string, key: string): string {
  const cleanKey = sanitizeText(key, 80).replace(/[^A-Za-z0-9_]/g, "");
  const cleanCategory = sanitizeText(category, 40).toLowerCase().replace(/[^a-z0-9]/g, "");
  return cleanCategory ? `${cleanCategory}_${cleanKey}` : cleanKey;
}

function labelForCategoryKey(category: string, sourceKey: string, fallback: string): string {
  const cleanCategory = sanitizeText(category, 40).toLowerCase();
  const cleanSourceKey = sanitizeText(sourceKey, 80);
  if (cleanCategory === "pitching" && cleanSourceKey === "hits") return "Hits Allowed";
  if (cleanCategory === "pitching" && cleanSourceKey === "walks") return "Walks Allowed";
  if (cleanCategory === "pitching" && cleanSourceKey === "runs") return "Runs Allowed";
  return sanitizeText(fallback, 120) || sanitizeText(sourceKey, 120);
}

function describeDerivedMetric(key: string): { label: string; description: string } {
  switch (key) {
    case "threePointersMade":
      return { label: "Threes Made", description: "3-point field goals made" };
    case "threePointersAttempted":
      return { label: "Three-Point Attempts", description: "3-point field goals attempted" };
    case "fieldGoalsMade":
      return { label: "Field Goals Made", description: "Field goals made" };
    case "fieldGoalsAttempted":
      return { label: "Field Goal Attempts", description: "Field goals attempted" };
    case "freeThrowsMade":
      return { label: "Free Throws Made", description: "Free throws made" };
    case "freeThrowsAttempted":
      return { label: "Free Throw Attempts", description: "Free throws attempted" };
    case "batting_totalBases":
      return { label: "Total Bases", description: "Total batting bases from hits, doubles, triples, and home runs" };
    case "batting_singles":
      return { label: "Singles", description: "Single-base hits" };
    default:
      return { label: sanitizeText(key, 120), description: sanitizeText(key, 180) };
  }
}

function mapEspnTeam(raw: unknown): NormalizedSportsTeam | null {
  const team = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const id = sanitizeText(team.id, 40);
  if (!id) return null;
  const logos = Array.isArray(team.logos) ? team.logos : [];
  const preferredLogo =
    logos.find((logo) => Array.isArray((logo as Record<string, unknown>)?.rel) && ((logo as Record<string, unknown>).rel as unknown[]).includes("default")) ||
    logos[0] ||
    {};
  return {
    id,
    abbreviation: sanitizeText(team.abbreviation, 20).toUpperCase(),
    displayName: sanitizeText(team.displayName, 120) || sanitizeText(team.name, 120),
    shortDisplayName: sanitizeText(team.shortDisplayName, 120) || sanitizeText(team.displayName, 120),
    location: sanitizeText(team.location, 120),
    slug: sanitizeText(team.slug, 120),
    color: formatColor(team.color),
    logoUrl: sanitizeText((preferredLogo as Record<string, unknown>).href, 1000),
  };
}

function mapEspnPlayer(raw: unknown, fallbackTeam: NormalizedSportsTeam): NormalizedSportsPlayer | null {
  const player = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const id = sanitizeText(player.id, 40);
  if (!id) return null;
  const position = player.position && typeof player.position === "object" ? (player.position as Record<string, unknown>) : {};
  const headshot = player.headshot && typeof player.headshot === "object" ? (player.headshot as Record<string, unknown>) : {};
  return {
    id,
    displayName: sanitizeText(player.displayName, 120) || sanitizeText(player.fullName, 120),
    shortName: sanitizeText(player.shortName, 120) || sanitizeText(player.displayName, 120),
    firstName: sanitizeText(player.firstName, 80),
    lastName: sanitizeText(player.lastName, 80),
    jersey: sanitizeText(player.jersey, 20),
    position: sanitizeText(position.displayName || position.name, 80),
    positionAbbreviation: sanitizeText(position.abbreviation, 20),
    headshotUrl: sanitizeText(headshot.href, 1000),
    teamId: fallbackTeam.id,
    teamAbbreviation: fallbackTeam.abbreviation,
    active: true,
  };
}

async function fetchCachedJson<T>(url: string, ttlMs = 15 * 60 * 1000): Promise<T> {
  const cleanUrl = sanitizeText(url, 2000);
  const now = Date.now();
  const cached = CACHE.get(cleanUrl);
  if (cached && cached.expiresAt > now) {
    return cached.value as Promise<T>;
  }
  const pending = (async () => {
    const response = await fetch(cleanUrl, {
      headers: {
        Accept: "application/json",
        "User-Agent": "QuanturaSportsForecast/1.0",
      },
    });
    if (!response.ok) {
      throw new Error(`sports_provider_http_${response.status}`);
    }
    return (await response.json()) as T;
  })();
  CACHE.set(cleanUrl, { expiresAt: now + ttlMs, value: pending });
  try {
    return await pending;
  } catch (error) {
    CACHE.delete(cleanUrl);
    throw error;
  }
}

function leagueInfo(value: unknown): SportsLeagueInfo {
  return SPORTS_LEAGUES[normalizeLeagueKey(value)];
}

function teamsUrl(league: SportsLeagueInfo): string {
  return `${ESPN_BASE}/sports/${encodeURIComponent(league.sportSlug)}/${encodeURIComponent(league.leagueSlug)}/teams`;
}

function rosterUrl(league: SportsLeagueInfo, teamId: string): string {
  return `${ESPN_BASE}/sports/${encodeURIComponent(league.sportSlug)}/${encodeURIComponent(league.leagueSlug)}/teams/${encodeURIComponent(teamId)}/roster`;
}

function scheduleUrl(league: SportsLeagueInfo, teamId: string, seasonYear = ""): string {
  const params = new URLSearchParams();
  if (seasonYear) params.set("season", seasonYear);
  const query = params.toString();
  return `${ESPN_BASE}/sports/${encodeURIComponent(league.sportSlug)}/${encodeURIComponent(league.leagueSlug)}/teams/${encodeURIComponent(teamId)}/schedule${
    query ? `?${query}` : ""
  }`;
}

function normalizeSportsGameDate(value: unknown): string {
  const raw = sanitizeText(value, 40);
  if (!raw) throw new Error("invalid_sports_game_date");
  const parsed = new Date(`${raw}T12:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) throw new Error("invalid_sports_game_date");
  return formatDateYmd(parsed);
}

function normalizeOptionalSportsGameDate(value: unknown): string {
  const raw = sanitizeText(value, 40);
  if (!raw) return "";
  return normalizeSportsGameDate(raw);
}

function normalizeSportsHomeAwayFilter(value: unknown): SportsTeamTotalFilter {
  const clean = sanitizeText(value, 20).toLowerCase();
  if (clean === "home" || clean === "away") return clean;
  return "any";
}

function scheduleSeasonCandidatesForDate(league: SportsLeagueInfo, gameDate: string): string[] {
  const parsed = new Date(`${gameDate}T12:00:00Z`);
  const year = parsed.getUTCFullYear();
  const offset = Math.max(0, Math.floor(asFinite(league.defaultTeamSeasonOffset, 0)));
  return Array.from(
    new Set(
      [year, year - 1, year + 1, year - offset, year - 1 - offset, year + 1 - offset]
        .filter((value) => Number.isFinite(value) && value > 0)
        .map((value) => String(value))
    )
  );
}

function gameLogUrl(league: SportsLeagueInfo, playerId: string, seasonYear = "", category = ""): string {
  const params = new URLSearchParams();
  if (seasonYear) {
    params.set(league.key === "mlb" ? "season" : "year", seasonYear);
  }
  if (category) params.set("category", category);
  const query = params.toString();
  return `${ESPN_COMMON_BASE}/sports/${encodeURIComponent(league.sportSlug)}/${encodeURIComponent(league.leagueSlug)}/athletes/${encodeURIComponent(
    playerId
  )}/gamelog${query ? `?${query}` : ""}`;
}

function flattenRosterEntries(raw: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(raw)) return [];
  if (raw.length && Array.isArray((raw[0] as Record<string, unknown>)?.items)) {
    return raw.flatMap((group) => {
      const record = group && typeof group === "object" ? (group as Record<string, unknown>) : {};
      return Array.isArray(record.items)
        ? record.items.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
        : [];
    });
  }
  return raw.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"));
}

function extractSeasonYears(payload: Record<string, unknown>): number[] {
  const filters = Array.isArray(payload.filters) ? payload.filters : [];
  const seasonFilter =
    filters.find((entry) => sanitizeText((entry as Record<string, unknown>)?.name, 40).toLowerCase() === "season") || {};
  const options = Array.isArray((seasonFilter as Record<string, unknown>).options)
    ? ((seasonFilter as Record<string, unknown>).options as Array<Record<string, unknown>>)
    : [];
  return options
    .map((option) => Math.floor(asFinite(option.value, Number.NaN)))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => right - left);
}

function extractCategoryOptions(payload: Record<string, unknown>): string[] {
  const filters = Array.isArray(payload.filters) ? payload.filters : [];
  const categoryFilter =
    filters.find((entry) => sanitizeText((entry as Record<string, unknown>)?.name, 40).toLowerCase() === "category") || {};
  const options = Array.isArray((categoryFilter as Record<string, unknown>).options)
    ? ((categoryFilter as Record<string, unknown>).options as Array<Record<string, unknown>>)
    : [];
  const values = options
    .map((option) => sanitizeText(option.value, 40).toLowerCase())
    .filter(Boolean);
  if (values.length) return Array.from(new Set(values));
  const current = sanitizeText((categoryFilter as Record<string, unknown>).value, 40).toLowerCase();
  return current ? [current] : [""];
}

function deriveSplitMetrics(
  descriptor: StatDescriptor,
  rawValue: string
): Array<{ key: string; label: string; value: number | null; sourceKey: string }> {
  const parts = rawValue.split("-").map((item) => safeNumber(item));
  if (descriptor.sourceKey === "fieldGoalsMade-fieldGoalsAttempted" && parts.length >= 2) {
    return [
      { key: "fieldGoalsMade", label: "Field Goals Made", value: parts[0], sourceKey: descriptor.sourceKey },
      { key: "fieldGoalsAttempted", label: "Field Goal Attempts", value: parts[1], sourceKey: descriptor.sourceKey },
    ];
  }
  if (descriptor.sourceKey === "threePointFieldGoalsMade-threePointFieldGoalsAttempted" && parts.length >= 2) {
    return [
      { key: "threePointersMade", label: "Threes Made", value: parts[0], sourceKey: descriptor.sourceKey },
      { key: "threePointersAttempted", label: "Three-Point Attempts", value: parts[1], sourceKey: descriptor.sourceKey },
    ];
  }
  if (descriptor.sourceKey === "freeThrowsMade-freeThrowsAttempted" && parts.length >= 2) {
    return [
      { key: "freeThrowsMade", label: "Free Throws Made", value: parts[0], sourceKey: descriptor.sourceKey },
      { key: "freeThrowsAttempted", label: "Free Throw Attempts", value: parts[1], sourceKey: descriptor.sourceKey },
    ];
  }
  return [];
}

function buildStatDescriptors(
  payload: Record<string, unknown>,
  categoryKey: string
): StatDescriptor[] {
  const rawNames = Array.isArray(payload.names) ? payload.names : [];
  const rawLabels = Array.isArray(payload.displayNames) ? payload.displayNames : [];
  return rawNames.map((entry, index) => {
    const sourceKey = sanitizeText(entry, 120);
    const displayName = sanitizeText(rawLabels[index], 120) || sanitizeText(entry, 120);
    return {
      key: withCategoryPrefix(categoryKey, sourceKey.replace(/[^A-Za-z0-9_]/g, "")),
      sourceKey,
      label: labelForCategoryKey(categoryKey, sourceKey, displayName),
      category: sanitizeText(categoryKey, 40),
      index,
    };
  });
}

function buildBaseEventRow(meta: Record<string, unknown>): ParsedEventAccumulator | null {
  const gameId = sanitizeText(meta.id, 40);
  const gameDate = sanitizeText(meta.gameDate || meta.date, 80);
  if (!gameId || !gameDate) return null;
  const team = meta.team && typeof meta.team === "object" ? (meta.team as Record<string, unknown>) : {};
  const opponent = meta.opponent && typeof meta.opponent === "object" ? (meta.opponent as Record<string, unknown>) : {};
  const atVs = sanitizeText(meta.atVs, 20).toLowerCase();
  const homeAway: "home" | "away" = atVs === "vs" ? "home" : "away";
  const score = sanitizeText(meta.score, 80);
  return {
    gameId,
    gameDate,
    displayDate: formatDateLabel(gameDate),
    seasonLabel: "",
    teamId: sanitizeText(team.id, 40),
    teamAbbreviation: sanitizeText(team.abbreviation, 20).toUpperCase(),
    opponentTeamId: sanitizeText(opponent.id, 40),
    opponentAbbreviation: sanitizeText(opponent.abbreviation, 20).toUpperCase(),
    opponentDisplayName: sanitizeText(opponent.displayName, 120),
    homeAway,
    result: sanitizeText(meta.gameResult, 40),
    score,
    teamScore: safeNumber(meta.score === score ? "" : meta.teamScore) ?? safeNumber(meta.homeTeamScore),
    opponentScore: safeNumber(meta.opponentScore) ?? safeNumber(meta.awayTeamScore),
    metrics: {},
    rawStats: {},
  };
}

function applyDerivedBaseballMetrics(row: ParsedEventAccumulator) {
  const hits = row.metrics.batting_hits;
  const doubles = row.metrics.batting_doubles;
  const triples = row.metrics.batting_triples;
  const homeRuns = row.metrics.batting_homeRuns;
  if (
    typeof hits === "number" &&
    Number.isFinite(hits) &&
    typeof doubles === "number" &&
    Number.isFinite(doubles) &&
    typeof triples === "number" &&
    Number.isFinite(triples) &&
    typeof homeRuns === "number" &&
    Number.isFinite(homeRuns)
  ) {
    const singles = Math.max(0, hits - doubles - triples - homeRuns);
    row.metrics.batting_singles = singles;
    row.metrics.batting_totalBases = singles + doubles * 2 + triples * 3 + homeRuns * 4;
    row.rawStats.batting_singles = String(singles);
    row.rawStats.batting_totalBases = String(row.metrics.batting_totalBases ?? "");
  }
}

function preferredStatOrder(leagueKey: SportsLeagueKey): string[] {
  if (leagueKey === "nba") return NBA_STAT_PRIORITY;
  if (leagueKey === "nfl") return NFL_STAT_PRIORITY;
  return MLB_STAT_PRIORITY;
}

function buildStatCatalog(
  leagueKey: SportsLeagueKey,
  rows: NormalizedSportsHistoryRow[],
  labelLookup: Map<string, { label: string; description: string; category: string; sourceKey: string }>
): NormalizedSportsStat[] {
  const stats = new Map<
    string,
    { count: number; total: number; latest: number | null; label: string; description: string; category: string; sourceKey: string }
  >();
  rows.forEach((row) => {
    Object.entries(row.metrics).forEach(([key, value]) => {
      if (!Number.isFinite(Number(value))) return;
      const existing = stats.get(key) || {
        count: 0,
        total: 0,
        latest: null,
        label: labelLookup.get(key)?.label || sanitizeText(key, 120),
        description: labelLookup.get(key)?.description || sanitizeText(key, 180),
        category: labelLookup.get(key)?.category || "",
        sourceKey: labelLookup.get(key)?.sourceKey || key,
      };
      existing.count += 1;
      existing.total += Number(value);
      existing.latest = Number(value);
      stats.set(key, existing);
    });
  });
  const keys = Array.from(stats.keys());
  const preferred = preferredStatOrder(leagueKey);
  return keys
    .sort((left, right) => {
      const leftIndex = preferred.indexOf(left);
      const rightIndex = preferred.indexOf(right);
      if (leftIndex >= 0 || rightIndex >= 0) {
        if (leftIndex < 0) return 1;
        if (rightIndex < 0) return -1;
        return leftIndex - rightIndex;
      }
      return (stats.get(left)?.label || left).localeCompare(stats.get(right)?.label || right);
    })
    .map((key) => {
      const entry = stats.get(key)!;
      return {
        key,
        label: entry.label,
        description: entry.description,
        category: entry.category,
        sourceKey: entry.sourceKey,
        count: entry.count,
        average: entry.count ? Number((entry.total / entry.count).toFixed(4)) : null,
        latest: entry.latest,
      };
    })
    .filter((entry) => entry.count >= 3);
}

export async function listSportsTeams(leagueKey: unknown): Promise<NormalizedSportsTeam[]> {
  const league = leagueInfo(leagueKey);
  const payload = (await fetchCachedJson<Record<string, unknown>>(teamsUrl(league), 12 * 60 * 60 * 1000)) || {};
  const sports = Array.isArray(payload.sports) ? payload.sports : [];
  const leagueNode = sports
    .flatMap((sport) => (Array.isArray((sport as Record<string, unknown>)?.leagues) ? ((sport as Record<string, unknown>).leagues as unknown[]) : []))
    .find((entry) => sanitizeText((entry as Record<string, unknown>)?.slug, 40).toLowerCase() === league.leagueSlug);
  const teams = Array.isArray((leagueNode as Record<string, unknown>)?.teams) ? ((leagueNode as Record<string, unknown>).teams as unknown[]) : [];
  return teams
    .map((entry) => mapEspnTeam((entry as Record<string, unknown>)?.team))
    .filter((entry): entry is NormalizedSportsTeam => Boolean(entry))
    .sort((left, right) => left.displayName.localeCompare(right.displayName));
}

export async function listSportsPlayers(leagueKey: unknown, teamId: unknown): Promise<NormalizedSportsPlayer[]> {
  const league = leagueInfo(leagueKey);
  const cleanTeamId = sanitizeText(teamId, 40);
  if (!cleanTeamId) throw new Error("invalid_team_id");
  const teams = await listSportsTeams(league.key);
  const team = teams.find((entry) => entry.id === cleanTeamId);
  if (!team) throw new Error("team_not_found");
  const payload = (await fetchCachedJson<Record<string, unknown>>(rosterUrl(league, cleanTeamId), 6 * 60 * 60 * 1000)) || {};
  return flattenRosterEntries(payload.athletes)
    .map((entry) => mapEspnPlayer(entry, team))
    .filter((entry): entry is NormalizedSportsPlayer => Boolean(entry))
    .sort((left, right) => left.displayName.localeCompare(right.displayName));
}

export async function listSportsUpcomingGames(leagueKey: unknown, teamId: unknown): Promise<NormalizedSportsGame[]> {
  const league = leagueInfo(leagueKey);
  const cleanTeamId = sanitizeText(teamId, 40);
  if (!cleanTeamId) throw new Error("invalid_team_id");
  const now = Date.now() - NOW_BUFFER_MS;
  const mapUpcomingGames = (payload: Record<string, unknown>): NormalizedSportsGame[] => {
    const events = Array.isArray(payload.events) ? payload.events : [];
    return events
      .map((event) => {
        const record = event && typeof event === "object" ? (event as Record<string, unknown>) : {};
        const date = sanitizeText(record.date, 80);
        const gameMs = Date.parse(date);
        if (!date || !Number.isFinite(gameMs) || gameMs < now) return null;
        const competitions = Array.isArray(record.competitions) ? record.competitions : [];
        const competition = competitions[0] && typeof competitions[0] === "object" ? (competitions[0] as Record<string, unknown>) : {};
        const competitors = Array.isArray(competition.competitors) ? competition.competitors : [];
        const teamCompetitor = competitors.find(
          (entry) => sanitizeText((entry as Record<string, unknown>)?.team && ((entry as Record<string, unknown>).team as Record<string, unknown>).id, 40) === cleanTeamId
        );
        const opponentCompetitor = competitors.find(
          (entry) => sanitizeText((entry as Record<string, unknown>)?.team && ((entry as Record<string, unknown>).team as Record<string, unknown>).id, 40) !== cleanTeamId
        );
        if (!teamCompetitor || !opponentCompetitor) return null;
        const teamNode = (teamCompetitor as Record<string, unknown>).team as Record<string, unknown>;
        const opponentNode = (opponentCompetitor as Record<string, unknown>).team as Record<string, unknown>;
        const homeAway = sanitizeText((teamCompetitor as Record<string, unknown>).homeAway, 20).toLowerCase() === "home" ? "home" : "away";
        const venueNode = asPlainObject(competition.venue);
        const venueAddress = asPlainObject(venueNode.address);
        const venue = sanitizeText(venueNode.fullName || venueAddress.city, 160);
        return {
          id: sanitizeText(record.id, 40),
          date,
          displayDate: formatDateLabel(date),
          opponentTeamId: sanitizeText(opponentNode?.id, 40),
          opponentAbbreviation: sanitizeText(opponentNode?.abbreviation, 20).toUpperCase(),
          opponentDisplayName: sanitizeText(opponentNode?.displayName, 120),
          homeAway,
          label: `${formatDateLabel(date)} ${homeAway === "home" ? "vs" : "@"} ${sanitizeText(opponentNode?.abbreviation, 20).toUpperCase()}`,
          status: sanitizeText(((competition.status as Record<string, unknown>)?.type as Record<string, unknown>)?.detail, 160) || "Scheduled",
          venue,
          teamAbbreviation: sanitizeText(teamNode?.abbreviation, 20).toUpperCase(),
        } satisfies NormalizedSportsGame;
      })
      .filter((entry): entry is NormalizedSportsGame => Boolean(entry))
      .sort((left, right) => Date.parse(left.date) - Date.parse(right.date));
  };

  const payload = (await fetchCachedJson<Record<string, unknown>>(scheduleUrl(league, cleanTeamId), 30 * 60 * 1000)) || {};
  const directGames = mapUpcomingGames(payload);
  if (directGames.length || league.key !== "nfl") return directGames;

  const currentYear = new Date().getUTCFullYear();
  const candidateSeasons = Array.from(
    new Set(
      [currentYear, currentYear + 1].filter((value) => Number.isFinite(value) && value > 0).map((value) => String(value))
    )
  );
  for (const seasonYear of candidateSeasons) {
    const fallbackPayload =
      (await fetchCachedJson<Record<string, unknown>>(scheduleUrl(league, cleanTeamId, seasonYear), 30 * 60 * 1000)) || {};
    const games = mapUpcomingGames(fallbackPayload);
    if (games.length) return games;
  }
  return [];
}

export async function buildSportsTeamGameTotalsSnapshot(
  leagueKey: unknown,
  teamId: unknown,
  gameDate: unknown,
  homeAwayFilterRaw: unknown,
  timeZoneRaw: unknown = "UTC"
): Promise<SportsTeamGameTotalsSnapshot> {
  const league = leagueInfo(leagueKey);
  const cleanTeamId = sanitizeText(teamId, 40);
  const targetGameDate = normalizeOptionalSportsGameDate(gameDate);
  const homeAwayFilter = normalizeSportsHomeAwayFilter(homeAwayFilterRaw);
  const timeZone = normalizeSportsTimeZone(timeZoneRaw);
  if (!cleanTeamId) throw new Error("invalid_team_id");

  const teams = await listSportsTeams(league.key);
  const team = teams.find((entry) => entry.id === cleanTeamId);
  if (!team) throw new Error("team_not_found");

  const headers = [
    "league",
    "team",
    "teamDisplayName",
    "gameId",
    "gameDate",
    "displayDate",
    "homeAway",
    "opponentAbbreviation",
    "opponentDisplayName",
    "status",
    "result",
    "score",
    "teamTotalPoints",
    "opponentTotalPoints",
    "venue",
  ];

  const rowsByGameId = new Map<string, SportsTeamGameTotalRow>();
  const collectRows = (payload: Record<string, unknown>) => {
    const events = Array.isArray(payload.events) ? payload.events : [];
    events.forEach((event) => {
      const record = event && typeof event === "object" ? (event as Record<string, unknown>) : {};
      const date = sanitizeText(record.date, 80);
      if (!date) return;
      const parsedDate = new Date(date);
      if (!Number.isFinite(parsedDate.getTime())) return;
      const eventYmd = formatDateYmd(parsedDate, timeZone);
      if (targetGameDate && eventYmd !== targetGameDate) return;
      const competitions = Array.isArray(record.competitions) ? record.competitions : [];
      const competition = competitions[0] && typeof competitions[0] === "object" ? (competitions[0] as Record<string, unknown>) : {};
      const competitors = Array.isArray(competition.competitors) ? competition.competitors : [];
      const teamCompetitor = competitors.find(
        (entry) => sanitizeText((entry as Record<string, unknown>)?.team && ((entry as Record<string, unknown>).team as Record<string, unknown>).id, 40) === cleanTeamId
      );
      const opponentCompetitor = competitors.find(
        (entry) => sanitizeText((entry as Record<string, unknown>)?.team && ((entry as Record<string, unknown>).team as Record<string, unknown>).id, 40) !== cleanTeamId
      );
      if (!teamCompetitor || !opponentCompetitor) return;
      const homeAway = sanitizeText((teamCompetitor as Record<string, unknown>).homeAway, 20).toLowerCase() === "home" ? "home" : "away";
      if (homeAwayFilter !== "any" && homeAway !== homeAwayFilter) return;
      const teamNode = asPlainObject((teamCompetitor as Record<string, unknown>).team);
      const opponentNode = asPlainObject((opponentCompetitor as Record<string, unknown>).team);
      const teamTotalPoints = safeNumber((teamCompetitor as Record<string, unknown>).score);
      const opponentTotalPoints = safeNumber((opponentCompetitor as Record<string, unknown>).score);
      if (teamTotalPoints === null && opponentTotalPoints === null) return;
      const statusType = asPlainObject(asPlainObject(competition.status).type);
      const venueNode = asPlainObject(competition.venue);
      const venueAddress = asPlainObject(venueNode.address);
      const status =
        sanitizeText(statusType.detail || statusType.description, 160) ||
        sanitizeText(asPlainObject(competition.status).displayClock, 80) ||
        "Final";
      const result =
        teamTotalPoints !== null && opponentTotalPoints !== null
          ? teamTotalPoints > opponentTotalPoints
            ? "W"
            : teamTotalPoints < opponentTotalPoints
              ? "L"
              : "T"
          : "";
      const score =
        teamTotalPoints !== null && opponentTotalPoints !== null ? `${teamTotalPoints}-${opponentTotalPoints}` : "";
      const gameId = sanitizeText(record.id, 40);
      if (!gameId) return;
      rowsByGameId.set(gameId, {
        league: league.label,
        team: sanitizeText(teamNode.abbreviation || team.abbreviation, 20).toUpperCase() || team.abbreviation,
        teamDisplayName: sanitizeText(teamNode.displayName || team.displayName, 120) || team.displayName,
        gameId,
        gameDate: eventYmd,
        displayDate: formatDateLabel(date, timeZone),
        homeAway,
        opponentTeamId: sanitizeText(opponentNode.id, 40),
        opponentAbbreviation: sanitizeText(opponentNode.abbreviation, 20).toUpperCase(),
        opponentDisplayName: sanitizeText(opponentNode.displayName, 120),
        status,
        result,
        score,
        teamTotalPoints,
        opponentTotalPoints,
        venue: sanitizeText(venueNode.fullName || venueAddress.city, 160),
      });
    });
  };

  const directPayload = (await fetchCachedJson<Record<string, unknown>>(scheduleUrl(league, cleanTeamId), 30 * 60 * 1000)) || {};
  collectRows(directPayload);
  const seasonSeedDate = targetGameDate || formatDateYmd(new Date());
  for (const seasonYear of scheduleSeasonCandidatesForDate(league, seasonSeedDate)) {
    if (targetGameDate && rowsByGameId.size) break;
    const seasonPayload =
      (await fetchCachedJson<Record<string, unknown>>(scheduleUrl(league, cleanTeamId, seasonYear), 30 * 60 * 1000)) || {};
    collectRows(seasonPayload);
  }

  const rows = Array.from(rowsByGameId.values()).sort((left, right) => {
    const leftMs = Date.parse(left.gameDate);
    const rightMs = Date.parse(right.gameDate);
    if (Number.isFinite(leftMs) && Number.isFinite(rightMs) && leftMs !== rightMs) return rightMs - leftMs;
    return right.gameId.localeCompare(left.gameId);
  });
  return {
    league,
    team,
    filters: {
      gameDate: targetGameDate,
      homeAway: homeAwayFilter,
      scope: targetGameDate ? "single_day" : "all_history",
      timeZone,
    },
    headers,
    rows,
    csvText: buildCsv(headers, rows),
  };
}

export async function buildSportsPlayerContext(
  leagueKey: unknown,
  teamId: unknown,
  playerId: unknown
): Promise<SportsPlayerContext> {
  const league = leagueInfo(leagueKey);
  const cleanTeamId = sanitizeText(teamId, 40);
  const cleanPlayerId = sanitizeText(playerId, 40);
  if (!cleanTeamId) throw new Error("invalid_team_id");
  if (!cleanPlayerId) throw new Error("invalid_player_id");

  const teams = await listSportsTeams(league.key);
  const team = teams.find((entry) => entry.id === cleanTeamId);
  if (!team) throw new Error("team_not_found");
  const players = await listSportsPlayers(league.key, cleanTeamId);
  const player = players.find((entry) => entry.id === cleanPlayerId);
  if (!player) throw new Error("player_not_found");

  const basePayload = (await fetchCachedJson<Record<string, unknown>>(gameLogUrl(league, cleanPlayerId), 15 * 60 * 1000)) || {};
  const seasonYears = extractSeasonYears(basePayload);
  const categoryOptions = extractCategoryOptions(basePayload);
  const yearsToFetch: number[] = [];
  const rowsByGameId = new Map<string, ParsedEventAccumulator>();
  const labelLookup = new Map<string, { label: string; description: string; category: string; sourceKey: string }>();
  const baseFilters = Array.isArray(basePayload.filters) ? (basePayload.filters as Array<Record<string, unknown>>) : [];
  const baseSeasonValue = Math.floor(
    asFinite(
      (baseFilters.find((entry) => sanitizeText(entry?.name, 40).toLowerCase() === "season") || {}).value,
      Number.NaN
    )
  );
  const baseCategoryValue = sanitizeText(
    (baseFilters.find((entry) => sanitizeText(entry?.name, 40).toLowerCase() === "category") || {}).value,
    40
  ).toLowerCase();

  const fetchPayloads = async (seasonYear: number) => {
    const outputs: Array<{ payload: Record<string, unknown>; category: string; seasonYear: number }> = [];
    for (const category of categoryOptions.length ? categoryOptions : [""]) {
      const isBase = seasonYear === baseSeasonValue && category === baseCategoryValue;
      const payload = isBase
        ? basePayload
        : (((await fetchCachedJson<Record<string, unknown>>(
            gameLogUrl(league, cleanPlayerId, String(seasonYear), category),
            15 * 60 * 1000
          )) || {}) as Record<string, unknown>);
      outputs.push({ payload, category, seasonYear });
    }
    return outputs;
  };

  const targetMinimumGames = league.key === "mlb" ? 24 : 18;
  for (const seasonYear of seasonYears.slice(0, 4)) {
    const payloads = await fetchPayloads(seasonYear);
    let rowsAddedThisYear = 0;
    payloads.forEach(({ payload, category }) => {
      const descriptors = buildStatDescriptors(payload, category);
      descriptors.forEach((descriptor) => {
        if (!labelLookup.has(descriptor.key)) {
          labelLookup.set(descriptor.key, {
            label: descriptor.label,
            description: descriptor.label,
            category: descriptor.category,
            sourceKey: descriptor.sourceKey,
          });
        }
      });
      const metaEvents = payload.events && typeof payload.events === "object" ? (payload.events as Record<string, unknown>) : {};
      const seasonTypes = Array.isArray(payload.seasonTypes) ? payload.seasonTypes : [];
      seasonTypes.forEach((seasonType) => {
        const seasonNode = seasonType && typeof seasonType === "object" ? (seasonType as Record<string, unknown>) : {};
        const seasonLabel = sanitizeText(seasonNode.displayName, 120);
        const categories = Array.isArray(seasonNode.categories) ? seasonNode.categories : [];
        categories.forEach((group) => {
          const groupNode = group && typeof group === "object" ? (group as Record<string, unknown>) : {};
          const events = Array.isArray(groupNode.events) ? groupNode.events : [];
          events.forEach((eventEntry) => {
            const eventNode = eventEntry && typeof eventEntry === "object" ? (eventEntry as Record<string, unknown>) : {};
            const eventId = sanitizeText(eventNode.eventId, 40);
            const stats = Array.isArray(eventNode.stats) ? eventNode.stats : [];
            const meta = metaEvents[eventId] && typeof metaEvents[eventId] === "object" ? (metaEvents[eventId] as Record<string, unknown>) : {};
            if (!eventId || !Object.keys(meta).length) return;
            const baseRow = rowsByGameId.get(eventId) || buildBaseEventRow({ id: eventId, ...meta });
            if (!baseRow) return;
            baseRow.seasonLabel = seasonLabel || baseRow.seasonLabel;
            descriptors.forEach((descriptor) => {
              const rawValue = asString(stats[descriptor.index], "");
              if (rawValue) baseRow.rawStats[descriptor.key] = rawValue;
              const numeric = safeNumber(rawValue);
              if (numeric !== null) {
                baseRow.metrics[descriptor.key] = numeric;
              }
              deriveSplitMetrics(descriptor, rawValue).forEach((derived) => {
                const derivedKey = withCategoryPrefix(descriptor.category, derived.key);
                baseRow.rawStats[derivedKey] = rawValue;
                baseRow.metrics[derivedKey] = derived.value;
                if (!labelLookup.has(derivedKey)) {
                  labelLookup.set(derivedKey, {
                    label: derived.label,
                    description: derived.label,
                    category: descriptor.category,
                    sourceKey: derived.sourceKey,
                  });
                }
              });
            });
            applyDerivedBaseballMetrics(baseRow);
            rowsByGameId.set(eventId, baseRow);
            rowsAddedThisYear += 1;
          });
        });
      });
    });
    if (rowsAddedThisYear > 0) yearsToFetch.push(seasonYear);
    const currentCount = Array.from(rowsByGameId.values()).length;
    if (currentCount >= targetMinimumGames) break;
  }

  const historicalRows = Array.from(rowsByGameId.values())
    .map(
      (row): NormalizedSportsHistoryRow => ({
        gameId: row.gameId,
        gameDate: row.gameDate,
        displayDate: row.displayDate,
        seasonLabel: row.seasonLabel,
        teamId: row.teamId,
        teamAbbreviation: row.teamAbbreviation,
        opponentTeamId: row.opponentTeamId,
        opponentAbbreviation: row.opponentAbbreviation,
        opponentDisplayName: row.opponentDisplayName,
        homeAway: row.homeAway,
        result: row.result,
        score: row.score,
        teamScore: row.teamScore,
        opponentScore: row.opponentScore,
        metrics: row.metrics,
        rawStats: row.rawStats,
      })
    )
    .sort((left, right) => Date.parse(left.gameDate) - Date.parse(right.gameDate));

  if (!historicalRows.length) throw new Error("sports_history_unavailable");

  const statCatalog = buildStatCatalog(league.key, historicalRows, labelLookup);
  if (!statCatalog.length) throw new Error("sports_stats_unavailable");

  const futureGames = await listSportsUpcomingGames(league.key, cleanTeamId);
  return {
    league,
    team,
    player,
    statCatalog,
    historicalRows,
    futureGames,
    seasonsUsed: yearsToFetch,
    defaultStatKey: statCatalog[0]?.key || "",
  };
}

export function buildSportsHistoricalCsv(
  context: SportsPlayerContext,
  selectedStatKey = ""
): { csvText: string; rows: Array<Record<string, unknown>>; headers: string[] } {
  const baseHeaders = [
    "league",
    "team",
    "player",
    "position",
    "gameDate",
    "displayDate",
    "seasonLabel",
    "homeAway",
    "opponentAbbreviation",
    "opponentDisplayName",
    "result",
    "score",
    "teamScore",
    "opponentScore",
  ];
  const statKeys = context.statCatalog.map((entry) => entry.key);
  const selected = sanitizeText(selectedStatKey, 120);
  const headers = selected && statKeys.includes(selected)
    ? [...baseHeaders, selected, ...statKeys.filter((key) => key !== selected)]
    : [...baseHeaders, ...statKeys];
  const rows = context.historicalRows.map((row) => {
    const out: Record<string, unknown> = {
      league: context.league.label,
      team: context.team.abbreviation,
      player: context.player.displayName,
      position: context.player.positionAbbreviation || context.player.position,
      gameDate: row.gameDate,
      displayDate: row.displayDate,
      seasonLabel: row.seasonLabel,
      homeAway: row.homeAway,
      opponentAbbreviation: row.opponentAbbreviation,
      opponentDisplayName: row.opponentDisplayName,
      result: row.result,
      score: row.score,
      teamScore: row.teamScore ?? "",
      opponentScore: row.opponentScore ?? "",
    };
    statKeys.forEach((key) => {
      const value = row.metrics[key];
      out[key] = Number.isFinite(Number(value)) ? value : "";
    });
    return out;
  });
  return {
    csvText: buildCsv(headers, rows),
    rows,
    headers,
  };
}

export function buildSportsAutopilotDatasetCsv(
  context: SportsPlayerContext,
  statKey: string
): {
  csvText: string;
  rowCount: number;
  previewRows: Array<Record<string, string | number>>;
  recentValues: number[];
  lastHistoryDate: string;
} {
  const stat = context.statCatalog.find((entry) => entry.key === statKey);
  if (!stat) throw new Error("sports_stat_not_found");
  const rows = context.historicalRows
    .map((row) => {
      const value = row.metrics[statKey];
      if (!Number.isFinite(Number(value))) return null;
      return {
        item_id: `${context.league.key}_${context.player.id}_${statKey}`,
        timestamp: formatDateYmd(new Date(row.gameDate)),
        closing_price: Number(Number(value).toFixed(6)),
      };
    })
    .filter((row): row is { item_id: string; timestamp: string; closing_price: number } => Boolean(row));
  if (rows.length < 6) throw new Error("sports_history_too_short");
  return {
    csvText: buildCsv(["item_id", "timestamp", "closing_price"], rows),
    rowCount: rows.length,
    previewRows: rows.slice(0, 12),
    recentValues: rows.slice(-5).map((row) => row.closing_price),
    lastHistoryDate: rows[rows.length - 1]?.timestamp || "",
  };
}

export function buildSportsRunPayloadExport(
  context: SportsPlayerContext,
  stat: NormalizedSportsStat,
  targetGame: NormalizedSportsGame,
  dataset: {
    rowCount: number;
    recentValues: number[];
    lastHistoryDate: string;
  }
): Record<string, unknown> {
  return {
    type: "sports_forecast_input",
    league: context.league,
    team: context.team,
    player: context.player,
    stat,
    targetGame,
    dataset: {
      rowCount: dataset.rowCount,
      lastHistoryDate: dataset.lastHistoryDate,
      recentValues: dataset.recentValues,
    },
  };
}

export function analyzeSportsPredictionCsv(
  csvText: string,
  options: {
    syntheticTicker?: string;
    statKey: string;
    statLabel: string;
    leagueLabel: string;
    playerName: string;
    teamAbbreviation: string;
    opponentAbbreviation: string;
    targetGameDate: string;
    targetGameLabel: string;
    historicalRows: NormalizedSportsHistoryRow[];
  }
): SportsPredictionAnalysisResult {
  const table = parseCsvTable(csvText);
  const quantileColumns = extractQuantileColumns(table.headers);
  const ticker = sanitizeText(options.syntheticTicker, 40) || sanitizeText(options.teamAbbreviation, 20);
  const timeColumnIndex = table.headers.findIndex((header) =>
    ["timestamp", "date", "datetime", "time"].includes(normalizeCsvHeader(header))
  );
  if (timeColumnIndex < 0 || quantileColumns.length < 3) {
    return {
      kind: "prediction_output",
      status: "error",
      ticker,
      rowCount: table.rows.length,
      columns: table.headers,
      summary: "Sports forecast output did not include a usable timestamp column and at least three quantile columns.",
      markdown: "Sports forecast output could not be analyzed because the prediction CSV schema was incomplete.",
      metrics: {},
      analysis: {},
      previewRows: previewRowsFromTable(table),
    };
  }

  const median = pickMedianQuantile(quantileColumns);
  if (!median) {
    return {
      kind: "prediction_output",
      status: "error",
      ticker,
      rowCount: table.rows.length,
      columns: table.headers,
      summary: "Sports forecast output did not include a usable median quantile column.",
      markdown: "Sports forecast output could not be analyzed because no median quantile column was detected.",
      metrics: {},
      analysis: {},
      previewRows: previewRowsFromTable(table),
    };
  }

  const targetDate = formatDateYmd(new Date(options.targetGameDate));
  const predictionRows = table.rows
    .map((row) => {
      const timestampRaw = asString(row[timeColumnIndex], "");
      const parsedMs = Date.parse(timestampRaw);
      if (!timestampRaw || !Number.isFinite(parsedMs)) return null;
      const quantiles = Object.fromEntries(
        quantileColumns.map((column) => [column.label, safeNumber(row[column.index])])
      );
      return {
        timestamp: timestampRaw,
        timestampDay: formatDateYmd(new Date(parsedMs)),
        quantiles,
        medianValue: safeNumber(row[median.index]),
      };
    })
    .filter(
      (
        row
      ): row is { timestamp: string; timestampDay: string; quantiles: Record<string, number | null>; medianValue: number | null } =>
        Boolean(row)
    );

  const targetPrediction =
    predictionRows.find((row) => row.timestampDay === targetDate) ||
    predictionRows.find((row) => Date.parse(row.timestamp) >= Date.parse(options.targetGameDate)) ||
    predictionRows[predictionRows.length - 1];
  if (!targetPrediction || !Number.isFinite(Number(targetPrediction.medianValue))) {
    return {
      kind: "prediction_output",
      status: "error",
      ticker,
      rowCount: table.rows.length,
      columns: table.headers,
      summary: "Sports forecast output did not contain a numeric point forecast for the selected game.",
      markdown: "Sports forecast output could not be analyzed because the target row was missing a numeric median forecast.",
      metrics: {},
      analysis: {},
      previewRows: previewRowsFromTable(table),
    };
  }

  const lowerColumn = quantileColumns[0];
  const upperColumn = quantileColumns[quantileColumns.length - 1];
  const lowerBound = safeNumber(targetPrediction.quantiles[lowerColumn.label]);
  const upperBound = safeNumber(targetPrediction.quantiles[upperColumn.label]);
  const recentHistory = options.historicalRows
    .map((row) => ({
      gameDate: row.gameDate,
      opponent: row.opponentAbbreviation,
      value: row.metrics[options.statKey],
    }))
    .filter((row) => Number.isFinite(Number(row.value)))
    .slice(-8)
    .map((row) => ({
      gameDate: row.gameDate,
      opponent: row.opponent,
      value: Number(Number(row.value).toFixed(4)),
    }));
  const recentValues = recentHistory.map((entry) => entry.value);
  const recentAverage = recentValues.length
    ? Number((recentValues.reduce((sum, value) => sum + value, 0) / recentValues.length).toFixed(4))
    : null;
  const summary = `${options.playerName} ${options.statLabel.toLowerCase()} forecast for ${options.targetGameLabel} centers on ${Number(
    targetPrediction.medianValue
  ).toFixed(2)}${
    lowerBound !== null && upperBound !== null ? ` with a ${lowerColumn.label}-${upperColumn.label} range of ${lowerBound.toFixed(2)} to ${upperBound.toFixed(2)}` : ""
  }.`;
  const markdown = [
    "## Sports Forecast",
    "",
    `**League:** ${sanitizeText(options.leagueLabel, 40)}`,
    `**Player:** ${sanitizeText(options.playerName, 120)}`,
    `**Team:** ${sanitizeText(options.teamAbbreviation, 20)}`,
    `**Opponent:** ${sanitizeText(options.opponentAbbreviation, 20)}`,
    `**Target game:** ${sanitizeText(options.targetGameLabel, 160)}`,
    `**Forecast stat:** ${sanitizeText(options.statLabel, 80)}`,
    "",
    "### Forecast",
    `- Point forecast (${median.label}): **${Number(targetPrediction.medianValue).toFixed(2)}**`,
    lowerBound !== null ? `- Lower bound (${lowerColumn.label}): **${lowerBound.toFixed(2)}**` : "",
    upperBound !== null ? `- Upper bound (${upperColumn.label}): **${upperBound.toFixed(2)}**` : "",
    `- Prediction row used: **${sanitizeText(targetPrediction.timestamp, 80)}**`,
    "",
    "### Recent History",
    recentHistory.length
      ? recentHistory.map((entry) => `- ${entry.gameDate} vs ${entry.opponent}: **${Number(entry.value).toFixed(2)}**`).join("\n")
      : "- Historical stat preview unavailable.",
    "",
    recentAverage !== null ? `Recent ${options.statLabel.toLowerCase()} average across ${recentHistory.length} games: **${recentAverage.toFixed(2)}**` : "",
    "",
    "### Summary",
    summary,
  ]
    .filter(Boolean)
    .join("\n");

  return {
    kind: "prediction_output",
    status: "ok",
    ticker,
    rowCount: table.rows.length,
    columns: table.headers,
    summary,
    markdown,
    metrics: {
      stat: sanitizeText(options.statLabel, 80),
      targetGameDate: sanitizeText(options.targetGameDate, 40),
      forecastValue: Number(Number(targetPrediction.medianValue).toFixed(6)),
      lowerBound: lowerBound !== null ? Number(lowerBound.toFixed(6)) : null,
      upperBound: upperBound !== null ? Number(upperBound.toFixed(6)) : null,
      recentAverage,
      historicalGames: recentHistory.length,
    },
    analysis: {
      statKey: sanitizeText(options.statKey, 120),
      statLabel: sanitizeText(options.statLabel, 80),
      targetGameDate: sanitizeText(options.targetGameDate, 40),
      targetGameLabel: sanitizeText(options.targetGameLabel, 160),
      opponentAbbreviation: sanitizeText(options.opponentAbbreviation, 20),
      recentHistory,
      predictionSeries: predictionRows.slice(0, 120).map((row) => ({
        timestamp: row.timestamp,
        median: row.medianValue,
        lower: lowerBound !== null ? safeNumber(row.quantiles[lowerColumn.label]) : null,
        upper: upperBound !== null ? safeNumber(row.quantiles[upperColumn.label]) : null,
      })),
      selectedForecast: {
        timestamp: targetPrediction.timestamp,
        quantiles: targetPrediction.quantiles,
        median: targetPrediction.medianValue,
      },
      quantileColumns: quantileColumns.map((column) => ({
        label: column.label,
        quantile: column.quantile,
      })),
    },
    previewRows: previewRowsFromTable(table),
  };
}
