(() => {
  const CACHE_PREFIX = "quantura_analysis_cache_v1";
  const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

  const SYSTEM_PROMPT = [
    "You are Quantura's informational market analysis assistant.",
    "Output must stay informational and factual.",
    "Do not provide investment advice.",
    "Never output buy/sell/hold directives.",
    "Return strict JSON only.",
  ].join(" ");

  const USER_PROMPT_TEMPLATE = [
    "Return STRICT JSON with exact keys:",
    '{"thesis_summary":"string","key_drivers":["string"],"risks":["string"],"data_quality_notes":["string"],"follow_up_questions":["string"],"confidence":"low|medium|high"}',
    "No markdown code fences. No prose outside JSON.",
    "Ticker: {{TICKER}}",
    "Follow-up: {{FOLLOW_UP}}",
    "Feature pack JSON:",
    "{{FEATURE_PACK_JSON}}",
  ].join("\n");

  const parseJsonObjectFromText = (text) => {
    const raw = String(text || "").trim();
    if (!raw) throw new Error("Empty model response.");

    const noFence = raw
      .replace(/^```(?:json)?/i, "")
      .replace(/```$/i, "")
      .trim();

    const start = noFence.indexOf("{");
    const end = noFence.lastIndexOf("}");
    if (start < 0 || end < start) {
      throw new Error("Model response did not include a JSON object.");
    }

    return JSON.parse(noFence.slice(start, end + 1));
  };

  const validateAnalysisSchema = (payload) => {
    if (!payload || typeof payload !== "object") {
      return { ok: false, error: "Response is not an object." };
    }

    const asStringArray = (value) =>
      Array.isArray(value)
        ? value.map((item) => String(item ?? "").trim()).filter(Boolean)
        : null;

    const thesis = String(payload.thesis_summary || "").trim();
    const keyDrivers = asStringArray(payload.key_drivers);
    const risks = asStringArray(payload.risks);
    const dataNotes = asStringArray(payload.data_quality_notes);
    const followUps = asStringArray(payload.follow_up_questions);
    const confidence = String(payload.confidence || "").trim().toLowerCase();

    if (!thesis) return { ok: false, error: "thesis_summary is required." };
    if (!keyDrivers) return { ok: false, error: "key_drivers must be an array of strings." };
    if (!risks) return { ok: false, error: "risks must be an array of strings." };
    if (!dataNotes) return { ok: false, error: "data_quality_notes must be an array of strings." };
    if (!followUps) return { ok: false, error: "follow_up_questions must be an array of strings." };
    if (!["low", "medium", "high"].includes(confidence)) {
      return { ok: false, error: "confidence must be low|medium|high." };
    }

    return {
      ok: true,
      value: {
        thesis_summary: thesis,
        key_drivers: keyDrivers,
        risks,
        data_quality_notes: dataNotes,
        follow_up_questions: followUps,
        confidence,
      },
    };
  };

  const getHistoryRows = async (ticker) => {
    try {
      const end = new Date();
      const start = new Date(end.getTime() - 50 * 24 * 60 * 60 * 1000);
      const params = new URLSearchParams({
        ticker: String(ticker || "").trim().toUpperCase(),
        interval: "1d",
        start: start.toISOString().slice(0, 10),
        end: end.toISOString().slice(0, 10),
      });
      const response = await fetch(`/api/ticker/history?${params.toString()}`, {
        method: "GET",
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) return [];
      const rows = payload?.rows;
      return Array.isArray(rows) ? rows : [];
    } catch {
      return [];
    }
  };

  const toNumber = (value) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const computeReturns = (rows) => {
    const closes = (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const close = toNumber(row?.Close ?? row?.close ?? row?.Price ?? row?.price);
        return close;
      })
      .filter((value) => Number.isFinite(value));

    if (closes.length < 2) {
      return { oneDayPct: null, fiveDayPct: null, oneMonthPct: null, points: closes.length };
    }

    const last = closes[closes.length - 1];
    const pct = (fromIndex) => {
      if (closes.length + fromIndex < 0) return null;
      const base = closes[closes.length + fromIndex];
      if (!Number.isFinite(base) || base === 0) return null;
      return ((last / base) - 1) * 100;
    };

    return {
      oneDayPct: pct(-2),
      fiveDayPct: closes.length >= 6 ? pct(-6) : null,
      oneMonthPct: closes.length >= 22 ? pct(-22) : null,
      points: closes.length,
    };
  };

  const buildFeaturePack = ({ symbol, record, historyRows }) => {
    const price = record?.price && typeof record.price === "object" ? record.price : {};
    const valuation = record?.valuation && typeof record.valuation === "object" ? record.valuation : {};
    const fundamentals = record?.fundamentals && typeof record.fundamentals === "object" ? record.fundamentals : {};
    const profile = record?.profile && typeof record.profile === "object" ? record.profile : {};
    const risk = record?.risk && typeof record.risk === "object" ? record.risk : {};
    const dividends = record?.dividends && typeof record.dividends === "object" ? record.dividends : {};

    const returns = computeReturns(historyRows || []);
    const missingFields = [];

    for (const [sectionName, sectionObj] of Object.entries({ valuation, fundamentals, profile, risk, dividends })) {
      for (const [key, value] of Object.entries(sectionObj || {})) {
        if (value === null || value === undefined || value === "") {
          missingFields.push(`${sectionName}.${key}`);
        }
      }
    }

    return {
      symbol,
      asOf: record?.asOf || null,
      source: record?.source || "yfinance",
      price,
      valuation,
      fundamentals,
      profile,
      risk,
      dividends,
      returns,
      dataQuality: {
        historyPoints: returns.points,
        missingCount: missingFields.length,
        missingFields: missingFields.slice(0, 60),
      },
    };
  };

  const readCached = (key) => {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      const payload = JSON.parse(raw);
      const cachedAt = Number(payload?.cachedAt || 0);
      if (!Number.isFinite(cachedAt) || Date.now() - cachedAt > CACHE_TTL_MS) {
        localStorage.removeItem(key);
        return null;
      }
      return payload;
    } catch {
      return null;
    }
  };

  const writeCache = (key, value) => {
    try {
      localStorage.setItem(
        key,
        JSON.stringify({
          cachedAt: Date.now(),
          value,
        })
      );
    } catch {
      // Ignore cache write failures.
    }
  };

  const streamChatJson = async ({ ticker, prompt, model = "gpt-5-mini" }) => {
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      credentials: "same-origin",
      body: JSON.stringify({
        ticker,
        question: prompt,
        language: "en",
        model,
        messages: [{ role: "user", content: prompt }],
        meta: { feature: "iterative_analysis" },
      }),
    });

    if (!response.ok) {
      let detail = `HTTP ${response.status}`;
      try {
        const err = await response.json();
        if (err?.error) detail = String(err.error);
      } catch {
        // Ignore parsing errors.
      }
      throw new Error(detail);
    }

    if (!response.body) {
      throw new Error("Streaming response body unavailable.");
    }

    const decoder = new TextDecoder();
    const reader = response.body.getReader();
    let buffer = "";
    let answer = "";
    let usage = null;

    const flushEventBlock = (block) => {
      const lines = String(block || "")
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean);
      const dataLines = lines.filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trim());
      if (!dataLines.length) return;

      const raw = dataLines.join("\n");
      if (!raw || raw === "[DONE]") return;

      let payload = null;
      try {
        payload = JSON.parse(raw);
      } catch {
        payload = null;
      }
      if (!payload || typeof payload !== "object") return;

      if (payload.type === "delta") {
        answer += String(payload.text || "");
      } else if (payload.type === "done") {
        usage = payload.usage && typeof payload.usage === "object" ? payload.usage : usage;
      } else if (payload.type === "error") {
        throw new Error(String(payload.message || "Streaming chat request failed."));
      }
    };

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      while (true) {
        const boundary = buffer.indexOf("\n\n");
        if (boundary < 0) break;
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        flushEventBlock(block);
      }
    }

    if (buffer.trim()) {
      flushEventBlock(buffer);
    }

    return {
      answer: answer.trim(),
      usage,
    };
  };

  const buildPrompt = ({ ticker, featurePack, followUp }) => {
    return USER_PROMPT_TEMPLATE
      .replace("{{TICKER}}", String(ticker || ""))
      .replace("{{FOLLOW_UP}}", String(followUp || "none"))
      .replace("{{FEATURE_PACK_JSON}}", JSON.stringify(featurePack, null, 2));
  };

  const run = async ({ symbol, record, followUp = "", model = "gpt-5-mini" }) => {
    const ticker = String(symbol || "").trim().toUpperCase();
    if (!ticker) {
      throw new Error("Ticker is required for analysis.");
    }

    const cacheKey = `${CACHE_PREFIX}:${ticker}:${model}:${followUp || "base"}`;
    const cached = readCached(cacheKey);
    if (cached?.value) {
      return { ...cached.value, cached: true };
    }

    const historyRows = await getHistoryRows(ticker);
    const featurePack = buildFeaturePack({ symbol: ticker, record, historyRows });

    let prompt = `${SYSTEM_PROMPT}\n\n${buildPrompt({ ticker, featurePack, followUp })}`;
    let parsedResult = null;
    let usage = null;

    for (let attempt = 0; attempt < 3; attempt += 1) {
      const streamed = await streamChatJson({ ticker, prompt, model });
      usage = streamed.usage;
      const parsed = parseJsonObjectFromText(streamed.answer);
      const validation = validateAnalysisSchema(parsed);
      if (validation.ok) {
        parsedResult = validation.value;
        break;
      }
      prompt += "\n\nYour previous response failed schema validation. Return ONLY valid JSON that matches the required schema exactly.";
      if (attempt === 2) {
        throw new Error(validation.error || "Model response schema invalid.");
      }
    }

    const output = {
      result: parsedResult,
      usage,
      featurePack,
      model,
      ticker,
    };
    writeCache(cacheKey, output);
    return { ...output, cached: false };
  };

  window.QuanturaAnalysisService = {
    run,
    validateAnalysisSchema,
    buildFeaturePack,
  };
})();
