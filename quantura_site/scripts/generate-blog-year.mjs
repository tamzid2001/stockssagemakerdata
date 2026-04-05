#!/usr/bin/env node
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const root = path.resolve(__dirname, "..");
const pagesDir = path.join(root, "pages", "blog");
const pagesPostsDir = path.join(pagesDir, "posts");
const pagesTopicsDir = path.join(pagesDir, "topics");
const publicBlogDir = path.join(root, "public", "blog");
const publicPostsDir = path.join(publicBlogDir, "posts");
const publicTopicsDir = path.join(publicBlogDir, "topics");

const SITE_URL = "https://quantura.studio";

const TOPICS = [
  { slug: "macro-signals", label: "Macro Signals", description: "Regime-aware macro signals and scenario framing for institutional workflows." },
  { slug: "technical-risk", label: "Technical Risk", description: "Momentum, structure, and risk trigger design for decision-ready execution." },
  { slug: "forecasting-workflows", label: "Forecasting Workflows", description: "From quantiles to scenario envelopes and execution checkpoints." },
  { slug: "market-narratives", label: "Market Narratives", description: "Narrative tracking before consensus reprices and liquidity rotates." },
  { slug: "watchlists-alerts", label: "Watchlists & Alerts", description: "Operational playbooks for watchlists, alerting, and action routing." },
  { slug: "explore-workflows", label: "Explore Workflows", description: "Public research pipelines, publishing workflows, and feedback loops." },
  { slug: "model-council", label: "Model Council", description: "Multi-model verification, citations, and guardrail-first decision support." },
  { slug: "build-notes", label: "Build Notes", description: "Engineering notes for Firebase, Cloud Functions, and native shells." },
  { slug: "data-validation", label: "Data Validation", description: "Data quality controls, source cross-checking, and governance practices." },
  { slug: "portfolio-playbooks", label: "Portfolio Playbooks", description: "Portfolio process, position sizing, and review cadences." },
  { slug: "sagemaker-canvas", label: "SageMaker Canvas", description: "No-code modeling and forecasting workflows with SageMaker Canvas." },
];

const TOPIC_BY_SLUG = new Map(TOPICS.map((topic) => [topic.slug, topic]));

const CATEGORY_SPECS = {
  "macro-signals": {
    tags: ["macro", "regime", "liquidity", "rates"],
    code: `const regimeScore = ratesDelta * 0.35 + creditSpreadDelta * 0.25 + usdImpulse * 0.2 + oilShock * 0.2;\nconst regime = regimeScore > 0.6 ? "risk-off" : regimeScore < -0.4 ? "risk-on" : "mixed";\nreturn { regimeScore, regime };`,
    why: "Macro signals shape the cost of capital, correlation regimes, and drawdown velocity. If the macro layer is wrong, security-level precision usually fails.",
    checklist: [
      "Track rates, credit spreads, and USD direction in one dashboard.",
      "Define hard regime thresholds before discussing single-name conviction.",
      "Pre-write response actions for risk-on, mixed, and risk-off states.",
      "Re-rank watchlist names by macro sensitivity every week.",
    ],
  },
  "technical-risk": {
    tags: ["technical", "momentum", "volatility", "risk"],
    code: `const setup = {\n  trend: ema20 > ema50 && ema50 > ema200,\n  momentum: rsi14 > 52 && macdHist > 0,\n  trigger: close > priorHigh\n};\nconst riskState = setup.trend && setup.momentum && setup.trigger ? "engage" : "wait";`,
    why: "Technical structure converts market noise into executable condition checks. Good risk desks enforce consistent trigger logic instead of improvised chart interpretation.",
    checklist: [
      "Separate trend qualification from entry trigger criteria.",
      "Define invalidation levels before entering exposure.",
      "Use volatility-adjusted stops, not fixed-price heuristics.",
      "Audit hit-rate and payoff by setup type each month.",
    ],
  },
  "forecasting-workflows": {
    tags: ["forecast", "quantiles", "scenarios", "process"],
    code: `const scenario = {\n  base: q50[last],\n  downside: q10[last],\n  upside: q90[last],\n};\nconst bandWidth = (scenario.upside - scenario.downside) / Math.max(close, 1);\nreturn { scenario, bandWidth };`,
    why: "Forecast outputs are only useful when tied to scenario handling rules. Quantile bands should inform sizing, not replace judgement.",
    checklist: [
      "Store baseline assumptions with each forecast run.",
      "Track band width drift through time, not only terminal values.",
      "Require a scenario-specific action plan before publication.",
      "Compare forecast deltas against realized macro shifts weekly.",
    ],
  },
  "market-narratives": {
    tags: ["narrative", "catalyst", "sentiment", "repricing"],
    code: `const narrativeDelta = headlineVelocity * 0.4 + optionsSkewChange * 0.3 + shortInterestShift * 0.3;\nconst narrativeState = narrativeDelta > 0.5 ? "accelerating" : narrativeDelta < -0.3 ? "fading" : "stable";\nreturn { narrativeDelta, narrativeState };`,
    why: "Narrative compression often precedes volatility expansion. Teams that formalize narrative shifts can react before consensus reprices.",
    checklist: [
      "Document the core bull and bear narratives side-by-side.",
      "Tie each narrative to at least one measurable trigger.",
      "Detect divergence between price action and narrative intensity.",
      "Escalate narrative breaks into explicit risk review.",
    ],
  },
  "watchlists-alerts": {
    tags: ["watchlist", "alerts", "ops", "execution"],
    code: `const alert = {\n  symbol,\n  trigger: close >= level ? "above" : "below",\n  urgency: atrPct > 0.035 ? "high" : "normal",\n};\nqueueNotification(alert);`,
    why: "Watchlists fail when they become passive archives. Alerting should be an operations discipline with explicit ownership and follow-through.",
    checklist: [
      "Limit watchlists to names with a current thesis.",
      "Attach an owner and a review cadence to each alert.",
      "Distinguish informational alerts from action-required alerts.",
      "Archive stale names aggressively to keep signal density high.",
    ],
  },
  "explore-workflows": {
    tags: ["explore", "publishing", "community", "workflow"],
    code: `const publishPayload = {\n  requestId,\n  visibility: autoPublish ? "public" : "unlisted",\n  preview: summarizeOutput(output),\n};\npostToExploreFeed(publishPayload);`,
    why: "Explore-style research loops create measurable accountability. Publishing assumptions improves model quality and team learning velocity.",
    checklist: [
      "Publish concise previews with explicit assumptions.",
      "Link every post back to source requests for auditability.",
      "Track engagement as a feedback channel, not a vanity metric.",
      "Unpublish stale theses when invalidation criteria are met.",
    ],
  },
  "model-council": {
    tags: ["llm", "verification", "citations", "guardrails"],
    code: `const draft = improvePromptEnabled ? rewritePrompt(prompt) : prompt;\nconst answer = await runProvider({ provider, model, prompt: draft });\nconst verified = verifyAgainstData(answer, modules);\nreturn { draft, answer, verified };`,
    why: "Model outputs are powerful but brittle without verification. A council approach improves reliability by combining structured context and explicit checks.",
    checklist: [
      "Enable prompt improvement for ambiguous requests.",
      "Require source-linked citations for factual claims.",
      "Store like/dislike signals to tune future prompts.",
      "Run post-answer validation against market data modules.",
    ],
  },
  "build-notes": {
    tags: ["engineering", "firebase", "gcloud", "native"],
    code: `gcloud functions deploy quanturaExploreApi \\\n  --gen2 --runtime=nodejs24 --region=us-central1 \\\n  --source=functions_explore --entry-point=quanturaExploreApi --trigger-http\n\nfirebase deploy --only hosting`,
    why: "Execution quality depends on deployment determinism. Documented build paths reduce incident frequency and rollback uncertainty.",
    checklist: [
      "Pin runtimes and deployment commands in one script.",
      "Separate hosting deploy from backend rollout checks.",
      "Log function revision IDs after every release.",
      "Validate routing with synthetic endpoint probes.",
    ],
  },
  "data-validation": {
    tags: ["data", "validation", "qa", "governance"],
    code: `const checks = [\n  notNull("symbol"),\n  range("close", 0, 1_000_000),\n  monotonicDate("timestamp"),\n  crossSourceTolerance("volume", 0.08),\n];\nreturn runChecks(dataset, checks);`,
    why: "Forecast quality is bounded by source reliability. Validation pipelines protect against silent failures and narrative drift.",
    checklist: [
      "Enforce schema and numeric bounds at ingest time.",
      "Cross-check critical fields across independent sources.",
      "Flag stale data windows before model execution.",
      "Store validation outcomes with every published artifact.",
    ],
  },
  "portfolio-playbooks": {
    tags: ["portfolio", "sizing", "risk-budget", "playbook"],
    code: `const riskBudget = portfolioVolTarget * capital;\nconst positionSize = Math.min(maxNotional, riskBudget / Math.max(expectedDrawdown, 0.01));\nreturn { riskBudget, positionSize };`,
    why: "A thesis without sizing logic is incomplete. Playbooks should map conviction and uncertainty into consistent position construction.",
    checklist: [
      "Define risk budget before selecting names.",
      "Link forecast uncertainty to notional exposure.",
      "Use scenario stress tests before increasing size.",
      "Review realized versus expected drawdown monthly.",
    ],
  },
  "sagemaker-canvas": {
    tags: ["sagemaker-canvas", "no-code", "mlops", "forecasting"],
    code: `const canvasWorkflow = {\n  dataset: "price_features_v3",\n  target: "next_10d_return",\n  objective: "regression",\n  split: "time_ordered",\n};\nconst exportJob = "canvas_prediction_export_to_quantura";\nreturn { canvasWorkflow, exportJob };`,
    why: "SageMaker Canvas gives non-coding teams a fast way to prototype models and generate decision-ready prediction exports without waiting for full engineering cycles.",
    checklist: [
      "Design features in a leakage-safe, time-ordered table.",
      "Benchmark Canvas output against a naive baseline first.",
      "Export predictions with metadata for downstream audit.",
      "Validate drift and retraining cadence by market regime.",
    ],
  },
};

const TITLE_LIBRARY = [
  "Macro Regime Map Before Positioning Risk",
  "When Momentum Confirmation Fails at the Worst Time",
  "Scenario Bands for Operators, Not Spectators",
  "Narrative Drift Detection Before Consensus Reprices",
  "Watchlist Alert Design That Actually Gets Used",
  "Publishing Research to Explore Without Creating Noise",
  "Model Council Verification Workflow for High-Impact Calls",
  "Deployment Discipline for Fast, Safe Market Releases",
  "Data Validation Rules That Prevent Silent Forecast Breaks",
  "Portfolio Sizing Under Uncertain Macro Backdrops",
  "SageMaker Canvas Baselines for Weekly Forecast Cycles",
  "Liquidity Regime Checklists for Better Entry Timing",
  "ATR-Conditioned Risk Triggers for Tactical Trades",
  "Forecast Review Rituals That Improve Decision Quality",
  "Catalyst Mapping with Explicit Invalidation Levels",
  "Alert Escalation Ladders for Small Teams",
  "Explore Feed Governance: Publish, Review, Unpublish",
  "Prompt Rewrites That Improve Model Council Precision",
  "Cloud Function Rollouts with Measurable Blast Radius",
  "Cross-Source Data Reconciliation for Live Pipelines",
  "Risk Budget Allocation Across Correlated Themes",
  "SageMaker Canvas What-If Testing for Earnings Weeks",
  "Rates Volatility and Sector Rotation Playbooks",
  "Structure Breaks Versus False Breakouts in Practice",
  "From Quantiles to Orders: Closing the Workflow Gap",
  "Narrative Compression Signals Ahead of Event Windows",
  "Turning Watchlists into Action Queues",
  "Explore Publishing Metrics That Matter",
  "Citation-First LLM Output Standards",
  "SSR + Hosting Route Checks for Production Reliability",
  "Data Freshness SLAs for Forecast Credibility",
  "Drawdown-Aware Position Sizing for Multi-Asset Books",
  "SageMaker Canvas Feature Design for Regime Shifts",
  "Macro Shock Drills for Weekly Research Meetings",
  "Trend + Volatility Filters for Cleaner Signal Intake",
  "Forecast Error Decomposition for Better Retraining",
  "Narrative-to-Trade Translation Without Overfitting",
  "Alert Fatigue Reduction in Active Watchlists",
  "Explore Post Templates for Reproducible Research",
  "Guardrails for Multi-Provider Model Routing",
  "Release Checklists for Native + Web Shells",
  "Input Validation Patterns for Market Data APIs",
  "Scenario Trees for Concentrated Portfolio Risk",
  "SageMaker Canvas to Quantura Export Pipeline",
  "Macro Breadth Dashboards for Cross-Asset Context",
  "Momentum Exhaustion Signals and Risk Tapering",
  "Forecast Confidence Intervals in Position Reviews",
  "Narrative Contradictions and Tactical Repositioning",
  "Watchlist Ownership Models for Team Execution",
  "Explore Audit Trails for Institutional Compliance",
  "Model Council Escalation Paths for High Uncertainty",
  "SageMaker Canvas Governance: Leakage, Drift, and Audit",
];

const TOPIC_SEQUENCE = [
  "macro-signals","technical-risk","forecasting-workflows","market-narratives","watchlists-alerts","explore-workflows","model-council","build-notes","data-validation","portfolio-playbooks","sagemaker-canvas",
  "macro-signals","technical-risk","forecasting-workflows","market-narratives","watchlists-alerts","sagemaker-canvas","model-council","build-notes","data-validation","portfolio-playbooks","macro-signals",
  "technical-risk","forecasting-workflows","sagemaker-canvas","market-narratives","watchlists-alerts","explore-workflows","model-council","build-notes","data-validation","portfolio-playbooks",
  "sagemaker-canvas","macro-signals","technical-risk","forecasting-workflows","market-narratives","watchlists-alerts","explore-workflows","model-council","build-notes","data-validation",
  "portfolio-playbooks","macro-signals","technical-risk","forecasting-workflows","market-narratives","watchlists-alerts","explore-workflows","model-council","sagemaker-canvas","sagemaker-canvas",
];

if (TOPIC_SEQUENCE.length !== 52 || TITLE_LIBRARY.length !== 52) {
  throw new Error("Topic/title libraries must include exactly 52 entries.");
}

const FILLER_PARAGRAPHS = [
  "Institutional workflows fail when process drift is tolerated. Quantura teams reduce drift by turning every signal into a tracked assumption, every assumption into a scenario, and every scenario into an action threshold that can be audited later.",
  "In practice, signal quality deteriorates when context windows are inconsistent. Use one ticker context, one date horizon, and one source-of-truth notebook so that forecast updates, indicator changes, and narrative shifts remain comparable over time.",
  "High-quality execution requires a separation between idea generation and risk approval. The fastest teams are not the teams that trade first; they are the teams that can explain why they acted, what invalidates the thesis, and what to do next.",
  "Research loop compression is not about skipping diligence. It is about reducing avoidable context switching, preserving assumptions in structured form, and minimizing handoff loss between forecasting, validation, and execution review.",
  "A recurring problem in discretionary workflows is undocumented confidence. Quantura playbooks require confidence to be represented as concrete ranges, expected volatility bands, and downside triggers that can be measured after the fact.",
  "When market conditions shift, stale process can be more dangerous than stale data. Weekly process reviews should compare what was planned, what was executed, and where workflow friction introduced avoidable error.",
  "For teams operating under time pressure, decision hygiene is a compounding edge. Tight templates, explicit caveats, and reproducible checkpoints keep output quality stable even when headlines are noisy.",
  "Any model-derived recommendation should be treated as a proposal, not a verdict. Cross-checking output with market structure, liquidity context, and catalyst timing prevents over-reliance on a single signal source.",
];

const EXTENDED_PARAGRAPHS = [
  "Teams that treat weekly review as a lightweight governance layer usually outperform teams that treat review as a postmortem ritual. The difference is timing: governance before a size increase is a risk control, governance after a drawdown is just documentation. Quantura operators should maintain a one-page review packet that compares expected path, realized path, and the delta explanation in plain language.",
  "A practical way to reduce rework is to keep one shared assumptions register for each live thesis. This register should include confidence bands, expected catalyst timing, and a forced-choice invalidation rule. If a thesis cannot be invalidated with observable market evidence, it is not yet operationally ready. The register also improves handoffs across time zones and between research and execution roles.",
  "Execution speed should not be measured by how quickly an order is sent. It should be measured by how quickly the team can move from a new signal to a verified action plan with known downside boundaries. This includes data validation, scenario refresh, and communication quality. The most expensive delays often come from ambiguous ownership, not slow models.",
  "Market environments can change faster than model retraining cycles. Because of that mismatch, every model-driven process needs a regime override policy. The override policy should define exactly when human operators can down-weight or ignore model output, and how that override is recorded. Over time, these overrides become valuable training data for process improvements.",
  "Institutional consistency also depends on presentation quality. A dense, reproducible template helps decision committees compare opportunities without being distracted by formatting differences. Quantura outputs should include a short thesis, a quantified risk envelope, a catalyst map, and a status line that states whether conditions are improving, deteriorating, or unchanged.",
  "Signal quality degrades quickly when watchlists grow without ownership constraints. Enforce explicit owner assignments and review dates per symbol. If a symbol has no owner or no next review date, it should not remain in active workflow. This is a simple operational rule that materially improves focus and reduces false urgency.",
  "Another common failure mode is over-optimization to recent data. Teams should pair each advanced model with at least one conservative baseline and track performance spread between them. When spread widens unexpectedly, that is a warning that process assumptions may be drifting. Treat these divergences as triggers for validation, not as immediate proof of superior alpha.",
  "Decision-ready output requires clear narrative discipline. Every thesis should include one paragraph for the base case, one for upside, and one for downside, each tied to measurable evidence. Ambiguous narrative language should be removed. This practice not only improves decision quality but also makes retrospective learning far easier.",
];

const METRIC_FRAMES = [
  "Forecast hit-rate by horizon bucket",
  "Average band-width drift versus prior week",
  "Time-to-escalation from alert trigger to owner action",
  "Thesis invalidation frequency by sector",
  "Cross-source discrepancy rate for critical fields",
  "Model council answer revision rate after verification",
  "Scenario plan adherence versus realized market path",
  "Average decision latency from signal intake to action",
];

const QUOTES = [
  { text: "Risk comes from not knowing what you're doing.", source: "Warren Buffett", url: "https://www.berkshirehathaway.com" },
  { text: "The goal of a successful trader is to make the best trades.", source: "Alexander Elder", url: "https://www.goodreads.com/quotes/795577" },
  { text: "In investing, what is comfortable is rarely profitable.", source: "Robert Arnott", url: "https://www.researchaffiliates.com" },
  { text: "Control your losses; let your winners run.", source: "Michael Covel", url: "https://www.goodreads.com/quotes/739643" },
  { text: "The four most dangerous words in investing are: this time is different.", source: "John Templeton", url: "https://www.templeton.org" },
  { text: "An investment operation is one which, upon thorough analysis, promises safety of principal.", source: "Benjamin Graham", url: "https://www.grahamanddoddsville.net" },
];

function fmtDate(date) {
  return date.toISOString().slice(0, 10);
}

function humanDate(date) {
  return date.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric", timeZone: "UTC" });
}

function rfc2822(date) {
  return date.toUTCString();
}

function slugify(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-");
}

function uniqTags(topicSlug, spec, week) {
  const base = new Set([topicSlug, ...(spec?.tags || []), "quantura", "markets"]);
  if (week % 2 === 0) base.add("execution");
  if (week % 3 === 0) base.add("risk-management");
  if (week % 5 === 0) base.add("workflow");
  return Array.from(base).slice(0, 7);
}

function buildChecklist(items) {
  return `<ul>\n${items.map((item) => `  <li>${item}</li>`).join("\n")}\n</ul>`;
}

function buildBody({ title, topic, tags, dateIso, weekIndex }) {
  const spec = CATEGORY_SPECS[topic];
  const topicMeta = TOPIC_BY_SLUG.get(topic);
  const fillerA = FILLER_PARAGRAPHS[weekIndex % FILLER_PARAGRAPHS.length];
  const fillerB = FILLER_PARAGRAPHS[(weekIndex + 2) % FILLER_PARAGRAPHS.length];
  const fillerC = FILLER_PARAGRAPHS[(weekIndex + 4) % FILLER_PARAGRAPHS.length];
  const extA = EXTENDED_PARAGRAPHS[weekIndex % EXTENDED_PARAGRAPHS.length];
  const extB = EXTENDED_PARAGRAPHS[(weekIndex + 2) % EXTENDED_PARAGRAPHS.length];
  const extC = EXTENDED_PARAGRAPHS[(weekIndex + 4) % EXTENDED_PARAGRAPHS.length];
  const extD = EXTENDED_PARAGRAPHS[(weekIndex + 6) % EXTENDED_PARAGRAPHS.length];
  const metricA = METRIC_FRAMES[weekIndex % METRIC_FRAMES.length];
  const metricB = METRIC_FRAMES[(weekIndex + 3) % METRIC_FRAMES.length];
  const quote = QUOTES[weekIndex % QUOTES.length];

  const sectionSteps = [
    `Define the market objective for this cycle and pin it to one decision horizon.`,
    `Load context in <a href=\"/terminal\">Terminal</a> and collect structured modules that support or reject the thesis.`,
    `Run scenario framing in <a href=\"/forecasting\">Forecast</a> and record quantile boundaries with expected catalysts.`,
    `Cross-check signal quality with <a href=\"/research\">Research</a> and inspect narrative divergence before escalation.`,
    `Publish a concise note to <a href=\"/explore\">Explore Feed</a> and route unresolved uncertainty to <a href=\"/model-council\">Model Council</a>.`,
    `Convert approved actions into alert thresholds and assign owner-level accountability.`,
  ];

  const risks = [
    "Data leakage can produce deceptively strong backtests that collapse out of sample.",
    "Regime shifts can invalidate historical relationships quickly, especially around policy events.",
    "Narrative momentum can overpower model outputs in short windows; sizing must reflect that uncertainty.",
    "Cross-source discrepancies can create false precision if validation checks are skipped.",
  ];

  const canvasBlock = topic === "sagemaker-canvas"
    ? `
      <h2>Quantura + Canvas workflow</h2>
      <p>
        For SageMaker Canvas programs, the production-ready handoff is: feature preparation in a leakage-safe table,
        Canvas training and evaluation, prediction export, and ingestion into Quantura for visualization and decision routing.
        This keeps no-code experimentation aligned with institutional controls.
      </p>
      <ol>
        <li><strong>Prepare features:</strong> keep time-ordered joins and explicit train/test cutoff dates.</li>
        <li><strong>Train in Canvas:</strong> compare baseline and tuned configurations; keep evaluation artifacts.</li>
        <li><strong>Evaluate rigorously:</strong> include directional accuracy, error distribution, and stability by regime.</li>
        <li><strong>Export predictions:</strong> include symbol, horizon, model version, and confidence fields.</li>
        <li><strong>Visualize in Quantura:</strong> overlay forecasts with market structure and live narrative signals.</li>
        <li><strong>Operationalize:</strong> convert outputs into watchlist actions and alert ownership.</li>
      </ol>
      <p>
        The practical constraint is governance: even no-code workflows must satisfy reproducibility, traceability,
        and rollback requirements. Canvas accelerates iteration, but discipline still determines quality.
      </p>
    `
    : "";

  const codeFence = `<pre><code class=\"language-javascript\">${spec.code.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</code></pre>`;

  const tagsLine = tags.map((tag) => `#${tag}`).join(" ");

  return `
    <figure style="margin: 0 0 18px;">
      <img src="/assets/hero-illustration.svg" alt="Quantura research workflow visual" style="width:100%;border-radius:16px;display:block;" />
      <figcaption class="small muted" style="margin-top:8px;">Quantura institutional workflow brief · ${topicMeta?.label || topic}</figcaption>
    </figure>
    <p>
      ${title} is written for operators who need a repeatable bridge between signal intake and action execution.
      The core objective is to reduce latency without reducing rigor. ${fillerA}
    </p>
    <p>
      In this playbook, the emphasis is not prediction theater; it is process reliability.
      ${fillerB}
    </p>
    <p>${extA}</p>
    <p>${extB}</p>

    <h2>Why it matters</h2>
    <p>${spec.why}</p>
    <p>
      ${fillerC}
      <em>"${quote.text}"</em> — ${quote.source}.
      <span class=\"small\">Source: <a href=\"${quote.url}\" target=\"_blank\" rel=\"noopener noreferrer\">${quote.url}</a></span>
    </p>
    <p>${extC}</p>

    <h2>Practical checklist</h2>
    ${buildChecklist(spec.checklist)}

    <h2>Execution steps</h2>
    <ol>
      ${sectionSteps.map((step) => `<li>${step}</li>`).join("\n      ")}
    </ol>

    <h2>Implementation snippet</h2>
    <p>
      Keep implementation explicit and auditable. The pseudo-code below illustrates one way to formalize
      the decision layer for this workflow.
    </p>
    ${codeFence}

    ${canvasBlock}

    <h2>Data and validation notes</h2>
    <p>
      Every run should log source timestamps, transformation version, and the validation scorecard used before decisions were made.
      This is critical for governance and for reliable debriefs when the market path diverges from expectations.
      ${extD}
    </p>
    <p>
      If you rely on no-code outputs in SageMaker Canvas or model-assisted drafting in Model Council, keep a strict separation
      between exploratory notes and decision-authorized notes. Exploratory artifacts can move quickly; decision artifacts must be reproducible.
    </p>

    <h2>Execution metrics to track</h2>
    <ul>
      <li>${metricA}</li>
      <li>${metricB}</li>
      <li>Owner response time for watchlist alerts tagged as high urgency</li>
      <li>Percent of published notes with explicit invalidation rules</li>
      <li>Share of decisions that include documented downside and scenario response</li>
    </ul>

    <h2>Risks / caveats</h2>
    <p class=\"small\"><strong>LLMs can sometimes make mistakes.</strong></p>
    ${buildChecklist(risks)}

    <h2>Weekly review template</h2>
    <ol>
      <li>What changed in macro context and why does it matter for this thesis?</li>
      <li>Did forecast dispersion widen or narrow, and what does that imply for sizing?</li>
      <li>Which catalyst is now most likely to break the current narrative?</li>
      <li>What is the single highest-impact risk if the thesis is wrong right now?</li>
      <li>What action should be taken before the next review window?</li>
    </ol>

    <h2>Decision handoff</h2>
    <p>
      Before finalizing decisions, route findings to <a href=\"/pricing\">Pricing</a> tier policy checks,
      validate entitlement limits, and ensure the request metadata is stored for future review.
      This is where process quality compounds over time.
    </p>
    <p>
      Final operator note (${dateIso}): ${tagsLine}. Keep assumptions explicit, keep triggers measurable,
      and never separate signal quality from execution quality.
    </p>
  `;
}

function blogPostHtml(meta) {
  const { title, dateIso, excerpt, slug, tags, topic } = meta;
  const canonical = `${SITE_URL}/blog/posts/${slug}`;
  const dateObj = new Date(`${dateIso}T00:00:00.000Z`);
  const topicMeta = TOPIC_BY_SLUG.get(topic);
  const body = buildBody(meta);
  const keywords = Array.from(new Set(["quantura", "blog", topic, ...(tags || [])])).join(", ");
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${title} | Quantura Blog</title>
    <meta name="description" content="${excerpt}" />
    <meta name="keywords" content="${keywords}" />
    <meta name="robots" content="index, follow" />
    <link rel="canonical" href="${canonical}" />
    <meta property="og:title" content="${title} | Quantura Blog" />
    <meta property="og:description" content="${excerpt}" />
    <meta property="og:type" content="article" />
    <meta property="og:url" content="${canonical}" />
    <meta property="og:image" content="/assets/hero-illustration.svg" />
    <meta property="og:site_name" content="Quantura" />
    <meta property="og:locale" content="en_US" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="${title} | Quantura Blog" />
    <meta name="twitter:description" content="${excerpt}" />
    <meta name="twitter:image" content="/assets/hero-illustration.svg" />
    <meta name="twitter:url" content="${canonical}" />
    <meta property="article:published_time" content="${dateIso}" />
    <meta property="article:section" content="${topicMeta?.label || "Research"}" />
    ${tags.map((tag) => `<meta property="article:tag" content="${tag}" />`).join("\n    ")}
    <link rel="alternate" type="application/rss+xml" title="Quantura Blog RSS" href="/blog/rss.xml" />
    <link rel="icon" href="/assets/quantura-icon.svg" type="image/svg+xml" />
    <link rel="manifest" href="/manifest.json" />
    <link rel="stylesheet" href="/styles.css" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/iconoir-icons/iconoir@main/css/iconoir.css" />
    <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "BlogPosting",
        "headline": "${title}",
        "description": "${excerpt}",
        "datePublished": "${dateIso}",
        "dateModified": "${dateIso}",
        "author": {
          "@type": "Organization",
          "name": "Quantura"
        },
        "publisher": {
          "@type": "Organization",
          "name": "Quantura"
        },
        "mainEntityOfPage": "${canonical}"
      }
    </script>
    <script defer src="/app.js"></script>
  </head>
  <body>
    ${siteHeaderHtml()}

    <main>
      <section class="page-hero">
        <div class="container content-grid">
          <div>
            <div class="eyebrow">Quantura Blog</div>
            <h1>${title}</h1>
            <p>${excerpt}</p>
            <div class="small muted">Published ${humanDate(dateObj)} · Topic: <a href="/blog/topics/${topic}">${topicMeta?.label || topic}</a></div>
          </div>
          <div class="card">
            <h3>Post metadata</h3>
            <div class="small"><strong>Slug:</strong> ${slug}</div>
            <div class="small"><strong>Date:</strong> ${dateIso}</div>
            <div class="small"><strong>Tags:</strong> ${tags.join(", ")}</div>
            <div class="small" style="margin-top:8px;"><a href="/blog">Back to blog index</a></div>
          </div>
        </div>
      </section>

      <section class="section">
        <div class="container">
          <article class="card" style="padding: 28px;">
            ${body}
          </article>
        </div>
      </section>
    </main>

    <footer class="footer">
      <div class="container footer-grid">
        <div>
          <div class="logo">QUANTURA</div>
          <p class="small">Decision intelligence for market research, forecasting, and execution.</p>
        </div>
        <div class="small">
          <strong>Explore</strong>
          <div><a href="/">Home</a></div>
          <div><a href="/terminal">Terminal</a></div>
          <div><a href="/explore">Explore Feed</a></div>
          <div><a href="/research">Research</a></div>
          <div><a href="/blog">Blog</a></div>
          <div><a href="/pricing">Pricing</a></div>
        </div>
      </div>
      <div class="container small" style="margin-top: 24px;">© 2026 Quantura. All rights reserved.</div>
    </footer>
  </body>
</html>
`;
}

function siteHeaderHtml() {
  return `
    <header class="header">
      <div class="container nav">
        <a class="logo" href="/" aria-label="Quantura home">
          <img class="logo-img" src="/assets/quantura-icon.svg" alt="" aria-hidden="true" />
          <span>QUANTURA</span>
        </a>
        <nav class="nav-links">
          <a href="/explore" data-analytics="nav_explore"><i class="iconoir-binocular" aria-hidden="true"></i><span>Explore</span></a>
          <a href="/research" data-analytics="nav_research"><i class="iconoir-bookmark-book" aria-hidden="true"></i><span>Research</span></a>
          <a href="/blog" data-analytics="nav_blog"><i class="iconoir-page" aria-hidden="true"></i><span>Blog</span></a>
          <a href="/events" data-analytics="nav_events"><i class="iconoir-calendar" aria-hidden="true"></i><span>Events</span></a>
          <a href="/shop" data-analytics="nav_shop"><i class="iconoir-shop" aria-hidden="true"></i><span>Shop</span></a>
          <a href="/about" data-analytics="nav_about"><i class="iconoir-info-circle" aria-hidden="true"></i><span>About</span></a>
          <a href="/pricing" data-analytics="nav_pricing"><i class="iconoir-wallet" aria-hidden="true"></i><span>Pricing</span></a>
          <a href="/contact" data-analytics="nav_contact"><i class="iconoir-mail" aria-hidden="true"></i><span>Contact Us</span></a>
        </nav>
        <div class="nav-actions"></div>
      </div>
    </header>
  `;
}

function blogIndexHtml(posts) {
  const latest = posts.slice(0, 12);
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Quantura Blog | Institutional Research Workflows</title>
    <meta name="description" content="Institutional-grade research notes on macro signals, forecasting workflows, and execution playbooks." />
    <meta name="keywords" content="quantura blog, market research blog, forecasting workflows, model council, institutional investing" />
    <meta name="robots" content="index, follow" />
    <link rel="canonical" href="${SITE_URL}/blog" />
    <meta property="og:title" content="Quantura Blog" />
    <meta property="og:description" content="Signal → scenario → decision-ready output with weekly Quantura research notes." />
    <meta property="og:type" content="website" />
    <meta property="og:url" content="${SITE_URL}/blog" />
    <meta property="og:image" content="/assets/quantura-logo.svg" />
    <meta property="og:site_name" content="Quantura" />
    <meta property="og:locale" content="en_US" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="Quantura Blog" />
    <meta name="twitter:description" content="Signal → scenario → decision-ready output with weekly Quantura research notes." />
    <meta name="twitter:image" content="/assets/quantura-logo.svg" />
    <meta name="twitter:url" content="${SITE_URL}/blog" />
    <link rel="alternate" type="application/rss+xml" title="Quantura Blog RSS" href="/blog/rss.xml" />
    <link rel="icon" href="/assets/quantura-icon.svg" type="image/svg+xml" />
    <link rel="stylesheet" href="/styles.css" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/iconoir-icons/iconoir@main/css/iconoir.css" />
    <script defer src="/app.js"></script>
  </head>
  <body>
    ${siteHeaderHtml()}

    <main>
      <section class="page-hero">
        <div class="container content-grid">
          <div>
            <div class="eyebrow">Quantura Blog</div>
            <h1>Institutional workflow, simplified.</h1>
            <p>From signal → scenario → decision-ready output, with weekly notes on execution, controls, and model operations.</p>
          </div>
          <div class="card">
            <h3>Popular topics</h3>
            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px;">
              ${TOPICS.map((topic) => `<a class="tag" href="/blog/topics/${topic.slug}">${topic.label}</a>`).join("\n              ")}
            </div>
          </div>
        </div>
      </section>

      <section class="section">
        <div class="container">
          <div class="section-title">
            <h2>Latest posts</h2>
            <p class="small">52 scheduled weekly posts across one year.</p>
          </div>
          <div class="grid-3">
            ${latest.map((post) => `
            <a class="card" href="/blog/posts/${post.slug}">
              <h3>${post.title}</h3>
              <div class="small">${humanDate(new Date(`${post.dateIso}T00:00:00.000Z`))}</div>
              <p class="small" style="margin-top: 12px;">${post.excerpt}</p>
              <div class="tag" style="margin-top: 14px;">${TOPIC_BY_SLUG.get(post.topic)?.label || post.topic}</div>
            </a>`).join("\n")}
          </div>
        </div>
      </section>
    </main>
  </body>
</html>
`;
}

function topicPageHtml(topicSlug, posts) {
  const topic = TOPIC_BY_SLUG.get(topicSlug);
  const filtered = posts.filter((post) => post.topic === topicSlug).slice(0, 52);
  const topicKeywords = `quantura, ${topic.slug}, ${topic.label.toLowerCase()}, institutional research, market workflows`;
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${topic.label} | Quantura Blog Topic</title>
    <meta name="description" content="${topic.description}" />
    <meta name="keywords" content="${topicKeywords}" />
    <meta name="robots" content="index, follow" />
    <link rel="canonical" href="${SITE_URL}/blog/topics/${topic.slug}" />
    <meta property="og:title" content="${topic.label} | Quantura Blog Topic" />
    <meta property="og:description" content="${topic.description}" />
    <meta property="og:type" content="website" />
    <meta property="og:url" content="${SITE_URL}/blog/topics/${topic.slug}" />
    <meta property="og:image" content="/assets/hero-illustration.svg" />
    <meta property="og:site_name" content="Quantura" />
    <meta property="og:locale" content="en_US" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="${topic.label} | Quantura Blog Topic" />
    <meta name="twitter:description" content="${topic.description}" />
    <meta name="twitter:image" content="/assets/hero-illustration.svg" />
    <meta name="twitter:url" content="${SITE_URL}/blog/topics/${topic.slug}" />
    <link rel="icon" href="/assets/quantura-icon.svg" type="image/svg+xml" />
    <link rel="stylesheet" href="/styles.css" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/iconoir-icons/iconoir@main/css/iconoir.css" />
    <script defer src="/app.js"></script>
  </head>
  <body>
    ${siteHeaderHtml()}

    <main>
      <section class="page-hero">
        <div class="container content-grid">
          <div>
            <div class="eyebrow">Topic</div>
            <h1>${topic.label}</h1>
            <p>${topic.description}</p>
            <p class="small"><a href="/blog">Back to blog index</a></p>
          </div>
        </div>
      </section>
      <section class="section">
        <div class="container">
          <div class="grid-3">
            ${filtered.map((post) => `
            <a class="card" href="/blog/posts/${post.slug}">
              <h3>${post.title}</h3>
              <div class="small">${humanDate(new Date(`${post.dateIso}T00:00:00.000Z`))}</div>
              <p class="small" style="margin-top: 12px;">${post.excerpt}</p>
            </a>`).join("\n")}
          </div>
        </div>
      </section>
    </main>
  </body>
</html>`;
}

function rssXml(posts) {
  const items = posts.slice(0, 52).map((post) => {
    const link = `${SITE_URL}/blog/posts/${post.slug}`;
    return `
    <item>
      <title>${post.title} | Quantura Blog</title>
      <link>${link}</link>
      <guid>${link}</guid>
      <pubDate>${rfc2822(new Date(`${post.dateIso}T00:00:00.000Z`))}</pubDate>
      <description><![CDATA[${post.excerpt}]]></description>
    </item>`;
  }).join("\n");

  const latestDate = posts[0] ? rfc2822(new Date(`${posts[0].dateIso}T00:00:00.000Z`)) : new Date().toUTCString();

  return `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Quantura Blog</title>
    <link>${SITE_URL}/blog</link>
    <description>Institutional-grade market workflows and forecasting notes from Quantura.</description>
    <language>en-us</language>
    <lastBuildDate>${latestDate}</lastBuildDate>${items}
  </channel>
</rss>
`;
}

async function ensureCleanDir(dir) {
  await fs.rm(dir, { recursive: true, force: true });
  await fs.mkdir(dir, { recursive: true });
}

async function writeFile(p, content) {
  await fs.mkdir(path.dirname(p), { recursive: true });
  await fs.writeFile(p, content, "utf8");
}

async function main() {
  const start = new Date("2025-03-06T00:00:00.000Z");
  const posts = [];
  const usedSlugs = new Set();

  for (let i = 0; i < 52; i += 1) {
    const date = new Date(start);
    date.setUTCDate(start.getUTCDate() + i * 7);
    const dateIso = fmtDate(date);
    const topic = TOPIC_SEQUENCE[i];
    const rawTitle = TITLE_LIBRARY[i];
    const title = `Week ${i + 1}: ${rawTitle}`;
    const baseSlug = `${dateIso}-${slugify(rawTitle)}`;
    let slug = baseSlug;
    let n = 2;
    while (usedSlugs.has(slug)) {
      slug = `${baseSlug}-${n}`;
      n += 1;
    }
    usedSlugs.add(slug);
    const spec = CATEGORY_SPECS[topic];
    const tags = uniqTags(topic, spec, i + 1);
    const excerpt = `${TOPIC_BY_SLUG.get(topic)?.label || "Quantura"}: weekly operator notes on signal quality, scenario framing, and execution controls.`;

    posts.push({
      weekIndex: i + 1,
      title,
      rawTitle,
      slug,
      dateIso,
      topic,
      tags,
      excerpt,
      heroImage: "/assets/hero-illustration.svg",
      canonical: `${SITE_URL}/blog/posts/${slug}`,
      description: excerpt,
    });
  }

  posts.sort((a, b) => (a.dateIso < b.dateIso ? 1 : -1));

  await ensureCleanDir(pagesPostsDir);
  await ensureCleanDir(publicPostsDir);
  await ensureCleanDir(pagesTopicsDir);
  await ensureCleanDir(publicTopicsDir);

  for (const post of posts) {
    const html = blogPostHtml(post);
    await writeFile(path.join(pagesPostsDir, `${post.slug}.html`), html);
    await writeFile(path.join(publicPostsDir, `${post.slug}.html`), html);
  }

  const indexHtml = blogIndexHtml(posts);
  await writeFile(path.join(pagesDir, "index.html"), indexHtml);
  await writeFile(path.join(publicBlogDir, "index.html"), indexHtml);

  for (const topic of TOPICS) {
    const html = topicPageHtml(topic.slug, posts);
    await writeFile(path.join(pagesTopicsDir, `${topic.slug}.html`), html);
    await writeFile(path.join(publicTopicsDir, `${topic.slug}.html`), html);
  }

  await writeFile(path.join(publicBlogDir, "rss.xml"), rssXml(posts));

  const manifest = {
    generatedAt: new Date().toISOString(),
    count: posts.length,
    topics: TOPICS,
    posts,
  };
  await writeFile(path.join(pagesDir, "posts.manifest.json"), JSON.stringify(manifest, null, 2));
  await writeFile(path.join(publicBlogDir, "posts.manifest.json"), JSON.stringify(manifest, null, 2));

  console.log(`Generated ${posts.length} posts from ${posts[posts.length - 1].dateIso} to ${posts[0].dateIso}.`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
