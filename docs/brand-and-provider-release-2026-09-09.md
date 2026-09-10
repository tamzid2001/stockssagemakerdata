# Quantura presentation and provider update

## Shared application, not a replacement stack

Canonical HTML remains in `quantura_site/pages`, with synchronized Express SSR templates. The shared vanilla-JavaScript shell continues to use Firebase authentication, workspace authorization and existing forecast/data services. No production data migration or numerical model replacement is included in this release.

The supplied Quantura artwork is the source for favicon, install icons, social preview and header branding. The final supplied Earth, light background, wave and laptop assets are optimized WebP derivatives. The laptop is visibly labeled a product concept with example values; it is not a live terminal or a claim of available trading functionality. The fixed homepage dock is removed. Mobile menus expose text for Q Forecast, Screener, API Docs, Notifications, and Sign in/Dashboard.

## Editorial images

All 78 current blog posts use 75 reviewed Unsplash API photographs (three data-center photographs are reused). `quantura_site/brand/blog-photos.json` records source IDs, image URLs, photographer links and download tracking timestamps. Original images remain hotlinked to Unsplash with `ixid`; every figure has photographer/Unsplash attribution and referral parameters. The selected-image download endpoint was called before publication. No Unsplash credential is included in pages or the registry.

`node quantura_site/scripts/blog-photos.mjs --render` rebuilds existing reviewed selections without API calls. `--publish` tracks newly selected images before rendering, checkpointing each successful tracking call for retry safety. The editorial tool requires server-side `UNSPLASH_ACCESS_KEY` only when it calls the API. Do not run it on page views or every deployment. No full copyrighted articles are copied.

## Kalshi protocol review

The linked manifest records 231 official documentation pages retrieved with HTTP 200 on September 9, 2026, including the standard, margin, WebSocket and FIX documentation. The REST operation inventory contains 157 operations. This is documentation coverage, not a claim that Quantura implements trading/FIX/margin operations or tested every endpoint against production.

Changes to existing read-only integrations include the current external API origin, fixed-point dollar/quantity field handling, documented archive routing/cutoffs, cursor safeguards, explicit truncated-discovery reporting, settlement-state distinctions, and server-verified event milestones for pregame cutoffs. Missing/ambiguous starts fail safely; expiration is not treated as game start. Binary and scalar contracts are distinguished. Quantura does not add order placement in this release.

## Credentials

The primary Vercel API project consumes existing server-only names `POLYMARKET_PUBLIC_KEY` (the API key ID) and `POLYMARKET_SECRET_KEY` (Ed25519 signing material). Both were updated as sensitive production/preview variables. A signed read-only `GET /v1/account/balances` returned HTTP 200; the response body was not logged. Deployment is required to activate updated variables in new production instances. No values are committed. Public history/discovery does not itself prove authenticated access.

## Support assistant

`POST /api/support/chat` reuses platform session/API-key authentication, requires `account:read` and a non-anonymous account, and enforces per-user and global Firestore request budgets. `OPENAI_API_KEY` remains server-side. GPT-5.6 Luna uses low reasoning effort, structured output, `store:false`, curated product guidance, and no account actions or arbitrary tools. Answers render as text with allowlisted documentation links; conversations are memory-only in the browser. Optional configuration: `SUPPORT_CHAT_DAILY_LIMIT` (default 30/user) and `SUPPORT_CHAT_GLOBAL_DAILY_LIMIT` (default 1,000). Live model/schema smoke passed; authenticated production chat is a separate verification step.

## Contentsquare verification limitation

Tag `c64e9d54ef9d4` is consent-gated to public homepage, About and Pricing routes. Private paths, URLs with query/hash data, global privacy control, and unconsented visits are excluded. Forms, account navigation and support chat are masked/excluded. The official wizard was run twice: neither observed a tag load or pageview, and neither reported a CSP violation. Per the supplied installation skill, further changes are paused for user guidance. Collection is **not verified**; do not describe this integration as complete.

## Performance and compatibility

The served main JS falls from 1,319,404 bytes (277,812 gzip) to approximately 775,729 bytes (207,606 gzip), a 25.3% gzip reduction. CSS is bundled/minified to avoid a second local render-blocking stylesheet request. Plotly, native React Native Web and support UI load only when needed; offscreen charts and videos defer work. Navigation initializes at DOMContentLoaded instead of waiting for external images. This is measured asset improvement, not a claimed Lighthouse/Core Web Vitals score. Field performance must be measured after deployment.

The retired binary scoring field remains read-only in legacy v1 responses where necessary, with no new calculation or visible product metric. Historical database records are preserved. Current event reporting uses calibration and log score; time-series metrics are unchanged.

## GitHub code search

The repository is public, not archived, not a fork, and uses `main`. The code-search API currently returns zero results with `incomplete_results:true`. GitHub controls indexing and provides no supported force-index endpoint. Search eligibility and a current default branch can be checked; successful indexing must not be claimed until GitHub returns results. GitHub Support may need to investigate.
