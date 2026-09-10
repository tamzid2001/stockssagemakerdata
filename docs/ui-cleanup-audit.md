# UI cleanup audit — 2026-09-08

Baseline: `f831c77c6407b80306e71075003d1fc93df383a9`, current production `main`.

## Architecture and constraints

- Server-rendered HTML templates in `quantura_site/pages`, synchronized to the Express/Firebase Admin SSR service. No Next.js or server-component migration is appropriate here.
- Vanilla JavaScript app shell and feature modules; React 18.3.1 / React Native Web 0.21.2 only for embedded native-compatible surfaces. esbuild 0.25.12 builds those surfaces and Vercel Analytics/Speed Insights.
- CSS: legacy `styles.css` imports `professional.css` before redefining its variables. The latter has light/dark tokens, but many component declarations still hard-code dark surfaces. Screener adds a third, dark-only palette. This cascade explains mixed foreground/surface colors.
- State: module closures and DOM state, URL panel/filter state, Firebase session and workspace records. Mutable membership is checked by `apiAccess.ts` / `workspaces.ts`, not by UI labels.
- Backend: TypeScript 5.9.3 / Express 4.22.2, Firestore, scoped API keys, Vercel routes; separate Python compatibility and asynchronous forecasting workers. No authentication, billing, numerical-model or storage-schema rewrite is needed.
- Charts: Plotly 2.35.2, TradingView embeds and lightweight SVG event trajectories. Plotly is eagerly included in forecasting/ticker HTML; React Native Web is loaded on ordinary web visits even though its surface is native-only.
- Search: `market-search.js` uses the existing grouped `/api/market-search` service. Screener uses a single server-filtered, paginated implementation loaded by `screener-workspace-loader.js` for the forecasting panel.
- Feedback: shared `#toast`, notification-center components, native select controls and transformed toggle groups. Theme preference is applied late inside the large app bundle.
- Docs: `docs.json`, `docs/*.mdx`, method/API Markdown and generated OpenAPI from `functions_explore/src/openapi.ts`; Swagger at `/developers/api`.
- Deployment: independent Vercel web/API/SSR/compatibility services; GitHub app CI and Firestore emulator tests. Speed Insights is already built and injected; no claim of improved field metrics can precede post-deployment collection.
- Breakpoints currently include 440/480/600/640/720/760/768/980/1024/1080/1100/1380/1400. Grid minimum widths and card-style mobile Screener rows require testing at all requested widths, including embedded mode.

## Baseline checks

- `npm test --prefix quantura_site/functions_explore`: 171 passed, one emulator-dependent skip. Includes strict TypeScript compilation.
- Firestore emulator integration command from app CI: 2 passed, no skips.
- Python 3.12 with the test dependencies (NumPy, pandas, requests, dotenv, httpx, exchange-calendars, pandas-market-calendars): 107 passed across site, forecast intelligence and ensemble suites. Initial test-environment runs lacked httpx / pandas-market-calendars; these were environment dependency failures, not application failures.
- `npm run build --prefix quantura_site`: passed (RNWeb 242 KiB; observability 3.6 KiB).
- `npm run lint` and `npm run typecheck` in the web package: missing scripts. Backend strict TypeScript build passed. New repeatable web checks will be added.
- Browser: at 390px in light mode, Screener cards used RGB(12,25,41), while page foreground was RGB(16,32,51). Pale description text and dark cards made the mixed theme visible. Search and forecast filters consumed excessive vertical space.

## Change boundaries

Preserve all untracked local duplicate files and historical database records. Retire the legacy binary score from current public views and new calculations; retain read-only legacy fields for existing v1 consumers where necessary. Use outcome calibration for event probabilities, not price-error metrics. Reuse common theme tokens, chart loader, and market search. Do not add sample forecasts or change numerical model behavior.
