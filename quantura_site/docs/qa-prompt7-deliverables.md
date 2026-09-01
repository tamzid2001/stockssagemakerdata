# Prompt 7 Deliverables QA (Backend + UI)

Date: 2026-03-03 (America/New_York)
Environment:
- Backend deployed with `gcloud functions deploy` (Gen2)
- Frontend deployed with `firebase deploy --only hosting`
- Deployed runtime validation: `https://quantura.studio`
- Local template preview for page-level HTML updates: `python3 -m http.server` against merged `public + pages`

## 1) Native ads only on iOS/Android
Status: PASS (web hidden), native verification ready for device QA

Checks:
- Web runtime does not render native ad slots (`AdSlot.web.tsx` returns null and web bridge gating is enforced).
- Feed pages render without native ad card injections in desktop browser.

Evidence:
- Web visual QA evidence was reviewed during the implementation run and is intentionally stored as short-lived CI output rather than committed binary files.

Manual native check to run on devices:
- iOS shell: confirm native cards injected into feed sections.
- Android shell: confirm native cards injected into feed sections.
- Premium account: no ad slots.

## 2) Events (Calendly) responsive embed
Status: PASS

Checks:
- Events route renders Calendly inline widget.
- Widget script loads and iframe appears.

Evidence:
- Events/Calendly visual QA completed; generated evidence is not retained in Git.

## 3) CRUD My Requests + share/unpublish
Status: PASS (backend + UI)

Checks:
- Backend routes present: list/create/read/update/share/unpublish/duplicate/delete.
- Firestore rules include owner/public/unlisted access for requests + request shares.
- UI panels added under Forecasting/Indicators/Model Council/Screener contexts.

Evidence (UI + code path):
- Forecasting/Screener layouts with My Requests cards were visually reviewed; generated evidence is not retained in Git.
- Backend handlers: `functions_explore/src/index.ts` (`/my-requests*` routes)

## 4) Polymarket cards formatting (Gamma-only)
Status: PASS

Checks:
- Gamma endpoints used: `/public-search`, `/events`.
- Safe parsing for JSON-encoded `outcomes` and `outcomePrices`.
- Polymarket-style cards with grouped events/outcome pills.
- New endpoint added: `/api/polymarket/price`.

Evidence:
- Predictions panel was reviewed in the deployed runtime; generated evidence is not retained in Git.
- Rewrites + endpoints in code:
  - `firebase.json`
  - `functions_explore/src/index.ts`

## 5) Currency conversion works
Status: PASS

Checks:
- `/fx` redirect to `/tools/fx`.
- FX tool route renders and loads conversion UI.

Evidence:
- Currency conversion visual QA completed; generated evidence is not retained in Git.

## 6) Forecast fan chart + client-only storage
Status: PASS (code + runtime behavior)

Checks:
- Client cache logic via IndexedDB for forecast chart series.
- Forecast request metadata stored server-side; full chart series flagged as client-side.
- Forecast AI summary actions (Like/Dislike/Share) with disclaimer caption.

Evidence:
- Forecast terminal view reviewed; generated evidence is not retained in Git.
- Client cache and summary code: `public/app.js`
- Metadata-only storage updates: `functions/main.py`

## 7) Deployment wiring
Status: PASS

Checks:
- `deploy.sh` runs both commands sequentially:
  1. `gcloud functions deploy ...`
  2. `firebase deploy --only hosting`
- Runtime pinned to latest configured Node.js runtime (`nodejs24`) for Gen2 function deploys.

Evidence:
- `deploy.sh`
- `DEPLOY.md`

## 8) Marketing copy + use cases + quote carousel
Status: PASS

Checks:
- Homepage/Pricing/Shop copy updated with institutional messaging.
- Use-case cards added with required external links.
- Quote carousel script added and wired.

Evidence:
- Homepage marketing sections reviewed; generated evidence is not retained in Git.
- Pricing copy/cards/tables reviewed; generated evidence is not retained in Git.
- Carousel script: `public/marketing-carousel.js`
