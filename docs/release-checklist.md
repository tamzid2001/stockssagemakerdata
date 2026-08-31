# Quantura Release Checklist

## Web and API Readiness

### Hosting and app shell
- [ ] Public assets built from `quantura_site/public/`
- [ ] Source page changes synced into `quantura_site/functions_ssr/templates/`
- [ ] Vercel project links, production aliases, and route configuration reviewed
- [ ] Core logged-out routes render correctly
- [ ] Core logged-in dashboard routes render correctly

### Notifications and messaging
- [ ] `firebase-messaging-sw.js` deployed at the site root
- [ ] `FCM_WEB_VAPID_KEY` configured in the applicable encrypted runtime environment
- [ ] Notification registration and send-test endpoints deployed
- [ ] Foreground notification test passes
- [ ] Background notification test passes
- [ ] Notification click deep link opens the correct route

### Forecasting and data workflows
- [ ] Forecast Foundry create/save/reopen flow verified
- [ ] Historical-data download flow verified
- [ ] Explore publication flow verified
- [ ] Sports forecasting or other newly added forecast workflows verified if part of the release
- [ ] Any new export/download payloads verified against real sample data

---

## Remote Config and feature flags

- [ ] Required feature flags exist in Remote Config
- [ ] Default values are mirrored between SSR and hydrated client behavior where applicable
- [ ] Rollout-sensitive flags are documented before shipping
- [ ] Ad and monetization flags point to the intended environment values

---

## Billing and access control

- [ ] Stripe checkout or billing portal changes validated if touched
- [ ] Premium gating and entitlement checks verified
- [ ] Creator/support/subscription flows verified if touched
- [ ] Anonymous, authenticated, and admin-only access boundaries validated for changed surfaces

---

## Privacy and security

- [ ] Privacy policy and legal links remain current
- [ ] No API keys, tokens, `.env` files, or service-account credentials are present in git
- [ ] Local credential files remain ignored by `.gitignore`
- [ ] Vercel environment variables and any remaining Google event-job secrets reviewed

---

## Build and deployment

- [ ] `node --check` or equivalent syntax checks pass for touched browser and SSR files
- [ ] `npm run build` passes for touched JavaScript or TypeScript packages
- [ ] Targeted tests pass for the changed area
- [ ] `./deploy.sh` is used for production deploys
- [ ] Live smoke checks pass after deploy

---

## What you must supply

| Item | Status | Notes |
|------|--------|-------|
| Managed secret values | Required | Provisioned in Vercel or the applicable event-job secret store |
| Ad or monetization config | Conditional | Required only when the release touches those surfaces |
| Stripe secrets | Conditional | Required only for billing-related releases |
| Provider API keys | Conditional | Required only for changed data or model integrations |

---

## Quick test commands

```bash
# Web syntax checks
node --check quantura_site/public/app.js
node --check quantura_site/functions_ssr/index.js

# Optional targeted tests
pytest -q quantura_site/tests

# Deploy
./deploy.sh
```
