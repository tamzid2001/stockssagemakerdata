# Quantura production deployment

## Authoritative runtime

`quantura.studio` is served by Vercel. The public web application, Express API,
SSR service, legacy API compatibility service, and newsletter service are linked
as separate Vercel projects under `quantura_site/`.

Google Cloud Functions are not the authoritative request-serving backend. Some
event-driven Firestore, Pub/Sub, and scheduled jobs still exist in Google Cloud
while they are inventoried and migrated. Do not delete those jobs until their
consumers, traffic, scheduler configuration, and replacement have been verified.

## Production deploy

Authenticate the Vercel CLI and ensure each service has its local
`.vercel/project.json` link, then run:

```bash
./deploy.sh
```

The default deployment order is:

1. `quantura-api`
2. `quantura-legacy-api`
3. `quantura-newsletter`
4. `quantura-ssr`
5. `quantura`

Use a dry run to validate project links and commands without publishing:

```bash
DEPLOY_DRY_RUN=true ./deploy.sh
```

The deployment accepts the standard `VERCEL_TOKEN` environment variable in
non-interactive environments. Runtime secrets belong in the corresponding
Vercel project environment and must never be committed.

## Archived Google workflow

The old Google Cloud/Firebase workflow is retained temporarily for explicit
event-job maintenance only and is never the default:

```bash
DEPLOY_PROVIDER=google-legacy ./deploy.sh
```

This opt-in path is operationally separate from the Vercel production deploy.
Four obsolete Firestore entrypoints that are no longer exported by the current
API source are intentionally not redeployed. Existing Google functions must be
retired through an audited migration rather than bulk-deleted.

## Aikido Zen Firewall

The Vercel Express entrypoint loads `@aikidosec/firewall` before Express. The
archived Google HTTP functions use the documented Node preload through
`NODE_OPTIONS`. Configure these server-only environment variables:

```text
AIKIDO_TOKEN
AIKIDO_BLOCK=false
```

Start in detection-only mode and enable blocking only after reviewing findings.
Never expose the runtime token through a public/browser environment variable.

## Local validation

```bash
cd quantura_site
npm test
npm run build

cd functions_explore
npm run build
NODE_OPTIONS='-r @aikidosec/firewall/instrument' npm start
```

## Live verification

```bash
curl -sS -D - https://quantura.studio/api/health
curl -sS https://quantura.studio/api/shop/catalog
```

The public response should be served by Vercel. Verify the Vercel production
deployment for each linked project and inspect runtime logs after release.
