# Quantura Web App

This directory contains the live Quantura website and dashboard application.

Production URLs:

- [https://quantura.studio](https://quantura.studio)
- [https://quantura-e2e3d.web.app](https://quantura-e2e3d.web.app)

## Directory map

```text
quantura_site/
├── pages/                 # Source HTML pages
├── public/                # Public assets, app.js, styles.css, service workers, static pages
├── functions_explore/     # Gen2 Node API for app features and AWS integrations
├── functions_ssr/         # Gen2 Node SSR renderer and template sync script
├── functions_newsletter/  # Python newsletter handlers and schedulers
├── firestore.rules
├── firestore.indexes.json
├── storage.rules
└── firebase.json
```

## Main product areas

- public home, pricing, blog, research, and legal pages
- dashboard, account, notifications, productivity, collaboration, and admin
- prediction-CSV Forecast Foundry and separate historical-data workflows
- private collaboration, saved analyses, and assigned workspace tasks
- market headlines, screening, charting, and research surfaces
- GitHub Actions social publishing sourced from site content and logged in Firestore

## Source of truth

- Edit page markup in `pages/`
- Edit client behavior in `public/app.js` and related assets
- Edit styling in `public/styles.css` and page-specific CSS files
- Edit API behavior in `functions_explore/src/`
- Edit SSR behavior in `functions_ssr/index.js`

Do not edit the synced SSR templates directly. They are generated from `pages/`.

## Local setup

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site
npm install
```

Optional checks:

```bash
node --check public/app.js
node --check functions_ssr/index.js
pytest -q tests
node functions_ssr/scripts/sync-templates.js
git diff -- functions_ssr/templates
```

## Deploy

The preferred deploy path is the root script:

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata
./deploy.sh
```

That script handles template sync, function deploys, scheduler setup, and hosting release.

Scoped deploys can still be useful during debugging:

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site
firebase deploy --only hosting
firebase deploy --only functions:ssr
```

## Notes for contributors

- If you change `pages/`, make sure SSR templates stay in sync.
- If you change dashboard or collaboration behavior, test both logged-out and logged-in states.
- If you touch app-side copy or workflow behavior, update the relevant docs and screenshots when appropriate.
- If you change social publishing behavior or channel constraints, also update `quantura_site/docs/social_media_setup.md` and `scripts/lib/social/config/channels/`.
- Never commit secrets, service-account JSON files, or production mobile config files.

Repository-wide contribution, conduct, and security guidance lives in:

- [CONTRIBUTING.md](../CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md)
- [SECURITY.md](../SECURITY.md)
