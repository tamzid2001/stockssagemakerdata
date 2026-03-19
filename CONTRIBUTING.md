# Contributing to Quantura

Thanks for contributing.

This repository mixes a live website, serverless APIs, scheduled automation, and mobile workspaces, so small changes can have broad effects. Please read this file before opening a pull request.

## Before you start

- read [README.md](README.md) for repo layout
- follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- read [SECURITY.md](SECURITY.md) before reporting sensitive bugs
- search existing issues and pull requests before opening a new one

## Development setup

### Web app

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata/quantura_site
npm install
```

### Python tooling

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata
python3 -m pip install -r requirements.txt
```

## Contribution rules

- keep changes scoped to the problem you are solving
- do not commit secrets, API keys, service-account files, or production mobile config files
- update docs when behavior, routes, or operator workflows change
- preserve existing product language and routing unless the change intentionally updates them
- if you edit `quantura_site/pages/`, sync SSR templates before you finish

## Common validation commands

Use the checks that match your change set.

```bash
node --check quantura_site/public/app.js
node --check quantura_site/functions_ssr/index.js
pytest -q quantura_site/tests
node quantura_site/functions_ssr/scripts/sync-templates.js
python daily_prophet_signal_tracker.py --help
```

If you changed GitHub Actions, validate the YAML carefully and explain any new required secrets or schedules in your PR.

## Pull request expectations

Each PR should include:

- a short problem statement
- the user-facing or operator-facing change
- test or validation notes
- screenshots for UI changes when they materially help review
- deploy notes if production rollout or secret changes are required

## Branches and commits

- use focused branches
- keep commit messages explicit
- avoid mixing unrelated work in one PR

## Issue guidance

Use the GitHub issue templates when possible:

- bug report for breakages, regressions, or incidents
- feature request for new capabilities, workflow changes, or product ideas

Security issues should not be filed publicly. Use [SECURITY.md](SECURITY.md) instead.

## Deployments

Maintainers handle production deploys. The standard deploy path is:

```bash
cd /Users/tamzidullah/Desktop/stockssagemakerdata
./deploy.sh
```

If your PR requires deployment, say so clearly and note whether it affects hosting, SSR, Node functions, Python functions, rules, schedulers, or GitHub Actions.
