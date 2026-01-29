# stockssagemakerdata

Small helper to download stock close prices (daily or hourly) using `yfinance`.

Usage
- Install dependencies (recommended inside a virtualenv or Codespace):

  pip install -r requirements.txt

- Download data (example):

  python fetch_data.py -t AAPL,MSFT -s 2024-01-01 -e 2024-01-08 -i 1d -o data

Files added
- [fetch_data.py](fetch_data.py) — CLI to download `Close` prices (CSV output).
- [requirements.txt](requirements.txt) — Python deps.
- [.devcontainer/devcontainer.json](.devcontainer/devcontainer.json) and [.devcontainer/Dockerfile](.devcontainer/Dockerfile) — Codespace/devcontainer setup for MLOps development.
- [.github/workflows/mlops-data.yml](.github/workflows/mlops-data.yml) — Scheduled/manual workflow to fetch data and store artifacts.

Codespace / Devcontainer
- Open this repo in GitHub Codespaces or VS Code Remote — the devcontainer will install deps automatically via `postCreateCommand`.

CI / MLOps
- The workflow `mlops-data` runs daily (06:00 UTC) and can be triggered manually. It checks out the repo, installs dependencies, runs a sample fetch, and uploads results as an artifact.

Next steps
- Change sample tickers and date range in [.github/workflows/mlops-data.yml](.github/workflows/mlops-data.yml) or call the script from your own pipelines.
# stockssagemakerdata
Use this repo to turn stocks compute to data with sagemaker
