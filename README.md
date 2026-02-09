# stockssagemakerdata

Small helper to download stock close prices (daily or hourly) using `yfinance` and upload to AWS S3. Also includes utilities for weekday prediction analysis.

## Features

- Download historical stock data using `yfinance`
- Upload data to AWS S3 (S3 bucket: `stockscompute` in `us-east-2`)
- Generate price predictions using technical indicators (SMA, EMA)
- Upload predictions to S3 in separate folder
- Check if a given date or prediction window lands on a weekday
- Support for both daily (1d) and hourly (1h) intervals

## Installation

Install dependencies (recommended inside a virtualenv or Codespace):

```bash
pip install -r requirements.txt
```

## Usage

### Basic: Download stocks locally

```bash
python fetch_data.py -t AAPL,MSFT -s 2024-01-01 -e 2024-01-08 -i 1d -o data
```

Or using Make:
```bash
make sample  # Quick example with AAPL
```

### AWS S3: Upload to S3 bucket

First, configure AWS credentials from your GitHub Codespaces secrets:

```bash
make setup-aws
```

Then fetch and upload to the stockscompute bucket:

```bash
make fetch-s3-bucket TICKERS="AAPL" START=2024-01-01 END=2024-01-08 INTERVAL=1d PREFIX=stock_data/
```

Or use the Python script directly:
```bash
python fetch_data_s3.py -t AAPL,MSFT -s 2024-01-01 -e 2024-01-08 -i 1d -b stockscompute -p stock_data/ -r us-east-2
```

The script will automatically use `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

### Check Weekday Prediction

Check if a prediction date lands on a weekday:

```bash
# Check a specific date with N days offset
python check_weekday.py 2024-01-08 30

# Check today
python check_weekday.py today

# Check yesterday
python check_weekday.py yesterday
```

Or using Make:
```bash
make check-weekday START_DATE=2024-01-08 DAYS=30
make check-weekday-today
make check-weekday-yesterday
```

### Generate Price Predictions

Create a predictions CSV file locally and upload it to S3:

```bash
# Create your own predictions.csv file, then upload it
python predictions.py -f predictions.csv -b stockscompute -p predictions/ -r us-east-2
```

Or using Make:
```bash
# Create sample predictions and upload
make predict-sample

# Upload your own CSV file
make predict FILE=your_predictions.csv PREFIX=predictions/
```

**Expected CSV Format** (example):
```csv
Ticker,Date,Prediction,Confidence
AAPL,2024-02-06,180.50,0.95
MSFT,2024-02-07,427.00,0.91
```

The predictions CSV is uploaded to `stockscompute/predictions/` with a timestamp appended to the filename.

### Combined Stock Screener + Slack Delivery

Generate a formatted CSV with headlines and optional Slack delivery:

```bash
python combined_stock_screener.py
```

Optional environment variables:
- `SLACK_WEBHOOK_URL` for posting a summary message.
- `SLACK_BOT_TOKEN` + `SLACK_CHANNEL` for uploading the CSV file to Slack.
- `SCREENING_OUTPUT_FILE` to override the CSV output filename.

### Quantura Marketing Site

Static site assets live in `quantura_site/`. Open `quantura_site/index.html` in a browser
and replace the Stripe placeholder buttons with live checkout URLs before deploying.

## Files

- [fetch_data.py](fetch_data.py) — CLI to download `Close` prices locally (CSV output).
- [fetch_data_s3.py](fetch_data_s3.py) — CLI to download `Close` prices and upload to AWS S3.
- [check_weekday.py](check_weekday.py) — Utility to check if a date is a weekday.
- [predictions.py](predictions.py) — Generate stock price predictions and upload to S3.
- [combined_stock_screener.py](combined_stock_screener.py) — Combined screener that outputs formatted CSV with headlines and can send results to Slack.
- [quantura_site/](quantura_site/) — Static marketing site for Quantura with Stripe-ready CTA placeholders.
- [Makefile](Makefile) — Convenient targets for common tasks.
- [requirements.txt](requirements.txt) — Python dependencies.
- [setup_aws.sh](setup_aws.sh) — Script to configure AWS CLI from environment variables.
- [.env.example](.env.example) — Template for AWS configuration.
- [.devcontainer/devcontainer.json](.devcontainer/devcontainer.json) and [.devcontainer/Dockerfile](.devcontainer/Dockerfile) — Codespace/devcontainer setup for development.
- [.github/workflows/mlops-data.yml](.github/workflows/mlops-data.yml) — Scheduled/manual workflow to fetch data and store artifacts.
- [.github/workflows/weekly-stock-screening.yml](.github/workflows/weekly-stock-screening.yml) — Weekly combined screening job with optional Slack delivery.
- [.github/workflows/linear-sync.yml](.github/workflows/linear-sync.yml) — GitHub → Linear issue sync workflow.

## Makefile Targets

```bash
make install                  # Install Python dependencies
make fetch                    # Quick fetch example (AAPL local)
make fetch-ticker             # Parameterized local fetch
make sample                   # Sample fetch to local data/
make fetch-s3                 # Quick fetch to S3
make fetch-s3-ticker          # Parameterized S3 fetch
make sample-s3                # Sample fetch to S3
make setup-aws                # Configure AWS credentials
make fetch-s3-bucket          # Fetch to stockscompute bucket
make predict                  # Upload predictions CSV to S3
make predict-sample           # Create sample CSV and upload
make check-weekday            # Check if prediction date is weekday
make check-weekday-today      # Check if today is weekday
make check-weekday-yesterday  # Check if yesterday is weekday
make git-push                 # Push changes to main branch
```

## AWS Integration

### Setup

1. Add `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to GitHub Codespaces secrets
2. Run `make setup-aws` to configure the AWS CLI

### Bucket Information

- **Bucket Name**: `stockscompute`
- **AWS Region**: `us-east-2`
- **ARN**: `arn:aws:s3:::stockscompute`

### Environment Variables

The S3 upload script reads credentials from:
- `AWS_ACCESS_KEY_ID` — AWS access key ID
- `AWS_SECRET_ACCESS_KEY` — AWS secret access key

These can be set as environment variables or passed as command-line arguments.

## Codespace / Devcontainer

Open this repo in GitHub Codespaces or VS Code Remote — the devcontainer will install dependencies automatically via `postCreateCommand`.

## CI / MLOps

The workflow `mlops-data` runs daily (06:00 UTC) and can be triggered manually. It checks out the repo, installs dependencies, runs a sample fetch, and uploads results as an artifact.

## Next Steps

- Customize tickers and date ranges in the Makefile targets or workflow files
- Set up AWS credentials in GitHub Codespaces secrets for S3 uploads
- Integrate into your own data pipelines or SageMaker workflows
