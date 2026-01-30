.PHONY: install fetch fetch-ticker sample git-push check-weekday setup-aws fetch-s3-bucket predict predict-sample

install:
	pip install -r requirements.txt

# Example: make fetch
fetch:
	python fetch_data.py -t AAPL -s 2024-01-01 -e 2024-01-08 -i 1d -o data

# Parameterized fetch: set TICKERS, START, END, INTERVAL, OUTDIR
# Usage: make fetch-ticker TICKERS="AAPL" START=2024-01-01 END=2024-01-02 INTERVAL=1d OUTDIR=data
fetch-ticker:
	python fetch_data.py -t "$(TICKERS)" -s "$(START)" -e "$(END)" -i "$(INTERVAL)" -o "$(OUTDIR)"

# Run a sample fetch (convenience)
sample:
	$(MAKE) fetch-ticker TICKERS="AAPL" START=2024-01-01 END=2024-01-08 INTERVAL=1d OUTDIR=data

# Example: make fetch-s3
fetch-s3:
	python fetch_data_s3.py -t AAPL -s 2024-01-01 -e 2024-01-08 -i 1d -b my-bucket -p stock_data/

# Parameterized fetch to S3: set TICKERS, START, END, INTERVAL, BUCKET, PREFIX, REGION
# Usage: make fetch-s3-ticker TICKERS="AAPL" START=2024-01-01 END=2024-01-02 INTERVAL=1d BUCKET=my-bucket PREFIX=stock_data/ REGION=us-east-1
fetch-s3-ticker:
	python fetch_data_s3.py -t "$(TICKERS)" -s "$(START)" -e "$(END)" -i "$(INTERVAL)" -b "$(BUCKET)" -p "$(PREFIX)" -r "$(REGION)"

# Run a sample S3 fetch (convenience)
sample-s3:
	$(MAKE) fetch-s3-ticker TICKERS="AAPL" START=2024-01-01 END=2024-01-08 INTERVAL=1d BUCKET=my-bucket PREFIX=stock_data/ REGION=us-east-1

# Check if prediction date lands on weekday
# Usage: make check-weekday START_DATE=2026-01-08 DAYS=30
# Or: make check-weekday-today or make check-weekday-yesterday
check-weekday:
	python check_weekday.py "$(START_DATE)" "$(DAYS)"

check-weekday-today:
	python check_weekday.py today

check-weekday-yesterday:
	python check_weekday.py yesterday

# Setup AWS credentials from environment variables
setup-aws:
	bash setup_aws.sh

# Fetch to stockscompute bucket in us-east-2
# Usage: make fetch-s3-bucket TICKERS="AAPL" START=2024-01-01 END=2024-01-08 INTERVAL=1d PREFIX=stock_data/
fetch-s3-bucket:
	python fetch_data_s3.py -t "$(TICKERS)" -s "$(START)" -e "$(END)" -i "$(INTERVAL)" -b stockscompute -p "$(PREFIX)" -r us-east-2

# Generate price predictions and upload to S3
# Usage: make predict TICKERS="AAPL,MSFT" START=2024-01-01 END=2024-01-31 DAYS=5 PREFIX=predictions/
predict:
	python predictions.py -t "$(TICKERS)" -s "$(START)" -e "$(END)" -b stockscompute -p "$(PREFIX)" -d "$(DAYS)" -r us-east-2

# Generate predictions for AAPL (sample)
# Usage: make predict-sample
predict-sample:
	$(MAKE) predict TICKERS="AAPL" START=2024-01-01 END=2024-01-31 DAYS=5 PREFIX=predictions/

# Commit & push changes to origin/main
git-push:
	git add -A
	git commit -m "$(MSG)" || true
	git push origin main
