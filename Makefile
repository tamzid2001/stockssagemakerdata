.PHONY: install fetch fetch-ticker sample git-push

install:
	pip install -r requirements.txt

# Example: make fetch
fetch:
	python fetch_data.py -t AAPL,MSFT -s 2024-01-01 -e 2024-01-08 -i 1d -o data

# Parameterized fetch: set TICKERS, START, END, INTERVAL, OUTDIR
# Usage: make fetch-ticker TICKERS="AAPL" START=2024-01-01 END=2024-01-02 INTERVAL=1d OUTDIR=data
fetch-ticker:
	python fetch_data.py -t "$(TICKERS)" -s "$(START)" -e "$(END)" -i "$(INTERVAL)" -o "$(OUTDIR)"

# Run a sample fetch (convenience)
sample:
	$(MAKE) fetch-ticker TICKERS="AAPL" START=2024-01-01 END=2024-01-08 INTERVAL=1d OUTDIR=data

# Commit & push changes to origin/main
# Usage: make git-push MSG="Update Makefile"
git-push:
	git add -A
	git commit -m "$(MSG)" || true
	git push origin main
