.PHONY: install fetch

install:
	pip install -r requirements.txt

fetch:
	python fetch_data.py -t AAPL,MSFT -s 2024-01-01 -e 2024-01-08 -i 1d -o data
