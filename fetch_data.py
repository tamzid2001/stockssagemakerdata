#!/usr/bin/env python3
"""Download close prices (daily or hourly) using yfinance.

Usage examples:
  python fetch_data.py -t AAPL,MSFT -s 2023-01-01 -e 2023-12-31 -i 1d
  python fetch_data.py -t AAPL -s 2024-01-01 -e 2024-01-10 -i 1h
"""
from __future__ import annotations
import argparse
import os
from typing import List

import pandas as pd
import yfinance as yf


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download close prices via yfinance")
    p.add_argument("--tickers", "-t", required=True, help="Comma-separated tickers, e.g. AAPL,MSFT")
    p.add_argument("--start", "-s", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", "-e", required=True, help="End date YYYY-MM-DD")
    p.add_argument("--interval", "-i", choices=["1d", "1h"], default="1d", help="Data interval: 1d or 1h")
    p.add_argument("--outdir", "-o", default="data", help="Output directory for CSV files")
    return p.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _normalize_tickers(t: str) -> List[str]:
    return [x.strip() for x in t.split(",") if x.strip()]


def download_close(ticker: str, start: str, end: str, interval: str) -> pd.Series | None:
    df = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
    if df is None or df.empty:
        print(f"No data for {ticker} in range {start}..{end} with interval {interval}")
        return None
    # df may be a DataFrame with columns including 'Close'
    if isinstance(df.columns, pd.MultiIndex):
        # When multiple tickers passed to yf.download, caller ensures single-ticker calls here.
        if (ticker, "Close") in df.columns:
            series = df[(ticker, "Close")]
        else:
            # try top-level 'Close'
            series = df.xs("Close", axis=1, level=1, drop_level=False)
            series = series.iloc[:, 0]
    else:
        series = df["Close"]
    series.index = pd.to_datetime(series.index)
    series.name = "Close"
    return series


def main() -> None:
    args = parse_args()
    tickers = _normalize_tickers(args.tickers)
    ensure_dir(args.outdir)
    for t in tickers:
        print(f"Downloading {t}: {args.start} -> {args.end} ({args.interval})")
        s = download_close(t, args.start, args.end, args.interval)
        if s is None:
            continue
        safe_t = t.replace("/", "_")
        out_name = f"{safe_t}_{args.interval}_{args.start}_{args.end}.csv"
        out_path = os.path.join(args.outdir, out_name)
        s.to_frame().to_csv(out_path, index_label="Datetime")
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
