#!/usr/bin/env python3
"""Download history prices using yfinance and write clean CSVs.

CSV format produced:
- columns: Item_Id, Date, Price

This mirrors the Colab helper used elsewhere and removes yfinance's
second-line metadata when present.
"""
from __future__ import annotations
import argparse
import os
from io import StringIO
from typing import List, Optional
from datetime import datetime

import pandas as pd
import yfinance as yf

try:
    # Optional, used only when running inside Google Colab
    from google.colab import files as colab_files  # type: ignore
except Exception:
    colab_files = None


def _force_remove_second_line(csv_text: str) -> str:
    lines = csv_text.splitlines(True)
    if len(lines) > 1:
        del lines[1]
    return "".join(lines)


def colab_download_price_csv(ticker: str, start: str, end: str, interval: str = "1d", out_path: Optional[str] = None) -> str:
    df = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
    if df is None or df.empty:
        raise RuntimeError("No data returned; check ticker/dates/interval.")
    outdf = df[["Close"]].rename(columns={"Close": "Price"}).copy()
    outdf.reset_index(inplace=True)
    # Ensure the date column is named 'Date' for clarity
    first_col_name = outdf.columns[0]
    if isinstance(first_col_name, tuple):
        first_col_name = first_col_name[0]
    if str(first_col_name).lower() != "date":
        outdf.rename(columns={outdf.columns[0]: "Date"}, inplace=True)
    outdf.insert(0, "Item_Id", ticker.lower())
    buf = StringIO()
    outdf.to_csv(buf, index=False)
    buf.seek(0)
    csv_clean = _force_remove_second_line(buf.getvalue())
    if not out_path:
        out_path = f"{ticker.upper()}_{start}_{end}.csv"
    ensure_dir(os.path.dirname(out_path) or "./")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(csv_clean)
    # If running in Colab, trigger a download in the browser
    if colab_files is not None:
        try:
            colab_files.download(out_path)
        except Exception:
            pass
    return out_path


def ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _normalize_tickers(t: str) -> List[str]:
    return [x.strip() for x in t.split(",") if x.strip()]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download close prices via yfinance (clean CSV output)")
    p.add_argument("--tickers", "-t", required=True, help="Comma-separated tickers, e.g. AAPL,MSFT")
    p.add_argument("--start", "-s", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", "-e", required=True, help="End date YYYY-MM-DD")
    p.add_argument("--interval", "-i", choices=["1d", "1h"], default="1d", help="Data interval: 1d or 1h")
    p.add_argument("--outdir", "-o", default="data", help="Output directory for CSV files")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    tickers = _normalize_tickers(args.tickers)
    ensure_dir(args.outdir)
    for t in tickers:
        print(f"Downloading {t}: {args.start} -> {args.end} ({args.interval})")
        try:
            out_name = f"{t.upper()}_{args.start}_{args.end}.csv"
            out_path = os.path.join(args.outdir, out_name)
            written = colab_download_price_csv(t, args.start, args.end, interval=args.interval, out_path=out_path)
            print(f"Wrote {written}")
        except Exception as e:
            print(f"Failed to download {t}: {e}")


if __name__ == "__main__":
    main()
