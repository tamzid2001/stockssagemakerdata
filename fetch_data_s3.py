#!/usr/bin/env python3
"""Download history prices using yfinance and upload to AWS S3.

CSV format produced:
- columns: Item_Id, Date, Price

This mirrors fetch_data.py but uploads CSV files directly to S3 instead of
saving locally.
"""
from __future__ import annotations
import argparse
import os
from io import StringIO
from typing import List, Optional
from datetime import datetime

import pandas as pd
import yfinance as yf
import boto3
from botocore.exceptions import ClientError

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


def fetch_and_upload_to_s3(
    ticker: str,
    start: str,
    end: str,
    bucket_name: str,
    s3_prefix: str = "",
    interval: str = "1d",
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: str = "us-east-1",
) -> str:
    """Fetch stock data and upload CSV to S3.
    
    Args:
        ticker: Stock ticker symbol
        start: Start date YYYY-MM-DD
        end: End date YYYY-MM-DD
        bucket_name: S3 bucket name
        s3_prefix: S3 key prefix (e.g., 'stock_data/')
        interval: Data interval ('1d' or '1h')
        aws_access_key_id: AWS access key (uses AWS_ACCESS_KEY_ID env var if None)
        aws_secret_access_key: AWS secret key (uses AWS_SECRET_ACCESS_KEY env var if None)
        region_name: AWS region
        
    Returns:
        S3 object key where the file was uploaded
    """
    # Use environment variables for AWS credentials if not provided
    if aws_access_key_id is None:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    if aws_secret_access_key is None:
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise RuntimeError(
            "AWS credentials not provided. Set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY environment variables or pass them as arguments."
        )
    
    # Download data from yfinance
    df = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
    if df is None or df.empty:
        raise RuntimeError("No data returned; check ticker/dates/interval.")
    
    # Prepare dataframe
    outdf = df[["Close"]].rename(columns={"Close": "Price"}).copy()
    outdf.reset_index(inplace=True)
    first_col_name = outdf.columns[0]
    if isinstance(first_col_name, tuple):
        first_col_name = first_col_name[0]
    if str(first_col_name).lower() != "date":
        outdf.rename(columns={outdf.columns[0]: "Date"}, inplace=True)
    outdf.insert(0, "Item_Id", ticker.lower())
    
    # Generate CSV content
    buf = StringIO()
    outdf.to_csv(buf, index=False)
    buf.seek(0)
    csv_clean = _force_remove_second_line(buf.getvalue())
    
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    
    # Generate S3 key
    file_name = f"{ticker.upper()}_{start}_{end}.csv"
    s3_key = os.path.join(s3_prefix, file_name).replace("\\", "/")
    
    # Upload to S3
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_clean.encode("utf-8"),
            ContentType="text/csv",
        )
        print(f"Successfully uploaded s3://{bucket_name}/{s3_key}")
        return s3_key
    except ClientError as e:
        raise RuntimeError(f"Failed to upload to S3: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download close prices via yfinance and upload to S3")
    p.add_argument("--tickers", "-t", required=True, help="Comma-separated tickers, e.g. AAPL,MSFT")
    p.add_argument("--start", "-s", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", "-e", required=True, help="End date YYYY-MM-DD")
    p.add_argument("--interval", "-i", choices=["1d", "1h"], default="1d", help="Data interval: 1d or 1h")
    p.add_argument("--bucket", "-b", required=True, help="S3 bucket name")
    p.add_argument("--prefix", "-p", default="", help="S3 key prefix (e.g., 'stock_data/')")
    p.add_argument("--region", "-r", default="us-east-2", help="AWS region (default: us-east-2)")
    p.add_argument("--access-key", default=None, help="AWS access key ID (uses AWS_ACCESS_KEY_ID env var if not provided)")
    p.add_argument("--secret-key", default=None, help="AWS secret access key (uses AWS_SECRET_ACCESS_KEY env var if not provided)")
    return p.parse_args()
    return p.parse_args()


def _normalize_tickers(t: str) -> List[str]:
    return [x.strip() for x in t.split(",") if x.strip()]


def main() -> None:
    args = parse_args()
    tickers = _normalize_tickers(args.tickers)
    
    for ticker in tickers:
        print(f"Downloading {ticker}: {args.start} -> {args.end} ({args.interval})")
        try:
            s3_key = fetch_and_upload_to_s3(
                ticker=ticker,
                start=args.start,
                end=args.end,
                bucket_name=args.bucket,
                s3_prefix=args.prefix,
                interval=args.interval,
                aws_access_key_id=args.access_key,
                aws_secret_access_key=args.secret_key,
                region_name=args.region,
            )
            print(f"Success: {ticker}")
        except Exception as e:
            print(f"Failed to download and upload {ticker}: {e}")


if __name__ == "__main__":
    main()
