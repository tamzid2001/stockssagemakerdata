#!/usr/bin/env python3
"""Generate stock price predictions and upload to AWS S3.

Uses simple moving average (SMA) and exponential moving average (EMA) based
predictions to forecast next 5 trading days.

CSV format produced:
- columns: Ticker, Date, Close_Price, SMA_20, EMA_12, Prediction, Confidence

This script downloads historical data, calculates technical indicators,
generates predictions, and uploads results to S3.
"""
from __future__ import annotations
import argparse
import os
from io import StringIO
from typing import List, Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import yfinance as yf
import boto3
from botocore.exceptions import ClientError


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate SMA and EMA technical indicators.
    
    Args:
        df: DataFrame with 'Close' column
        
    Returns:
        DataFrame with added SMA_20 and EMA_12 columns
    """
    df_copy = df.copy()
    df_copy["SMA_20"] = df_copy["Close"].rolling(window=20).mean()
    df_copy["EMA_12"] = df_copy["Close"].ewm(span=12, adjust=False).mean()
    return df_copy


def generate_predictions(df: pd.DataFrame, ticker: str, days_ahead: int = 5) -> pd.DataFrame:
    """Generate price predictions based on technical indicators.
    
    Args:
        df: DataFrame with OHLCV data and technical indicators
        ticker: Stock ticker symbol
        days_ahead: Number of days to predict ahead (default: 5)
        
    Returns:
        DataFrame with predictions
    """
    if df.empty or len(df) < 20:
        raise ValueError(f"Insufficient data for {ticker}. Need at least 20 days.")
    
    # Use latest data for prediction
    latest_close = df["Close"].iloc[-1]
    latest_sma = df["SMA_20"].iloc[-1]
    latest_ema = df["EMA_12"].iloc[-1]
    
    # Simple prediction logic: average of recent trend indicators
    avg_indicator = (latest_sma + latest_ema) / 2
    trend = (latest_close - avg_indicator) / avg_indicator if avg_indicator != 0 else 0
    
    # Calculate daily change volatility
    recent_returns = df["Close"].pct_change().tail(20)
    volatility = recent_returns.std()
    
    predictions = []
    current_date = df.index[-1]
    
    for i in range(1, days_ahead + 1):
        # Move to next trading day
        current_date = current_date + timedelta(days=1)
        
        # Skip weekends
        while current_date.weekday() > 4:  # 5=Saturday, 6=Sunday
            current_date = current_date + timedelta(days=1)
        
        # Generate prediction with trend and noise
        prediction_change = trend + (volatility * 0.5 * (np.random.randn()))
        predicted_price = latest_close * (1 + prediction_change * (i * 0.3))
        
        # Confidence decreases further into future
        confidence = max(0.5, 1.0 - (i * 0.1))
        
        predictions.append({
            "Ticker": ticker,
            "Date": current_date.strftime("%Y-%m-%d"),
            "Close_Price": round(latest_close, 2),
            "SMA_20": round(latest_sma, 2),
            "EMA_12": round(latest_ema, 2),
            "Prediction": round(predicted_price, 2),
            "Confidence": round(confidence, 2),
        })
    
    return pd.DataFrame(predictions)


def fetch_and_predict(
    ticker: str,
    start: str,
    end: str,
    bucket_name: str,
    s3_prefix: str = "",
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: str = "us-east-2",
    days_ahead: int = 5,
) -> str:
    """Fetch stock data, generate predictions, and upload to S3.
    
    Args:
        ticker: Stock ticker symbol
        start: Start date YYYY-MM-DD
        end: End date YYYY-MM-DD
        bucket_name: S3 bucket name
        s3_prefix: S3 key prefix (e.g., 'predictions/')
        aws_access_key_id: AWS access key (uses AWS_ACCESS_KEY_ID env var if None)
        aws_secret_access_key: AWS secret key (uses AWS_SECRET_ACCESS_KEY env var if None)
        region_name: AWS region
        days_ahead: Number of days to predict ahead
        
    Returns:
        S3 object key where predictions were uploaded
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
    
    # Download historical data
    print(f"Downloading {ticker} data from {start} to {end}...")
    df = yf.download(ticker, start=start, end=end, progress=False)
    
    if df is None or df.empty:
        raise RuntimeError(f"No data returned for {ticker}. Check ticker/dates.")
    
    print(f"✓ Downloaded {len(df)} days of data")
    
    # Calculate indicators
    print("Calculating technical indicators...")
    df_with_indicators = calculate_technical_indicators(df)
    
    # Generate predictions
    print(f"Generating {days_ahead}-day predictions...")
    predictions_df = generate_predictions(df_with_indicators, ticker, days_ahead)
    
    # Prepare CSV content
    buf = StringIO()
    predictions_df.to_csv(buf, index=False)
    buf.seek(0)
    csv_content = buf.getvalue()
    
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    
    # Generate S3 key
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{ticker}_{start}_{end}_predictions_{timestamp}.csv"
    s3_key = os.path.join(s3_prefix, file_name).replace("\\", "/")
    
    # Upload to S3
    try:
        print(f"Uploading predictions to S3...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv",
        )
        print(f"✓ Successfully uploaded s3://{bucket_name}/{s3_key}")
        return s3_key
    except ClientError as e:
        raise RuntimeError(f"Failed to upload predictions to S3: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate stock price predictions and upload to S3"
    )
    p.add_argument(
        "--tickers", "-t", required=True, help="Comma-separated tickers, e.g. AAPL,MSFT"
    )
    p.add_argument("--start", "-s", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", "-e", required=True, help="End date YYYY-MM-DD")
    p.add_argument(
        "--bucket", "-b", required=True, help="S3 bucket name"
    )
    p.add_argument(
        "--prefix", "-p", default="predictions/", help="S3 key prefix (default: predictions/)"
    )
    p.add_argument(
        "--region", "-r", default="us-east-2", help="AWS region (default: us-east-2)"
    )
    p.add_argument(
        "--days", "-d", type=int, default=5, help="Days ahead to predict (default: 5)"
    )
    p.add_argument(
        "--access-key", default=None, help="AWS access key ID (uses AWS_ACCESS_KEY_ID env var if not provided)"
    )
    p.add_argument(
        "--secret-key", default=None, help="AWS secret access key (uses AWS_SECRET_ACCESS_KEY env var if not provided)"
    )
    return p.parse_args()


def _normalize_tickers(t: str) -> List[str]:
    return [x.strip() for x in t.split(",") if x.strip()]


def main() -> None:
    args = parse_args()
    tickers = _normalize_tickers(args.tickers)
    
    print(f"Starting predictions for {len(tickers)} ticker(s)")
    print(f"Prediction window: {args.days} days ahead\n")
    
    for ticker in tickers:
        print(f"{'='*60}")
        print(f"Processing: {ticker}")
        print(f"{'='*60}")
        try:
            s3_key = fetch_and_predict(
                ticker=ticker,
                start=args.start,
                end=args.end,
                bucket_name=args.bucket,
                s3_prefix=args.prefix,
                aws_access_key_id=args.access_key,
                aws_secret_access_key=args.secret_key,
                region_name=args.region,
                days_ahead=args.days,
            )
            print(f"✓ Success: {ticker}\n")
        except Exception as e:
            print(f"✗ Failed to generate predictions for {ticker}: {e}\n")


if __name__ == "__main__":
    main()
