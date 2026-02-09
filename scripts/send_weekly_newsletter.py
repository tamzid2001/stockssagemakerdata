#!/usr/bin/env python3
"""Generate a weekly stock newsletter with OpenAI and send it to Slack."""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

import boto3
import pandas as pd
import pytz
import requests
from openai import OpenAI
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


@dataclass
class StockSnapshot:
    ticker: str
    last_close: Optional[float]
    prev_close: Optional[float]
    change_pct: Optional[float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a weekly stock newsletter and post to Slack.")
    parser.add_argument("--tickers", help="Comma-separated tickers, e.g. AAPL,MSFT")
    parser.add_argument("--input-csv", help="Path to CSV with stock history")
    parser.add_argument("--s3-uri", help="S3 URI to CSV (s3://bucket/key)")
    parser.add_argument("--timezone", default="America/New_York", help="Timezone for report date")
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback window for summary")
    parser.add_argument("--openai-model", default=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini"))
    parser.add_argument("--slack-channel", default=os.environ.get("SLACK_CHANNEL"))
    parser.add_argument("--dry-run", action="store_true", help="Print message without posting to Slack")
    return parser.parse_args()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError("S3 URI must start with s3://")
    _, rest = uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)
    return bucket, key


def download_s3_csv(uri: str, dest_path: str, region: Optional[str] = None) -> str:
    bucket, key = parse_s3_uri(uri)
    s3 = boto3.client("s3", region_name=region)
    s3.download_file(bucket, key, dest_path)
    return dest_path


def load_history(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    # Expected columns: Ticker, Date, Close (case-insensitive); fall back to Price.
    rename_map = {}
    for col in df.columns:
        lower = col.lower()
        if lower in {"ticker", "symbol"}:
            rename_map[col] = "ticker"
        elif lower in {"date", "timestamp", "ts"}:
            rename_map[col] = "date"
        elif lower in {"close", "adj_close", "price"}:
            rename_map[col] = "close"
    df = df.rename(columns=rename_map)
    if "date" not in df.columns or "close" not in df.columns:
        raise ValueError("CSV must include date and close/price columns.")
    if "ticker" not in df.columns:
        df["ticker"] = "UNKNOWN"
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def summarize_history(df: pd.DataFrame, lookback_days: int) -> list[StockSnapshot]:
    snapshots: list[StockSnapshot] = []
    cutoff = df["date"].max() - timedelta(days=lookback_days)
    for ticker, group in df.groupby("ticker"):
        recent = group[group["date"] >= cutoff]
        if recent.empty:
            snapshots.append(StockSnapshot(ticker=ticker, last_close=None, prev_close=None, change_pct=None))
            continue
        last_close = float(recent.iloc[-1]["close"])
        prev_close = float(recent.iloc[-2]["close"]) if len(recent) > 1 else None
        change_pct = None
        if prev_close and prev_close != 0:
            change_pct = (last_close - prev_close) / prev_close * 100
        snapshots.append(
            StockSnapshot(ticker=ticker, last_close=last_close, prev_close=prev_close, change_pct=change_pct)
        )
    return snapshots


def build_prompt(
    report_date: str,
    tickers: list[str],
    snapshots: Optional[list[StockSnapshot]] = None,
) -> str:
    lines = [
        "You are a concise markets newsletter writer.",
        "Write a short weekly stock newsletter for a retail audience.",
        "Mark it clearly as a TEST MESSAGE.",
        "Include a short disclaimer: informational only, not investment advice.",
        f"Report date: {report_date}.",
        f"Tickers: {', '.join(tickers) if tickers else 'N/A'}.",
        "Keep it under 1800 characters.",
    ]
    if snapshots:
        snapshot_lines = []
        for snap in snapshots:
            change = "n/a" if snap.change_pct is None else f"{snap.change_pct:+.2f}%"
            last_close = "n/a" if snap.last_close is None else f"{snap.last_close:.2f}"
            snapshot_lines.append(f"{snap.ticker}: last_close={last_close}, weekly_change={change}")
        lines.append("Recent performance:")
        lines.extend(snapshot_lines)
    return "\n".join(lines)


def generate_newsletter(prompt: str, model: str) -> str:
    client = OpenAI()
    response = client.responses.create(
        model=model,
        input=prompt,
    )
    return response.output_text.strip()


def post_to_slack(text: str, channel: str) -> None:
    token = os.environ.get("SLACK_BOT_TOKEN")
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    if webhook_url:
        resp = requests.post(webhook_url, json={"text": text}, timeout=30)
        if resp.status_code >= 400:
            raise RuntimeError(f"Slack webhook failed: {resp.status_code} {resp.text}")
        return

    if not token:
        raise RuntimeError("Missing SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL.")
    if not channel:
        raise RuntimeError("Missing Slack channel (use --slack-channel or SLACK_CHANNEL).")

    client = WebClient(token=token)
    try:
        client.chat_postMessage(channel=channel, text=text)
    except SlackApiError as exc:
        raise RuntimeError(f"Slack API error: {exc.response['error']}") from exc


def main() -> None:
    args = parse_args()
    tz = pytz.timezone(args.timezone)
    report_date = datetime.now(tz).strftime("%Y-%m-%d")

    tickers = [t.strip().upper() for t in (args.tickers or "").split(",") if t.strip()]

    snapshots: Optional[list[StockSnapshot]] = None
    if args.s3_uri or args.input_csv:
        csv_path = args.input_csv
        if args.s3_uri:
            dest = os.path.join("/tmp", "newsletter_stock_history.csv")
            csv_path = download_s3_csv(args.s3_uri, dest, region=os.environ.get("AWS_REGION"))
        if csv_path:
            history_df = load_history(csv_path)
            if not tickers:
                tickers = sorted({t for t in history_df["ticker"].unique() if isinstance(t, str)})
            snapshots = summarize_history(history_df, args.lookback_days)

    prompt = build_prompt(report_date, tickers, snapshots)
    newsletter = generate_newsletter(prompt, args.openai_model)

    if args.dry_run:
        print(newsletter)
        return

    post_to_slack(newsletter, args.slack_channel)
    print("Slack newsletter sent.")


if __name__ == "__main__":
    main()
