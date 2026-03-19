#!/usr/bin/env python3
"""
Combined stock screener that blends fundamentals, agent-style scoring,
headline context, and AWS-backed notifications.
"""

import csv
import datetime
import json
import os
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

import boto3
import requests
import yfinance as yf
from openai import OpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OUTPUT_FILE = os.getenv("SCREENING_OUTPUT_FILE", "combined_screening_results.csv")
TICKERS_FILE = os.getenv("TICKERS_FILE", "tickers.txt")
SCREENER_TICKERS = os.getenv("SCREENER_TICKERS", "")
HEADLINES_PER_TICKER = int(os.getenv("HEADLINES_PER_TICKER", "3"))
MIN_MARKET_CAP = float(os.getenv("MIN_MARKET_CAP", "100000000000"))

AWS_REGION = os.getenv("AWS_REGION")
AWS_SNS_TOPIC_ARN = os.getenv("AWS_SNS_TOPIC_ARN")
AWS_SES_FROM_EMAIL = os.getenv("AWS_SES_FROM_EMAIL")
AWS_SES_TO_EMAILS = [email.strip() for email in os.getenv("AWS_SES_TO_EMAILS", "").split(",") if email.strip()]

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def load_tickers() -> List[str]:
    env_tickers = [
        ticker.strip().upper()
        for ticker in SCREENER_TICKERS.replace("\n", ",").split(",")
        if ticker.strip()
    ]
    if env_tickers:
        return list(dict.fromkeys(env_tickers))

    if not os.path.exists(TICKERS_FILE):
        raise FileNotFoundError(
            f"{TICKERS_FILE} not found. Provide SCREENER_TICKERS or create the file with one ticker per line."
        )

    with open(TICKERS_FILE, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    if len(tickers) < 2:
        print("Warning: Less than two tickers provided; add more tickers for a broader screen.")

    return tickers


def market_cap_category(market_cap: Optional[float]) -> str:
    if not market_cap:
        return "Unknown"
    if market_cap < 2e9:
        return "Small"
    if market_cap < 10e9:
        return "Mid"
    return "Large"


def get_headlines(ticker: str) -> List[str]:
    try:
        news_items = yf.Ticker(ticker).news or []
    except Exception as exc:
        print(f"Error fetching headlines for {ticker}: {exc}")
        news_items = []

    headlines = []
    for item in news_items[:HEADLINES_PER_TICKER]:
        title = item.get("title")
        publisher = item.get("publisher")
        link = item.get("link")
        if title and link:
            source = f" ({publisher})" if publisher else ""
            headlines.append(f"{title}{source} - {link}")

    return headlines


def format_market_cap(market_cap: Optional[float]) -> str:
    if not market_cap:
        return "Unknown"
    return f"${market_cap / 1e9:,.1f}B"


def build_boto3_client(service_name: str):
    kwargs = {"region_name": AWS_REGION} if AWS_REGION else {}
    return boto3.client(service_name, **kwargs)


def get_fundamentals(ticker: str) -> Optional[dict]:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        current_price = info.get("currentPrice", info.get("regularMarketPrice"))

        today = datetime.date.today()
        month_start = datetime.date(today.year, today.month, 1)
        hist = yf.download(ticker, start=month_start, end=today, progress=False)

        month_pct_down = None
        if len(hist) > 0 and current_price:
            month_open = hist.iloc[0]["Open"]
            month_change = ((current_price - month_open) / month_open * 100) if month_open > 0 else None
            month_pct_down = -month_change if month_change and month_change < 0 else None

        analyst_target = info.get("targetMeanPrice")
        upside_to_target = None
        if analyst_target and current_price:
            upside_to_target = ((analyst_target - current_price) / current_price * 100)

        return {
            "ticker": ticker,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "market_cap": info.get("marketCap"),
            "market_cap_category": market_cap_category(info.get("marketCap")),
            "current_price": current_price,
            "pe": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "peg_ratio": info.get("pegRatio"),
            "price_to_sales": info.get("priceToSalesTrailing12Months"),
            "price_to_book": info.get("priceToBook"),
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            "profit_margin": info.get("profitMargins"),
            "debt_to_equity": info.get("debtToEquity"),
            "free_cash_flow": info.get("freeCashflow"),
            "month_pct_down": month_pct_down,
            "analyst_target_price": analyst_target,
            "upside_to_target_pct": upside_to_target,
        }
    except Exception as exc:
        print(f"Error fetching fundamentals for {ticker}: {exc}")
        return None


def score_stock_with_agent(fundamentals: dict, headlines: List[str]) -> dict:
    if client is None:
        raise RuntimeError("OPENAI_API_KEY is required to score stocks with the agent.")

    schema = {
        "name": "ticker_screening",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "ticker": {"type": "string"},
                "sector": {"type": "string"},
                "market_cap_category": {"type": "string"},
                "value_score": {"type": "integer", "minimum": 1, "maximum": 10},
                "growth_score": {"type": "integer", "minimum": 1, "maximum": 10},
                "earnings_beat_probability": {"type": "string", "enum": ["High", "Medium", "Low"]},
                "upside_score": {"type": "integer", "minimum": 1, "maximum": 10},
                "key_bull_thesis": {"type": "string"},
                "key_risk": {"type": "string"},
                "confidence_level": {"type": "string", "enum": ["High", "Medium", "Low"]},
            },
            "required": [
                "ticker",
                "sector",
                "market_cap_category",
                "value_score",
                "growth_score",
                "earnings_beat_probability",
                "upside_score",
                "key_bull_thesis",
                "key_risk",
                "confidence_level",
            ],
        },
        "strict": True,
    }

    prompt = """
You are a buy-side equity research analyst. Use the fundamentals and headlines below
(and your general market context) to score the stock.

Return JSON only that matches the schema exactly.
"""

    payload = {
        "fundamentals": fundamentals,
        "headlines": headlines,
        "instructions": "Use concise, actionable phrasing. Keep bull thesis and key risk to 2 sentences max.",
    }

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": f"{prompt}\n\n{json.dumps(payload, indent=2, default=str)}"},
        ],
        temperature=0.2,
        response_format={"type": "json_schema", "json_schema": schema},
    )

    return json.loads(response.choices[0].message.content)


def send_slack_webhook(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return

    response = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    if response.status_code >= 400:
        print(f"Slack webhook error: {response.status_code} {response.text}")


def send_slack_file(csv_path: str) -> None:
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL:
        return

    with open(csv_path, "rb") as f:
        response = requests.post(
            "https://slack.com/api/files.upload",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            data={"channels": SLACK_CHANNEL, "filename": os.path.basename(csv_path)},
            files={"file": f},
            timeout=30,
        )

    if response.status_code >= 400 or not response.json().get("ok"):
        print(f"Slack file upload error: {response.status_code} {response.text}")


def build_notification_message(results: List[dict], screened_count: int, skipped_for_market_cap: int) -> str:
    lines = [
        "Stock Screener Results",
        f"Tickers processed: {screened_count}",
        f"Qualified results: {len(results)}",
        f"Skipped below ${MIN_MARKET_CAP / 1e9:,.0f}B market cap: {skipped_for_market_cap}",
    ]
    for row in results[:5]:
        lines.append(
            f"- {row['ticker']} | Market Cap {format_market_cap(row.get('market_cap'))} | "
            f"Value {row['value_score']}/10 | Growth {row['growth_score']}/10 | "
            f"Upside {row['upside_score']}/10 | {row['confidence_level']}"
        )
        if row.get("headlines"):
            lines.append(f"  Headlines: {row['headlines']}")

    return "\n".join(lines)


def send_sns_notification(message: str) -> None:
    if not AWS_SNS_TOPIC_ARN:
        return

    try:
        build_boto3_client("sns").publish(
            TopicArn=AWS_SNS_TOPIC_ARN,
            Subject="Stock Screener Results",
            Message=message,
        )
        print("✓ SNS notification sent")
    except Exception as exc:
        print(f"SNS notification error: {exc}")


def send_email_notification(message: str, csv_path: str) -> None:
    if not AWS_SES_FROM_EMAIL or not AWS_SES_TO_EMAILS:
        return

    email_message = MIMEMultipart()
    email_message["Subject"] = f"Stock Screener Results - {datetime.date.today().isoformat()}"
    email_message["From"] = AWS_SES_FROM_EMAIL
    email_message["To"] = ", ".join(AWS_SES_TO_EMAILS)
    email_message.attach(MIMEText(message, "plain", "utf-8"))

    with open(csv_path, "rb") as attachment:
        part = MIMEApplication(attachment.read())
        part.add_header("Content-Disposition", "attachment", filename=os.path.basename(csv_path))
        email_message.attach(part)

    try:
        build_boto3_client("ses").send_raw_email(
            Source=AWS_SES_FROM_EMAIL,
            Destinations=AWS_SES_TO_EMAILS,
            RawMessage={"Data": email_message.as_string()},
        )
        print("✓ SES email notification sent")
    except Exception as exc:
        print(f"SES email notification error: {exc}")


def main() -> None:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is required.")

    tickers = load_tickers()
    results = []
    screened_count = 0
    skipped_for_market_cap = 0

    for ticker in tickers:
        print(f"Processing {ticker}...")
        fundamentals = get_fundamentals(ticker)
        if not fundamentals:
            print(f"Skipping {ticker} due to data fetch error")
            continue

        screened_count += 1
        market_cap = fundamentals.get("market_cap")
        if not market_cap or market_cap <= MIN_MARKET_CAP:
            skipped_for_market_cap += 1
            print(
                f"Skipping {ticker} because market cap {format_market_cap(market_cap)} "
                f"is not above {format_market_cap(MIN_MARKET_CAP)}"
            )
            continue

        headlines = get_headlines(ticker)
        agent_scores = score_stock_with_agent(fundamentals, headlines)

        row = {
            **agent_scores,
            "market_cap": market_cap,
            "current_price": fundamentals.get("current_price"),
            "analyst_target_price": fundamentals.get("analyst_target_price"),
            "upside_to_target_pct": fundamentals.get("upside_to_target_pct"),
            "headlines": "; ".join(headlines),
            "screening_date": datetime.date.today().isoformat(),
        }

        if not row.get("sector"):
            row["sector"] = fundamentals.get("sector") or "Unknown"
        if not row.get("market_cap_category"):
            row["market_cap_category"] = fundamentals.get("market_cap_category")

        results.append(row)
        print(f"✓ Processed {ticker}")

    if not results:
        print("No results to output")
        return

    results.sort(key=lambda x: x.get("upside_score", 0), reverse=True)
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✓ Results saved to {OUTPUT_FILE}")

    notification_message = build_notification_message(results, screened_count, skipped_for_market_cap)
    send_sns_notification(notification_message)
    send_email_notification(notification_message, OUTPUT_FILE)

    if not AWS_SNS_TOPIC_ARN and not (AWS_SES_FROM_EMAIL and AWS_SES_TO_EMAILS):
        send_slack_webhook(notification_message)
        send_slack_file(OUTPUT_FILE)


if __name__ == "__main__":
    main()
