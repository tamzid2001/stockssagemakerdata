#!/usr/bin/env python3
"""
Combined stock screener that:
1. Generates a curated watchlist of tickers using ChatGPT
2. Fetches fundamentals, technical indicators, and headlines
3. Scores each stock with agent-style analysis
4. Uploads watchlist and results to S3
5. Delivers results via Slack
"""

import csv
import datetime
import json
import os
from typing import List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from openai import OpenAI
import boto3

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OUTPUT_FILE = os.getenv("SCREENING_OUTPUT_FILE", "combined_screening_results.csv")
WATCHLIST_FILE = os.getenv("WATCHLIST_FILE", "watchlist.csv")
TICKERS_FILE = os.getenv("TICKERS_FILE", "tickers.txt")
HEADLINES_PER_TICKER = int(os.getenv("HEADLINES_PER_TICKER", "3"))

# AWS S3 Configuration
AWS_BUCKET = os.getenv("AWS_BUCKET", "stockscompute")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_PREFIX = os.getenv("S3_PREFIX", "screening_results/")

# Slack Configuration
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

client = OpenAI(api_key=OPENAI_API_KEY)
s3_client = boto3.client("s3", region_name=AWS_REGION) if (os.getenv("AWS_ACCESS_KEY_ID")) else None


def generate_watchlist(num_tickers: int = 10) -> List[str]:
    """Generate a curated watchlist of tickers using ChatGPT."""
    print(f"\n📊 Generating watchlist of {num_tickers} tickers...")
    
    prompt = f"""
You are a stock market analyst. Generate a curated watchlist of {num_tickers} publicly traded companies
that show interesting technical setups and fundamental strength.

Consider:
- Recent momentum and technical setups
- Earnings catalysts
- Relative strength vs peers
- Popular growth and value plays

Return ONLY a JSON array of ticker symbols (strings), no explanations.
Example: ["AAPL", "MSFT", "TSLA", ...]
"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    
    try:
        tickers = json.loads(response.choices[0].message.content)
        if isinstance(tickers, list):
            return [t.upper().strip() for t in tickers]
    except json.JSONDecodeError:
        print("Error parsing watchlist from ChatGPT")
    
    return []


def calculate_technical_indicators(ticker: str, period_days: int = 90) -> dict:
    """Calculate RSI, MA crosses, support/resistance levels."""
    try:
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=period_days)
        
        hist = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        if hist.empty:
            return {}
        
        close = hist["Close"]
        
        # RSI (14-period)
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1] if not rsi.empty else None
        
        # Moving Averages
        ma_20 = close.rolling(window=20).mean()
        ma_50 = close.rolling(window=50).mean()
        ma_200 = close.rolling(window=200).mean()
        
        current_price = close.iloc[-1]
        current_ma20 = ma_20.iloc[-1] if not ma_20.empty else None
        current_ma50 = ma_50.iloc[-1] if not ma_50.empty else None
        current_ma200 = ma_200.iloc[-1] if not ma_200.empty else None
        
        # MA crossovers
        ma_20_above_50 = current_ma20 > current_ma50 if (current_ma20 and current_ma50) else None
        ma_50_above_200 = current_ma50 > current_ma200 if (current_ma50 and current_ma200) else None
        
        # Support and Resistance (52-week)
        high_52w = close.rolling(window=252).max().iloc[-1]
        low_52w = close.rolling(window=252).min().iloc[-1]
        
        # Distance from support/resistance
        distance_to_high = ((high_52w - current_price) / current_price * 100) if current_price else None
        distance_to_low = ((current_price - low_52w) / current_price * 100) if current_price else None
        
        return {
            "rsi_14": round(current_rsi, 2) if current_rsi else None,
            "ma_20": round(current_ma20, 2) if current_ma20 else None,
            "ma_50": round(current_ma50, 2) if current_ma50 else None,
            "ma_200": round(current_ma200, 2) if current_ma200 else None,
            "price_above_ma20": current_ma20 and current_price > current_ma20,
            "price_above_ma50": current_ma50 and current_price > current_ma50,
            "price_above_ma200": current_ma200 and current_price > current_ma200,
            "ma20_above_ma50": ma_20_above_50,
            "ma50_above_ma200": ma_50_above_200,
            "52w_high": round(high_52w, 2),
            "52w_low": round(low_52w, 2),
            "distance_from_52w_high_pct": round(distance_to_high, 2) if distance_to_high else None,
            "distance_from_52w_low_pct": round(distance_to_low, 2) if distance_to_low else None,
        }
    except Exception as e:
        print(f"Error calculating technical indicators for {ticker}: {e}")
        return {}


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
            headlines.append(f"{title}{source}")

    return headlines


def get_fundamentals_and_technicals(ticker: str) -> Optional[dict]:
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

        # Get technical indicators
        technicals = calculate_technical_indicators(ticker)

        return {
            "ticker": ticker,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "market_cap": info.get("marketCap"),
            "market_cap_billions": info.get("marketCap") / 1e9 if info.get("marketCap") else None,
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
            **technicals,  # Include all technical indicators
        }
    except Exception as exc:
        print(f"Error fetching data for {ticker}: {exc}")
        return None


def score_stock_with_agent(fundamentals: dict, technicals: dict, headlines: List[str]) -> dict:
    """Score stock using agent with technical indicator context."""
    schema = {
        "name": "ticker_screening",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "ticker": {"type": "string"},
                "sector": {"type": "string"},
                "value_score": {"type": "integer", "minimum": 1, "maximum": 10},
                "growth_score": {"type": "integer", "minimum": 1, "maximum": 10},
                "technical_score": {"type": "integer", "minimum": 1, "maximum": 10},
                "earnings_beat_probability": {"type": "string", "enum": ["High", "Medium", "Low"]},
                "upside_score": {"type": "integer", "minimum": 1, "maximum": 10},
                "key_bull_thesis": {"type": "string"},
                "key_risk": {"type": "string"},
                "technical_setup": {"type": "string"},
                "confidence_level": {"type": "string", "enum": ["High", "Medium", "Low"]},
            },
            "required": [
                "ticker",
                "sector",
                "value_score",
                "growth_score",
                "technical_score",
                "earnings_beat_probability",
                "upside_score",
                "key_bull_thesis",
                "key_risk",
                "technical_setup",
                "confidence_level",
            ],
        },
        "strict": True,
    }

    prompt = f"""
You are a buy-side equity research analyst with expertise in technical analysis.

Analyze this stock using:
1. Fundamental metrics (valuation, growth, profitability)
2. Technical indicators (RSI, MA alignment, support/resistance)
3. Recent news headlines
4. General market context

Technical Scoring Guidance:
- RSI > 70: Overbought (bearish), RSI < 30: Oversold (bullish)
- Price > MA20 > MA50 > MA200: Strong uptrend (bullish)
- MA20 crosses above MA50: Golden cross (very bullish)
- Price near 52-week high: Breakout potential (bullish)
- Price near 52-week low: Value opportunity (bullish if fundamentals strong)

Return JSON matching the schema exactly.
Be concise: max 2 sentences for theses, 1 sentence for technical setup.
"""

    payload = {
        "fundamentals": {
            k: v for k, v in fundamentals.items() 
            if k in ["ticker", "sector", "pe", "forward_pe", "peg_ratio", "price_to_sales", 
                    "revenue_growth", "earnings_growth", "profit_margin", "analyst_target_price", "upside_to_target_pct"]
        },
        "technical_indicators": technicals,
        "headlines": headlines[:3],
    }

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Return only valid JSON matching the schema."},
            {"role": "user", "content": f"{prompt}\n\n{json.dumps(payload, indent=2, default=str)}"},
        ],
        temperature=0.2,
        response_format={"type": "json_schema", "json_schema": schema},
    )

    return json.loads(response.choices[0].message.content)


def save_watchlist_to_s3(watchlist: List[str], tickers_with_data: List[Tuple[str, dict]]) -> str:
    """Save watchlist to S3 in a formatted CSV."""
    if not s3_client or not watchlist:
        return None
    
    try:
        # Build watchlist data with price and performance
        watchlist_data = []
        for ticker, data in tickers_with_data:
            watchlist_data.append({
                "Ticker": ticker,
                "Price": f"${data.get('current_price', 'N/A')}",
                "1D": f"{data.get('month_pct_down', 'N/A')}%",  # Placeholder
                "5D": "N/A",
                "1M": f"{data.get('month_pct_down', 'N/A')}%",
                "6M": "N/A",
            })
        
        # Save to local file first
        with open(WATCHLIST_FILE, "w", newline="") as f:
            if watchlist_data:
                writer = csv.DictWriter(f, fieldnames=watchlist_data[0].keys())
                writer.writeheader()
                writer.writerows(watchlist_data)
        
        # Upload to S3
        date_str = datetime.date.today().isoformat()
        s3_key = f"{S3_PREFIX}watchlist_{date_str}.csv"
        
        s3_client.upload_file(WATCHLIST_FILE, AWS_BUCKET, s3_key)
        print(f"✓ Watchlist uploaded to S3: s3://{AWS_BUCKET}/{s3_key}")
        
        return s3_key
    except Exception as e:
        print(f"Error uploading watchlist to S3: {e}")
        return None


def send_slack_watchlist(watchlist: List[str], tickers_with_data: List[Tuple[str, dict]]) -> None:
    """Send formatted watchlist to Slack."""
    if not SLACK_WEBHOOK_URL or not watchlist:
        return

    # Build table message
    lines = [
        "*📊 Weekly Stock Watchlist*",
        f"Generated: {datetime.date.today().isoformat()}",
        "```",
        "Ticker | Price | 1D | 5D | 1M | 6M",
        "-" * 50,
    ]
    
    for ticker, data in tickers_with_data[:10]:
        price = f"${data.get('current_price', 'N/A'):.2f}" if data.get('current_price') else "N/A"
        lines.append(f"{ticker:6} | {price:>10}")
    
    lines.append("```")
    
    message = "\n".join(lines)
    
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
        if response.status_code < 400:
            print("✓ Watchlist sent to Slack")
    except Exception as e:
        print(f"Slack webhook error: {e}")


def send_slack_results(results: List[dict]) -> None:
    """Send screening results summary to Slack."""
    if not SLACK_WEBHOOK_URL or not results:
        return

    lines = ["*📈 Stock Screening Results*", f"Total tickers screened: {len(results)}"]
    
    for row in results[:5]:
        lines.append(
            f"• {row['ticker']} | Value {row['value_score']}/10 | Tech {row.get('technical_score', 'N/A')}/10 | "
            f"Upside {row['upside_score']}/10 | {row['confidence_level']}"
        )

    message = "\n".join(lines)
    
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
        print("✓ Results summary sent to Slack")
    except Exception as e:
        print(f"Slack webhook error: {e}")


def upload_results_to_s3(csv_path: str) -> None:
    """Upload screening results to S3."""
    if not s3_client or not os.path.exists(csv_path):
        return
    
    try:
        date_str = datetime.date.today().isoformat()
        s3_key = f"{S3_PREFIX}screening_results_{date_str}.csv"
        
        s3_client.upload_file(csv_path, AWS_BUCKET, s3_key)
        print(f"✓ Results uploaded to S3: s3://{AWS_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Error uploading results to S3: {e}")


def main() -> None:
    # Step 1: Generate watchlist
    watchlist = generate_watchlist(num_tickers=10)
    if not watchlist:
        print("❌ Failed to generate watchlist")
        return
    
    print(f"✓ Generated watchlist: {watchlist}")
    
    # Step 2: Screen each ticker
    results = []
    tickers_with_data = []

    for ticker in watchlist:
        print(f"\nProcessing {ticker}...")
        
        fundamentals = get_fundamentals_and_technicals(ticker)
        if not fundamentals:
            print(f"  ✗ Failed to fetch data for {ticker}")
            continue

        headlines = get_headlines(ticker)
        
        # Extract technical indicators for agent
        technicals = {k: v for k, v in fundamentals.items() 
                     if k.startswith(("rsi", "ma_", "price_", "52w_", "distance_"))}
        
        agent_scores = score_stock_with_agent(fundamentals, technicals, headlines)

        row = {
            **agent_scores,
            **{k: fundamentals[k] for k in ["current_price", "market_cap", "month_pct_down", 
                                            "analyst_target_price", "upside_to_target_pct"]},
            "headlines": "; ".join(headlines[:2]),
            "screening_date": datetime.date.today().isoformat(),
        }

        results.append(row)
        tickers_with_data.append((ticker, fundamentals))
        print(f"  ✓ {ticker} scored")

    if not results:
        print("❌ No results to output")
        return

    # Step 3: Save watchlist
    print("\n" + "="*60)
    print("Saving watchlist and results...")
    
    save_watchlist_to_s3(watchlist, tickers_with_data)
    
    # Save watchlist to file
    with open(TICKERS_FILE, "w") as f:
        for ticker in watchlist:
            f.write(f"{ticker}\n")
    print(f"✓ Watchlist saved to {TICKERS_FILE}")

    # Step 4: Save screening results
    results.sort(key=lambda x: x.get("upside_score", 0), reverse=True)
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"✓ Results saved to {OUTPUT_FILE}")

    # Step 5: Upload to S3 and send to Slack
    upload_results_to_s3(OUTPUT_FILE)
    send_slack_watchlist(watchlist, tickers_with_data)
    send_slack_results(results)
    
    print("\n" + "="*60)
    print("✅ Stock screening pipeline complete!")


if __name__ == "__main__":
    main()
