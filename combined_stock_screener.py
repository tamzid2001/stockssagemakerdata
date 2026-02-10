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

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
s3_client = boto3.client("s3", region_name=AWS_REGION) if (os.getenv("AWS_ACCESS_KEY_ID")) else None
DEFAULT_FALLBACK_TICKERS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    "AMD",
    "JPM",
    "XOM",
]


def generate_watchlist(num_tickers: int = 10) -> List[str]:
    """Generate a curated watchlist of tickers using ChatGPT."""
    if not client:
        return []

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

def load_tickers_file(path: str) -> List[str]:
    """Load a fallback ticker universe from a local text file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [line.strip().upper() for line in f if line.strip() and not line.strip().startswith("#")]
    except FileNotFoundError:
        return []


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.2f}%"


def _money(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.2f}"


def _fmt_int(value: int | float | None) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{int(value):,}"
    except Exception:
        return str(value)


def calculate_returns(close: pd.Series) -> dict:
    close = close.dropna()
    if close.empty:
        return {}

    def _ret(n: int) -> float | None:
        # n=1 means 1D (previous close), n=5 means 1W, etc.
        if len(close) <= n:
            return None
        base = float(close.iloc[-(n + 1)])
        last = float(close.iloc[-1])
        if base == 0:
            return None
        return (last - base) / base * 100.0

    return {
        "ret_1d_pct": _ret(1),
        "ret_5d_pct": _ret(5),
        "ret_21d_pct": _ret(21),
        "ret_63d_pct": _ret(63),
        "ret_126d_pct": _ret(126),
    }


def calculate_technical_indicators(ticker: str, period_days: int = 420) -> dict:
    """Calculate RSI, MA crosses, support/resistance levels."""
    try:
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=period_days)
        
        hist = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
        
        if hist.empty:
            return {}
        
        close = hist["Close"]
        returns = calculate_returns(close)
        
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
            **returns,
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
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)

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
    if not client:
        raise RuntimeError("OPENAI_API_KEY is required for agent scoring.")

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

def _clamp_score(value: float | None, *, lo: int = 1, hi: int = 10, default: int = 5) -> int:
    if value is None:
        return default
    return max(lo, min(hi, int(round(value))))


def score_stock_basic(fundamentals: dict, headlines: List[str]) -> dict:
    """Deterministic fallback when OpenAI is not configured."""
    ticker = fundamentals.get("ticker", "")
    sector = fundamentals.get("sector") or ""
    pe = fundamentals.get("pe")
    revenue_growth = fundamentals.get("revenue_growth")
    earnings_growth = fundamentals.get("earnings_growth")
    profit_margin = fundamentals.get("profit_margin")
    upside_to_target_pct = fundamentals.get("upside_to_target_pct")
    rsi = fundamentals.get("rsi_14")

    # Value: lower PE is better (rough heuristic).
    value_score = 5
    if isinstance(pe, (int, float)) and pe > 0:
        # 8 at PE~10, 5 at PE~25, 2 at PE~60.
        value_score = _clamp_score(10 - (pe - 10) / 7.5)

    # Growth: reward higher revenue/earnings growth.
    growth_signal = None
    if isinstance(revenue_growth, (int, float)):
        growth_signal = revenue_growth * 20  # 0.30 -> 6
    elif isinstance(earnings_growth, (int, float)):
        growth_signal = earnings_growth * 18
    growth_score = _clamp_score(growth_signal, default=5)

    # Technical: trend + RSI positioning.
    trend = _trend_label(fundamentals)
    tech_score = 5
    if trend == "Uptrend":
        tech_score += 2
    elif trend == "Momentum":
        tech_score += 1
    elif trend == "Weak":
        tech_score -= 1
    if isinstance(rsi, (int, float)):
        if rsi < 35:
            tech_score += 1  # mean-reversion potential
        elif rsi > 72:
            tech_score -= 1  # stretched
    technical_score = _clamp_score(tech_score)

    # Upside: map percent upside to 1-10.
    upside_signal = None
    if isinstance(upside_to_target_pct, (int, float)):
        upside_signal = upside_to_target_pct / 5  # 25% -> 5
    upside_score = _clamp_score(upside_signal, default=5)

    # Earnings beat probability: simple heuristic.
    beat = "Medium"
    if isinstance(profit_margin, (int, float)) and isinstance(revenue_growth, (int, float)):
        if profit_margin > 0.15 and revenue_growth > 0.15:
            beat = "High"
        elif profit_margin < 0.05 and revenue_growth < 0.05:
            beat = "Low"

    confidence = "Low"
    score_inputs = [pe, revenue_growth, upside_to_target_pct, rsi]
    present = sum(1 for v in score_inputs if isinstance(v, (int, float)))
    if present >= 3:
        confidence = "High"
    elif present >= 2:
        confidence = "Medium"

    headline = headlines[0] if headlines else ""
    bull = f"{trend} profile with RSI {rsi:.0f}." if isinstance(rsi, (int, float)) else f"{trend} profile."
    if isinstance(upside_to_target_pct, (int, float)):
        bull += f" Analyst target implies {_pct(upside_to_target_pct)}."
    if headline:
        bull += f" Recent headline: {headline}"

    risk = "Trend can reverse quickly on macro shocks or earnings volatility."
    if isinstance(pe, (int, float)) and pe > 40:
        risk = "Valuation risk remains elevated; drawdowns can be sharp on earnings misses."
    elif trend == "Weak":
        risk = "Price action is weak; consider waiting for stabilization or confirmation."

    setup = f"{trend} bias. Watch MA alignment + RSI for confirmation."

    return {
        "ticker": ticker,
        "sector": sector,
        "value_score": int(value_score),
        "growth_score": int(growth_score),
        "technical_score": int(technical_score),
        "earnings_beat_probability": beat,
        "upside_score": int(upside_score),
        "key_bull_thesis": bull[:280],
        "key_risk": risk[:220],
        "technical_setup": setup[:220],
        "confidence_level": confidence,
    }


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


def _slack_post(payload: dict) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)
        if response.status_code >= 400:
            print(f"Slack webhook failed: {response.status_code} {response.text}")
            return
        print("✓ Slack message sent")
    except Exception as exc:
        print(f"Slack webhook error: {exc}")


def _trend_label(data: dict) -> str:
    if data.get("price_above_ma20") and data.get("ma20_above_ma50") and data.get("ma50_above_ma200"):
        return "Uptrend"
    if data.get("price_above_ma50") and data.get("ma20_above_ma50"):
        return "Momentum"
    if data.get("price_above_ma200"):
        return "Base"
    return "Weak"


def send_slack_screener_report(results: List[dict], tickers_with_data: List[Tuple[str, dict]]) -> None:
    """Send an informative, desk-style screener report to Slack."""
    if not SLACK_WEBHOOK_URL or not results:
        return

    report_date = datetime.date.today().isoformat()
    top = results[:8]
    by_ticker = {t: d for t, d in tickers_with_data}

    # Watchlist table
    table_lines = []
    table_lines.append("Ticker  Price      1D     1W     1M     3M   RSI  Trend  Upside")
    table_lines.append("-" * 72)
    for row in results[:12]:
        t = row.get("ticker")
        d = by_ticker.get(t, {})
        price = d.get("current_price")
        rsi = d.get("rsi_14")
        trend = _trend_label(d)
        upside = row.get("upside_to_target_pct")
        table_lines.append(
            f"{t:<6}  {_money(price):>8}  {_pct(d.get('ret_1d_pct')):>6}  {_pct(d.get('ret_5d_pct')):>6}  "
            f"{_pct(d.get('ret_21d_pct')):>6}  {_pct(d.get('ret_63d_pct')):>6}  "
            f"{(f'{rsi:>4.0f}' if isinstance(rsi, (int, float)) else ' n/a'):>4}  {trend:<7}  {_pct(upside):>6}"
        )
    watchlist_table = "```" + "\n".join(table_lines) + "```"

    # Top picks narrative
    pick_lines = []
    for idx, row in enumerate(top, start=1):
        t = row.get("ticker")
        value = row.get("value_score")
        growth = row.get("growth_score")
        tech = row.get("technical_score")
        upside_score = row.get("upside_score")
        conf = row.get("confidence_level")
        bull = row.get("key_bull_thesis", "").strip()
        risk = row.get("key_risk", "").strip()
        setup = row.get("technical_setup", "").strip()
        headlines = (row.get("headlines") or "").split(";")[:1]
        headline = headlines[0].strip() if headlines else ""
        pick_lines.append(
            f"*{idx}. {t}*  (V{value}/G{growth}/T{tech} • Upside {upside_score}/10 • {conf})\n"
            f"• Bull: {bull}\n"
            f"• Risk: {risk}\n"
            f"• Setup: {setup}"
            + (f"\n• Headline: {headline}" if headline else "")
        )

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"Quantura Weekly Screener • {report_date}"}},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"*Universe:* {len(results)} tickers • *Method:* fundamentals + technicals + headlines"},
            ],
        },
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top opportunities*\n" + "\n\n".join(pick_lines)}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Watchlist snapshot*\n" + watchlist_table}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "_Informational only. Not investment advice._"}]},
    ]

    _slack_post({"text": f"Quantura Weekly Screener {report_date}", "blocks": blocks})


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
        # Fall back to tickers.txt so the pipeline remains useful when OPENAI_API_KEY is not configured.
        watchlist = load_tickers_file(TICKERS_FILE)
        if not watchlist:
            watchlist = DEFAULT_FALLBACK_TICKERS.copy()
            print("⚠️  Using built-in fallback tickers (OPENAI_API_KEY and tickers.txt are not configured).")
    
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

        if client:
            agent_scores = score_stock_with_agent(fundamentals, technicals, headlines)
        else:
            agent_scores = score_stock_basic(fundamentals, headlines)

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
    send_slack_screener_report(results, tickers_with_data)
    
    print("\n" + "="*60)
    print("✅ Stock screening pipeline complete!")


if __name__ == "__main__":
    main()
