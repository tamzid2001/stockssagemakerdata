import os
import csv
import json
import datetime
from datetime import timedelta
import yfinance as yf
from openai import OpenAI

# --------------------
# CONFIG
# --------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OUTPUT_FILE = "value_candidates.csv"
TICKERS_FILE = "tickers.txt"  # one ticker per line

client = OpenAI(api_key=OPENAI_API_KEY)

# --------------------
# LOAD TICKERS
# --------------------
def load_tickers():
    with open(TICKERS_FILE, "r") as f:
        return [line.strip() for line in f if line.strip()]

# --------------------
# FUNDAMENTALS + PRICE METRICS
# --------------------
def get_fundamentals(ticker):
    """
    Get fundamental data and price performance metrics.
    Uses fundamentals + general market context (no external news fetching).
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get current price
        current_price = info.get("currentPrice", info.get("regularMarketPrice"))
        
        # Calculate month-to-date performance
        today = datetime.date.today()
        month_start = datetime.date(today.year, today.month, 1)
        
        # Download last month of price data
        hist = yf.download(ticker, start=month_start, end=today, progress=False)
        
        if len(hist) > 0:
            month_open = hist.iloc[0]["Open"]
            month_change = ((current_price - month_open) / month_open * 100) if month_open > 0 else None
            month_pct_down = -month_change if month_change and month_change < 0 else None
        else:
            month_change = None
            month_pct_down = None
        
        # Get analyst price target (median)
        analyst_target = info.get("targetMeanPrice")
        upside_to_target = None
        if analyst_target and current_price:
            upside_to_target = ((analyst_target - current_price) / current_price * 100)
        
        return {
            "ticker": ticker,
            "current_price": current_price,
            "market_cap": info.get("marketCap"),
            "market_cap_billions": info.get("marketCap") / 1e9 if info.get("marketCap") else None,
            "pe": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "peg_ratio": info.get("pegRatio"),
            "price_to_sales": info.get("priceToSalesTrailing12Months"),
            "price_to_book": info.get("priceToBook"),
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            "profit_margin": info.get("profitMargins"),
            "debt_to_equity": info.get("debtToEquity"),
            "current_ratio": info.get("currentRatio"),
            "quick_ratio": info.get("quickRatio"),
            "free_cash_flow": info.get("freeCashflow"),
            "operating_cash_flow": info.get("operatingCashflow"),
            "month_pct_change": month_change,
            "month_pct_down": month_pct_down,
            "analyst_target_price": analyst_target,
            "upside_to_target_pct": upside_to_target,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "employee_count": info.get("fullTimeEmployees")
        }
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return None

# --------------------
# GPT SCORING (FUNDAMENTALS + GENERAL MARKET CONTEXT ONLY)
# --------------------
def score_stock(data):
    """
    Score stock using fundamentals and general market context.
    No external news fetching - relies on fundamental metrics and LLM's training data context.
    """
    prompt = f"""
You are a buy-side equity research analyst operating like a hedge-fund analyst.

Score this stock using the fundamentals below AND your general awareness of
current market conditions, sector trends, and macro environment:

Return STRICT JSON ONLY with this schema:
{{
    "value_score": <1-10>,
    "growth_score": <1-10>,
    "earnings_beat_probability": "<High/Medium/Low>",
    "upside_score": <1-10>,
    "key_bull_thesis": "<max 2 sentences>",
    "key_risk": "<max 1 sentence>",
    "confidence_level": "<High/Medium/Low>"
}}

Evaluation framework:
- Value score: P/E, Forward P/E, P/S, Price/Book ratios vs sector + quality metrics (margins, FCF, debt)
- Growth score: Revenue/earnings growth, acceleration, and segment-level drivers
- Earnings beat probability: Historical beat/miss patterns + estimate revisions + guidance (if available)
- Upside score: Analyst target price upside + sector tailwinds + company-specific catalysts
- Be conservative if data is incomplete (low confidence)

Stock fundamentals and metrics:
{json.dumps(data, indent=2)}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )

    return json.loads(response.choices[0].message.content)

# --------------------
# MAIN
# --------------------
def main():
    if not os.path.exists(TICKERS_FILE):
        print(f"Error: {TICKERS_FILE} not found. Please create it with one ticker per line.")
        return
    
    tickers = load_tickers()
    if not tickers:
        print(f"Error: No tickers found in {TICKERS_FILE}")
        return
    
    results = []

    for ticker in tickers:
        print(f"Processing {ticker}...")
        try:
            fundamentals = get_fundamentals(ticker)
            if fundamentals is None:
                print(f"Skipping {ticker} due to data fetch error")
                continue
            
            score = score_stock(fundamentals)

            row = {
                **fundamentals,
                **score,
                "screening_date": datetime.date.today().isoformat()
            }

            results.append(row)
            print(f"✓ Processed {ticker}")

        except Exception as e:
            print(f"✗ Error processing {ticker}: {e}")

    if results:
        # Sort by upside_score descending
        results.sort(key=lambda x: x.get("upside_score", 0), reverse=True)
        
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✓ Results saved to {OUTPUT_FILE}")
    else:
        print("No results to output")

if __name__ == "__main__":
    main()
