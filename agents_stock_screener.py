#!/usr/bin/env python3
"""
Advanced stock screening agent with multi-step analysis.
Uses OpenAI API with structured outputs for reliable JSON responses.
Can be extended to use OpenAI's Agents API later.
"""

import os
import csv
import json
import datetime
from typing import Optional
import yfinance as yf
from openai import OpenAI

# --------------------
# CONFIG
# --------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OUTPUT_FILE = "agent_screening_results.csv"
TICKERS_FILE = "tickers.txt"

client = OpenAI(api_key=OPENAI_API_KEY)

# --------------------
# DATA COLLECTION
# --------------------
def get_fundamentals(ticker: str) -> Optional[dict]:
    """Fetch comprehensive fundamental data for a stock."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        current_price = info.get("currentPrice", info.get("regularMarketPrice"))
        
        # Month-to-date performance
        today = datetime.date.today()
        month_start = datetime.date(today.year, today.month, 1)
        
        hist = yf.download(ticker, start=month_start, end=today, progress=False)
        
        month_pct_down = None
        if len(hist) > 0 and current_price:
            month_open = hist.iloc[0]["Open"]
            month_change = ((current_price - month_open) / month_open * 100) if month_open > 0 else None
            month_pct_down = -month_change if month_change and month_change < 0 else None
        
        # Analyst estimates
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
            "revenue_per_share": info.get("revenuePerShare"),
            "profit_margin": info.get("profitMargins"),
            "operating_margin": info.get("operatingMargins"),
            "return_on_equity": info.get("returnOnEquity"),
            "return_on_assets": info.get("returnOnAssets"),
            "debt_to_equity": info.get("debtToEquity"),
            "current_ratio": info.get("currentRatio"),
            "quick_ratio": info.get("quickRatio"),
            "free_cash_flow": info.get("freeCashflow"),
            "operating_cash_flow": info.get("operatingCashflow"),
            "month_pct_down": month_pct_down,
            "analyst_target_price": analyst_target,
            "upside_to_target_pct": upside_to_target,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "dividend_yield": info.get("dividendYield"),
            "52_week_high": info.get("fiftyTwoWeekHigh"),
            "52_week_low": info.get("fiftyTwoWeekLow"),
        }
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return None

# --------------------
# STRUCTURED AGENT ANALYSIS
# --------------------
def analyze_valuation(fundamentals: dict) -> dict:
    """Agent step 1: Analyze valuation metrics."""
    prompt = f"""
Analyze the valuation of this stock based on fundamental metrics.

Return JSON with this EXACT schema:
{{
    "valuation_assessment": "<bullish/neutral/bearish>",
    "valuation_score": <1-10>,
    "key_valuation_factors": ["<factor1>", "<factor2>", "<factor3>"],
    "vs_sector": "<undervalued/fairly_valued/overvalued>"
}}

Data:
{json.dumps(fundamentals, indent=2, default=str)}
"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    
    return json.loads(response.choices[0].message.content)

def analyze_growth(fundamentals: dict) -> dict:
    """Agent step 2: Analyze growth metrics."""
    prompt = f"""
Analyze the growth profile and quality of this stock.

Return JSON with this EXACT schema:
{{
    "growth_assessment": "<high/medium/low>",
    "growth_score": <1-10>,
    "growth_drivers": ["<driver1>", "<driver2>"],
    "growth_sustainability": "<sustainable/cyclical/declining>"
}}

Data:
{json.dumps(fundamentals, indent=2, default=str)}
"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    
    return json.loads(response.choices[0].message.content)

def analyze_earnings_quality(fundamentals: dict) -> dict:
    """Agent step 3: Analyze earnings quality and beat probability."""
    prompt = f"""
Assess earnings quality and likelihood of next quarter earnings beat.

Return JSON with this EXACT schema:
{{
    "earnings_quality": "<high/medium/low>",
    "beat_probability": "<high/medium/low>",
    "quality_factors": ["<factor1>", "<factor2>"],
    "catalysts": ["<catalyst1>", "<catalyst2>"] or []
}}

Data:
{json.dumps(fundamentals, indent=2, default=str)}
"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    
    return json.loads(response.choices[0].message.content)

def synthesize_recommendation(fundamentals: dict, valuation: dict, growth: dict, earnings: dict) -> dict:
    """Agent step 4: Synthesize all analyses into final recommendation."""
    combined_data = {
        "fundamentals_summary": {
            "ticker": fundamentals["ticker"],
            "sector": fundamentals["sector"],
            "current_price": fundamentals["current_price"],
            "month_pct_down": fundamentals.get("month_pct_down"),
            "upside_to_target_pct": fundamentals.get("upside_to_target_pct"),
        },
        "valuation_analysis": valuation,
        "growth_analysis": growth,
        "earnings_analysis": earnings
    }
    
    prompt = f"""
Based on the comprehensive analysis below, provide a final investment score and recommendation.

Return JSON with this EXACT schema:
{{
    "overall_score": <1-10>,
    "recommendation": "<strong_buy/buy/hold/reduce/avoid>",
    "bull_thesis": "<max 2 sentences>",
    "key_risks": "<max 2 sentences>",
    "target_price_upside": "<what % upside to analyst target>",
    "confidence_level": "<high/medium/low>",
    "investment_horizon": "<3-6 months/6-12 months/12+ months>"
}}

Analysis Results:
{json.dumps(combined_data, indent=2, default=str)}
"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    
    return json.loads(response.choices[0].message.content)

# --------------------
# ORCHESTRATION
# --------------------
def screen_stock(ticker: str) -> Optional[dict]:
    """Run complete multi-agent analysis on a stock."""
    print(f"\n{'='*60}")
    print(f"Screening: {ticker}")
    print(f"{'='*60}")
    
    # Step 1: Data collection
    print(f"[1/4] Fetching fundamentals...")
    fundamentals = get_fundamentals(ticker)
    if not fundamentals:
        print(f"✗ Failed to fetch data for {ticker}")
        return None
    
    # Step 2: Valuation analysis
    print(f"[2/4] Analyzing valuation...")
    valuation = analyze_valuation(fundamentals)
    print(f"  Valuation Score: {valuation['valuation_score']}/10 ({valuation['valuation_assessment']})")
    
    # Step 3: Growth analysis
    print(f"[3/4] Analyzing growth...")
    growth = analyze_growth(fundamentals)
    print(f"  Growth Score: {growth['growth_score']}/10 ({growth['growth_assessment']})")
    
    # Step 4: Earnings quality analysis
    print(f"[4/4] Analyzing earnings quality...")
    earnings = analyze_earnings_quality(fundamentals)
    print(f"  Beat Probability: {earnings['beat_probability']}")
    
    # Step 5: Synthesize recommendation
    print(f"[5/5] Synthesizing recommendation...")
    recommendation = synthesize_recommendation(fundamentals, valuation, growth, earnings)
    print(f"  Overall Score: {recommendation['overall_score']}/10")
    print(f"  Recommendation: {recommendation['recommendation'].upper()}")
    
    # Combine results
    result = {
        **fundamentals,
        **valuation,
        **growth,
        **earnings,
        **recommendation,
        "screening_date": datetime.date.today().isoformat()
    }
    
    return result

# --------------------
# MAIN
# --------------------
def main():
    if not os.path.exists(TICKERS_FILE):
        print(f"Error: {TICKERS_FILE} not found. Please create it with one ticker per line.")
        return
    
    with open(TICKERS_FILE, "r") as f:
        tickers = [line.strip() for line in f if line.strip()]
    
    if not tickers:
        print(f"Error: No tickers found in {TICKERS_FILE}")
        return
    
    results = []
    for ticker in tickers:
        try:
            result = screen_stock(ticker)
            if result:
                results.append(result)
        except Exception as e:
            print(f"✗ Error processing {ticker}: {e}")
    
    if results:
        # Sort by overall_score descending
        results.sort(key=lambda x: x.get("overall_score", 0), reverse=True)
        
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n{'='*60}")
        print(f"✓ Screening complete! Results saved to {OUTPUT_FILE}")
        print(f"✓ Analyzed {len(results)} stocks")
        print(f"{'='*60}")
    else:
        print("No results to output")

if __name__ == "__main__":
    main()
