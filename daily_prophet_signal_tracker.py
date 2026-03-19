#!/usr/bin/env python3
import json
import logging
import os
import time
import warnings
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd
import yfinance as yf
from prophet import Prophet
from zoneinfo import ZoneInfo


warnings.filterwarnings("ignore")
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
logging.getLogger("prophet").setLevel(logging.ERROR)


NY_TZ = ZoneInfo("America/New_York")

DEFAULT_START_DATE = "2020-01-01"
DEFAULT_SLEEP_BETWEEN_TICKERS = 0.05
DEFAULT_CHECKPOINT_EVERY = 25
DEFAULT_CHANGEPOINT_PRIOR_SCALE = 0.05
DEFAULT_SEASONALITY_PRIOR_SCALE = 10.0
DEFAULT_UNCERTAINTY_SAMPLES = 400
DEFAULT_MCMC_SAMPLES = 0
DEFAULT_UNIVERSE = "both"
DEFAULT_OUTPUT_DIR = "artifacts"
DEFAULT_STATE_DIR = "state"
DEFAULT_MIN_MARKET_CAP = 100_000_000_000.0


def ensure_dir(path: str) -> Path:
    output = Path(path)
    output.mkdir(parents=True, exist_ok=True)
    return output


def now_ny() -> pd.Timestamp:
    return pd.Timestamp.now(tz=NY_TZ)


def fmt_date(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(NY_TZ).tz_localize(None)
    return ts.strftime("%Y-%m-%d")


def fmt_datetime(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(NY_TZ)
    else:
        ts = ts.tz_convert(NY_TZ)
    return ts.strftime("%Y-%m-%d %H:%M:%S %Z")


def normalize_yf_symbol(symbol: str) -> str:
    return str(symbol).strip().upper().replace(".", "-")


def fmt_market_cap(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    value = float(value)
    abs_value = abs(value)
    if abs_value >= 1e12:
        return f"${value / 1e12:.2f}T"
    if abs_value >= 1e9:
        return f"${value / 1e9:.2f}B"
    if abs_value >= 1e6:
        return f"${value / 1e6:.2f}M"
    return f"${value:,.0f}"


def fmt_period_end(period_end) -> Optional[str]:
    if period_end is None or pd.isna(period_end):
        return None
    ts = pd.Timestamp(period_end)
    return f"{ts.strftime('%b %Y')} period end ({ts.strftime('%Y-%m-%d')})"


def safe_float(value) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def to_naive_timestamp(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return pd.NaT
    return ts.tz_convert(None)


def get_sp500_tickers() -> List[str]:
    import requests
    from io import StringIO

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    html = response.text

    try:
        tables = pd.read_html(StringIO(html), attrs={"id": "constituents"})
    except Exception:
        tables = []

    if not tables:
        tables = pd.read_html(StringIO(html))

    df = tables[0].copy()
    symbol_col = "Symbol" if "Symbol" in df.columns else df.columns[0]
    return sorted({normalize_yf_symbol(x) for x in df[symbol_col].dropna().astype(str)})


def get_nasdaq_tickers() -> List[str]:
    df = pd.read_csv(
        "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
        sep="|",
    )
    df = df[df["Symbol"] != "File Creation Time"].copy()
    df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper()
    df = df[(df["Test Issue"] == "N") & (df["ETF"] == "N")].copy()

    bad_name_pattern = r"(?i)\b(unit|units|right|rights|warrant|warrants|preferred|depositary|notes)\b"
    df = df[~df["Security Name"].astype(str).str.contains(bad_name_pattern, regex=True, na=False)].copy()

    return sorted({normalize_yf_symbol(x) for x in df["Symbol"].tolist() if x})


def build_universe(universe: str, max_tickers: Optional[int] = None):
    sp500 = get_sp500_tickers()
    nasdaq = get_nasdaq_tickers()

    membership = {}
    for ticker in sp500:
        membership.setdefault(ticker, set()).add("sp500")
    for ticker in nasdaq:
        membership.setdefault(ticker, set()).add("nasdaq")

    if universe == "sp500":
        tickers = sp500
    elif universe == "nasdaq":
        tickers = nasdaq
    else:
        tickers = sorted(set(sp500).union(nasdaq))

    if max_tickers is not None:
        tickers = tickers[:max_tickers]

    return tickers, membership, sp500, nasdaq


def get_price_history(ticker: str, start_date: str, min_rows: int, use_adj_close: bool) -> Optional[pd.DataFrame]:
    raw = yf.download(
        ticker,
        start=start_date,
        progress=False,
        auto_adjust=False,
        threads=False,
    )

    if raw is None or raw.empty:
        return None

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    raw = raw.dropna(how="all").copy()
    if raw.empty:
        return None

    price_col = "Adj Close" if use_adj_close and "Adj Close" in raw.columns else "Close"
    if price_col not in raw.columns:
        return None

    px = raw.reset_index()[["Date", price_col]].rename(columns={"Date": "ds", price_col: "y_usd"})
    px["ds"] = pd.to_datetime(px["ds"], errors="coerce")
    px["y_usd"] = pd.to_numeric(px["y_usd"], errors="coerce")
    px = px.dropna().sort_values("ds").reset_index(drop=True)
    px = px[px["y_usd"] > 0].copy()

    if len(px) < min_rows:
        return None

    return px


def _predict_quantile_frame(
    model: Prophet,
    df_dates: pd.DataFrame,
    interval_width: float,
    low_name: str,
    high_name: str,
) -> pd.DataFrame:
    model.interval_width = interval_width
    forecast = model.predict(df_dates)[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    forecast[["yhat", "yhat_lower", "yhat_upper"]] = np.exp(forecast[["yhat", "yhat_lower", "yhat_upper"]])
    return forecast.rename(columns={"yhat_lower": low_name, "yhat_upper": high_name})


def run_prophet_quantiles(
    px: pd.DataFrame,
    changepoint_prior_scale: float,
    seasonality_prior_scale: float,
    uncertainty_samples: int,
    mcmc_samples: int,
) -> Dict[str, Any]:
    df = px[["ds", "y_usd"]].copy()
    df["y"] = np.log(df["y_usd"])

    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=True,
        changepoint_prior_scale=changepoint_prior_scale,
        seasonality_prior_scale=seasonality_prior_scale,
        interval_width=0.80,
        uncertainty_samples=uncertainty_samples,
        mcmc_samples=mcmc_samples,
    )
    model.add_country_holidays(country_name="US")
    model.add_seasonality(name="monthly", period=30.5, fourier_order=5)
    model.fit(df[["ds", "y"]])

    dates = df[["ds"]].copy()
    fc_10_90 = _predict_quantile_frame(model, dates, 0.80, "p10", "p90")[["ds", "yhat", "p10", "p90"]]
    fc_01_99 = _predict_quantile_frame(model, dates, 0.98, "p1", "p99")[["ds", "p1", "p99"]]

    out = (
        fc_10_90
        .merge(fc_01_99, on="ds", how="left")
        .merge(px.rename(columns={"y_usd": "actual"}), on="ds", how="left")
        .sort_values("ds")
        .reset_index(drop=True)
    )
    out["p50"] = out["yhat"]

    last_row = out.iloc[-1]
    last_actual = float(last_row["actual"])
    p1 = float(last_row["p1"])
    p10 = float(last_row["p10"])
    p50 = float(last_row["p50"])
    p90 = float(last_row["p90"])
    p99 = float(last_row["p99"])

    status = None
    gap_to_band_pct = None
    central_delta = None
    tail_delta = None
    central_delta_label = None
    tail_delta_label = None

    if last_actual < p10:
        status = "below_p10"
        gap_to_band_pct = (p10 / last_actual - 1.0) * 100.0
        central_delta = p50 - p10
        tail_delta = p99 - p10
        central_delta_label = "p50_minus_p10"
        tail_delta_label = "p99_minus_p10"
    elif last_actual > p90:
        status = "above_p90"
        gap_to_band_pct = (last_actual / p90 - 1.0) * 100.0
        central_delta = p90 - p50
        tail_delta = p90 - p1
        central_delta_label = "p90_minus_p50"
        tail_delta_label = "p90_minus_p1"

    return {
        "last_date": pd.Timestamp(last_row["ds"]),
        "last_price": last_actual,
        "p1": p1,
        "p10": p10,
        "p50": p50,
        "p90": p90,
        "p99": p99,
        "status": status,
        "gap_to_band_pct": gap_to_band_pct,
        "central_delta": central_delta,
        "tail_delta": tail_delta,
        "central_delta_label": central_delta_label,
        "tail_delta_label": tail_delta_label,
    }


def passes_delta_filters(
    status: Optional[str],
    central_delta: Optional[float],
    tail_delta: Optional[float],
    min_central_delta: Optional[float],
    min_tail_delta: Optional[float],
) -> bool:
    if status not in {"below_p10", "above_p90"}:
        return False

    if min_central_delta is not None and (central_delta is None or central_delta < min_central_delta):
        return False

    if min_tail_delta is not None and (tail_delta is None or tail_delta < min_tail_delta):
        return False

    return True


def get_statement_periods(tk: yf.Ticker) -> List[pd.Timestamp]:
    for attr in ["quarterly_income_stmt", "quarterly_balance_sheet", "quarterly_cashflow"]:
        try:
            obj = getattr(tk, attr)
            if isinstance(obj, pd.DataFrame) and obj.shape[1] > 0:
                cols = [to_naive_timestamp(c) for c in obj.columns]
                cols = [pd.Timestamp(c).normalize() for c in cols if pd.notna(c)]
                if cols:
                    return sorted(set(cols))
        except Exception:
            pass
    return []


def infer_last_report_period(last_earnings_date, periods: List[pd.Timestamp]) -> Optional[pd.Timestamp]:
    if last_earnings_date is None or pd.isna(last_earnings_date) or not periods:
        return None
    last_earnings_date = pd.Timestamp(last_earnings_date).normalize()
    candidates = [period for period in periods if period <= last_earnings_date]
    if not candidates:
        return None
    best = max(candidates)
    if (last_earnings_date - best).days > 220:
        return None
    return best


def infer_next_report_period(next_earnings_date, periods: List[pd.Timestamp]) -> Optional[pd.Timestamp]:
    if next_earnings_date is None or pd.isna(next_earnings_date) or not periods:
        return None

    next_earnings_date = pd.Timestamp(next_earnings_date).normalize()
    current = max(periods)
    chosen = None

    for _ in range(8):
        current = (current + pd.DateOffset(months=3)).normalize()
        if current <= next_earnings_date:
            chosen = current
        else:
            break

    if chosen is None:
        chosen = (max(periods) + pd.DateOffset(months=3)).normalize()

    return chosen


def get_recent_earnings(tk: yf.Ticker, periods: List[pd.Timestamp]) -> Dict[str, Any]:
    out = {
        "last_earnings_date": None,
        "last_report_period_end": None,
        "last_eps_estimate": None,
        "last_reported_eps": None,
        "last_surprise_pct": None,
    }

    try:
        earnings_dates = tk.get_earnings_dates(limit=12)
    except Exception:
        earnings_dates = None

    try:
        if earnings_dates is None or len(earnings_dates) == 0:
            return out

        temp = earnings_dates.reset_index().copy()
        date_col = temp.columns[0]
        temp["earnings_dt"] = pd.to_datetime(temp[date_col], errors="coerce", utc=True).dt.tz_convert(None)
        temp = temp[temp["earnings_dt"].notna()].copy()
        temp = temp[temp["earnings_dt"] <= pd.Timestamp.utcnow().tz_localize(None)].copy()
        if temp.empty:
            return out

        temp = temp.sort_values("earnings_dt")
        row = temp.iloc[-1]

        last_dt = pd.Timestamp(row["earnings_dt"]).normalize()
        out["last_earnings_date"] = last_dt
        out["last_report_period_end"] = infer_last_report_period(last_dt, periods)

        for src, dst in [
            ("EPS Estimate", "last_eps_estimate"),
            ("Reported EPS", "last_reported_eps"),
            ("Surprise(%)", "last_surprise_pct"),
        ]:
            if src in temp.columns:
                out[dst] = safe_float(row.get(src))

        return out
    except Exception:
        return out


def get_upcoming_earnings(tk: yf.Ticker, periods: List[pd.Timestamp]) -> Dict[str, Any]:
    out = {
        "next_earnings_date": None,
        "next_report_period_end": None,
    }

    try:
        cal = tk.calendar
        values = []

        if isinstance(cal, dict):
            for key, value in cal.items():
                lowered = str(key).lower()
                if "earn" in lowered and "date" in lowered:
                    if isinstance(value, (list, tuple, pd.Series, pd.Index, np.ndarray)):
                        values.extend(list(value))
                    else:
                        values.append(value)
        elif isinstance(cal, pd.DataFrame):
            for column in cal.columns:
                lowered = str(column).lower()
                if "earn" in lowered and "date" in lowered:
                    values.extend(cal[column].tolist())

            if not values and "Value" in cal.columns:
                idx = pd.Index(cal.index.astype(str).str.lower())
                mask = idx.str.contains("earn") & idx.str.contains("date")
                if mask.any():
                    value = cal.loc[mask, "Value"].iloc[0]
                    if isinstance(value, (list, tuple, pd.Series, pd.Index, np.ndarray)):
                        values.extend(list(value))
                    else:
                        values.append(value)

        cleaned = []
        today_utc_naive = pd.Timestamp.utcnow().tz_localize(None).normalize()
        for value in values:
            dt = to_naive_timestamp(value)
            if pd.notna(dt):
                cleaned.append(pd.Timestamp(dt).normalize())

        future_dates = sorted({dt for dt in cleaned if dt >= today_utc_naive - pd.Timedelta(days=2)})
        if future_dates:
            next_dt = future_dates[0]
            out["next_earnings_date"] = next_dt
            out["next_report_period_end"] = infer_next_report_period(next_dt, periods)

        return out
    except Exception:
        return out


def get_market_cap(tk: yf.Ticker, last_price: Optional[float] = None) -> Optional[float]:
    try:
        fast_info = tk.fast_info
        if hasattr(fast_info, "get"):
            for key in ["marketCap", "market_cap"]:
                value = fast_info.get(key)
                if value is not None and not pd.isna(value):
                    return float(value)
    except Exception:
        pass

    try:
        info = tk.info
        if isinstance(info, dict):
            value = info.get("marketCap")
            if value is not None and not pd.isna(value):
                return float(value)
    except Exception:
        pass

    try:
        if last_price is not None:
            shares = tk.get_shares_full()
            if shares is not None and len(shares.dropna()) > 0:
                latest_shares = float(shares.dropna().iloc[-1])
                return latest_shares * float(last_price)
    except Exception:
        pass

    return None


def load_prior_active(state_dir: Path) -> pd.DataFrame:
    fp = state_dir / "active_signals.csv"
    if fp.exists():
        try:
            return pd.read_csv(fp)
        except Exception:
            pass
    return pd.DataFrame()


def summarize_transition(prev_row: pd.Series, curr_row: Optional[pd.Series], run_ts: str) -> Dict[str, Any]:
    status = str(prev_row["status"])

    if status == "below_p10":
        event_type = "moved_back_above_p10"
    elif status == "above_p90":
        event_type = "moved_back_below_p90"
    else:
        event_type = "resolved"

    out = {
        "event_time_ny": run_ts,
        "ticker": prev_row.get("ticker"),
        "prior_status": prev_row.get("status"),
        "event_type": event_type,
        "prior_last_price": prev_row.get("last_price"),
        "prior_p1": prev_row.get("p1"),
        "prior_p10": prev_row.get("p10"),
        "prior_p50": prev_row.get("p50"),
        "prior_p90": prev_row.get("p90"),
        "prior_p99": prev_row.get("p99"),
        "prior_central_delta": prev_row.get("central_delta"),
        "prior_tail_delta": prev_row.get("tail_delta"),
        "new_last_price": None,
        "new_p1": None,
        "new_p10": None,
        "new_p50": None,
        "new_p90": None,
        "new_p99": None,
        "new_central_delta": None,
        "new_tail_delta": None,
        "new_status": None,
        "details": None,
    }

    if curr_row is not None:
        out["new_last_price"] = curr_row.get("last_price")
        out["new_p1"] = curr_row.get("p1")
        out["new_p10"] = curr_row.get("p10")
        out["new_p50"] = curr_row.get("p50")
        out["new_p90"] = curr_row.get("p90")
        out["new_p99"] = curr_row.get("p99")
        out["new_central_delta"] = curr_row.get("central_delta")
        out["new_tail_delta"] = curr_row.get("tail_delta")
        out["new_status"] = curr_row.get("status")

        if event_type == "moved_back_above_p10":
            out["details"] = f"{curr_row.get('ticker')} is now back above p10."
        elif event_type == "moved_back_below_p90":
            out["details"] = f"{curr_row.get('ticker')} is now back below p90."
        else:
            out["details"] = f"{curr_row.get('ticker')} changed status."
    else:
        out["details"] = f"{prev_row.get('ticker')} is no longer in the active breach set."

    return out


def compute_transitions(prev_active: pd.DataFrame, curr_all: pd.DataFrame, run_ts: str) -> pd.DataFrame:
    if prev_active is None or prev_active.empty:
        return pd.DataFrame()

    curr_all_by_ticker = {}
    if curr_all is not None and not curr_all.empty:
        for _, row in curr_all.iterrows():
            curr_all_by_ticker[str(row["ticker"])] = row

    transitions = []
    for _, prev_row in prev_active.iterrows():
        ticker = str(prev_row["ticker"])
        curr_row = curr_all_by_ticker.get(ticker)

        if curr_row is None:
            transitions.append(summarize_transition(prev_row, None, run_ts))
            continue

        prev_status = str(prev_row.get("status"))
        curr_status = curr_row.get("status")

        if prev_status == "below_p10" and curr_status != "below_p10":
            transitions.append(summarize_transition(prev_row, curr_row, run_ts))
        elif prev_status == "above_p90" and curr_status != "above_p90":
            transitions.append(summarize_transition(prev_row, curr_row, run_ts))

    if not transitions:
        return pd.DataFrame()

    return pd.DataFrame(transitions)


def print_filtered_signals(df: pd.DataFrame):
    if df.empty:
        print("No active tickers below p10 or above p90 after delta filters.")
        return

    print("\n=== ACTIVE FILTERED TICKERS ===")
    for _, row in df.iterrows():
        print(
            f"{row['ticker']:>6} | {row['status']:<9} | "
            f"px={row['last_price']:.2f} | "
            f"p1={row['p1']:.2f} | p10={row['p10']:.2f} | p50={row['p50']:.2f} | "
            f"p90={row['p90']:.2f} | p99={row['p99']:.2f} | "
            f"{row['central_delta_label']}={row['central_delta']:.2f} | "
            f"{row['tail_delta_label']}={row['tail_delta']:.2f} | "
            f"mcap={row['market_cap_fmt']} | "
            f"last ER={row['last_earnings_date']} ({row['last_report_period']}) | "
            f"next ER={row['next_earnings_date']} ({row['next_report_period']}) | "
            f"universe={row['universe']}"
        )


def write_step_summary(
    curr_signals: pd.DataFrame,
    transitions: pd.DataFrame,
    universe_name: str,
    run_ts: str,
    min_market_cap: float,
    screened_count: int,
):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    def render_table(df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return "None"
        return "```text\n" + df.to_string(index=False) + "\n```"

    lines = [
        "# Daily Prophet Signal Tracker",
        "",
        f"- Run time (New York): {run_ts}",
        f"- Universe: {universe_name}",
        f"- Minimum market cap: {fmt_market_cap(min_market_cap)}",
        f"- Screened tickers above market-cap floor: {screened_count}",
        f"- Active breaches after delta filters: {len(curr_signals)}",
        f"- Resolved transitions: {0 if transitions is None else len(transitions)}",
        "",
    ]

    if curr_signals is not None and not curr_signals.empty:
        lines.extend(["## Active filtered tickers", ""])
        show_cols = [
            "ticker",
            "universe",
            "status",
            "last_price",
            "p1",
            "p10",
            "p50",
            "p90",
            "p99",
            "central_delta_label",
            "central_delta",
            "tail_delta_label",
            "tail_delta",
            "market_cap_fmt",
            "last_earnings_date",
            "next_earnings_date",
        ]
        lines.append(render_table(curr_signals[show_cols].head(200)))
        lines.append("")
    else:
        lines.extend(["## Active filtered tickers", "", "None", ""])

    if transitions is not None and not transitions.empty:
        lines.extend(["## Resolved transitions", ""])
        show_cols = [
            "ticker",
            "prior_status",
            "event_type",
            "prior_last_price",
            "new_last_price",
            "prior_p10",
            "new_p10",
            "prior_p90",
            "new_p90",
        ]
        lines.append(render_table(transitions[show_cols].head(200)))
        lines.append("")

    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def run(args):
    run_time_ny = now_ny()
    run_ts = fmt_datetime(run_time_ny)

    if args.enforce_5pm_newyork_window and run_time_ny.hour != 17:
        print(f"Skipping run. Current New York time is {run_ts}, not in the 5 PM ET window.")
        return

    out_dir = ensure_dir(args.output_dir)
    state_dir = ensure_dir(args.state_dir)

    tickers, membership, sp500, nasdaq = build_universe(args.universe, args.max_tickers)

    print(f"Run time (New York): {run_ts}")
    print(f"S&P 500 tickers: {len(sp500)}")
    print(f"Nasdaq tickers:  {len(nasdaq)}")
    print(f"Universe:        {args.universe}")
    print(f"Run size:        {len(tickers)}")
    print(f"Minimum market cap:      {fmt_market_cap(args.min_market_cap)}")
    print(f"Min central delta filter: {args.min_central_delta}")
    print(f"Min tail delta filter:    {args.min_tail_delta}")

    active_signals = []
    all_latest_rows = []
    errors = []
    market_cap_skipped = 0
    screened_count = 0

    for index, ticker in enumerate(tickers, start=1):
        try:
            tk = yf.Ticker(ticker)
            market_cap = get_market_cap(tk)
            if market_cap is not None and market_cap < args.min_market_cap:
                market_cap_skipped += 1
                continue

            px = get_price_history(
                ticker=ticker,
                start_date=args.start_date,
                min_rows=args.min_rows,
                use_adj_close=args.use_adj_close,
            )
            if px is None or px.empty:
                continue

            if market_cap is None:
                market_cap = get_market_cap(tk, last_price=float(px["y_usd"].iloc[-1]))
            if market_cap is None or market_cap < args.min_market_cap:
                market_cap_skipped += 1
                continue
            screened_count += 1

            q = run_prophet_quantiles(
                px=px,
                changepoint_prior_scale=args.changepoint_prior_scale,
                seasonality_prior_scale=args.seasonality_prior_scale,
                uncertainty_samples=args.uncertainty_samples,
                mcmc_samples=args.mcmc_samples,
            )

            periods = get_statement_periods(tk)
            recent_er = get_recent_earnings(tk, periods)
            next_er = get_upcoming_earnings(tk, periods)

            row = {
                "run_time_ny": run_ts,
                "ticker": ticker,
                "universe": ",".join(sorted(membership.get(ticker, []))),
                "status": q["status"],
                "last_date": fmt_date(q["last_date"]),
                "last_price": round(q["last_price"], 6),
                "gap_to_band_pct": round(q["gap_to_band_pct"], 6) if q["gap_to_band_pct"] is not None else None,
                "p1": round(q["p1"], 6),
                "p10": round(q["p10"], 6),
                "p50": round(q["p50"], 6),
                "p90": round(q["p90"], 6),
                "p99": round(q["p99"], 6),
                "central_delta_label": q["central_delta_label"],
                "central_delta": round(q["central_delta"], 6) if q["central_delta"] is not None else None,
                "tail_delta_label": q["tail_delta_label"],
                "tail_delta": round(q["tail_delta"], 6) if q["tail_delta"] is not None else None,
                "market_cap": market_cap,
                "market_cap_fmt": fmt_market_cap(market_cap),
                "last_earnings_date": fmt_date(recent_er["last_earnings_date"]),
                "last_report_period": fmt_period_end(recent_er["last_report_period_end"]),
                "last_eps_estimate": recent_er["last_eps_estimate"],
                "last_reported_eps": recent_er["last_reported_eps"],
                "last_surprise_pct": recent_er["last_surprise_pct"],
                "next_earnings_date": fmt_date(next_er["next_earnings_date"]),
                "next_report_period": fmt_period_end(next_er["next_report_period_end"]),
            }

            all_latest_rows.append(row)

            if passes_delta_filters(
                status=q["status"],
                central_delta=q["central_delta"],
                tail_delta=q["tail_delta"],
                min_central_delta=args.min_central_delta,
                min_tail_delta=args.min_tail_delta,
            ):
                active_signals.append(row)
                print(
                    f"{ticker:>6} | {q['status']:<9} | "
                    f"px={q['last_price']:.2f} | p1={q['p1']:.2f} | p10={q['p10']:.2f} | "
                    f"p50={q['p50']:.2f} | p90={q['p90']:.2f} | p99={q['p99']:.2f} | "
                    f"{q['central_delta_label']}={q['central_delta']:.2f} | "
                    f"{q['tail_delta_label']}={q['tail_delta']:.2f}"
                )
        except Exception as exc:
            errors.append({"ticker": ticker, "error": str(exc)[:500]})

        if args.sleep_between_tickers > 0:
            time.sleep(args.sleep_between_tickers)

        if index % args.checkpoint_every == 0:
            pd.DataFrame(active_signals).to_csv(out_dir / "active_signals_checkpoint.csv", index=False)
            pd.DataFrame(errors).to_csv(out_dir / "errors_checkpoint.csv", index=False)

    curr_active = pd.DataFrame(active_signals)
    curr_all = pd.DataFrame(all_latest_rows)
    prev_active = load_prior_active(state_dir)
    transitions = compute_transitions(prev_active, curr_all, run_ts)

    if not curr_active.empty:
        sort_map = {"below_p10": 0, "above_p90": 1}
        curr_active["status_order"] = curr_active["status"].map(sort_map).fillna(99)
        curr_active = (
            curr_active.sort_values(["status_order", "gap_to_band_pct"], ascending=[True, False])
            .drop(columns="status_order")
            .reset_index(drop=True)
        )

    if not curr_all.empty:
        curr_all = curr_all.sort_values(["ticker"]).reset_index(drop=True)

    if transitions is not None and not transitions.empty:
        transitions = transitions.sort_values(["event_time_ny", "ticker"]).reset_index(drop=True)

    print_filtered_signals(curr_active)

    if transitions is not None and not transitions.empty:
        print("\n=== RESOLVED TRANSITIONS ===")
        for _, row in transitions.iterrows():
            print(
                f"{row['ticker']:>6} | {row['event_type']} | "
                f"prior_status={row['prior_status']} | "
                f"prior_px={row['prior_last_price']} -> new_px={row['new_last_price']}"
            )
    else:
        print("\nNo resolved transitions this run.")

    curr_active.to_csv(out_dir / "active_signals.csv", index=False)
    curr_all.to_csv(out_dir / "all_latest_rows.csv", index=False)
    pd.DataFrame(errors).to_csv(out_dir / "errors.csv", index=False)

    signal_list = [] if curr_active.empty else curr_active["ticker"].tolist()
    with open(out_dir / "signal_tickers.txt", "w", encoding="utf-8") as handle:
        handle.write("\n".join(signal_list))
    with open(out_dir / "signal_tickers.json", "w", encoding="utf-8") as handle:
        json.dump(signal_list, handle, indent=2)

    transition_history_fp = state_dir / "transition_history.csv"
    if transitions is not None and not transitions.empty:
        if transition_history_fp.exists():
            old_history = pd.read_csv(transition_history_fp)
            new_history = pd.concat([old_history, transitions], ignore_index=True)
        else:
            new_history = transitions.copy()
        new_history.to_csv(transition_history_fp, index=False)
        transitions.to_csv(out_dir / "resolved_transitions.csv", index=False)
    else:
        pd.DataFrame().to_csv(out_dir / "resolved_transitions.csv", index=False)

    curr_active.to_csv(state_dir / "active_signals.csv", index=False)
    snapshot_name = f"active_signals_{run_time_ny.strftime('%Y%m%d')}.csv"
    curr_active.to_csv(state_dir / snapshot_name, index=False)

    write_step_summary(
        curr_signals=curr_active,
        transitions=transitions,
        universe_name=args.universe,
        run_ts=run_ts,
        min_market_cap=args.min_market_cap,
        screened_count=screened_count,
    )

    print("\n=== OUTPUT FILES ===")
    for fp in [
        out_dir / "active_signals.csv",
        out_dir / "all_latest_rows.csv",
        out_dir / "resolved_transitions.csv",
        out_dir / "signal_tickers.txt",
        out_dir / "signal_tickers.json",
        out_dir / "errors.csv",
        state_dir / "active_signals.csv",
        state_dir / "transition_history.csv",
    ]:
        print(f"{fp} | exists = {fp.exists()}")

    print(f"\nSkipped below market-cap floor: {market_cap_skipped}")
    print(f"Screened above market-cap floor: {screened_count}")
    print("\n=== SIGNAL LIST ===")
    print(signal_list)


def parse_args():
    parser = argparse.ArgumentParser(description="Daily Prophet signal tracker for S&P 500 + Nasdaq.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--universe", default=DEFAULT_UNIVERSE, choices=["sp500", "nasdaq", "both"])
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--min-rows", type=int, default=500)
    parser.add_argument("--use-adj-close", action="store_true", default=True)
    parser.add_argument("--no-use-adj-close", dest="use_adj_close", action="store_false")
    parser.add_argument("--changepoint-prior-scale", type=float, default=DEFAULT_CHANGEPOINT_PRIOR_SCALE)
    parser.add_argument("--seasonality-prior-scale", type=float, default=DEFAULT_SEASONALITY_PRIOR_SCALE)
    parser.add_argument("--uncertainty-samples", type=int, default=DEFAULT_UNCERTAINTY_SAMPLES)
    parser.add_argument("--mcmc-samples", type=int, default=DEFAULT_MCMC_SAMPLES)
    parser.add_argument("--sleep-between-tickers", type=float, default=DEFAULT_SLEEP_BETWEEN_TICKERS)
    parser.add_argument("--checkpoint-every", type=int, default=DEFAULT_CHECKPOINT_EVERY)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    parser.add_argument("--min-central-delta", type=float, default=None)
    parser.add_argument("--min-tail-delta", type=float, default=None)
    parser.add_argument("--min-market-cap", type=float, default=DEFAULT_MIN_MARKET_CAP)
    parser.add_argument("--enforce-5pm-newyork-window", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
