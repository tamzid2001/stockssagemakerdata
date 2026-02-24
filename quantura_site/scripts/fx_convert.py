#!/usr/bin/env python3
"""Local CLI for Quantura FX conversion endpoint."""

from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_BASE = os.environ.get("QUANTURA_MARKET_DATA_BASE", "http://127.0.0.1:8090").rstrip("/")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert FX amounts via Quantura market-data service.")
    parser.add_argument("amount", type=float, help="Amount in source currency")
    parser.add_argument("from_currency", type=str, help="From currency (e.g., USD)")
    parser.add_argument("to_currency", type=str, help="To currency (e.g., EUR)")
    parser.add_argument("--base", default=DEFAULT_BASE, help=f"API base URL (default: {DEFAULT_BASE})")
    parser.add_argument("--json", action="store_true", help="Print raw JSON response")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    from_code = args.from_currency.strip().upper()
    to_code = args.to_currency.strip().upper()

    if len(from_code) != 3 or len(to_code) != 3:
        print("Currencies must be 3-letter codes (e.g., USD EUR).", file=sys.stderr)
        return 2

    query = urlencode(
        {
            "amount": args.amount,
            "from": from_code,
            "to": to_code,
        }
    )
    url = f"{args.base.rstrip('/')}/fx/convert?{query}"

    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        print(f"FX conversion request failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    amount_in = payload.get("amountIn")
    amount_out = payload.get("amountOut")
    rate = payload.get("rate")
    symbol = payload.get("symbolUsed")
    as_of = payload.get("asOf")

    print(f"{amount_in} {from_code} -> {amount_out} {to_code}")
    print(f"Rate: {rate}")
    print(f"Symbol: {symbol}")
    print(f"As of: {as_of}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
