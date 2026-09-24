"""Publish only aggregate first-minute P90/sticky replay statistics by series.

The source is an immutable encrypted interval report. No orders are submitted,
and individual trades or quote paths are never written to Actions artifacts.
"""
import argparse
import json
import os
from pathlib import Path
import re

from .btc_bucket_report import _record, replay_recovery_scenario, streaks
from .interval_archive_replay import SERIES, VERSION
from .recovery_cloud import Campaign


def load(series: str, campaign_id: str, report_key: str) -> dict:
    if series not in SERIES or series == 'KXBTC15M':
        raise ValueError('NON_BTC_INTERVAL_SERIES_REQUIRED')
    if not re.fullmatch(r'p90-[a-f0-9]{24}', campaign_id):
        raise ValueError('INVALID_CAMPAIGN_ID')
    if not re.fullmatch(r'report-[a-f0-9]{32}', report_key):
        raise ValueError('INVALID_REPORT_KEY')
    campaign = Campaign(campaign_id, 'interval-safe-aggregate-reader')
    compact = _record(campaign, report_key)
    if (compact.get('version') != VERSION or not compact.get('complete')
            or compact.get('configuration', {}).get('series') != series
            or compact.get('fee_status') != 'current_schedule_sensitivity'):
        raise ValueError('INCOMPLETE_OR_WRONG_INTERVAL_REPORT')
    origin = compact['origins']['1']['p90_sticky']['fixed_one']
    full = _record(campaign, origin['report_key'])
    trades = full['p90_sticky']['fixed_one']['trades']
    prior = full['p90_sticky']['fixed_one']['summary']
    if (not isinstance(trades, list) or len(trades) != prior['closed_trades']
            or prior['max_quantity_used'] != 1
            or prior['wins'] + prior['losses'] + prior['breakeven'] != len(trades)):
        raise ValueError('INCOMPLETE_OR_NON_UNIT_TRADE_LEDGER')
    policy = compact['fee_policy']
    if policy.get('fee_type') not in ('quadratic', 'quadratic_with_maker_fees') or policy.get('multiplier') != 1:
        raise ValueError('UNVERIFIED_TAKER_FEE_POLICY')
    replay = replay_recovery_scenario(trades, {'fee_type': 'quadratic', 'multiplier': 1},
                                      starting_contracts=1, max_increases=3)
    return {'series': series, 'campaign_id': campaign_id, 'report_key': report_key,
            'as_of': compact['as_of'], 'coverage': compact['coverage'],
            'strategy': 'first_minute_three_model_p90_sticky_next_minute_ask_hold_settlement',
            'history_minutes': 1, 'forecast_minutes': 14,
            'starting_contracts': 1, 'recovery_multiplier': 2.5,
            'max_recovery_increases': 3, 'model_count': 3,
            'replay': replay, 'streaks': streaks(trades),
            'paper_only': True, 'fills_verified': False,
            'limitations': compact.get('limitations', [])}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--series', required=True)
    parser.add_argument('--campaign-id', required=True)
    parser.add_argument('--report-key', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = load(args.series, args.campaign_id, args.report_key)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if args.output.exists():
        raise ValueError('OUTPUT_ALREADY_EXISTS')
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    os.chmod(args.output, 0o600)
    replay = result['replay']
    summary = (f"{args.series}: {replay['wins']}W/{replay['losses']}L, "
               f"net ${replay['net_pnl']:.2f}, realized drawdown "
               f"${replay['realized_equity_max_drawdown']:.2f}; simulated only")
    if path := os.getenv('GITHUB_STEP_SUMMARY'):
        with open(path, 'a', encoding='utf-8') as target:
            target.write(summary + '\n')
    print(summary, flush=True)


if __name__ == '__main__':
    main()
