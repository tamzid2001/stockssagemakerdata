"""Read-only, bounded audit of independently journaled Kalshi coin sizing.

No order-write approval is passed to this process. Never emit credentials,
client order IDs, raw exchange responses, or full Firestore documents.
"""

import argparse
import json
import time

from .kalshi_execution import Config, CoinConfig, KalshiExecution
from .kalshi_live_state import LiveJournal
from .kalshi_shared_account import SERIES


def safe_trade(row):
    intent = row.get('intent') or {}
    return {'ticker': row.get('ticker'), 'created_at': row.get('created_at'),
        'status': row.get('status'),
        'requested': row.get('target_contracts', intent.get('count')),
        'latest_order_requested': intent.get('count'),
        'filled': row.get('filled'), 'attempt': row.get('attempt'),
        'net_pnl': row.get('net_pnl')}


def safe_fill(row):
    """Only exchange execution facts; never disclose private order identifiers."""
    return {'ticker': row.get('ticker'), 'created_time': row.get('created_time'),
        'action': row.get('action'), 'outcome_side': row.get('outcome_side'),
        'book_side': row.get('book_side'), 'count': row.get('count_fp'),
        'yes_price': row.get('yes_price_dollars'), 'no_price': row.get('no_price_dollars')}


def inspect(subaccount, limit, lookback_hours):
    broker = KalshiExecution(Config(subaccount=subaccount), requested_live=False)
    try:
        orders, positions = broker.account()
        recent_fills = broker.pages('/portfolio/fills', 'fills',
            min_ts=int(time.time()) - lookback_hours * 3600)
        print(json.dumps({'event': 'account_read_only', 'writes_authorized': broker.enabled,
            'resting_orders': len(orders), 'open_positions': len(positions),
            'recent_exchange_fill_records': len(recent_fills)}), flush=True)
        for series in SERIES:
            config = (Config(subaccount=subaccount) if series == 'KXBTC15M'
                else CoinConfig(series, subaccount=subaccount))
            journal = LiveJournal(config, broker.key_id, 'read-only-coin-audit', live=True)
            root = journal.ref.get().to_dict() or {}
            state = root.get('state') or {}
            saved_config = root.get('configuration') or {}
            active = state.get('active') or {}
            rows = journal.ref.collection('markets').order_by('created_at',
                direction=journal.fs.Query.DESCENDING).limit(limit).stream()
            trades = [safe_trade(snapshot.to_dict() or {}) for snapshot in rows]
            fills = [safe_fill(row) for row in recent_fills
                if str(row.get('ticker', '')).startswith(series + '-')]
            print(json.dumps({'event': 'series_recovery_audit', 'series': series,
                'session_exists': bool(root), 'configured_start': saved_config.get('starting_contracts'),
                'configured_multiplier': saved_config.get('recovery_multiplier'),
                'configured_max_increases': saved_config.get('max_recovery_increases'),
                'next_contracts': state.get('size'), 'recovery_increases': state.get('recovery_increases'),
                'recovery_cycle_pnl': state.get('cycle'),
                'stop_triggers': (state.get('stats') or {}).get('stop_triggers', 0),
                'stop_attempts': (state.get('stats') or {}).get('stop_attempts', 0),
                'stopped': (state.get('stats') or {}).get('stopped', 0),
                'active': {'ticker': active.get('ticker'), 'requested':
                    (active.get('intent') or {}).get('count')} if active else None,
                'trades_newest_first': trades, 'recent_exchange_fills': fills[-limit:]}), flush=True)
    finally:
        broker.client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--subaccount', type=int, default=0)
    parser.add_argument('--limit', type=int, default=25)
    parser.add_argument('--lookback-hours', type=int, default=4)
    args = parser.parse_args()
    if (not 0 <= args.subaccount <= 63 or not 1 <= args.limit <= 50
            or not 1 <= args.lookback_hours <= 24):
        parser.error('Subaccount must be 0..63, limit 1..50, and lookback 1..24 hours')
    inspect(args.subaccount, args.limit, args.lookback_hours)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        code = str(exc)
        print(json.dumps({'event': 'coin_recovery_audit_failed',
            'code': code if code.isupper() and len(code) <= 80 else 'READ_ONLY_AUDIT_FAILED'}), flush=True)
        raise SystemExit(1) from None
