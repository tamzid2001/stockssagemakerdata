"""Proof-gated recovery of one stale, unacknowledged Kalshi intent.

This is intentionally not a retry path. The broker is read-only. Recovery is
permitted only after the market closed and Kalshi's account read model has
advanced beyond both the intent and close, with no matching order, fill, open
position, or resting order.
"""
import argparse
import json
import os
import time

from .engine import stamp
from .kalshi_execution import Config, KalshiExecution, money
from .kalshi_live_state import LiveJournal

CONFIRMATION = 'resolve-no-order'
RECOVERY_CODE = 'LEGACY_V2_REQUEST_REJECTED_NO_EXCHANGE_RECORD'


def recover(subaccount, ticker, confirmation):
    if confirmation != CONFIRMATION:
        raise RuntimeError('RECOVERY_CONFIRMATION_REQUIRED')
    config = Config(subaccount=subaccount)
    broker = KalshiExecution(config, requested_live=False)
    journal = LiveJournal(config, broker.key_id,
        os.environ.get('GITHUB_RUN_ID', 'manual-recovery'), live=True)
    claimed = False
    try:
        journal.claim()
        claimed = True
        state = journal.state()
        active = state.get('active')
        if (not active or active.get('ticker') != ticker or active.get('acknowledgement')
                or active.get('status') != 'delivery_unknown'):
            raise RuntimeError('STALE_INTENT_NOT_ELIGIBLE')
        market = broker.market(ticker)
        close_at = stamp(market['close_time'])
        now = time.time()
        if now < close_at + 120 or now < active['created_at'] + 300:
            raise RuntimeError('STALE_INTENT_WAIT_REQUIRED')
        user_as_of = stamp(broker.request('GET', '/exchange/user_data_timestamp')['as_of_time'])
        if user_as_of < max(close_at + 60, active['created_at'] + 180):
            raise RuntimeError('ACCOUNT_READ_MODEL_NOT_CAUGHT_UP')
        order = broker.find_order(active['intent'])
        fills = broker.pages('/portfolio/fills', 'fills', ticker=ticker,
            exchange_index=active['intent']['exchange_index'])
        orders, positions = broker.account()
        exposed = [p for p in positions if money(p.get('position_fp', '0')) != 0]
        if order is not None or fills or orders or exposed:
            raise RuntimeError('EXCHANGE_EXPOSURE_PRESENT')
        proof = {'market_close_at': close_at, 'user_data_as_of': user_as_of,
            'checked_at': now, 'matching_orders': 0, 'fills': 0,
            'resting_orders': 0, 'open_positions': 0}
        journal.finish({**active, 'status': 'rejected', 'rejection_code': RECOVERY_CODE,
                        'recovery_proof': proof})
        print(json.dumps({'event': 'stale_intent_resolved', 'ticker': ticker,
            'code': RECOVERY_CODE, **journal.public_summary()}, sort_keys=True), flush=True)
    finally:
        if claimed:
            journal.release()
        broker.client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--subaccount', type=int, default=0)
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--confirmation', required=True)
    args = parser.parse_args()
    if not 0 <= args.subaccount <= 63:
        parser.error('Subaccount must be 0..63')
    recover(args.subaccount, args.ticker, args.confirmation)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        code = str(exc)
        print(json.dumps({'event': 'recovery_failed', 'error_type': type(exc).__name__,
            'code': code if code.isupper() and len(code) <= 100 else 'RECOVERY_FAILED'}), flush=True)
        raise SystemExit(1) from None
