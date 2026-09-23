"""Settle an already-filled v2 live position without authorizing any order POST.

This bridge preserves the exact legacy journal configuration until the open
position's official exchange settlement is accounted for. A v3 configuration
may subsequently be adopted only at a flat, zero-P&L recovery boundary.
"""
import argparse
from dataclasses import dataclass
import json
import os

from .kalshi_execution import KalshiExecution, money
from .kalshi_live_state import LiveJournal, Trader


CONFIRMATION = 'reconcile-existing-fill'
LEGACY_VERSION = 'btc-p90-sticky-hold-live-v2'


@dataclass(frozen=True)
class LegacyConfig:
    history_minutes: int = 1
    subaccount: int = 0
    direction_policy: str = 'provisional_near_close'
    starting_contracts: int = 1
    recovery_multiplier: str = '2.5'
    max_contracts: int = 100
    max_order_dollars: str = '100'
    daily_loss_dollars: str = '100'
    max_ask: str = '0.99'
    version: str = LEGACY_VERSION

    @property
    def max_recovery_increases(self):
        # Only for finalizing the pre-migration trade. The old policy had no
        # increase count, but was bounded by the 100-contract hard ceiling.
        return 6


def settle_existing_fill(subaccount, ticker, confirmation):
    if confirmation != CONFIRMATION:
        raise RuntimeError('SETTLEMENT_CONFIRMATION_REQUIRED')
    if not 0 <= subaccount <= 63:
        raise ValueError('INVALID_SUBACCOUNT')
    config = LegacyConfig(subaccount=subaccount)
    broker = KalshiExecution(config, requested_live=False)
    journal = LiveJournal(config, broker.key_id,
        os.environ.get('GITHUB_RUN_ID', 'manual-settlement'), live=True)
    claimed = False
    try:
        journal.claim()
        claimed = True
        entry = journal.state().get('active')
        if (not entry or entry.get('ticker') != ticker or not entry.get('acknowledgement')
                or entry.get('status') != 'acknowledged'):
            raise RuntimeError('FILLED_POSITION_NOT_ELIGIBLE')
        order = broker.find_order(entry['intent'], entry['acknowledgement'].get('order_id'))
        if not order:
            raise RuntimeError('ACKNOWLEDGED_ORDER_NOT_VISIBLE')
        from .kalshi_execution import reconciled_order
        fill = reconciled_order(order, entry['intent'], entry['side'])
        if money(fill['filled']) <= 0:
            raise RuntimeError('POSITIVE_FILL_REQUIRED')
        market = broker.market(ticker)
        if market.get('status') not in ('settled', 'finalized') or market.get('result') not in ('yes', 'no'):
            raise RuntimeError('OFFICIAL_SETTLEMENT_PENDING')
        # The broker is read-only, and retry is disabled even if a later read
        # unexpectedly reports zero fill. Trader verifies settlement accounting.
        result = Trader(config, broker, journal).reconcile(allow_order_retry=False)
        if result != 'settled':
            raise RuntimeError('SETTLEMENT_ACCOUNTING_PENDING')
        summary = journal.public_summary()
        print(json.dumps({'event': 'existing_fill_settled', 'ticker': ticker,
            'next_contracts': summary['next_contracts'],
            'recovery_cycle_pnl': summary['recovery_cycle_pnl'],
            'realized_net_pnl': summary['realized_net_pnl']}), flush=True)
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
    settle_existing_fill(args.subaccount, args.ticker, args.confirmation)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        code = str(exc)
        print(json.dumps({'event': 'settlement_reconciliation_failed',
            'error_type': type(exc).__name__,
            'code': code if code.isupper() and len(code) <= 100 else 'SETTLEMENT_FAILED'}), flush=True)
        raise SystemExit(1) from None
