"""Read-only diagnostics for a blocked Kalshi live execution session.

This module has no write-authorized broker and never claims or mutates the
execution lease. It reports only bounded operational metadata, not credentials,
raw account payloads, client order IDs, or exchange response bodies.
"""
import argparse
import json

from .kalshi_execution import Config, KalshiExecution
from .kalshi_live_state import LiveJournal


def safe_order(order):
    if not order:
        return None
    return {'status': order.get('status'), 'ticker': order.get('ticker'),
            'book_side': order.get('book_side'), 'outcome_side': order.get('outcome_side'),
            'fill_count': order.get('fill_count_fp'),
            'remaining_count': order.get('remaining_count_fp')}


def run(subaccount):
    config = Config(subaccount=subaccount)
    broker = KalshiExecution(config, requested_live=False)
    # live=True selects the existing live journal namespace. No claim/write is made.
    journal = LiveJournal(config, broker.key_id, 'read-only-diagnostic', live=True)
    try:
        state = journal.state()
        active = state.get('active')
        orders, positions = broker.account()
        result = {'event': 'read_only_execution_diagnostic',
            'writes_authorized': broker.enabled, 'journal': journal.public_summary(),
            'resting_order_count': len(orders),
            'open_position_count': sum(1 for p in positions if p.get('position_fp') not in (None, '0', '0.00'))}
        if active:
            acknowledgement = active.get('acknowledgement') or {}
            order = broker.find_order(active['intent'], acknowledgement.get('order_id'))
            fills = broker.pages('/portfolio/fills', 'fills', ticker=active['ticker'],
                exchange_index=active['intent']['exchange_index'])
            result.update(active_order_found=order is not None,
                active_order=safe_order(order), active_ticker_fill_records=len(fills))
        print(json.dumps(result, sort_keys=True), flush=True)
    finally:
        broker.client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--subaccount', type=int, default=0)
    args = parser.parse_args()
    if not 0 <= args.subaccount <= 63:
        parser.error('Subaccount must be 0..63')
    run(args.subaccount)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Never log raw exchange response bodies, request headers, or secrets.
        code = str(exc)
        print(json.dumps({'event': 'diagnostic_failed', 'error_type': type(exc).__name__,
            'code': code if code.isupper() and len(code) <= 80 else 'DIAGNOSTIC_FAILED'}), flush=True)
        raise SystemExit(1) from None
