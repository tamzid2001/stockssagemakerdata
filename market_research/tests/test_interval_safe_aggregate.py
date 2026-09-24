"""Offline aggregate-reader checks; no cloud access or exchange requests."""
import pytest

from market_research import interval_safe_aggregate as report
from market_research.btc_sticky_tracking import taker_fee


def records():
    trade = {'status': 'closed', 'market_id': 'KXBNB15M-TEST',
             'game_id': 'KXBNB15M-TEST', 'trade_id': 'trade-1',
             'entry_at': 10, 'exit_at': 20, 'outcome_confirmed_at': 20,
             'entry_price': 0.6, 'exit_price': 1,
             'quantity': 1, 'fees': taker_fee(1, 0.6, '0.0001', 1)}
    trade['net_pnl'] = 1 - trade['entry_price'] - trade['fees']
    full = {'p90_sticky': {'fixed_one': {'trades': [trade],
            'summary': {'closed_trades': 1, 'max_quantity_used': 1,
                        'wins': 1, 'losses': 0, 'breakeven': 0}}}}
    compact = {'version': report.VERSION, 'complete': True,
               'configuration': {'series': 'KXBNB15M'}, 'fee_status': 'current_schedule_sensitivity',
               'fee_policy': {'fee_type': 'quadratic', 'multiplier': 1},
               'as_of': 100, 'coverage': {'analyzed_markets': 1},
               'origins': {'1': {'p90_sticky': {'fixed_one':
                           {'report_key': 'origin-1-' + 'b'*32}}}}}
    return compact, full


def test_load_exact_three_increase_replay_without_trades_in_output(monkeypatch):
    compact, full = records()
    monkeypatch.setattr(report, 'Campaign', lambda *args: object())
    monkeypatch.setattr(report, '_record', lambda _, key: compact if key.startswith('report-') else full)
    result = report.load('KXBNB15M', 'p90-' + 'a'*24, 'report-' + 'c'*32)
    assert result['history_minutes'] == 1 and result['forecast_minutes'] == 14
    assert result['replay']['wins'] == 1 and result['replay']['maximum_loss_escalations_per_recovery_cycle'] == 3
    assert result['streaks']['longest_winning_streak'] == 1
    assert 'trades' not in result and 'trade' not in result['replay']


@pytest.mark.parametrize('series,campaign,key', [
    ('KXBTC15M', 'p90-'+'a'*24, 'report-'+'b'*32),
    ('KXUNKNOWN15M', 'p90-'+'a'*24, 'report-'+'b'*32),
    ('KXBNB15M', 'wrong', 'report-'+'b'*32),
    ('KXBNB15M', 'p90-'+'a'*24, 'wrong'),
])
def test_rejects_wrong_identity_before_cloud_access(series, campaign, key):
    with pytest.raises(ValueError):
        report.load(series, campaign, key)


def test_incomplete_report_never_publishes_numbers(monkeypatch):
    compact, full = records()
    compact['complete'] = False
    monkeypatch.setattr(report, 'Campaign', lambda *args: object())
    monkeypatch.setattr(report, '_record', lambda _, key: compact if key.startswith('report-') else full)
    with pytest.raises(ValueError, match='INCOMPLETE_OR_WRONG_INTERVAL_REPORT'):
        report.load('KXBNB15M', 'p90-'+'a'*24, 'report-'+'b'*32)
