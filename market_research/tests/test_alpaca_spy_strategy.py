from datetime import datetime, timedelta, timezone
from decimal import Decimal

from market_research.alpaca_spy_strategy import (
    MinuteBar, active_window, confirmed_entry_signal, entry_signal, exit_reason,
    nearest_atm_contract,
    tradable_windows,
)


def at(hour, minute=0):
    return datetime(2026, 9, 25, hour, minute, tzinfo=timezone.utc)


def rows(*items):
    return [{'timestamp': stamp.isoformat(), 'quantiles': {'0.5': median, '0.9': upper}}
            for stamp, median, upper in items]


def test_regular_day_has_six_complete_windows_and_early_close_has_three():
    normal = tradable_windows(at(13, 30), at(20))
    assert len(normal) == 6
    assert normal[0].start.hour == 9 and normal[-1].end.hour == 15
    assert active_window(at(18), normal) == normal[4]
    assert active_window(at(19, 30), normal) is None
    assert len(tradable_windows(at(13, 30), at(17))) == 3


def test_completed_minute_close_crosses_p90_in_both_directions():
    levels = rows((at(14, 31), 500, 505), (at(14, 32), 500, 505))
    assert entry_signal(MinuteBar(at(14, 31), 506, 507, 505), MinuteBar(at(14, 32), 504, 506, 503), levels) == 'put'
    assert entry_signal(MinuteBar(at(14, 31), 504, 505, 503), MinuteBar(at(14, 32), 506, 507, 504), levels) == 'call'
    assert entry_signal(MinuteBar(at(14, 31), 506, 507, 505), MinuteBar(at(14, 33), 504, 506, 503), levels) is None


def test_entry_requires_the_next_completed_minute_to_confirm_the_cross():
    forecast = rows((at(14, 33), 500, 505))
    pending = {'kind': 'put', 'crossed_at': at(14, 32).isoformat(), 'stop_p90': 505}
    assert confirmed_entry_signal(pending, MinuteBar(at(14, 33), 504, 506, 503), forecast) == ('put', 505)
    assert confirmed_entry_signal(pending, MinuteBar(at(14, 33), 506, 507, 504), forecast) is None
    assert confirmed_entry_signal(pending, MinuteBar(at(14, 34), 504, 506, 503), forecast) is None


def test_put_stops_conservatively_before_median_target_and_call_stops_at_p90():
    forecast = rows((at(14, 32), 500, 505))
    window = tradable_windows(at(13, 30), at(20))[1]
    both = MinuteBar(at(14, 32), 502, 506, 499)
    assert exit_reason('put', both, forecast, window) == 'p90_stop'
    assert exit_reason('call', both, forecast, window) == 'p90_stop'
    assert exit_reason('put', MinuteBar(at(14, 32), 499, 504, 499), forecast, window) == 'median_target'
    assert exit_reason('call', MinuteBar(at(15, 30), 501, 502, 500), forecast, window) == 'window_end'


def test_stop_can_remain_at_the_p90_crossed_on_entry():
    forecast = rows((at(14, 32), 500, 505))
    window = tradable_windows(at(13, 30), at(20))[1]
    bar = MinuteBar(at(14, 32), 504, 504, 502)
    assert exit_reason('call', bar, forecast, window) == 'p90_stop'
    assert exit_reason('call', bar, forecast, window, stop_level=501) is None
    assert exit_reason('put', bar, forecast, window, stop_level=503) == 'p90_stop'


def test_nearest_atm_option_must_be_recent_and_cost_no_more_than_200():
    contracts = [
        {'symbol': 'SPY260925C00500000', 'type': 'call', 'expiration_date': '2026-09-25', 'tradable': True, 'underlying_symbol': 'SPY', 'strike_price': '500'},
        {'symbol': 'SPY260925C00501000', 'type': 'call', 'expiration_date': '2026-09-25', 'tradable': True, 'underlying_symbol': 'SPY', 'strike_price': '501'},
    ]
    now = at(14, 32)
    quotes = {contract['symbol']: {'t': now.isoformat(), 'ap': '1.95'} for contract in contracts}
    assert nearest_atm_contract(contracts, 'call', 500.1, '2026-09-25', quotes, now=now) == (contracts[0]['symbol'], Decimal('1.95'))
    quotes[contracts[0]['symbol']]['ap'] = '2.01'
    assert nearest_atm_contract(contracts, 'call', 500.1, '2026-09-25', quotes, now=now) is None
    quotes[contracts[0]['symbol']]['ap'] = '1.95'
    quotes[contracts[0]['symbol']]['t'] = (now - timedelta(minutes=1)).isoformat()
    assert nearest_atm_contract(contracts, 'call', 500.1, '2026-09-25', quotes, now=now) is None
