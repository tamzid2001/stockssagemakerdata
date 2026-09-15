"""BTC P90 buy / P10-or-opposite-P90 switch, evaluated on saved minute quotes.

No new forecasts, order endpoints or updates to the immutable input corpus.
The older low-to-high path experiments remain a separately named report.
"""
from copy import deepcopy
import json

from .engine import stamp
from .recovery_switch import VERSION as REPLAY_VERSION, simulate
from .quantile_paths import model_participation

VERSION = "btc_first2_next13_p90_p10_switch_v1"


def simulation_inputs(store):
    records = {r['market']['ticker']: r for r in store.values('checkpoints')
               if isinstance(r, dict) and r.get('market') and r.get('available_at')}
    forecasts = []
    for saved in store.values('forecasts'):
        if saved.get('strategy') not in ('kalshi_btc_first2_next13_v1', 'kalshi_btc_first1_next14_v1'):
            continue
        f = deepcopy(saved)
        ticker = f['market_context']['market_id']
        record = records.get(ticker)
        if not record:
            continue
        market = record['market']
        f.update(strategy=REPLAY_VERSION, game_start=stamp(market['open_time']), expected_side_count=2)
        forecasts.append(f)
    resolutions = {}
    for r in store.values('checkpoints'):
        if not isinstance(r, dict) or r.get('resolution_status') != 'resolved':
            continue
        ticker = r['contract_id'].rsplit(':', 1)[0]
        if ticker not in records or type(r.get('selected_side_won')) is not bool:
            continue
        resolutions[r['contract_id']] = {**r, 'settled_at': stamp(records[ticker]['market']['close_time']),
                                         'selected_side_payout': int(r['selected_side_won'])}
    return forecasts, store.values('observations'), resolutions


def from_store(store, as_of):
    forecasts, observations, resolutions = simulation_inputs(store)
    report = simulate(forecasts, observations, resolutions, as_of=as_of,
                      switch_on_other_p90=True, p90_touch=True)
    one = any(c.get('version') == 'kalshi_btc_first1_next14_v1' for c in store.values('configuration'))
    # The BTC strategy is separately versioned even though it shares execution.
    report.update(version=VERSION, input_forecast_count=len(forecasts), forecast_count=len(forecasts),
                  description='First two completed minutes → 13-minute forecast. Buy on P90 touch or higher; hold one side; reverse on held P10 or opposite P90. No fixed stop. Multiply next size 2.5x while cumulative game net P&L is negative, capped at 100; reset to one on cumulative recovery.',
                  horizon_minutes=13, history_minutes=2,
                  signal_labels={'p90': 'Buy signal', 'p10': 'Sell signal'},
                  validation='Two observations are not evidence of forecasting reliability. Quote fills assume no depth or queue constraints; not live exchange trades.')
    if one:
        from .btc_limits import simulate as limits
        report = limits(forecasts, observations, resolutions, as_of=as_of)
        # Existing report/export envelope; the new version explicitly identifies
        # candidate maker fills and includes all unfilled/repriced orders.
        report['summary'] = {report['version']: report['summary']}
        report.update(forecast_count=len(forecasts), input_forecast_count=len(forecasts),
                      history_minutes=1, horizon_minutes=14,
                      model_participation=model_participation(forecasts))
    return report


def export(store, as_of):
    report = from_store(store, as_of)
    with (store.directory / 'btc_signal_report.json').open('w') as out:
        json.dump(report, out, allow_nan=False, indent=2)
    return report
