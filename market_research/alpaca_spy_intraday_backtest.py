"""Read-only, point-in-time SPY 0DTE replay with actual Alpaca minute bars.

Option trade bars are a pricing sensitivity, not executable bid/ask fills.
No brokerage order endpoint is called by this module.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path

from .alpaca_spy_strategy import (
    MAX_PREMIUM_DOLLARS, NEW_YORK, MinuteBar, Window, entry_signal, exit_reason,
    forecast_levels,
)
from .alpaca_spy_worker import AlpacaAPI


QUANTILES = (0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)
MODELS = ('prophet', 'toto', 'granite', 'chronos', 'timesfm')
HORIZONS = (15, 30, 45, 60)


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('NAIVE_PROVIDER_TIMESTAMP')
    return parsed.astimezone(timezone.utc)


def stock_minutes(api: AlpacaAPI, day: date, feed: str) -> list[MinuteBar]:
    """Fetch provider-observed extended-session bars, never fill missing minutes."""
    if feed not in ('iex', 'sip'):
        raise ValueError('STOCK_FEED_MUST_BE_IEX_OR_SIP')
    start = datetime.combine(day - timedelta(days=16), datetime.min.time(), timezone.utc)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), timezone.utc)
    token = ''
    found: dict[datetime, MinuteBar] = {}
    while True:
        params = {'timeframe': '1Min', 'start': start.isoformat(), 'end': end.isoformat(),
            'limit': 10000, 'sort': 'asc', 'adjustment': 'split', 'feed': feed}
        if token:
            params['page_token'] = token
        response = api.request('GET', '/v2/stocks/SPY/bars', data=True, params=params)
        for row in response.get('bars') or []:
            bar = MinuteBar(timestamp(row['t']) + timedelta(minutes=1),
                float(row['c']), float(row['h']), float(row['l']), float(row['o']))
            if not all(math.isfinite(v) and v > 0 for v in (bar.open, bar.close, bar.high, bar.low)):
                raise ValueError('INVALID_SPY_BAR')
            if bar.end in found and found[bar.end] != bar:
                raise ValueError('CONFLICTING_SPY_BAR')
            found[bar.end] = bar
        next_token = str(response.get('next_page_token') or '')
        if not next_token:
            break
        if next_token == token:
            raise ValueError('STOCK_PAGINATION_STALLED')
        token = next_token
    return sorted(found.values(), key=lambda bar: bar.end)


def listed_contracts(api: AlpacaAPI, day: date) -> list[dict]:
    found = {}
    for status in ('active', 'inactive'):
        token = ''
        while True:
            params = {'underlying_symbols': 'SPY', 'expiration_date': day.isoformat(),
                'status': status, 'limit': 10000}
            if token:
                params['page_token'] = token
            response = api.request('GET', '/v2/options/contracts', params=params)
            for row in response.get('option_contracts') or []:
                if row.get('symbol'):
                    found[row['symbol']] = row
            next_token = str(response.get('next_page_token') or '')
            if not next_token:
                break
            if next_token == token:
                raise ValueError('CONTRACT_PAGINATION_STALLED')
            token = next_token
    return list(found.values())


def option_minutes(api: AlpacaAPI, symbol: str, day: date) -> dict[datetime, float]:
    start = datetime.combine(day, datetime.min.time(), NEW_YORK).astimezone(timezone.utc)
    # Accounts without real-time OPRA may only request bars at least 15 minutes
    # old. Do not ask the provider for the still-future remainder of today.
    end = min(start + timedelta(days=1), datetime.now(timezone.utc) - timedelta(minutes=16))
    if end <= start:
        return {}
    token = ''
    found = {}
    while True:
        params = {'symbols': symbol, 'timeframe': '1Min', 'start': start.isoformat(),
            'end': end.isoformat(), 'sort': 'asc', 'limit': 10000}
        if token:
            params['page_token'] = token
        response = api.request('GET', '/v1beta1/options/bars', data=True, params=params)
        for row in (response.get('bars') or {}).get(symbol) or []:
            at, price = timestamp(row['t']), float(row['o'])
            if not math.isfinite(price) or price <= 0:
                raise ValueError('INVALID_OPTION_TRADE_BAR')
            if at in found and found[at] != price:
                raise ValueError('CONFLICTING_OPTION_TRADE_BAR')
            found[at] = price
        next_token = str(response.get('next_page_token') or '')
        if not next_token:
            break
        if next_token == token:
            raise ValueError('OPTION_PAGINATION_STALLED')
        token = next_token
    return found


def first_observed_price(prices: dict[datetime, float], at: datetime, *, minutes: int = 2):
    eligible = [stamp for stamp in prices if at <= stamp <= at + timedelta(minutes=minutes)]
    if not eligible:
        return None
    selected = min(eligible)
    return selected, prices[selected]


def forecast_window(history: list[MinuteBar], horizon: int) -> dict:
    from ensemble_forecasting.worker import execute_job

    rows = [{'timestamp': bar.end.isoformat(), 'target': bar.close} for bar in history]
    result = execute_job({'request': {
        'prediction_length': horizon, 'horizon_mode': 'frequency_periods',
        'frequency': '1min', 'calendar': 'NONE', 'transform': 'log',
        'context_length': 500, 'failure_policy': 'fail', 'quantiles': QUANTILES,
        'models': {model: {'enabled': True, 'weight': 1} for model in MODELS},
        'model_checkpoints': {'toto': 'Datadog/Toto-2.0-4m'},
        }, 'runtime_mode': 'production',
        'input': {'rows': rows, 'frequency': '1min', 'timezone': 'America/New_York'},
        'source': {'type': 'ticker', 'symbol': 'SPY', 'provider': 'alpaca'}})
    if (len(result['predictions']) != horizon or
            {row['model'] for row in result['model_runs'] if row['status'] == 'completed'} != set(MODELS) or
            result['failures']):
        raise RuntimeError('FIVE_MODEL_FORECAST_INCOMPLETE')
    first = timestamp(result['predictions'][0]['timestamp'])
    if first != history[-1].end + timedelta(minutes=1):
        raise RuntimeError('FORECAST_ORIGIN_MISMATCH')
    return result


def replay_window(api: AlpacaAPI, bars: list[MinuteBar], contracts: list[dict],
                  window: Window, *, feed: str, forecast_fn=forecast_window) -> dict:
    origin = window.start.astimezone(timezone.utc)
    horizon = int((window.end - window.start).total_seconds() // 60)
    history = [bar for bar in bars if bar.end <= origin][-500:]
    record = {'origin': origin.isoformat(), 'end': window.end.astimezone(timezone.utc).isoformat(),
        'horizon_minutes': horizon, 'stock_feed': feed, 'history_count': len(history),
        'history_first': history[0].end.isoformat() if history else None,
        'history_last': history[-1].end.isoformat() if history else None,
        'trades': [], 'signals': [], 'signals_skipped': {},
        'option_price_type': 'historical_trade_bar_open_proxy'}
    if len(history) != 500:
        return {**record, 'status': 'insufficient_observed_history'}
    if history[-1].end != origin:
        return {**record, 'status': 'origin_minute_missing'}
    record['history_sha256'] = hashlib.sha256(json.dumps(
        [(bar.end.isoformat(), bar.close) for bar in history], separators=(',', ':')).encode()).hexdigest()
    try:
        forecast = forecast_fn(history, horizon)
    except Exception as exc:
        return {**record, 'status': 'forecast_failed', 'error_type': type(exc).__name__,
            'error_code': getattr(exc, 'code', None)}
    predictions = forecast['predictions']
    record['forecast_hash'] = forecast['result_hash']
    record['model_runs'] = [{k: row.get(k) for k in ('model', 'checkpoint', 'status', 'duration_seconds')}
        for row in forecast['model_runs']]
    record['predictions'] = predictions
    runtime = float(forecast['runtime_seconds'])
    record['inference_seconds'] = runtime
    available_at = origin + timedelta(seconds=runtime)
    record['earliest_actionable_at'] = available_at.isoformat()
    observed = [bar for bar in bars if origin < bar.end <= window.end.astimezone(timezone.utc)]
    record['future_observed_count'] = len(observed)
    option_cache: dict[str, dict[datetime, float]] = {}
    option_history_forbidden = False
    position = None
    previous = None
    for bar in observed:
        if position:
            reason = exit_reason(position['kind'], bar, predictions, window)
            if reason:
                price = first_observed_price(option_cache[position['symbol']], bar.end)
                trade = {**position, 'exit_signal_at': bar.end.isoformat(), 'exit_reason': reason}
                if price is None:
                    trade.update(status='exit_unpriced', exit_bar_at=None, pnl_proxy_usd=None)
                else:
                    exit_at, exit_price = price
                    trade.update(status='priced_trade_bar_proxy', exit_bar_at=exit_at.isoformat(),
                        exit_bar_open=exit_price, pnl_proxy_usd=round((exit_price - position['entry_bar_open']) * 100, 4))
                record['trades'].append(trade)
                position = None
        elif previous and bar.end >= available_at and bar.end < window.end.astimezone(timezone.utc):
            kind = entry_signal(previous, bar, predictions)
            if kind:
                signal = {'kind': kind, 'at': bar.end.isoformat(), 'spy_close': bar.close}
                record['signals'].append(signal)
                eligible = [row for row in contracts if row.get('type') == kind and
                    row.get('expiration_date') == window.start.date().isoformat() and
                    row.get('underlying_symbol') == 'SPY' and
                    (row.get('tradable') is True or window.start.date() < datetime.now(NEW_YORK).date())]
                if not eligible:
                    reason = 'contract_not_listed'
                else:
                    nearest = min(eligible, key=lambda row: (
                        abs(float(row['strike_price']) - bar.close),
                        not (float(row['strike_price']) <= bar.close if kind == 'call'
                             else float(row['strike_price']) >= bar.close), row['symbol']))
                    symbol = nearest['symbol']
                    signal['contract_symbol'] = symbol
                    if option_history_forbidden:
                        reason = 'option_history_forbidden'
                    else:
                        if symbol not in option_cache:
                            try:
                                option_cache[symbol] = option_minutes(api, symbol, window.start.date())
                            except RuntimeError as exc:
                                if str(exc) != 'ALPACA_HTTP_403':
                                    raise
                                option_history_forbidden = True
                                record['option_history_error'] = 'ALPACA_HTTP_403'
                        price = (first_observed_price(option_cache[symbol], bar.end)
                            if symbol in option_cache else None)
                        if option_history_forbidden:
                            reason = 'option_history_forbidden'
                        elif price is None:
                            reason = 'option_entry_bar_missing'
                        elif price[1] * 100 > float(MAX_PREMIUM_DOLLARS):
                            reason = 'option_bar_premium_above_200'
                        else:
                            position = {'kind': kind, 'symbol': symbol,
                                'strike': float(nearest['strike_price']),
                                'signal_at': bar.end.isoformat(), 'signal_spy_close': bar.close,
                                'entry_bar_at': price[0].isoformat(), 'entry_bar_open': price[1],
                                'premium_proxy_usd': round(price[1] * 100, 4)}
                            reason = None
                if reason:
                    record['signals_skipped'][reason] = record['signals_skipped'].get(reason, 0) + 1
        previous = bar
    if position:
        price = first_observed_price(option_cache[position['symbol']], window.end.astimezone(timezone.utc))
        trade = {**position, 'exit_signal_at': window.end.astimezone(timezone.utc).isoformat(),
            'exit_reason': 'window_end'}
        if price is None:
            trade.update(status='exit_unpriced', exit_bar_at=None, pnl_proxy_usd=None)
        else:
            exit_at, exit_price = price
            trade.update(status='priced_trade_bar_proxy', exit_bar_at=exit_at.isoformat(),
                exit_bar_open=exit_price, pnl_proxy_usd=round((exit_price - position['entry_bar_open']) * 100, 4))
        record['trades'].append(trade)
    return {**record, 'status': 'replayed'}


def replay_underlying_window(bars: list[MinuteBar], window: Window, *, feed: str,
                             forecast_fn=forecast_window) -> dict:
    """Price the signal in SPY dollars/share, never as a fictitious option fill."""
    origin = window.start.astimezone(timezone.utc)
    end = window.end.astimezone(timezone.utc)
    horizon = int((end - origin).total_seconds() // 60)
    history = [bar for bar in bars if bar.end <= origin][-500:]
    record = {'origin': origin.isoformat(), 'end': end.isoformat(),
        'horizon_minutes': horizon, 'stock_feed': feed, 'history_count': len(history),
        'history_first': history[0].end.isoformat() if history else None,
        'history_last': history[-1].end.isoformat() if history else None,
        'price_type': 'next_observed_spy_minute_open', 'signals': [], 'trades': [],
        'signals_skipped': {}}
    if len(history) != 500:
        return {**record, 'status': 'insufficient_observed_history'}
    if history[-1].end != origin:
        return {**record, 'status': 'origin_minute_missing'}
    record['history_sha256'] = hashlib.sha256(json.dumps(
        [(bar.end.isoformat(), bar.close) for bar in history],
        separators=(',', ':')).encode()).hexdigest()
    try:
        forecast = forecast_fn(history, horizon)
    except Exception as exc:
        return {**record, 'status': 'forecast_failed',
            'error_type': type(exc).__name__, 'error_code': getattr(exc, 'code', None)}
    predictions = forecast['predictions']
    record['forecast_hash'] = forecast['result_hash']
    record['model_runs'] = [{k: row.get(k) for k in
        ('model', 'checkpoint', 'status', 'duration_seconds')}
        for row in forecast['model_runs']]
    record['predictions'] = predictions
    runtime = float(forecast['runtime_seconds'])
    record['inference_seconds'] = runtime
    available_at = origin + timedelta(seconds=runtime)
    record['earliest_actionable_at'] = available_at.isoformat()
    observed = [bar for bar in bars if origin < bar.end <= end]
    record['future_observed_count'] = len(observed)
    # A completed bar ending at t can only be traded using the next bar's
    # observed open at t. Sparse provider minutes are not interpolated.
    next_opens = {bar.end - timedelta(minutes=1): bar.open
                  for bar in bars if bar.open is not None}
    position = None
    previous = None
    for bar in observed:
        if position:
            reason = exit_reason(position['kind'], bar, predictions, window,
                                 stop_level=position['stop_p90'])
            if reason:
                exit_open = next_opens.get(bar.end)
                trade = {**position, 'exit_signal_at': bar.end.isoformat(),
                    'exit_reason': reason, 'exit_at': bar.end.isoformat() if exit_open else None,
                    'exit_spy_open': exit_open}
                if exit_open is None:
                    trade.update(status='exit_unpriced', pnl_underlying_usd_per_share=None)
                    record['status'] = 'exit_open_missing'
                else:
                    sign = 1 if position['kind'] == 'call' else -1
                    trade.update(status='priced_underlying',
                        pnl_underlying_usd_per_share=round(
                            sign * (exit_open - position['entry_spy_open']), 6))
                record['trades'].append(trade)
                position = None
                if exit_open is None:
                    break
        elif previous and bar.end >= available_at and bar.end < end:
            kind = entry_signal(previous, bar, predictions)
            if kind:
                level = forecast_levels(predictions, bar.end)
                signal = {'kind': kind, 'at': bar.end.isoformat(),
                    'spy_close': bar.close, 'p90': level[1] if level else None}
                record['signals'].append(signal)
                entry_open = next_opens.get(bar.end)
                if entry_open is None:
                    record['signals_skipped']['entry_open_missing'] = (
                        record['signals_skipped'].get('entry_open_missing', 0) + 1)
                else:
                    position = {'kind': kind, 'signal_at': bar.end.isoformat(),
                        'signal_spy_close': bar.close, 'stop_p90': level[1],
                        'entry_at': bar.end.isoformat(), 'entry_spy_open': entry_open}
        previous = bar
    if position:
        exit_open = next_opens.get(end)
        trade = {**position, 'exit_signal_at': end.isoformat(),
            'exit_reason': 'window_end', 'exit_at': end.isoformat() if exit_open else None,
            'exit_spy_open': exit_open}
        if exit_open is None:
            trade.update(status='exit_unpriced', pnl_underlying_usd_per_share=None)
            record['status'] = 'exit_open_missing'
        else:
            sign = 1 if position['kind'] == 'call' else -1
            trade.update(status='priced_underlying',
                pnl_underlying_usd_per_share=round(
                    sign * (exit_open - position['entry_spy_open']), 6))
        record['trades'].append(trade)
    return {**record, 'status': record.get('status', 'replayed')}


def summarize_underlying(windows: list[dict]) -> dict:
    trades = [trade for row in windows for trade in row.get('trades', [])]
    priced = [trade for trade in trades if trade['status'] == 'priced_underlying']
    equity = peak = drawdown = 0.0
    wins = losses = 0
    for trade in priced:
        pnl = trade['pnl_underlying_usd_per_share']
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
        wins += pnl > 0
        losses += pnl < 0
    return {'planned_forecasts': len(windows),
        'completed_forecasts': sum(row['status'] == 'replayed' for row in windows),
        'forecast_failures': sum(row['status'] == 'forecast_failed' for row in windows),
        'underlying_cross_signals': sum(len(row.get('signals', [])) for row in windows),
        'trade_count': len(trades), 'priced_trade_count': len(priced),
        'unpriced_trade_count': len(trades) - len(priced),
        'wins': wins, 'losses': losses,
        'win_rate': wins / (wins + losses) if wins + losses else None,
        'net_directional_spy_change_usd_per_share': round(equity, 6) if priced else None,
        'closed_trade_drawdown_usd_per_share': round(drawdown, 6) if priced else None,
        'complete': len(windows) > 0 and all(row['status'] == 'replayed' for row in windows)
            and len(priced) == len(trades)}


def summarize(windows: list[dict]) -> dict:
    trades = [trade for row in windows for trade in row.get('trades', [])]
    priced = [trade for trade in trades if trade['status'] == 'priced_trade_bar_proxy']
    equity = peak = drawdown = capital = 0.0
    wins = losses = 0
    for trade in priced:
        capital = max(capital, trade['premium_proxy_usd'] - equity)
        equity += trade['pnl_proxy_usd']
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
        wins += trade['pnl_proxy_usd'] > 0
        losses += trade['pnl_proxy_usd'] < 0
    return {'planned_forecasts': len(windows), 'completed_forecasts': sum(row['status'] == 'replayed' for row in windows),
        'forecast_failures': sum(row['status'] == 'forecast_failed' for row in windows),
        'underlying_cross_signals': sum(len(row.get('signals', [])) for row in windows),
        'option_history_forbidden_windows': sum(row.get('option_history_error') == 'ALPACA_HTTP_403' for row in windows),
        'trade_count': len(trades), 'priced_trade_count': len(priced),
        'unpriced_trade_count': len(trades) - len(priced), 'wins': wins, 'losses': losses,
        'win_rate': wins / (wins + losses) if wins + losses else None,
        'net_pnl_trade_bar_proxy_usd_before_costs': round(equity, 4) if priced else None,
        'closed_trade_drawdown_proxy_usd': round(drawdown, 4) if priced else None,
        'minimum_starting_cash_proxy_usd': round(capital, 4) if priced else None,
        'complete': len(windows) > 0 and all(row['status'] == 'replayed' for row in windows)
            and len(priced) == len(trades)
            and not any(row.get('option_history_error') for row in windows)}


def run(day: date, horizon: int, output: Path, *, feed: str, data_only: bool = False,
        pricing: str = 'underlying') -> dict:
    import pandas_market_calendars as mcal

    if horizon not in HORIZONS:
        raise ValueError('UNSUPPORTED_HORIZON')
    if pricing not in ('underlying', 'option_bars'):
        raise ValueError('UNSUPPORTED_PRICING')
    if day > datetime.now(NEW_YORK).date():
        raise ValueError('FUTURE_DAY_NOT_BACKTESTABLE')
    schedule = mcal.get_calendar('NYSE').schedule(start_date=day, end_date=day)
    if schedule.empty:
        raise ValueError('NYSE_CLOSED_ON_DAY')
    opening = schedule.iloc[0]['market_open'].to_pydatetime().astimezone(NEW_YORK)
    closing = schedule.iloc[0]['market_close'].to_pydatetime().astimezone(NEW_YORK)
    if opening.hour != 9 or opening.minute != 30 or closing.hour < 16:
        raise ValueError('EARLY_CLOSE_EXCLUDED')
    api = AlpacaAPI('paper')
    output.mkdir(parents=True, exist_ok=True)
    try:
        bars = stock_minutes(api, day, feed)
        contracts = listed_contracts(api, day) if pricing == 'option_bars' else []
        origins = [opening + timedelta(hours=i) for i in range(6)]
        windows = [Window(origin, origin + timedelta(minutes=horizon)) for origin in origins]
        coverage = [{'origin': origin.isoformat(),
            'history_count': len([bar for bar in bars if bar.end <= origin])}
            for origin in origins]
        if data_only:
            report = {'day': day.isoformat(), 'horizon_minutes': horizon, 'stock_feed': feed,
                'stock_observed_minutes': len(bars),
                'listed_expiring_contracts': len(contracts) if pricing == 'option_bars' else None,
                'coverage': coverage, 'orders_sent': 0, 'mode': 'data_only'}
            (output / 'coverage.json').write_text(json.dumps(report, indent=2))
            return report
        results = []
        for index, window in enumerate(windows):
            record = (replay_underlying_window(bars, window, feed=feed)
                if pricing == 'underlying' else replay_window(api, bars, contracts, window, feed=feed))
            results.append(record)
            (output / f'window-{index:02d}.json').write_text(json.dumps(record, indent=2, default=str))
            print(json.dumps({'event': 'spy_backtest_window', 'day': day.isoformat(),
                'horizon': horizon, 'origin': record['origin'], 'status': record['status'],
                'trade_count': len(record['trades'])}), flush=True)
        report = {'day': day.isoformat(), 'horizon_minutes': horizon, 'stock_feed': feed,
            'stock_observed_minutes': len(bars),
            'listed_expiring_contracts': len(contracts) if pricing == 'option_bars' else None,
            'pricing': pricing, 'coverage': coverage,
            'summary': summarize_underlying(results) if pricing == 'underlying' else summarize(results),
            'orders_sent': 0,
            'limitations': ([
                'Results are signed SPY price changes in dollars per share, not option returns or fills.',
                'Historical option ask/bid and the $200 option-premium rule are not evaluated.',
                'Entries and exits use the next observed one-minute SPY open after a completed-bar signal.',
                'First actionable signal is delayed by measured model inference runtime.',
                'IEX is a single-exchange feed; absent one-minute bars are never filled synthetically.',
            ] if pricing == 'underlying' else [
                'Historical option bars contain trades, not executable bid/ask quotes or verified fills.',
                'Dollar P&L is a trade-bar-open proxy before fees, spread and slippage; not realized brokerage return.',
                'The $200 entry cap uses the option trade-bar open, not the historical ask.',
                'First actionable signal is delayed by measured model inference runtime.',
                'Missing stock minutes and option bars are never filled or priced synthetically.',
            ])}
        (output / 'summary.json').write_text(json.dumps(report, indent=2, default=str))
        return report
    finally:
        api.client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--day', type=date.fromisoformat, required=True)
    parser.add_argument('--horizon', type=int, choices=HORIZONS, required=True)
    parser.add_argument('--feed', choices=('iex', 'sip'), default='iex')
    parser.add_argument('--pricing', choices=('underlying', 'option_bars'), default='underlying')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--data-only', action='store_true')
    args = parser.parse_args()
    report = run(args.day, args.horizon, args.output, feed=args.feed,
                 data_only=args.data_only, pricing=args.pricing)
    print(json.dumps({'event': 'spy_backtest_complete', 'report': report}, default=str), flush=True)


if __name__ == '__main__':
    main()
