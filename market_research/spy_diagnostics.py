"""Price-level diagnostics only: never infer an option premium or option P&L."""
import argparse
import csv
from pathlib import Path
from statistics import mean

import pandas as pd

from .spy_exit_sweep import load_archive
from .spy_weekly import aggregate_minutes, digest, save, session_grid, simulate


def difference(price, reference):
    return {'usd': price-reference, 'pct': (price/reference-1)*100}


def local(timestamp):
    return pd.Timestamp(timestamp).tz_convert('America/New_York').strftime('%a %Y-%m-%d %I:%M %p %Z')


def diagnose(bars, forecasts):
    """Frozen Friday forecasts, arithmetic means over forecast rows, hourly signals.

    The first future P50 is not the last actual close. Means of quantile paths
    are not quantiles of the weekly average or terminal-price distribution.
    """
    baseline = simulate(bars, forecasts, contrarian=True)
    entries = {t['signal_time']: t for t in baseline['trades']}
    by_time = {b['timestamp']: b for b in bars}
    grid = sorted((r for f in forecasts for r in f['future_grid']), key=lambda r:r['timestamp'])
    next_grid = {a['timestamp']: b for a,b in zip(grid,grid[1:])}
    weeks, signals = [], []
    for f in forecasts:
        predictions = f['predictions']
        if not predictions:
            raise ValueError('EMPTY_FORECAST')
        averaged = {q: mean(r['quantiles'][q] for r in predictions) for q in predictions[0]['quantiles']}
        initial = predictions[0]['quantiles']['0.5']
        history = [b for b in bars if b['timestamp']<=f['origin']]
        actual = [by_time[r['timestamp']] for r in predictions if r['timestamp'] in by_time]
        if not history or not actual or actual[-1]['timestamp']!=f['week_end']:
            raise ValueError('INCOMPLETE_WEEK')
        last = history[-1]['close']; terminal = actual[-1]['close']
        put, call = averaged['0.1'], averaged['0.9']
        # Continuous, unlisted reference levels: this is a payoff ceiling on
        # premium for a hypothetical held-to-expiry pair, NOT an options trade.
        call_intrinsic, put_intrinsic = max(0.,terminal-call), max(0.,put-terminal)
        week = {'origin':f['origin'], 'origin_et':local(f['origin']), 'end':f['week_end'],
            'initial_future_p50':initial, 'initial_future_p50_time':predictions[0]['timestamp'],
            'average_quantiles':averaged, 'average_method':'equal weight per forecast row, including final partial session bar',
            'last_actual_close':last, 'next_session_open':actual[0]['open'], 'next_session_time_et':local(actual[0]['start']),
            'end_close':terminal, 'week_high':max(b['high'] for b in actual), 'week_low':min(b['low'] for b in actual),
            'p10_minus_initial_p50':difference(put,initial), 'p90_minus_initial_p50':difference(call,initial),
            'p10_minus_average_p50':difference(put,averaged['0.5']), 'p90_minus_average_p50':difference(call,averaged['0.5']),
            'p10_minus_origin_close':difference(put,last), 'p90_minus_origin_close':difference(call,last),
            'both_reference_strikes_otm_at_origin_close':put<last<call,
            'both_reference_strikes_otm_at_next_open':put<actual[0]['open']<call,
            'upper_reference_touched':any(b['high']>=call for b in actual),
            'lower_reference_touched':any(b['low']<=put for b in actual),
            'hypothetical_continuous_strike_payoff':{'call_per_share':call_intrinsic,'put_per_share':put_intrinsic,
                'total_per_share_before_premiums':call_intrinsic+put_intrinsic,
                'options_net_profit':None,'reason':'Historical listed strikes, premiums, spreads and fills not established'}}
        weeks.append(week)
        predictions_by_time = {r['timestamp']:r for r in predictions}
        for s in baseline['signals']:
            if s['timestamp'] not in predictions_by_time:
                continue
            q = predictions_by_time[s['timestamp']]['quantiles']
            key = '0.9' if s['side']=='short' else '0.1'
            nxt = next_grid.get(s['timestamp'])
            fill = by_time.get(nxt['timestamp']) if nxt else None
            entry = entries.get(s['timestamp'])
            signals.append({'timestamp':s['timestamp'],'time_et':local(s['timestamp']), 'origin':f['origin'],
                'quantile':'P90' if key=='0.9' else 'P10', 'contrarian_side':s['side'],
                'signal_kind':'first_forecast_bar_already_beyond_threshold' if s['timestamp']==predictions[0]['timestamp'] else 'hourly_region_entry',
                'hourly_signal_close':s['close'],'time_aligned_threshold':q[key], 'time_aligned_p50':q['0.5'],
                'initial_future_p50':initial, 'average_p50':averaged['0.5'], 'average_quantile':averaged[key],
                'signal_minus_average_quantile':difference(s['close'],averaged[key]),
                'threshold_minus_initial_p50':difference(q[key],initial),
                'next_open':fill['open'] if fill else None, 'next_open_time_et':local(fill['start']) if fill else None,
                'next_open_minus_average_quantile':difference(fill['open'],averaged[key]) if fill else None,
                'next_open_minus_aligned_threshold':difference(fill['open'],q[key]) if fill else None,
                'baseline_trade_entered':entry is not None, 'baseline_entry_price':entry['entry_price'] if entry else None,
                'note':'New hourly-close signal region, not an exact quantile-level limit fill; repeated same-side signals may not trade'})
    return {'weeks':weeks,'crossings':signals,'strategy':'contrarian no-trailing baseline',
        'option_strategy_interpretation':'buy call near averaged P90 plus put near averaged P10; following Friday expiry; premiums unknown',
        'limitations':['No option P&L or Greek delta can be inferred from underlying price differences.',
            'Average path levels may not be OTM and may not be listed option strikes.',
            'A forecast using the Friday 4pm close cannot be assumed available for an instantaneous fill at that same close.',
            'Next session can be Tuesday after a Monday holiday; Friday-frozen and Monday-retrained forecasts are different tests.',
            'Signal detection is hourly; intrahour touches are not additional strategy entries.']}


def run(archive, output):
    records, manifest = load_archive(archive)
    source = records['report-spy-source.json']
    forecasts = sorted((v for k,v in records.items() if k.startswith('report-spy-forecast-')),key=lambda f:f['origin'])
    bars,_ = aggregate_minutes(source['rows'],session_grid(source['request']['start'][:10],source['request']['end'][:10]))
    report = {**diagnose(bars,forecasts),'source_manifest_sha256':digest(manifest),'source_run_id':manifest['run_id']}
    output = Path(output);output.mkdir(parents=True,exist_ok=True)
    save(output/'report-spy-crossings.json',report)
    # Flat, spreadsheet-friendly rows; preserve exact values in immutable JSON.
    for name in ('weeks','crossings'):
        rows=[]
        for r in report[name]:
            flat={}
            for k,v in r.items():
                if isinstance(v,dict):
                    flat.update({f'{k}.{sub}':value for sub,value in v.items()})
                else:
                    flat[k]=v
            rows.append(flat)
        fields=list(dict.fromkeys(k for r in rows for k in r))
        with (output/f'{name}.csv').open('x',newline='') as target:
            writer=csv.DictWriter(target,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--archive',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();run(args.archive,args.output)
