"""Frozen terminal P99/P1 path studies, not executions or trading win rates."""
from collections import defaultdict

from .engine import digest
from .recovery_switch import VERSION as FORECAST_VERSION

VERSION = "p90_terminal_p99_p10_terminal_p1_v1"


def study(forecasts, observations, *, as_of):
    tape = defaultdict(dict)
    for q in observations:
        if q.get('observed') and q['timestamp'] % 60 == 0 and q['timestamp'] <= as_of and 0 <= q['bid'] <= q['ask'] <= 1:
            tape[q['contract_id']][q['timestamp']] = q
    grouped = defaultdict(list)
    for f in forecasts:
        if f.get('strategy') == FORECAST_VERSION and f['available_at'] < as_of:
            grouped[(f['market_context']['event_id'], f['origin'])].append(f)
    by_side = defaultdict(list)
    for pair in grouped.values():
        if len(pair) != pair[0]['expected_side_count'] or len({f['market_context']['contract_id'] for f in pair}) != len(pair) or len({f['available_at'] for f in pair}) != 1:
            continue
        for f in pair: by_side[f['market_context']['contract_id']].append(f)
    episodes = []
    for side, series in by_side.items():
        series.sort(key=lambda f:(f['available_at'],f['origin']))
        for index, f in enumerate(series):
            curves = {r['timestamp']:r['quantiles'] for r in f['rows']}
            if not curves: continue
            deadline = max(curves)
            terminal = curves[deadline]
            next_publication = series[index+1]['available_at'] if index+1 < len(series) else as_of
            for t, q in sorted(tape[side].items()):
                # Previous/current bars must both have been observed after this
                # forecast became available. A revision/gap cannot invent a cross.
                prior = tape[side].get(t-60)
                if not (f['available_at'] < t-60 and t <= next_publication and prior and t in curves and t-60 in curves):
                    continue
                for name, level, target, direction in (
                    ('p90_buy_to_terminal_p99','0.9','0.99',1),
                    ('p10_sell_to_terminal_p1','0.1','0.01',-1)):
                    before, now = prior['bid'], q['bid']
                    crossed = (before < curves[t-60][level] and now >= curves[t][level] if direction == 1
                               else before > curves[t-60][level] and now <= curves[t][level])
                    if not crossed or target not in terminal: continue
                    value = terminal[target]
                    beyond = direction * (now-value) >= 0
                    future = [tape[side][n] for n in sorted(tape[side]) if t < n <= min(deadline,as_of)]
                    hit = next((r for r in future if direction * (r['bid']-value) >= 0),None)
                    complete = deadline <= as_of and all(n in tape[side] for n in range(t+60,deadline+1,60))
                    state = ('already_met_at_signal' if beyond else 'hit' if hit else
                             'miss' if complete else 'pending' if as_of < deadline else 'censored_missing_minutes')
                    episodes.append({'episode_id':digest([VERSION,name,f['forecast_id'],t]),'study':name,
                        'game_id':f['market_context']['event_id'],'contract_id':side,'forecast_id':f['forecast_id'],
                        'forecast_origin':f['origin'],'forecast_available_at':f['available_at'],
                        'signal_at':t,'signal_bid':now,'signal_quantile':curves[t][level],
                        'target_quantile':target,'target_price':value,'target_timestamp':deadline,
                        'status':state,'hit_at':hit['timestamp'] if hit and not beyond else None,
                        'hit_bid':hit['bid'] if hit and not beyond else None,
                        'minutes_to_hit':(hit['timestamp']-t)//60 if hit and not beyond else None})
    summary = {}
    for name in ('p90_buy_to_terminal_p99','p10_sell_to_terminal_p1'):
        rows=[e for e in episodes if e['study']==name]
        counts={s:sum(e['status']==s for e in rows) for s in ('hit','miss','already_met_at_signal','pending','censored_missing_minutes')}
        eligible=len(rows)-counts['already_met_at_signal']
        completed=counts['hit']+counts['miss']
        summary[name]={'signals':len(rows),**counts,'eligible_new_target_paths':eligible,
                      'completed_paths':completed,'hit_rate_completed_paths':counts['hit']/completed if completed else None,
                      'hit_rate_all_eligible_lower_bound':counts['hit']/eligible if eligible else None,
                      'hit_rate_all_eligible_upper_bound':(eligible-counts['miss'])/eligible if eligible else None}
    return {'version':VERSION,'summary':summary,'episodes':episodes,
            'definition':'Strict consecutive-minute closing-bid crossing of time-aligned P90/P10, then original forecast terminal P99/P1 by its original deadline. Targets never revise.',
            'limitations':'Quote-path hit rates, not executable trade win rates. Repeated signals are correlated. Missing minute paths without an observed hit are censored; already-met targets are excluded.'}
