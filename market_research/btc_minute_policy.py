"""New BTC minute-signal benchmark on saved forecasts, never real orders.

Keeps the original BTC book intact. Fee-free results are a sensitivity scenario,
NOT evidence that resting post-only orders filled. Minute quotes cannot establish
queue position, execution depth, maker status or the exact beginning-of-minute
price. Decisions therefore use the completed bar and a strictly later quote.
"""
from collections import Counter

from .recovery_switch import simulate, BTC_MINUTE_POLICY


def compare(forecasts, observations, resolutions, *, as_of):
    from .btc_limits import simulate as limits
    scenarios = {}
    for name, fee in (('one_percent_notional_assumption', .01), ('zero_fee_sensitivity_only', 0.)):
        report = simulate(forecasts, observations, resolutions, as_of=as_of,
                          multiplier=2.5, max_shares=100, fee_rate=fee,
                          p90_touch=True, switch_on_other_p90=True, btc_minute_policy=True)
        stats = report['summary'][BTC_MINUTE_POLICY]
        stats['exit_counts'] = dict(Counter(t.get('exit_reason', 'open') for t in report['trades']))
        stats['markets_traded'] = len({t['game_id'] for t in report['trades']})
        stats['markets_evaluated'] = len(report['per_game'])
        report['fee_scenario'] = name
        scenarios[name] = report
    return {'version': BTC_MINUTE_POLICY, 'paper_only': True, 'as_of': as_of,
            'post_only_limit_proxy': limits(forecasts, observations, resolutions, as_of=as_of),
            'scenarios': scenarios,
            'limitations': [
                'No maker-fill claim: post-only execution requires order/trade data not present in this minute-quote tape.',
                'Reduce-only is an exit-position constraint, not a fee discount and not valid for opening the other side.',
                'All eligible markets evaluated; no entry is fabricated for absent forecasts, missing quotes or conflicting signals.',
                'Hold one side until settlement except held P10 or opposite P90. No take-profit or fixed stop.',
                'Completed minute only; next genuine quote within 120 seconds. No intraminute or start-of-bar hindsight.',
                'Fee scenarios change cumulative recovery and therefore subsequent size; they must be replayed independently.',
                'Not a recommendation or validated edge; two input points and parameter selection are material limitations.'
            ]}
