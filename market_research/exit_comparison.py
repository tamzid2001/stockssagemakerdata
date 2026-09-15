"""Versioned exit-policy research on an immutable common forecast/quote tape.

Never chooses a winner for deployment. Saved forecasts are reused, not retrained
or selected using observed trading outcomes. All variants remain visible.
"""
import csv
import gzip
import json
from collections import Counter

from .recovery_switch import EXIT_FRACTIONS, simulate

VERSION = "p90_percentage_exit_sweep_v2_btc_minute_policy"


def compare(forecasts, observations, resolutions, *, as_of, multiplier=2.5, max_shares=100, fee_rate=.01):
    from .signal_targets import study
    policies = [None] + [{"kind": kind, "fraction": fraction}
                         for kind in ("take_profit", "trailing_stop") for fraction in EXIT_FRACTIONS]
    summary, per_game, trades = {}, {}, []
    for policy in policies:
        report = simulate(forecasts, observations, resolutions, as_of=as_of, multiplier=multiplier,
                          max_shares=max_shares, fee_rate=fee_rate, switch_on_other_p90=True,
                          p90_touch=True, exit_policy=policy)
        name, stats = next(iter(report["summary"].items()))
        ledger = report["trades"]
        summary[name] = {**stats, "exit_counts": dict(Counter(t.get("exit_reason", "open") for t in ledger)),
                         "unreachable_take_profit_entries": sum(
                             t["entry_price"] * (1 + policy["fraction"]) > 1 for t in ledger
                         ) if policy and policy["kind"] == "take_profit" else 0}
        for game, values in report["per_game"].items():
            per_game.setdefault(game, {}).update(values)
        trades.extend(ledger)
    return {"version": VERSION, "paper_only": True, "as_of": as_of, "forecast_count": len(forecasts),
            "game_count": len(per_game), "summary": summary, "per_game": per_game,
            "signal_target_study": study(forecasts, observations, as_of=as_of),
            "trades": sorted(trades, key=lambda t: (t["entry_at"], t["game_id"], t["variant"])),
            "configuration": {"percentages": [round(p * 100) for p in EXIT_FRACTIONS],
                "base_shares": 1, "multiplier": multiplier, "max_shares": max_shares,
                "reset": "cumulative_game_net_pnl_nonnegative", "fee_rate_assumption": fee_rate,
                "basis": "relative_to_entry_ask_for_take_profit_and_running_bid_peak_for_trailing_stop",
                "execution": "next_genuine_minute_bid_ask_within_120_seconds",
                "percentage_exit": "flat_until_fresh_p90_cross; baseline P10/opposite P90 reversals remain active",
                "trailing_activation": "from_entry; not_only_after_profit",
                "priority": "pending_order > percentage_exit > P10 > opposite_P90",
                "unreachable_targets": "not_clamped", "fixed_price_stop": None},
            "limitations": ["Exploratory in-sample strategy comparison, not out-of-sample proof or a recommendation.",
                "One-minute quote simulation; no intraminute highs/lows, order queue or market-depth model.",
                "Fees are a 1% entry/exit notional assumption, not a verified exchange schedule.",
                "Return is net P&L divided by closed entry notional, not bankroll ROI.",
                "Multiplying size can amplify losses and does not guarantee recovery.",
                "Comparing many settings increases selection bias; forward validation is required."]}


def inputs_from_store(store, as_of=None):
    versions = {c.get("version") for c in store.values("configuration") if c}
    if versions & {'kalshi_btc_first2_next13_v1', 'kalshi_btc_first1_next14_v1'}:
        from .btc_signals import simulation_inputs
        forecasts, observations, resolutions = simulation_inputs(store)
        return forecasts, observations, resolutions, as_of
    if "p1_minute_accumulation_v3" in versions:
        from .engine import stamp
        from .recovery_switch import VERSION as replay_version
        publication = {fid: r["available_at"] for r in store.values("checkpoints") if isinstance(r, dict)
                       for fid in r.get("path_forecast_ids", [])}
        contracts = {r["contract_id"]: r.get("contract", {}) for r in store.values("contracts")}
        forecasts = []
        for f in store.values("forecasts"):
            c = contracts.get(f["market_context"]["contract_id"], {})
            if not c.get("eventStart") or f["forecast_id"] not in publication:
                continue
            forecasts.append({**f, "strategy": replay_version, "game_start": stamp(c["eventStart"]),
                              "available_at": max(f["available_at"], publication[f["forecast_id"]])})
        outcomes = {r["contract_id"]: r for r in store.values("checkpoints")
                    if isinstance(r, dict) and r.get("resolution_status")}
        return forecasts, store.values("observations"), outcomes, as_of
    coverage = store._get("checkpoints", "recovery_coverage") or {}
    resolutions = {r["contract_id"]: r for r in store.values("checkpoints")
                   if isinstance(r, dict) and r.get("resolution_status")}
    return store.values("forecasts"), store.values("observations"), resolutions, coverage.get("as_of", as_of or 0)


def from_store(store, as_of=None):
    forecasts, observations, resolutions, cutoff = inputs_from_store(store, as_of)
    if cutoff is None:
        raise ValueError("COMPARISON_AS_OF_REQUIRED")
    result = compare(forecasts, observations, resolutions, as_of=cutoff)
    result["source_configuration"] = store.values("configuration")
    if any(c.get('version') in ('kalshi_btc_first2_next13_v1', 'kalshi_btc_first1_next14_v1') for c in result['source_configuration']):
        from .btc_minute_policy import compare as btc_compare
        result['btc_minute_policy'] = btc_compare(forecasts, observations, resolutions, as_of=cutoff)
    return result


def export(result, directory):
    with (directory / "report-exit-comparison.json").open("x") as out:
        json.dump(result, out, allow_nan=False, indent=2)
    if result.get('btc_minute_policy'):
        with gzip.open(directory / 'btc_minute_policy_trades.csv.gz', 'wt', newline='') as out:
            rows = [{**trade, 'fee_scenario': name} for name, report in result['btc_minute_policy']['scenarios'].items()
                    for trade in report['trades']]
            writer = csv.DictWriter(out, fieldnames=sorted({key for row in rows for key in row}) or ['trade_id'])
            writer.writeheader(); writer.writerows(rows)
        proxy = result['btc_minute_policy']['post_only_limit_proxy']
        for name in ('orders', 'trades'):
            rows = proxy[name]
            with gzip.open(directory / f'btc_limit_{name}.csv.gz', 'wt', newline='') as out:
                writer = csv.DictWriter(out, fieldnames=sorted({key for row in rows for key in row}) or ['id'])
                writer.writeheader(); writer.writerows(rows)
    with gzip.open(directory / "exit_comparison_trades.csv.gz", "wt", newline="") as out:
        rows = result["trades"]
        writer = csv.DictWriter(out, fieldnames=sorted({k for row in rows for k in row}) or ["trade_id"])
        writer.writeheader(); writer.writerows(rows)
    with gzip.open(directory / "signal_target_paths.csv.gz", "wt", newline="") as out:
        rows=result['signal_target_study']['episodes']
        writer=csv.DictWriter(out,fieldnames=list(rows[0]) if rows else ['episode_id'])
        writer.writeheader();writer.writerows(rows)
    with (directory / "exit_comparison_summary.csv").open("w", newline="") as out:
        rows = [{"variant": key, **{k: v for k, v in value.items() if not isinstance(v, dict)},
                 "exit_counts_json": json.dumps(value["exit_counts"], sort_keys=True)}
                for key, value in result["summary"].items()]
        writer = csv.DictWriter(out, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
