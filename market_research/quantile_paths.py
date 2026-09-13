"""Deterministic prospective low-to-high paper experiments over immutable tapes.

Each origin/stratum/exit is a separate experiment, not additive portfolio P&L.
Buy the first low ask, never condition entry on a subsequent successful rally.
Only later observed bids may fill exits. No intrabar high/low execution guesses.
"""
import csv
import gzip
import json
import math
from collections import defaultdict
from .engine import digest

STRATA = ("below_p1", "p1_to_p10", "below_p10")
TARGETS = ("0.9", "0.99")
VERSION = "prospective_low_high_v1"


def in_stratum(ask, levels, stratum):
    if stratum == "below_p1":
        return ask < levels["0.01"]
    if stratum == "p1_to_p10":
        return levels["0.01"] < ask < levels["0.1"]
    return ask < levels["0.1"]


def simulate(forecasts, observations, resolutions, as_of, fee_rate=.01, stop_price=None):
    if not math.isfinite(fee_rate) or not 0 <= fee_rate <= .1:
        raise ValueError("INVALID_PAPER_FEE")
    if stop_price is not None and (not math.isfinite(stop_price) or not 0 < stop_price < 1):
        raise ValueError("INVALID_ABSOLUTE_STOP")
    tape = defaultdict(dict)
    for q in observations:
        if q.get("observed") and q["timestamp"] <= as_of and 0 <= q["bid"] <= q["ask"] <= 1:
            tape[q["contract_id"]][q["timestamp"]] = q
    groups = defaultdict(list)
    for f in forecasts:
        c = f.get("market_context", {})
        if not c.get("contract_id") or f.get("available_at", as_of + 1) > as_of:
            continue
        # All sides of a game share a publication time and origin. Missing sides
        # cannot cause an implicit choice of the only successful model/side.
        groups[(c.get("event_id") or c["market_id"], f["origin"], len(f["rows"]))].append(f)
    episodes, opportunities = [], defaultdict(int)
    for (game, origin, horizon), pair in sorted(groups.items()):
        expected=max(f.get("expected_side_count",2) for f in pair)
        if len(pair) != expected or len({f["market_context"]["contract_id"] for f in pair}) != len(pair):
            continue
        published = max(f["available_at"] for f in pair)
        end = min(f["rows"][-1]["timestamp"] for f in pair)
        sides = {f["market_context"]["contract_id"]: f for f in pair}
        curves = {s: {r["timestamp"]: r["quantiles"] for r in f["rows"]} for s, f in sides.items()}
        timestamps = sorted({t for s in sides for t in tape[s] if published < t <= min(end, as_of)})
        for stratum in STRATA:
            for target in TARGETS:
                key = f"{horizon}m:{stratum}->{target}" + (f":stop_{stop_price}" if stop_price is not None else ":no_stop")
                opportunities[key] += 1
                entry = None
                for t in timestamps:
                    candidates = []
                    for side in sides:
                        q, levels = tape[side].get(t), curves[side].get(t)
                        if q and levels and in_stratum(q["ask"], levels, stratum) and (stop_price is None or q["bid"] > stop_price):
                            candidates.append((side, q, levels))
                    if len(candidates) > 1:
                        # Unknown order of opposite-side signals within one bar.
                        # Skip the entire origin for this stratum, not cherry-pick.
                        entry = {"status": "ambiguous", "entry_at": t}
                        break
                    if candidates:
                        side, q, levels = candidates[0]
                        entry = {"status": "open", "contract_id": side, "entry_at": t,
                                 "entry_ask": q["ask"], "entry_p1": levels["0.01"],
                                 "entry_p10": levels["0.1"], "quantity": 1}
                        break
                if entry is None:
                    continue
                row = {"episode_id": digest([game, origin, horizon, stratum, target, stop_price]),
                       "game_id": game, "origin": origin, "available_at": published,
                       "end": end, "horizon_minutes": horizon, "stratum": stratum,
                       "target_quantile": target, "stop_price": stop_price, "experiment": key, **entry}
                if row["status"] != "ambiguous":
                    side = row["contract_id"]
                    row["forecast_id"] = sides[side]["forecast_id"]
                    row["side"] = sides[side]["market_context"]["side"]
                    row["outcome_label"] = sides[side]["market_context"].get("outcome")
                    later = [tape[side][t] for t in timestamps if t > row["entry_at"] and t in tape[side]]
                    # Time-aligned forecast limits; never a future-revised curve.
                    hit = next((q for q in later if q["timestamp"] in curves[side]
                                and q["bid"] >= curves[side][q["timestamp"]][target]
                                and curves[side][q["timestamp"]][target] > row["entry_ask"]), None)
                    stopped = next((q for q in later if stop_price is not None and q["bid"] <= stop_price), None)
                    if stopped and (not hit or stopped["timestamp"] <= hit["timestamp"]):
                        row.update(status="stop_loss", exit_at=stopped["timestamp"], exit_price=stopped["bid"])
                    elif hit:
                        row.update(status="target_hit", exit_at=hit["timestamp"],
                                   exit_price=curves[side][hit["timestamp"]][target])
                    elif as_of >= end:
                        # Defined time exit only at a genuine final-minute bid.
                        final = next((q for q in reversed(later) if q["timestamp"] == end), None)
                        if final:
                            row.update(status="horizon_exit", exit_at=end, exit_price=final["bid"])
                        else:
                            row["status"] = "censored_missing_end_quote"
                    if "exit_price" in row:
                        row["gross_pnl"] = row["exit_price"] - row["entry_ask"]
                        row["fees"] = fee_rate * (row["exit_price"] + row["entry_ask"])
                        row["net_pnl"] = row["gross_pnl"] - row["fees"]
                    else:
                        mark = later[-1]["bid"] if later else tape[side][row["entry_at"]]["bid"]
                        row["mark_net_pnl"] = mark - row["entry_ask"] - fee_rate * (mark + row["entry_ask"])
                    outcome = resolutions.get(side, {})
                    row["selected_side_won"] = outcome.get("selected_side_won") if outcome.get("resolution_status") == "resolved" else None
                episodes.append(row)
    summary = {}
    for key, count in sorted(opportunities.items()):
        rows = [e for e in episodes if e["experiment"] == key]
        entries = [e for e in rows if e["status"] != "ambiguous"]
        closed = [e for e in entries if "net_pnl" in e]
        hits = [e for e in entries if e["status"] == "target_hit"]
        # Overlapping rolling origins are NOT independent game outcomes.
        resolved = {(e["game_id"], e["contract_id"]): e for e in hits if e["selected_side_won"] is not None}
        cost = sum(e["entry_ask"] for e in closed)
        pnl = sum(e["net_pnl"] for e in closed)
        summary[key] = {"forecast_game_origins": count, "entries": len(entries), "target_hits": len(hits),
                        "completed_path_count": len(closed),
                        "target_hit_rate_completed_paths": len(hits) / len(closed) if closed else None,
                        "target_hit_fraction_all_entries_lower_bound":len(hits)/len(entries) if entries else None,
                        "target_hit_fraction_all_entries_upper_bound":(len(hits)+len(entries)-len(closed))/len(entries) if entries else None,
                        "closed_trades": len(closed), "profitable_trades": sum(e["net_pnl"] > 0 for e in closed),
                        "stop_losses": sum(e["status"] == "stop_loss" for e in closed),
                        "net_win_rate": sum(e["net_pnl"] > 0 for e in closed) / len(closed) if closed else None,
                        "net_pnl": pnl, "fees": sum(e["fees"] for e in closed),
                        "net_return_on_closed_cost": pnl / cost if cost else None,
                        "open_or_censored": len(entries) - len(closed),
                        "open_or_censored_mark_pnl": sum(e.get("mark_net_pnl", 0) for e in entries),
                        "ambiguous_origins_excluded": len(rows) - len(entries),
                        "resolved_hit_game_sides": len(resolved),
                        "winning_hit_game_sides": sum(e["selected_side_won"] for e in resolved.values()),
                        "eventual_win_rate_after_hit": sum(e["selected_side_won"] for e in resolved.values()) / len(resolved) if resolved else None}
    return {"version": VERSION, "as_of": as_of, "paper_only": True, "fee_rate_assumption": fee_rate,
            "summary": summary, "episodes": episodes,
            "stop_price": stop_price,
            "method": "First low ask entry; one side per game/origin/stratum/exit. Later bid >= time-aligned P90/P99 above entry. Absolute stop variant requires bid above stop at entry and exits at first bid <= stop (gaps included). Final-minute bid time exit; missing end censored. Independent experiments, not portfolio ROI."}


def from_store(store, as_of):
    outcomes = {r["contract_id"]: r for r in store.values("checkpoints")
                if isinstance(r, dict) and r.get("resolution_status") and r.get("contract_id")}
    publication={fid:r["available_at"] for r in store.values("checkpoints") if isinstance(r,dict)
                 for fid in r.get("path_forecast_ids",[])}
    forecasts=[{**f,"available_at":max(f["available_at"],publication.get(f["forecast_id"],0))}
               for f in store.values("forecasts") if not f.get("paper_publication_required") or f["forecast_id"] in publication]
    results=[simulate(forecasts,store.values("observations"),outcomes,as_of,stop_price=stop)
             for stop in (None,.51)]
    return {**results[0],"stop_price":"separate no-stop and absolute $0.51-stop experiments; entries already at/below stop excluded",
            "summary":{k:v for r in results for k,v in r["summary"].items()},
            "episodes":[e for r in results for e in r["episodes"]]}


def export(store, as_of):
    result = from_store(store, as_of)
    with (store.directory / "quantile_path_report.json").open("w") as out:
        json.dump(result, out, allow_nan=False, indent=2)
    with gzip.open(store.directory / "quantile_path_trades.csv.gz", "wt", newline="") as out:
        fields = sorted({k for row in result["episodes"] for k in row}) or ["episode_id", "status"]
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader()
        writer.writerows(result["episodes"])
    return result
