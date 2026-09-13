"""Post-forecast path cohorts. Outcome labels are NEVER forecast model inputs."""
from .engine import digest


def collect(store,provider,contracts):
    for c in contracts:
        old=store._get("checkpoints","resolution:"+c["contractId"])
        if old and old.get("resolution_status")=="resolved":continue
        try:
            result=provider.resolution(c)
        except (ValueError,RuntimeError,OSError):
            continue  # Unverified remains unknown, not a loss or fabricated win.
        store.checkpoint("resolution:"+c["contractId"],result)


def cohorts(forecasts,observations,resolutions):
    by_side={}
    for r in observations:
        by_side.setdefault(r["contract_id"],{})[r["timestamp"]]=r
    output={}
    episodes=[]
    for f in forecasts:
        c=f["market_context"];side=c["contract_id"]
        end=f["rows"][-1]["timestamp"];levels=f["rows"][-1]["quantiles"]
        quotes=sorted((r for r in by_side.get(side,{}).values() if r.get("observed")
            and f["available_at"]<r["timestamp"]<=end),key=lambda r:r["timestamp"])
        for entry in ("0.01","0.1"):
            entered=next((r for r in quotes if r["ask"]<levels[entry] or (entry=="0.01" and r["ask"]==levels[entry])),None)
            if not entered:continue
            for target in ("0.9","0.99"):
                hit=next((r for r in quotes if r["timestamp"]>entered["timestamp"] and r["bid"]>=levels[target]),None)
                outcome=resolutions.get(side,{})
                episodes.append({"forecast_id":f["forecast_id"],"horizon":len(f["rows"]),
                    "game_id":c.get("event_id") or c["market_id"],"contract_id":side,"side":c["side"],
                    "outcome_label":c.get("outcome"),"entry_quantile":entry,"target_quantile":target,
                    "entry_at":entered["timestamp"],"entry_ask":entered["ask"],"entry_level":levels[entry],
                    "target_level":levels[target],"hit_at":hit["timestamp"] if hit else None,
                    "selected_side_won":outcome.get("selected_side_won") if outcome.get("resolution_status")=="resolved" else None})
    for horizon in sorted({len(f["rows"]) for f in forecasts}):
        for entry in ("0.01","0.1"):
            for target in ("0.9","0.99"):
                rows=[r for r in episodes if r["horizon"]==horizon and r["entry_quantile"]==entry and r["target_quantile"]==target]
                hits=[r for r in rows if r["hit_at"] is not None]
                resolved=[r for r in hits if r["selected_side_won"] is not None]
                unique={(r["game_id"],r["contract_id"]):r for r in resolved}
                output[f"{horizon}m:{entry}->{target}"]={"entry_episodes":len(rows),"hit_episodes":len(hits),
                    "within_horizon_hit_rate":len(hits)/len(rows) if rows else None,
                    "unique_hit_games":len({r["game_id"] for r in hits}),
                    "resolved_hit_episodes":len(resolved),"unknown_hit_episodes":len(hits)-len(resolved),
                    "resolved_game_sides":len(unique),"winning_game_sides":sum(r["selected_side_won"] for r in unique.values()),
                    "game_side_win_rate_after_hit":sum(r["selected_side_won"] for r in unique.values())/len(unique) if unique else None}
    return {"method":"Frozen terminal quantiles; genuine ask entry, later bid target within same forecast horizon. Strictly below P10; at/below P1.",
        "interpretation":"Contract resolution, not a forecast probability. Soccer short/NO means not that outcome, not necessarily an opposing team's win.",
        "cohorts":output,"episodes":episodes}


def from_store(store):
    outcomes={r["contract_id"]:r for r in store.values("checkpoints") if isinstance(r,dict) and r.get("resolution_status") and r.get("contract_id")}
    return cohorts(store.values("forecasts"),store.values("observations"),outcomes)
