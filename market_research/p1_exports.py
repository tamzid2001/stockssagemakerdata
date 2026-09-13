"""Readable audit files generated from a CONSISTENT online SQLite backup."""
import csv
import gzip
import io
import json
from .local_store import LocalStore
from .p1_oco import VERSION, QUANTILES, summary
import time


def export(directory):
    store=LocalStore("export","export",directory)
    try:
        versions={c.get("version") for c in store.values("configuration") if c}
        if "in_game_p90_switch_v1" in versions:
            from .recovery_switch import export as export_recovery
            export_recovery(store)
        if not versions.intersection({VERSION,"kalshi_btc_first2_next13_v1"}):
            return
        from .quantile_paths import export as export_paths
        export_paths(store,int(time.time()))
        if 'kalshi_btc_first2_next13_v1' in versions:
            from .btc_signals import export as export_signals
            export_signals(store, int(time.time()))
        fields=["timestamp","sequence","game_id","contract_id","kind","level","price",
            "added_quantity","quantity","average_cost","cost_basis","exit","fees","net_pnl","details_json"]
        with (store.directory/"p1_orders_and_fills.csv.gz").open("wb") as raw, gzip.GzipFile(fileobj=raw,mode="wb",mtime=0) as gz:
            with io.TextIOWrapper(gz,encoding="utf-8",newline="") as out:
                writer=csv.DictWriter(out,fieldnames=fields);writer.writeheader()
                for e in sorted(store.values("trades"),key=lambda e:(e["timestamp"],e.get("game_id",""),e["sequence"])):
                    writer.writerow({**{k:e.get(k) for k in fields[:-1]},"details_json":json.dumps(e,sort_keys=True)})
        fields=["forecast_id","game_id","contract_id","side","input_cutoff","available_at","history_count","timestamp",*[f"p{int(q*100):02d}" for q in QUANTILES]]
        filename="p1_forecast_quantiles.csv.gz" if VERSION in versions else "btc_forecast_quantiles.csv.gz"
        with (store.directory/filename).open("wb") as raw,gzip.GzipFile(fileobj=raw,mode="wb",mtime=0) as gz:
            with io.TextIOWrapper(gz,encoding="utf-8",newline="") as out:
                writer=csv.writer(out);writer.writerow(fields)
                for f in sorted(store.values("forecasts"),key=lambda f:(f["origin"],f["forecast_id"])):
                    c=f["market_context"]
                    for r in f["rows"]:
                        writer.writerow([f["forecast_id"],c.get("event_id"),c["contract_id"],c["side"],
                            f["origin"],f["available_at"],f["history_count"],r["timestamp"],
                            *[r["quantiles"][str(q)] for q in QUANTILES]])
        reports=store.values("reports")
        latest=max(reports,key=lambda r:r["created_at"])["report"] if reports else None
        if latest:
            from .p1_outcomes import from_store
            paths=from_store(store)
            latest={**latest,"snapshot_generated_at":int(time.time()),
                "levels":summary(r["state"] for r in store.values("contracts") if r.get("contract_id","").startswith("p1-game:")),
                "forecast_count":len(store.values("forecasts")),"outcome_cohorts":paths["cohorts"]}
            with (store.directory/"p1_path_outcomes.json").open("w") as out:
                json.dump(paths,out,allow_nan=False,indent=2)
        with (store.directory/"p1_summary.json").open("w") as out:
            json.dump(latest,out,allow_nan=False,indent=2)
    finally:
        store.db.close()
