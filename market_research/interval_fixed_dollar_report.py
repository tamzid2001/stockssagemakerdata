"""Read-only opposite $1 comparisons for the 13 original non-BTC cohorts."""
import argparse
import csv
import json
from pathlib import Path

from .btc_fixed_report import load

MANIFEST = Path(__file__).with_name("interval_original_cohorts.json")

def original_cohorts():
    rows=json.loads(MANIFEST.read_text())
    from .interval_archive_replay import SERIES
    if {r['series'] for r in rows} != set(SERIES)-{'KXBTC15M'} or len(rows)!=13 or not all(r['complete'] and r['as_of']==1789832648 for r in rows):
        raise ValueError("THIRTEEN_ORIGINAL_COHORTS_REQUIRED")
    return {r['series']:r for r in rows}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--series',required=True,choices=list(original_cohorts()))
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args();original=original_cohorts()[args.series]
    report=load(args.series,original['campaign_id'],original['report_key'],original['source_run'],
                original['analyzed_markets'],expected_evaluations=None)
    if report['as_of'] != original['as_of']:
        raise ValueError("ORIGINAL_COHORT_CUTOFF_MISMATCH")
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'report.json').write_text(json.dumps(report,indent=2,allow_nan=False))
    for name,key in (('fixed-one.csv','rows'),('opposite-fixed-dollar.csv','opposite_fixed_dollar_rows')):
        with (args.output/name).open('w') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(report[key][0]));writer.writeheader();writer.writerows(report[key])
    candidates=[r for r in report['opposite_fixed_dollar_rows'] if r['price_policy']=='recorded_opposite_ask'
                and not r['budget_includes_fees'] and r['balance_precision']=='0.0001' and r['trades']>0]
    print(json.dumps({'series':args.series,'coverage':report['coverage'],'best':max(candidates,key=lambda r:r['net_pnl_usd']) if candidates else None}),flush=True)

if __name__=='__main__':main()
