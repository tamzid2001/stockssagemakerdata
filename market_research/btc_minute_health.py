"""Read-only aggregate verification of the encrypted live BTC checkpoint."""
import argparse
from collections import Counter
import json
from pathlib import Path
import tempfile
import time
from .btc_minute_forecast_worker import CloudState, VERSION, ORIGINS
from .interval_studies import validate_pair_member
from .local_store import LocalStore

def summarize(store):
    records=store.values('btc_live_origins')
    usable=[r for r in records if r['status']=='published']
    for r in usable:
        n=r['history_minutes'];models=('granite','chronos','timesfm') if n==1 else ('prophet','granite','chronos','timesfm')
        if n not in ORIGINS or len(r['forecasts'])!=2:
            raise ValueError('INVALID_LIVE_PAIR')
        for f in r['forecasts']:
            validate_pair_member(f,f['origin'],15-n,models)
            if (f['available_at']!=r['published_at'] or f['available_at']>=f['origin']+60
                    or f['available_at']<r['started_at'] or not f['live_origin_usable']
                    or f['publication_clock']!='actual_live_inference_completion'):
                raise ValueError('INVALID_LIVE_PUBLICATION_CLOCK')
    latest=max(usable,key=lambda r:r['published_at'],default=None)
    minutes=store.values('btc_minutes')
    return {'version':VERSION,'sampled_at':int(time.time()),'paper_only':True,'live_orders_enabled':False,
        'firestore_writes':0,'publication_status_counts':dict(Counter(r['status'] for r in records)),
        'buffered_paired_minutes':len(minutes),'timely_paired_minutes':sum(r.get('timely',False) for r in minutes),
        'collector':store._get('checkpoints','btc_collector_health'),
        'archived_totals':store._get('checkpoints','archived_totals') or {},
        'latest_usable_pair':{'history_minutes':latest['history_minutes'],'horizon_minutes':latest['horizon_minutes'],
            'published_at':latest['published_at'],'models':[m.get('id',m.get('model')) for m in latest['forecasts'][0]['models']],
            'quantiles':list(latest['forecasts'][0]['rows'][0]['quantiles'])} if latest else None,
        'scope':'Current durable buffer and independent per-origin retired paper totals; no raw prices or forecast arrays.'}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    with tempfile.TemporaryDirectory() as folder:
        store=LocalStore(VERSION,'read-only-health',folder);cloud=CloudState();cloud.restore(store)
        if not cloud.generation:raise ValueError('BTC_MINUTE_CHECKPOINT_MISSING')
        value=summarize(store);value['checkpoint_generation']=str(cloud.generation)
        value['checkpoint_updated_at']=int(cloud.checkpoint.updated.timestamp())
        store.db.close()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(value,indent=2,allow_nan=False))
    print(json.dumps(value),flush=True)

if __name__=='__main__':main()
