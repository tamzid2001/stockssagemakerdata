"""Exit-only research on a saved, previously decrypted SPY ensemble archive.

No inference, market-data calls, broker orders, or edits to source checkpoints.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import zipfile

from .spy_weekly import aggregate_minutes, digest, save, session_grid, simulate

VERSION = 'spy_contrarian_trailing_v1'
TRAILS = (None, .001, .0025, .005, .0075, .01, .015, .02, .03, .05, .10)
COSTS = (0., 1., 5.)


def load_archive(path):
    with zipfile.ZipFile(path) as archive:
        if len(archive.infolist()) > 20 or sum(f.file_size for f in archive.infolist()) > 64*1024*1024:
            raise ValueError('UNEXPECTED_RESEARCH_ARCHIVE_SIZE')
        if len(archive.namelist()) != len(set(archive.namelist())):
            raise ValueError('DUPLICATE_ARCHIVE_MEMBER')
        manifest = json.loads(archive.read('manifest.json'))
        if set(archive.namelist()) != {'manifest.json', *[f['name'] for f in manifest['files']]}:
            raise ValueError('UNEXPECTED_ARCHIVE_MEMBERS')
        records = {}
        for f in manifest['files']:
            name = f['name']
            if '/' in name or '\\' in name or not name.startswith('report-spy-') or not name.endswith('.json'):
                raise ValueError('UNEXPECTED_RESEARCH_MEMBER')
            raw = archive.read(name)
            if len(raw) != f['bytes'] or hashlib.sha256(raw).hexdigest() != f['sha256']:
                raise ValueError('SOURCE_CHECKSUM_MISMATCH')
            records[name] = json.loads(raw)
    return records, manifest


def run(archive_path, output):
    records, manifest = load_archive(archive_path)
    source = records['report-spy-source.json']
    original = records['report-spy-summary.json']
    forecasts = sorted((f for name,f in records.items() if name.startswith('report-spy-forecast-')), key=lambda f:f['origin'])
    bars,rejected = aggregate_minutes(source['rows'],session_grid(source['request']['start'][:10],source['request']['end'][:10]))
    for f in forecasts:
        training = [b for b in bars if b['timestamp']<=f['origin']][-500:]
        if len(training)!=500 or digest(training)!=f['training_sha256']:
            raise ValueError('POINT_IN_TIME_TRAINING_HASH_MISMATCH')
        if {m['model_id'] for m in f['model_runs']} != {'prophet','toto','granite','chronos','timesfm'}:
            raise ValueError('FIVE_APPROVED_MODEL_FORECAST_REQUIRED')
    baseline = simulate(bars,forecasts)
    if baseline != original['scenarios']['0.0']:
        raise ValueError('ORIGINAL_MOMENTUM_REGRESSION')
    comparisons={}; table=[]
    for monitor in ('minute', 'hourly'):
        for cost in COSTS:
            for fraction in TRAILS:
                label='none' if fraction is None else f'{fraction*100:g}%'
                key=f'{monitor}/cost_{cost:g}bp/trail_{label}'
                result=simulate(bars,forecasts,cost_bps=cost,contrarian=True,trailing_fraction=fraction,
                                minute_bars=source['rows'] if monitor=='minute' else None)
                comparisons[key]=result
                stats=result['summary']
                table.append({'scenario':key,'monitor':monitor,'cost_bps':cost,'trail':label,
                    **{k:stats[k] for k in ('trades','wins','losses','win_rate','net_pnl_usd_one_share',
                       'return_on_initial_one_share_capital_pct','observation_marked_max_drawdown_usd','trailing_exits',
                       'skipped_missing_execution_bar')}})
    # A no-trailing reversal must invert the original gross trade arithmetic.
    reversed_base=comparisons['minute/cost_0bp/trail_none']
    if abs(reversed_base['summary']['net_pnl_usd_one_share']+baseline['summary']['net_pnl_usd_one_share'])>1e-8:
        raise ValueError('SIGNAL_REVERSAL_REGRESSION')
    rankings={}
    for monitor in ('minute','hourly'):
        for cost in COSTS:
            candidates=[r for r in table if r['monitor']==monitor and r['cost_bps']==cost]
            rankings[f'{monitor}/cost_{cost:g}bp']=sorted(candidates,key=lambda r:(-r['net_pnl_usd_one_share'],r['scenario']))
    files=(Path(__file__),Path(__file__).with_name('spy_weekly.py'))
    report={'version':VERSION,'source_run_id':manifest['run_id'],'source_code_sha':manifest['commit'],
        'source_manifest_sha256':digest(manifest),'source_snapshot_sha256':digest(source),
        'analysis_source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
        'rejected_hourly_bars':rejected,'rankings':rankings,'scenarios':comparisons,
        'configuration':{'contrarian':'short_at_or_above_P90; long_at_or_below_P10',
            'signals':'completed_hourly_close; original saved five-model forecasts',
            'trail_fractions':TRAILS,'cost_bps_per_fill':COSTS,'shares':1,
            'trailing_reference':'highest completed close for long; lowest completed close for short; armed from entry',
            'execution':'next scheduled monitoring-bar open; not a guaranteed stop-level fill',
            'priority':'opposite hourly quantile reversal before simultaneous trailing stop',
            'after_trailing_exit':'flat until a new hourly signal-region entry; no immediate same-signal re-entry',
            'weekly_refresh':'does not reset position or favorable watermark',
            'end_of_test':'close remaining position at last observed close',
            'selection':'all predefined settings reported; best in-sample is not a live recommendation'},
        'limitations':original['limitations']+[
            'Trailing triggers sample minute/hour closes, not a continuously monitored broker/tick trailing stop.',
            'Actual bid/ask spread, short availability, borrow fees, overnight financing and fills are unverified.',
            'Reusing four weeks to pick a stop is exploratory in-sample selection; no independent validation set.',
            'Returns use initial one-share SPY price, not margin or annualized return.',
            'No percentage take-profit or 2.5x recovery sizing is used.']}
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    save(output/'report-spy-reversal-sweep.json',report)
    with (output/'comparison.csv').open('x',newline='') as out:
        writer=csv.DictWriter(out,fieldnames=list(table[0]));writer.writeheader();writer.writerows(table)
    print(json.dumps({'source_run_id':manifest['run_id'],'scenarios':len(table),'rankings':rankings},indent=2))
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--archive',required=True,help='Previously authenticated/decrypted SPY ZIP; original bytes unchanged')
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    run(args.archive,args.output)
