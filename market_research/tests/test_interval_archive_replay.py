from copy import deepcopy
import pytest

from market_research import interval_archive_replay as replay
from market_research import interval_studies as study
from market_research.tests.test_interval_research import OPEN, MARKET, model
from market_research.tests.test_btc_horizons import Archive
from market_research.engine import digest


def archived():
    minutes=[{'market_id':MARKET['ticker'],'timestamp':OPEN+n*60,'received_at':OPEN+n*60+8,
        'collection_mode':'live','yes_ask':.6,'yes_bid':.59,'no_ask':.41,'no_bid':.4}
        for n in range(1,16)]
    return {'market':MARKET,'records':{'btc_minutes':minutes}}


@pytest.mark.parametrize('n',range(1,13))
def test_archive_prefix_each_minute_flat_valid_and_no_provider_io(n):
    provider=replay.PairedProvider('KXETH15M');source,tape=replay.inputs(archived(),provider)
    provider.get=lambda *a,**k:pytest.fail('must use stored minutes, never download new prices')
    archive=Archive();config=study.configuration('KXETH15M',OPEN+1800,2,'a'*40)
    result=study.forecast_origin(MARKET,n,source,config,archive,provider,model)
    assert len(tape)==30
    for f in result['forecasts']:
        assert f['history_count']==n
        assert f['available_at']>=OPEN+n*60+9
        assert len(f['rows'])==15-n
        assert [q['timestamp'] for q in f['input_snapshot']]==[OPEN+i*60 for i in range(1,n+1)]
        assert 'toto' not in [m['id'] for m in f['models']]


def test_late_history_delays_publication_but_never_becomes_trade_tape():
    raw=archived();raw['records']['btc_minutes'][0]['received_at']+=31
    provider=replay.PairedProvider('KXETH15M');source,tape=replay.inputs(raw,provider)
    assert len(tape)==28
    result=study.forecast_origin(MARKET,1,source,study.configuration('KXETH15M',OPEN+1800,2,'a'*40),Archive(),provider,model)
    assert result['input_ready_at']==OPEN+60+39
    assert all(f['available_at']>=OPEN+60+40 for f in result['forecasts'])
    assert all(q['timestamp']!=OPEN+60 for q in tape)


def test_missing_and_revised_minutes_not_filled():
    raw=archived();raw['records']['btc_minutes'].pop(0)
    provider=replay.PairedProvider('KXETH15M');source,_=replay.inputs(raw,provider)
    with pytest.raises(ValueError,match='MISSING_FIRST'):
        study.forecast_origin(MARKET,1,source,study.configuration('KXETH15M',OPEN+1800,2,'a'*40),Archive(),provider,model)
    raw=archived();raw['records']['btc_minutes'].append(raw['records']['btc_minutes'][0])
    with pytest.raises(ValueError,match='DUPLICATE'):replay.inputs(raw,provider)


def test_history_arriving_after_close_skips_compute_entirely():
    raw=archived();raw['records']['btc_minutes'][0]['received_at']=OPEN+960
    provider=replay.PairedProvider('KXETH15M');source,_=replay.inputs(raw,provider)
    result=study.forecast_origin(MARKET,1,source,study.configuration('KXETH15M',OPEN+1800,2,'a'*40),
        Archive(),provider,lambda *a,**k:pytest.fail('No useful horizon remains'))
    assert result['status']=='missed_deadline' and result['forecasts']==[]


def test_settlement_only_seeds_not_forecast_candidates_but_update_direction():
    market=archived();market['lifecycle']={'close_at':OPEN+900}
    seed=deepcopy(market);seed['records']['btc_minutes']=[]
    seed['market']['ticker']='KXETH15M-SEED';seed['lifecycle']['close_at']=OPEN
    seed['records']['btc_settlements']=[{'market_id':'KXETH15M-SEED','result':'yes','first_confirmed_at':OPEN+5}]
    selected,settlements,coverage=replay.select_sources([seed,market],OPEN+1800,100)
    assert selected==[market] and settlements==seed['records']['btc_settlements']
    assert coverage=={'closed_source_records':2,'quote_bearing_markets':1,'settlement_only_records':1}


def test_failure_metadata_keeps_model_and_retryability_without_raw_exception():
    from ensemble_forecasting.adapters.base import ModelExecutionError
    result=replay.failure_record(MARKET['ticker'],2,ModelExecutionError('prophet','MODEL_INFERENCE_FAILED'))
    assert result['model']=='prophet' and not result['retryable'] and result['error_code']=='MODEL_INFERENCE_FAILED'
    assert replay.failure_record('x',1,ValueError('MISSING_FIRST_N_COMPLETED_MINUTES'))['status']=='skipped_missing_history'
    assert 'secret' not in str(replay.failure_record('x',1,RuntimeError('https://secret@example.com')))


def test_archive_identity_and_bid_ask_rejection():
    raw=archived();raw['records']['btc_minutes'][0]['market_id']='KXBTC15M-OTHER'
    with pytest.raises(ValueError,match='IDENTITY'):replay.inputs(raw,replay.PairedProvider('KXETH15M'))
    raw=archived();raw['records']['btc_minutes'][0]['yes_bid']=.8
    with pytest.raises(ValueError,match='invalid_quote'):replay.inputs(raw,replay.PairedProvider('KXETH15M'))
    assert len(replay.SERIES)==14 and len(set(map(replay.collector_id,replay.SERIES)))==14


def exit_fixture():
    provider=replay.PairedProvider('KXETH15M');source,tape=replay.inputs(archived(),provider)
    pair=study.forecast_origin(MARKET,5,source,study.configuration('KXETH15M',OPEN+1800,2,'a'*40),Archive(),provider,model)
    # Model's first evaluation minute is 6; only NO is at or above its P90.
    for f in pair['forecasts']:
        for row in f['rows']:row['quantiles']['0.9']=.9 if f['market_context']['side']=='yes' else .1
    entry={'market_id':MARKET['ticker'],'contract_id':MARKET['ticker']+':yes',
           'entry_at':OPEN+4*60+8,'market_end':OPEN+900}
    settlement={'market_id':'KXETH15M-PRIOR','close_at':OPEN,
        'first_confirmed_at':OPEN+5,'result':'no','resolution_status':'resolved'}
    return entry,pair,tape,[settlement]


def test_disagreement_exits_next_bid_not_signal_price_or_ask():
    entry,pair,tape,settlements=exit_fixture()
    assert replay.disagreement(entry,[pair],tape,settlements,4,'opposite_p90') is None
    result=replay.disagreement(entry,[pair],tape,settlements,5,'opposite_p90')
    assert result['exit_at']==OPEN+7*60+8 and result['exit_signal_at']==OPEN+6*60
    assert result['exit_price']==.59 and result['exit_origin_minutes']==5


def test_neutral_is_distinct_from_opposite_signal():
    entry,pair,tape,settlements=exit_fixture()
    for f in pair['forecasts']:
        for row in f['rows']:row['quantiles']['0.9']=.99
    assert replay.disagreement(entry,[pair],tape,settlements,11,'opposite_p90') is None
    assert replay.disagreement(entry,[pair],tape,settlements,11,'no_longer_agrees')['neutral_or_ambiguous']


def test_never_uses_pre_entry_forecast_missing_quote_or_cross_market():
    entry,pair,tape,settlements=exit_fixture()
    old=deepcopy(pair)
    for f in old['forecasts']:f['available_at']=entry['entry_at']-1
    assert replay.disagreement(entry,[old],tape,settlements,11,'no_longer_agrees') is None
    missing=[q for q in tape if q['timestamp']!=OPEN+7*60]
    assert replay.disagreement(entry,[pair],missing,settlements,11,'opposite_p90') is None
    pair['market']='KXBTC15M-OTHER'
    assert replay.disagreement(entry,[pair],tape,settlements,11,'no_longer_agrees') is None


def test_fee_and_recovery_recomputed_after_early_exit():
    trades=[]
    for i in range(12):
        trades.append({'market_id':str(i),'game_id':str(i),'trade_id':str(i),
            'status':'closed','entry_at':OPEN+i*900+120,
            'entry_price':.6,'quantity':1,'exit_at':OPEN+(i+1)*900,
            'exit_price':0,'exit_reason':'authoritative_settlement',
            'outcome_confirmed_at':OPEN+(i+1)*900+60})
    base=replay.price_exits(trades,{},'recover_cycle',1)
    assert [t['quantity'] for t in base['trades'][:4]]==[1,2,5,12]
    assert max(t['quantity'] for t in base['trades'])==100
    decision={'exit_reason':'opposite_p90','exit_at':OPEN+600,
              'outcome_confirmed_at':OPEN+600,'exit_price':.9}
    result=replay.price_exits(trades,{'0':decision},'recover_cycle',1)
    assert result['trades'][0]['fees']>replay.taker_fee(1,.6)
    assert result['trades'][1]['quantity']==1
    # Settlement known AFTER the next entry may not increase that entry size.
    trades[0]['outcome_confirmed_at']=trades[1]['entry_at']+1
    assert replay.price_exits(trades,{},'recover_cycle',1)['trades'][1]['quantity']==1


def test_report_is_split_and_references_immutable_ledgers():
    archive=Archive();report={'origins':{'2':{'p90_sticky':{'fixed_one':{
        'summary':{'wins':2},'trades':[{'private':'ledger'}]}}}},'early_exit_after_two_minutes':{}}
    key,compact=replay.save_report(archive,report)
    ref=compact['origins']['2']['p90_sticky']['fixed_one']['report_key']
    assert archive.get('report',ref)==report['origins']['2']
    assert 'trades' not in compact['origins']['2']['p90_sticky']['fixed_one']
    assert key=='report-'+digest(compact)[:32]


def test_workflow_dispatch_preserves_frozen_source_code_and_cohort():
    from pathlib import Path
    import yaml
    path=Path(__file__).resolve().parents[2]/'.github/workflows/kalshi-interval-studies.yml'
    raw=path.read_text();yaml.safe_load(raw)
    assert 'market_research.interval_archive_replay' in raw
    assert '--ref "$GITHUB_REF_NAME"' in raw and '-f code_ref="$CODE"' in raw and '-f source="$SOURCE"' in raw
    assert 'max-parallel: 2' in raw and 'cancel-in-progress: false' in raw
    assert 'upload-artifact' not in raw
