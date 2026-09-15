from copy import deepcopy
import json
import pytest

from market_research.btc_ladder import compare, compact_report, ladder_prices
from market_research.tests.test_recovery_switch import forecasts, quote


def inputs(prices=(.71, .69, .65, .59, .49, .39, .29, .19, .09), payout=1):
    fs = forecasts(end=840)
    for f in fs:
        f['history_count'] = 1
        for row in f['rows']:
            row['quantiles']['0.9'] = .65 if f['market_context']['contract_id'] == 'g-yes' else .99
    rows = []
    for i, ask in enumerate(prices):
        t = 180 + i * 60
        for side, p in (('yes', ask), ('no', 1-ask+.01)):
            rows.append({**quote(t, p, side=side), 'received_at': t+3, 'collection_mode': 'live'})
    outcomes = {'g-yes': {'resolution_status': 'resolved', 'settled_at': 840,
                         'first_confirmed_at': 845, 'selected_side_payout': payout}}
    return fs, rows, outcomes


def ladder(report):
    return report['scenarios']['entry_fee_1pct']['average_down_10c']


def test_exact_cent_levels_and_floor():
    assert ladder_prices(.7) == [.6,.5,.4,.3,.2,.1]
    assert ladder_prices(.5) == [.4,.3,.2,.1]
    assert ladder_prices(.69) == [.59,.49,.39,.29,.19,.1]
    assert ladder_prices(.1) == [] and ladder_prices(.05) == []
    for value in (True, 0, 1, -.1, float('nan'), float('inf'), .505, '0.7'):
        with pytest.raises(ValueError): ladder_prices(value)


def test_doubling_contracts_and_position_cap_accounting():
    args = inputs()
    r = compare(*args, as_of=900)
    posted = [o for o in r['orders'] if o['role']=='average_down']
    assert [o['limit_price'] for o in posted] == [.6,.5,.4,.3,.2,.1]
    assert [o['requested_quantity'] for o in posted] == [2,4,8,16,32,64]
    assert [o['quantity'] for o in posted] == [2,4,8,16,32,37]
    assert posted[-1]['cap_limited']
    assert all(o['status']=='candidate_fill' for o in posted)
    t = ladder(r)['trades'][0]
    assert t['quantity']==100 and t['entry_price']==pytest.approx(.22)
    assert t['net_pnl']==pytest.approx(100-22*1.01)
    assert t['filled_rungs']==6 and t['averaging_fills']==99
    assert r['per_market'][0]['maximum_committed_contracts']==100
    assert len(r['lots'])==7  # one per rung, never replenish
    assert t['exit_reason']=='authoritative_settlement'
    assert r['live_orders_enabled'] is False


def test_first_fill_required_and_never_fill_activation_minute():
    fs,rows,res=inputs((.71,.70,.70,.70))
    r=compare(fs,rows,res,as_of=900)
    assert not r['lots'] and not ladder(r)['trades']
    assert not [o for o in r['orders'] if o['role']=='average_down']
    fs,rows,res=inputs((.71,.09,.08))
    r=compare(fs,rows,res,as_of=900)
    assert len(r['lots'])==1
    assert all(o['status']=='skipped_marketable_at_activation' for o in r['orders'] if o['role']=='average_down')


def test_ladder_touch_does_not_fill_and_lower_orders_expire():
    r=compare(*inputs((.71,.69,.60,.50)),as_of=900)
    orders=[o for o in r['orders'] if o['role']=='average_down']
    assert orders[0]['filled_at']==363  # .60 touch excluded; later .50 < .60
    assert orders[1]['status']=='expired_unfilled'  # .50 touch never filled
    assert ladder(r)['summary']['filled_contracts']==3


def test_gap_receipt_and_unconfirmed_settlement_not_fabricated():
    fs,rows,res=inputs()
    rows=[q for q in rows if q['timestamp']!=300]
    r=compare(fs,rows,res,as_of=900)
    assert ladder(r)['summary']['filled_contracts']==1
    assert set(o['status'] for o in r['orders'] if o['role']=='average_down')=={'cancelled_data_gap_unknown_fill'}
    r=compare(*inputs(),as_of=842)
    assert ladder(r)['summary']['closed_trades']==0
    assert ladder(r)['summary']['open_positions']==1
    for patch in ({'received_at':9999}, {'collection_mode':'historical'}):
        assert not compare(fs,[{**q,**patch} for q in rows],res,as_of=900)['lots']


def test_negative_settlement_amplifies_loss_but_does_not_change_market_win_rate():
    r=compare(*inputs(payout=0),as_of=900)
    base=r['scenarios']['entry_fee_1pct']['first_entry_only']['summary']
    added=ladder(r)['summary']
    assert base['losses']==added['losses']==1
    assert added['net_pnl']==pytest.approx(-22.22)
    assert added['net_pnl'] < base['net_pnl']
    assert r['scenarios']['zero_fee_sensitivity']['average_down_10c']['summary']['net_pnl']==pytest.approx(-22)


def test_replay_prefix_stable_originals_unchanged_and_one_side():
    fs,rows,res=inputs(); before=deepcopy((fs,rows,res))
    early=compare(fs,rows,res,as_of=423)
    final=compare(fs,rows,res,as_of=900)
    assert early['lots']==[l for l in final['lots'] if l['entry_at']<=423]
    assert ladder(early)['trades'][0]['trade_id']==ladder(final)['trades'][0]['trade_id']
    assert (fs,rows,res)==before
    assert {l['contract_id'] for l in final['lots']}=={'g-yes'}
    compact=compact_report(final)
    assert 'lots' not in compact and 'orders' not in compact and 'per_market' not in compact
    assert len(json.dumps(compact))<16000


def test_horizon_isolation_and_conflicting_quotes():
    fs,rows,res=inputs()
    fs[1]['history_count']=2
    with pytest.raises(ValueError,match='COHORTS'): compare(fs,rows,res,as_of=900)
    fs[0]['history_count']=2
    assert compare(fs,rows,res,as_of=900)['horizon_minutes']==13
    with pytest.raises(ValueError,match='CONFLICTING'):
        compare(fs,rows+[dict(rows[0],ask=.8)],res,as_of=900)


def test_exported_comparison_includes_full_ladder_and_old_hold_unchanged(tmp_path,monkeypatch):
    from market_research import exit_comparison
    from market_research.btc_hold_tracking import compare as original_hold
    from market_research.artifact import package, decrypt
    import zipfile
    fs,rows,res=inputs()
    class Store:
        def values(self,kind):
            return [{'version':'kalshi_btc_first1_next14_v1'}] if kind=='configuration' else []
    monkeypatch.setattr(exit_comparison,'inputs_from_store',lambda store,as_of:(fs,rows,res,as_of))
    report=exit_comparison.from_store(Store(),900)
    assert report['btc_hold_tracking']==original_hold(fs,rows,res,as_of=900)
    assert report['btc_average_down_ladder']==compare(fs,rows,res,as_of=900)
    exit_comparison.export(report,tmp_path)
    monkeypatch.setenv('QUANTURA_RESEARCH_ARTIFACT_KEY','ab'*32)
    package(tmp_path,tmp_path/'snapshot.enc')
    decrypt(tmp_path/'snapshot.enc',tmp_path/'snapshot.zip')
    with zipfile.ZipFile(tmp_path/'snapshot.zip') as z:
        saved=json.loads(z.read('report-exit-comparison.json'))
        assert saved['btc_average_down_ladder']['lots']==report['btc_average_down_ladder']['lots']


def test_live_minute_report_includes_bounded_ladder_summary(tmp_path):
    from market_research import kalshi_btc
    from market_research.local_store import LocalStore
    store=LocalStore('live-ladder-test','test',tmp_path)
    store.claim({'version':kalshi_btc.ONE_MINUTE_VERSION})
    result=kalshi_btc.report(store,{},[])
    assert result['p90_average_down_ladder']['configuration']['rung_multiplier']==2
    assert not result['p90_average_down_ladder']['live_orders_enabled']
    assert 'orders' not in result['p90_average_down_ladder']
    store.db.close()
