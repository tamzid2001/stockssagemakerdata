import json
from pathlib import Path
import zipfile

import pytest
import yaml

from market_research.exit_comparison import compare, export
from market_research.recovery_switch import simulate, EXIT_FRACTIONS
from market_research.tests.test_recovery_switch import forecasts, quote


def curves(end=1500):
    fs = forecasts(end=end)
    for f in fs:
        for row in f['rows']:
            row['quantiles']['0.1'] = .01
            row['quantiles']['0.9'] = .4
    return fs


def run(rows, policy, end=1500):
    return simulate(curves(end), rows, {}, as_of=max(r['timestamp'] for r in rows),
                    p90_touch=True, switch_on_other_p90=True, exit_policy=policy)


def test_fixed_profit_is_relative_to_paid_ask_and_exits_next_minute_bid():
    rows = [quote(180,.5), quote(240,.5,bid=.49), quote(300,.57,bid=.56), quote(360,.53,bid=.52)]
    report = run(rows, {'kind':'take_profit','fraction':.1})
    trade = report['trades'][0]
    assert trade['entry_at'] == 240 and trade['entry_price'] == .5
    assert trade['exit_signal_at'] == 300 and trade['exit_at'] == 360
    assert trade['exit_price'] == .52  # Not the .55 target or signal bid .56.
    assert trade['exit_signal_threshold'] == pytest.approx(.55)
    assert trade['exit_reason'] == 'take_profit'
    assert len(report['trades']) == 1  # Staying above P90 is not a fresh crossing.


def test_trailing_stop_uses_only_completed_bids_and_survives_forecast_end():
    rows = [quote(180,.5),quote(240,.5,bid=.49),quote(300,.61,bid=.6),
            quote(360,.57,bid=.56),quote(361,.99,bid=.98),quote(420,.54,bid=.53),quote(480,.52,bid=.51)]
    trade = run(rows, {'kind':'trailing_stop','fraction':.1},end=300)['trades'][0]
    assert trade['peak_completed_minute_bid'] == .6  # Ignore stream tick.
    assert trade['exit_signal_at'] == 420 and trade['exit_at'] == 480
    assert trade['exit_signal_threshold'] == pytest.approx(.54)
    assert trade['exit_price'] == .51
    assert trade['exit_reason'] == 'trailing_stop'


def test_take_profit_and_trailing_stop_are_distinct_no_stop_relabeling():
    rows = [quote(180,.6),quote(240,.6,bid=.59),quote(300,.66,bid=.65),quote(360,.60,bid=.59)]
    assert run(rows,{'kind':'take_profit','fraction':.05})['trades'][0]['status'] == 'closed'
    assert run(rows,{'kind':'trailing_stop','fraction':.10})['trades'][0]['status'] == 'open'


def test_100_percent_targets_never_clamped_to_binary_payout():
    rows=[quote(180,.6),quote(240,.6,bid=.59),quote(300,.99,bid=.98),quote(360,1,bid=1)]
    r=compare(curves(),rows,{},as_of=360)
    assert len(r['summary']) == 25
    s=r['summary']['p90_take_profit_100pct']
    assert s['unreachable_take_profit_entries'] == 1 and s['open_positions'] == 1
    assert s['win_rate'] is None
    assert r['summary']['p90_trailing_stop_100pct']['open_positions'] == 1


def test_one_percent_gross_profit_can_lose_net_and_keeps_recovery_size():
    rows=[quote(180,.5),quote(240,.5,bid=.49),quote(300,.52,bid=.51),quote(360,.52,bid=.51)]
    trade=run(rows,{'kind':'take_profit','fraction':.01})['trades'][0]
    assert trade['gross_pnl'] > 0 and trade['net_pnl'] < 0 and trade['next_quantity'] == 2.5


def test_no_retroactive_exit_or_synthetic_gap_fill_and_baseline_unchanged():
    rows=[quote(180,.5),quote(240,.5,bid=.49),quote(300,.57,bid=.56),quote(480,.53,bid=.52)]
    r=run(rows,{'kind':'take_profit','fraction':.1})
    assert r['trades'][0]['status'] == 'open'
    baseline=simulate(curves(),rows,{},as_of=480,switch_on_other_p90=True,p90_touch=True)
    sweep=compare(curves(),rows,{},as_of=480)
    s=sweep['summary']['p90_cross_p10_opposite']
    assert all(s[k] == v for k,v in baseline['summary']['p90_cross_p10_opposite'].items())


@pytest.mark.parametrize('fraction', EXIT_FRACTIONS)
def test_every_requested_percentage_validated(fraction):
    for kind in ('take_profit','trailing_stop'):
        assert run([quote(180,.5)],{'kind':kind,'fraction':fraction})['trades'] == []


@pytest.mark.parametrize('policy',[{}, {'kind':'stop','fraction':.1}, {'kind':'take_profit','fraction':float('nan')},
                                    {'kind':'trailing_stop','fraction':0}, {'kind':'take_profit','fraction':True}])
def test_invalid_percentage_policies_fail(policy):
    with pytest.raises(ValueError,match='INVALID_EXIT_COMPARISON_POLICY'):
        run([quote(180,.5)],policy)


def test_report_exports_preserve_all_variants_and_encrypt(tmp_path,monkeypatch):
    from market_research.artifact import package,decrypt
    monkeypatch.setenv('QUANTURA_RESEARCH_ARTIFACT_KEY','ab'*32)
    result=compare(curves(),[quote(180,.5),quote(240,.5)],{},as_of=240)
    root=tmp_path/'report';root.mkdir();export(result,root)
    package(root,tmp_path/'out.enc');decrypt(tmp_path/'out.enc',tmp_path/'out.zip')
    with zipfile.ZipFile(tmp_path/'out.zip') as z:
        assert 'exit_comparison_summary.csv' in z.namelist()
        assert len(json.loads(z.read('report-exit-comparison.json'))['summary']) == 25


def test_observer_rejects_zip_path_traversal(tmp_path):
    from market_research.exit_observer import extract_checkpoint
    file=tmp_path/'evil.zip'
    with zipfile.ZipFile(file,'w') as z:z.writestr('../research.qra.enc',b'x'*100)
    with pytest.raises(ValueError,match='UNAPPROVED_CHECKPOINT'):
        extract_checkpoint(file,tmp_path/'output')


def test_comparison_workflow_has_no_exchange_orders_or_model_downloads():
    root=Path(__file__).resolve().parents[2]
    w=yaml.safe_load((root/'.github/workflows/p90-exit-comparison.yml').read_text())
    assert w['permissions']['actions'] == 'read'
    assert w['concurrency']['cancel-in-progress'] is False
    assert w[True]['schedule'] and w['jobs']['compare']['timeout-minutes'] == 25
    text=json.dumps(w)
    assert 'HF_TOKEN' not in text and 'upload-artifact' not in text
    assert 'QUANTURA_RESEARCH_ARTIFACT_KEY' in text


def test_checkpoint_recency_is_creation_time_not_numeric_id():
    from market_research.btc_handoff import latest_checkpoint
    old={'id':900,'created_at':'2026-09-13T21:30:00Z'}
    new={'id':800,'created_at':'2026-09-13T21:35:00Z'}
    assert latest_checkpoint([old,new])==new
