from datetime import date, datetime, timedelta, timezone
import json
import pytest
from market_research.megacap_p01_backtest import (
    MinuteBar, SYMBOLS, aggregate, bars_and_splits, context_at, plan, replay, summarize,
)

OPEN=datetime(2026,9,25,13,30,tzinfo=timezone.utc)
CLOSE=OPEN+timedelta(hours=6,minutes=30)


def predictions(rising=True):
    return [{'timestamp':(OPEN+timedelta(minutes=i)).isoformat(),'p01':99.,
             'p50':100.+i*.001*(1 if rising else -1)}for i in range(1,361)]


def test_plan_covers_all_ten_without_duplicate_dates_or_short_sessions():
    study=plan(date(2025,9,26),date(2026,9,25))
    assert study['symbols']==list(SYMBOLS)
    assert study['planned_forecasts']==2490
    assert len(study['include'])==40
    assert study['excluded_early_closes']==['2025-11-28','2025-12-24']
    for symbol in SYMBOLS:
        chunks=[r for r in study['include']if r['symbol']==symbol]
        assert chunks[0]['start']=='2025-09-26'and chunks[-1]['end']=='2026-09-25'
        assert all(a['end']<b['start']for a,b in zip(chunks,chunks[1:]))


def test_actual_downward_spacing_latency_and_eod_exit():
    bars=[MinuteBar(OPEN+timedelta(minutes=1),99,101,98,100),
          MinuteBar(OPEN+timedelta(minutes=2),98,102,97,99),
          MinuteBar(OPEN+timedelta(minutes=3),97,104,96,98),
          MinuteBar(OPEN+timedelta(minutes=361),95,99,94,96),
          MinuteBar(CLOSE,102,103,100,101)]
    result=replay(bars,predictions(),OPEN+timedelta(seconds=42),OPEN,CLOSE)
    assert [t['entry']for t in result['trades']]==[99,98]
    assert all(t['exit_at']==CLOSE.isoformat()for t in result['trades'])
    assert result['pnl']==7
    assert result['curve'][3]['open_entries']==2 # no additions after six-hour forecast


def test_declining_forecast_median_rejects_all_entries():
    bars=[MinuteBar(OPEN+timedelta(minutes=2),98,99,97,99),MinuteBar(CLOSE,96,97,95,96)]
    assert replay(bars,predictions(False),OPEN,OPEN,CLOSE)['entries']==0


def test_no_synthetic_close_or_missing_predictions():
    with pytest.raises(ValueError,match='EXACT_EXCHANGE_CLOSE_MISSING'):
        replay([MinuteBar(OPEN+timedelta(minutes=2),98,99,97,99)],predictions(),OPEN,OPEN,CLOSE)
    with pytest.raises(ValueError,match='FORECAST_TIMESTAMPS_INVALID'):
        replay([],predictions()[:-1],OPEN,OPEN,CLOSE)


def test_split_context_is_effective_date_only_and_excludes_future_bars():
    origin=datetime(2025,11,17,14,30,tzinfo=timezone.utc)
    past=MinuteBar(origin-timedelta(days=3),1000,1010,990,1000)
    future=MinuteBar(origin+timedelta(minutes=1),101,102,100,101)
    splits=[{'ex_date':'2025-11-17','old_rate':1,'new_rate':10},
            {'ex_date':'2026-11-17','old_rate':1,'new_rate':2}]
    adjusted=context_at([past,future],origin,splits)
    assert len(adjusted)==1 and adjusted[0].close==100
    assert past.close==1000 # execution/source data is immutable and raw
    assert context_at([past],origin-timedelta(days=4),splits)==[]
    assert context_at([past],origin-timedelta(days=2),splits)[0].close==1000


def test_data_fetch_is_get_only_raw_and_paginates_splits():
    calls=[]
    class API:
        def request(self,method,path,*,data,params):
            calls.append((method,path,dict(params)))
            assert method=='GET'and data
            if path.endswith('/bars'):
                assert params['adjustment']=='raw'
                return {'bars':[{'t':'2025-11-14T14:30:00Z','o':1000,'h':1001,'l':999,'c':1000}]}
            if 'page_token'not in params:
                return {'corporate_actions':{'forward_splits':[{'symbol':'NFLX','ex_date':'2025-11-17','old_rate':1,'new_rate':10}]},'next_page_token':'p2'}
            return {'corporate_actions':{}}
    bars,splits=bars_and_splits(API(),'NFLX',date(2025,11,14),date(2025,11,18))
    assert len(bars)==len(splits)==1 and len(calls)==3
    assert all('/orders'not in p for _,p,_ in calls)


def test_summary_drawdown_includes_open_losses_not_only_realized_pnl():
    record={'day':'2026-09-25','scenarios':[{'curve':[{'low':-10,'close':1,'open_entries':2}],
        'pnl':2,'entries':1,'trades':[{'pnl':2}]}]}
    result=summarize([record]);assert result['net_per_one_share_per_entry']==2
    assert result['max_equity_drawdown']==result['max_daily_loss']==10


def test_aggregate_marks_missing_days_incomplete(tmp_path):
    result=aggregate(tmp_path,{'symbols':['AAPL'],'days':['2026-09-25'],
        'start':'2026-09-25','end':'2026-09-25'},tmp_path/'report')
    assert not result['complete']
    assert result['stocks'][0]['missing_or_failed_days']==['2026-09-25']
