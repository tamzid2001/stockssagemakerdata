from market_research.pregame_screener import eligible, game_date, through_deadline, document
from market_research.engine import stamp, Quote
from market_research.provider import QuanturaProvider, KalshiProvider
import pytest


def contract(start="2026-09-26T23:30:00Z"):
    return dict(eventStart=start,status="open",source="kalshi",contractId="GAME:yes",providerSymbol="GAME",eventId="GAME",eventTitle="A vs B",outcome="A")


def test_start_hour_stops_even_before_partial_hour_kickoff():
    c=contract()
    assert eligible(c,stamp("2026-09-26T22:59:59Z"))
    assert not eligible(c,stamp("2026-09-26T23:00:00Z"))
    assert not eligible(c,stamp("2026-09-26T23:29:59Z"))
    assert not eligible({**c,"eventStart":""},stamp("2026-09-26T22:00:00Z"))
    assert not eligible({**c,"live":True},stamp("2026-09-26T22:00:00Z"))


def test_new_york_today_includes_utc_next_day_and_dst_transition():
    assert game_date(stamp("2026-09-27T01:00:00Z"))=="2026-09-26"
    assert eligible(contract("2026-09-27T01:30:00Z"),stamp("2026-09-26T23:00:00Z"))
    assert not eligible(contract("2026-09-27T13:30:00Z"),stamp("2026-09-26T23:00:00Z"))
    assert game_date(stamp("2026-11-01T05:30:00Z"))==game_date(stamp("2026-11-01T06:30:00Z"))


def test_partial_hour_end_interpolates_model_only_and_preserves_ordered_bands():
    rows=[{"timestamp":3600,"quantiles":{"0.1":.1,"0.5":.3,"0.9":.6}}, {"timestamp":7200,"quantiles":{"0.1":.2,"0.5":.5,"0.9":.8}}]
    result=through_deadline(rows,0,.2,5400)
    assert result[-1]["timestamp"]==5400
    assert result[-1]["quantiles"]["0.5"]==pytest.approx(.4)
    assert result[-1]["interpolated_model_point"]
    assert rows[-1]["timestamp"]==7200


@pytest.mark.parametrize('provider,offset',[(QuanturaProvider,3600),(KalshiProvider,0)])
def test_hourly_history_has_no_incomplete_or_filled_observations(provider,offset):
    p=provider();start=stamp('2026-09-26T00:00:00Z');end=start+4*3600
    calls=[]
    def request(path,body):
        calls.append(body)
        return {"rows":[{"timestamp":"2026-09-26T01:00:00Z","price":.4,"ask":.4},
                        {"timestamp":"2026-09-26T02:00:00Z","price":.5,"ask":.5,"is_forward_filled":True},
                        {"timestamp":"2026-09-26T04:30:00Z","price":.6,"ask":.6}]}
    p.request=request
    quotes=p.hourly_history(contract(),start,end)
    assert quotes==[Quote(start+3600+offset,.4,.4)]
    assert calls[0]['frequency']=='1h' and calls[0]['missing']=='leave'
    assert calls[0]['history_phase']=='pregame'


def test_late_model_completion_never_materializes_a_published_document():
    with pytest.raises(ValueError,match='GAME_START_HOUR_REACHED'):
        document(contract(),{},stamp('2026-09-26T23:00:00Z'))


def test_kalshi_retains_genuine_book_candles_without_trade_prices():
    p=KalshiProvider();start=stamp('2026-09-26T00:00:00Z')
    p.request=lambda *_: {"rows":[{"timestamp":"2026-09-26T01:00:00Z","price":None,"ask":.6,"bid":.5}]}
    assert p.hourly_history(contract(),start,start+7200)==[Quote(start+3600,.6,.6)]


def test_hourly_ensemble_advances_hours_and_does_not_call_regular_hour_steps_gaps(monkeypatch):
    from market_research import forecast
    from ensemble_forecasting.worker import execute_job
    monkeypatch.setattr(forecast,'execute_job',lambda job,**kw:execute_job(job,mock=True,**kw))
    result=forecast.forecast_window([Quote(3600,.4,.4),Quote(7200,.42,.42)],7,models=('prophet','chronos'),frequency='1h',failure_policy='fail')
    assert result['rows'][0]['timestamp']==10800
    assert result['rows'][-1]['timestamp']==32400
    assert result['history_gap_count']==0 and result['imputed_context_steps']==0
