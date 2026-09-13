import io
import json
import pytest
from market_research.provider import QuanturaProvider


@pytest.mark.parametrize("side,payout,expected",[("long",1,True),("short",1,False),("short",0,True)])
def test_only_official_resolved_matching_sides_are_winners(monkeypatch,side,payout,expected):
    replies=[{"market":{"status":"MARKET_STATUS_RESOLVED","marketSides":[{"id":"1","long":side=="long","price":str(int(expected))}]}},
        {"slug":"game","settlement":payout}]
    urls=[]
    def request(url,timeout):
        urls.append(url);return io.BytesIO(json.dumps(replies.pop(0)).encode())
    monkeypatch.setattr("urllib.request.urlopen",request)
    outcome=QuanturaProvider().resolution({"providerSymbol":"game","contractId":"1","side":side})
    assert outcome["selected_side_won"] is expected
    assert all(url.startswith("https://gateway.polymarket.us/v1/") for url in urls)


def test_live_one_dollar_quote_and_void_are_not_winning_resolution(monkeypatch):
    replies=[{"market":{"status":"MARKET_STATUS_OPEN","marketSides":[{"price":"1"}]}}]
    monkeypatch.setattr("urllib.request.urlopen",lambda *a,**k:io.BytesIO(json.dumps(replies.pop(0)).encode()))
    provider=QuanturaProvider();contract={"providerSymbol":"game","contractId":"1","side":"long"}
    assert provider.resolution(contract)["selected_side_won"] is None
    replies.extend([{"market":{"status":"MARKET_STATUS_RESOLVED"}},{"slug":"game","settlement":.5}])
    assert provider.resolution(contract)["resolution_status"]=="partial_void_or_unverified"
    with pytest.raises(ValueError):provider.resolution({**contract,"providerSymbol":"../admin"})
