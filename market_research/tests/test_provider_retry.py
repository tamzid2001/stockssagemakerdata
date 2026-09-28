import io
import json
import urllib.error
from email.utils import formatdate

import pytest

from market_research import provider


@pytest.mark.parametrize('header,expected', [('30', 30), ('bad', 10), (None, 10)])
def test_rate_limit_waits_for_retry_after_or_longer_backoff(monkeypatch, header, expected):
    delays = []
    headers = {'Retry-After': header} if header else {}
    attempts = iter([
        urllib.error.HTTPError('https://quantura.studio', 429, 'busy', headers, io.BytesIO(b'{"error":"rate_limited"}')),
        {'items': []},
    ])
    def request(*args, **kwargs):
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return io.BytesIO(json.dumps(result).encode())
    monkeypatch.setattr(provider.urllib.request, 'urlopen', request)
    monkeypatch.setattr(provider.time, 'sleep', delays.append)
    assert provider.KalshiProvider().request('/api/sports/prediction-markets/research-catalog') == {'items': []}
    assert delays == [expected]


def test_retry_after_accepts_http_date_and_ignores_non_finite_values(monkeypatch):
    monkeypatch.setattr(provider.time, 'time', lambda: 1000)
    assert provider.retry_after_seconds({'Retry-After': formatdate(1045, usegmt=True)}) == 45
    assert provider.retry_after_seconds({'Retry-After': 'NaN'}) is None
    assert provider.retry_after_seconds({'Retry-After': 'inf'}) is None


def test_persistent_rate_limit_remains_an_explicit_bounded_failure(monkeypatch):
    delays, calls = [], []
    def request(*args, **kwargs):
        calls.append(1)
        raise urllib.error.HTTPError('https://quantura.studio', 429, 'busy', {}, io.BytesIO(b'{"error":"rate_limited","private":"not-for-logs"}'))
    monkeypatch.setattr(provider.urllib.request, 'urlopen', request)
    monkeypatch.setattr(provider.time, 'sleep', delays.append)
    with pytest.raises(RuntimeError, match='^DATA_HTTP_429_RATE_LIMITED$'):
        provider.KalshiProvider().request('/api/sports/prediction-markets/research-catalog')
    assert len(calls) == 4 and delays == [10, 20, 40]


def test_long_server_cooldown_is_not_retried_early(monkeypatch):
    def request(*args, **kwargs):
        raise urllib.error.HTTPError('https://quantura.studio', 429, 'busy', {'Retry-After': '300'}, io.BytesIO(b'{"error":"rate_limited"}'))
    delays = []
    monkeypatch.setattr(provider.urllib.request, 'urlopen', request)
    monkeypatch.setattr(provider.time, 'sleep', delays.append)
    with pytest.raises(RuntimeError, match='^DATA_HTTP_429_RATE_LIMITED$'):
        provider.KalshiProvider().request('/api/sports/prediction-markets/research-catalog')
    assert delays == []
