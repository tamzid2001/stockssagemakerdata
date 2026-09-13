import httpx
import pytest

from ensemble_forecasting.worker import WorkerApi


@pytest.mark.parametrize("action", ["progress", "complete", "fail"])
def test_callbacks_retry_transient_errors_without_reclaiming_or_recomputing(monkeypatch, action):
    monkeypatch.setattr("ensemble_forecasting.worker.time.sleep", lambda _: None)
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if len(calls) == 1:
            raise httpx.ReadTimeout("test interrupted connection", request=request)
        return httpx.Response(503 if len(calls) == 2 else 200)

    api = WorkerApi("https://example.invalid", "test-only", "fixture")
    api.client.close()
    api.client = httpx.Client(base_url=api.base_url, transport=httpx.MockTransport(handler))
    getattr(api, action)({"test": True})
    assert calls == [f"/api/internal/ensemble-forecasts/fixture/{action}"] * 3
    api.client.close()


@pytest.mark.parametrize("status", [401, 403, 409, 422])
def test_callbacks_never_retry_auth_conflicts_or_invalid_results(monkeypatch, status):
    calls = []
    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(status)
    api = WorkerApi("https://example.invalid", "test-only", "fixture")
    api.client.close()
    api.client = httpx.Client(base_url=api.base_url, transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        api.complete({})
    assert len(calls) == 1
    api.client.close()
