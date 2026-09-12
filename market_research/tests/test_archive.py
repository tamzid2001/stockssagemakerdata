from types import SimpleNamespace
import pytest

from market_research.archive import collect, collect_batches, unique_games
from market_research.engine import iso
from market_research.provider import historical_range, QuanturaProvider, KalshiProvider


def contracts():
    return [
        {
            "marketId": str(i),
            "providerSymbol": f"game-{i}",
            "contractId": f"{i}-{side}",
            "side": side,
            "eventStart": iso(100000),
        }
        for i in range(3)
        for side in ("long", "short")
    ]


class Storage:
    def __init__(self):
        self.holder = "test"
        self.reports = []
        self.progress = []
        self.histories = {}

    def save_catalog(self, values, coverage, as_of):
        self.catalog = values, {"coverage": coverage, "as_of": as_of}
        return "a" * 64

    def load_catalog(self, _):
        return self.catalog

    def archived_history(self, c, start, end):
        return self.histories.get(c["providerSymbol"])

    def archive_history(self, c, start, end, rows):
        self.histories[c["providerSymbol"]] = rows
        return {"row_count": len(rows)}

    def archive_progress(self, catalog, index, contract, result, report):
        self.progress.append((index, result.copy(), report.copy()))

    def report(self, identifier, report):
        self.reports.append((identifier, report))


def test_archive_downloads_once_per_game_and_resumes_private_catalog(monkeypatch):
    monkeypatch.setattr("market_research.archive.time.sleep", lambda _: None)
    requests = []

    def history(c, start, end):
        requests.append(c["providerSymbol"])
        if c["marketId"] == "1":
            return []
        if c["marketId"] == "2":
            raise RuntimeError("unavailable")
        return [
            {"timestamp": iso(t), "long_price": 0.5, "short_price": 0.52}
            for t in (100001, 100061, 100121)
        ]

    provider = SimpleNamespace(
        discover=lambda *a: (contracts(), {"next_cursor": None}), history=history
    )
    store = Storage()
    args = SimpleNamespace(
        duration_minutes=3, catalog_id="", max_pages=10, start_cursor=0, market_offset=0
    )
    result = collect(args, store, provider, SimpleNamespace(check=lambda: None))
    assert requests == ["game-0", "game-1", "game-2"]
    assert result["games_discovered"] == 3 and result["attempted"] == 3
    assert result["missing"] == 1 and result["failed"] == 1
    assert result["at_least_two_observations"] == 1
    assert result["catalog_complete"] and not result["all_discovered_ranges_available"]
    args.catalog_id = "a" * 64
    args.market_offset = 1
    again = collect(args, store, provider, SimpleNamespace(check=lambda: None))
    assert again["cache_hits"] == 1 and again["failed"] == 1
    assert len(requests) == 4


def test_history_bounds_include_pregame_and_longer_than_six_hour_game():
    c = contracts()[0]
    start, end = historical_range(c, 500000)
    assert start == 100000 - 501 * 60
    assert end == 100000 + 36 * 3600
    assert end - start <= 48 * 3600
    assert historical_range({**c, "resolutionTime": iso(150000)}, 160000)[1] == 150060
    assert historical_range(c, 110000)[1] == 110000


def test_discovery_cursor_pagination_and_two_sides_share_one_game(monkeypatch):
    provider = QuanturaProvider()
    requests = []

    def page(path):
        requests.append(path)
        return {"items": contracts(), "events_scanned": 3, "next_cursor": None}

    monkeypatch.setattr(provider, "request", page)
    items, coverage = provider.discover("historical", 100, "2000")
    assert "cursor=2000" in requests[0]
    assert len(unique_games(items)) == 3 and not coverage["discovery_truncated"]
    with pytest.raises(ValueError):
        provider.discover("historical", start_cursor="../bad")


def test_streaming_archive_checkpoints_every_page_before_next_discovery(monkeypatch):
    monkeypatch.setattr("market_research.archive.time.sleep", lambda _: None)
    store = Storage()
    calls = []

    def discover(mode, pages, cursor):
        assert pages == 1
        calls.append(str(cursor))
        if len(calls) == 2:
            assert len(store.reports) == 1 and len(store.progress) == 4
        return contracts(), {"next_cursor": "100" if str(cursor) == "0" else None}

    args = SimpleNamespace(
        duration_minutes=5, catalog_id="", max_pages=10, start_cursor=0, market_offset=0
    )
    provider = SimpleNamespace(discover=discover, history=lambda *a: [])
    result = collect_batches(args, store, provider, SimpleNamespace(check=lambda: None))
    assert result["pages"] == 2 and result["completed"]
    assert calls == ["0", "100"]
    assert len({key for key, _ in store.reports}) == 2


def test_kalshi_series_event_cursor_and_independent_team_markets(monkeypatch):
    provider = KalshiProvider()

    def request(path):
        if "series_ticker" not in path:
            return {
                "series": [
                    {"ticker": "AGAME", "game_candidate": True},
                    {"ticker": "BGAME", "game_candidate": True},
                    {"ticker": "TOTAL", "game_candidate": False},
                ]
            }
        return {
            "items": [
                {"source": "kalshi", "providerSymbol": "TEAM-A", "side": "yes"},
                {"source": "kalshi", "providerSymbol": "TEAM-B", "side": "yes"},
            ],
            "events_scanned": 1,
            "next_cursor": None,
        }

    monkeypatch.setattr(provider, "request", request)
    items, coverage = provider.discover("historical")
    assert len(unique_games(items)) == 2
    assert coverage["series_discovered"] == 3 and coverage["eligible_game_series"] == 2
    assert coverage["series"] == "AGAME"
    _, second = provider.discover("historical", start_cursor=coverage["next_cursor"])
    assert second["series"] == "BGAME" and second["next_cursor"] is None


def test_archive_keeps_gaps_and_both_polymarket_side_quotes(monkeypatch):
    monkeypatch.setattr("market_research.archive.time.sleep", lambda _: None)
    rows = [
        {"timestamp": iso(t), "long_price": 0.4, "short_price": 0.62}
        for t in (100001, 100601)
    ]
    store = Storage()
    args = SimpleNamespace(
        duration_minutes=3, catalog_id="", max_pages=1, start_cursor=0, market_offset=0
    )
    collect(
        args,
        store,
        SimpleNamespace(
            discover=lambda *a: (contracts()[:2], {}), history=lambda *a: rows
        ),
        SimpleNamespace(check=lambda: None),
    )
    assert store.histories["game-0"] == rows
    assert len(store.histories["game-0"]) == 2  # no filled intervening minutes


def test_kalshi_live_reuses_catalog_with_explicit_live_mode(monkeypatch):
    provider = KalshiProvider()
    calls = []
    def request(path):
        calls.append(path)
        if "series_ticker" not in path:
            return {"series": [{"ticker": "AGAME", "game_candidate": True}]}
        return {"items": [{"contractId": "A:yes", "side": "yes"}], "events_scanned": 1, "next_cursor": None}
    monkeypatch.setattr(provider, "request", request)
    items, coverage = provider.discover("live", 2)
    assert items[0]["live"] is True
    assert "mode=live" in calls[-1] and coverage["next_cursor"] is None
