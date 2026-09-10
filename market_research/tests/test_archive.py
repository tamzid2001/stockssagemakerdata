from types import SimpleNamespace
import pytest

from market_research.archive import collect, unique_games
from market_research.engine import iso
from market_research.provider import historical_range, QuanturaProvider


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
