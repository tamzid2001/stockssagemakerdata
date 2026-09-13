"""Research consumes the same Quantura adapter used by website downloads."""

import json
import urllib.parse
import urllib.request
import urllib.error
import time
import copy


class QuanturaProvider:
    source = "polymarket_us"

    def __init__(self, origin="https://quantura.studio"):
        if origin != "https://quantura.studio":
            raise ValueError("unapproved_api_origin")
        self.origin = origin
        self.cache = {}

    def request(self, path, body=None):
        # The only POST is Quantura's read-only dataset export. This adapter
        # cannot address an exchange order endpoint or accept arbitrary URLs.
        if path.split("?")[0] not in {
            "/api/sports/prediction-markets/research-catalog",
            "/api/sports/prediction-markets/export",
        }:
            raise ValueError("unapproved_read_endpoint")
        for attempt in range(4):
            request = urllib.request.Request(
                self.origin + path,
                data=json.dumps(body).encode() if body else None,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Quantura-Paper-Research/1",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                if (
                    error.code == 404
                    and path == "/api/sports/prediction-markets/export"
                ):
                    try:
                        payload = json.load(error)
                    except (ValueError, OSError):
                        payload = {}
                    if payload.get("error") == "no_data":
                        return {
                            "rows": [],
                            "metadata": {"availability": "missing_history"},
                        }
                if error.code in {429, 502, 503, 504} and attempt < 3:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError(f"DATA_HTTP_{error.code}") from None
            except (urllib.error.URLError, TimeoutError):
                if attempt < 3:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError("DATA_UNAVAILABLE") from None

    def discover(self, mode, max_pages=20, start_cursor="0"):
        contracts = {}
        cursor = str(start_cursor)
        if not cursor.isdigit() or not 0 <= int(cursor) <= 100000:
            raise ValueError("INVALID_DISCOVERY_CURSOR")
        seen = set()
        events = 0
        for _ in range(max_pages):
            if cursor in seen:
                raise RuntimeError("REPEATED_CURSOR")
            seen.add(cursor)
            result = self.request(
                "/api/sports/prediction-markets/research-catalog?"
                + urllib.parse.urlencode({"mode": mode, "cursor": cursor})
            )
            events += result["events_scanned"]
            for c in result["items"]:
                contracts[c["contractId"]] = c
            cursor = result.get("next_cursor")
            if cursor is None:
                break
            time.sleep(0.1)
        return list(contracts.values()), {
            "start_cursor": str(start_cursor),
            "events_scanned": events,
            "contracts_discovered": len(contracts),
            "discovery_truncated": cursor is not None,
            "next_cursor": cursor,
        }

    def history(self, contract, start, end, history_phase="both", history_lookback_minutes=0):
        from .engine import iso
        if history_phase not in {"both", "pregame", "in_game"} or type(history_lookback_minutes) is not int or not 0 <= history_lookback_minutes <= 129600:
            raise ValueError("INVALID_HISTORY_SELECTION")
        key = (contract["providerSymbol"], start, end, history_phase, history_lookback_minutes)
        cached = self.cache.get(key)
        if cached and time.monotonic() - cached[0] < 30:
            rows = copy.deepcopy(cached[1])
            for row in rows:
                row.setdefault("raw", {})["selected_position"] = contract["side"]
                row["selected_position"] = contract["side"]
            return rows
        result = self.request(
            "/api/sports/prediction-markets/export",
            {
                "source": self.source,
                "contracts": [contract],
                "start": iso(start),
                "end": iso(end),
                "frequency": "1m" if self.source == "kalshi" else "raw",
                "mode": "raw",
                "target": "price",
                "missing": "leave",
                "history_phase": history_phase,
                "history_lookback_minutes": history_lookback_minutes,
                "pregameOnly": history_phase == "pregame",
                "format": "json",
            },
        )
        self.cache = {
            k: v for k, v in self.cache.items() if time.monotonic() - v[0] < 30
        }
        self.cache[key] = (time.monotonic(), result["rows"])
        rows = copy.deepcopy(result["rows"])
        for row in rows:
            row["selected_position"] = contract["side"]
        return rows


class KalshiProvider(QuanturaProvider):
    """Same website download service; cursor addresses one Sports game at a time."""

    source = "kalshi"

    def __init__(self, series_ticker=""):
        super().__init__()
        self.series_ticker = series_ticker
        self.series = None

    def discover(self, mode, max_pages=1, start_cursor="0"):
        if mode == "live":
            contracts, cursor, coverage = {}, start_cursor, {}
            for _ in range(max_pages):
                items, coverage = self._page(mode, cursor)
                for item in items:
                    contracts[item["contractId"]] = {**item, "live": True,
                        "live_classification": "started_open_market_not_live_score_confirmation"}
                cursor = coverage["next_cursor"]
                if cursor is None:
                    break
            return list(contracts.values()), {**coverage, "contracts_discovered": len(contracts), "next_cursor": cursor}
        if mode != "historical" or max_pages != 1:
            raise ValueError("KALSHI_ARCHIVE_REQUIRES_SINGLE_EVENT_PAGES")
        return self._page(mode, start_cursor)

    def _page(self, mode, start_cursor):
        root = "/api/sports/prediction-markets/research-catalog?"
        if self.series is None:
            inventory = self.request(root + "source=kalshi")["series"]
            self.series_total = len(inventory)
            self.series = sorted(
                [
                    s["ticker"]
                    for s in inventory
                    if (
                        s["ticker"] == self.series_ticker
                        if self.series_ticker
                        else s["game_candidate"]
                    )
                ]
            )
            if not self.series:
                raise ValueError("NO_ELIGIBLE_SPORTS_SERIES")
        state = {} if str(start_cursor) in {"", "0"} else json.loads(start_cursor)
        if not isinstance(state, dict) or set(state) - {"series", "cursor"}:
            raise ValueError("INVALID_DISCOVERY_CURSOR")
        selected = state.get("series", self.series[0])
        if selected not in self.series or not isinstance(state.get("cursor", ""), str):
            raise ValueError("INVALID_DISCOVERY_CURSOR")
        result = self.request(
            root
            + urllib.parse.urlencode(
                {
                    "source": "kalshi",
                    "mode": mode,
                    "series_ticker": selected,
                    "cursor": state.get("cursor", ""),
                }
            )
        )
        if result.get("next_cursor"):
            next_state = {"series": selected, "cursor": result["next_cursor"]}
        else:
            index = self.series.index(selected) + 1
            next_state = (
                {"series": self.series[index], "cursor": ""}
                if index < len(self.series)
                else None
            )
        cursor = json.dumps(next_state, separators=(",", ":")) if next_state else None
        return result["items"], {
            "source": self.source,
            "series": selected,
            "series_discovered": self.series_total,
            "eligible_game_series": len(self.series),
            "events_scanned": result["events_scanned"],
            "contracts_discovered": len(result["items"]),
            "next_cursor": cursor,
            "discovery_truncated": cursor is not None,
            "classification": "game_match_moneyline_series_suffix_excluding_periods_props",
        }


def historical_range(contract, now):
    """Pregame plus game replay, bounded to 48h and never beyond collection time."""
    from .engine import stamp

    if contract.get("source") == "kalshi" and not contract.get("eventStart"):
        # Missing sports milestones do not erase raw data. Use the market's
        # genuine open/close metadata, without calling open time a game start.
        start = stamp(contract["availableFrom"])
        end = min(now, stamp(contract.get("resolutionTime") or contract["availableTo"]))
        # The shared web download service is bounded to 90 days.
        if end <= start or end - start > 90 * 86400:
            raise ValueError("KALSHI_HISTORY_RANGE_REQUIRES_PARTITIONING")
        return start, end
    event_start = stamp(contract["eventStart"])
    start = event_start - 501 * 60
    resolution = contract.get("resolutionTime")
    end = min(
        now,
        stamp(resolution) + 60 if resolution else event_start + 36 * 3600,
        start + 48 * 3600,
    )
    if end <= start:
        raise ValueError("INVALID_HISTORY_WINDOW")
    return start, end
