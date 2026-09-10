"""Research consumes the same Quantura adapter used by website downloads."""

import json
import urllib.parse
import urllib.request
import urllib.error
import time
import copy


class QuanturaProvider:
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

    def history(self, contract, start, end):
        from .engine import iso

        key = (contract["providerSymbol"], start, end)
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
                "source": "polymarket_us",
                "contracts": [contract],
                "start": iso(start),
                "end": iso(end),
                "frequency": "raw",
                "mode": "raw",
                "target": "price",
                "missing": "leave",
                "pregameOnly": False,
                "format": "json",
            },
        )
        self.cache = {
            k: v for k, v in self.cache.items() if time.monotonic() - v[0] < 30
        }
        self.cache[key] = (time.monotonic(), result["rows"])
        return result["rows"]


def historical_range(contract, now):
    """Pregame plus game replay, bounded to 48h and never beyond collection time."""
    from .engine import stamp

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
