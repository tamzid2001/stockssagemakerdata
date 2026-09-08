"""Read-only, bounded GitLab Observability verification; never prints raw records/keys."""
import argparse
import json
import os
import time
import urllib.error
import urllib.request


def query_payload(signal, service, end):
    spec = {
        "name": "A", "signal": signal, "stepInterval": 60, "disabled": False,
        "filter": {"expression": f"service.name = '{service}'"},
        "aggregations": [{"expression": "count()"}],
    }
    if signal == "metrics":
        spec["aggregations"] = [{"metricName": "http.server.request.count", "timeAggregation": "max", "spaceAggregation": "sum"}]
    return {
        "start": end - 30 * 60 * 1000, "end": end, "requestType": "time_series",
        "compositeQuery": {"queries": [{"type": "builder_query", "spec": spec}]},
    }


def numeric_values(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "value" and isinstance(value, (int, float)):
                yield value
            elif isinstance(value, (list, dict)):
                yield from numeric_values(value)
    elif isinstance(node, list):
        for value in node:
            yield from numeric_values(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", default="quantura-api")
    args = parser.parse_args()
    if not args.service.replace("-", "").isalnum():
        parser.error("Service must contain only letters, digits and hyphens")
    key = os.environ.get("GITLAB_OBSERVABILITY_API_KEY", "")
    if not key:
        raise SystemExit("GITLAB_OBSERVABILITY_API_KEY is required")
    failed = False
    for signal in ("traces", "metrics", "logs"):
        req = urllib.request.Request(
            "https://140928869.gitlab-o11y.com/api/v5/query_range",
            headers={"SIGNOZ-API-KEY": key, "Content-Type": "application/json"},
            data=json.dumps(query_payload(signal, args.service, int(time.time() * 1000))).encode(),
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                payload = json.load(response)
                values = list(numeric_values(payload))
                present = any(value > 0 for value in values)
                print(json.dumps({"signal": signal, "service": args.service, "http_status": response.status, "data_points": len(values), "positive_data_found": present}))
                failed |= not present
        except urllib.error.HTTPError as error:
            print(json.dumps({"signal": signal, "http_status": error.code, "error": "QUERY_REJECTED"}))
            failed = True
        except (OSError, ValueError):
            print(json.dumps({"signal": signal, "error": "QUERY_UNAVAILABLE"}))
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
