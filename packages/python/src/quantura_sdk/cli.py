import argparse
import json
from pathlib import Path
import sys

from .client import Quantura, QuanturaError, DEFAULT_BASE_URL
from .oauth import login, logout, access_token


def main():
    parser = argparse.ArgumentParser(prog="quantura", description="Quantura API and OAuth CLI. Paid Pro or administrator API access required.")
    parser.add_argument("command", choices=["login", "logout", "whoami", "search", "models", "forecast", "get", "download", "history", "scout"])
    parser.add_argument("args", nargs="*")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--file")
    parser.add_argument("--output")
    parser.add_argument("--source", default="auto")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--key")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--all-pages", action="store_true")
    config = parser.parse_args()
    def body():
        if not config.file:
            parser.error("Provide --file with a JSON request.")
        return json.loads(Path(config.file).read_text())
    def forecast_id():
        if len(config.args) != 1:
            parser.error("Provide a forecast ID.")
        return config.args[0]
    try:
        if config.command == "login":
            login(no_browser=config.no_browser)
            print("Signed in to Quantura.")
            return
        if config.command == "logout":
            confirmed = logout()
            print("Signed out; OAuth grant revoked." if confirmed else "Local credentials removed. Remote revocation was not confirmed.")
            return
        client = Quantura(lambda: access_token(config.base_url), base_url=config.base_url)
        if config.command == "whoami":
            result = client.access()
        elif config.command == "search":
            if not config.args:
                parser.error("Provide a search query.")
            result = client.search(" ".join(config.args), source=config.source, limit=config.limit)
        elif config.command == "models":
            result = client.models()
        elif config.command == "forecast":
            result = client.create_forecast(body(), idempotency_key=config.key)
        elif config.command == "get":
            result = client.wait_for_forecast(forecast_id()) if config.wait else client.get_forecast(forecast_id())
        elif config.command == "download":
            if not config.output:
                parser.error("Provide --output for the forecast CSV.")
            result = client.download_forecast(forecast_id())
        elif config.command == "history":
            request = body()
            result = list(client.history_pages(request)) if config.all_pages else client.history(request)
        else:
            question = body()
            result = client.ask_scout(question["context"], question["question"],
                                      conversation_id=question.get("conversation_id"), turn_id=question.get("turn_id"))
        output = result if isinstance(result, bytes) else (json.dumps(result, indent=2) + "\n").encode()
        if config.output:
            Path(config.output).write_bytes(output)
            print("Saved " + config.output, file=sys.stderr)
        else:
            sys.stdout.buffer.write(output)
    except (QuanturaError, ValueError, OSError, KeyError) as error:
        reference = getattr(error, "request_id", None)
        print(f"{getattr(error, 'code', 'ERROR')}: {error}" + (f" · Reference {reference}" if reference else ""), file=sys.stderr)
        sys.exit(1)
