"""Browser OAuth with PKCE; credentials shared with the Quantura npm CLI."""
from __future__ import annotations

import base64
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import secrets
import tempfile
import threading
import time
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, build_opener
import webbrowser

from .client import _NoRedirect

ISSUER = "https://clerk.quantura.studio"
RESOURCE = "https://quantura.studio"
CLIENT_ID = "pOqhsrxJxggZ8m8a"
REDIRECT = "http://127.0.0.1:8766/callback"


def credential_file():
    return Path(os.environ.get("QUANTURA_CONFIG_HOME", str(Path.home() / ".config" / "quantura"))) / "credentials.json"


def pkce():
    verifier = secrets.token_urlsafe(32)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    return verifier, challenge, secrets.token_urlsafe(32)


def valid_callback(url, state):
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    return parsed.path == "/callback" and hmac.compare_digest(query.get("state", [""])[0], state) and query.get("iss") == [ISSUER]


def _read():
    target = credential_file()
    if target.is_symlink():
        raise ValueError("Refusing a symlinked credentials file.")
    return json.loads(target.read_text())


def _save(value):
    target = credential_file()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=target.parent, delete=False) as stream:
            name = stream.name
            os.chmod(name, 0o600)
            json.dump(value, stream)
        os.replace(name, target)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def _json(url, body=None):
    headers = {}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = urlencode(body).encode()
    with build_opener(_NoRedirect()).open(Request(url, data=data, headers=headers), timeout=20) as response:
        return json.load(response)


def _metadata():
    value = _json(ISSUER + "/.well-known/oauth-authorization-server")
    if value.get("issuer") != ISSUER or "S256" not in value.get("code_challenge_methods_supported", []):
        raise ValueError("OAuth discovery is invalid.")
    for key in ("authorization_endpoint", "token_endpoint", "revocation_endpoint"):
        parsed = urlsplit(value[key])
        if parsed.scheme + "://" + parsed.netloc != ISSUER:
            raise ValueError("OAuth endpoint is invalid.")
    return value


def _token(endpoint, body):
    try:
        value = _json(endpoint, body)
    except Exception:
        raise ValueError("OAuth token exchange failed. Run quantura login again.") from None
    if not value.get("access_token") or str(value.get("token_type")).lower() != "bearer":
        raise ValueError("OAuth token response is invalid.")
    return value


def login(*, no_browser=False):
    metadata = _metadata()
    verifier, challenge, state = pkce()
    authorize = metadata["authorization_endpoint"] + "?" + urlencode({"client_id": CLIENT_ID, "redirect_uri": REDIRECT,
        "response_type": "code", "scope": "openid profile email offline_access", "resource": RESOURCE,
        "state": state, "code_challenge": challenge, "code_challenge_method": "S256"})
    outcome = {}
    class Callback(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Never log a callback authorization code.

        def do_GET(self):
            if not valid_callback(self.path, state):
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid OAuth callback.")
                return
            query = parse_qs(urlsplit(self.path).query)
            try:
                if query.get("error") or not query.get("code"):
                    raise ValueError("Sign-in was canceled.")
                value = _token(metadata["token_endpoint"], {"grant_type": "authorization_code", "client_id": CLIENT_ID,
                    "redirect_uri": REDIRECT, "resource": RESOURCE, "code": query["code"][0], "code_verifier": verifier})
                _save({**value, "client_id": CLIENT_ID, "resource": RESOURCE, "expires_at": time.time() + float(value.get("expires_in", 3600))})
                outcome["done"] = True
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"Signed in to Quantura. You can close this window and return to your terminal.")
            except Exception:
                outcome["error"] = "Sign-in could not complete. Run quantura login again."
                self.send_response(400)
                self.end_headers()
                self.wfile.write(outcome["error"].encode())

    with HTTPServer(("127.0.0.1", 8766), Callback) as server:
        server.timeout = 1
        print("Sign in and approve Quantura access in your browser:\n" + authorize)
        if not no_browser:
            webbrowser.open(authorize)
        deadline = time.monotonic() + 300
        while not outcome and time.monotonic() < deadline:
            server.handle_request()
    if not outcome.get("done"):
        raise ValueError(outcome.get("error", "Sign-in timed out after five minutes."))


_refresh_lock = threading.Lock()
def access_token(base_url=RESOURCE):
    if os.environ.get("QUANTURA_API_KEY") or os.environ.get("QUANTURA_ACCESS_TOKEN"):
        return os.environ.get("QUANTURA_API_KEY") or os.environ["QUANTURA_ACCESS_TOKEN"]
    with _refresh_lock:
        try:
            value = _read()
        except (OSError, ValueError):
            raise ValueError("Run quantura login or set QUANTURA_API_KEY.") from None
        parsed = urlsplit(base_url)
        if parsed.scheme + "://" + parsed.netloc != value.get("resource") or value.get("resource") != RESOURCE or value.get("client_id") != CLIENT_ID:
            raise ValueError("Saved OAuth credentials belong to another API origin.")
        if float(value.get("expires_at", 0)) > time.time() + 60:
            return value["access_token"]
        if not value.get("refresh_token"):
            raise ValueError("Sign-in expired. Run quantura login.")
        metadata = _metadata()
        try:
            fresh = _token(metadata["token_endpoint"], {"grant_type": "refresh_token", "client_id": CLIENT_ID,
                "resource": RESOURCE, "refresh_token": value["refresh_token"]})
        except ValueError:
            reread = _read()
            if reread.get("access_token") != value["access_token"] and reread.get("expires_at", 0) > time.time() + 60:
                return reread["access_token"]
            raise
        _save({**value, **fresh, "expires_at": time.time() + float(fresh.get("expires_in", 3600))})
        return fresh["access_token"]


def logout():
    revoked = False
    try:
        value, metadata = _read(), _metadata()
        data = urlencode({"client_id": CLIENT_ID, "token": value.get("refresh_token") or value["access_token"]}).encode()
        request = Request(metadata["revocation_endpoint"], data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        with build_opener(_NoRedirect()).open(request, timeout=20) as response:
            response.read()  # RFC 7009 revocation may return an empty successful body.
        revoked = True
    except Exception:
        pass
    credential_file().unlink(missing_ok=True)
    return revoked
