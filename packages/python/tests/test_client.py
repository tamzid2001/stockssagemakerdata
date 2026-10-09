import base64
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from quantura_sdk import Quantura, QuanturaError
from quantura_sdk.client import api_base
from quantura_sdk.oauth import pkce, valid_callback, _save, _read

class Opener:
    def __init__(self, values):
        self.values, self.calls = list(values), []

    def open(self, request, **kwargs):
        self.calls.append(request)
        value = self.values.pop(0)
        if isinstance(value, Exception):
            raise value
        return io.BytesIO(value if isinstance(value, bytes) else json.dumps(value).encode())

class Tests(unittest.TestCase):
    def test_root_url_and_token_forwarding_boundaries(self):
        self.assertEqual(api_base("https://quantura.studio"),"https://quantura.studio/api/v1")
        with self.assertRaises(ValueError):api_base("http://remote.test")
        client=Quantura("test",opener=Opener([]))
        for value in ("https://untrusted.test", "../secret", "%2e%2e/secret"):
            with self.assertRaises(ValueError):client.request(value)

    def test_forecast_body_and_idempotency_not_retried(self):
        opener=Opener([{"data":{"id":"test"}}]);client=Quantura("test",opener=opener)
        request={"source":{"symbol":"AAPL"},"history_lag_minutes":172800}
        client.create_forecast(request,idempotency_key="stable-key")
        self.assertEqual(json.loads(opener.calls[0].data),request)
        self.assertEqual(opener.calls[0].get_header("Idempotency-key"),"stable-key")
        broken=Opener([OSError("connection lost")]);client.opener=broken
        with self.assertRaises(OSError):client.create_forecast({})
        self.assertEqual(len(broken.calls),1)

    def test_cursor_settings_and_loop_detection(self):
        opener=Opener([{"rows":[],"next_cursor":"next"},{"rows":[],"next_cursor":None}]);client=Quantura("test",opener=opener)
        request={"source":"dukascopy","symbol":"XAUUSD","start":"2025-10-08","end":"2026-10-07","timeframe":"1Hour"}
        self.assertEqual(len(list(client.history_pages(request))),2)
        first,second=[json.loads(r.data) for r in opener.calls]
        self.assertEqual(second,{**first,"cursor":"next"})
        client.opener=Opener([{"next_cursor":"same"},{"next_cursor":"same"}])
        with self.assertRaisesRegex(QuanturaError,"stalled"):list(client.history_pages(request))

    def test_error_reference_and_non_json_success(self):
        error=HTTPError("url",403,"Forbidden",{},io.BytesIO(json.dumps({"error":{"code":"PAID_API_REQUIRED","request_id":"ref"}}).encode()))
        client=Quantura("test",opener=Opener([error]))
        with self.assertRaises(QuanturaError) as caught:client.models()
        self.assertEqual(caught.exception.status,403);self.assertEqual(caught.exception.request_id,"ref")
        client.opener=Opener([b"not json"])
        with self.assertRaisesRegex(QuanturaError,"JSON"):client.models()

    def test_pkce_state_issuer_and_private_shared_credentials(self):
        verifier,challenge,state=pkce()
        self.assertGreaterEqual(len(verifier),43)
        expected=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        self.assertEqual(challenge,expected)
        url=f"http://127.0.0.1:8766/callback?state={state}&iss=https%3A%2F%2Fclerk.quantura.studio"
        self.assertTrue(valid_callback(url,state));self.assertFalse(valid_callback(url,"wrong"))
        self.assertFalse(valid_callback(url.replace("clerk.quantura.studio","untrusted.test"),state))
        with tempfile.TemporaryDirectory() as directory,patch.dict("os.environ",{"QUANTURA_CONFIG_HOME":directory}):
            _save({"access_token":"fake-test-token"})
            self.assertEqual(_read(),{"access_token":"fake-test-token"})
            self.assertEqual((Path(directory)/"credentials.json").stat().st_mode & 0o777,0o600)

    def test_proof_download_preserves_committed_bytes(self):
        original=b'{"nonce":"salt","value":100}'
        opener=Opener([{"data":{"status":"stamped"}},{"data":{"content_matches":True,"stamp_found":True}},original])
        client=Quantura("test",opener=opener)
        client.stamp_forecast("forecast-1")
        self.assertTrue(client.verify_forecast("forecast-1")["data"]["stamp_found"])
        self.assertEqual(client.download_forecast_proof("forecast-1"),original)
        self.assertEqual(opener.calls[0].method,"POST")
        self.assertTrue(opener.calls[1].full_url.endswith("/proof?verify=true"))

if __name__=="__main__":unittest.main()
