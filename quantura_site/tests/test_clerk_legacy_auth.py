import importlib.util
from pathlib import Path
import time
from types import SimpleNamespace
from unittest.mock import patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

spec = importlib.util.spec_from_file_location("quantura_clerk_legacy", Path(__file__).parents[1] / "functions_legacy_vercel/clerk_auth.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def signed(**changes):
    now = int(time.time())
    return jwt.encode({"iss": module.ISSUER, "azp": "https://quantura.studio", "sub": "user_Test", "sid": "sess_Test",
                       "iat": now, "nbf": now - 1, "exp": now + 60, **changes}, key, algorithm="RS256")


def test_clerk_compatibility_keeps_authoritative_imported_uid_and_verified_email():
    user = {"id": "user_Test", "external_id": "old_uid", "primary_email_address_id": "email_Test",
            "email_addresses": [{"id": "email_Test", "email_address": "hello@example.test", "verification": {"status": "verified"}}],
            "unsafe_metadata": {"uid": "victim", "admin": True}}
    with patch.object(module._jwks, "get_signing_key_from_jwt", return_value=SimpleNamespace(key=key.public_key())), patch.object(module, "_user", return_value=user):
        identity = module.verify_clerk_session(signed(admin=True))
        assert identity["uid"] == "old_uid"
        assert identity["admin"] is False
        assert identity["email_verified"] is True
        for changes in [{"iss": "https://attacker.example"}, {"azp": "https://attacker.example"}, {"exp": 1}, {"sid": "../../secret"}]:
            with pytest.raises((ValueError, jwt.InvalidTokenError)):
                module.verify_clerk_session(signed(**changes))


def test_failed_clerk_verification_never_falls_back_to_native_verifier():
    calls = []
    auth = SimpleNamespace(verify_id_token=lambda token, **kw: calls.append(token) or {"uid": "native"}, InvalidIdTokenError=ValueError)
    module.install_clerk_verifier(auth)
    with patch.object(module, "verify_clerk_session", side_effect=ValueError("invalid")):
        with pytest.raises(ValueError, match="Invalid Clerk session"):
            auth.verify_id_token(signed())
        assert calls == []
        assert auth.verify_id_token("native-token")["uid"] == "native"
