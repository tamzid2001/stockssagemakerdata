"""Verify Clerk sessions at the retained callable/HTTP compatibility boundary."""
import os
import re
import threading
import time

import jwt
import requests

ISSUER = "https://clerk.quantura.studio"
_jwks = jwt.PyJWKClient(ISSUER + "/.well-known/jwks.json", lifespan=300, timeout=5)
_users = {}
_lock = threading.Lock()


def _user(user_id):
    now = time.monotonic()
    with _lock:
        cached = _users.get(user_id)
        if cached and cached[0] > now:
            return cached[1]
    secret = os.environ.get("CLERK_SECRET_KEY")
    if not secret:
        raise ValueError("Clerk is not configured")
    response = requests.get("https://api.clerk.com/v1/users/" + user_id,
                            headers={"Authorization": "Bearer " + secret}, timeout=5)
    response.raise_for_status()
    user = response.json()
    if user.get("id") != user_id or user.get("banned") or user.get("locked"):
        raise ValueError("Inactive identity")
    with _lock:
        if len(_users) >= 2000:
            _users.pop(next(iter(_users)))
        _users[user_id] = (now + 60, user)
    return user


def verify_clerk_session(token, check_revoked=False):
    key = _jwks.get_signing_key_from_jwt(token).key
    claims = jwt.decode(token, key, algorithms=["RS256"], issuer=ISSUER,
                        options={"verify_aud": False, "require": ["exp", "iat", "nbf", "sub", "sid", "azp", "iss"]})
    origins = {"https://quantura.studio", "https://www.quantura.studio"}
    origins.update(x.strip() for x in os.environ.get("CLERK_AUTHORIZED_PARTIES", "").split(",") if x.strip())
    if os.environ.get("VERCEL_URL"):
        origins.add("https://" + os.environ["VERCEL_URL"])
    if claims["azp"] not in origins or not re.fullmatch(r"user_[A-Za-z0-9]+", claims["sub"]) or not re.fullmatch(r"sess_[A-Za-z0-9]+", claims["sid"]):
        raise ValueError("Invalid identity")
    user = _user(claims["sub"])
    if check_revoked:
        response = requests.get("https://api.clerk.com/v1/sessions/" + claims["sid"],
                                headers={"Authorization": "Bearer " + os.environ["CLERK_SECRET_KEY"]}, timeout=5)
        response.raise_for_status()
        session = response.json()
        if session.get("status") != "active" or session.get("user_id") != user["id"]:
            raise ValueError("Inactive session")
    uid = user.get("external_id") or user["id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", uid):
        raise ValueError("Invalid identity")
    email = next((x for x in user.get("email_addresses", []) if x.get("id") == user.get("primary_email_address_id")), {})
    verified = email.get("verification", {}).get("status") == "verified"
    return {**claims, "uid": uid, "sub": uid, "user_id": uid, "admin": False,
            "clerk_user_id": user["id"], "clerk_session_id": claims["sid"],
            "email": email.get("email_address", "") if verified else "", "email_verified": verified,
            "firebase": {"identities": {}, "sign_in_provider": "clerk"}}


def install_clerk_verifier(auth_module):
    """Firebase's callable wrapper uses this same module-level verifier.
    Non-Clerk native tokens retain Firebase's signature/revocation checks.
    A token claiming Clerk identity never falls back to another verifier.
    """
    legacy = auth_module.verify_id_token
    def verify(token, app=None, check_revoked=False, clock_skew_seconds=0):
        try:
            untrusted = jwt.decode(token, options={"verify_signature": False})
        except Exception:
            return legacy(token, app=app, check_revoked=check_revoked, clock_skew_seconds=clock_skew_seconds)
        issuer = str(untrusted.get("iss", ""))
        clerk = not issuer.startswith("https://securetoken.google.com/") and (
            issuer == ISSUER or str(untrusted.get("sub", "")).startswith("user_") or bool(untrusted.get("sid")))
        if not clerk:
            return legacy(token, app=app, check_revoked=check_revoked, clock_skew_seconds=clock_skew_seconds)
        try:
            return verify_clerk_session(token, check_revoked=check_revoked)
        except Exception as error:
            raise auth_module.InvalidIdTokenError("Invalid Clerk session") from error
    auth_module.verify_id_token = verify
