import importlib.util
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "functions_legacy_vercel" / "openai_ads.py"
spec = importlib.util.spec_from_file_location("openai_ads_under_test", SOURCE)
ads = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ads)


def test_contact_conversion_preserves_raw_attribution_and_deduplication(monkeypatch):
    calls = []
    monkeypatch.setattr(ads.requests, "post", lambda *a, **kw: calls.append((a, kw)))
    secrets = {"OPENAI_ADS_CONVERSIONS_API_KEY": "example-test-only", "OPENAI_ADS_PIXEL_ID": "fixture-pixel"}
    result = ads.report_contact_lead("contact123", {"consent": "granted", "sourceUrl": "https://quantura.studio/contact?private=yes#form", "oppref": "client"},
                                     {"Cookie": "__oppref=raw%2F+value; __obref=opaque-browser"}, secrets.get)
    assert result == "lead_contact123"
    event = calls[0][1]["json"]["events"][0]
    assert event["id"] == result
    assert event["type"] == "lead_created" and event["data"] == {"type": "customer_action"}
    assert event["source_url"] == "https://quantura.studio/contact"
    assert event["oppref"] == "raw%2F+value"
    assert event["user"] == {"obref": "opaque-browser"}
    assert event["opt_out"] is True
    assert calls[0][1]["json"]["validate_only"] is False
    assert calls[0][1]["timeout"] == (0.5, 0.5)


def test_consent_gpc_missing_secrets_and_transport_failure_never_fail_contact(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("measurement must not be sent")
    monkeypatch.setattr(ads.requests, "post", forbidden)
    for context, headers in [(None, {}), ({"consent": "denied"}, {}), ({"consent": "granted"}, {"Sec-GPC": "1"})]:
        assert ads.report_contact_lead("abc", context, headers, forbidden) is None
    assert ads.report_contact_lead("abc", {"consent": "granted"}, {}, lambda _: "") is None
    assert ads.report_contact_lead("abc", {"consent": "granted"}, {}, lambda _: "fixture-only") is None


def test_untrusted_source_urls_use_canonical_public_page():
    for value in ["javascript:alert(1)", "https://attacker.example/contact", "https://quantura.studio@attacker.example", "//attacker.example", "http://quantura.studio/contact"]:
        assert ads.sanitized_source_url(value) == "https://quantura.studio/contact"
