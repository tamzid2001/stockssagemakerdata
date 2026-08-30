import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = ROOT.parent


def test_aikido_firewall_loads_before_express_and_stays_server_side():
    entrypoint = (ROOT / "functions_explore" / "src" / "index.ts").read_text(encoding="utf-8")
    vercel_entrypoint = (ROOT / "functions_explore" / "index.ts").read_text(encoding="utf-8")
    package = json.loads((ROOT / "functions_explore" / "package.json").read_text(encoding="utf-8"))
    root_env = (REPOSITORY / ".env.example").read_text(encoding="utf-8")
    function_env = (ROOT / "functions_explore" / ".env.example").read_text(encoding="utf-8")

    assert entrypoint.index('import "@aikidosec/firewall";') < entrypoint.index('from "express"')
    assert vercel_entrypoint.index('import "@aikidosec/firewall";') < vercel_entrypoint.index('from "express"')
    assert package["dependencies"]["@aikidosec/firewall"] == "1.8.37"
    assert "AIKIDO_TOKEN=" in root_env
    assert "AIKIDO_BLOCK=false" in root_env
    assert "AIKIDO_NODE_OPTIONS=-r @aikidosec/firewall/instrument" in root_env
    assert "AIKIDO_TOKEN=" in function_env
    assert "NEXT_PUBLIC_AIKIDO" not in root_env


def test_aikido_secret_is_scoped_to_express_deployments():
    deploy = (REPOSITORY / "deploy.sh").read_text(encoding="utf-8")

    assert 'AIKIDO_BLOCK="${AIKIDO_BLOCK:-false}"' in deploy
    assert 'AIKIDO_NODE_OPTIONS="${AIKIDO_NODE_OPTIONS:--r @aikidosec/firewall/instrument}"' in deploy
    assert 'AIKIDO_SECRET_BINDING="AIKIDO_TOKEN=projects/${PROJECT_ID}/secrets/AIKIDO_TOKEN:latest"' in deploy
    assert "EXPRESS_EXTRA_FLAGS" in deploy
    assert deploy.count('${EXPRESS_EXTRA_FLAGS[@]+"${EXPRESS_EXTRA_FLAGS[@]}"}') == 2
    assert deploy.count("NODE_OPTIONS=${AIKIDO_NODE_OPTIONS}") == 2


def test_public_aikido_badge_is_accessible_without_certification_claims():
    homepage = (ROOT / "pages" / "index.html").read_text(encoding="utf-8")

    assert "https://app.aikido.dev/audit-report/external/" in homepage
    assert "Aikido Security Audit Report" in homepage
    assert 'rel="noopener noreferrer"' in homepage
    assert "SOC 2 certified" not in homepage


def test_existing_password_account_falls_back_before_provider_recovery():
    client = (ROOT / "public" / "app.js").read_text(encoding="utf-8")
    function_start = client.index("const linkOrSignInWithCredential = async")
    function_end = client.index("const createEmailPasswordAccount = async", function_start)
    function_body = client[function_start:function_end]

    password_fallback = function_body.index('String(methodHint || "").trim().toLowerCase() === "password"')
    fallback_call = function_body.index("const result = await fallbackSignIn();", password_fallback)
    generic_recovery = function_body.index("recoverFromAuthCollision(linkError", fallback_call)

    assert password_fallback < fallback_call < generic_recovery


def test_untracked_duplicate_html_cannot_shadow_ssr_routes():
    firebase = json.loads((ROOT / "firebase.json").read_text(encoding="utf-8"))
    ignored = set(firebase["hosting"]["ignore"])

    assert "index.html" in ignored
    assert "screener.html" in ignored
    assert "**/* 2.*" in ignored
