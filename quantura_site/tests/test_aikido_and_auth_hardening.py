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

    # The shared Google Functions module includes non-HTTP Pub/Sub exports, so
    # Zen stays scoped to request-serving processes. Google HTTP functions use
    # NODE_OPTIONS below, while Vercel has an Express-only entrypoint that can
    # safely load Zen before Express.
    assert '@aikidosec/firewall' not in entrypoint
    assert vercel_entrypoint.index('import "@aikidosec/firewall";') < vercel_entrypoint.index('from "express"')
    assert 'import helmet from "helmet";' in entrypoint
    assert entrypoint.index("app.use(helmet());") < entrypoint.index("app.use(cors(")
    assert 'import helmet from "helmet";' in vercel_entrypoint
    assert vercel_entrypoint.index("app.use(helmet());") < vercel_entrypoint.index("app.use(shopApi)")
    assert package["dependencies"]["@aikidosec/firewall"] == "1.8.37"
    assert package["dependencies"]["helmet"] == "8.1.0"
    assert "AIKIDO_TOKEN=" in root_env
    assert "AIKIDO_BLOCK=false" in root_env
    assert "AIKIDO_NODE_OPTIONS=-r @aikidosec/firewall/instrument" in root_env
    assert "AIKIDO_TOKEN=" in function_env
    assert "NEXT_PUBLIC_AIKIDO" not in root_env


def test_gitlab_observability_initializes_before_express_and_stays_server_side():
    vercel_entrypoint = (ROOT / "functions_explore" / "index.ts").read_text(encoding="utf-8")
    instrumentation = (ROOT / "functions_explore" / "src" / "gitlabObservability.ts").read_text(encoding="utf-8")
    package = json.loads((ROOT / "functions_explore" / "package.json").read_text(encoding="utf-8"))
    root_env = (REPOSITORY / ".env.example").read_text(encoding="utf-8")

    assert vercel_entrypoint.index('import "./src/gitlabObservability";') < vercel_entrypoint.index('from "express"')
    assert package["dependencies"]["@opentelemetry/sdk-trace-node"] == "2.11.0"
    assert "new HttpInstrumentation()" in instrumentation
    assert "new ExpressInstrumentation()" in instrumentation
    assert '"gitlab.project.id"' in instrumentation
    assert "ATTR_SERVICE_VERSION" in instrumentation
    assert "ATTR_DEPLOYMENT_ENVIRONMENT_NAME" in instrumentation
    assert "GITLAB_OBSERVABILITY_ENABLED=false" in root_env
    assert "GITLAB_OTEL_HTTP_ENDPOINT=https://140928869.otel.gitlab-o11y.com:14318" in root_env
    assert "NEXT_PUBLIC_GITLAB" not in root_env


def test_aikido_secret_is_scoped_to_express_deployments():
    deploy = (REPOSITORY / "deploy.sh").read_text(encoding="utf-8")

    assert 'AIKIDO_BLOCK="${AIKIDO_BLOCK:-false}"' in deploy
    assert 'AIKIDO_NODE_OPTIONS="${AIKIDO_NODE_OPTIONS:--r @aikidosec/firewall/instrument}"' in deploy
    assert 'AIKIDO_SECRET_BINDING="AIKIDO_TOKEN=projects/${PROJECT_ID}/secrets/AIKIDO_TOKEN:latest"' in deploy
    assert "EXPRESS_EXTRA_FLAGS" in deploy
    assert deploy.count('${EXPRESS_EXTRA_FLAGS[@]+"${EXPRESS_EXTRA_FLAGS[@]}"}') == 2
    assert deploy.count("NODE_OPTIONS=${AIKIDO_NODE_OPTIONS}") == 2


def test_production_deploy_defaults_to_vercel_and_archives_google_workflow():
    deploy = (REPOSITORY / "deploy.sh").read_text(encoding="utf-8")

    assert 'DEPLOY_PROVIDER="${DEPLOY_PROVIDER:-vercel}"' in deploy
    assert 'if [[ "${DEPLOY_PROVIDER}" == "vercel" ]]' in deploy
    assert "Quantura API" in deploy
    assert "Quantura web application" in deploy
    assert '--project "${project_name}" --cwd "${VERCEL_SOURCE_DIR}"' in deploy
    assert 'git -C "${ROOT_DIR}" archive HEAD | tar -x -C "${VERCEL_SOURCE_DIR}"' in deploy
    assert 'deploy_vercel_project "Quantura API" "quantura-api"' in deploy
    assert 'deploy_vercel_project "Quantura web application" "quantura"' in deploy
    assert '--cwd "${project_dir}"' not in deploy
    assert '--env "GITLAB_SERVICE_VERSION=${DEPLOY_COMMIT_SHA}"' in deploy
    assert '--meta "gitCommitSha=${DEPLOY_COMMIT_SHA}"' in deploy
    assert 'if [[ "${DEPLOY_PROVIDER}" != "google-legacy" ]]' in deploy
    assert "archived Google Cloud/Firebase deployment workflow" in deploy
    assert "--entry-point=onForecastCreated" not in deploy
    assert "--entry-point=onBacktestCreated" not in deploy


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
