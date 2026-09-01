(function () {
  "use strict";

  const form = document.getElementById("api-key-form");
  if (!form) return;
  const scopesHost = document.getElementById("api-key-scopes");
  const listHost = document.getElementById("api-key-list");
  const secretHost = document.getElementById("api-key-secret");
  const statusHost = document.getElementById("api-key-status");
  const refreshButton = document.getElementById("api-key-refresh");
  const readDefaults = new Set(["account:read", "workspaces:read", "forecasts:read", "predictions:read", "screener:read", "market_data:read", "options:read", "sports:read", "datasets:read", "backtests:read", "api_usage:read", "sagemaker:read"]);

  const escapeHtml = (value) => String(value ?? "").replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
  const setStatus = (message, tone = "") => { statusHost.textContent = message; statusHost.dataset.tone = tone; };

  async function sessionRequest(path, options = {}) {
    const user = window.firebase?.auth?.().currentUser;
    if (!user || user.isAnonymous) throw new Error("Sign in with a full account to manage API keys.");
    const token = await user.getIdToken();
    const response = await fetch(path, {
      ...options,
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}), ...(options.headers || {}) },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.error?.message || "The API-key request could not be completed.");
    return payload.data;
  }

  function formatDate(value) {
    if (!value) return "Never";
    const parsed = new Date(value);
    return Number.isFinite(parsed.getTime()) ? parsed.toLocaleString() : "Unavailable";
  }

  async function loadScopes() {
    const scopes = await sessionRequest("/api/account/api-keys/scopes");
    scopesHost.innerHTML = scopes.map((scope) => `<label class="api-scope-option"><input type="checkbox" name="api_scope" value="${escapeHtml(scope)}" ${readDefaults.has(scope) ? "checked" : ""} /><span><strong>${escapeHtml(scope)}</strong><small>${scope.endsWith(":write") || scope.endsWith(":run") || scope.endsWith(":execute") ? "Allows a state-changing operation where role and plan permit." : "Read-only access; workspace authorization still applies."}</small></span></label>`).join("");
  }

  function showSecret(created) {
    secretHost.classList.remove("hidden");
    secretHost.innerHTML = `<strong>Copy this key now. It will not be shown again.</strong><code>${escapeHtml(created.key)}</code><div class="hero-actions"><button class="cta secondary small" type="button" data-copy-api-key>Copy key</button><button class="task-chip" type="button" data-dismiss-api-key>Dismiss</button></div>`;
    secretHost.querySelector("[data-copy-api-key]")?.addEventListener("click", async () => {
      await navigator.clipboard.writeText(created.key);
      setStatus("API key copied. Store it in a password manager or secret store.", "success");
    });
    secretHost.querySelector("[data-dismiss-api-key]")?.addEventListener("click", () => {
      secretHost.textContent = "";
      secretHost.classList.add("hidden");
    });
  }

  function renderKeys(keys) {
    if (!keys.length) {
      listHost.innerHTML = '<div class="empty-state">No API keys yet.</div>';
      return;
    }
    listHost.innerHTML = keys.sort((left, right) => String(right.created_at).localeCompare(String(left.created_at))).map((key) => {
      const state = key.revoked_at ? "Revoked" : key.expires_at && Date.parse(key.expires_at) <= Date.now() ? "Expired" : "Active";
      const actions = state === "Active" ? `<button class="task-chip" type="button" data-replace-key="${escapeHtml(key.id)}">Replace</button><button class="task-chip danger" type="button" data-revoke-key="${escapeHtml(key.id)}">Revoke</button>` : "";
      return `<article class="order-card api-key-card"><div class="order-header"><div><div class="order-title">${escapeHtml(key.name || "API key")}</div><code>${escapeHtml(key.prefix || "qnt_live_")}…</code></div><span class="status ${state === "Active" ? "completed" : "pending"}">${state}</span></div><div class="order-meta"><div><strong>Created</strong> ${escapeHtml(formatDate(key.created_at))}</div><div><strong>Last used</strong> ${escapeHtml(formatDate(key.last_used_at))}</div><div><strong>Expires</strong> ${escapeHtml(formatDate(key.expires_at))}</div><div><strong>Scopes</strong> ${escapeHtml((key.scopes || []).join(", "))}</div></div><div class="order-actions">${actions}</div></article>`;
    }).join("");
  }

  async function loadKeys() {
    refreshButton.disabled = true;
    try {
      const [keys] = await Promise.all([sessionRequest("/api/account/api-keys"), scopesHost.querySelector("input") ? Promise.resolve() : loadScopes()]);
      renderKeys(keys);
      setStatus("API keys are hashed server-side. Workspace access is resolved at request time.");
    } catch (error) {
      listHost.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
      setStatus(error.message, "error");
    } finally { refreshButton.disabled = false; }
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const submit = form.querySelector('button[type="submit"]');
    const scopes = [...form.querySelectorAll('input[name="api_scope"]:checked')].map((input) => input.value);
    if (!scopes.length) { setStatus("Select at least one scope.", "error"); return; }
    submit.disabled = true;
    setStatus("Creating a securely hashed API key…");
    try {
      const expiration = document.getElementById("api-key-expiration").value;
      const created = await sessionRequest("/api/account/api-keys", { method: "POST", body: JSON.stringify({ name: document.getElementById("api-key-name").value, scopes, expires_at: expiration ? `${expiration}T23:59:59.999Z` : null }) });
      showSecret(created);
      form.reset();
      scopesHost.querySelectorAll('input[name="api_scope"]').forEach((input) => { input.checked = readDefaults.has(input.value); });
      setStatus("API key created. Copy the secret before dismissing it.", "success");
      void window.QuanturaProductivity?.record("api_key_created", "API key created", { resource_type: "api_key", resource_id: created.id });
      await loadKeys();
    } catch (error) { setStatus(error.message, "error"); }
    finally { submit.disabled = false; }
  });

  listHost.addEventListener("click", async (event) => {
    const revoke = event.target.closest("[data-revoke-key]");
    const replace = event.target.closest("[data-replace-key]");
    const id = revoke?.dataset.revokeKey || replace?.dataset.replaceKey;
    if (!id) return;
    event.target.disabled = true;
    try {
      if (replace) {
        const created = await sessionRequest(`/api/account/api-keys/${encodeURIComponent(id)}/replace`, { method: "POST" });
        showSecret(created);
        setStatus("Replacement created; the previous key was revoked immediately.", "success");
      } else {
        await sessionRequest(`/api/account/api-keys/${encodeURIComponent(id)}`, { method: "DELETE" });
        setStatus("API key revoked immediately.", "success");
        void window.QuanturaProductivity?.record("api_key_revoked", "API key revoked", { resource_type: "api_key", resource_id: id });
      }
      await loadKeys();
    } catch (error) { setStatus(error.message, "error"); event.target.disabled = false; }
  });

  refreshButton.addEventListener("click", loadKeys);
  window.addEventListener("quantura:panel", (event) => { if (event.detail?.panel === "developer") void loadKeys(); });
  if (window.firebase?.auth) window.firebase.auth().onAuthStateChanged((user) => {
    if (user && !user.isAnonymous) void loadKeys();
    else {
      scopesHost.innerHTML = '<span class="small muted">Sign in to load available scopes.</span>';
      listHost.innerHTML = '<div class="empty-state">Sign in to manage API keys.</div>';
    }
  });
})();
