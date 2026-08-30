(() => {
  "use strict";

  const status = document.getElementById("forecast-admin-status");
  const result = (element, message, tone = "") => {
    if (!element) return;
    element.className = `small ${tone}`;
    element.textContent = message;
  };

  const currentUser = () => window.firebase?.auth?.().currentUser || null;
  const adminFetch = async (path, options = {}) => {
    const user = currentUser();
    if (!user) throw new Error("Sign in with the authorized admin account first.");
    const token = await user.getIdToken();
    const response = await fetch(path, {
      ...options,
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json", ...(options.headers || {}) },
      credentials: "same-origin",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.error?.message || payload?.error?.code || "Admin request failed.");
    return payload;
  };

  const asIso = (value) => {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) throw new Error("Enter valid UTC timestamps.");
    return parsed.toISOString();
  };

  let selectedForecast = null;

  const selectedId = () => {
    const value = String(document.querySelector("#forecast-draft-review-form [name=forecast_id]")?.value || "").trim();
    if (!value) throw new Error("Choose a forecast from the review queue first.");
    return value;
  };

  const populateSelectedForecast = (forecast) => {
    selectedForecast = forecast;
    const form = document.getElementById("forecast-draft-review-form");
    const set = (name, value) => {
      const input = form?.querySelector(`[name="${name}"]`);
      if (input) input.value = value ?? "";
    };
    set("forecast_id", forecast.forecast_id);
    set("current_probability", forecast.probability);
    set("question", forecast.question);
    set("possible_future_headline", forecast.possible_future_headline);
    set("short_summary", forecast.short_summary);
    set("resolution_rule", forecast.resolution?.rule);
    set("resolution_source", forecast.resolution?.source);
    set("review_status", forecast.review_status || "needs_review");
    const summary = document.getElementById("forecast-selected-summary");
    summary.textContent = `${forecast.forecast_id} · ${forecast.status} · ${Math.round(Number(forecast.probability) * 100)}% · ${forecast.question}`;
    const provenance = document.getElementById("forecast-selected-provenance");
    provenance.textContent = JSON.stringify({ model: forecast.model, provenance: forecast.provenance, evidence: forecast.evidence, structured_reasoning: forecast.structured_reasoning }, null, 2);
    form?.querySelectorAll("textarea, input:not([type=hidden]), select").forEach((control) => {
      control.disabled = forecast.status !== "draft";
    });
    const publish = document.getElementById("forecast-publish");
    if (publish) publish.disabled = forecast.status !== "draft" || forecast.review_status !== "approved";
  };

  const renderForecastList = (rows) => {
    const host = document.getElementById("forecast-admin-list");
    host.innerHTML = "";
    if (!rows.length) {
      const empty = document.createElement("p");
      empty.className = "small muted";
      empty.textContent = "No forecasts are available in the review queue.";
      host.appendChild(empty);
      return;
    }
    rows.forEach((forecast) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "forecast-admin-list-item";
      const title = document.createElement("strong");
      title.textContent = forecast.question;
      const meta = document.createElement("span");
      meta.textContent = `${forecast.status} · ${Math.round(Number(forecast.probability) * 100)}% · ${forecast.entity?.name || forecast.category}`;
      button.append(title, meta);
      button.addEventListener("click", () => populateSelectedForecast(forecast));
      host.appendChild(button);
    });
  };

  const refreshForecastList = async () => {
    const host = document.getElementById("forecast-admin-list");
    result(host, "Loading reviewed forecast records…");
    const response = await adminFetch("/api/forecasts/admin/list?limit=100");
    renderForecastList(response.data || []);
    if (selectedForecast) {
      const fresh = (response.data || []).find((item) => item.forecast_id === selectedForecast.forecast_id);
      if (fresh) populateSelectedForecast(fresh);
    }
  };

  const refreshApiKeys = async () => {
    const host = document.getElementById("forecast-key-list");
    result(host, "Loading scoped keys…");
    const response = await adminFetch("/api/forecasts/admin/api-keys");
    host.innerHTML = "";
    (response.data || []).forEach((key) => {
      const row = document.createElement("div");
      row.className = "forecast-admin-list-item";
      const title = document.createElement("strong");
      title.textContent = `${key.label || "API key"} · ${key.key_prefix}`;
      const meta = document.createElement("span");
      meta.textContent = `${key.customer_id} · ${(key.scopes || []).join(", ")} · ${key.revoked_at ? "revoked" : "active"}`;
      row.append(title, meta);
      if (!key.revoked_at) {
        const revoke = document.createElement("button");
        revoke.className = "cta secondary";
        revoke.type = "button";
        revoke.textContent = "Revoke";
        revoke.addEventListener("click", async () => {
          if (!window.confirm(`Revoke ${key.key_prefix}? Access will stop immediately.`)) return;
          await adminFetch(`/api/forecasts/admin/api-keys/${encodeURIComponent(key.key_id)}/revoke`, { method: "POST", body: "{}" });
          await refreshApiKeys();
        });
        row.appendChild(revoke);
      }
      host.appendChild(row);
    });
    if (!(response.data || []).length) result(host, "No enterprise API keys have been created.");
  };

  document.addEventListener("DOMContentLoaded", () => {
    if (window.firebase?.auth) {
      window.firebase.auth().onAuthStateChanged((user) => {
        status.textContent = user ? `Signed in as ${user.email || user.uid}. Admin permissions are verified by the server.` : "Sign in with an authorized admin account.";
        if (user) {
          refreshForecastList().catch((error) => result(document.getElementById("forecast-admin-list"), error.message, "negative"));
          refreshApiKeys().catch((error) => result(document.getElementById("forecast-key-list"), error.message, "negative"));
        }
      });
    }

    document.getElementById("forecast-create-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = event.currentTarget;
      const data = new FormData(form);
      try {
        const evidence = JSON.parse(String(data.get("evidence_json") || "[]"));
        if (!Array.isArray(evidence)) throw new Error("Evidence JSON must be an array.");
        const payload = {
          category: data.get("category"),
          slug: data.get("slug"),
          entity_name: data.get("entity_name"),
          entity_id: data.get("entity_id"),
          entity_type: data.get("entity_type"),
          ticker: data.get("ticker"),
          question: data.get("question"),
          possible_future_headline: data.get("possible_future_headline"),
          short_summary: data.get("short_summary"),
          probability: Number(data.get("probability")),
          input_cutoff_at: asIso(data.get("input_cutoff_at")),
          resolution_deadline: asIso(data.get("resolution_deadline")),
          bull_case: data.get("bull_case"),
          base_case: data.get("base_case"),
          bear_case: data.get("bear_case"),
          reasoning_summary: data.get("reasoning_summary"),
          resolution_rule: data.get("resolution_rule"),
          resolution_source: data.get("resolution_source"),
          model_provider: data.get("model_provider"),
          model_name: data.get("model_name"),
          model_version: data.get("model_version"),
          forecast_method: data.get("forecast_method"),
          evidence,
          review_status: data.get("review_approved") ? "approved" : "needs_review",
        };
        result(status, "Creating validated draft…");
        const response = await adminFetch("/api/forecasts/admin", { method: "POST", body: JSON.stringify(payload) });
        result(status, `Draft ${response.data.forecast_id} created. Publication remains a separate explicit action.`, "positive");
        await refreshForecastList();
      } catch (error) {
        result(status, error.message || "Unable to create draft.", "negative");
      }
    });

    document.getElementById("forecast-refresh-list")?.addEventListener("click", () => {
      refreshForecastList().catch((error) => result(document.getElementById("forecast-admin-list"), error.message, "negative"));
    });

    document.getElementById("forecast-draft-review-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const output = document.getElementById("forecast-operation-result");
      const data = new FormData(event.currentTarget);
      try {
        result(output, "Saving draft-only review changes…");
        await adminFetch(`/api/forecasts/admin/${encodeURIComponent(selectedId())}/draft`, {
          method: "PUT",
          body: JSON.stringify({
            question: data.get("question"),
            current_probability: Number(data.get("current_probability")),
            possible_future_headline: data.get("possible_future_headline"),
            short_summary: data.get("short_summary"),
            resolution_rule: data.get("resolution_rule"),
            resolution_source: data.get("resolution_source"),
            review_status: data.get("review_status"),
          }),
        });
        result(output, "Draft review saved. No published history was changed.", "positive");
        await refreshForecastList();
      } catch (error) {
        result(output, error.message || "Unable to save the draft.", "negative");
      }
    });

    document.getElementById("forecast-publish")?.addEventListener("click", async () => {
      const output = document.getElementById("forecast-operation-result");
      try {
        const id = selectedId();
        if (!window.confirm("Publish this reviewed forecast? Its initial snapshot and probability will become immutable.")) return;
        result(output, "Publishing immutable initial forecast…");
        await adminFetch(`/api/forecasts/admin/${encodeURIComponent(id)}/publish`, { method: "POST", body: "{}" });
        result(output, "Forecast published with an immutable initial probability record.", "positive");
        await refreshForecastList();
      } catch (error) {
        result(output, error.message || "Unable to publish the forecast.", "negative");
      }
    });

    document.getElementById("forecast-revision-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const output = document.getElementById("forecast-operation-result");
      const data = new FormData(event.currentTarget);
      try {
        const evidence = JSON.parse(String(data.get("evidence_json") || "[]"));
        if (!Array.isArray(evidence) || !evidence.length) throw new Error("New evidence JSON must be a non-empty array.");
        result(output, "Appending probability revision…");
        await adminFetch(`/api/forecasts/admin/${encodeURIComponent(selectedId())}/revisions`, {
          method: "POST",
          body: JSON.stringify({ probability: Number(data.get("probability")), reasoning_delta: data.get("reasoning_delta"), input_cutoff_at: asIso(data.get("input_cutoff_at")), model_provider: data.get("model_provider"), model_name: data.get("model_name"), model_version: data.get("model_version"), evidence }),
        });
        result(output, "Immutable probability revision appended.", "positive");
        event.currentTarget.reset();
        await refreshForecastList();
      } catch (error) {
        result(output, error.message || "Unable to append the revision.", "negative");
      }
    });

    document.getElementById("forecast-resolution-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const output = document.getElementById("forecast-operation-result");
      const data = new FormData(event.currentTarget);
      try {
        const evidence = JSON.parse(String(data.get("evidence_json") || "[]"));
        if (!Array.isArray(evidence)) throw new Error("Resolution evidence JSON must be an array.");
        if (!window.confirm("Record this outcome? Resolution status and scoring will be visible in the public record.")) return;
        result(output, "Recording outcome and scoring binary resolution…");
        await adminFetch(`/api/forecasts/admin/${encodeURIComponent(selectedId())}/resolve`, { method: "POST", body: JSON.stringify({ outcome: data.get("outcome"), notes: data.get("notes"), evidence }) });
        result(output, "Resolution recorded without rewriting probability history.", "positive");
        await refreshForecastList();
      } catch (error) {
        result(output, error.message || "Unable to resolve the forecast.", "negative");
      }
    });

    document.getElementById("forecast-amendment-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const output = document.getElementById("forecast-operation-result");
      const data = new FormData(event.currentTarget);
      try {
        result(output, "Creating transparent amendment record…");
        await adminFetch(`/api/forecasts/admin/${encodeURIComponent(selectedId())}/amendments`, { method: "POST", body: JSON.stringify({ field: data.get("field"), reason: data.get("reason"), corrected_display_value: data.get("corrected_display_value"), note: data.get("note") }) });
        result(output, "Amendment published separately from immutable forecast history.", "positive");
        event.currentTarget.reset();
      } catch (error) {
        result(output, error.message || "Unable to create the amendment.", "negative");
      }
    });

    document.getElementById("forecast-key-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = event.currentTarget;
      const data = new FormData(form);
      const output = document.getElementById("forecast-key-result");
      try {
        const scopes = Array.from(form.querySelector("[name=scopes]").selectedOptions).map((option) => option.value);
        const response = await adminFetch("/api/forecasts/admin/api-keys", {
          method: "POST",
          body: JSON.stringify({ customer_id: data.get("customer_id"), label: data.get("label"), scopes }),
        });
        output.innerHTML = "";
        const notice = document.createElement("p");
        notice.textContent = "Copy this key now. Only its secure hash is retained by Quantura.";
        const code = document.createElement("code");
        code.textContent = response.data.api_key;
        output.append(notice, code);
        await refreshApiKeys();
      } catch (error) {
        result(output, error.message || "Unable to create API key.", "negative");
      }
    });

    document.getElementById("forecast-refresh-keys")?.addEventListener("click", () => {
      refreshApiKeys().catch((error) => result(document.getElementById("forecast-key-list"), error.message, "negative"));
    });

    document.getElementById("forecast-run-lifecycle")?.addEventListener("click", async () => {
      const output = document.getElementById("forecast-job-result");
      try {
        result(output, "Running idempotent lifecycle stages…");
        const response = await adminFetch("/api/forecasts/admin/jobs/all", { method: "POST", body: "{}" });
        result(output, `Completed. Expired ${response.data.expired}; recalibrated ${response.data.calibration_records}; refreshed ${response.data.feed_records}; indexed ${response.data.search_records}.`, "positive");
      } catch (error) {
        result(output, error.message || "Lifecycle job failed.", "negative");
      }
    });
  });
})();
