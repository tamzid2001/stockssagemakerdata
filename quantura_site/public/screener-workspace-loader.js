(function () {
  "use strict";
  let mountPromise = null;

  function ensureStylesheet() {
    if (document.querySelector('link[data-quantura-screener-workspace]')) return;
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = "/screener.css?v=20260831a";
    link.dataset.quanturaScreenerWorkspace = "true";
    document.head.appendChild(link);
  }

  function loadBehavior() {
    if (document.querySelector('script[data-quantura-screener-workspace]')) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = "/screener.js?v=20260831a";
      script.async = true;
      script.dataset.quanturaScreenerWorkspace = "true";
      script.addEventListener("load", resolve, { once: true });
      script.addEventListener("error", () => reject(new Error("Unable to load the Screener workspace.")), { once: true });
      document.head.appendChild(script);
    });
  }

  async function mount(root) {
    const host = typeof root === "string" ? document.querySelector(root) : root;
    if (!host) throw new Error("Screener workspace host was not found.");
    if (host.dataset.mounted === "true") return;
    if (mountPromise) return mountPromise;
    mountPromise = (async () => {
      host.classList.add("quant-screener-page");
      host.setAttribute("aria-busy", "true");
      host.innerHTML = '<div class="card"><p class="small muted">Loading the validated Screener workspace…</p></div>';
      ensureStylesheet();
      const response = await fetch("/screener?workspace_fragment=1", { headers: { Accept: "text/html" } });
      if (!response.ok) throw new Error(`Unable to load the Screener workspace (${response.status}).`);
      const copy = new DOMParser().parseFromString(await response.text(), "text/html");
      const workspace = copy.querySelector(".qs-main");
      if (!workspace) throw new Error("The Screener workspace response was incomplete.");
      host.replaceChildren(workspace);
      host.dataset.mounted = "true";
      host.removeAttribute("aria-busy");
      await loadBehavior();
      host.dispatchEvent(new CustomEvent("quantura:screener-mounted", { bubbles: true }));
    })().catch((error) => {
      mountPromise = null;
      host.removeAttribute("aria-busy");
      host.innerHTML = '<div class="card error-state"><strong>Screener unavailable</strong><p class="small">The workspace could not be loaded.</p><button class="cta secondary small" type="button" data-screener-retry>Retry</button></div>';
      host.querySelector("[data-screener-retry]")?.addEventListener("click", () => void mount(host), { once: true });
      throw error;
    });
    return mountPromise;
  }

  window.QuanturaScreenerWorkspace = Object.freeze({ mount });
})();
