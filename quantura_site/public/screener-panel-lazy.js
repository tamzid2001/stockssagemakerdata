(function () {
  "use strict";
  let loaderPromise = null;

  function requested() {
    if (new URLSearchParams(window.location.search).get("panel") === "screener") return true;
    const panel = document.querySelector('[data-panel="screener"]');
    return Boolean(panel && !panel.classList.contains("hidden"));
  }

  function ensureLoader() {
    if (!requested()) return Promise.resolve();
    const host = document.getElementById("forecasting-screener-workspace");
    if (!host || host.dataset.mounted === "true") return Promise.resolve();
    if (!loaderPromise) {
      loaderPromise = new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = "/screener-workspace-loader.js?v=20260831a";
        script.async = true;
        script.dataset.quanturaScreenerLoader = "true";
        script.addEventListener("load", resolve, { once: true });
        script.addEventListener("error", reject, { once: true });
        document.head.appendChild(script);
      });
    }
    return loaderPromise.then(() => window.QuanturaScreenerWorkspace.mount(host));
  }

  document.addEventListener("click", (event) => {
    if (event.target.closest('[data-panel-target="screener"]')) window.setTimeout(() => void ensureLoader(), 0);
  });
  window.addEventListener("popstate", () => void ensureLoader());
  document.addEventListener("DOMContentLoaded", () => void ensureLoader());
})();
