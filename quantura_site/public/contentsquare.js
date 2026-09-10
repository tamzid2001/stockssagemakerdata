/* Consent-gated public-page analytics. Never loaded on private research surfaces. */
(() => {
  "use strict";
  const permitted = ["/", "/about", "/pricing"].includes(location.pathname.replace(/\/$/, "") || "/");
  let started = false;
  function update() {
    let consent = false;
    try { consent = localStorage.getItem("quantura_cookie_consent") === "accepted"; } catch { return; }
    if (!consent) { if (started) window._uxa?.push(["optout"]); return; }
    if (started || !permitted || location.search || location.hash || navigator.globalPrivacyControl === true) return;
    started = true;
    window._uxa = window._uxa || [];
    window._uxa.push(["setQuery", ""]);
    window._uxa.push(["referrer:removeQueryString"]);
    window._uxa.push(["setPIISelectors", { PIISelectors: [".header, .modal, dialog, .toast, form, [data-cs-mask], #quantura-support"] }]);
    // Private referrers may contain resource IDs; never collect their original URLs.
    try {
      const referrer = new URL(document.referrer);
      const maskedPath = referrer.pathname.split("/").filter(Boolean).map((_, index) => `:PATH_${index}`).join("/");
      window._uxa.push(["referrer:maskUrl", `${referrer.origin}/${maskedPath}`]);
    } catch { /* No valid referrer to redact. */ }
    const script = document.createElement("script");
    script.defer = true; script.src = "https://t.contentsquare.net/uxa/c64e9d54ef9d4.js";
    script.dataset.quanturaContentsquare = "true";
    script.onerror = () => { started = false; script.remove(); };
    document.head.append(script);
  }
  document.addEventListener("quantura:consent-change", update);
  window.addEventListener("storage", event => { if (event.key === "quantura_cookie_consent") update(); });
  update();
})();
