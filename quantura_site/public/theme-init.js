/* Resolve the theme before CSS paints. Explicit choices override the system. */
(() => {
  "use strict";
  const preference = matchMedia("(prefers-color-scheme: dark)");
  const stored = () => { try { return localStorage.getItem("quantura_theme"); } catch (_) { return null; } };
  const apply = theme => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    document.querySelector('meta[name="theme-color"]')?.setAttribute("content", theme === "dark" ? "#07101f" : "#f4f7fb");
  };
  const choice = stored();
  apply(["light", "dark"].includes(choice) ? choice : preference.matches ? "dark" : "light");
  preference.addEventListener?.("change", event => {
    if (["light", "dark"].includes(stored())) return;
    const theme = event.matches ? "dark" : "light";
    apply(theme);
    document.dispatchEvent(new CustomEvent("quantura:theme-change", {detail:{theme}}));
  });
  document.addEventListener("quantura:theme-change", event => apply(event.detail.theme));
})();
