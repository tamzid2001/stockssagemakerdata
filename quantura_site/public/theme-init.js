/* Before CSS: avoid painting the wrong theme while the app bundle loads. */
(() => {
  "use strict";
  let theme;
  try { theme = localStorage.getItem("quantura_theme"); } catch (_) { /* Storage may be disabled. */ }
  if (theme !== "light" && theme !== "dark") theme = matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  document.documentElement.dataset.theme = theme;
})();
