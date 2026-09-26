/* Installed-app launch treatment, dismissed when the page is ready. */
(() => {
  if (!matchMedia("(display-mode: standalone)").matches && !navigator.standalone) return;
  const root = document.documentElement;
  root.classList.add("pwa-launching");
  const style = document.createElement("style");
  style.textContent = '.pwa-launching::before{content:"";position:fixed;inset:0;z-index:2147483646;background:#07101f}.pwa-launching::after{content:"QUANTURA\\A A clearer view of what comes next.";white-space:pre;position:fixed;inset:0;display:grid;place-content:center;text-align:center;z-index:2147483647;color:#edf3fb;font:600 20px/2.4 system-ui,sans-serif;letter-spacing:.08em;pointer-events:none}';
  document.head.append(style);
  const finish = () => { root.classList.remove("pwa-launching"); style.remove(); };
  if (document.readyState === "complete") finish();
  else window.addEventListener("load", finish, {once:true});
  // A failed third-party asset must never trap users behind the splash.
  setTimeout(finish, 2500);
  window.addEventListener("pageshow", event => { if (event.persisted) finish(); });
})();
