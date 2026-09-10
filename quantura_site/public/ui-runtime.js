/* Shared presentation runtime. No credentials, provider data, or model logic. */
(() => {
  "use strict";
  const scripts = new Map();
  const deferred = new Map();
  const observer = typeof IntersectionObserver === "function" ? new IntersectionObserver(entries => {
    for (const entry of entries) if (entry.isIntersecting) {
      const run = deferred.get(entry.target);
      deferred.delete(entry.target);
      observer.unobserve(entry.target);
      if (run) run();
    }
  }, { rootMargin: "80px" }) : null;

  function loadScript(src) {
    if (scripts.has(src)) return scripts.get(src);
    const promise = new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = src;
      script.async = true;
      script.onload = () => resolve();
      script.onerror = () => { script.remove(); scripts.delete(src); reject(new Error("Optional display module could not load. Please retry.")); };
      document.head.appendChild(script);
    });
    scripts.set(src, promise);
    return promise;
  }

  function whenVisible(element, run) {
    if (!element || !element.isConnected) return;
    const execute = () => Promise.resolve().then(run).catch(() => {
      element.removeAttribute("aria-busy");
      element.textContent = "Chart unavailable. Your data and downloads are unchanged. ";
      const retry = document.createElement("button");
      retry.type = "button"; retry.className = "cta secondary small"; retry.textContent = "Retry chart";
      retry.addEventListener("click", execute, { once: true }); element.appendChild(retry);
    });
    if (!observer) { execute(); return; }
    deferred.set(element, execute);
    observer.observe(element);
  }

  // Callers return immediately when an offscreen chart is scheduled. The latest
  // update replaces the previous callback; hidden panels never load Plotly.
  function deferChart(element, run) {
    const rect = element.getBoundingClientRect();
    if (element.getClientRects().length && rect.bottom >= 0 && rect.top <= innerHeight + 80) return false;
    whenVisible(element, run);
    return true;
  }

  function palette() {
    const style = getComputedStyle(document.documentElement);
    const token = name => style.getPropertyValue(name).trim();
    return { foreground: token("--foreground"), card: token("--card"), muted: token("--muted-foreground"), border: token("--border") };
  }
  function chartTheme() {
    const colors = palette();
    return {
      "font.color": colors.foreground, paper_bgcolor: "transparent", plot_bgcolor: colors.card,
      "legend.font.color": colors.foreground, "hoverlabel.bgcolor": colors.card,
      "hoverlabel.font.color": colors.foreground, "hoverlabel.bordercolor": colors.border,
      ...Object.fromEntries(["xaxis", "yaxis", "xaxis2", "yaxis2"].flatMap(axis => [
        [`${axis}.gridcolor`, colors.border], [`${axis}.zerolinecolor`, colors.border],
        [`${axis}.tickfont.color`, colors.muted], [`${axis}.title.font.color`, colors.foreground],
      ])),
    };
  }

  let plotlyFacade;
  async function loadPlotly() {
    if (!window.Plotly) await loadScript("https://cdn.plot.ly/plotly-2.35.2.min.js");
    if (!window.Plotly) throw new Error("Chart library unavailable.");
    if (plotlyFacade) return plotlyFacade;
    plotlyFacade = Object.create(window.Plotly);
    for (const method of ["react", "newPlot"]) plotlyFacade[method] = async (host, traces, layout, config) => {
      host.setAttribute("aria-busy", "true");
      try {
        const result = await window.Plotly[method](host, traces, layout, config);
        await window.Plotly.relayout(host, chartTheme());
        return result;
      } finally { host.removeAttribute("aria-busy"); }
    };
    return plotlyFacade;
  }
  document.addEventListener("quantura:theme-change", () => {
    if (!window.Plotly) return;
    document.querySelectorAll(".js-plotly-plot").forEach(chart => {
      window.Plotly.relayout(chart, chartTheme()).catch(() => {});
    });
  });

  document.addEventListener("DOMContentLoaded", () => {
    loadScript("/contentsquare.js?v=20260909a").catch(() => {});
    const support = document.createElement("button");
    support.type = "button"; support.className = "support-launcher";
    support.id = "quantura-support-launcher"; support.setAttribute("aria-haspopup", "dialog");
    support.setAttribute("aria-label", "Open Quantura support assistant");
    support.textContent = "Help";
    support.addEventListener("click", async () => {
      support.disabled = true;
      try {
        await loadScript("/support-chat.js?v=20260909a");
        window.QuanturaSupport.open(support);
      } catch { support.textContent = "Retry help"; }
      finally { support.disabled = false; }
    });
    document.body.appendChild(support);
    document.querySelectorAll("video[data-lazy-video]").forEach(video => {
      if (matchMedia("(prefers-reduced-motion: reduce)").matches || navigator.connection?.saveData) {
        const load = () => { video.src = video.dataset.lazyVideo; video.play().catch(() => {}); };
        video.addEventListener("click", load, { once: true });
        video.addEventListener("keydown", event => { if (event.key === "Enter" || event.key === " ") load(); }, { once: true });
        return;
      }
      whenVisible(video, () => {
        video.src = video.dataset.lazyVideo;
        video.play().catch(() => {});
      });
    });
  });
  window.QuanturaUI = Object.freeze({ loadPlotly, whenVisible, deferChart, palette, chartTheme,
    loadNative: () => loadScript("/assets/rnweb/quantura-rnw.js?v=20260908a") });
})();
