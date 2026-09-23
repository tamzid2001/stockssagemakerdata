(function () {
  "use strict";
  const section = document.getElementById("home-buy-signals");
  if (!section) return;
  const status = section.querySelector("[data-buy-status]");
  const cards = section.querySelector("[data-buy-cards]");
  const money = value => new Intl.NumberFormat(undefined, {style:"currency",currency:"USD",maximumFractionDigits:2}).format(value);
  async function load() {
    try {
      const response = await fetch("/api/screener/data?pageSize=24&sort=ticker", {signal:AbortSignal.timeout(12000)});
      if (!response.ok) throw Error("unavailable");
      const data = await response.json();
      const rows = (data.items || []).filter(row => row?.ticker && Number.isFinite(row.actual_price) && Number.isFinite(row.p50)).slice(0,6);
      status.textContent = rows.length ? `${rows.length} of ${data.total ?? rows.length} published stocks · scan ${data.scanDate || "latest"}` : "No validated stock forecasts are available yet. A new scan may still be running.";
      for (const row of rows) {
        const article=document.createElement("article");
        const link=document.createElement("a");
        // Never trust a provider URL as a website action.
        link.href="/forecasting?panel=forecast&screenerTicker="+encodeURIComponent(row.ticker)+"&screenerScan="+encodeURIComponent(data.scanId);
        link.textContent=row.ticker;link.className="home-buy-symbol";
        const title=document.createElement("p");title.textContent=row.company_name || row.ticker;
        const details=document.createElement("dl");
        for (const [name,value] of [["Daily close",row.actual_price],["First P50",row.p50],["First P99",row.p99]]) {
          const dt=document.createElement("dt"),dd=document.createElement("dd");dt.textContent=name;dd.textContent=Number.isFinite(value)?money(value):"Unavailable";details.append(dt,dd);
        }
        article.append(link,title,details);cards.append(article);
      }
    } catch { status.textContent="The daily publication is temporarily unavailable. Open Q Screener to retry."; }
  }
  if ("IntersectionObserver" in window) {
    const observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting)){observer.disconnect();load();}},{rootMargin:"200px"});observer.observe(section);
  } else load();
})();
