(function () {
  "use strict";
  const section = document.getElementById("home-buy-signals");
  if (!section) return;
  const status = section.querySelector("[data-buy-status]");
  const cards = section.querySelector("[data-buy-cards]");
  const money = value => new Intl.NumberFormat(undefined, {style:"currency",currency:"USD",maximumFractionDigits:2}).format(value);
  async function load() {
    try {
      const response = await fetch("/api/screener/data?signal=buy&pageSize=6&sort=ticker", {signal:AbortSignal.timeout(12000)});
      if (!response.ok) throw Error("unavailable");
      const data = await response.json();
      const rows = (data.items || []).filter(row => row.signal === "buy" && row.current_signal?.rule === "daily_close_above_first_p99_v2" && Number.isFinite(row.buy_price_target));
      status.textContent = rows.length ? `Input close: ${rows[0].current_signal.input_date} · ${data.total ?? rows.length} matching stocks` : "No Buy signals in the latest eligible daily publication. A new scan may still be running.";
      for (const row of rows) {
        const article=document.createElement("article");
        const link=document.createElement("a");
        // Never trust a provider URL as a website action.
        link.href="/forecasting?panel=forecast&screenerTicker="+encodeURIComponent(row.ticker)+"&screenerScan="+encodeURIComponent(data.scanId);
        link.textContent=row.ticker;link.className="home-buy-symbol";
        const title=document.createElement("p");title.textContent=row.company_name || row.ticker;
        const details=document.createElement("dl");
        for (const [name,value] of [["Daily close",row.actual_price],["First P99",row.current_signal.p99],["7-session target",row.buy_price_target]]) {
          const dt=document.createElement("dt"),dd=document.createElement("dd");dt.textContent=name;dd.textContent=money(value);details.append(dt,dd);
        }
        article.append(link,title,details);cards.append(article);
      }
    } catch { status.textContent="The daily publication is temporarily unavailable. Open Q Screener to retry."; }
  }
  if ("IntersectionObserver" in window) {
    const observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting)){observer.disconnect();load();}},{rootMargin:"200px"});observer.observe(section);
  } else load();
})();
