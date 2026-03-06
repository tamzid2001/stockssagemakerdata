(() => {
  const statusNode = document.getElementById("order-status");
  const summaryNode = document.getElementById("order-summary");
  const summarySession = document.getElementById("summary-session");
  const summaryStatus = document.getElementById("summary-status");
  const summaryEmail = document.getElementById("summary-email");
  const summaryTotal = document.getElementById("summary-total");
  const summaryShipping = document.getElementById("summary-shipping");
  const summaryItems = document.getElementById("summary-items");

  if (!statusNode) return;

  const toMoney = (cents, currency = "USD") => {
    const value = Number(cents);
    if (!Number.isFinite(value)) return "$0.00";
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: String(currency || "USD").toUpperCase(),
    }).format(value / 100);
  };

  const setStatus = (text, level = "info") => {
    statusNode.textContent = text;
    statusNode.classList.remove("warn", "success");
    if (level === "warn") statusNode.classList.add("warn");
    if (level === "success") statusNode.classList.add("success");
  };

  const params = new URLSearchParams(window.location.search);
  const sessionId = String(params.get("session_id") || "").trim();

  if (!sessionId) {
    setStatus("Missing checkout session ID. Return to Shop and try again.", "warn");
    return;
  }

  async function loadOrder() {
    try {
      const response = await fetch(`/api/shop/order/${encodeURIComponent(sessionId)}`, {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      });

      if (response.status === 404) {
        setStatus("Order is still being confirmed. Refresh this page in a moment.", "warn");
        return;
      }

      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.order) {
        throw new Error(String(payload.error || "Unable to load order summary."));
      }

      const order = payload.order;
      setStatus("Order confirmed and paid.", "success");

      summarySession.textContent = String(order.sessionId || "-");
      summaryStatus.textContent = String(order.status || order.paymentStatus || "-");
      summaryEmail.textContent = String(order.customerEmail || "-");
      summaryTotal.textContent = toMoney(order.amountTotal, order.currency);
      summaryShipping.textContent = toMoney(order.shipping?.shippingCost || 0, order.currency);

      const items = Array.isArray(order.items) ? order.items : [];
      summaryItems.innerHTML = items.length
        ? items
            .map((item) => {
              const qty = Number(item.qty || 0);
              const unit = toMoney(item.unitAmount, order.currency);
              const name = escapeHtml(item.name || item.sku || "Item");
              return `<li>${name} x ${qty} (${unit})</li>`;
            })
            .join("")
        : "<li>No line items available yet.</li>";

      summaryNode.classList.remove("hidden");
    } catch (error) {
      setStatus(error.message || "Unable to load order summary.", "warn");
    }
  }

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  loadOrder();
})();
