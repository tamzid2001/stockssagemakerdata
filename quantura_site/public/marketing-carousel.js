"use strict";

(() => {
  const normalizeInterval = (raw) => {
    const value = Number(raw);
    if (!Number.isFinite(value)) return 7000;
    return Math.max(2500, Math.min(20000, Math.round(value)));
  };

  const initQuoteCarousel = (root) => {
    const slides = Array.from(root.querySelectorAll("[data-quote-slide]"));
    if (!slides.length) return;

    const prevBtn = root.querySelector("[data-quote-prev]");
    const nextBtn = root.querySelector("[data-quote-next]");
    const intervalMs = normalizeInterval(root.dataset.quoteInterval);

    let activeIndex = 0;
    let timer = null;

    const apply = () => {
      slides.forEach((slide, idx) => {
        const active = idx === activeIndex;
        slide.hidden = !active;
        slide.classList.toggle("is-active", active);
      });
    };

    const stopAuto = () => {
      if (!timer) return;
      window.clearInterval(timer);
      timer = null;
    };

    const startAuto = () => {
      stopAuto();
      if (slides.length < 2) return;
      timer = window.setInterval(() => {
        activeIndex = (activeIndex + 1) % slides.length;
        apply();
      }, intervalMs);
    };

    const jump = (delta) => {
      if (slides.length < 2) return;
      activeIndex = (activeIndex + delta + slides.length) % slides.length;
      apply();
    };

    prevBtn?.addEventListener("click", () => {
      jump(-1);
      startAuto();
    });

    nextBtn?.addEventListener("click", () => {
      jump(1);
      startAuto();
    });

    apply();
    startAuto();
  };

  const init = () => {
    const carousels = Array.from(document.querySelectorAll("[data-quote-carousel]"));
    carousels.forEach((node) => initQuoteCarousel(node));
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
