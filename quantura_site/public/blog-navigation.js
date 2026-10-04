(() => {
  "use strict";
  const button = document.querySelector("[data-copy-blog-link]");
  const status = document.querySelector("[data-blog-share-status]");
  if (!button || !status) return;
  button.addEventListener("click", async () => {
    const canonical = document.querySelector('link[rel="canonical"]')?.href;
    const url = new URL(canonical || location.pathname, location.origin);
    url.search = "";
    url.hash = "";
    try {
      if (!navigator.clipboard?.writeText) throw new Error("clipboard_unavailable");
      await navigator.clipboard.writeText(url.href);
      status.textContent = "Link copied.";
    } catch {
      // A selectable URL remains usable when browser clipboard access is denied.
      let input = document.querySelector("[data-blog-share-url]");
      if (!input) {
        input = document.createElement("input");
        input.type = "url";
        input.readOnly = true;
        input.dataset.blogShareUrl = "";
        input.setAttribute("aria-label", "Shareable blog link");
        button.closest("nav").append(input);
      }
      input.value = url.href;
      input.focus();
      input.select();
      status.textContent = "Copy the selected link.";
    }
  });
})();
