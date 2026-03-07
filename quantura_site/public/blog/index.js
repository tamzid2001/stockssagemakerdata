(() => {
  const PAGE_SIZE = 12;
  const MANIFEST_URL = "/blog/posts.manifest.json";

  const statusEl = document.getElementById("blog-archive-status");
  const gridEl = document.getElementById("blog-archive-grid");
  const paginationEl = document.getElementById("blog-pagination");

  if (!statusEl || !gridEl || !paginationEl) return;

  const params = new URLSearchParams(window.location.search || "");
  const rawPage = Number(params.get("page") || "1");
  const currentPage = Number.isFinite(rawPage) && rawPage > 0 ? Math.floor(rawPage) : 1;

  const titleCaseTopic = (slug) =>
    String(slug || "")
      .split("-")
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");

  const escapeHtml = (value) =>
    String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

  const formatDate = (iso) => {
    const date = new Date(String(iso || ""));
    if (Number.isNaN(date.getTime())) return "Unknown date";
    return new Intl.DateTimeFormat("en-US", { month: "long", day: "numeric", year: "numeric" }).format(date);
  };

  const parseManifestPosts = (payload) => {
    const posts = Array.isArray(payload?.posts) ? payload.posts : [];
    return posts
      .map((post) => {
        const slug = String(post?.slug || "").trim();
        const title = String(post?.title || "").trim();
        const dateIso = String(post?.dateIso || "").trim();
        const excerpt = String(post?.excerpt || "").trim();
        if (!slug || !title || !dateIso) return null;
        return {
          slug,
          title,
          dateIso,
          excerpt,
          topic: String(post?.topic || "").trim(),
        };
      })
      .filter(Boolean)
      .sort((a, b) => b.dateIso.localeCompare(a.dateIso));
  };

  const pageUrl = (page) => {
    const next = new URLSearchParams(window.location.search || "");
    if (page <= 1) next.delete("page");
    else next.set("page", String(page));
    const query = next.toString();
    return query ? `/blog?${query}` : "/blog";
  };

  const renderPagination = (page, totalPages) => {
    if (totalPages <= 1) {
      paginationEl.innerHTML = "";
      return;
    }
    const start = Math.max(1, page - 2);
    const end = Math.min(totalPages, start + 4);
    const pages = [];
    for (let i = start; i <= end; i += 1) pages.push(i);

    paginationEl.innerHTML = `
      <a class="cta secondary small ${page <= 1 ? "disabled" : ""}" ${page <= 1 ? 'aria-disabled="true"' : `href="${pageUrl(page - 1)}"`}>Previous</a>
      ${pages
        .map(
          (item) =>
            `<a class="cta secondary small ${item === page ? "active" : ""}" href="${pageUrl(item)}" ${
              item === page ? 'aria-current="page"' : ""
            }>${item}</a>`
        )
        .join("")}
      <a class="cta secondary small ${page >= totalPages ? "disabled" : ""}" ${
        page >= totalPages ? 'aria-disabled="true"' : `href="${pageUrl(page + 1)}"`
      }>Next</a>
    `;
  };

  const renderPosts = (posts, page) => {
    const total = posts.length;
    const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
    const safePage = Math.min(Math.max(1, page), totalPages);
    const start = (safePage - 1) * PAGE_SIZE;
    const slice = posts.slice(start, start + PAGE_SIZE);

    if (!slice.length) {
      statusEl.textContent = "No posts found for this page.";
      gridEl.innerHTML = `<div class="card"><p class="small muted">No posts available.</p></div>`;
      paginationEl.innerHTML = "";
      return;
    }

    statusEl.textContent = `Showing ${start + 1}-${start + slice.length} of ${total} posts.`;
    gridEl.innerHTML = slice
      .map((post) => {
        const topicLabel = titleCaseTopic(post.topic) || "Quantura";
        return `
          <a class="card" href="/blog/posts/${encodeURIComponent(post.slug)}">
            <h3>${escapeHtml(post.title)}</h3>
            <div class="small">${escapeHtml(formatDate(post.dateIso))}</div>
            <p class="small" style="margin-top: 12px;">${escapeHtml(post.excerpt || "Quantura research note.")}</p>
            <div class="tag" style="margin-top: 14px;">${escapeHtml(topicLabel)}</div>
          </a>
        `;
      })
      .join("");
    renderPagination(safePage, totalPages);
  };

  const renderFailure = (message) => {
    statusEl.textContent = "Unable to load blog archive.";
    gridEl.innerHTML = `<div class="card"><p class="small muted">${escapeHtml(message || "Try again later.")}</p></div>`;
    paginationEl.innerHTML = "";
  };

  const loadArchive = async () => {
    statusEl.textContent = "Loading blog archive...";
    try {
      const response = await fetch(MANIFEST_URL, { credentials: "same-origin" });
      if (!response.ok) throw new Error(`Manifest request failed (${response.status})`);
      const payload = await response.json();
      const posts = parseManifestPosts(payload);
      renderPosts(posts, currentPage);
    } catch (error) {
      renderFailure(String(error?.message || "Unable to load posts."));
    }
  };

  loadArchive();
})();
