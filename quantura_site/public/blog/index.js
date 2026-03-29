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

  const clearNode = (node) => {
    while (node.firstChild) node.removeChild(node.firstChild);
  };

  const createArchiveCard = (message) => {
    const card = document.createElement("div");
    card.className = "card";
    const paragraph = document.createElement("p");
    paragraph.className = "small muted";
    paragraph.textContent = message;
    card.appendChild(paragraph);
    return card;
  };

  const createPaginationLink = (label, href, className, disabled = false, current = false) => {
    const link = document.createElement("a");
    link.className = className;
    link.textContent = label;
    if (disabled) {
      link.setAttribute("aria-disabled", "true");
    } else {
      link.href = href;
    }
    if (current) link.setAttribute("aria-current", "page");
    return link;
  };

  const renderPagination = (page, totalPages) => {
    clearNode(paginationEl);
    if (totalPages <= 1) {
      return;
    }
    const start = Math.max(1, page - 2);
    const end = Math.min(totalPages, start + 4);
    const pages = [];
    for (let i = start; i <= end; i += 1) pages.push(i);
    const fragment = document.createDocumentFragment();
    fragment.appendChild(
      createPaginationLink(
        "Previous",
        pageUrl(page - 1),
        `cta secondary small ${page <= 1 ? "disabled" : ""}`,
        page <= 1
      )
    );
    pages.forEach((item) => {
      fragment.appendChild(
        createPaginationLink(
          String(item),
          pageUrl(item),
          `cta secondary small ${item === page ? "active" : ""}`,
          false,
          item === page
        )
      );
    });
    fragment.appendChild(
      createPaginationLink(
        "Next",
        pageUrl(page + 1),
        `cta secondary small ${page >= totalPages ? "disabled" : ""}`,
        page >= totalPages
      )
    );
    paginationEl.appendChild(fragment);
  };

  const renderPosts = (posts, page) => {
    const total = posts.length;
    const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
    const safePage = Math.min(Math.max(1, page), totalPages);
    const start = (safePage - 1) * PAGE_SIZE;
    const slice = posts.slice(start, start + PAGE_SIZE);

    if (!slice.length) {
      statusEl.textContent = "No posts found for this page.";
      clearNode(gridEl);
      gridEl.appendChild(createArchiveCard("No posts available."));
      clearNode(paginationEl);
      return;
    }

    statusEl.textContent = `Showing ${start + 1}-${start + slice.length} of ${total} posts.`;
    clearNode(gridEl);
    const fragment = document.createDocumentFragment();
    slice.forEach((post) => {
      const topicLabel = titleCaseTopic(post.topic) || "Quantura";
      const card = document.createElement("a");
      card.className = "card";
      card.href = `/blog/posts/${encodeURIComponent(post.slug)}`;

      const heading = document.createElement("h3");
      heading.textContent = post.title;

      const date = document.createElement("div");
      date.className = "small";
      date.textContent = formatDate(post.dateIso);

      const excerpt = document.createElement("p");
      excerpt.className = "small";
      excerpt.style.marginTop = "12px";
      excerpt.textContent = post.excerpt || "Quantura research note.";

      const topic = document.createElement("div");
      topic.className = "tag";
      topic.style.marginTop = "14px";
      topic.textContent = topicLabel;

      card.append(heading, date, excerpt, topic);
      fragment.appendChild(card);
    });
    gridEl.appendChild(fragment);
    renderPagination(safePage, totalPages);
  };

  const renderFailure = (message) => {
    statusEl.textContent = "Unable to load blog archive.";
    clearNode(gridEl);
    gridEl.appendChild(createArchiveCard(message || "Try again later."));
    clearNode(paginationEl);
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
