/** Homepage-only gallery integration; loaded only when the gallery exists. */
  const UNSPLASH_CACHE_KEY = "quantura_unsplash_gallery_v1";
  const UNSPLASH_CACHE_TTL_MS = 1000 * 60 * 60 * 6;
  const UNSPLASH_FALLBACK_IMAGES = [
    {
      url: "https://images.unsplash.com/photo-1535320903710-d993d3d77d29?auto=format&fit=crop&w=1280&q=80",
      alt: "Finance workspace with market charts",
      link: "https://unsplash.com/photos/laptop-computer-on-glass-top-table-near-window-nA0UDNDbxys",
      photographer: "Adeolu Eletu",
      photographerLink: "https://unsplash.com/@adeolueletu",
    },
    {
      url: "https://images.unsplash.com/photo-1590283603385-17ffb3a7f29f?auto=format&fit=crop&w=1280&q=80",
      alt: "Stock market dashboard on laptop",
      link: "https://unsplash.com/photos/macbook-air-near-white-paper-BStWzy4M7vA",
      photographer: "Austin Distel",
      photographerLink: "https://unsplash.com/@austindistel",
    },
    {
      url: "https://images.unsplash.com/photo-1642790106117-e829e14a795f?auto=format&fit=crop&w=1280&q=80",
      alt: "Tablet with candlestick chart",
      link: "https://unsplash.com/photos/black-and-white-smartphone-on-brown-wooden-table-8wVYO8rK1j0",
      photographer: "Tech Daily",
      photographerLink: "https://unsplash.com/@techdailyca",
    },
    {
      url: "https://images.unsplash.com/photo-1559526324-4b87b5e36e44?auto=format&fit=crop&w=1280&q=80",
      alt: "Financial team reviewing growth metrics",
      link: "https://unsplash.com/photos/people-sitting-in-front-of-computer-MYbhN8KaaEc",
      photographer: "Campaign Creators",
      photographerLink: "https://unsplash.com/@campaign_creators",
    },
  ];
  const hydrateUnsplashGallery = async (functionsClient) => {
    const gallery = document.getElementById("unsplash-grid");
    if (!gallery) return;

    const cards = Array.from(gallery.querySelectorAll("[data-unsplash-slot]"));
    if (!cards.length) return;

    const applyPhotos = (photos) => {
      if (!Array.isArray(photos) || !photos.length) return;
      cards.forEach((card, idx) => {
        const photo = photos[idx % photos.length];
        if (!photo || !photo.url) return;
        const img = card.querySelector("[data-unsplash-img]");
        if (img) {
          img.src = photo.url;
          img.alt = photo.alt || "Market imagery from Unsplash";
          img.loading = "lazy";
          img.decoding = "async";
        }
        const creditLink = card.querySelector("[data-unsplash-credit]");
        if (creditLink) {
          const creditText = photo.photographer
            ? `Photo by ${photo.photographer} on Unsplash`
            : "Photo on Unsplash";
          creditLink.textContent = creditText;
          creditLink.href = photo.photographerLink || photo.link || "https://unsplash.com/";
          creditLink.setAttribute("target", "_blank");
          creditLink.setAttribute("rel", "noopener noreferrer");
        }
      });
    };

    try {
      const raw = sessionStorage.getItem(UNSPLASH_CACHE_KEY);
      if (raw) {
        const cached = JSON.parse(raw);
        if (
          cached &&
          Array.isArray(cached.photos) &&
          cached.timestamp &&
          Date.now() - Number(cached.timestamp) < UNSPLASH_CACHE_TTL_MS
        ) {
          applyPhotos(cached.photos);
          return;
        }
      }
    } catch (error) {
      // Ignore cache read failures and continue to fetch.
    }

    applyPhotos(UNSPLASH_FALLBACK_IMAGES);

    if (!functionsClient || typeof functionsClient.httpsCallable !== "function") return;

    const rawQuery = String(gallery.dataset.unsplashQuery || "stock market, trading desk");
    const count = Math.max(1, Math.min(8, Number(gallery.dataset.unsplashCount || cards.length || 4)));

    try {
      const getGallery = functionsClient.httpsCallable("get_unsplash_gallery");
      const result = await getGallery({ query: rawQuery, count });
      const payload = result?.data && typeof result.data === "object" ? result.data : {};
      const photos = Array.isArray(payload.photos) ? payload.photos : [];

      if (!photos.length) return;
      applyPhotos(photos);
      try {
        sessionStorage.setItem(
          UNSPLASH_CACHE_KEY,
          JSON.stringify({
            timestamp: Date.now(),
            photos,
          })
        );
      } catch (error) {
        // Ignore cache write failures.
      }
    } catch (error) {
      // Keep fallback visuals when API is unavailable.
    }
  };
export { hydrateUnsplashGallery };
