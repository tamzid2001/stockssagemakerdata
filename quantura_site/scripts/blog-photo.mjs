export const escapeHtml = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
export function imageUrl(photo, width = 1280) {
  const url = new URL(photo.image_url);
  if (url.protocol !== "https:" || url.hostname !== "images.unsplash.com" || !url.searchParams.has("ixid")) throw new Error("Expected an attributed Unsplash API image URL");
  for (const [key, value] of Object.entries({ w: width, h: Math.round(width * 9 / 16), fit: "crop", auto: "format", q: 78 })) url.searchParams.set(key, String(value));
  return url.href;
}
export function referralUrl(value) {
  const url = new URL(value);
  if (url.hostname !== "unsplash.com" || !["https:", "http:"].includes(url.protocol)) throw new Error("Invalid Unsplash attribution URL");
  url.protocol = "https:";
  url.searchParams.set("utm_source", "quantura"); url.searchParams.set("utm_medium", "referral");
  return url.href;
}
export function photoFigure(photo) {
  if (!photo?.download_tracked_at) throw new Error("Select and track the Unsplash photo before publication");
  const srcset = [320, 640, 960, 1280].map(width => `${escapeHtml(imageUrl(photo, width))} ${width}w`).join(", ");
  return `<figure class="blog-photo" data-unsplash-photo="${escapeHtml(photo.id)}">
      <img src="${escapeHtml(imageUrl(photo))}" srcset="${srcset}" sizes="(max-width: 768px) calc(100vw - 32px), (max-width: 1280px) 70vw, 960px" alt="${escapeHtml(photo.alt)}" width="1280" height="720" decoding="async" fetchpriority="high" />
      <figcaption>Photo by <a href="${escapeHtml(referralUrl(photo.photographer_url))}" target="_blank" rel="noopener noreferrer">${escapeHtml(photo.photographer)}</a> on <a href="${escapeHtml(referralUrl(photo.photo_url))}" target="_blank" rel="noopener noreferrer">Unsplash</a>. Illustrative photography.</figcaption>
    </figure>`;
}
