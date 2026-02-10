#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "quantura_site" / "public"
BLOG_INDEX = PUBLIC / "blog" / "index.html"
POSTS_DIR = PUBLIC / "blog" / "posts"
RSS_PATH = PUBLIC / "blog" / "rss.xml"
SITEMAP = PUBLIC / "sitemap.xml"

TRENDING_URL = "https://query1.finance.yahoo.com/v1/finance/trending/US"
SITE_ORIGIN = (os.environ.get("SITE_ORIGIN") or "https://quantura-e2e3d.web.app").rstrip("/")


def _fmt_month_day_year(date_obj: dt.date) -> str:
    return date_obj.strftime("%B %d, %Y")


def _fmt_rfc2822(date_obj: dt.date) -> str:
    # RSS uses RFC 2822 timestamps (we keep it simple and publish at midnight UTC).
    dt_obj = dt.datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=dt.timezone.utc)
    return dt_obj.strftime("%a, %d %b %Y %H:%M:%S GMT")


def fetch_trending_tickers(max_names: int = 10) -> list[str]:
    try:
        resp = requests.get(TRENDING_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        out = []
        for item in quotes:
            symbol = item.get("symbol")
            if isinstance(symbol, str) and symbol:
                out.append(symbol.upper())
        return out[:max_names]
    except Exception:
        return []


def download_snapshot(tickers: list[str]) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()
    df = yf.download(" ".join(tickers), period="45d", interval="1d", group_by="ticker", progress=False)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        # Flatten: (field, ticker) -> ticker.field
        df.columns = [".".join([str(a), str(b)]) for a, b in df.columns.to_list()]

    rows: list[dict] = []
    for t in tickers:
        close_col = f"Close.{t}"
        if close_col not in df.columns:
            continue
        close = df[close_col].dropna()
        if close.empty or len(close) < 6:
            continue
        last = float(close.iloc[-1])
        prev = float(close.iloc[-2])
        w1 = float(close.iloc[-6])  # ~5 trading days ago
        chg_1d = (last - prev) / prev * 100 if prev else None
        chg_1w = (last - w1) / w1 * 100 if w1 else None
        rows.append(
            {
                "ticker": t,
                "last_close": last,
                "chg_1d_pct": chg_1d,
                "chg_1w_pct": chg_1w,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("chg_1w_pct", ascending=False).reset_index(drop=True)


def build_post_html(slug: str, published_date: dt.date, snapshot: pd.DataFrame) -> str:
    date_iso = published_date.isoformat()
    date_human = _fmt_month_day_year(published_date)

    movers = snapshot.head(6).to_dict(orient="records") if not snapshot.empty else []
    mover_lines = []
    for row in movers:
        chg_1w = row.get("chg_1w_pct")
        chg_1d = row.get("chg_1d_pct")
        mover_lines.append(
            f"<li><strong>{row.get('ticker')}</strong> "
            f"<span class=\"small\">(1W {chg_1w:+.2f}% • 1D {chg_1d:+.2f}% • last {row.get('last_close'):.2f})</span></li>"
        )
    movers_html = "\n".join(mover_lines) if mover_lines else "<li class=\"small\">Unable to load trending tickers right now.</li>"

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Weekly Market Brief ({date_iso}) | Quantura Blog</title>
    <meta name="description" content="Weekly market brief: trending tickers, momentum snapshot, and a clean workflow for follow-up research in Quantura." />
    <meta name="robots" content="index, follow" />
    <link rel="canonical" href="{SITE_ORIGIN}/blog/posts/{slug}" />
    <meta property="og:title" content="Weekly Market Brief ({date_iso})" />
    <meta property="og:description" content="Trending tickers, momentum snapshot, and a clean workflow for follow-up research." />
    <meta property="og:type" content="article" />
    <meta property="og:url" content="{SITE_ORIGIN}/blog/posts/{slug}" />
    <meta property="og:image" content="/assets/quantura-logo.svg" />
    <link rel="icon" href="/assets/quantura-icon.svg" type="image/svg+xml" />
    <link rel="manifest" href="/manifest.json" />
    <link rel="stylesheet" href="/styles.css" />

    <script defer src="/__/firebase/12.9.0/firebase-app-compat.js"></script>
    <script defer src="/__/firebase/12.9.0/firebase-auth-compat.js"></script>
    <script defer src="/__/firebase/12.9.0/firebase-firestore-compat.js"></script>
    <script defer src="/__/firebase/12.9.0/firebase-functions-compat.js"></script>
    <script defer src="/__/firebase/12.9.0/firebase-storage-compat.js"></script>
    <script defer src="/__/firebase/12.9.0/firebase-analytics-compat.js"></script>
    <script defer src="/__/firebase/init.js?useEmulator=false"></script>
    <script defer src="/app.js"></script>

    <script type="application/ld+json">
      {{
        "@context": "https://schema.org",
        "@type": "BlogPosting",
        "headline": "Weekly Market Brief ({date_iso})",
        "datePublished": "{date_iso}",
        "dateModified": "{date_iso}",
        "author": {{ "@type": "Organization", "name": "Quantura" }},
        "publisher": {{
          "@type": "Organization",
          "name": "Quantura",
          "logo": {{ "@type": "ImageObject", "url": "{SITE_ORIGIN}/assets/quantura-logo.svg" }}
        }},
        "mainEntityOfPage": "{SITE_ORIGIN}/blog/posts/{slug}"
      }}
    </script>
  </head>
  <body>
    <header class="header">
      <div class="container nav">
        <a class="logo" href="/" aria-label="Quantura home">
          <img class="logo-img" src="/assets/quantura-logo.svg" alt="Quantura logo" />
        </a>
        <nav class="nav-links">
          <a href="/forecasting" data-analytics="nav_forecasting">Forecasting</a>
          <a href="/screener" data-analytics="nav_screener">Screener</a>
          <a href="/dashboard" data-analytics="nav_dashboard">Dashboard</a>
          <a href="/pricing" data-analytics="nav_pricing">Pricing</a>
          <a href="/blog" data-analytics="nav_blog">Blog</a>
          <a href="/contact" data-analytics="nav_contact">Contact</a>
        </nav>
        <div class="nav-actions">
          <span id="header-user-email" class="small" style="max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">Guest</span>
          <span id="header-user-status" class="pill">Guest</span>
          <button class="cta secondary" id="header-auth" type="button">Sign in</button>
          <button class="cta secondary hidden" id="header-signout" type="button">Sign out</button>
          <a class="cta secondary hidden" id="header-dashboard" href="/dashboard">Dashboard</a>
          <a class="cta" href="/forecasting">Open terminal</a>
        </div>
      </div>
    </header>

    <main>
      <section class="page-hero">
        <div class="container content-grid">
          <div>
            <div class="eyebrow">Weekly brief</div>
            <h1>Weekly Market Brief</h1>
            <p class="small">Published {date_human}</p>
            <p style="margin-top: 14px;">
              This is a lightweight workflow post. The goal is not to predict the market, it is to surface where attention
              and momentum are concentrated, then use the Quantura terminal to validate the next moves.
            </p>
            <div class="hero-actions">
              <a class="cta" href="/forecasting" data-analytics="blog_open_terminal">Open the terminal</a>
              <a class="cta secondary" href="/screener" data-analytics="blog_open_screener">Run the screener</a>
            </div>
          </div>
          <div class="card">
            <h3>Trending tickers snapshot</h3>
            <p class="small">Based on public trending signals and last-close momentum.</p>
            <ul style="margin: 14px 0 0 18px; color: var(--graphite); line-height: 1.7;">
              {movers_html}
            </ul>
          </div>
        </div>
      </section>

      <section class="section">
        <div class="container content-grid">
          <div>
            <h2>How to use this brief</h2>
            <p>
              Load any ticker above in the terminal, add indicator overlays, and run a forecast band to understand where
              uncertainty expands or contracts. Save runs, then compare next week.
            </p>
            <div class="testimonial">
              “Good research is a loop: observe, model, record, and revisit.”
              <div class="small">— Quantura workflow note</div>
            </div>
          </div>
          <div class="card">
            <h3>Fast workflow</h3>
            <div class="feature-list">
              <div class="feature"><span></span>Open `/forecasting` and load a ticker</div>
              <div class="feature"><span></span>Run indicators and overlay them on the chart</div>
              <div class="feature"><span></span>Generate forecast bands and save the run</div>
              <div class="feature"><span></span>Share with collaborators for review</div>
            </div>
          </div>
        </div>
      </section>
    </main>

    <footer class="footer">
      <div class="container footer-grid">
        <div>
          <div class="logo">QUANTURA</div>
          <p class="small">Forecasting and market intelligence for high-conviction decisions.</p>
          <span class="tag">Blog</span>
          <div class="footer-social" aria-label="Quantura social links">
            <a class="social-link" href="#" data-analytics="social_x" aria-label="X (coming soon)">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M18.9 2H22l-6.8 7.8L23 22h-6.9l-5.4-6.9L4.7 22H1.6l7.3-8.4L1 2h7.1l4.9 6.3L18.9 2zm-1.2 18h1.7L7.2 3.9H5.4L17.7 20z"/></svg>
            </a>
            <a class="social-link" href="#" data-analytics="social_linkedin" aria-label="LinkedIn (coming soon)">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4.98 3.5C4.98 4.88 3.87 6 2.5 6S0 4.88 0 3.5 1.12 1 2.5 1 4.98 2.12 4.98 3.5zM0.5 8.5H4.5V23H0.5V8.5zM8.5 8.5H12.3V10.4h.1c.5-1 1.9-2.1 3.9-2.1 4.2 0 5 2.8 5 6.5V23h-4v-6.7c0-1.6 0-3.7-2.2-3.7-2.3 0-2.6 1.8-2.6 3.6V23h-4V8.5z"/></svg>
            </a>
            <a class="social-link" href="#" data-analytics="social_github" aria-label="GitHub (coming soon)">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 .5C5.73.5.75 5.63.75 12c0 5.1 3.29 9.41 7.86 10.94.58.11.79-.26.79-.57v-2.1c-3.2.71-3.88-1.57-3.88-1.57-.53-1.37-1.29-1.74-1.29-1.74-1.05-.74.08-.73.08-.73 1.16.08 1.77 1.23 1.77 1.23 1.03 1.8 2.7 1.28 3.36.98.1-.77.4-1.28.72-1.58-2.55-.3-5.23-1.31-5.23-5.84 0-1.29.45-2.35 1.19-3.18-.12-.3-.52-1.53.11-3.2 0 0 .97-.32 3.18 1.21.92-.26 1.9-.39 2.88-.39.98 0 1.96.13 2.88.39 2.2-1.53 3.17-1.21 3.17-1.21.63 1.67.23 2.9.11 3.2.74.83 1.19 1.89 1.19 3.18 0 4.54-2.69 5.54-5.25 5.83.41.37.77 1.1.77 2.22v3.29c0 .31.21.68.8.57A11.3 11.3 0 0 0 23.25 12C23.25 5.63 18.27.5 12 .5z"/></svg>
            </a>
          </div>
          <div class="footer-badges" aria-label="Quantura technology stack">
            <span class="badge-chip">Weekly</span>
            <span class="badge-chip">Workflow</span>
            <span class="badge-chip">Terminal</span>
          </div>
        </div>
        <div class="small">
          <strong>Explore</strong>
          <div><a href="/blog">Blog</a></div>
          <div><a href="/forecasting">Forecasting</a></div>
          <div><a href="/screener">Screener</a></div>
          <div><a href="/dashboard">Dashboard</a></div>
          <div><a href="/pricing">Pricing</a></div>
        </div>
        <div class="small">
          <strong>Product</strong>
          <div>Terminal</div>
          <div>Screener</div>
          <div>Collaboration</div>
        </div>
      </div>
      <div class="container small" style="margin-top: 24px;">
        © 2026 Quantura. All rights reserved.
      </div>
    </footer>

    <div id="toast" class="toast" role="status" aria-live="polite"></div>
  </body>
</html>
"""


def insert_into_blog_index(slug: str, date_iso: str) -> None:
    if not BLOG_INDEX.exists():
        return
    html = BLOG_INDEX.read_text(encoding="utf-8")
    if f"/blog/posts/{slug}" in html:
        return

    date_human = _fmt_month_day_year(dt.date.fromisoformat(date_iso))
    card = f"""
            <a class="card" href="/blog/posts/{slug}">
              <h3>Weekly Market Brief ({date_iso})</h3>
              <div class="small">{date_human}</div>
              <p class="small" style="margin-top: 12px;">
                Trending tickers, momentum snapshot, and a clean workflow to validate setups in the terminal.
              </p>
              <div class="tag" style="margin-top: 14px;">Weekly brief</div>
            </a>"""

    html = re.sub(r'(<div class="grid-3">)', r"\1" + card, html, count=1)
    BLOG_INDEX.write_text(html, encoding="utf-8")


def insert_into_sitemap(slug: str, lastmod: str) -> None:
    if not SITEMAP.exists():
        return
    xml = SITEMAP.read_text(encoding="utf-8")
    loc = f"{SITE_ORIGIN}/blog/posts/{slug}"
    if loc in xml:
        return
    entry = f"""  <url>
    <loc>{loc}</loc>
    <lastmod>{lastmod}</lastmod>
  </url>
"""
    xml = xml.replace("</urlset>", entry + "</urlset>")
    SITEMAP.write_text(xml, encoding="utf-8")


def rebuild_rss(max_items: int = 12) -> None:
    POSTS_DIR.mkdir(parents=True, exist_ok=True)
    items: list[dict[str, str]] = []

    for path in sorted(POSTS_DIR.glob("*.html"), key=lambda p: p.name, reverse=True):
        slug = path.stem
        if len(slug) < 10:
            continue
        try:
            published = dt.date.fromisoformat(slug[:10])
        except Exception:
            continue
        html = path.read_text(encoding="utf-8", errors="ignore")
        title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        title = title_match.group(1).strip() if title_match else slug
        title = re.sub(r"\\s*\\|\\s*Quantura\\s*Blog\\s*$", "", title).strip()

        desc_match = re.search(
            r'<meta\\s+name=\"description\"\\s+content=\"([^\"]+)\"\\s*/?>',
            html,
            flags=re.IGNORECASE,
        )
        description = desc_match.group(1).strip() if desc_match else "Quantura market workflow notes."

        items.append(
            {
                "title": title,
                "link": f"{SITE_ORIGIN}/blog/posts/{slug}",
                "guid": f"{SITE_ORIGIN}/blog/posts/{slug}",
                "pubDate": _fmt_rfc2822(published),
                "description": description,
            }
        )
        if len(items) >= max_items:
            break

    last_build = _fmt_rfc2822(dt.date.today())
    rss = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0">',
        "  <channel>",
        "    <title>Quantura Blog</title>",
        f"    <link>{SITE_ORIGIN}/blog</link>",
        "    <description>Forecasting notes and market workflows from Quantura.</description>",
        "    <language>en-us</language>",
        f"    <lastBuildDate>{last_build}</lastBuildDate>",
        "",
    ]

    for item in items:
        rss.extend(
            [
                "    <item>",
                f"      <title>{item['title']}</title>",
                f"      <link>{item['link']}</link>",
                f"      <guid>{item['guid']}</guid>",
                f"      <pubDate>{item['pubDate']}</pubDate>",
                f"      <description><![CDATA[{item['description']}]]></description>",
                "    </item>",
                "",
            ]
        )

    rss.extend(["  </channel>", "</rss>", ""])
    RSS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RSS_PATH.write_text("\n".join(rss), encoding="utf-8")


def main() -> int:
    POSTS_DIR.mkdir(parents=True, exist_ok=True)

    today = dt.date.today()
    date_iso = today.isoformat()
    slug = f"{date_iso}-weekly-market-brief"
    post_path = POSTS_DIR / f"{slug}.html"
    if post_path.exists():
        print(f"Post already exists: {post_path}")
        return 0

    tickers = fetch_trending_tickers(max_names=10)
    snapshot = download_snapshot(tickers)
    post_html = build_post_html(slug, today, snapshot)
    post_path.write_text(post_html, encoding="utf-8")

    insert_into_blog_index(slug, date_iso)
    insert_into_sitemap(slug, date_iso)
    rebuild_rss()
    print(f"Wrote blog post: {post_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
