# Prompt 3 QA Checklist (Footer + Header + About/Events/Shop + Contact Solve now)

Date: 2026-02-25

## Scope checks

- [x] Footer social links use the requested destinations:
  - TikTok: `http://www.tiktok.com/@quanturaai`
  - Instagram: `https://www.instagram.com/quanturaai_market_forecasts?igsh=ZTZuNW16ZmxuaHl4&utm_source=qr`
  - Facebook: `https://www.facebook.com/quanturaai/`
  - Threads: `https://www.threads.com/@quanturaai_market_forecasts`
  - Reddit: `https://www.reddit.com/r/Quantura_AI/`
  - LinkedIn: `https://www.linkedin.com/company/quanturaai/?viewAsMember=true`
- [x] Footer social icons are consistent (`/assets/social/*.svg`) and rendered in updated pages.
- [x] Header links include `Events`, `Shop`, `About`, and `Contact Us`.
- [x] `Contact` label is renamed to `Contact Us` in updated page headers.
- [x] `/events` and `/about` are mapped via Firebase Hosting rewrites to SSR.
- [x] `Contact Us` page includes `Solve now` trigger (`data-action="open-solve-now"`).
- [x] Solve now modal flow in `public/app.js` returns structured output:
  - Summary section
  - Suggested next steps list
  - AI output details
  - Caption: `LLMs can sometimes make mistakes.`
- [x] About page scaffolding includes:
  - Developer hook section
  - LinkedIn profile link: `https://www.linkedin.com/in/tamzid-ullah-8a50a2234/`
  - Profile image upload + render preview
  - Tagline chip: `Developer`
  - Bio line: `Masters in Computer Science from New York Institute of Technology`
- [x] Events, Shop, and About pages include title/description metadata placeholders.

## Screenshots

- Contact: `docs/screenshots/prompt3/contact.png`
- About: `docs/screenshots/prompt3/about.png`
- Events: `docs/screenshots/prompt3/events.png`
- Shop: `docs/screenshots/prompt3/shop.png`
