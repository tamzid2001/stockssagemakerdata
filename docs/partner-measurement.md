# Partner measurement and credentials

## October 2026 setup

Quantura's static JavaScript and Express SSR load one browser measurement helper through the existing GA4 context script. The compatibility Flask API reports confirmed contact submissions after Firestore saves them. This is the first OpenAI Ads pixel + Conversions API integration.

| Measurement | Boundary |
| --- | --- |
| OpenAI Ads `page_viewed` / `contents` | Consented public page view, once per document |
| OpenAI Ads `lead_created` / `customer_action` | Contact successfully saved, paired browser/server event ID `lead_<contactId>` |
| AWS Marketplace Zift analytics | Consented public page with no query or fragment |
| Pinterest verification | Public `p:domain_verify` meta in the homepage and SSR page heads |

The public pixel identifier is in `public/partner-measurement.js`. The server's `OPENAI_ADS_PIXEL_ID` must match it. `OPENAI_ADS_CONVERSIONS_API_KEY` is a sensitive production/preview Vercel variable on `quantura-legacy-api`; the same pair is configured on `quantura-api` for future confirmed API conversion boundaries. The Ads management key is separate and is not used for conversion reporting or campaign changes.

## Consent, attribution and failure handling

- The existing `quantura_cookie_consent=accepted` choice is required. Global Privacy Control overrides it. Cookie preferences can be reopened in the footer. Revoking consent ends the OpenAI pixel's measurement; if Zift has loaded, the page reloads to end that script and the saved denial prevents a new load.
- Public paths are home, About, Pricing, Contact, Blog and Shop. Account, uploaded data and private research/Forecast/Screener pages do not load the tags. Zift additionally excludes query/fragment URLs.
- `__oppref` remains opaque: its cookie value is forwarded unchanged, with server cookies preferred and minimal browser context used when the callable proxy does not forward them. `__obref` is forwarded unchanged in `user.obref` when available. Neither reference is logged.
- Browser `sourceUrl` is origin plus path. The server accepts only the canonical Quantura HTTPS origins and strips queries/fragments again; untrusted inputs fall back to `https://quantura.studio/contact`. The pixel SDK supplies its own documented source metadata.
- Contact text, email, account identity, research data and private URLs are not passed as event data or matching fields. The existing analytics choice is not separate permission for identity matching, so no second user-data `init` or hashed identity sharing is enabled. Events opt out of future user-level personalization.
- Browser helper errors are isolated. The whole server conversion path is caught, with no retries and 0.5-second connect/read timeouts. A failed report cannot undo the saved contact. This synchronous dispatch can add bounded network latency; no new queue or persistence infrastructure was added.
- Production uses `validate_only: false` and `debug: false`. An October 3 API smoke test used `validate_only: true` and returned HTTP 200 without creating a real conversion.

## Other supplied credentials

Sensitive Vercel variables on `quantura-api` hold `PINTEREST_APP_SECRET`, `TIKTOK_CLIENT_SECRET`, `GEMINI_PREDICTION_API_KEY`, `GEMINI_PREDICTION_API_SECRET`, `OPENAI_ADS_MANAGEMENT_API_KEY` and `CALENDLY_ACCESS_TOKEN`. Pinterest app ID and TikTok client key are configuration identifiers. No values belong in git, wiki, frontend bundles or logs.

These credentials are provisioned for server integrations, not new OAuth/publishing/trading flows. Pinterest/TikTok user authorization and redirect URLs, a confirmed Calendly event type, and a verified prediction-provider contract are still needed for those product features. Gemini prediction credentials are distinct from the existing Google Gemini model API key.

## Coverage and validation

Initial coverage is public page views and confirmed contact leads. `contents_viewed`, `items_added`, `checkout_started`, `order_created` and `subscription_created` are deferred until the complete commerce flow carries current analytics consent and a stable payment-confirmed attribution context. Registration/trial events are deferred because anonymous guest creation is not a confirmed registration/trial. `appointment_scheduled` requires a verified booking success boundary. No speculative custom events are emitted; mobile/native surfaces are not instrumented by this web helper.

Tests cover consent/GPC, private paths, one pixel initialization, lead deduplication, raw attribution, URL trust, missing credentials and transport exceptions. Browser and API validation should check those boundaries after any change. Review future extensions against Quantura's privacy, consent and data handling requirements before deployment.

[OpenAI Pixel](https://developers.openai.com/ads/measurement-pixel) · [Conversions API](https://developers.openai.com/ads/conversions-api) · [Vercel storage](https://vercel.com/docs/deployment-storage)
