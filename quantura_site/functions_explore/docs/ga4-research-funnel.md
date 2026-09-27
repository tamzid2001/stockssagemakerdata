# GA4 research funnel

The production API reads `GA4_MEASUREMENT_ID` and the sensitive, production-only
`GA4_API_SECRET` from Vercel. The secret is never shipped to browser code. The web
stream is `G-R9Y1C8WBKS`. Keep new secret values out of source control and logs.

Browser collection requires the existing accepted cookie preference and respects
Global Privacy Control. Editorial pages use the same web stream. Page URLs omit
queries and fragments. The product video reports `product_tour_started` and
`product_tour_completed`; existing navigation events measure the forecast and
screener entry points.

An authenticated forecast submission optionally captures the existing Google tag's
client and session IDs. The server reports `forecast_completed` after its result
has been saved, and checks the account's current consent again before sending.
The event includes provider, model count, quantile count, and horizon length. It
excludes user identity, ticker symbols, uploaded rows, market links, search text,
and prediction values. Advertising consent remains denied. Missing consent or tag
IDs disable this optional event without affecting forecasting.

Each job claims one send attempt. A 2xx response records `accepted`, which is not
proof of reporting or attribution. Network failures record `unconfirmed` without
an automatic retry. Pending jobs older than 24 hours are not attributed to the
original session. Debug validation returned HTTP 200 with no validation messages
for the configured stream and a non-ingested fixture. This validates the payload
format; it does not verify the secret or a real reporting event.

Review the journey from homepage entry to submission and completion, then segment
by provider and model count. Avoid labeling a page visit as a completed forecast.

Reference: https://developers.google.com/analytics/devguides/collection/protocol/ga4
