# Today’s game forecasts

The public Screener contains only games with a known official start time on the current New York calendar date. Kalshi sports milestones and Polymarket US game metadata determine kickoff; market expiration is not treated as kickoff.

The hourly GitHub workflow runs at minute 3. It discovers premarket full-game winner outcomes from both providers, requests up to ten days of hourly history, leaves missing observations missing, and uses only completed pregame hours. The probability ensemble uses Prophet, Granite and Chronos, with commercially available TimesFM and Toto when its minimum observed context is met. With approved TimesFM, 2–31 genuine observations use four models; from 32, five models participate. Without TimesFM, at least 32 observations are needed for four models. Every requested model must complete. Published quantiles are P01, P25, P50, P75, P90 and P99, with weights renormalized over models that actually support each quantile; Toto/TimesFM tails are not fabricated. Every card records actual observed history length and update time. Unknown start times, missing history, model failures and partial scan coverage are not presented as successful forecasts.

The horizon ends four hours after kickoff. For a partial-hour kickoff, the final point is interpolated between **model forecasts**, never historical observations or simulated executions. Probabilities are not validated sports win probabilities.

No inference starts on or after the start hour (19:00 for a 19:30 game). A runner timer interrupts inference that reaches that boundary, and publication rechecks eligibility. The latest pregame snapshot stays viewable for the remainder of today.

`game_forecast_catalog` stores one latest document per provider/outcome; runs remove expired public snapshots in bounded batches. Firestore client rules deny direct access. Public API routes `/api/screener/games` and `/api/screener/games/:id` expose a validated projection with no worker credentials or private account data. Only the detail endpoint returns chart rows. No trading capability is used.

Cards group overlapping games with provider and Yes/No controls, exact external market links and quantile-relative filters. View forecast opens the saved result with its genuine historical overlay. Signed-in viewers can save a private request in their profile. A saved game does not lock the form: selecting another instrument restores model and horizon controls. Provider scans are paced and honor Retry-After; an early failure still writes a sanitized report for encrypted artifact packaging.
