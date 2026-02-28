# Explore QA Checklist

## Auto-publish
- [ ] Run Forecast flow and confirm a new `/posts/{postId}` document appears with `type: "forecast"`.
- [ ] Run Backtest flow and confirm a new post appears with `type: "backtest"`.
- [ ] Run Screener flow and confirm a new post appears with `type: "screener"`.
- [ ] Run AI Agent flow and confirm a new `agent_runs` doc and corresponding `type: "agent"` post appear.

## Feed and pagination
- [ ] Open `/explore` and verify Masonry cards render.
- [ ] Switch tabs (`Trending`, `Latest`, `Following`, `Tickers`) and verify feed changes.
- [ ] Scroll to bottom and verify next page loads using cursor pagination.
- [ ] Search by ticker (`AAPL`), tag (`#forecast`), and handle (`@...`) and verify results.

## Post actions
- [ ] Like toggles on/off and count updates.
- [ ] Repost toggles on/off and count updates.
- [ ] Share triggers Web Share or clipboard fallback and increments share count.
- [ ] Comment create works; owner can delete own comment.
- [ ] Report submission records report and increments report count.

## Profile controls
- [ ] Open `/profile` and verify own posts list loads.
- [ ] Change post visibility (`public` <-> `unlisted`) and verify persistence.
- [ ] Delete post and verify cascade cleanup (comments/likes/reposts/reports removed).

## Notifications
- [ ] Enable push token registration on `/profile` and verify token is stored under `/users/{uid}/fcmTokens/{token}`.
- [ ] Toggle notification preferences (`global`, `following`, `tickers`) and verify saved values.
- [ ] Add/remove watch tickers and followed authors; verify topic sync calls succeed.
- [ ] After new post auto-publish, verify push notification arrives for opted-in users/topics.

