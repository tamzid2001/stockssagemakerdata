# Quantura homepage / shared presentation QA

final result: passed

## Source and rendered evidence

- Source visual truth: `/Users/tamzidullah/Desktop/quanturaimages/ChatGPT Image Sep 9, 2026, 03_17_49 PM.png` (1672 × 941 brand board), plus the final individual artwork supplied in the same directory.
- Implementation: existing Quantura homepage, `http://localhost:4175/`, browser-rendered guest session, both themes.
- Desktop screenshot: `/private/tmp/quantura-home-dark-final.png` (1440 × 1000 CSS pixels, screenshot at 1×). Source board and rendered screenshot were opened together in the same comparison input.
- Additional captures: `/private/tmp/quantura-home-light-final.png`, `/private/tmp/quantura-home-light-mobile.png` (390 × 844), `/private/tmp/quantura-screener-dark-mobile.png` (390 × 844).
- The reference is a multi-panel identity board, not an exact webpage viewport. Comparison therefore uses its laptop composition, typography hierarchy, mark, navy/cyan palette and Earth panel; it does not treat the whole collage as one page or copy its unsupported statistics.

## Comparison history / resolved findings

1. P1: bright wave artwork interfered with headline contrast. Dimmed its compositing against the semantic dark surface and verified the revised header/hero. Pale artwork is used in light mode.
2. P1: old global CSS hid the Q Forecast navigation link. Removed the suppressing selector and verified visible Q Forecast, Screener and API Docs links.
3. P1: translucent mobile menus displayed underlying text and blurred the header. Menus now use solid semantic popover surfaces; expensive backdrop blur is removed; Close remains above the backdrop.
4. P2: original menu was a tall single-column list. Two columns now fit 320px–980px, with minimum 44px touch targets and text labels for sign-in/dashboard and notifications.
5. P1: light-theme headings on the dark Earth banner inherited the wrong foreground. Brand-dark content explicitly retains its contrasting semantic foreground.

## Required fidelity surfaces

- Typography: existing Inter retained; compact controls and readable 16px mobile inputs. Headline wraps naturally; tracked uppercase brand/eyebrow follows the reference. No artificial screenshot text is reconstructed as UI.
- Layout: two-column desktop hero, single-column mobile hero; responsive image ratio prevents shift. Real image assets replace the former generic hero. Compact workflow strip links to existing services. The bottom homepage dock is absent.
- Colors: one shared semantic token set drives light/dark cards, menus, tables and toast surfaces. Automated text-token checks meet 4.5:1 for the tested pairs. Decorative dark panels retain dedicated foreground tokens.
- Assets: supplied blue Q, laptop, Earth, pale Q and wave imagery are reused. Raster-derived favicon/install/social assets use explicit dimensions and a distinct safe-zone maskable icon. Standard UI icons reuse Iconoir.
- Copy: no invented market counts, performance claims or live prices. The laptop has a visible “product concept / example values, not live market data” caption. Existing product positioning and routes remain.

## Browser evidence

- Homepage and Screener: 320, 375, 390, 430, 768, 1024, 1280, 1440 and 1920px, light and dark. All 36 geometry checks had document width equal to viewport width.
- Mobile menu opened, theme switched, labeled Sign in navigated to the real account authentication form. No browser console errors observed in that interaction.
- Real Screener loaded 3,598 securities; AAPL search returned the Apple row. CSV download returned HTTP 200, CSV headers, 3,598 data rows and no HTML body. Controlled table scrolling stays inside the results region.
- Support dialog guest guard, link safety and interaction behavior tested in browser/unit tests. Actual GPT-5.6 Luna structured-answer smoke returned HTTP 200. Production signed-in chat is not implied by these checks.
- All 78 editorial images were visually reviewed as contact sheets, with unrelated results replaced. Every canonical/static post has tracked Unsplash provenance and visible attribution.

## Remaining limitations / follow-up

No Lighthouse score or post-deployment Speed Insights improvement is claimed. Field metrics require production observation. The app shell remains large despite minification/lazy-heavy-module improvements; splitting account/admin logic is a future iteration. Source imagery includes concept-screen typography which is not a shipped UI contract. Authenticated collaborator/paid inference flows were not newly exercised during this visual release.

Contentsquare collection remains unverified after two official wizard failures and is paused for user guidance. This is an integration limitation, not a passed analytics acceptance check.
