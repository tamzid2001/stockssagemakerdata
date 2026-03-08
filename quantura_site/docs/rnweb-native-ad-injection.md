# React Native for Web Injection + Native-Only Ads

Quantura uses a hybrid architecture:

- Web: HTML/CSS/JS pages served by Firebase Hosting + SSR templates.
- iOS/Android: native Swift/Kotlin shells that host the Quantura website in WebView.
- Native ads: served by native ad managers through bridge events (`quantura:native-feed-ad`).

## What was added

- RNW injection entry: `rnweb/index.web.tsx`
- Platform split ad slot components:
  - `rnweb/components/NativeAdSlot.web.tsx`
  - `rnweb/components/NativeAdSlot.native.tsx`
- Shared bridge utilities: `rnweb/bridge.ts`
- Build script: `rnweb/build-rnw.mjs`
- Output asset: `public/assets/rnweb/quantura-rnw.js`

## Build

```bash
cd quantura_site
npm run build:rnweb
```

## Mounting RNW into HTML

Add a mount zone in any HTML surface:

```html
<div
  data-quantura-rn-root
  data-rn-slot-id="example-slot"
  data-rn-placement="home_surface"
  data-rn-context="home"
  data-rn-title="Injected RN panel"
  data-rn-body="RNW mount with native-only ad runtime"
  data-rn-placeholder="true"
></div>
```

Then include:

```html
<script defer src="/assets/rnweb/quantura-rnw.js"></script>
```

## Runtime behavior

- Browser web: no native ad SDK call; RNW slot renders non-ad placeholder (or null if configured).
- Native iOS/Android WebView: RNW slot requests ad payload via bridge (`requestNativeFeedAd`) and renders native-ad data.

This keeps native ad serving mobile-only while preserving a shared RN-style component layer.
