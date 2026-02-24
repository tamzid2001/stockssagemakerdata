# Changelog: Phases 3, 5, 6 (Push, Monetization, QA)

## Share Button (Native Share)

### Changes
- **app.js**: Added `performShare(url, title, text)` that:
  1. Tries Web Share API (`navigator.share`) when available
  2. Falls back to QuanturaBridge `share` action when in Android/iOS wrapper
  3. Fallback: copy to clipboard
- All share buttons (forecast, screener, upload, AI agent) now use `performShare` instead of `copyToClipboard`
- **QuanturaJavascriptBridge.kt**: Added `share` action that launches `Intent.ACTION_SEND` with the URL

### Files
- `quantura_site/public/app.js`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/web/QuanturaJavascriptBridge.kt`

---

## Phase 3: Push Notifications

### Web
- **firebase-messaging-sw.js**: Improved `notificationclick` handler to focus existing Quantura tab when opened, then navigate to deep link; otherwise open new window
- **app.js**: `registerNotificationToken` now accepts native FCM token via `window.__NATIVE_FCM_TOKEN__` when in Android wrapper

### Android Native
- **QuanturaMessagingService.kt**: New service handling FCM `onNewToken`, `onMessageReceived`; shows local notification with deep link
- **QuanturaFcmTokenHolder.kt**: Stores FCM token in SharedPreferences
- **MainActivity.kt**: Requests POST_NOTIFICATIONS (Android 13+), fetches FCM token, injects `window.__NATIVE_FCM_TOKEN__` into WebView on `onPageFinished`, handles `EXTRA_DEEP_LINK_URL` from notification click
- **AndroidManifest.xml**: Added QuanturaMessagingService, POST_NOTIFICATIONS permission

### Files
- `quantura_site/public/firebase-messaging-sw.js`
- `quantura_site/public/app.js`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/messaging/QuanturaMessagingService.kt`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/messaging/QuanturaFcmTokenHolder.kt`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/MainActivity.kt`
- `quantura_android/app/src/main/AndroidManifest.xml`

---

## Phase 5: Monetization

### AdMob
- **RemoteConfigManager.kt**: Added `adaptiveBanner`, `appOpen` to AdUnitIds; updated default demo IDs to user-provided values
- **BannerAdView.kt**: New adaptive banner view; loads when `ads_enabled` is true
- **MainActivity.kt**: Banner placed at bottom of screen via AndroidView
- Demo IDs used: Adaptive Banner, Interstitial, Rewarded, App Open (all ca-app-pub-3940256099942544/...)

### IAP (Scaffolding)
- **IapService.kt**: Interface for purchase, restore, offerings, entitlement
- **PlayBillingIapService.kt**: Stub implementation; documents Play Console setup
- **build.gradle.kts**: Added `billing-ktx:6.1.0`

### Files
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/config/RemoteConfigManager.kt`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/ads/BannerAdView.kt`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/MainActivity.kt`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/iap/IapService.kt`
- `quantura_android/app/src/main/java/com/quantura/quanturaapp/iap/PlayBillingIapService.kt`
- `quantura_android/app/build.gradle.kts`

---

## Phase 6: Release Checklist

### New File
- **docs/release-checklist.md**: Covers push (web + native), ads, IAP, privacy, build steps; documents blocked items (Apple account, APNs, production ad IDs)

---

## How to Test

### Share
1. Web: Click any Share button; on supported devices (iOS Safari, Android Chrome) native share sheet should appear
2. Android app: Share button should open system share chooser

### Web Push
1. Sign in, go to /notifications, enable notifications
2. Send test notification from Firebase Console or `send_test_notification` callable
3. Foreground: toast appears
4. Background: system notification appears; click → app opens to /dashboard (or data.url)

### Android Push
1. Build app: `cd quantura_android && ./gradlew assembleDebug`
2. Install on device/emulator with Play Services
3. Sign in, enable notifications (native token auto-registers)
4. Send test notification; tap → app opens to deep link

### Ads
1. Ensure `feature_flags.ads_enabled` is true in Remote Config
2. Run app; banner should appear at bottom (demo ad)
3. Web can trigger interstitial via `QuanturaBridge.postMessage(JSON.stringify({action:"showInterstitialAd"}))`

### IAP
- Stub only; create products in Play Console and wire `PlayBillingIapService` for full flow

---

## Blocked / Pending

| Item | Action |
|------|--------|
| Apple Developer account | Required for iOS push, IAP |
| APNs key | Upload to Firebase after account |
| AdMob production IDs | Replace demo IDs in Remote Config |
| Play Console IAP products | Create product IDs, wire PlayBillingIapService |
| UMP (GDPR consent) | Add SDK, implement consent flow before EU |
