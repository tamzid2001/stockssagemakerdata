# Quantura Release Checklist

## Push Notifications

### Web / PWA
- [ ] `firebase-messaging-sw.js` deployed at site root (quantura_site/public/)
- [ ] FCM_WEB_VAPID_KEY configured in Secret Manager
- [ ] `register_notification_token` and `send_test_notification` functions deployed
- [ ] Test: foreground notification (toast)
- [ ] Test: background notification (system notification)
- [ ] Test: click notification → deep link opens correct screen

### Android Native
- [ ] QuanturaMessagingService registered in AndroidManifest
- [ ] POST_NOTIFICATIONS permission requested (Android 13+)
- [ ] google-services.json present with correct package
- [ ] FCM token fetched and injected into WebView (window.__NATIVE_FCM_TOKEN__)
- [ ] Test: send test notification to native app
- [ ] Test: tap notification → app opens to deep link URL

### iOS Native (blocked until Apple Developer account)
- [ ] Apple Developer Program membership
- [ ] APNs key (.p8) created in Apple Developer Console
- [ ] APNs key uploaded to Firebase Console (Project Settings → Cloud Messaging)
- [ ] GoogleService-Info.plist with correct bundle ID
- [ ] Push Notifications capability enabled in Xcode
- [ ] Test: iOS simulator/device receive notification
- **Stub:** iOS project not yet configured; add when account available.

---

## Ads (AdMob)

### Configuration
- [ ] Replace demo ad unit IDs with production IDs in Remote Config:
  - `adaptiveBanner`: ca-app-pub-3940256099942544/9214589741 (demo) → your banner ID
  - `interstitial`: ca-app-pub-3940256099942544/1033173712 (demo) → your interstitial ID
  - `rewarded`: ca-app-pub-3940256099942544/5224354917 (demo) → your rewarded ID
  - `appOpen`: ca-app-pub-3940256099942544/9257395921 (demo) → your app open ID
- [ ] `feature_flags.ads_enabled` = true in Remote Config (or default)
- [ ] AdMob App ID in AndroidManifest (com.google.android.gms.ads.APPLICATION_ID)

### GDPR / Consent (UMP)
- [ ] Add User Messaging Platform SDK: `implementation("com.google.android.ump:user-messaging-platform:2.x")`
- [ ] Request consent before loading ads (ConsentInformation.requestConsentInfoUpdate)
- [ ] Document: UMP integration stubbed; implement before EU rollout.

### Testing
- [ ] Test banner renders at bottom of screen
- [ ] Test interstitial triggers (via QuanturaBridge showInterstitialAd)
- [ ] No crashes with missing google-services.json (local builds)

---

## In-App Purchases (IAP)

### Android (Play Billing)
- [ ] Billing dependency: `implementation("com.android.billingclient:billing-ktx:6.x")`
- [ ] Product IDs created in Play Console (e.g. `quantura_pro_monthly`)
- [ ] License testers added in Play Console
- [ ] IapService implementation wired to BillingClient
- [ ] Paywall screen: list offerings, purchase, restore
- [ ] Entitlement check gates premium feature

### iOS (StoreKit) – blocked
- [ ] Apple Developer account required
- [ ] In-App Purchase products in App Store Connect
- [ ] StoreKit 2 integration
- **Stub:** iOS IAP not implemented; add when account available.

### RevenueCat (optional)
- [ ] If using RevenueCat: API keys in Secret Manager
- [ ] Configure product IDs in RevenueCat dashboard

---

## Privacy & Permissions

- [ ] Privacy policy URL updated and linked
- [ ] App Store / Play Store privacy declarations (data collected)
- [ ] POST_NOTIFICATIONS (Android 13+): request only when needed
- [ ] No API keys or secrets in version control
- [ ] .env / google-services.json in .gitignore

---

## Build & Versioning

- [ ] versionCode / versionName incremented (Android)
- [ ] CFBundleShortVersionString incremented (iOS)
- [ ] Firebase / GCP project matches deployment target
- [ ] CI build passes (if present)
- [ ] ProGuard rules keep required classes (release)

---

## What You Must Supply

| Item | Status | Notes |
|------|--------|-------|
| Apple Developer account | Blocked | Required for iOS push, IAP |
| APNs key (.p8) | Blocked | Upload to Firebase after Apple account |
| AdMob production IDs | Pending | Replace demo IDs in Remote Config |
| Play Console IAP products | Pending | Create product IDs for paywall |
| RevenueCat keys (optional) | Optional | If using RevenueCat |
| google-services.json | Required | In quantura_android/app/ for Firebase |

---

## Quick Test Commands

```bash
# Web push (from Firebase Console or send_test_notification callable)
# Android: build and run on emulator/device
cd quantura_android && ./gradlew assembleDebug

# Firebase deploy (site + functions)
cd quantura_site && firebase deploy
```
