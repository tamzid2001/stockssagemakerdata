# Mobile Auth + Ads Troubleshooting

## Why app open ads may not show immediately
- App Open ads are preloaded first. On a cold launch, the first foreground may happen before preload completes.
- iOS shows App Open ads only when no other modal/fullscreen UI is presented.
- Android App Open ads show on `ProcessLifecycleOwner` foreground transitions and are skipped if another fullscreen ad is active.
- Use debug logs:
  - iOS: `[Ads][iOS][AppOpen] ...`
  - Android: `AppOpenAdManager` / `AdManager`

## Verify Firebase Auth config
- Before mobile/Firebase-dependent local testing, run:
  - `./scripts/setup_local_firebase_credentials.sh`
- iOS:
  - Ensure `GoogleService-Info.plist` is in the app bundle, or `GoogleService-Info.local.plist` is present and bundled.
  - Confirm startup logs show Firebase configured: `[Firebase][iOS] Firebase configured successfully.`
- Android:
  - Ensure `quantura_android/app/google-services.json` exists locally.
  - Confirm `com.google.gms.google-services` is applied in `app/build.gradle.kts`.
  - Confirm startup logs do not report Firebase disabled.

## Verify Google Sign-In config
- iOS:
  - Confirm URL scheme includes Firebase `REVERSED_CLIENT_ID` in `quantura_ios/quantura-ios-Info.plist`.
  - Confirm Google sign-in starts from native flow and returns an ID token.
- Android:
  - Confirm `default_web_client_id` is generated from `google-services.json`.
  - Ensure Firebase project has Android app SHA certificates configured; missing SHA commonly causes `DEVELOPER_ERROR`.
  - Confirm native logs show Google sign-in started and auth success/error details.
  - If `google-services.json` is missing, rerun `./scripts/setup_local_firebase_credentials.sh` and verify Secret Manager access.

## Verify AdMob config and test ads
- Debug builds use test ad units by default.
- iOS debug test IDs used:
  - App Open: `ca-app-pub-3940256099942544/5575463023`
  - Banner: `ca-app-pub-3940256099942544/2435281174`
- Android debug test IDs used:
  - App Open: `ca-app-pub-3940256099942544/9257395921`
  - Banner: `ca-app-pub-3940256099942544/9214589741`
- Confirm banner is mounted in UI:
  - iOS: bottom safe-area banner container
  - Android: `BannerAdView` under main WebView
- Check load/show callbacks:
  - iOS: `[Ads][iOS]` / `[Ads][iOS][Banner]`
  - Android: `AdManager`, `BannerAdView`, `AppOpenAdManager`

## Android emulator Play services and banner sizing checks
- If logs show `Google Play services out of date`, ad testing can be unreliable on that emulator.
- App now logs this as:
  - `[Ads][Android] Google Play services issue code=...`
- Recommended QA path:
  - Use an emulator image with Google Play and update Play services from Play Store.
  - Validate once on a physical Android device before production rollout.
- Banner sizing diagnostics are logged as:
  - `[Ads][Android] Banner sizing rawPx=... density=... widthDp=...`
- If banner load fails with `Ad size will not fit on screen`, confirm width is being measured from the mounted container and converted to dp.

## Remote Config keys to set in Firebase Console
- Ads rollout (test -> production):
  - `ads_use_real_ios` (`true` to use production iOS IDs, `false` for test IDs)
  - `ads_use_real_android` (`true` to use production Android IDs, `false` for test IDs)
  - `ad_unit_ids` (JSON override for all unit IDs; optional)
- Native checkout policy:
  - `native_ios_storekit_checkout_only` (`true` recommended; iOS native app enforces StoreKit checkout path)
  - `native_android_play_billing_enabled` (`true` to route Android native checkout through Play Billing bridge)
  - `native_iap_product_ids` (JSON map for plan -> productId, e.g. `{"pro":"quantura_pro_monthly","desk":"quantura_pro_monthly","forecast":"quantura_pro_monthly","default":"quantura_pro_monthly"}`)
- Web checkout controls:
  - `stripe_checkout_enabled`
  - `stripe_public_key`

Note: the same keys should exist in the server-side template defaults (SSR function) and in client defaults so first paint and hydrated runtime stay consistent.
