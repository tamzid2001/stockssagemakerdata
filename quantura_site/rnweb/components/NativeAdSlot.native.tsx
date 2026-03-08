import React, { useEffect, useMemo, useState } from "react";
import { Platform, Pressable, StyleSheet, Text, View } from "react-native";
import {
  NativeAd,
  NativeAdView,
  NativeAsset,
  NativeAssetType,
  NativeMediaView,
  TestIds,
} from "react-native-google-mobile-ads";

import type { NativeAdSlotProps } from "../types";

const LIVE_NATIVE_UNITS = {
  ios: "ca-app-pub-5322412772082850/5615422478",
  android: "ca-app-pub-5322412772082850/1144501483",
};

const resolveNativeUnitId = (): string => {
  if (__DEV__) return TestIds.NATIVE;
  return Platform.OS === "ios" ? LIVE_NATIVE_UNITS.ios : LIVE_NATIVE_UNITS.android;
};

const log = (eventName: string, payload: Record<string, unknown>) => {
  // eslint-disable-next-line no-console
  console.info(`[RN][NativeAdSlot] ${eventName}`, payload);
};

export default function NativeAdSlot({ slotId, placement, title, body }: NativeAdSlotProps) {
  const [ad, setAd] = useState<NativeAd | null>(null);
  const [errorMessage, setErrorMessage] = useState("");

  const adUnitId = useMemo(() => resolveNativeUnitId(), []);

  useEffect(() => {
    let mounted = true;
    log("ad_request", { slotId, placement, adUnitId });

    NativeAd.createForAdRequest(adUnitId, {
      requestNonPersonalizedAdsOnly: false,
    })
      .then((nextAd) => {
        if (!mounted) {
          nextAd.destroy();
          return;
        }
        setAd(nextAd);
        log("ad_loaded", { slotId, placement, adUnitId });
      })
      .catch((error) => {
        if (!mounted) return;
        const reason = String(error?.message || "native_ad_request_failed");
        setErrorMessage(reason);
        log("ad_failed", { slotId, placement, adUnitId, reason });
      });

    return () => {
      mounted = false;
      setAd((previous) => {
        previous?.destroy();
        return null;
      });
    };
  }, [adUnitId, placement, slotId]);

  if (!ad) {
    return (
      <View style={[styles.shell, errorMessage ? styles.errorShell : null]}>
        <Text style={styles.badge}>Ad</Text>
        <Text style={styles.title}>{title || "Sponsored insight"}</Text>
        <Text style={styles.bodyText}>
          {errorMessage || body || "Loading native ad inventory..."}
        </Text>
      </View>
    );
  }

  return (
    <NativeAdView
      nativeAd={ad}
      style={styles.shell}
      onAdImpression={() => log("ad_impression", { slotId, placement, adUnitId })}
      onAdClicked={() => log("ad_click", { slotId, placement, adUnitId })}
    >
      <View style={styles.headerRow}>
        <Text style={styles.badge}>Ad</Text>
        <Text style={styles.adChoices}>AdChoices</Text>
      </View>

      <View style={styles.contentRow}>
        <View style={styles.copyWrap}>
          <NativeAsset assetType={NativeAssetType.HEADLINE}>
            <Text style={styles.title}>{ad.headline || title || "Sponsored insight"}</Text>
          </NativeAsset>
          <NativeAsset assetType={NativeAssetType.BODY}>
            <Text style={styles.bodyText}>{ad.body || body || "Native ad body"}</Text>
          </NativeAsset>
          <NativeAsset assetType={NativeAssetType.ADVERTISER}>
            <Text style={styles.metaText}>{ad.advertiser || "Sponsored"}</Text>
          </NativeAsset>
        </View>

        <NativeAsset assetType={NativeAssetType.ICON}>
          <View style={styles.iconWrap} />
        </NativeAsset>
      </View>

      <NativeAsset assetType={NativeAssetType.MEDIA}>
        <NativeMediaView style={styles.media} />
      </NativeAsset>

      <NativeAsset assetType={NativeAssetType.CALL_TO_ACTION}>
        <Pressable style={styles.ctaButton}>
          <Text style={styles.ctaText}>{ad.callToAction || "Learn more"}</Text>
        </Pressable>
      </NativeAsset>
    </NativeAdView>
  );
}

const styles = StyleSheet.create({
  shell: {
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.3)",
    backgroundColor: "rgba(248, 250, 252, 0.95)",
    padding: 14,
    gap: 10,
    marginVertical: 10,
  },
  errorShell: {
    borderColor: "rgba(248, 113, 113, 0.4)",
  },
  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  badge: {
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.3,
    color: "#0f172a",
    backgroundColor: "rgba(226, 232, 240, 0.92)",
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
  },
  adChoices: {
    fontSize: 11,
    color: "#64748b",
    fontWeight: "600",
  },
  contentRow: {
    flexDirection: "row",
    gap: 10,
    alignItems: "flex-start",
  },
  copyWrap: {
    flex: 1,
    gap: 5,
  },
  title: {
    fontSize: 17,
    lineHeight: 23,
    fontWeight: "700",
    color: "#0f172a",
  },
  bodyText: {
    fontSize: 14,
    lineHeight: 20,
    color: "#334155",
  },
  metaText: {
    fontSize: 12,
    color: "#475569",
    fontWeight: "600",
  },
  iconWrap: {
    width: 42,
    height: 42,
    borderRadius: 12,
    backgroundColor: "rgba(203, 213, 225, 0.7)",
  },
  media: {
    width: "100%",
    height: 160,
    borderRadius: 12,
    overflow: "hidden",
  },
  ctaButton: {
    alignSelf: "flex-start",
    borderRadius: 999,
    backgroundColor: "#0b1526",
    paddingHorizontal: 16,
    paddingVertical: 9,
  },
  ctaText: {
    color: "#f8fafc",
    fontWeight: "700",
    fontSize: 13,
  },
});
