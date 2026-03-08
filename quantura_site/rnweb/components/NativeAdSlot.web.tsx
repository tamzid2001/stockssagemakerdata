import React, { useEffect, useMemo, useRef, useState } from "react";
import { Image, Pressable, StyleSheet, Text, View } from "react-native";

import { isNativeRuntime, reportNativeFeedEvent, requestNativeFeedAd } from "../bridge";
import type { NativeFeedAdBridgePayload, NativeAdSlotProps } from "../types";

type SlotStatus = "idle" | "loading" | "ready" | "failed";

const asDisplayUrl = (value: string | undefined): string => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^data:image\//i.test(raw)) return raw;
  if (/^https?:\/\//i.test(raw)) return raw;
  return `data:image/png;base64,${raw}`;
};

const log = (eventName: string, payload: Record<string, unknown>) => {
  // Useful runtime trace for native-webview ad rendering diagnostics.
  // eslint-disable-next-line no-console
  console.info(`[RNW][NativeAdSlot] ${eventName}`, payload);
};

export default function NativeAdSlot({
  slotId,
  placement,
  title,
  body,
  showPlaceholderOnWeb = true,
}: NativeAdSlotProps) {
  const native = isNativeRuntime();
  const [status, setStatus] = useState<SlotStatus>(native ? "loading" : "idle");
  const [payload, setPayload] = useState<NativeFeedAdBridgePayload | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const cardRef = useRef<HTMLDivElement | null>(null);
  const impressionTrackedRef = useRef(false);

  const ad = payload?.ad && typeof payload.ad === "object" ? payload.ad : {};
  const adUnitId = String(payload?.adUnitId || ad.adUnitId || "").trim();
  const iconUrl = asDisplayUrl(ad.iconDataUrl || ad.iconUrl);
  const mediaUrl = asDisplayUrl(ad.mediaDataUrl || ad.mediaUrl);
  const headline = String(ad.headline || title || "Sponsored insight").trim();
  const copy = String(ad.body || body || "This section is sponsored in the native Quantura app.").trim();
  const cta = String(ad.callToAction || "Learn more").trim();
  const advertiser = String(ad.advertiser || "Sponsored").trim();
  const destinationUrl = /^https?:\/\//i.test(String(ad.destinationUrl || "").trim()) ? String(ad.destinationUrl || "").trim() : "";

  useEffect(() => {
    if (!native) {
      setStatus("idle");
      return;
    }
    let active = true;
    setStatus("loading");
    log("ad_request", { slotId, placement, runtime: "native_webview" });

    requestNativeFeedAd({ slotId, placement })
      .then((detail) => {
        if (!active) return;
        setPayload(detail);
        setStatus("ready");
        log("ad_loaded", { slotId, placement, adUnitId: detail.adUnitId || detail.ad?.adUnitId || "" });
      })
      .catch((error) => {
        if (!active) return;
        const reason = String(error?.message || "load_failed");
        setErrorMessage(reason);
        setStatus("failed");
        log("ad_failed", { slotId, placement, reason });
      });

    return () => {
      active = false;
    };
  }, [native, placement, slotId]);

  useEffect(() => {
    if (status !== "ready" || !native || impressionTrackedRef.current) return;
    const node = cardRef.current;
    if (!node || typeof IntersectionObserver === "undefined") return;

    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting || entry.intersectionRatio < 0.5 || impressionTrackedRef.current) return;
          impressionTrackedRef.current = true;
          reportNativeFeedEvent("nativeFeedAdImpression", { slotId, placement, adUnitId });
          log("ad_impression", { slotId, placement, adUnitId });
          observer.disconnect();
        });
      },
      { threshold: [0.5] }
    );

    observer.observe(node);
    return () => observer.disconnect();
  }, [adUnitId, native, placement, slotId, status]);

  const webPlaceholder = useMemo(() => {
    if (!showPlaceholderOnWeb || native) return null;
    return (
      <View style={[styles.shell, styles.placeholderShell]}>
        <Text accessibilityRole="text" style={styles.placeholderLabel}>
          Native app slot
        </Text>
        <Text style={styles.placeholderBody}>This sponsored slot appears only in Quantura iOS and Android builds.</Text>
      </View>
    );
  }, [native, showPlaceholderOnWeb]);

  if (!native) return webPlaceholder;

  if (status === "loading") {
    return (
      <View style={styles.shell}>
        <Text style={styles.badge}>Ad</Text>
        <View style={styles.skeletonLineShort} />
        <View style={styles.skeletonLine} />
        <View style={styles.skeletonMedia} />
      </View>
    );
  }

  if (status === "failed") {
    return (
      <View style={[styles.shell, styles.failedShell]}>
        <Text style={styles.badge}>Ad</Text>
        <Text style={styles.title}>Sponsored insight</Text>
        <Text style={styles.bodyText}>Ad inventory is refreshing. Continue using Quantura while this slot retries.</Text>
        <Text style={styles.metaText}>{errorMessage || "native_feed_ad_failed"}</Text>
      </View>
    );
  }

  return (
    <View ref={cardRef as unknown as React.Ref<View>} style={styles.shell}>
      <View style={styles.headerRow}>
        <Text style={styles.badge}>Ad</Text>
        <Text style={styles.adChoices}>AdChoices</Text>
      </View>
      <View style={styles.mainRow}>
        <View style={styles.copyWrap}>
          <Text numberOfLines={2} style={styles.title}>
            {headline}
          </Text>
          <Text numberOfLines={3} style={styles.bodyText}>
            {copy}
          </Text>
          <Text numberOfLines={1} style={styles.metaText}>
            {advertiser}
          </Text>
        </View>
        {iconUrl ? <Image accessibilityIgnoresInvertColors source={{ uri: iconUrl }} style={styles.icon} /> : null}
      </View>
      {mediaUrl ? <Image accessibilityIgnoresInvertColors source={{ uri: mediaUrl }} style={styles.media} /> : null}
      <Pressable
        accessibilityRole="button"
        onPress={() => {
          reportNativeFeedEvent("nativeFeedAdClick", { slotId, placement, adUnitId });
          log("ad_click", { slotId, placement, adUnitId });
          if (destinationUrl) {
            window.open(destinationUrl, "_blank", "noopener,noreferrer");
          }
        }}
        style={styles.ctaButton}
      >
        <Text style={styles.ctaText}>{cta}</Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  shell: {
    borderRadius: 18,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.32)",
    backgroundColor: "rgba(248, 250, 252, 0.9)",
    paddingVertical: 14,
    paddingHorizontal: 14,
    marginVertical: 12,
    gap: 10,
  },
  placeholderShell: {
    backgroundColor: "rgba(241, 245, 249, 0.88)",
  },
  placeholderLabel: {
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.4,
    textTransform: "uppercase",
    color: "#64748b",
  },
  placeholderBody: {
    fontSize: 13,
    lineHeight: 19,
    color: "#334155",
  },
  failedShell: {
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
    backgroundColor: "rgba(226, 232, 240, 0.9)",
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
    overflow: "hidden",
    alignSelf: "flex-start",
  },
  adChoices: {
    fontSize: 11,
    color: "#64748b",
    fontWeight: "600",
  },
  mainRow: {
    flexDirection: "row",
    gap: 10,
    alignItems: "flex-start",
  },
  copyWrap: {
    flex: 1,
    gap: 4,
  },
  title: {
    fontSize: 18,
    lineHeight: 24,
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
  icon: {
    width: 44,
    height: 44,
    borderRadius: 12,
    backgroundColor: "#e2e8f0",
  },
  media: {
    width: "100%",
    height: 164,
    borderRadius: 14,
    backgroundColor: "#dbeafe",
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
  skeletonLine: {
    height: 11,
    borderRadius: 999,
    backgroundColor: "rgba(148, 163, 184, 0.35)",
    width: "100%",
  },
  skeletonLineShort: {
    height: 11,
    borderRadius: 999,
    backgroundColor: "rgba(148, 163, 184, 0.35)",
    width: "55%",
  },
  skeletonMedia: {
    width: "100%",
    height: 134,
    borderRadius: 12,
    backgroundColor: "rgba(148, 163, 184, 0.25)",
  },
});
