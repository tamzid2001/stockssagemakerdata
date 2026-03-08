import React from "react";
import { StyleSheet, Text, View } from "react-native";

import { isNativeRuntime } from "../bridge";
import NativeAdSlot from "./NativeAdSlot.web";

interface InjectedPanelProps {
  slotId: string;
  placement: string;
  contextLabel: string;
  title?: string;
  body?: string;
  showPlaceholderOnWeb?: boolean;
}

export default function InjectedPanel({
  slotId,
  placement,
  contextLabel,
  title,
  body,
  showPlaceholderOnWeb = true,
}: InjectedPanelProps) {
  if (!isNativeRuntime()) {
    return null;
  }

  return (
    <View style={styles.wrapper}>
      <View style={styles.header}>
        <Text style={styles.kicker}>React Native for Web Surface</Text>
        <Text style={styles.heading}>{title || "Injected Quantura panel"}</Text>
        <Text style={styles.subheading}>{body || `Shared RN UI mounted in ${contextLabel}.`}</Text>
      </View>
      <NativeAdSlot
        slotId={slotId}
        placement={placement}
        title="Sponsored insight"
        body="Native ads render only in installed Quantura iOS/Android builds."
        showPlaceholderOnWeb={showPlaceholderOnWeb}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    borderRadius: 20,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.28)",
    backgroundColor: "rgba(255, 255, 255, 0.72)",
    paddingHorizontal: 14,
    paddingVertical: 14,
    marginVertical: 14,
    gap: 8,
  },
  header: {
    gap: 4,
  },
  kicker: {
    fontSize: 11,
    lineHeight: 16,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
    color: "#64748b",
  },
  heading: {
    fontSize: 19,
    lineHeight: 25,
    fontWeight: "700",
    color: "#0f172a",
  },
  subheading: {
    fontSize: 14,
    lineHeight: 20,
    color: "#334155",
  },
});
