import React from "react";
import { StyleSheet, View } from "react-native";

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
  showPlaceholderOnWeb = true,
}: InjectedPanelProps) {
  if (!isNativeRuntime()) {
    return null;
  }

  return (
    <View style={styles.wrapper}>
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
    marginVertical: 4,
  },
});
