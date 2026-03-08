export type NativePlatform = "ios" | "android";

export interface NativeAdAsset {
  headline?: string;
  body?: string;
  callToAction?: string;
  advertiser?: string;
  iconUrl?: string;
  mediaUrl?: string;
  iconDataUrl?: string;
  mediaDataUrl?: string;
  adUnitId?: string;
  destinationUrl?: string;
}

export interface NativeFeedAdBridgePayload {
  ok?: boolean;
  slotId?: string;
  placement?: string;
  adUnitId?: string;
  error?: string;
  ad?: NativeAdAsset;
}

export interface NativeAdSlotProps {
  slotId: string;
  placement: string;
  title?: string;
  body?: string;
  showPlaceholderOnWeb?: boolean;
  className?: string;
}
