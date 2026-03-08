import React from "react";
import { createRoot, Root } from "react-dom/client";
import { AppRegistry } from "react-native";

import InjectedPanel from "./components/InjectedPanel.web";

const APP_NAME = "QuanturaRNWInject";

type MountDataset = {
  rnSlotId?: string;
  rnPlacement?: string;
  rnTitle?: string;
  rnBody?: string;
  rnContext?: string;
  rnPlaceholder?: string;
};

const roots = new Map<HTMLElement, Root>();

const parseBool = (value: string | undefined, fallback = true): boolean => {
  if (value == null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
  if (normalized === "false" || normalized === "0" || normalized === "no") return false;
  return fallback;
};

const ensureStyles = () => {
  AppRegistry.registerComponent(APP_NAME, () => InjectedPanel);
};

const mountElement = (el: HTMLElement) => {
  const data = (el.dataset || {}) as MountDataset;
  const slotId = String(data.rnSlotId || el.id || `rn-slot-${Math.random().toString(36).slice(2, 8)}`).trim();
  const placement = String(data.rnPlacement || "rnw_inline").trim();
  const title = String(data.rnTitle || "").trim();
  const body = String(data.rnBody || "").trim();
  const contextLabel = String(data.rnContext || window.location.pathname || "quantura_site").trim();
  const showPlaceholderOnWeb = parseBool(data.rnPlaceholder, true);

  let root = roots.get(el);
  if (!root) {
    root = createRoot(el);
    roots.set(el, root);
  }

  root.render(
    <InjectedPanel
      slotId={slotId}
      placement={placement}
      contextLabel={contextLabel}
      title={title || undefined}
      body={body || undefined}
      showPlaceholderOnWeb={showPlaceholderOnWeb}
    />
  );
};

const mountAll = () => {
  ensureStyles();
  const mountNodes = Array.from(document.querySelectorAll<HTMLElement>("[data-quantura-rn-root]"));
  if (!mountNodes.length) return;
  mountNodes.forEach((node) => mountElement(node));
  // eslint-disable-next-line no-console
  console.info("[RNW][Quantura] mounted roots", {
    count: mountNodes.length,
    path: window.location.pathname,
  });
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", mountAll, { once: true });
} else {
  mountAll();
}
