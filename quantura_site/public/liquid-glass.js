(() => {
  const SVG_NS = "http://www.w3.org/2000/svg";
  const XLINK_NS = "http://www.w3.org/1999/xlink";
  const ROOT_ATTR = "data-liquid-glass-mode";
  const SVG_ID = "quantura-liquid-glass-defs";
  const PANEL_FILTER_ID = "quantura-liquid-glass-panel-filter";
  const CONTROL_FILTER_ID = "quantura-liquid-glass-control-filter";

  const clamp = (value, min = 0, max = 1) => Math.min(max, Math.max(min, value));

  const supportsBackdropBlur = () => {
    try {
      return Boolean(
        window.CSS?.supports?.("backdrop-filter: blur(1px)")
          || window.CSS?.supports?.("-webkit-backdrop-filter: blur(1px)")
      );
    } catch (_error) {
      return false;
    }
  };

  const isChromiumLike = () => {
    const brands = Array.isArray(navigator.userAgentData?.brands) ? navigator.userAgentData.brands : [];
    if (brands.length) {
      const brandText = brands.map((entry) => String(entry?.brand || "")).join(" ");
      return /(Chromium|Google Chrome|Microsoft Edge|Opera)/i.test(brandText);
    }

    const userAgent = String(navigator.userAgent || "");
    if (/Firefox|FxiOS/i.test(userAgent)) return false;
    if (/Safari/i.test(userAgent) && !/Chrome|CriOS|Chromium|Edg|OPR/i.test(userAgent)) return false;
    return /Chrome|CriOS|Chromium|Edg|OPR/i.test(userAgent);
  };

  const createNode = (name, attrs = {}) => {
    const node = document.createElementNS(SVG_NS, name);
    Object.entries(attrs).forEach(([key, value]) => {
      node.setAttribute(key, String(value));
    });
    return node;
  };

  const buildDisplacementMapDataUrl = ({
    size = 180,
    innerThreshold = 0.64,
    fieldPower = 4,
    gain = 1,
  } = {}) => {
    const canvas = document.createElement("canvas");
    canvas.width = size;
    canvas.height = size;
    const context = canvas.getContext("2d", { willReadFrequently: true });
    if (!context) return "";

    const image = context.createImageData(size, size);
    const data = image.data;

    for (let y = 0; y < size; y += 1) {
      for (let x = 0; x < size; x += 1) {
        const offset = (y * size + x) * 4;
        const px = (x / (size - 1)) * 2 - 1;
        const py = (y / (size - 1)) * 2 - 1;
        const ax = Math.abs(px);
        const ay = Math.abs(py);
        const squircleRadius = Math.pow(Math.pow(ax, fieldPower) + Math.pow(ay, fieldPower), 1 / fieldPower);

        let displacement = 0;
        if (squircleRadius > innerThreshold) {
          const edgeProgress = clamp((squircleRadius - innerThreshold) / (1 - innerThreshold));
          displacement = clamp(1 - Math.pow(1 - edgeProgress, fieldPower), 0, 1) * gain;
        }

        const gradientX = px === 0 ? 0 : Math.sign(px) * Math.pow(ax, fieldPower - 1);
        const gradientY = py === 0 ? 0 : Math.sign(py) * Math.pow(ay, fieldPower - 1);
        const gradientLength = Math.hypot(gradientX, gradientY) || 1;
        const normalX = gradientX / gradientLength;
        const normalY = gradientY / gradientLength;

        data[offset] = Math.round(128 + normalX * displacement * 127);
        data[offset + 1] = Math.round(128 + normalY * displacement * 127);
        data[offset + 2] = 128;
        data[offset + 3] = 255;
      }
    }

    context.putImageData(image, 0, 0);
    return canvas.toDataURL("image/png");
  };

  const appendFilter = (defs, { id, mapUrl, scale }) => {
    const filter = createNode("filter", {
      id,
      x: "-22%",
      y: "-22%",
      width: "144%",
      height: "144%",
      "color-interpolation-filters": "sRGB",
    });

    const displacementMap = createNode("feImage", {
      x: "0",
      y: "0",
      width: "100%",
      height: "100%",
      preserveAspectRatio: "none",
      result: "liquid-map",
    });
    displacementMap.setAttribute("href", mapUrl);
    displacementMap.setAttributeNS(XLINK_NS, "href", mapUrl);

    const displacement = createNode("feDisplacementMap", {
      in: "SourceGraphic",
      in2: "liquid-map",
      scale,
      xChannelSelector: "R",
      yChannelSelector: "G",
      result: "liquid-displaced",
    });

    const soften = createNode("feGaussianBlur", {
      in: "liquid-displaced",
      stdDeviation: "0.12",
      edgeMode: "duplicate",
      result: "liquid-soft",
    });

    const blend = createNode("feBlend", {
      in: "liquid-displaced",
      in2: "liquid-soft",
      mode: "screen",
    });

    filter.append(displacementMap, displacement, soften, blend);
    defs.appendChild(filter);
  };

  const ensureSvgDefs = () => {
    if (document.getElementById(SVG_ID)) return;

    const mountPoint = document.body || document.documentElement;
    if (!mountPoint) return;

    const svg = createNode("svg", {
      id: SVG_ID,
      "aria-hidden": "true",
      focusable: "false",
      width: "0",
      height: "0",
      style: "position:fixed;left:-9999px;top:-9999px;pointer-events:none;overflow:hidden",
    });
    const defs = createNode("defs");

    const panelMap = buildDisplacementMapDataUrl({
      size: 192,
      innerThreshold: 0.62,
      fieldPower: 4,
      gain: 1,
    });
    const controlMap = buildDisplacementMapDataUrl({
      size: 160,
      innerThreshold: 0.7,
      fieldPower: 4,
      gain: 0.84,
    });

    if (!panelMap || !controlMap) return;

    appendFilter(defs, {
      id: PANEL_FILTER_ID,
      mapUrl: panelMap,
      scale: 20,
    });
    appendFilter(defs, {
      id: CONTROL_FILTER_ID,
      mapUrl: controlMap,
      scale: 11,
    });

    svg.appendChild(defs);
    mountPoint.appendChild(svg);
  };

  const ensure = () => {
    if (!document?.documentElement) return { mode: "fallback" };

    const mode = supportsBackdropBlur() && isChromiumLike() ? "refraction" : "fallback";
    document.documentElement.setAttribute(ROOT_ATTR, mode);
    if (mode === "refraction") ensureSvgDefs();
    return { mode };
  };

  window.QuanturaLiquidGlass = {
    ensure,
    isChromiumLike,
    supportsBackdropBlur,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", ensure, { once: true });
  } else {
    ensure();
  }
})();
