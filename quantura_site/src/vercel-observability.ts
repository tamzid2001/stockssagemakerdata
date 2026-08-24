import { inject } from "@vercel/analytics";
import { injectSpeedInsights } from "@vercel/speed-insights";

declare const __VERCEL_OBSERVABILITY_CLIENT_CONFIG__: string;

const clientConfig = __VERCEL_OBSERVABILITY_CLIENT_CONFIG__ || undefined;

inject({ mode: "production" }, clientConfig);
injectSpeedInsights({}, clientConfig);
