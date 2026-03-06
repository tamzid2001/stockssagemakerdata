import { refreshFiscalRegistryDefaults } from "../fiscaldataProxy";

type RefreshOptions = {
  db: FirebaseFirestore.Firestore;
  fetchImpl?: typeof fetch;
};

export async function runScheduledFiscaldataRefresh(options: RefreshOptions): Promise<void> {
  const startedAt = Date.now();
  const result = await refreshFiscalRegistryDefaults(options.db, options.fetchImpl);
  const durationMs = Date.now() - startedAt;
  const summary = {
    refreshed: result.refreshed,
    failed: result.failed.length,
    durationMs,
  };
  if (result.failed.length) {
    console.error("[FiscalData] scheduled refresh completed with partial failures", {
      ...summary,
      failures: result.failed,
    });
  } else {
    console.log("[FiscalData] scheduled refresh completed", summary);
  }
}
