import assert from "node:assert/strict";
import test from "node:test";

import { FiscalDataClient } from "./FiscalDataClient";

test("builds filter strings with gte and in operators", () => {
  const filter = FiscalDataClient.buildFilterString([
    { field: "record_date", op: "gte", value: "2015-01-01" },
    { field: "country_currency_desc", op: "in", value: ["Canada-Dollar", "Mexico-Peso"] },
  ]);

  assert.equal(filter, "record_date:gte:2015-01-01,country_currency_desc:in:(Canada-Dollar,Mexico-Peso)");
});

test("builds URL with fields filter sort and paging", () => {
  const url = FiscalDataClient.buildUrl("https://api.fiscaldata.treasury.gov/services/api/fiscal_service", {
    endpoint: "/v1/accounting/od/rates_of_exchange",
    fields: ["country_currency_desc", "exchange_rate", "record_date"],
    filters: [{ field: "record_date", op: "gte", value: "2025-01-01" }],
    sort: ["-record_date"],
    page: { number: 2, size: 100 },
  });

  assert.match(url, /\/v1\/accounting\/od\/rates_of_exchange\?/);
  assert.match(url, /fields=country_currency_desc%2Cexchange_rate%2Crecord_date/);
  assert.match(url, /filter=record_date%3Agte%3A2025-01-01/);
  assert.match(url, /sort=-record_date/);
  assert.match(url, /page%5Bnumber%5D=2/);
  assert.match(url, /page%5Bsize%5D=100/);
  assert.match(url, /format=json/);
});

test("normalizes Fiscal Data meta object", () => {
  const normalized = FiscalDataClient.normalizeMeta({
    labels: { record_date: "Record Date", exchange_rate: "Exchange Rate" },
    dataTypes: { record_date: "DATE", exchange_rate: "NUMBER" },
    dataFormats: { record_date: "YYYY-MM-DD", exchange_rate: "10.2" },
    "total-count": "1000",
    "total-pages": "10",
    count: "100",
  });

  assert.equal(normalized.labels.record_date, "Record Date");
  assert.equal(normalized.dataTypes.exchange_rate, "NUMBER");
  assert.equal(normalized.dataFormats.record_date, "YYYY-MM-DD");
  assert.equal(normalized.totalCount, 1000);
  assert.equal(normalized.totalPages, 10);
  assert.equal(normalized.count, 100);
});
