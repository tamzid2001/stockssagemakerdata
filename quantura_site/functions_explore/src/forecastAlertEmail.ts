import type { ForecastAlertEmailProvider, ForecastAlertEvent } from "./forecastPriceAlerts";

function cleanText(value: unknown, maxLength = 500): string {
  return String(value ?? "")
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function htmlEscape(value: unknown): string {
  return cleanText(value, 2000)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatPrice(value: number): string {
  return Number(value).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 6 });
}

export function buildForecastBoundaryEmail(event: ForecastAlertEvent): { subject: string; text: string; html: string } {
  const first = event.crossings[0];
  const subject = event.crossings.length === 1
    ? `${event.ticker} crossed ${first.direction} ${first.boundary} forecast boundary`
    : `${event.ticker} crossed ${event.crossings.length} forecast boundaries`;
  const textRows = event.crossings.map((crossing) => [
    `${crossing.boundary}: crossed ${crossing.direction}`,
    `Forecast date: ${crossing.forecastDate}`,
    `Boundary: $${formatPrice(crossing.boundaryValue)}`,
    `Previous price: $${formatPrice(crossing.previousPrice)}`,
    `Current price: $${formatPrice(crossing.currentPrice)}`,
    `Market timestamp: ${crossing.priceTimestamp}`,
  ].join("\n"));
  const text = [
    subject,
    "",
    `${event.ticker} moved across a monitored forecast quantile boundary.`,
    "",
    textRows.join("\n\n"),
    "",
    event.analysisUrl ? `Open this private analysis: ${event.analysisUrl}` : "",
    "",
    "This is a model-derived monitoring notification, not financial advice.",
  ].filter((line) => line !== "").join("\n");
  const rows = event.crossings.map((crossing) => `
    <tr>
      <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;font-weight:700;">${htmlEscape(crossing.boundary)}</td>
      <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;text-transform:capitalize;">Crossed ${htmlEscape(crossing.direction)}</td>
      <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;">$${htmlEscape(formatPrice(crossing.boundaryValue))}</td>
      <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;">$${htmlEscape(formatPrice(crossing.previousPrice))} → $${htmlEscape(formatPrice(crossing.currentPrice))}</td>
      <td style="padding:10px 12px;border-bottom:1px solid #e5e7eb;">${htmlEscape(crossing.forecastDate)}</td>
    </tr>`).join("");
  const html = `<!doctype html>
<html><body style="margin:0;background:#f4f6f8;color:#17202a;font-family:Inter,Arial,sans-serif;">
  <div style="max-width:680px;margin:0 auto;padding:32px 18px;">
    <div style="background:#101820;color:#fff;border-radius:14px 14px 0 0;padding:22px 26px;">
      <div style="font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:#83e6c0;">Quantura Forecast Alert</div>
      <h1 style="font-size:23px;line-height:1.3;margin:8px 0 0;">${htmlEscape(subject)}</h1>
    </div>
    <div style="background:#fff;border:1px solid #e5e7eb;border-top:0;border-radius:0 0 14px 14px;padding:24px 26px;">
      <p style="margin:0 0 18px;line-height:1.6;">${htmlEscape(event.ticker)} moved across a monitored forecast boundary using the latest completed Alpaca 1-minute bar close.</p>
      <div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:14px;"><thead><tr style="text-align:left;background:#f6f8fa;"><th style="padding:10px 12px;">Boundary</th><th style="padding:10px 12px;">Direction</th><th style="padding:10px 12px;">Level</th><th style="padding:10px 12px;">Price move</th><th style="padding:10px 12px;">Forecast date</th></tr></thead><tbody>${rows}</tbody></table></div>
      <p style="font-size:13px;color:#5f6b76;margin:18px 0 0;">Market timestamp: ${htmlEscape(first.priceTimestamp)}</p>
      ${event.analysisUrl ? `<p style="margin:22px 0 0;"><a href="${htmlEscape(event.analysisUrl)}" style="display:inline-block;background:#146c55;color:#fff;text-decoration:none;border-radius:8px;padding:11px 16px;font-weight:700;">Open private analysis</a></p>` : ""}
      <p style="font-size:12px;color:#6b7280;line-height:1.5;margin:24px 0 0;">This is a model-derived monitoring notification, not financial advice or a guarantee of any trading outcome.</p>
    </div>
  </div>
</body></html>`;
  return { subject, text, html };
}

export class ResendForecastAlertEmailProvider implements ForecastAlertEmailProvider {
  private readonly fromEmail: string;
  private readonly apiKey: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: { apiKey?: string; fromEmail?: string; fetchImpl?: typeof fetch } = {}) {
    this.apiKey = cleanText(options.apiKey || process.env.RESEND_API_KEY, 500);
    this.fromEmail = cleanText(
      options.fromEmail || process.env.FORECAST_ALERT_EMAIL_FROM || process.env.AUTOMATION_EMAIL_FROM || "hell@quantura.studio",
      320
    );
    this.fetchImpl = options.fetchImpl || fetch;
  }

  async sendBoundaryCrossing(event: ForecastAlertEvent): Promise<{ messageId: string }> {
    if (!this.apiKey || !this.fromEmail) {
      const error = new Error("Forecast alert email sender is not configured.") as Error & { code?: string };
      error.code = "email_configuration";
      throw error;
    }
    if (!event.email || !event.email.includes("@")) {
      const error = new Error("A verified alert email is required.") as Error & { code?: string };
      error.code = "invalid_recipient";
      throw error;
    }
    const content = buildForecastBoundaryEmail(event);
    let response: Response;
    try {
      response = await this.fetchImpl("https://api.resend.com/emails", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.apiKey}`,
          "Content-Type": "application/json",
          "Idempotency-Key": `forecast-alert-${event.id}`,
        },
        body: JSON.stringify({
          from: this.fromEmail,
          to: [event.email],
          subject: content.subject,
          text: content.text,
          html: content.html,
          tags: [
            { name: "message_type", value: "forecast_boundary_alert" },
            { name: "ticker", value: cleanText(event.ticker, 24).replace(/[^A-Za-z0-9_-]/g, "_") || "unknown" },
          ],
        }),
        signal: AbortSignal.timeout(20_000),
      });
    } catch (_error) {
      const error = new Error("The email provider could not be reached.") as Error & { code?: string };
      error.code = "email_network";
      throw error;
    }
    if (!response.ok) {
      const error = new Error("The email provider rejected this notification.") as Error & { code?: string };
      error.code = response.status === 429 ? "email_rate_limit" : "email_provider";
      throw error;
    }
    const payload = await response.json().catch(() => ({})) as { id?: unknown };
    return { messageId: cleanText(payload.id, 220) || event.id };
  }
}
