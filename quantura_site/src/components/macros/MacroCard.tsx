import React from "react";

type MacroCardProps = {
  title: string;
  endpoint: string;
  cadence?: string;
  children?: React.ReactNode;
  onViewDetails?: () => void;
  onLoadMore?: () => void;
  canLoadMore?: boolean;
};

export function MacroCard({
  title,
  endpoint,
  cadence,
  children,
  onViewDetails,
  onLoadMore,
  canLoadMore = false,
}: MacroCardProps) {
  return (
    <article className="card">
      <header className="order-header">
        <div className="order-title">{title}</div>
        <span className="status pending">{cadence || "periodic"}</span>
      </header>
      <p className="small muted" style={{ marginTop: 6 }}>
        {endpoint}
      </p>
      <div style={{ marginTop: 12 }}>{children}</div>
      <div className="hero-actions" style={{ marginTop: 12 }}>
        <button className="cta secondary small" type="button" onClick={onViewDetails}>
          View details
        </button>
        {canLoadMore ? (
          <button className="cta secondary small" type="button" onClick={onLoadMore}>
            Load more
          </button>
        ) : null}
      </div>
    </article>
  );
}
