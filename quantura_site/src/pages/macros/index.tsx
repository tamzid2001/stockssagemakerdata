import React from "react";

import { MacroDashboard } from "../../components/macros/MacroDashboard";

export default function MacrosPage() {
  return (
    <main>
      <section className="section">
        <div className="container">
          <h1>Macro Dashboard</h1>
          <p className="small muted">U.S. Treasury Fiscal Data API macro cards.</p>
          <MacroDashboard />
        </div>
      </section>
    </main>
  );
}
