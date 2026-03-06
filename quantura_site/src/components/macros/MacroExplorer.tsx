import React from "react";

type MacroExplorerProps = {
  endpoint: string;
  fields: string;
  filter: string;
  sort: string;
  onEndpointChange: (value: string) => void;
  onFieldsChange: (value: string) => void;
  onFilterChange: (value: string) => void;
  onSortChange: (value: string) => void;
  onRun: () => void;
};

export function MacroExplorer(props: MacroExplorerProps) {
  return (
    <div className="card">
      <h3>Macro Explorer</h3>
      <div className="form-grid">
        <div className="field">
          <label className="label">Endpoint</label>
          <input value={props.endpoint} onChange={(event) => props.onEndpointChange(event.target.value)} />
        </div>
        <div className="field">
          <label className="label">Fields (csv)</label>
          <input value={props.fields} onChange={(event) => props.onFieldsChange(event.target.value)} />
        </div>
        <div className="field">
          <label className="label">Filter</label>
          <input value={props.filter} onChange={(event) => props.onFilterChange(event.target.value)} />
        </div>
        <div className="field">
          <label className="label">Sort (csv)</label>
          <input value={props.sort} onChange={(event) => props.onSortChange(event.target.value)} />
        </div>
      </div>
      <button className="cta small" type="button" onClick={props.onRun}>
        Run query
      </button>
    </div>
  );
}
