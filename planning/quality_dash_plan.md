## Sapflux Dash App Plan

### Goals

- Provide an interactive, browser-based view of sap flux quality plots.
- Support filtering (deployment, plant/stem, species, suspect points) and zooming without desktop tooling.
- Handle ~3.5 million rows without grinding to a halt.

### Architecture Overview

```
Dash app (Plotly + Dash callbacks)
  ├─ Parquet watcher (latest file in integration_tests/rileydata/output)
  ├─ Full-column ingestion (Polars / pyarrow)
  ├─ Local cache layer (DuckDB or Polars lazy, optional Arrow Flight)
  └─ Plot components (Plotly scattergl, facetting)
```

### Data Integration

1. **Data source**: always load the most recent parquet emitted to `integration_tests/rileydata/output`. The Dash app is not responsible for running `quality.py` or other preprocessing.
2. **File watcher**: implement a lightweight watcher (e.g., `watchdog` or periodic polling) that reloads the parquet when a newer timestamped file appears.

### Performance Strategy

1. **Column handling**: ingest all columns so exploratory visuals remain complete. Use Polars lazy operations or DuckDB views to project only what a specific graph needs at callback time.
2. **Row filtering**: downsample at query time based on user filters. Use Polars lazy expressions or DuckDB SQL to push predicate filters.
3. **Aggregation for high density**: if scatter is too dense, offer modes:
   - `scattergl` with density warning.
   - Rasterized heatmap using Datashader or Plotly’s histogram2d for >100k points.
4. **Caching**:
   - Maintain an in-memory Polars LazyFrame for the parquet file.
   - For repeated queries, use Dash’s `dcc.Store` or `cachetools` keyed by filter selection.
   - Consider a DuckDB database with persistent cache to accelerate aggregates (`duckdb.connect("sapflux_cache.duckdb")`).
5. **Chunked loading**: if the parquet is huge on startup, use `polars.scan_parquet` to lazily gather metadata, then materialize in segments.
6. **WebGL**: use Plotly `scattergl` when plotting more than ~50k points; fallback to standard scatter for tiny subsets.

### UI Components

- **Deployment dropdown** (`dcc.Dropdown`, multi-select).
- **Plant/stem dropdown** dynamically populated based on deployments.
- **Species filter** (multi-select).
- **Quality toggle** (`include suspect` / `exclude suspect`).
- **Depth coloring** toggle.
- **Chunk visualization**: optional overlay toggled by a checkbox.
- **Facet controls**: radio button to choose facet by plant/stem or use a single panel.
- **Stats panel**: summary card showing count, suspect count, time span.

### Plot Implementation

- Base figure: Plotly `px.scatter` or manual `go.Figure`.
- Use `figure.update_traces(marker={'size': 3, 'line': {'width': 0.3, 'color': 'orange'}})` for suspect outline.
- Add chunk boundaries via `figure.add_vline`.
- Provide axis format: `tickformat = "%Y-%m-%d\n%H:%M"`.
- Add `hovertemplate` to show depth, species, quality status.

### Backend Flow

1. On app start:
   - Locate latest parquet.
   - Build Polars LazyFrame with column pruning.
   - Extract unique metadata (deployments, plants/stems, species) for dropdown options.
2. Dash callbacks:
   - `@callback` triggered by dropdowns.
   - Execute Polars query with `.filter` for selected deployment/stem/species.
   - Drop suspect rows if the toggle is off.
   - Convert to Pandas (only the filtered subset) for Plotly.
   - Return a `figure` to the `dcc.Graph`.
3. Error handling: if the subset is large (>200k rows), show a toast suggesting the heatmap mode.

### Scaling Tips

- **Runtime context**: this runs locally on an M1 MacBook Pro; no Hetzner deployment required.
- **Memory**: a ~100 MB parquet with all columns should fit comfortably; still avoid unnecessary Pandas copies.
- **Alternative**: if the dataset grows dramatically, pre-aggregate:
  - Use DuckDB to compute daily/hourly means per plant/stem.
  - Expose a resolution toggle in the UI (raw vs aggregated).
- **Security**: optional for localhost; no production auth needed.

### Next Steps

1. Scaffold Dash app (e.g., `scripts/quality_dash.py`) with CLI flag for parquet path.
2. Implement parquet watcher + caching with Polars/DuckDB.
3. Build callbacks/filters and Plotly visuals.
4. Optional niceties: CLI reload command, packaging into a local `uv` entry, Apple Silicon optimization notes.
