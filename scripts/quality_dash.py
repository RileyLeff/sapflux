# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "dash>=2.17",
#     "dash-extensions>=1.0",
#     "plotly>=5.20",
#     "polars>=0.20",
#     "pandas>=2.2",
#     "pyarrow"
# ]
# ///

from __future__ import annotations

import argparse
import json
import math
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import polars as pl
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update
from dash.exceptions import PreventUpdate
from dash_extensions import EventListener
import plotly.express as px
import plotly.graph_objects as go


APP_TITLE = "Sapflux Quality Dashboard"
DEFAULT_DATA_DIR = Path("integration_tests/rileydata/output")
DEFAULT_MAX_POINTS = 120_000
REFRESH_INTERVAL_MS = 30_000
CACHE_SIZE = 16
STEM_METADATA_COLUMNS = [
    "project_name",
    "project_code",
    "site_name",
    "zone_name",
    "plot_name",
    "sdi12_address",
    "plant_code",
    "species_scientific_name",
    "stem_code",
    "deployment_id",
]
STEM_DISPLAY_FIELDS = [
    ("project_name", "Project"),
    ("project_code", "Project code"),
    ("species_scientific_name", "Species"),
    ("plant_code", "Plant"),
    ("stem_code", "Stem"),
    ("deployment_id", "Deployment"),
    ("site_name", "Site"),
    ("zone_name", "Zone"),
    ("plot_name", "Plot"),
    ("sdi12_address", "SDI address"),
]


@dataclass
class FilterResult:
    dataframe: pd.DataFrame
    total_rows: int
    displayed_rows: int
    suspect_rows: int
    time_min: datetime | None
    time_max: datetime | None

    @property
    def downsampled(self) -> bool:
        return self.displayed_rows < self.total_rows


def _format_stem_option(option: dict[str, Any]) -> dict[str, Any]:
    lines: list[str] = option.get("lines") or [option.get("label", "")]
    label_component = html.Div(
        [
            html.Div(
                lines[0],
                style={"fontWeight": "600", "fontSize": "14px"},
            )
        ]
        + [
            html.Div(
                line,
                style={"fontSize": "12px", "color": "#555555"},
            )
            for line in lines[1:]
        ],
        style={"display": "flex", "flexDirection": "column"},
    )
    return {"label": label_component, "value": option["value"]}


class DataCache:
    def __init__(
        self,
        *,
        explicit_path: Path | None,
        data_dir: Path,
        max_points: int,
    ) -> None:
        self._explicit_path = explicit_path
        self._data_dir = data_dir
        self._max_points = max_points

        self._dataset_path: Path | None = None
        self._dataset_mtime: float | None = None
        self._lazy_frame: pl.LazyFrame | None = None
        self._metadata: dict[str, Any] = {}
        self._filter_cache: OrderedDict[tuple[Any, ...], FilterResult] = OrderedDict()
        self._dataset_signature: tuple[str, int] | None = None
        self._stem_lookup: dict[str, dict[str, Any]] = {}
        self._stem_order: list[str] = []

        self._load_if_needed(force=True)

    def refresh(self) -> dict[str, Any]:
        self._load_if_needed(force=False)
        return self._metadata

    def filtered_dataframe(
        self,
        *,
        stem_value: str | None,
        thermistors: Sequence[Any],
        include_suspect: bool,
        time_range: tuple[datetime | None, datetime | None] | None,
    ) -> FilterResult:
        if self._lazy_frame is None or self._dataset_signature is None:
            raise RuntimeError("Dataset not loaded")

        selected_thermistors = list(thermistors) if thermistors else []
        normalized_thermistors = tuple(
            sorted(
                selected_thermistors,
                key=lambda value: (0, "") if value is None else (1, str(value)),
            )
        )

        key = (
            self._dataset_signature,
            stem_value,
            normalized_thermistors,
            bool(include_suspect),
            time_range[0].isoformat() if time_range and time_range[0] else None,
            time_range[1].isoformat() if time_range and time_range[1] else None,
        )

        cached = self._filter_cache.get(key)
        if cached is not None:
            # Move to the end to mark as most recently used.
            self._filter_cache.move_to_end(key)
            return cached

        result = self._compute_filtered_dataframe(
            stem_value=stem_value,
            thermistors=selected_thermistors,
            include_suspect=include_suspect,
            time_range=time_range,
        )

        self._filter_cache[key] = result
        if len(self._filter_cache) > CACHE_SIZE:
            self._filter_cache.popitem(last=False)

        return result

    def metadata_payload(self) -> dict[str, Any]:
        return self._metadata

    def stem_metadata(self, stem_value: str | None) -> dict[str, Any] | None:
        if not stem_value:
            return None
        payload = self._stem_lookup.get(stem_value)
        return dict(payload) if payload is not None else None

    def _load_if_needed(self, *, force: bool) -> None:
        target_path = self._resolve_target_path()
        target_stat = target_path.stat()
        needs_reload = (
            force
            or self._dataset_path is None
            or self._dataset_mtime is None
            or self._dataset_path != target_path
            or target_stat.st_mtime > self._dataset_mtime
        )

        if not needs_reload:
            return

        lazy_frame = pl.scan_parquet(target_path)
        metadata, stem_lookup, stem_order = self._build_metadata(
            lazy_frame, target_path, target_stat.st_mtime
        )

        self._dataset_path = target_path
        self._dataset_mtime = target_stat.st_mtime
        self._lazy_frame = lazy_frame
        self._metadata = metadata
        self._stem_lookup = stem_lookup
        self._stem_order = stem_order
        self._dataset_signature = (str(target_path.resolve()), int(target_stat.st_mtime))
        self._filter_cache.clear()

        print(
            f"[quality_dash] Loaded {target_path.name} "
            f"({metadata['record_count']:,} rows, mtime={metadata['last_modified']})"
        )

    def _resolve_target_path(self) -> Path:
        if self._explicit_path is not None:
            if not self._explicit_path.exists():
                raise FileNotFoundError(f"Parquet file not found: {self._explicit_path}")
            return self._explicit_path

        if not self._data_dir.exists():
            raise FileNotFoundError(
                f"Data directory not found: {self._data_dir}. "
                "Run the Rust pipeline before launching the dashboard."
            )

        candidates = sorted(
            (p for p in self._data_dir.glob("*.parquet") if p.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise FileNotFoundError(
                f"No parquet files found in {self._data_dir}. "
                "Expected the quality pipeline to have emitted at least one file."
            )
        return candidates[0]

    def _build_metadata(
        self,
        lazy_frame: pl.LazyFrame,
        data_path: Path,
        mtime: float,
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]], list[str]]:
        schema = lazy_frame.collect_schema()

        def clean(value: Any, fallback: str) -> str:
            if value is None:
                return fallback
            if isinstance(value, str):
                text = value.strip()
                return text if text else fallback
            return str(value)

        total_rows = lazy_frame.select(pl.len().alias("count")).collect().item()

        depth_values: list[Any] = []
        if "thermistor_depth" in schema:
            depth_values = (
                lazy_frame.select(pl.col("thermistor_depth").unique())
                .collect()
                .get_column("thermistor_depth")
                .to_list()
            )

        def depth_sort_key(value: Any) -> tuple[int, str]:
            if value is None:
                return (0, "")
            return (1, str(value))

        depth_values_sorted = sorted(depth_values, key=depth_sort_key)
        thermistor_options = [
            {
                "label": "Unknown depth" if value is None else str(value),
                "value": value,
            }
            for value in depth_values_sorted
        ]

        available_stem_cols = [col for col in STEM_METADATA_COLUMNS if col in schema]
        stem_lookup: dict[str, dict[str, Any]] = {}
        stem_order: list[str] = []
        stem_options: list[dict[str, Any]] = []

        if available_stem_cols:
            stem_records = (
                lazy_frame.select([pl.col(col) for col in available_stem_cols])
                .unique()
                .collect()
                .to_dicts()
            )

            def stem_sort_key(record: dict[str, Any]) -> tuple[str, ...]:
                project_label = clean(record.get("project_name") or record.get("project_code"), "Project ?")
                site_label = clean(record.get("site_name"), "Site ?")
                plant_label = clean(record.get("plant_code"), "Plant ?")
                stem_label = clean(record.get("stem_code"), "Stem ?")
                deployment_label = clean(record.get("deployment_id"), "Deployment ?")
                return (
                    project_label.lower(),
                    site_label.lower(),
                    plant_label.lower(),
                    stem_label.lower(),
                    deployment_label.lower(),
                )

            for record in sorted(stem_records, key=stem_sort_key):
                payload = {col: record.get(col) for col in available_stem_cols}
                value = json.dumps(payload, sort_keys=True, default=str)

                project_name = clean(record.get("project_name"), "Project ?")
                project_code = clean(record.get("project_code"), "")
                project_label = project_name
                if project_code and project_code.lower() not in {project_name.lower(), "project ?"}:
                    project_label = f"{project_name} ({project_code})"

                site_label = clean(record.get("site_name"), "Site ?")
                zone_label = clean(record.get("zone_name"), "?")
                plot_label = clean(record.get("plot_name"), "?")
                sdi_label = clean(record.get("sdi12_address"), "?")
                plant_label = clean(record.get("plant_code"), "?")
                species_label = clean(record.get("species_scientific_name"), "Species ?")
                stem_label = clean(record.get("stem_code"), "?")
                deployment_id = clean(record.get("deployment_id"), "?")
                deployment_prefix = deployment_id[:6] if deployment_id != "?" else "?"

                lines = [
                    f"{project_label} | {species_label}",
                    f"Site {site_label} | Zone {zone_label} | Plot {plot_label}",
                    f"SDI {sdi_label} | Plant {plant_label} | Stem {stem_label} | Dep {deployment_prefix}",
                ]

                stem_lookup[value] = payload
                stem_order.append(value)
                stem_options.append({"label": " | ".join(lines), "value": value, "lines": lines})

        metadata = {
            "path": str(data_path),
            "filename": data_path.name,
            "last_modified": datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat(),
            "record_count": int(total_rows),
            "stem_options": stem_options,
            "stem_order": stem_order,
            "default_stem": stem_order[0] if stem_order else None,
            "thermistor_options": thermistor_options,
        }
        return metadata, stem_lookup, stem_order

    def _compute_filtered_dataframe(
        self,
        *,
        stem_value: str | None,
        thermistors: Sequence[Any],
        include_suspect: bool,
        time_range: tuple[datetime | None, datetime | None] | None,
    ) -> FilterResult:
        assert self._lazy_frame is not None

        lazy_frame = self._lazy_frame
        filters: list[pl.Expr] = []

        if stem_value:
            stem_criteria = self._stem_lookup.get(stem_value)
            if stem_criteria:
                for column in ("deployment_id", "plant_code", "stem_code", "sdi12_address"):
                    value = stem_criteria.get(column)
                    if value is not None:
                        filters.append(pl.col(column) == value)

        if thermistors:
            selected = [value for value in thermistors if value is not None]
            include_null = any(value is None for value in thermistors)
            depth_expr: pl.Expr | None = None
            if selected:
                depth_expr = pl.col("thermistor_depth").is_in(selected)
            if include_null:
                null_expr = pl.col("thermistor_depth").is_null()
                depth_expr = null_expr if depth_expr is None else (depth_expr | null_expr)
            if depth_expr is not None:
                filters.append(depth_expr)

        if time_range:
            start, end = time_range
            if start is not None:
                filters.append(pl.col("timestamp") >= pl.lit(start))
            if end is not None:
                filters.append(pl.col("timestamp") <= pl.lit(end))

        if not include_suspect and "quality" in lazy_frame.schema:
            filters.append(pl.col("quality").is_null())

        filtered = lazy_frame
        for expr in filters:
            filtered = filtered.filter(expr)

        stats_frame = filtered.select(
            [
                pl.len().alias("total_rows"),
                pl.col("quality").is_not_null().sum().alias("suspect_rows")
                if "quality" in filtered.schema
                else pl.lit(0).alias("suspect_rows"),
                pl.col("timestamp").min().alias("min_timestamp")
                if "timestamp" in filtered.schema
                else pl.lit(None).alias("min_timestamp"),
                pl.col("timestamp").max().alias("max_timestamp")
                if "timestamp" in filtered.schema
                else pl.lit(None).alias("max_timestamp"),
            ]
        )
        stats_row = stats_frame.collect().row(0)
        total_rows = int(stats_row[0])
        suspect_rows = int(stats_row[1] or 0)
        min_ts = stats_row[2]
        max_ts = stats_row[3]

        if total_rows == 0:
            empty_df = pd.DataFrame(
                columns=[
                    "timestamp",
                    "sap_flux_density_j_dma_cm_hr",
                    "quality",
                    "plant_code",
                    "stem_code",
                    "species_scientific_name",
                    "thermistor_depth",
                    "deployment_id",
                ]
            )
            return FilterResult(
                dataframe=empty_df,
                total_rows=0,
                displayed_rows=0,
                suspect_rows=suspect_rows,
                time_min=None,
                time_max=None,
            )

        trimmed = filtered
        if total_rows > self._max_points:
            stride = int(math.ceil(total_rows / self._max_points))
            trimmed = (
                trimmed.with_row_count("row_nr")
                .filter(pl.col("row_nr") % stride == 0)
                .drop("row_nr")
            )

        columns = [
            col
            for col in [
                "timestamp",
                "sap_flux_density_j_dma_cm_hr",
                "quality",
                "plant_code",
                "stem_code",
                "species_scientific_name",
                "thermistor_depth",
                "deployment_id",
                "file_set_signature" if "file_set_signature" in filtered.schema else None,
            ]
            if col is not None and col in filtered.schema
        ]

        collected = trimmed.select(columns).collect()
        pandas_df = collected.to_pandas(use_pyarrow_extension_array=False)
        pandas_df = pandas_df.sort_values("timestamp").reset_index(drop=True)

        displayed_rows = len(pandas_df)
        min_dt = _to_datetime(min_ts)
        max_dt = _to_datetime(max_ts)

        return FilterResult(
            dataframe=pandas_df,
            total_rows=total_rows,
            displayed_rows=displayed_rows,
            suspect_rows=suspect_rows,
            time_min=min_dt,
            time_max=max_dt,
        )


def _to_datetime(value: Any) -> datetime | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _parse_time_range(
    payload: dict[str, Any] | None,
) -> tuple[datetime | None, datetime | None] | None:
    if not payload:
        return None
    start = _to_datetime(payload.get("start"))
    end = _to_datetime(payload.get("end"))
    if start and end and start > end:
        start, end = end, start
    if start is None and end is None:
        return None
    return start, end


def render_stem_metadata(metadata: dict[str, Any] | None) -> html.Div:
    if not metadata:
        return html.Div(
            "Select a stem to view metadata.",
            className="stem-metadata empty",
            style={"fontStyle": "italic", "color": "#666666"},
        )

    rows: list[html.Div] = []
    for key, label in STEM_DISPLAY_FIELDS:
        value = metadata.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            display_value = "—"
        else:
            display_value = str(value)
        rows.append(
            html.Div(
                [
                    html.Span(f"{label}:", style={"fontWeight": "600", "marginRight": "6px"}),
                    html.Span(display_value),
                ],
                style={"display": "flex", "gap": "4px"},
            )
        )

    return html.Div(
        rows,
        className="stem-metadata card",
        style={
            "display": "grid",
            "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
            "gap": "8px 16px",
            "padding": "12px 14px",
            "border": "1px solid #d9d9d9",
            "borderRadius": "6px",
            "backgroundColor": "#fafafa",
        },
    )


def build_layout(cache: DataCache) -> html.Div:
    metadata = cache.metadata_payload()
    raw_stem_options = metadata.get("stem_options", [])
    stem_options = [_format_stem_option(option) for option in raw_stem_options]
    default_stem = metadata.get("default_stem")
    thermistor_options = metadata.get("thermistor_options", [])
    return html.Div(
        className="app-container",
        children=[
            EventListener(
                id="key-listener",
                events=[{"event": "keydown", "props": ["key"], "target": "document"}],
            ),
            dcc.Store(id="metadata-store", data=metadata),
            dcc.Store(id="stem-order-store", data=metadata.get("stem_order", [])),
            dcc.Store(id="time-range-store", data=None),
            dcc.Interval(id="refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
            html.H1(APP_TITLE),
            html.Div(id="dataset-summary", children=render_dataset_summary(metadata)),
            html.Div(
                className="controls",
                style={
                    "display": "flex",
                    "flexWrap": "wrap",
                    "gap": "12px",
                    "marginBottom": "16px",
                },
                children=[
                    html.Div(
                        className="stem-block",
                        style={"minWidth": "360px", "flex": "1 1 360px"},
                        children=[
                            html.Label("Stem selection"),
                            dcc.Dropdown(
                                id="stem-selector",
                                options=stem_options,
                                value=default_stem,
                                placeholder="Select a stem / deployment",
                                clearable=False,
                                optionHeight=72,
                            ),
                        ],
                    ),
                    html.Div(
                        className="thermistor-block",
                        style={"minWidth": "220px", "flex": "1 1 220px"},
                        children=[
                            html.Label("Thermistor depth filter"),
                            dcc.Dropdown(
                                id="thermistor-filter",
                                options=thermistor_options,
                                value=[],
                                multi=True,
                                placeholder="All depths",
                            ),
                        ],
                    ),
                    html.Div(
                        className="toggle-block",
                        style={"minWidth": "220px", "flex": "1 1 220px"},
                        children=[
                            html.Label("Display options"),
                            dcc.Checklist(
                                id="depth-toggle",
                                options=[{"label": "Color by thermistor depth", "value": "depth"}],
                                value=["depth"],
                                inputStyle={"marginRight": "6px"},
                            ),
                            dcc.Checklist(
                                id="suspect-toggle",
                                options=[{"label": "Include suspect points", "value": "include"}],
                                value=[],
                                inputStyle={"marginRight": "6px"},
                            ),
                            dcc.Checklist(
                                id="suspect-highlight-toggle",
                                options=[{"label": "Highlight suspect points", "value": "highlight"}],
                                value=[],
                                inputStyle={"marginRight": "6px"},
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(
                id="selected-stem-metadata",
                style={"marginBottom": "16px"},
            ),
            dcc.Graph(
                id="sapflux-graph",
                config={
                    "displaylogo": False,
                    "modeBarButtonsToRemove": ["select2d", "lasso2d"],
                    "scrollZoom": True,
                },
                style={"height": "70vh"},
            ),
            html.Div(id="stats-panel", className="stats-panel"),
        ],
    )


def render_dataset_summary(metadata: dict[str, Any] | None) -> html.Div:
    if not metadata:
        return html.Div("Dataset not loaded", className="dataset-summary")

    last_modified = metadata.get("last_modified", "unknown")
    record_count = metadata.get("record_count")
    record_text = f"{record_count:,}" if isinstance(record_count, int) else "?"

    return html.Div(
        className="dataset-summary",
        style={
            "display": "flex",
            "flexWrap": "wrap",
            "gap": "16px",
            "marginBottom": "12px",
            "alignItems": "baseline",
        },
        children=[
            html.Strong(metadata.get("filename", "unknown.parquet")),
            html.Span(f"Rows: {record_text}"),
            html.Span(f"Last modified (UTC): {last_modified}"),
            html.Span(metadata.get("path", "")),
        ],
    )


def build_figure(df: pd.DataFrame, color_by_depth: bool, highlight_suspect: bool) -> go.Figure:
    figure = go.Figure()

    if df.empty:
        figure.update_layout(
            template="plotly_white",
            xaxis_title="Timestamp",
            yaxis_title="Sap flux density (J dma^-1 cm^-1 hr^-1)",
            annotations=[
                dict(
                    text="No data available for the current filters.",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                )
            ],
        )
        return figure

    df = df.copy()
    df["quality_label"] = df["quality"].fillna("OK")
    df["is_suspect"] = df["quality"].notna()
    df["thermistor_depth"] = df.get("thermistor_depth")

    hover_template = (
        "Time: %{x|%Y-%m-%d %H:%M}<br>"
        "Sap flux: %{y:.3f}<br>"
        "Quality: %{customdata[0]}<br>"
        "Plant: %{customdata[1]}<br>"
        "Stem: %{customdata[2]}<br>"
        "Species: %{customdata[3]}<br>"
        "Depth: %{customdata[4]}<br>"
        "<extra></extra>"
    )

    if highlight_suspect:
        base_df = df[~df["is_suspect"]]
        suspect_df = df[df["is_suspect"]]
    else:
        base_df = df
        suspect_df = df.iloc[0:0]

    if not base_df.empty:
        depth_series = base_df.get("thermistor_depth")
        if color_by_depth and depth_series is not None and depth_series.notna().any():
            palette = px.colors.qualitative.Dark24
            normalized_depths = depth_series.fillna("Unknown").astype(str)
            depth_values = sorted(normalized_depths.unique().tolist())
            for idx, depth in enumerate(depth_values):
                subset = base_df[normalized_depths == depth]
                if subset.empty:
                    continue
                figure.add_trace(
                    go.Scattergl(
                        x=subset["timestamp"],
                        y=subset["sap_flux_density_j_dma_cm_hr"],
                        mode="markers",
                        name=f"Depth: {depth}",
                        marker=dict(
                            size=6,
                            color=palette[idx % len(palette)],
                            opacity=0.85,
                            line=dict(width=0),
                        ),
                        customdata=_build_custom_data(subset),
                        hovertemplate=hover_template,
                    )
                )
        else:
            figure.add_trace(
                go.Scattergl(
                    x=base_df["timestamp"],
                    y=base_df["sap_flux_density_j_dma_cm_hr"],
                    mode="markers",
                    name="Measurements",
                    marker=dict(size=6, color="#1f77b4", opacity=0.85, line=dict(width=0)),
                    customdata=_build_custom_data(base_df),
                    hovertemplate=hover_template,
                )
            )

    if not suspect_df.empty:
        figure.add_trace(
            go.Scattergl(
                x=suspect_df["timestamp"],
                y=suspect_df["sap_flux_density_j_dma_cm_hr"],
                mode="markers",
                name="Suspect",
                marker=dict(
                    size=7,
                    color="#ff7f0e",
                    opacity=0.95,
                    line=dict(width=1.0, color="#b35700"),
                ),
                customdata=_build_custom_data(suspect_df),
                hovertemplate=hover_template,
            )
        )

    figure.update_layout(
        template="plotly_white",
        xaxis_title="Timestamp",
        yaxis_title="Sap flux density (J dma^-1 cm^-1 hr^-1)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=30, t=30, b=60),
        dragmode="zoom",
    )
    figure.update_xaxes(tickformat="%Y-%m-%d\n%H:%M", showgrid=True)
    figure.update_yaxes(showgrid=True)

    return figure


def _build_custom_data(df: pd.DataFrame) -> list[list[Any]]:
    cols = [
        df.get("quality_label"),
        df.get("plant_code"),
        df.get("stem_code"),
        df.get("species_scientific_name"),
        df.get("thermistor_depth"),
    ]
    rows: list[list[Any]] = []
    for idx in range(len(df)):
        row = []
        for series in cols:
            value = None if series is None else series.iloc[idx]
            if value is None or pd.isna(value):
                row.append("")
            else:
                row.append(str(value))
        rows.append(row)
    return rows


def render_stats(result: FilterResult) -> html.Div:
    if result.total_rows == 0:
        return html.Div("No rows match the current filters.", className="stats")

    total_text = f"{result.displayed_rows:,} shown / {result.total_rows:,} total"
    suspect_text = f"{result.suspect_rows:,} suspect"
    range_parts = []
    if result.time_min:
        range_parts.append(result.time_min.strftime("%Y-%m-%d %H:%M"))
    if result.time_max:
        range_parts.append(result.time_max.strftime("%Y-%m-%d %H:%M"))
    range_text = " -> ".join(range_parts) if range_parts else "N/A"

    details = [
        html.Span(f"Rows: {total_text}"),
        html.Span(f"Suspect: {suspect_text}"),
        html.Span(f"Range: {range_text}"),
    ]
    if result.downsampled and result.displayed_rows:
        stride = math.ceil(result.total_rows / max(result.displayed_rows, 1))
        details.append(html.Span(f"Downsample stride ~= {stride}"))

    return html.Div(
        className="stats",
        style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginTop": "12px"},
        children=details,
    )


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sapflux quality dashboard")
    parser.add_argument(
        "--parquet",
        type=Path,
        help="Path to a specific parquet file. Defaults to the newest file in integration_tests/rileydata/output.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory to scan for parquet files (default: {DEFAULT_DATA_DIR}).",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=DEFAULT_MAX_POINTS,
        help="Maximum number of points to render in the scatter plot (default: 120000).",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host interface for Dash (default: 127.0.0.1). Use 0.0.0.0 for remote access.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8050,
        help="Port for Dash server (default: 8050).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable Dash debug mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_cli_args()

    cache = DataCache(
        explicit_path=args.parquet,
        data_dir=args.data_dir,
        max_points=args.max_points,
    )

    app = Dash(__name__, title=APP_TITLE)
    app.layout = lambda: build_layout(cache)

    @app.callback(
        Output("metadata-store", "data"),
        Output("dataset-summary", "children"),
        Input("refresh-interval", "n_intervals"),
        prevent_initial_call=False,
    )
    def refresh_metadata(n_intervals: int) -> tuple[dict[str, Any], html.Div]:
        metadata = cache.refresh()
        return metadata, render_dataset_summary(metadata)

    @app.callback(
        Output("stem-selector", "options"),
        Output("stem-selector", "value"),
        Output("stem-order-store", "data"),
        Output("thermistor-filter", "options"),
        Output("thermistor-filter", "value"),
        Input("metadata-store", "data"),
        Input("key-listener", "event"),
        State("stem-selector", "value"),
        State("stem-order-store", "data"),
        State("thermistor-filter", "value"),
        prevent_initial_call=False,
    )
    def synchronize_controls(
        metadata: dict[str, Any] | None,
        key_event: dict[str, Any] | None,
        current_stem: str | None,
        stem_order: list[str] | None,
        current_depths: list[Any] | None,
    ) -> tuple[Any, Any, Any, Any, Any]:
        trigger_id = ctx.triggered_id
        if trigger_id == "key-listener":
            if not key_event or not stem_order:
                return (
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            key = key_event.get("key")
            if key not in {"ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"}:
                return (
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            if not stem_order:
                return (
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            active_value = current_stem if current_stem in stem_order else (stem_order[0] if stem_order else None)
            if active_value is None:
                return (
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            index = stem_order.index(active_value)
            delta = -1 if key in {"ArrowLeft", "ArrowUp"} else 1
            new_index = (index + delta) % len(stem_order)
            new_value = stem_order[new_index]
            return (
                no_update,
                new_value,
                no_update,
                no_update,
                no_update,
            )

        raw_stem_options = list(metadata.get("stem_options", [])) if metadata else []
        stem_options = [_format_stem_option(option) for option in raw_stem_options]
        stem_order_new = list(metadata.get("stem_order", [])) if metadata else []
        default_stem = metadata.get("default_stem") if metadata else None
        therm_options = list(metadata.get("thermistor_options", [])) if metadata else []

        sanitized_value = current_stem if current_stem in stem_order_new else None
        if sanitized_value is None and stem_order_new:
            sanitized_value = default_stem or stem_order_new[0]

        allowed_depth_values = {option["value"] for option in therm_options}
        sanitized_depths = [
            value for value in (current_depths or []) if value in allowed_depth_values
        ]

        return (
            stem_options,
            sanitized_value,
            stem_order_new,
            therm_options,
            sanitized_depths,
        )

    @app.callback(
        Output("time-range-store", "data"),
        Input("sapflux-graph", "relayoutData"),
        Input("stem-selector", "value"),
        prevent_initial_call=True,
    )
    def update_time_range(
        relayout_data: dict[str, Any] | None,
        stem_value: str | None,
    ) -> dict[str, Any] | None:
        trigger_id = ctx.triggered_id
        if trigger_id == "stem-selector":
            return None
        if not relayout_data:
            raise PreventUpdate
        if relayout_data.get("xaxis.autorange"):
            return None

        start = relayout_data.get("xaxis.range[0]")
        end = relayout_data.get("xaxis.range[1]")

        if start is None and end is None:
            range_list = relayout_data.get("xaxis.range")
            if isinstance(range_list, list) and len(range_list) == 2:
                start, end = range_list

        if start is None and end is None:
            raise PreventUpdate

        return {"start": start, "end": end}

    @app.callback(
        Output("sapflux-graph", "figure"),
        Output("stats-panel", "children"),
        Output("selected-stem-metadata", "children"),
        Input("metadata-store", "data"),
        Input("stem-selector", "value"),
        Input("thermistor-filter", "value"),
        Input("suspect-toggle", "value"),
        Input("suspect-highlight-toggle", "value"),
        Input("depth-toggle", "value"),
        Input("time-range-store", "data"),
    )
    def update_graph(
        metadata: dict[str, Any] | None,
        stem_value: str | None,
        thermistor_values: list[Any] | None,
        suspect_toggle: list[str] | None,
        highlight_toggle: list[str] | None,
        depth_toggle: list[str] | None,
        time_range_payload: dict[str, Any] | None,
    ) -> tuple[go.Figure, html.Div, html.Div]:
        if not metadata:
            return (
                go.Figure(),
                html.Div("Dataset metadata unavailable.", className="stats"),
                render_stem_metadata(None),
            )

        include_suspect = bool(suspect_toggle and "include" in suspect_toggle)
        highlight_suspect = bool(highlight_toggle and "highlight" in highlight_toggle)
        color_by_depth = bool(depth_toggle and "depth" in depth_toggle)
        if color_by_depth and not metadata.get("thermistor_options"):
            color_by_depth = False

        time_range = _parse_time_range(time_range_payload)

        result = cache.filtered_dataframe(
            stem_value=stem_value,
            thermistors=thermistor_values or [],
            include_suspect=include_suspect,
            time_range=time_range,
        )

        figure = build_figure(
            result.dataframe,
            color_by_depth=color_by_depth,
            highlight_suspect=highlight_suspect,
        )
        stats = render_stats(result)
        metadata_panel = render_stem_metadata(cache.stem_metadata(stem_value))
        return figure, stats, metadata_panel

    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
