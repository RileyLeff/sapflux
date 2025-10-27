# /// script
# requires-python = ">=3.13"
# dependencies = ["polars>=0.20", "pandas>=2.2", "matplotlib>=3.9", "pyarrow>=15.0"]
# ///

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import List

import argparse
from concurrent.futures import ProcessPoolExecutor

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.lines import Line2D
import pandas as pd
import polars as pl


OUTPUT_ROOT = Path("integration_tests/qc_outputs")
PIPELINE_OUTPUT_DIR = Path("integration_tests/rileydata/output")


def find_latest_parquet(directory: Path) -> Path:
    parquet_files = sorted(
        (p for p in directory.glob("*.parquet") if p.is_file()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not parquet_files:
        raise FileNotFoundError(
            "No parquet files found in integration_tests/rileydata/output."
        )
    return parquet_files[0]


def slugify(value: str | None) -> str:
    if not value:
        return "unknown"
    cleaned = value.strip().replace("/", "-").replace(" ", "-")
    return "".join(ch for ch in cleaned if ch.isalnum() or ch in {"-", "_"}).lower() or "unknown"


def deployment_title(metadata: dict[str, str | None]) -> str:
    site = metadata.get("site_name") or "Unknown site"
    zone = metadata.get("zone_name") or "Unknown zone"
    plot = metadata.get("plot_name") or "Unknown plot"
    logger = metadata.get("datalogger_id") or "Logger ?"
    deployment_id = metadata.get("deployment_id") or "?"
    return f"{site} | Zone {zone} | Plot {plot} | {logger} | {deployment_id[:6]}"


def first_non_null(series: pd.Series, fallback: str | None = None) -> str | None:
    if series is None:
        return fallback
    non_null = series.dropna()
    if non_null.empty:
        return fallback
    value = str(non_null.iloc[0]).strip()
    return value if value else fallback


def chunk_boundaries(timestamps: pd.Series, signatures: pd.Series) -> List[pd.Timestamp]:
    if signatures.isna().all():
        return []
    changes = signatures.ne(signatures.shift())
    indices = changes[changes].index.tolist()
    if not indices:
        return []
    # drop the first occurrence so we only mark subsequent chunk starts
    indices = indices[1:]
    return [timestamps.loc[idx] for idx in indices]


def format_panel_title(plant: str | None, stem: str | None, species: str | None) -> str:
    def clean(value: str | None, fallback: str) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return fallback
        text = str(value).strip()
        return text if text else fallback

    plant_label = clean(plant, "?")
    stem_label = clean(stem, "?")
    species_label = clean(species, "Species ?")
    return f"Tree {plant_label} | Stem {stem_label} | {species_label}"


def ensure_output_dir() -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    run_dir = OUTPUT_ROOT / timestamp
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def render_deployment(deployment_df: pl.DataFrame, output_dir: Path, exclude_suspect: bool) -> None:
    pandas_df = deployment_df.sort("timestamp").to_pandas(use_pyarrow_extension_array=False)
    if pandas_df.empty:
        return

    pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"], utc=False)
    pandas_df["quality_flagged"] = pandas_df["quality"].notna()

    if exclude_suspect:
        pandas_df = pandas_df[~pandas_df["quality_flagged"]].reset_index(drop=True)
        if pandas_df.empty:
            return

    if "file_set_signature" not in pandas_df.columns:
        pandas_df["file_set_signature"] = None
    if "plant_code" not in pandas_df.columns:
        pandas_df["plant_code"] = None
    if "stem_code" not in pandas_df.columns:
        pandas_df["stem_code"] = None
    if "species_scientific_name" not in pandas_df.columns:
        if "species_code" in pandas_df.columns:
            pandas_df["species_scientific_name"] = pandas_df["species_code"]
        else:
            pandas_df["species_scientific_name"] = "Species ?"

    metadata = {
        "site_name": first_non_null(pandas_df.get("site_name")),
        "zone_name": first_non_null(pandas_df.get("zone_name")),
        "plot_name": first_non_null(pandas_df.get("plot_name")),
        "datalogger_id": first_non_null(pandas_df.get("datalogger_id"), "Logger ?"),
        "deployment_id": first_non_null(pandas_df.get("deployment_id"), "?"),
    }

    chunk_lines = chunk_boundaries(pandas_df["timestamp"], pandas_df["file_set_signature"])

    panels = pandas_df.groupby(["plant_code", "stem_code", "species_scientific_name"], dropna=False)
    panel_items = list(panels)
    if not panel_items:
        return

    depth_values = pandas_df.get("thermistor_depth")
    depth_colors: dict[str, tuple] = {}
    if depth_values is not None:
        unique_depths = [str(val) for val in depth_values.dropna().unique()]
        cmap = plt.get_cmap("tab10")
        depth_colors = {depth: cmap(idx % 10) for idx, depth in enumerate(unique_depths)}

    n_panels = len(panel_items)
    ncols = min(3, n_panels)
    nrows = (n_panels + ncols - 1) // ncols
    figsize = (ncols * 4.0, nrows * 2.4)
    fig, axes = plt.subplots(
        nrows=nrows, ncols=ncols, sharex=True, sharey=True, figsize=figsize, squeeze=False
    )
    axes_flat = list(axes.flatten())

    for idx, (ax, ((plant, stem, species), panel_df)) in enumerate(zip(axes_flat, panel_items)):
        panel_df = panel_df.sort_values("timestamp")
        depth_series = panel_df.get("thermistor_depth")
        if depth_series is not None and depth_colors:
            depth_keys = depth_series.astype(str).fillna("Unknown")
            point_colors = depth_keys.map(depth_colors).fillna("#666666")
        else:
            depth_keys = None
            point_colors = "#333333"

        ax.scatter(
            panel_df["timestamp"],
            panel_df["sap_flux_density_j_dma_cm_hr"],
            c=point_colors,
            s=1.8,
            alpha=0.8,
        )

        if not exclude_suspect:
            flagged_mask = panel_df["quality_flagged"].fillna(False)
            if flagged_mask.any():
                ax.scatter(
                    panel_df.loc[flagged_mask, "timestamp"],
                    panel_df.loc[flagged_mask, "sap_flux_density_j_dma_cm_hr"],
                    facecolors="none",
                    edgecolors="orange",
                    s=6,
                    linewidths=0.5,
                )

        if depth_keys is not None and depth_colors and idx == 0:
            legend_handles = [
                Line2D([0], [0], marker="o", color="none", markerfacecolor=color, markersize=3)
                for color in depth_colors.values()
            ]
            legend_labels = list(depth_colors.keys())
            ax.legend(
                legend_handles,
                legend_labels,
                title="Depth",
                fontsize=3.5,
                title_fontsize=4,
                loc="upper right",
                frameon=False,
            )

        for boundary in chunk_lines:
            ax.axvline(boundary, color="gray", linestyle="--", linewidth=0.5, alpha=0.4)
        ax.set_title(format_panel_title(plant, stem, species), fontsize=4, pad=1)

    total_axes = len(axes_flat)
    for idx in range(n_panels, total_axes):
        axes_flat[idx].axis("off")

    for ax in axes_flat[:n_panels]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M"))
        ax.tick_params(axis="x", rotation=90, labelsize=3, pad=1)
        ax.tick_params(axis="y", labelsize=3)

    fig.suptitle(deployment_title(metadata), fontsize=6, y=0.98)
    fig.text(
        0.965,
        0.5,
        "sap_flux_density_j_dma_cm_hr",
        va="center",
        rotation=-90,
        fontsize=4,
    )
    fig.subplots_adjust(left=0.06, right=0.92, top=0.92, bottom=0.18, hspace=0.28, wspace=0.2)

    site_slug = slugify(metadata.get("site_name"))
    zone_slug = slugify(metadata.get("zone_name"))
    logger_slug = slugify(metadata.get("datalogger_id"))
    start_ts = pandas_df["timestamp"].min()
    start_str = start_ts.strftime("%Y%m%dT%H%M%S")
    deployment_suffix = metadata["deployment_id"][0:6]
    filename = f"{site_slug}_{zone_slug}_{logger_slug}_{start_str}_{deployment_suffix}.png"
    output_path = output_dir / filename
    fig.savefig(output_path, dpi=600)
    plt.close(fig)
    print(f"Saved {output_path}")


def create_plots(df: pl.DataFrame, output_dir: Path, exclude_suspect: bool) -> None:
    if df.is_empty():
        print("No rows available in parquet; nothing to plot.")
        return

    plt.rcParams.update(
        {
            "axes.titlesize": 4,
            "axes.labelsize": 5,
            "xtick.labelsize": 4,
            "ytick.labelsize": 4,
            "figure.titlesize": 6,
        }
    )

    deployments = df.partition_by("deployment_id", maintain_order=True)

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(render_deployment, deployment_df, output_dir, exclude_suspect)
            for deployment_df in deployments
        ]
        for future in futures:
            future.result()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sap flux QC plots")
    parser.add_argument(
        "--exclude-suspect",
        action="store_true",
        help="Exclude points flagged by quality filters",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        help="Optional path to a specific parquet file (defaults to latest smoke output)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    parquet_path = args.parquet or find_latest_parquet(PIPELINE_OUTPUT_DIR)
    print(f"Loading {parquet_path}")
    df = pl.read_parquet(parquet_path)
    output_dir = ensure_output_dir()
    create_plots(df, output_dir, exclude_suspect=args.exclude_suspect)
    print(f"QC plots written to {output_dir}")


if __name__ == "__main__":
    main()
