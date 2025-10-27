# /// script
# requires-python = ">=3.13"
# dependencies = ["pillow>=10.3"]
# ///

from __future__ import annotations

from pathlib import Path

from PIL import Image


OUTPUT_ROOT = Path("integration_tests/qc_outputs")


def find_latest_run(directory: Path) -> Path:
    runs = sorted(
        (p for p in directory.iterdir() if p.is_dir()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not runs:
        raise FileNotFoundError("No QC output folders found. Run scripts/quality.py first.")
    return runs[0]


def load_images(image_paths: list[Path]) -> list[Image.Image]:
    images: list[Image.Image] = []
    for path in image_paths:
        with Image.open(path) as img:
            images.append(img.convert("RGB"))
    return images


def main() -> None:
    latest_run = find_latest_run(OUTPUT_ROOT)
    png_files = sorted(latest_run.glob("*.png"))
    if not png_files:
        raise FileNotFoundError(f"No PNG files found in {latest_run}")

    images = load_images(png_files)
    output_pdf = latest_run / "combined_qc_plots.pdf"

    first, *rest = images
    first.save(output_pdf, save_all=True, append_images=rest)
    print(f"Wrote {output_pdf}")


if __name__ == "__main__":
    main()
