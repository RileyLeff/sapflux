# sapflux-parser

Note this is the old implementation, please read the parsers.md file that describes the new and improved, more generic approach.

`sapflux-parser` provides the ingestion layer for Campbell Scientific TOA5 logger exports used in the Gedan Lab sap flux pipeline. The crate detects the supported formats, validates their headers, converts `-99`/`NAN` placeholders into typed nulls, and produces a hierarchical structure of Polars `DataFrame`s that mirrors the logger → sensor → thermistor layout described in `convo.md`.

## Supported Formats

- **CR200/CR300 Table files** – names containing `Table1`, `Table2`, or `Table_S{addr}`.
- **SapFlowAll multi-sensor files** – the newer `SapFlowAll` table exported by updated CR300 firmware.

Each parser is validated with representative fixtures from `rawdata/`, and custom formats can be added by implementing the `SapflowParser` trait and registering it with the orchestrator.

## Parser Output

The top-level API is `parse_sapflow_file(&str) -> ParsedFileData`. A `ParsedFileData` value contains:

- the original `raw_text`
- `FileMetadata` extracted from the TOA5 header
- `LoggerData` with logger-level measurements and nested `SensorData`
- optional sensor-level tables (`SapFlwTot*`), plus per-thermistor `DataFrame`s keyed by `ThermistorDepth`

Serialization helpers are provided for bundling the parsed result into a ZIP archive with a `manifest.json` and individual Parquet files.

```rust
use sapflux_parser::parse_sapflow_file;

let raw = std::fs::read_to_string("rawdata/2025_08_27/CR300Series_420_SapFlowAll.dat")?;
let parsed = parse_sapflow_file(&raw)?;
println!("{} sensors parsed", parsed.logger.sensors.len());
```

## Running the Test Suite

```bash
cargo test -p sapflux-parser
```

The tests exercise both supported formats and ensure the archive round trip preserves structure.
