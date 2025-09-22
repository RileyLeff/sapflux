# sapflux-parser

## note from riley (important)

important stuff: 
generic, flexible handling: how they connect to dataformats, pipelines and so on
strictness: reject files that contain any invalid sdi-12 addresses, reject files that contain any non-sequential record numbers
important point: the sap flow all data file doesn't log the logger id in each row the same way that the other parser does. you need to extract the logger id from the header. it should be something like "405" or "302", usually a 3 digit number. The logger id should be added as a column in the logger level data in the parsed data structure. We should reject a data file if we find n>1 unique logger ids in a id column.

## old implementation

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
