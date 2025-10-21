mod common;
mod cr200_table;
mod cr300_legacy;
mod cr300_table;
mod sapflow_all;
pub(crate) mod schema;

pub use cr200_table::Cr200TableParser;
pub use cr300_legacy::Cr300LegacyParser;
pub use cr300_table::Cr300TableParser;
pub use sapflow_all::SapFlowAllParser;

pub(crate) use common::{
    build_logger_dataframe, derive_logger_id_from_header, make_logger_data, parse_metadata,
    parse_optional_f64, parse_optional_i64, parse_required_i64, parse_sdi12_address,
    parse_timestamp, ColumnRole, LoggerColumnKind, LoggerColumns, SensorFrameBuilder, SensorMetric,
    ThermistorMetric,
};
