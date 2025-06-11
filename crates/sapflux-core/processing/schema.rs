use polars::prelude::*;

/// Define the full schema that includes all columns from the new format
pub fn get_full_schema_columns() -> Vec<Expr> {
    vec![
        col("timestamp_naive").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
        col("record_number").cast(DataType::Int64),
        col("batt_volt").cast(DataType::Float64),
        col("ptemp_c").cast(DataType::Float64),
        col("sdi_address").cast(DataType::String),
        col("alpha_out").cast(DataType::Float64),
        col("alpha_in").cast(DataType::Float64),
        col("beta_out").cast(DataType::Float64),
        col("beta_in").cast(DataType::Float64),
        col("tmax_tout").cast(DataType::Float64),
        col("tmax_tinn").cast(DataType::Float64),
        col("tp_ds_out").cast(DataType::Float64),
        col("dt_ds_out").cast(DataType::Float64),
        col("ts_ds_out").cast(DataType::Float64),
        col("tp_us_out").cast(DataType::Float64),
        col("dt_us_out").cast(DataType::Float64),
        col("ts_us_out").cast(DataType::Float64),
        col("tp_ds_inn").cast(DataType::Float64),
        col("dt_ds_inn").cast(DataType::Float64),
        col("ts_ds_inn").cast(DataType::Float64),
        col("tp_us_inn").cast(DataType::Float64),
        col("dt_us_inn").cast(DataType::Float64),
        col("ts_us_inn").cast(DataType::Float64),
        col("tmax_tus_o").cast(DataType::Float64),
        col("tmax_tus_i").cast(DataType::Float64),
    ]
}