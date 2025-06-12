// crates/sapflux-core/src/processing/correction.rs

use super::schema::get_full_schema_columns;
use super::types::{DeploymentInfo, DstTransition};
use crate::error::{PipelineError, Result};
use polars::prelude::*;
use sqlx::PgPool;

pub async fn apply_dst_correction_and_map_deployments(
    unified_lf: LazyFrame,
    pool: &PgPool,
) -> Result<LazyFrame> {
    println!("   -> Step 2: Applying DST correction and mapping deployments...");

    let dst_rules = fetch_dst_rules(pool).await?;
    let deployments = fetch_deployments(pool).await?;

    let chunk_def_lf = unified_lf
        .clone()
        .group_by([col("logger_id"), col("timestamp_naive")])
        .agg([col("file_hash").list().sort(Default::default()).alias("file_hashes")])
        .group_by(["file_hashes"])
        .agg([])
        .with_row_index("chunk_id", Some(1));

    let chunked_lf = unified_lf.join(
        chunk_def_lf,
        &[col("logger_id"), col("timestamp_naive")],
        &[col("logger_id"), col("timestamp_naive")],
        JoinArgs::new(JoinType::Inner),
    );

    let chunk_offsets_lf = chunked_lf
        .clone()
        .group_by(["chunk_id"])
        .agg([col("timestamp_naive").min().alias("chunk_start_time")])
        // FIX: The method is called `join_asof` in this version of Polars.
        .join_asof(
            dst_rules.lazy(),
            col("chunk_start_time"),
            col("ts_local"),
            AsofStrategy::Backward,
            None,
        )?
        .with_column(
            when(col("transition_action").eq(lit("start")))
                .then(lit("-04:00"))
                .otherwise(lit("-05:00"))
                .alias("utc_offset"),
        )
        .select([col("chunk_id"), col("utc_offset")]);

    let utc_corrected_lf = chunked_lf
        .join(
            chunk_offsets_lf,
            &[col("chunk_id")],
            &[col("chunk_id")],
            JoinArgs::new(JoinType::Inner),
        )
        .with_column(
            (col("timestamp_naive").dt().strftime("%Y-%m-%d %H:%M:%S") + col("utc_offset"))
                .str()
                .to_datetime(Some(TimeUnit::Milliseconds), None, StrptimeOptions::default(), lit("raise"))
                .alias("timestamp_utc"),
        );

    let mapped_lf = utc_corrected_lf
        .clone()
        .join(
            deployments.lazy(),
            &[col("logger_id"), col("sdi_address")],
            &[col("datalogger_id"), col("sdi_address_right")],
            JoinArgs::new(JoinType::Inner),
        )
        .filter(
            col("timestamp_utc").gt_eq(col("start_time_utc"))
            .and(col("timestamp_utc").lt(col("end_time_utc")).or(col("end_time_utc").is_null())),
        );

    let final_lf = mapped_lf
        .select(get_full_schema_columns())
        .sort(["timestamp_utc"], SortMultipleOptions::default());
        
    // FIX: Use a standard join with JoinType::Anti for multiple columns
    let unmapped = utc_corrected_lf
        .join(
            final_lf.clone(),
            vec![col("logger_id"), col("sdi_address"), col("timestamp_utc")],
            vec![col("logger_id"), col("sdi_address"), col("timestamp_utc")],
            JoinArgs::new(JoinType::Anti)
        );

    let unmapped_count = unmapped.collect()?.height();

    if unmapped_count > 0 {
        return Err(PipelineError::Processing(format!("{} observations could not be mapped to a deployment.", unmapped_count)));
    }

    println!("   -> Step 2 Complete: All data time-corrected and mapped.");
    Ok(final_lf)
}

async fn fetch_dst_rules(pool: &PgPool) -> Result<DataFrame> {
    let rules = sqlx::query_as!(DstTransition, "SELECT transition_action, ts_local FROM dst_transitions ORDER BY ts_local ASC").fetch_all(pool).await?;
    DataFrame::new(vec![
        // FIX: Collect owned Strings, not references
        Series::new("transition_action".into(), rules.iter().map(|r| r.transition_action.clone()).collect::<Vec<_>>()).into(),
        Series::new("ts_local".into(), rules.iter().map(|r| r.ts_local).collect::<Vec<_>>()).into(),
    ]).map_err(PipelineError::from)
}

async fn fetch_deployments(pool: &PgPool) -> Result<DataFrame> {
    let deployments = sqlx::query_as!(DeploymentInfo, r#"SELECT d.datalogger_id, d.sdi_address, p.name as "project_name!", d.tree_id, s.sensor_id, d.start_time_utc, d.end_time_utc FROM deployments d JOIN projects p ON d.project_id = p.id JOIN sensors s ON d.sensor_id = s.id"#).fetch_all(pool).await?;

    // FIX: The correct and idiomatic way to create DateTime Series from chrono::DateTime<Utc>
    let start_times: Series = deployments
        .iter()
        .map(|d| d.start_time_utc)
        .collect::<Series>()
        .with_name("start_time_utc".into());

    let end_times: Series = deployments
        .iter()
        .map(|d| d.end_time_utc)
        .collect::<Series>()
        .with_name("end_time_utc".into());

    DataFrame::new(vec![
        Series::new("datalogger_id".into(), deployments.iter().map(|d| d.datalogger_id).collect::<Vec<_>>()).into(),
        Series::new("sdi_address_right".into(), deployments.iter().map(|d| d.sdi_address.clone()).collect::<Vec<_>>()).into(),
        Series::new("project_name".into(), deployments.iter().map(|d| d.project_name.clone()).collect::<Vec<_>>()).into(),
        Series::new("tree_id".into(), deployments.iter().map(|d| d.tree_id.clone()).collect::<Vec<_>>()).into(),
        Series::new("sensor_id".into(), deployments.iter().map(|d| d.sensor_id.clone()).collect::<Vec<_>>()).into(),
        start_times.into(),
        end_times.into(),
    ]).map_err(PipelineError::from)
}