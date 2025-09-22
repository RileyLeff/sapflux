use anyhow::Result;
use sqlx::postgres::PgQueryResult;
use tracing::info;
use uuid::Uuid;

use crate::data_formats::all_data_formats;
use crate::db::DbPool;
use crate::parsers::all_parser_descriptors;
use crate::pipelines::all_pipeline_descriptors;

pub async fn run(pool: &DbPool) -> Result<()> {
    seed_data_formats(pool).await?;
    seed_parsers(pool).await?;
    seed_pipelines(pool).await?;
    seed_parameters(pool).await?;
    Ok(())
}

async fn seed_data_formats(pool: &DbPool) -> Result<()> {
    for descriptor in all_data_formats() {
        let result: PgQueryResult = sqlx::query::<sqlx::Postgres>(
            r#"
            INSERT INTO data_formats (data_format_id, code_identifier, schema_definition)
            VALUES ($1, $2, $3)
            ON CONFLICT (code_identifier)
            DO UPDATE SET schema_definition = EXCLUDED.schema_definition
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(descriptor.code)
        .bind(descriptor.schema_json)
        .execute(pool)
        .await?;

        if result.rows_affected() > 0 {
            info!(code = descriptor.code, "Seeded data format");
        }
    }
    Ok(())
}

async fn seed_parsers(pool: &DbPool) -> Result<()> {
    for descriptor in all_parser_descriptors() {
        let result: PgQueryResult = sqlx::query::<sqlx::Postgres>(
            r#"
            INSERT INTO parsers (parser_id, code_identifier, version, output_data_format_id, include_in_pipeline)
            SELECT $1, $2, $3, data_format_id, $4
            FROM data_formats
            WHERE code_identifier = $5
            ON CONFLICT (code_identifier)
            DO UPDATE SET
                version = EXCLUDED.version,
                output_data_format_id = EXCLUDED.output_data_format_id,
                include_in_pipeline = EXCLUDED.include_in_pipeline
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(descriptor.code)
        .bind(descriptor.version)
        .bind(descriptor.include_in_pipeline)
        .bind(descriptor.output_format)
        .execute(pool)
        .await?;

        if result.rows_affected() > 0 {
            info!(code = descriptor.code, "Seeded parser");
        }
    }
    Ok(())
}

async fn seed_pipelines(pool: &DbPool) -> Result<()> {
    for descriptor in all_pipeline_descriptors() {
        let result: PgQueryResult = sqlx::query::<sqlx::Postgres>(
            r#"
            INSERT INTO processing_pipelines (pipeline_id, code_identifier, version, input_data_format_id, include_in_pipeline)
            SELECT $1, $2, $3, data_format_id, $4
            FROM data_formats
            WHERE code_identifier = $5
            ON CONFLICT (code_identifier)
            DO UPDATE SET
                version = EXCLUDED.version,
                input_data_format_id = EXCLUDED.input_data_format_id,
                include_in_pipeline = EXCLUDED.include_in_pipeline
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(descriptor.code)
        .bind(descriptor.version)
        .bind(descriptor.include_in_pipeline)
        .bind(descriptor.input_data_format)
        .execute(pool)
        .await?;

        if result.rows_affected() > 0 {
            info!(code = descriptor.code, "Seeded processing pipeline");
        }
    }
    Ok(())
}

#[derive(Debug)]
struct ParameterSeed {
    code: &'static str,
    description: &'static str,
    unit: Option<&'static str>,
}

static PARAMETER_SEEDS: &[ParameterSeed] = &[
    ParameterSeed {
        code: "parameter_thermal_diffusivity_k_cm2_s",
        description: "Bulk thermal diffusivity of the sapwood matrix",
        unit: Some("cm^2 s^-1"),
    },
    ParameterSeed {
        code: "parameter_probe_distance_downstream_cm",
        description: "Distance from heater to downstream thermistor centreline",
        unit: Some("cm"),
    },
    ParameterSeed {
        code: "parameter_probe_distance_upstream_cm",
        description: "Distance from heater to upstream thermistor centreline",
        unit: Some("cm"),
    },
    ParameterSeed {
        code: "parameter_heat_pulse_duration_s",
        description: "Heat pulse duration",
        unit: Some("s"),
    },
    ParameterSeed {
        code: "parameter_wound_correction_a",
        description: "Wound correction coefficient a",
        unit: None,
    },
    ParameterSeed {
        code: "parameter_wound_correction_b",
        description: "Wound correction coefficient b",
        unit: None,
    },
    ParameterSeed {
        code: "parameter_wound_correction_c",
        description: "Wound correction coefficient c",
        unit: None,
    },
    ParameterSeed {
        code: "parameter_wood_density_kg_m3",
        description: "Dry wood density",
        unit: Some("kg m^-3"),
    },
    ParameterSeed {
        code: "parameter_wood_specific_heat_j_kg_c",
        description: "Wood specific heat capacity",
        unit: Some("J kg^-1 °C^-1"),
    },
    ParameterSeed {
        code: "parameter_water_content_g_g",
        description: "Gravimetric water content",
        unit: Some("g g^-1"),
    },
    ParameterSeed {
        code: "parameter_water_specific_heat_j_kg_c",
        description: "Sap specific heat capacity",
        unit: Some("J kg^-1 °C^-1"),
    },
    ParameterSeed {
        code: "parameter_water_density_kg_m3",
        description: "Water density",
        unit: Some("kg m^-3"),
    },
    ParameterSeed {
        code: "quality_max_flux_cm_hr",
        description: "Maximum acceptable sap flux density",
        unit: Some("cm hr^-1"),
    },
    ParameterSeed {
        code: "quality_min_flux_cm_hr",
        description: "Minimum acceptable sap flux density",
        unit: Some("cm hr^-1"),
    },
    ParameterSeed {
        code: "quality_gap_years",
        description: "Maximum allowable record gap",
        unit: Some("years"),
    },
    ParameterSeed {
        code: "quality_deployment_start_grace_minutes",
        description: "Deployment start grace period",
        unit: Some("minutes"),
    },
    ParameterSeed {
        code: "quality_deployment_end_grace_minutes",
        description: "Deployment end grace period",
        unit: Some("minutes"),
    },
    ParameterSeed {
        code: "quality_future_lead_minutes",
        description: "Future timestamp lead window",
        unit: Some("minutes"),
    },
];

async fn seed_parameters(pool: &DbPool) -> Result<()> {
    for param in PARAMETER_SEEDS {
        let result: PgQueryResult = sqlx::query::<sqlx::Postgres>(
            r#"
            INSERT INTO parameters (parameter_id, code, description, unit)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (code)
            DO UPDATE SET
                description = EXCLUDED.description,
                unit = EXCLUDED.unit
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(param.code)
        .bind(param.description)
        .bind(param.unit)
        .execute(pool)
        .await?;

        if result.rows_affected() > 0 {
            info!(code = param.code, "Seeded parameter");
        }
    }
    Ok(())
}
