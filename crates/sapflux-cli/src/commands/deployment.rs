// crates/sapflux-cli/src/commands/deployment.rs

use anyhow::Result;
use chrono::{DateTime, Utc};
use sapflux_core::{
    // --- FIX #1: Removed unused `db` import ---
    metadata,
    types::{
        CoastalMonitoringAttributes, DeploymentAttributes, NewDeployment, SdiAddress,
        StemflowAttributes,
    },
};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(clap::Subcommand, Debug)]
pub enum DeploymentCommands {
    /// Create a new deployment, automatically superseding any previous active deployment.
    Create {
        #[arg(long)]
        project_name: String,
        #[arg(long)]
        datalogger_id: i32,
        #[arg(long)]
        sdi_address: String,
        #[arg(long)]
        tree_id: String,
        #[arg(long)]
        sensor_id: String,
        #[arg(long)]
        start_time_utc: DateTime<Utc>,
        #[arg(long)]
        species: String,

        // --- FIX #2: Corrected argument grouping logic ---

        // The presence of `--site-name` now requires both `--zone-name` and `--plot-name`
        // This is the correct logic for Coastal Monitoring deployments.
        #[arg(long, requires_all = ["zone_name", "plot_name"])]
        site_name: Option<String>,
        #[arg(long)]
        zone_name: Option<String>,
        #[arg(long)]
        plot_name: Option<String>,

        // The presence of `--health-status` now requires `--collar-present`.
        // This is the correct logic for Stemflow deployments.
        #[arg(long, requires_all = ["collar_present"])]
        health_status: Option<String>,
        #[arg(long, action = clap::ArgAction::Set)]
        collar_present: Option<bool>,
        // robyn_label remains fully optional
        #[arg(long)]
        robyn_label: Option<String>,
    },
    List,
    Delete {
        #[arg(long)]
        id: Uuid,
    },
    Update {
        #[arg(long)]
        id: Uuid,
    },
}

/// The main handler for all `sapflux deployment` commands.
pub async fn handle_deployment_command(command: DeploymentCommands, pool: &PgPool) -> Result<()> {
    match command {
        DeploymentCommands::Create {
            project_name,
            datalogger_id,
            sdi_address,
            tree_id,
            sensor_id,
            start_time_utc,
            species,
            site_name,
            zone_name,
            plot_name,
            health_status,
            collar_present,
            robyn_label,
        } => {
            println!("Attempting to create a new deployment...");

            let project = metadata::get_project_by_name(pool, &project_name).await?;
            let sensor = metadata::get_sensor_by_id_string(pool, &sensor_id).await?;

            let attributes = match project_name.as_str() {
                "Coastal Monitoring" => {
                    DeploymentAttributes::CoastalMonitoring(CoastalMonitoringAttributes {
                        site_name: site_name.ok_or_else(|| anyhow::anyhow!("--site-name is required for Coastal Monitoring"))?,
                        zone_name: zone_name.ok_or_else(|| anyhow::anyhow!("--zone-name is required for Coastal Monitoring"))?,
                        plot_name: plot_name.ok_or_else(|| anyhow::anyhow!("--plot-name is required for Coastal Monitoring"))?,
                        species,
                    })
                }
                "Stemflow Experiment" => {
                    DeploymentAttributes::Stemflow(StemflowAttributes {
                        site_name: site_name.ok_or_else(|| anyhow::anyhow!("--site-name is required for Stemflow Experiment"))?,
                        species,
                        health_status: health_status.ok_or_else(|| anyhow::anyhow!("--health-status is required for Stemflow Experiment"))?,
                        collar_present: collar_present.ok_or_else(|| anyhow::anyhow!("--collar-present is required for Stemflow Experiment"))?,
                        robyn_label,
                    })
                }
                _ => return Err(anyhow::anyhow!("Unknown project type for attribute construction.")),
            };

            let new_deployment_data = NewDeployment {
                start_time_utc,
                datalogger_id,
                sdi_address: SdiAddress::new(&sdi_address)?,
                tree_id,
                project_id: project.id,
                sensor_id: sensor.id,
                attributes,
            };

            metadata::create_deployment(pool, &new_deployment_data).await?;
        }
        DeploymentCommands::List => {
            println!("Logic for listing deployments will go here.");
        }
        DeploymentCommands::Delete { id } => {
            println!("Logic for deleting deployment with ID {} will go here.", id);
        }
        DeploymentCommands::Update { id } => {
            println!("Logic for updating deployment with ID {} will go here.", id);
        }
    }
    Ok(())
}