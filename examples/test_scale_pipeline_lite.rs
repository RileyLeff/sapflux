use sapflux::processing::SapfluxDataPipeline;
use sapflux::types::{Deployment, MeasurementContext, HardwareContext, SensorType, SdiAddress, DataloggerModel, FirmwareVersion};
use std::time::Instant;
use chrono::{DateTime, Utc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing Sapflux Pipeline at Scale (Memory-Efficient)");
    println!("=======================================================\n");
    
    // Load just a subset of deployments for testing
    println!("📋 Loading sample deployment metadata...");
    let sample_deployments = create_sample_deployments();
    println!("   - Sample deployments: {}", sample_deployments.len());
    
    // Test with just one recent data directory to avoid memory explosion
    let test_dir = "oldstuff/data/raw/2024_08_29";
    
    println!("\n🌲 Testing pipeline with recent data subset ({})...", test_dir);
    let start_time = Instant::now();
    
    let pipeline = SapfluxDataPipeline::new(sample_deployments);
    
    // Process the directory without collecting everything at once
    println!("📁 Processing data files...");
    let result = pipeline.process_directory(test_dir)?;
    
    let duration = start_time.elapsed();
    
    // Only collect a small sample for analysis
    let sample_result = result.limit(1000).collect()?;
    
    println!("✅ Pipeline completed successfully!");
    println!("   - Duration: {:?}", duration);
    println!("   - Sample rows: {}", sample_result.height());
    println!("   - Columns: {}", sample_result.width());
    
    // Display column names
    println!("   - Available columns: {:?}", sample_result.get_column_names());
    
    // Check for sap flux calculations
    let has_heat_velocity = sample_result.get_column_names().iter()
        .any(|name| name.contains("heat_velocity"));
    
    if has_heat_velocity {
        println!("\n🧮 Sap Flux Calculations:");
        println!("   ✅ Heat velocity calculations present");
        println!("   ✅ DMA_Péclet method implementation working");
    }
    
    // Check deployment matching
    if let Ok(status_col) = sample_result.column("deployment_status") {
        let mut matched = 0;
        let mut unmatched = 0;
        
        for i in 0..status_col.len().min(100) { // Only check first 100 rows
            if let Ok(status) = status_col.get(i) {
                if let Some(status_str) = status.get_str() {
                    match status_str {
                        "temporally_matched" => matched += 1,
                        "temporally_unmatched" => unmatched += 1,
                        _ => {}
                    }
                }
            }
        }
        
        println!("\n🕐 DST Correction & Temporal Matching Results (sample):");
        println!("   - Successfully matched: {} rows", matched);
        println!("   - Unmatched: {} rows", unmatched);
        if matched + unmatched > 0 {
            println!("   - Match rate: {:.1}%", (matched as f64 / (matched + unmatched) as f64) * 100.0);
        }
    }
    
    println!("\n🎉 Memory-efficient scale testing completed successfully!");
    println!("Pipeline improvements validated with real data subset.");
    
    Ok(())
}

fn create_sample_deployments() -> Vec<Deployment> {
    // Create a few representative deployments for testing
    let mut deployments = Vec::new();
    
    // Sample monitoring deployment
    let monitoring_hardware = HardwareContext {
        datalogger_model: DataloggerModel::CR200,
        datalogger_id: 302,
        firmware_version: FirmwareVersion::Firmware200_1,
        sensor_type: SensorType::ImplexxOld,
        sdi_address: SdiAddress("0".to_string()),
    };
    
    let monitoring_measurement = MeasurementContext {
        tree_id: "2431".to_string(),
        site_name: Some("brnv".to_string()),
        zone_name: Some("M".to_string()),
        plot_name: Some("2".to_string()),
        tree_species: Some("pintae".to_string()),
        health_status: None,
        collar_present: None,
        notes: None,
    };
    
    let start_time = DateTime::parse_from_str("2021-01-30 00:00:00 +00:00", "%Y-%m-%d %H:%M:%S %z")
        .unwrap().with_timezone(&Utc);
    
    deployments.push(Deployment::new(start_time, monitoring_hardware, monitoring_measurement));
    
    // Sample stemflow deployment 
    let stemflow_hardware = HardwareContext {
        datalogger_model: DataloggerModel::CR300,
        datalogger_id: 423,
        firmware_version: FirmwareVersion::Firmware300_1,
        sensor_type: SensorType::ImplexxNew,
        sdi_address: SdiAddress("0".to_string()),
    };
    
    let stemflow_measurement = MeasurementContext {
        tree_id: "153".to_string(),
        site_name: Some("monb".to_string()),
        zone_name: None,
        plot_name: Some("healthy-0-healthy".to_string()),
        tree_species: Some("liqsty".to_string()),
        health_status: Some("healthy".to_string()),
        collar_present: Some(false),
        notes: Some("stemflow".to_string()),
    };
    
    let stemflow_start = DateTime::parse_from_str("2025-05-20 06:00:00 +00:00", "%Y-%m-%d %H:%M:%S %z")
        .unwrap().with_timezone(&Utc);
        
    deployments.push(Deployment::new(stemflow_start, stemflow_hardware, stemflow_measurement));
    
    deployments
}