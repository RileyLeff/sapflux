use sapflux::processing::SapfluxDataPipeline;
use sapflux::types::DeploymentLoader;
use sapflux::parsers::CsvParser;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Sap Flux Data Processing Pipeline Demo");
    println!("=====================================\n");
    
    // Load deployments
    println!("Loading deployment metadata...");
    
    let monitoring_deployments = match DeploymentLoader::load_monitoring_deployments("oldstuff/data/deployments_monitoring.csv") {
        Ok(deployments) => {
            println!("✅ Loaded {} monitoring deployments", deployments.len());
            deployments
        },
        Err(e) => {
            eprintln!("❌ Failed to load monitoring deployments: {}", e);
            Vec::new()
        }
    };
    
    let stemflow_deployments = match DeploymentLoader::load_stemflow_deployments("oldstuff/data/deployments_stemflow.csv") {
        Ok(deployments) => {
            println!("✅ Loaded {} stemflow deployments", deployments.len());
            deployments
        },
        Err(e) => {
            eprintln!("❌ Failed to load stemflow deployments: {}", e);
            Vec::new()
        }
    };
    
    // Combine all deployments
    let mut all_deployments = monitoring_deployments;
    all_deployments.extend(stemflow_deployments);
    
    println!("Total deployments: {}\n", all_deployments.len());
    
    // Initialize pipeline with deployments
    let pipeline = SapfluxDataPipeline::new(all_deployments);
    
    // Test files including the new multi-sensor format
    let test_files = vec![
        PathBuf::from("oldstuff/data/raw/2024_04_17/CR200Series_601_Table1.dat"),
        PathBuf::from("oldstuff/data/raw/2024_05_13/CR300Series_502_L2_5491.csv"),
        PathBuf::from("oldstuff/data/raw/2025_05_20/CR300Series_421_SapFlowAll.dat"),
    ];
    
    println!("Testing parsers on different formats...\n");
    
    // Test each file individually to see parsing results
    for file_path in &test_files {
        print!("Parsing {}: ", file_path.file_name().unwrap().to_string_lossy());
        match CsvParser::parse_file(file_path.clone()) {
            Ok(raw_file) => {
                println!("✅ {} data points, table: {}", 
                    raw_file.data_points.len(),
                    raw_file.header.table_name
                );
                
                if let Some(first_point) = raw_file.data_points.first() {
                    println!("   First timestamp: {}, Logger: {:?}, SDI: {:?}", 
                        first_point.timestamp,
                        first_point.logger_id,
                        first_point.sdi_address
                    );
                }
            },
            Err(e) => println!("❌ {}", e),
        }
    }
    
    println!("\nProcessing all files through Polars pipeline...");
    
    match pipeline.process_files(test_files) {
        Ok(processed_df) => {
            println!("✅ Successfully processed data into Polars LazyFrame");
            
            // Generate summary report
            match pipeline.generate_summary_report(processed_df.clone()) {
                Ok(report) => println!("\n{}", report),
                Err(e) => eprintln!("Failed to generate report: {}", e),
            }
            
            // Export to Parquet for demonstration
            let output_path = "output/processed_sap_flux_demo.parquet";
            
            // Create output directory if it doesn't exist
            if let Some(parent) = std::path::Path::new(output_path).parent() {
                std::fs::create_dir_all(parent).ok();
            }
            
            match pipeline.export_processed_data(processed_df, output_path) {
                Ok(()) => println!("\n✅ Data exported to: {}", output_path),
                Err(e) => eprintln!("Failed to export data: {}", e),
            }
        },
        Err(e) => {
            eprintln!("❌ Pipeline failed: {}", e);
        }
    }
    
    Ok(())
}
