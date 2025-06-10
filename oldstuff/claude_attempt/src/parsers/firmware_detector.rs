use crate::types::{FirmwareVersion, DataloggerModel, RawDataHeader};

pub fn detect_firmware_version(
    header: &RawDataHeader,
    column_names: &[String],
) -> Option<FirmwareVersion> {
    let is_cr200 = header.datalogger_type.starts_with("CR2");
    let is_cr300 = header.datalogger_type.starts_with("CR3");
    
    // Check for multi-sensor format (new CR300) - specifically SapFlowAll table
    let has_multi_sensor_columns = column_names.iter().any(|col| 
        col.starts_with("S0_") || col.starts_with("S1_") || col.starts_with("S2_")
    );
    
    let is_sapflowall_table = header.table_name == "SapFlowAll";
    
    // Check for new-style naming (some CR300 files)
    let has_new_naming = column_names.iter().any(|col|
        col.contains("_L2_") || col.contains("_H2_") || col.contains("_M2_")
    );
    
    match (is_cr200, is_cr300, has_multi_sensor_columns, is_sapflowall_table, has_new_naming) {
        (true, false, false, _, _) => {
            // CR200 old vs new - differentiate by program name or column format
            if header.program_name.contains("cr200x_generator") {
                Some(FirmwareVersion::Firmware200_2)
            } else {
                Some(FirmwareVersion::Firmware200_1)
            }
        },
        (false, true, true, true, _) => Some(FirmwareVersion::Firmware300_2), // Multi-sensor CR300 with SapFlowAll
        (false, true, false, false, _) => Some(FirmwareVersion::Firmware300_1), // Single-sensor CR300
        _ => None,
    }
}

pub fn detect_datalogger_model(header: &RawDataHeader) -> Option<DataloggerModel> {
    if header.datalogger_type.starts_with("CR2") {
        Some(DataloggerModel::CR200)
    } else if header.datalogger_type.starts_with("CR3") {
        Some(DataloggerModel::CR300)
    } else {
        None
    }
}