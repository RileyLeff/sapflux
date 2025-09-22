use once_cell::sync::Lazy;

#[derive(Debug, Clone)]
pub struct DataFormatDescriptor {
    pub code: &'static str,
    pub schema_json: Option<&'static str>,
    pub description: &'static str,
}

static DATA_FORMATS: Lazy<Vec<DataFormatDescriptor>> = Lazy::new(|| {
    vec![DataFormatDescriptor {
        code: "sapflow_toa5_hierarchical_v1",
        schema_json: None,
        description:
            "Hierarchical Campbell Scientific TOA5 parse with logger-level and thermistor tables",
    }]
});

pub fn all_data_formats() -> &'static [DataFormatDescriptor] {
    DATA_FORMATS.as_slice()
}
