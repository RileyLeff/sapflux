use sapflux_core::{data_formats::all_data_formats, parsers::all_parser_descriptors, pipelines::all_pipeline_descriptors};

#[test]
fn registries_are_populated() {
    assert!(
        all_data_formats().iter().any(|df| df.code == "sapflow_toa5_hierarchical_v1"),
        "expected canonical data format to be registered"
    );

    assert!(
        all_parser_descriptors()
            .iter()
            .any(|parser| parser.code == "sapflow_all_v1"),
        "expected sapflow_all parser to be registered"
    );

    assert!(
        all_pipeline_descriptors()
            .iter()
            .any(|pipeline| pipeline.code == "standard_v1_dst_fix"),
        "expected standard pipeline to be registered"
    );
}
