1. Parsers Must Extract All Available Data
Principle: Parsers are responsible for extracting the complete set of measurements available in a raw file into the in-memory DataFrame, even if those measurements are not used by the current default processing pipeline.
Explanation: This principle ensures forward compatibility and prevents the need for costly, large-scale re-parsing of raw data in the future. If a new calculation method is introduced that requires a previously unused column, that data will already have been extracted and will be immediately available for processing.
Example:
The SapFlowAll file format contains many columns related to the upstream thermistor series (e.g., S{addr}_TpUsOut, S{addr}_dTUsOut, S{addr}_TsUsOut). While the initial dma_peclet_v1 calculation pipeline primarily uses alpha, beta, and tm_seconds, the SapFlowAllParserV1 must still parse these upstream temperature columns and include them in the DataFrame.
