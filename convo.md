# AI Studio Conversation

**User:**

User

![file thumbnail](https://lh3.googleusercontent.com/drive-storage/AJQWtBONmJied5UlWSL22f4gUqjMI21d7N53O2kXGWiz6zlTGBompMQoQWFNzjqOBcjZ2QM87fMvqSU3rkeMhgHywYCLYnM7BTK9-DzADHNxBbrrJeM)

 docs IX-SF30-Integrators-Guide-Implexx-Sap-Flow-Sensor-ver-Aug-2024.pdf2,323 tokens

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Examining Data Parser Needs**

I'm now diving deep into the user's request, focusing on the core need for a Rust-based CSV data parser. I've begun to break down the problem into key components to ensure a solid foundation.

**Outlining Parsing Architecture**

I'm now outlining a flexible parsing architecture. I'm focusing on handling both CSV formats, informative error messages, and extensibility. The user's input highlights two CSV formats with distinct headers, requiring a robust parsing strategy. I'm leveraging the SDI-12 guide to decipher column names. I also aim to develop a clear structure for output data. The CRBasic programs provide context, confirming the need for versatility.

**Structuring Data Parsing Logic**

I'm now analyzing the information to develop an architectural plan. I've got a grasp of the data, the files to consider, the parsing steps, and the overall scope of the work. I've begun reviewing the provided information to determine data structures and to identify column names. The provided documentation and CRBasic files are proving to be invaluable resources. It's becoming clearer how to best structure the parser for maximum flexibility and error handling. I'm focusing on defining clear inputs, outputs, and intermediate data models.

**Mapping Formats & Dependencies**

I've been analyzing the information and I'm confident in my progress. I'm focusing on defining the different data formats and I can see how they can be handled elegantly. I'm mapping the data fields to the CRBasic programs and looking to determine how the sensor data relates. The project structure and the Python scripts have provided context.

**Analyzing Format Specifics**

I'm now fully immersed in the different data formats. Format A, with its per-sensor tables, and Format B, encompassing all sensor data, are now clearly distinguished in my mind. I've begun to translate the cryptic column names into more informative labels based on the Implexx guide, the provided code, and the CRBasic files. I'm focusing on crafting a structure that can elegantly handle the commonalities and differences between these formats.

**Formulating Data Model & Renaming**

I'm now establishing a clear data model structure for the parsed output and renaming the CSV columns. I'm focusing on user-friendliness and clarity, translating cryptic sensor readings into descriptive field names. I'm also ensuring the structure supports both CSV formats. The Implexx guide, CRBasic code, and project context are invaluable for this translation and validation.

**Developing Data Parsing Strategy**

I'm developing a robust parsing strategy considering the complexities of Format A and B. I will leverage Rust's error handling for flexibility and extensibility. I will utilize the CRBasic code for validation. I'm focusing on parsing the differing headers. I'm working to identify the most efficient parsing steps.

I'm now refining the data parser, incorporating insights from the SDI-12 document to translate the sensor data.

**Implementing Data Parser Logic**

I'm now fully immersed in developing the data parser in Rust. I've defined the output data structures, and I'm developing a robust parsing strategy that balances flexibility with efficiency. I'm focusing on the Parser Trait, which will allow for extensible parser implementations and is the core component to solve the user's needs. I'm working to make sure that the program handles the different CRBasic datalogger programs, by testing with different CSV files.

**Developing Parsing Logic**

I've made considerable progress on defining parsing logic. I'm focusing on parsing the CSV header and data rows based on the defined data formats. I will ensure error handling, especially for inconsistent formats. I'm leveraging the outlined output structure and the "Parser Trait" to define how the program handles new datalogger programs, by testing with the various CSV files.

I've also begun writing unit tests for parsing a header row, column name translation, and some basic CSV data processing.

**Formulating Parser Implementation**

I'm now starting to develop the implementation. I'm focusing on defining a Parser trait for flexibility, error handling, and extensibility to adapt to new file versions and to other future sensor data. The use of a trait will make it easy to extend the program with new datalogger file formats. I'm also planning to use Rust's powerful error handling. I'm leveraging the outlined output data structure and my recent column renaming to guide this step.

**Implementing Parsing Strategy**

I'm now implementing the parser logic. I'm focusing on creating an implementation that utilizes a Trait which allows for extensibility, and is tailored for the complexities of the datalogger output. I am also working to ensure that the code supports CRBasic-based validation as the user requested. I am working towards a modular structure that makes it easier to test the different file types.

I've made considerable progress in defining the parsers. I'm implementing the Trait-based design to support flexible parsing of files with diverse datalogger programs. I'm also planning to incorporate the user-friendly column names. I'm focusing on defining and implementing parsing logic for the header row, the data columns, and error handling. I am also leveraging the data model that I previously outlined.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Refining User Input**

I'm currently working to fully unpack the user's feedback, dissecting each comment into distinct, actionable pieces. My primary focus is pinpointing the core issues raised, ensuring no detail slips through the cracks. Once I've a clear understanding of each point, I will begin to formulate targeted responses.

**Prioritizing Key Insights**

I've just finished deconstructing the user's feedback, identifying two key areas. Firstly, I'm adjusting the parsing logic so that certain values are validated but omitted from the final output, as requested. Secondly, I'm reworking the output structure to better support the user's need for hierarchical, columnar data and preservation of the raw input. This means a move away from row-based structs toward a structure that can accommodate nested columnar data, potentially using a HashMap or a custom struct with polars::Series.

**Adapting Output Structures**

Now, I'm diving deeper into the implications of the feedback on the output structure. The shift toward columnar data is taking shape. I'm leaning toward a nested structure using a HashMap to organize data by logger and sensor, with each sensor containing polars::Series for individual measurements. My focus is ensuring the raw input text is preserved, ideally alongside the parsed columnar data for later user manipulation. I am also working through how the suggested TOML configuration for column names will influence the data mapping process and make any necessary additions to the parser traits.

**Evaluating Parse Steps**

I'm now weighing the merits of a two-step parsing process versus a one-step approach. Initially, I leaned towards two steps: parse into an intermediate row-based structure, then transform into columnar data. However, after considering the implications, the complexity might outweigh the benefits. Now, I'm examining ways to streamline the parsing directly into a columnar format using Polars Series, potentially reducing the overall processing time. This could mean adjusting how the data is handled during the initial parsing stages.

**Streamlining the Parsing Process**

I'm now fully immersed in the merits of a single-step versus a two-step parsing approach. The preference leans toward the single-step strategy directly into columnar structures. This design optimizes both memory use and performance, leveraging Polars Series. By handling the raw input along with the parsed columnar data, the user's initial requirements are met. Adapting the parsing traits is now taking shape, moving toward a single concrete output to prioritize clarity.

**Evaluating Memory Optimization**

I'm now evaluating whether a two-step parsing process, using intermediate row-based structures before converting to columnar data, is best or if a one-step approach directly to polars::Series would be more efficient. The one-step strategy seems the optimal choice, especially considering the performance benefits and reduced memory overhead associated with columnar formats. This decision impacts how data is handled during the parsing, as well as the output structure.

**Analyzing Design Implications**

I'm now taking a deeper look at the initial raw text data and thinking about the design of a single-step parser. Handling the raw input text alongside the parsed columnar data is high-priority. I'm also examining how the TOML configuration for column names will influence the data mapping process, and deciding where to add the parse input traits.

\n\n**Finalizing Output Structures**

Now, I'm at the stage of evaluating how the output will look. The parsing will be done in one step and output columnar data. This means that my output structure is now RawInputText, LoggerData in a columnar DataFrame, and SensorData in a second columnar DataFrame. I'm taking the steps to preserve the raw input text in the output. I am preparing the appropriate structs.

**Evaluating Output Structures**

Now, I'm working to fully define the output data structure, ensuring it aligns with the one-step parsing approach. I'm focusing on creating two DataFrames: one for general logger data (timestamp, record number, etc.) and another for sensor-specific data (sdi12 address, alpha/beta values), with the timestamp acting as a key. Preserving the raw input is still a top priority.

**Evaluating Implementation Strategy**

I'm now fully immersed in the merits of a single-step versus a two-step parsing approach. The preference leans toward the single-step strategy, as this leverages memory and performance benefits of Polars. I am making the parser output a Result<ParsedColumnarData, ParserError>. Now I'm working through parsing directly into columnar data structures, optimizing for memory efficiency.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Examining Internal Naming**

I'm now zeroing in on the internal naming aspect. I see the need for each measurement to possess a vector representing various internal name options. I'm exploring the best way to structure this vector within the TOML format. Should it be an array of strings? Or perhaps a table with specific keys? The nuances are being considered.

**Structuring Internal Names**

I've been wrestling with how best to handle internal names and the sensor address placeholder. Currently, I'm leaning toward a table within the TOML for each measurement, listing potential internal names as strings. The placeholder itself is the sticking point. I considered using 'A', but string replacement seems too fragile. A regex-friendly placeholder like {addr} feels more robust and less prone to accidental conflicts. This ensures clarity and adaptability for diverse naming conventions.

**Considering Diverse Names**

I'm now focusing on how the TOML structure best accommodates diverse internal name options. Should I use an array of strings directly within the measurement definition, or consider a key-value format where keys might describe the naming convention? I'm leaning toward the latter for flexibility. I'm also planning to incorporate examples using the placeholder {addr} for clarity.

**Developing Vectorized Names**

I'm making progress on the internal name vector. I've been considering how to structure the list of potential internal names within the TOML for each measurement. It feels best to use an array of strings directly, for simplicity and clarity. I'm leaning toward incorporating examples with {addr} to show the user how these names will be used.

**Determining Placeholder Format**

My focus is now on the internal naming structure. I've chosen {addr} as the best placeholder for the sensor address within the internal names, due to its clarity and robustness. It avoids the pitfalls of simple string replacement. I'm moving forward with organizing these internal names.

\n\n

**Developing Final Internal Names**

Now, I'm integrating the comprehensive list of possible internal names with the {addr} placeholder into the TOML structure. The goal is a concise and flexible system.  Each measurement will include a clear array of internal name strings, which should cover all the various sensor address and formatting needs. I'm focusing on ensuring this final design is robust and easily understandable for the user.

**Refining Final Output**

I'm now fully immersed in the process of finalizing the output. My goal is to ensure the {addr} placeholder is clearly and effectively implemented. I'm focusing on crafting a concise explanation of the placeholder's benefits to the user. I'm also preparing to present an illustrative TOML example.

I've finalized the TOML structure with the detailed set of internal names, including the {addr} placeholder. I've also composed the supporting explanation for clarity, which I'm presenting now.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Examining Serialization Options**

I'm currently focused on the best approach for serializing the parser output, considering the downstream systems. I'm weighing the pros and cons of different serialization formats like JSON, Avro, and Protocol Buffers. This will influence how efficiently I can store and retrieve data from both PostgreSQL and Cloudflare R2. My goal is to maximize storage efficiency and retrieval speed.

**Choosing the Best Format**

I'm now zeroing in on the ideal serialization strategy. Apache Arrow IPC and Parquet have emerged as frontrunners due to their columnar nature. Arrow's zero-copy ability offers compelling speed advantages, while Parquet promises excellent storage efficiency. It's a trade-off that requires further investigation into my specific performance needs and storage constraints.

**Optimizing Data Placement**

I'm now refining the data placement strategy, considering the strengths of both Postgres and R2. The parser output will likely involve structured metadata alongside the raw data. I'm leaning toward storing the structured metadata and file hashes in Postgres, leveraging its querying and relational capabilities. The raw data itself, likely in a columnar format (Arrow or Parquet), will reside in Cloudflare R2 for cost-effective storage. This architecture aims to balance query performance with storage efficiency, while ensuring deduplication through file hashes.

**Formulating Data Pipelines**

I'm now devising the data flow between the parser, PostgreSQL, and R2. The core idea is to offload large data to object storage, leveraging Postgres for metadata management. A key element is the parser output's serialization format, which dictates the transfer efficiency. I'm focusing on a strategy ensuring data deduplication through file hashing.

**Designing the Data Pipeline**

I'm now putting together the core logic. After hashing a new file, I'll check Postgres for its hash. A hit means a duplicate, so I can skip processing. A miss triggers file upload to R2 and parsing. I'm focusing on making the parsing logic as parallelizable as possible using Rust's async runtime. This includes storing structured metadata in Postgres and the raw and processed data in R2 using efficient serialization.

**Formulating the Strategy**

I'm solidifying the data handling blueprint. A file's hash is the initial check in PostgreSQL for duplicates, guiding whether to upload the raw and processed data to R2.  The parsed output will be stored in R2, while essential metadata (file name, hash, and paths) will be managed by PostgreSQL. This approach balances storage efficiency with querying capabilities, and also ensures deduplication and data integrity.

**Implementing the Data Flow**

My focus is now on the data pipeline implementation. I'm leveraging the previously discussed strategies, with Arrow or Parquet for data serialization and Cloudflare R2 for storage. The plan is to create separate Rust programs for parsing and loading, with deduplication at the forefront to minimize redundant processing. I'll utilize PostgreSQL for metadata management and querying, ensuring data integrity.

**Formulating Data Storage**

I'm now determining how to handle file storage. The hash check will be the first step for deduplication. I'll load the output of parsing into R2 via file uploads. Structured metadata, including file names and paths, will be managed in Postgres for efficient data retrieval. The ultimate aim is an efficient and resilient data pipeline.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Considering Postgres 17**

I'm now zeroing in on the user's specific query about Postgres 17 and its implications for table design. I'm starting to deconstruct the question, identifying keywords like "Postgres 17" and "table design" to understand the core of their concern. This stage is all about carefully breaking down the question into its components.

**Examining Data Hierarchy**

I'm now fully immersed in the implications of Postgres 17 for the user's hierarchical data structure, specifically focusing on the idea of a file catalog. The JSONB data type immediately comes to mind as a key tool for efficiently storing nested metadata within a relational database. I am now evaluating the benefits and drawbacks of using JSONB against the classic relational approach. My thoughts on this are now clear.

**Clarifying Relational Structures**

I've been meticulously examining the user's conceptualization of nested data structures and comparing it with the principles of relational database design, focusing on Postgres 17's capabilities. My current thinking revolves around a delicate approach: validating their initial "intuitive" model while steering them toward the advantages of a normalized relational structure. The core challenge is explaining how this alignment enables easier querying and improved data analysis, particularly within the context of Postgres 17's advancements, like optimized JSONB handling.

**Proposing Data Flattening**

The current focus is on a strategic pivot, gently guiding the user toward a relational, "tidy data" structure, directly opposing the initial nested dataframe concept. I'm carefully constructing an explanation to illustrate why the initially desired nested structure is difficult to query and the superior performance of a flatter model. I am thinking through the examples to illustrate my points.

I have zeroed in on the need to clarify to the user why the tidy data paradigm is more powerful and flexible than the nested structure. I am mentally creating examples and analogies to convey the advantages.

**Proposing Tidy Data**

I'm now fully immersed in the most recent challenges. My focus is on clearly articulating why a "tidy" or relational data structure offers superior querying, data analysis, and long-term data management than the initial nested dataframe. I'm building a concise and convincing explanation, using analogies to compare and contrast. This is crucial for guiding the user to a more effective data design.

**Analyzing Data Structures**

I'm now zeroing in on the implications of a relational structure, which includes the suggestion to pivot away from a nested structure. My current focus is articulating why a relational data model is superior for querying and analysis, particularly when working with Postgres 17. I'm carefully constructing an explanation to illustrate this.

**Proposing a hybrid Postgres Schema**

I'm now fully immersed in the details. My current thinking is centered on a pragmatic Postgres solution, blending relational principles with the flexibility of JSONB. I'll recommend the user adopts a hybrid schema to optimize both performance and data modeling within their existing workflows.

**Considering Hybrid Schema**

I'm now zeroing in on a pragmatic solution, the hybrid approach. I'll recommend the user uses JSONB while still keeping key data columns separated. I need to clearly articulate why this offers the best of both worlds.

I'm constructing a final, compelling argument for the hybrid approach, balancing relational principles with JSONB flexibility for Postgres 17.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Analyzing User Preferences**

I'm currently dissecting the user's rejection of the tidy data model. My focus now is understanding their core objection and the reasoning behind it. It's becoming clearer that the immediate need to represent hierarchical relationships is a key driver. This informs the exploration of alternative data structures.

**Exploring Data Hierarchies**

I've zeroed in on the user's need for a hierarchical data structure. Their preference for preserving the semantic relationships between sensors and thermistors within files is very clear. This means a simple "tidy" data model won't suffice. I'm now investigating how to best represent these nested relationships in Rust, and specifically, the serialization/deserialization aspects are key, particularly the ability to serialize structs containing DataFrames with relationship mappings.

**Considering Binary Serialization**

My current thinking revolves around binary serialization. I'm actively assessing if tools like bincode can effectively preserve the hierarchical structure the user needs within a single file. The speed and compactness are attractive, but I'm grappling with the potential lock-in this approach creates. I'm now critically examining the trade-offs between serialization format and data accessibility for external tools.

**Prioritizing File-Level Context**

I'm now fully immersed in understanding the user's focus on the file as the key unit of processing, not just individual data points. The need to maintain context across sensors and thermistors within a single file is paramount. I realize the initial "tidy data" approach is fundamentally misaligned with this goal. This file-centric perspective has reshaped my entire approach.

**Revising Data Structure**

My current focus is on a significant shift in thinking. The user's emphasis on the file's inherent context is paramount, and my previous tidy data approach is no longer relevant. I am now centered on developing a file-centric model. I am re-evaluating the Rust structs, prioritizing a model that accommodates nested DataFrames to preserve the hierarchical sensor/thermistor relationships within the file.

**Evaluating Storage Solutions**

I'm now weighing the trade-offs between different storage approaches. While binary serialization offers speed, I'm concerned about long-term data accessibility. The hierarchical data formats, like HDF5 and Zarr, seem to fit the user's needs, but the ZIP archive approach is also worth considering due to its transparency and widespread compatibility. I'm focusing on providing the user with a variety of potential solutions.

**Designing a File Structure**

I'm now fully immersed in designing a file structure that centers the file as the core processing unit, reflecting the user's needs. The immediate challenge is crafting Rust structs to hold hierarchical DataFrames within a single file. My focus is on accommodating nested structures and ensuring efficient serialization and deserialization using formats that can be both performant and accessible, such as HDF5, Zarr, or a simple ZIP archive of Parquet files with metadata.

```
**Finalizing File Architecture**

I am now solidifying the details of the file architecture, fully incorporating the user's need to preserve semantic relationships within files as paramount. The best-suited format seems to be an archive, like ZIP, where each DataFrame from a single logger can be stored in its own Parquet file, with a manifest file inside providing relationship mappings. This approach provides both simplicity and access. This design choice is informed by a balance between performance, usability, and long-term data accessibility.
```

I'm focusing on defining the Rust structs to reflect this architecture.

**Defining the Final Format**

I'm solidifying the file structure, fully acknowledging the user's preference for semantic preservation within a file. The recommended design uses an archive, such as ZIP, containing a Parquet file for each DataFrame from a single logger, with an internal manifest file to map relationships.This approach balances simplicity, performance, and long-term access, and I am now concentrating on developing the Rust structs to reflect this architecture.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Delivering Code Samples**

I'm now generating Rust code examples incorporating the concepts we explored. The feedback is clear: strict adherence to no comments and using a typed enum for ThermistorPairData.id. I'm focusing on crafting concise, runnable code that perfectly reflects our discussion. The goal is clarity and directness.

**Implementing Relational Data**

I'm now generating the Rust code and SQL schema based on our discussion, focusing on a fully relational data model. I've created the ThermistorDepth enum with TryFrom<&str> for flexible string conversion, and AsRef<str> for serialization. I'm also confirming serde's ability to handle the ZIP archive abstraction, and I'll describe how it will work with the flat tables. My goal is to produce working code and a clear schema.

**Crafting Rust Implementations**

I've been working on the Rust code, focusing on the ThermistorDepth enum with TryFrom and AsRef implementations. I'm building out the ParsedFileData, LoggerData, SensorData, and ThermistorPairData structs, incorporating the enum. The SQL schema is taking shape as a fully flat table. Also, I'm proposing custom serialize and deserialize functions for ZIP archive handling with serde, which will not use derive macros, but instead manage the archive manually.

**Developing Serialized Archives**

I'm now generating the Rust code and SQL schema per our latest discussion, focused on custom serde implementations for ZIP archive handling. I'm building a plan to create custom serialize and deserialize functions for the ParsedFileData struct, which will allow us to serialize the metadata manifest using serde_json, and the dataframes using Polars' own serialization. We will not be using derive macros. The schema itself is a fully flat Postgres table. The focus is to create a working example.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Initiating the Breakdown**

I'm currently dissecting the user's request. My focus is on the core task: effectively dealing with -99 as a marker for missing data. I've begun to list actionable requirements, starting with how to handle this specific data value.

**Addressing Data Anomalies**

I'm now diving into the user's need to deal with "Time Travel." I've specified clear rules for identifying these invalid timestamps. My approach involves removing these offending rows from parsed DataFrames while keeping them in the raw data, for traceability. To ensure the DataFrames are consistent, I'm making sure all DataFrames maintain the same number of rows and same indices.

**Developing the Solution Outline**

I'm solidifying the core architecture for the solution. I've begun to define the key components and their interactions, focusing on the ParserConfig and how it will manage time-based validity checks. I am thinking about how to ensure data consistency across multiple dataframes during parsing and filtering. I'm also considering how to handle "flagging" large time jumps.

**Formulating Data Validation Steps**

Okay, I'm now honing in on the crucial steps for data validation. I've mapped out how the "flagging" of time jumps should occur after initial parsing and filtering. The focus is on a ParserConfig that includes parameters for minimum and maximum valid timestamps, as well as the jump threshold. I'm exploring the best approach to integrate these rules into the parsing process. It looks like the main filtering will happen first, then jump detection on the filtered data.

**Refining Data Validation**

I'm now zeroing in on the best method for data validation. I've designed the process to include handling both missing data and time-related issues, using configurations. I've also planned how to flag any large timestamp jumps, ensuring all the DataFrames remain aligned and have the correct indices after filtering.

**Structuring the Response**

I'm now organizing the flow of information for the response. I'll begin by acknowledging the common data quality issues and agreeing with the user's approach. My plan is to start with a breakdown of how the -99 handling will be done, followed by the "Time Travel" filtering logic. Finally, I'll address the "flagging" of time jumps. The overall design will include a live ParserConfig that controls filtering, with validation handled after filtering.

**Refining the Approach**

I'm solidifying the data handling and validation steps for the solution. I've broken down the user's requirements and have formulated plans for -99 as nulls, time travel filtering, and flagging of large time jumps.  I'm now thinking about how to integrate the time jump check efficiently into the overall data processing pipeline. The main focus remains on maintaining DataFrames with a unified data structure, post-filtering.

**Solidifying Data Validation**

I'm making progress on the validation plan. I've broken down the data processing flow into distinct steps, including handling missing data, identifying time travel anomalies, and flagging significant time jumps, all controlled by configuration parameters. Now, I'm focusing on ensuring data consistency by applying all filters consistently across multiple DataFrames after the initial parsing.

 Expand to view model thoughts 

 chevron_right

---

**Model:**

Model

![Thinking](https://www.gstatic.com/aistudio/watermark/watermark.png)

 Thoughts (experimental) Auto **Outlining the Core Elements**

I've been breaking down the request to develop a detailed plan for the Rust parser module. Currently, I'm identifying the core tasks and inputs. This involves analyzing the requirements for processing data from Campbell Scientific dataloggers, specifically the CR200-series. The goal is a comprehensive module that accurately interprets and handles the received data.

**Formulating Parser Architecture**

I'm now focusing on the architecture for the Rust parser module. Based on the clarified requirements, I'm leaning heavily towards a "Chain of Responsibility" pattern for format detection and parsing.  The hierarchical output structure is becoming clearer: File -> Logger -> Sensors -> ThermistorPairs, each level linked to an associated DataFrame. Error handling, especially the -99 code, is being addressed. I'm also ensuring the thermistor depth enum is robust. I'm considering using an intermediary data structure to manage file and sensor metadata.

**Refining Module Design**

I'm solidifying the core architecture for the parser. The "Chain of Responsibility" approach remains central, enabling sequential format handling. The clarified goals from the time-travel decision are shaping module boundaries. Focus is on individual value cleaning and error handling. I'm focusing on creating clean, parseable DataFrames at each hierarchical level.

**Formulating the Implementation**

I'm now fully immersed in the design details. The plan is to create a clear breakdown of structs and traits, including the central ParsedFileData and SapflowParser.  I'm outlining the execution flow of the "Chain of Responsibility", including error collection. Now I'm focusing on the implementation details, including the logic for individual parsers, particularly header validation and column mapping. I plan to address the complexities of handling column naming conventions and  the conversion of the -99 error code to null values.

**Constructing Detailed Documentation**

I'm currently crafting the plan's detailed documentation, breaking it into sections for clarity. This includes an overview, downstream pipeline description, and core module architecture with components like ParsedFileData and the "Chain of Responsibility" for the parsers. I'm focusing on the intricacies of column mapping within individual parser implementations and the -99 to null value transformation.

\n\n**Expanding on the Architecture**

I'm now detailing the planned Rust structs and traits, specifically ParsedFileData, SapflowParser, and ParserError. The "Chain of Responsibility" execution flow, including error collection, is being outlined. I'm focusing on the implementation of a single parser's parse method, covering header validation, column mapping (addressing the {addr} placeholder and varied naming conventions), data ingestion, and the construction of DataFrames and hierarchical assembly.

**Refining Data Structure Definitions**

I'm now detailing the planned Rust structs and traits, specifically ParsedFileData, SapflowParser, and ParserError. I've outlined the "Chain of Responsibility" and error-handling flow. The parse method of the individual parsers is the focus, detailing header validation, column mapping, data ingestion, DataFrame construction, and hierarchical assembly.

**Developing a Comprehensive Structure**

I'm expanding upon the detailed architectural structure for the Rust parser.  My plan now includes the explicit data structures, such as ParsedFileData and SapflowParser, with their core components described in detail. The "Chain of Responsibility" is being thoroughly mapped out, including a design for error collection.  I'm now focusing on the logic for the parse method of individual parsers: header validation, data ingestion, DataFrame construction and hierarchical assembly.

 Expand to view model thoughts 

 chevron_right

---

**User:**

I'm working on a parser for some sap flux data generated on implexx sensors and logged on campbell dataloggers. 

I want to write this in rust.

I have this initial description of the data formats for parsing: 

# Sapformats

Here are some formats for sap flux data.

First format:

"TOA5","CR200Series_304","CR2xx","","v05","Sapflux2sensor.","63887","Table2"
"TIMESTAMP","RECORD","BattV_Min","id","SDI1","SapFlwTot1","VhOut1","VhIn1","AlphaOut1","AlphaIn1","BetaOut1","BetaIn1","tMaxTout1","tMaxTin1"
"TS","RN","Volts","","","unit","unit","unit","unit","unit","unit","unit","unit","unit"
"","","Min","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp"
"2022-07-08 11:30:00",25996,11.03274,304,1,"NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN"
"2022-07-08 12:00:00",25997,11.03442,304,1,"NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN"
"2022-07-08 12:30:00",25998,11.07891,304,1,"NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN"
"2022-07-08 13:00:00",25999,11.05373,304,1,"NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN","NAN"

The first row first column should always be TOA5.
The first row second column should be the name of the logger, in this case CR200Series_304.
The first row third column should be the logger type, in this case CR2xx.
I'm not sure what the fourth position in the first row is, looks like it's blank here.
first row fifth position "v05" not sure what that is. 
first row sixth position is the name of the program.
I'm not sure what the 63887 is.
first row seventh position is the table number.

In second row, those are column names for the underlying data AND the sdi-12 address associated with a subset of the columns. Basically each file contains logger-level data, like timestamp, record, battv_min, and so on, and also contains 1 or more sensors, each with their own sdi-12 address postfix. I want to both extract the sdi12 addresses and map the column names to nicer names. A good parser should match on both the expected logger level data columns and some number of repititions of the expected sensor level data columns. The sensor-level data is further composed of thermistor pairs, each at a particular depth. That's the the "out" and "in" mean here. So we need to parse into some format like this:

file-level metadata + logger level data columns + vec(columns of sensors(columns of ThermistorPairs))

Does that make sense?

The third row is units, which you can see are mostly uninformative, but we should still parse them to ensure that we are getting consistently-formed data. We should always check to make sure we're getting exactly "TS","RN","Volts","","","unit","unit","unit","unit","unit","unit","unit","unit","unit".

The fourth row is measurement characteristics, again, mostly uninformative, but we should still parse them to ensure that we are getting consistently-formed data. We want to check for exactly "","","Min","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp".

The fifth row is the first row of actual data. We don't need to accept any files that don't contain at least one row of data.

The data columns need to be validated as follows:
first col -- consistently formatted timestamp like this: 2022-07-08 13:00:00
2nd col should be an int like this 25999
3rd col should be a float 
4th col int
5th col is an sdi12 address, which can be a single character a-z, A-Z, or 0-9. this should be a custom type with a fallible constructor, maybe built on ascii char in core or something like that.
The rest of the cols are sensor (thermistorpair) level data and should be parseable as floats.
The last two columns, "tMaxTout(address)","tMaxTin(address)" should be strictly positive.

I want a parser function that returns a nicely parsed structure on success and a nice informative ThisError error on failure. The parsing apparatus as a whole should basically be that we iterate through all possible parsers, collect errors if they happen, and return on the first successful parse. If we get all errors, we should return them with an error like "doesn't match any parser formats" or something like that.

Here's the other data format I know up for now. There may be other formats I want to add in the future, we should think about how to incorporate new formats and toggle known ones on/off.

Next format:
"TOA5","CR300Series_420","CR300","1740","CR300.Std.11.00","CPU:sapflux_2sensor_CR300_30min.cr300","60975","SapFlowAll"
"TIMESTAMP","RECORD","Batt_volt","PTemp_C","S0_AlpOut","S0_AlpInn","S0_BetOut","S0_BetInn","S0_tMxTout","S0_tMxTinn","S0_TpDsOut","S0_dTDsOut","S0_TsDsOut","S0_TpUsOut","S0_dTUsOut","S0_TsUsOut","S0_TpDsInn","S0_dTDsInn","S0_TsDsInn","S0_TpUsInn","S0_dTUsInn","S0_TsUsInn","S0_tMxTUsO","S0_tMxTUsI","S1_AlpOut","S1_AlpInn","S1_BetOut","S1_BetInn","S1_tMxTout","S1_tMxTinn","S1_TpDsOut","S1_dTDsOut","S1_TsDsOut","S1_TpUsOut","S1_dTUsOut","S1_TsUsOut","S1_TpDsInn","S1_dTDsInn","S1_TsDsInn","S1_TpUsInn","S1_dTUsInn","S1_TsUsInn","S1_tMxTUsO","S1_tMxTUsI"
"TS","RN","","","ratio","ratio","ratio","ratio","sec","sec","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","sec","sec","ratio","ratio","ratio","ratio","sec","sec","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","sec","sec"
"","","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp"
"2025-07-29 20:00:00",3420,13.03,33.1,0.04496,0.068,0.04836,0.06304,78.788,71.364,31.478,0.254,31.728,31.544,0.242,31.783,31.897,0.229,32.125,31.849,0.215,32.062,71.176,62.753,0.08498,0.15103,0.08798,0.16097,70.058,61.862,31.695,0.273,31.965,31.758,0.25,32.006,32.075,0.269,32.339,32.095,0.229,32.322,74.438,64.49
"2025-07-29 20:30:00",3421,13.02,31.89,0.02968,0.06034,0.0336,0.05056,76.71,69.785,30.992,0.242,31.231,31.06,0.234,31.292,31.496,0.223,31.718,31.459,0.212,31.668,68.599,62.923,0.07842,0.14516,0.07785,0.15103,69.608,59.161,31.161,0.267,31.426,31.235,0.247,31.48,31.639,0.264,31.898,31.662,0.227,31.886,73.236,64.54
"2025-07-29 21:00:00",3422,13.02,30.92,-0.02831,0.03736,-0.02393,0.03228,78.853,69.929,30.378,0.248,30.622,30.447,0.254,30.698,30.985,0.22,31.203,30.955,0.213,31.165,77.979,62.067,0.06601,0.13241,0.06152,0.14658,71.419,59.219,30.626,0.268,30.892,30.697,0.252,30.946,31.158,0.264,31.416,31.179,0.228,31.405,72.295,65.6
"2025-07-29 21:30:00",3423,13.01,30.14,-0.01589,0.03212,-0.00787,0.03185,79.782,70.111,29.845,0.255,30.095,29.94,0.257,30.194,30.484,0.223,30.705,30.467,0.216,30.681,74.915,64.995,0.0497,0.13241,0.04935,0.14282,68.986,57.787,30.105,0.27,30.373,30.163,0.257,30.418,30.678,0.263,30.936,30.695,0.228,30.921,74.616,65.921
"2025-07-29 22:00:00",3424,13,29.56,0.0084,0.03228,0.00834,0.0275,75.11,71.178,29.476,0.241,29.714,29.557,0.239,29.793,30.081,0.221,30.301,30.069,0.215,30.282,72.238,64.811,0.06176,0.12693,0.05737,0.1373,71.739,60.29,29.732,0.269,29.999,29.807,0.254,30.058,30.266,0.265,30.526,30.293,0.231,30.522,74.616,67.172
"2025-07-29 22:30:00",3425,13,29.13,-0.02387,0.02286,-0.01949,0.02264,78.849,70.425,29.126,0.254,29.375,29.229,0.259,29.484,29.703,0.223,29.924,29.696,0.218,29.912,78.724,65.808,0.04562,0.12262,0.0453,0.1373,71.736,62.291,29.347,0.271,29.616,29.417,0.259,29.674,29.879,0.265,30.139,29.905,0.231,30.135,76.116,68.421
"2025-07-29 23:00:00",3426,12.99,28.7,-0.04343,0.0184,-0.03879,0.01824,80.468,70.797,28.718,0.253,28.966,28.801,0.263,29.06,29.311,0.221,29.53,29.301,0.217,29.516,77.849,65.181,0.04634,0.1197,0.04204,0.13025,70.66,59.463,28.993,0.267,29.258,29.074,0.256,29.327,29.509,0.262,29.766,29.541,0.23,29.769,75.539,66.845
"2025-07-29 23:30:00",3427,12.99,28.35,-0.01659,0.02328,-0.00817,0.02306,77.422,68.496,28.322,0.244,28.562,28.394,0.246,28.638,28.921,0.219,29.138,28.911,0.214,29.123,72.553,65.376,0.0503,0.1197,0.04598,0.13025,72.095,60.898,28.631,0.267,28.896,28.706,0.255,28.958,29.136,0.262,29.393,29.165,0.23,29.393,75.223,68.029
"2025-07-30 00:00:00",3428,12.99,28.1,-0.04003,0.0184,-0.03537,0.01374,81.158,71.487,27.963,0.25,28.208,28.046,0.259,28.301,28.537,0.22,28.756,28.531,0.217,28.746,80.035,65.622,0.03442,0.11098,0.03414,0.12595,71.342,60.648,28.257,0.268,28.523,28.341,0.259,28.598,28.756,0.262,29.013,28.786,0.231,29.016,76.472,67.528

This one is similar: 

"TIMESTAMP","RECORD","Batt_volt","PTemp_C", in the third row = logger-level data.
This one has N > 1 sensors in the same file, which means more columns. Check these out: "S0_AlpOut","S0_AlpInn","S0_BetOut". That means Sensor 0 (sdi12 address), alpha (parameter), out (thermistorpair depth). Same with alpha inn (inner), and beta outer, and so on. 

The data columns that must be positive for this one are S(sensor address)_tMxTinn and S(sensor address)_tMxTOut. Otherwise the sensor data columns are all ok as regular floats.

I said I need to rename the columns. I need to come up with better names for the measurements. 

Here is some documentation on the implexx sensors attached as pdf. 

And here is the campbell cr200 and cr300 crbasic code I generated to run the programs: 

---
DIRECTORY STRUCTURE
---
- .gitignore
- .python-version
- README.md
- generate_variants.sh
- generated_programs/
  - cr200/
    - sapflux_1sensor_CR200X_30min.cr2
    - sapflux_2sensor_CR200X_30min.cr2
    - sapflux_3sensor_CR200X_30min.cr2
    - sapflux_4sensor_CR200X_30min.cr2
  - cr300/
    - sapflux_1sensor_CR300_30min.cr300
    - sapflux_2sensor_CR300_30min.cr300
    - sapflux_3sensor_CR300_30min.cr300
    - sapflux_4sensor_CR300_30min.cr300
  - other/
    - test_m6.cr300
- pyproject.toml
- src/
  - __init__.py
  - __pycache__/
    - __init__.cpython-313.pyc
    - cr200x_generator.cpython-313.pyc
    - main.cpython-313.pyc
  - cr200x_generator.py
  - cr300_generator.py
  - main.py
- uv.lock

---
FILE CONTENTS
---

--- FILE: generated_programs/cr200/sapflux_1sensor_CR200X_30min.cr2 ---
' CR200/CR200X Series
' Program to log standard data from Implexx Sap Flow Sensors
' Generated by Python Script (cr200x_generator.py)
' Number of Sensors: 1
' Measurement Interval: 30 minutes
' NOTE: This program uses one DataTable per sensor.
' CR200X supports a maximum of 8 DataTables with this strategy.

'--- Declare Variables and Units ---
Dim N
Public BattV
Public id
Public SDIData_Sensor0(9)

Public SensorAddress0

'--- Alias Declarations (Maps array elements to meaningful names) ---
Alias SDIData_Sensor0(1) = SapFlwTot0
Alias SDIData_Sensor0(2) = VhOuter0
Alias SDIData_Sensor0(3) = VhInner0
Alias SDIData_Sensor0(4) = AlphaOut0
Alias SDIData_Sensor0(5) = AlphaIn0
Alias SDIData_Sensor0(6) = BetaOut0
Alias SDIData_Sensor0(7) = BetaIn0
Alias SDIData_Sensor0(8) = tMaxTout0
Alias SDIData_Sensor0(9) = tMaxTin0

'--- Units Declarations ---
Units BattV=Volts
Units SapFlwTot0=literPerHour
Units VhOuter0=heatVelocity
Units VhInner0=heatVelocity
Units AlphaOut0=logTRatio
Units AlphaIn0=logTRatio
Units BetaOut0=logTRatio
Units BetaIn0=logTRatio
Units tMaxTout0=second
Units tMaxTin0=second

'--- Define Data Tables (One table per sensor due to CR200X field limit) ---
' Note: CR200X dataloggers have a limit of 16 fields per table and 8 tables total.
' DataTable names must be <= 12 characters.
DataTable(Table_S0,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress0)
	Sample(1,SapFlwTot0)
	Sample(1,VhOuter0)
	Sample(1,VhInner0)
	Sample(1,AlphaOut0)
	Sample(1,AlphaIn0)
	Sample(1,BetaOut0)
	Sample(1,BetaIn0)
	Sample(1,tMaxTout0)
	Sample(1,tMaxTin0)
EndTable

'--- Main Program ---
BeginProg
	Scan(30,Min)
		'Default CR200 Series Datalogger Battery Voltage measurement 'BattV'
		Battery(BattV)
		'User Entered Calculation (from example)
		id = Status.PakBusAddress(1,1)
		SensorAddress0 = 0

		' --- Collect standard data for Sensor 0 (Address "0") ---
		SDI12Recorder(SDIData_Sensor0(), "0M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor0(1) = NAN Then
			For N = 1 To 9
				SDIData_Sensor0(N) = NAN
			Next
		EndIf

		'Call Data Tables and Store Data
		CallTable Table_S0
	NextScan
EndProg

--- FILE: generate_variants.sh ---
#!/bin/bash

# Bash script to generate CRBasic programs for various sensor configurations

# --- Configuration ---
PYTHON_SCRIPT_PATH="src/main.py"
OUTPUT_DIR="generated_programs"
MEASUREMENT_INTERVAL_MIN=30 # Fixed at 30 minutes as requested

# --- Ensure output directory exists ---
mkdir -p "${OUTPUT_DIR}"
if [ ! -d "${OUTPUT_DIR}" ]; then
    echo "Error: Could not create output directory '${OUTPUT_DIR}'."
    exit 1
fi

# --- Python execution command ---
# Change this if you prefer to use python3 directly, e.g., "python3"
PYTHON_RUN_CMD="uv run python -m"

# --- Check if Python script exists ---
if [ ! -f "${PYTHON_SCRIPT_PATH}" ]; then
    echo "Error: Python script not found at '${PYTHON_SCRIPT_PATH}'."
    echo "Please ensure you are in the project root directory."
    exit 1
fi

echo "Starting CRBasic program generation..."
echo "--------------------------------------"

# --- Generate for CR200X ---
LOGGER_TYPE_CR200X="CR200X"
FILE_EXT_CR200X="cr2"
MAX_SENSORS_CR200X=4 # As requested, up to 4 for this script (actual limit is 8)

echo "\nGenerating for ${LOGGER_TYPE_CR200X} (Interval: ${MEASUREMENT_INTERVAL_MIN} min):"
for num_sensors in $(seq 1 ${MAX_SENSORS_CR200X}); do
    output_filename="${OUTPUT_DIR}/sapflux_${num_sensors}sensor_${LOGGER_TYPE_CR200X}_${MEASUREMENT_INTERVAL_MIN}min.${FILE_EXT_CR200X}"
    echo "  Generating: ${num_sensors} sensor(s) -> ${output_filename}"

    uv run python -m src.main \
        --logger-type "${LOGGER_TYPE_CR200X}" \
        -n "${num_sensors}" \
        -t "${MEASUREMENT_INTERVAL_MIN}" \
        -o "${output_filename}"

    if [ $? -eq 0 ]; then
        echo "    Successfully generated ${output_filename}"
    else
        echo "    ERROR generating ${output_filename}"
    fi
done

# --- Generate for CR300 ---
LOGGER_TYPE_CR300="CR300"
FILE_EXT_CR300="cr300" # Or .cr300, depending on compiler preference
MAX_SENSORS_CR300=4 # As requested, up to 4 for this script (actual limit is 62)

echo "\nGenerating for ${LOGGER_TYPE_CR300} (Interval: ${MEASUREMENT_INTERVAL_MIN} min):"
for num_sensors in $(seq 1 ${MAX_SENSORS_CR300}); do
    output_filename="${OUTPUT_DIR}/sapflux_${num_sensors}sensor_${LOGGER_TYPE_CR300}_${MEASUREMENT_INTERVAL_MIN}min.${FILE_EXT_CR300}"
    echo "  Generating: ${num_sensors} sensor(s) -> ${output_filename}"

    uv run python -m src.main \
        --logger-type "${LOGGER_TYPE_CR300}" \
        -n "${num_sensors}" \
        -t "${MEASUREMENT_INTERVAL_MIN}" \
        -o "${output_filename}"

    if [ $? -eq 0 ]; then
        echo "    Successfully generated ${output_filename}"
    else
        echo "    ERROR generating ${output_filename}"
    fi
done

echo "\n--------------------------------------"
echo "All generation tasks complete."
echo "Generated files are in the '${OUTPUT_DIR}/' directory."

--- FILE: .gitignore ---
cr2cmple.xxx
*.xxx
src/__pycache__

--- FILE: pyproject.toml ---
[project]
name = "sapflux-programs"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.13"
dependencies = []

--- FILE: generated_programs/cr200/sapflux_2sensor_CR200X_30min.cr2 ---
' CR200/CR200X Series
' Program to log standard data from Implexx Sap Flow Sensors
' Generated by Python Script (cr200x_generator.py)
' Number of Sensors: 2
' Measurement Interval: 30 minutes
' NOTE: This program uses one DataTable per sensor.
' CR200X supports a maximum of 8 DataTables with this strategy.

'--- Declare Variables and Units ---
Dim N_0 ' Loop counter for Sensor 0 error handling
Dim N_1 ' Loop counter for Sensor 1 error handling
Public BattV
Public id
Public SDIData_Sensor0(9)
Public SensorAddress0
Public SDIData_Sensor1(9)
Public SensorAddress1

'--- Alias Declarations (Maps array elements to meaningful names) ---
Alias SDIData_Sensor0(1) = SapFlwTot0
Alias SDIData_Sensor0(2) = VhOuter0
Alias SDIData_Sensor0(3) = VhInner0
Alias SDIData_Sensor0(4) = AlphaOut0
Alias SDIData_Sensor0(5) = AlphaIn0
Alias SDIData_Sensor0(6) = BetaOut0
Alias SDIData_Sensor0(7) = BetaIn0
Alias SDIData_Sensor0(8) = tMaxTout0
Alias SDIData_Sensor0(9) = tMaxTin0
Alias SDIData_Sensor1(1) = SapFlwTot1
Alias SDIData_Sensor1(2) = VhOuter1
Alias SDIData_Sensor1(3) = VhInner1
Alias SDIData_Sensor1(4) = AlphaOut1
Alias SDIData_Sensor1(5) = AlphaIn1
Alias SDIData_Sensor1(6) = BetaOut1
Alias SDIData_Sensor1(7) = BetaIn1
Alias SDIData_Sensor1(8) = tMaxTout1
Alias SDIData_Sensor1(9) = tMaxTin1

'--- Units Declarations ---
Units BattV=Volts
Units SapFlwTot0=literPerHour
Units VhOuter0=heatVelocity
Units VhInner0=heatVelocity
Units AlphaOut0=logTRatio
Units AlphaIn0=logTRatio
Units BetaOut0=logTRatio
Units BetaIn0=logTRatio
Units tMaxTout0=second
Units tMaxTin0=second
Units SapFlwTot1=literPerHour
Units VhOuter1=heatVelocity
Units VhInner1=heatVelocity
Units AlphaOut1=logTRatio
Units AlphaIn1=logTRatio
Units BetaOut1=logTRatio
Units BetaIn1=logTRatio
Units tMaxTout1=second
Units tMaxTin1=second

'--- Define Data Tables (One table per sensor due to CR200X field limit) ---
' Note: CR200X dataloggers have a limit of 16 fields per table and 8 tables total.
' DataTable names must be <= 12 characters.
DataTable(Table_S0,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress0)
	Sample(1,SapFlwTot0)
	Sample(1,VhOuter0)
	Sample(1,VhInner0)
	Sample(1,AlphaOut0)
	Sample(1,AlphaIn0)
	Sample(1,BetaOut0)
	Sample(1,BetaIn0)
	Sample(1,tMaxTout0)
	Sample(1,tMaxTin0)
EndTable

DataTable(Table_S1,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress1)
	Sample(1,SapFlwTot1)
	Sample(1,VhOuter1)
	Sample(1,VhInner1)
	Sample(1,AlphaOut1)
	Sample(1,AlphaIn1)
	Sample(1,BetaOut1)
	Sample(1,BetaIn1)
	Sample(1,tMaxTout1)
	Sample(1,tMaxTin1)
EndTable

'--- Main Program ---
BeginProg
	Scan(30,Min)
		'Default CR200 Series Datalogger Battery Voltage measurement 'BattV'
		Battery(BattV)
		'User Entered Calculation (from example)
		id = Status.PakBusAddress(1,1)
		SensorAddress0 = 0
		SensorAddress1 = 1

		' --- Collect standard data for Sensor 0 (Address "0") ---
		SDI12Recorder(SDIData_Sensor0(), "0M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor0(1) = NAN Then
			For N_0 = 1 To 9
				SDIData_Sensor0(N_0) = NAN
			Next
		EndIf

		' --- Collect standard data for Sensor 1 (Address "1") ---
		SDI12Recorder(SDIData_Sensor1(), "1M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor1(1) = NAN Then
			For N_1 = 1 To 9
				SDIData_Sensor1(N_1) = NAN
			Next
		EndIf

		'Call Data Tables and Store Data
		CallTable Table_S0
		CallTable Table_S1
	NextScan
EndProg

--- FILE: .python-version ---
3.13

--- FILE: generated_programs/cr200/sapflux_4sensor_CR200X_30min.cr2 ---
' CR200/CR200X Series
' Program to log standard data from Implexx Sap Flow Sensors
' Generated by Python Script (cr200x_generator.py)
' Number of Sensors: 4
' Measurement Interval: 30 minutes
' NOTE: This program uses one DataTable per sensor.
' CR200X supports a maximum of 8 DataTables with this strategy.

'--- Declare Variables and Units ---
Dim N_0 ' Loop counter for Sensor 0 error handling
Dim N_1 ' Loop counter for Sensor 1 error handling
Dim N_2 ' Loop counter for Sensor 2 error handling
Dim N_3 ' Loop counter for Sensor 3 error handling
Public BattV
Public id
Public SDIData_Sensor0(9)
Public SensorAddress0
Public SDIData_Sensor1(9)
Public SensorAddress1
Public SDIData_Sensor2(9)
Public SensorAddress2
Public SDIData_Sensor3(9)
Public SensorAddress3

'--- Alias Declarations (Maps array elements to meaningful names) ---
Alias SDIData_Sensor0(1) = SapFlwTot0
Alias SDIData_Sensor0(2) = VhOuter0
Alias SDIData_Sensor0(3) = VhInner0
Alias SDIData_Sensor0(4) = AlphaOut0
Alias SDIData_Sensor0(5) = AlphaIn0
Alias SDIData_Sensor0(6) = BetaOut0
Alias SDIData_Sensor0(7) = BetaIn0
Alias SDIData_Sensor0(8) = tMaxTout0
Alias SDIData_Sensor0(9) = tMaxTin0
Alias SDIData_Sensor1(1) = SapFlwTot1
Alias SDIData_Sensor1(2) = VhOuter1
Alias SDIData_Sensor1(3) = VhInner1
Alias SDIData_Sensor1(4) = AlphaOut1
Alias SDIData_Sensor1(5) = AlphaIn1
Alias SDIData_Sensor1(6) = BetaOut1
Alias SDIData_Sensor1(7) = BetaIn1
Alias SDIData_Sensor1(8) = tMaxTout1
Alias SDIData_Sensor1(9) = tMaxTin1
Alias SDIData_Sensor2(1) = SapFlwTot2
Alias SDIData_Sensor2(2) = VhOuter2
Alias SDIData_Sensor2(3) = VhInner2
Alias SDIData_Sensor2(4) = AlphaOut2
Alias SDIData_Sensor2(5) = AlphaIn2
Alias SDIData_Sensor2(6) = BetaOut2
Alias SDIData_Sensor2(7) = BetaIn2
Alias SDIData_Sensor2(8) = tMaxTout2
Alias SDIData_Sensor2(9) = tMaxTin2
Alias SDIData_Sensor3(1) = SapFlwTot3
Alias SDIData_Sensor3(2) = VhOuter3
Alias SDIData_Sensor3(3) = VhInner3
Alias SDIData_Sensor3(4) = AlphaOut3
Alias SDIData_Sensor3(5) = AlphaIn3
Alias SDIData_Sensor3(6) = BetaOut3
Alias SDIData_Sensor3(7) = BetaIn3
Alias SDIData_Sensor3(8) = tMaxTout3
Alias SDIData_Sensor3(9) = tMaxTin3

'--- Units Declarations ---
Units BattV=Volts
Units SapFlwTot0=literPerHour
Units VhOuter0=heatVelocity
Units VhInner0=heatVelocity
Units AlphaOut0=logTRatio
Units AlphaIn0=logTRatio
Units BetaOut0=logTRatio
Units BetaIn0=logTRatio
Units tMaxTout0=second
Units tMaxTin0=second
Units SapFlwTot1=literPerHour
Units VhOuter1=heatVelocity
Units VhInner1=heatVelocity
Units AlphaOut1=logTRatio
Units AlphaIn1=logTRatio
Units BetaOut1=logTRatio
Units BetaIn1=logTRatio
Units tMaxTout1=second
Units tMaxTin1=second
Units SapFlwTot2=literPerHour
Units VhOuter2=heatVelocity
Units VhInner2=heatVelocity
Units AlphaOut2=logTRatio
Units AlphaIn2=logTRatio
Units BetaOut2=logTRatio
Units BetaIn2=logTRatio
Units tMaxTout2=second
Units tMaxTin2=second
Units SapFlwTot3=literPerHour
Units VhOuter3=heatVelocity
Units VhInner3=heatVelocity
Units AlphaOut3=logTRatio
Units AlphaIn3=logTRatio
Units BetaOut3=logTRatio
Units BetaIn3=logTRatio
Units tMaxTout3=second
Units tMaxTin3=second

'--- Define Data Tables (One table per sensor due to CR200X field limit) ---
' Note: CR200X dataloggers have a limit of 16 fields per table and 8 tables total.
' DataTable names must be <= 12 characters.
DataTable(Table_S0,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress0)
	Sample(1,SapFlwTot0)
	Sample(1,VhOuter0)
	Sample(1,VhInner0)
	Sample(1,AlphaOut0)
	Sample(1,AlphaIn0)
	Sample(1,BetaOut0)
	Sample(1,BetaIn0)
	Sample(1,tMaxTout0)
	Sample(1,tMaxTin0)
EndTable

DataTable(Table_S1,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress1)
	Sample(1,SapFlwTot1)
	Sample(1,VhOuter1)
	Sample(1,VhInner1)
	Sample(1,AlphaOut1)
	Sample(1,AlphaIn1)
	Sample(1,BetaOut1)
	Sample(1,BetaIn1)
	Sample(1,tMaxTout1)
	Sample(1,tMaxTin1)
EndTable

DataTable(Table_S2,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress2)
	Sample(1,SapFlwTot2)
	Sample(1,VhOuter2)
	Sample(1,VhInner2)
	Sample(1,AlphaOut2)
	Sample(1,AlphaIn2)
	Sample(1,BetaOut2)
	Sample(1,BetaIn2)
	Sample(1,tMaxTout2)
	Sample(1,tMaxTin2)
EndTable

DataTable(Table_S3,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress3)
	Sample(1,SapFlwTot3)
	Sample(1,VhOuter3)
	Sample(1,VhInner3)
	Sample(1,AlphaOut3)
	Sample(1,AlphaIn3)
	Sample(1,BetaOut3)
	Sample(1,BetaIn3)
	Sample(1,tMaxTout3)
	Sample(1,tMaxTin3)
EndTable

'--- Main Program ---
BeginProg
	Scan(30,Min)
		'Default CR200 Series Datalogger Battery Voltage measurement 'BattV'
		Battery(BattV)
		'User Entered Calculation (from example)
		id = Status.PakBusAddress(1,1)
		SensorAddress0 = 0
		SensorAddress1 = 1
		SensorAddress2 = 2
		SensorAddress3 = 3

		' --- Collect standard data for Sensor 0 (Address "0") ---
		SDI12Recorder(SDIData_Sensor0(), "0M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor0(1) = NAN Then
			For N_0 = 1 To 9
				SDIData_Sensor0(N_0) = NAN
			Next
		EndIf

		' --- Collect standard data for Sensor 1 (Address "1") ---
		SDI12Recorder(SDIData_Sensor1(), "1M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor1(1) = NAN Then
			For N_1 = 1 To 9
				SDIData_Sensor1(N_1) = NAN
			Next
		EndIf

		' --- Collect standard data for Sensor 2 (Address "2") ---
		SDI12Recorder(SDIData_Sensor2(), "2M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor2(1) = NAN Then
			For N_2 = 1 To 9
				SDIData_Sensor2(N_2) = NAN
			Next
		EndIf

		' --- Collect standard data for Sensor 3 (Address "3") ---
		SDI12Recorder(SDIData_Sensor3(), "3M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor3(1) = NAN Then
			For N_3 = 1 To 9
				SDIData_Sensor3(N_3) = NAN
			Next
		EndIf

		'Call Data Tables and Store Data
		CallTable Table_S0
		CallTable Table_S1
		CallTable Table_S2
		CallTable Table_S3
	NextScan
EndProg

--- FILE: generated_programs/cr300/sapflux_2sensor_CR300_30min.cr300 ---
' CR300 Series Datalogger Program
' Program to log 20 specified measurements from Implexx Sap Flow Sensors
' Generated by Python Script (cr300_generator.py)
' Number of Sensors: 2
' Measurement Interval: 30 minutes

'--- Constants ---
Const MEAST_INTERVAL_MIN = 30
Const SDI12_PORT = C1 ' Default SDI-12 Port (e.g., C1, C2)

'--- Declare Public Variables ---
Public PTemp_C As Float
Public Batt_volt As Float
Dim N_Loop ' Generic loop counter for error handling

Public S0_M_Data(9) As Float
Public S0_M1_Data(6) As Float
Public S0_M2_Data(6) As Float
Public S0_M5_Data(2) As Float
Public S1_M_Data(9) As Float
Public S1_M1_Data(6) As Float
Public S1_M2_Data(6) As Float
Public S1_M5_Data(2) As Float

'--- Alias Declarations (for logged variables) ---
Alias S0_M_Data(4) = S0_AlpOut : Units S0_AlpOut=ratio
Alias S0_M_Data(5) = S0_AlpInn : Units S0_AlpInn=ratio
Alias S0_M_Data(6) = S0_BetOut : Units S0_BetOut=ratio
Alias S0_M_Data(7) = S0_BetInn : Units S0_BetInn=ratio
Alias S0_M_Data(8) = S0_tMxTout : Units S0_tMxTout=sec
Alias S0_M_Data(9) = S0_tMxTinn : Units S0_tMxTinn=sec
Alias S0_M1_Data(1) = S0_TpDsOut : Units S0_TpDsOut=degC
Alias S0_M1_Data(2) = S0_dTDsOut : Units S0_dTDsOut=degC
Alias S0_M1_Data(3) = S0_TsDsOut : Units S0_TsDsOut=degC
Alias S0_M1_Data(4) = S0_TpUsOut : Units S0_TpUsOut=degC
Alias S0_M1_Data(5) = S0_dTUsOut : Units S0_dTUsOut=degC
Alias S0_M1_Data(6) = S0_TsUsOut : Units S0_TsUsOut=degC
Alias S0_M2_Data(1) = S0_TpDsInn : Units S0_TpDsInn=degC
Alias S0_M2_Data(2) = S0_dTDsInn : Units S0_dTDsInn=degC
Alias S0_M2_Data(3) = S0_TsDsInn : Units S0_TsDsInn=degC
Alias S0_M2_Data(4) = S0_TpUsInn : Units S0_TpUsInn=degC
Alias S0_M2_Data(5) = S0_dTUsInn : Units S0_dTUsInn=degC
Alias S0_M2_Data(6) = S0_TsUsInn : Units S0_TsUsInn=degC
Alias S0_M5_Data(1) = S0_tMxTUsO : Units S0_tMxTUsO=sec
Alias S0_M5_Data(2) = S0_tMxTUsI : Units S0_tMxTUsI=sec
Alias S1_M_Data(4) = S1_AlpOut : Units S1_AlpOut=ratio
Alias S1_M_Data(5) = S1_AlpInn : Units S1_AlpInn=ratio
Alias S1_M_Data(6) = S1_BetOut : Units S1_BetOut=ratio
Alias S1_M_Data(7) = S1_BetInn : Units S1_BetInn=ratio
Alias S1_M_Data(8) = S1_tMxTout : Units S1_tMxTout=sec
Alias S1_M_Data(9) = S1_tMxTinn : Units S1_tMxTinn=sec
Alias S1_M1_Data(1) = S1_TpDsOut : Units S1_TpDsOut=degC
Alias S1_M1_Data(2) = S1_dTDsOut : Units S1_dTDsOut=degC
Alias S1_M1_Data(3) = S1_TsDsOut : Units S1_TsDsOut=degC
Alias S1_M1_Data(4) = S1_TpUsOut : Units S1_TpUsOut=degC
Alias S1_M1_Data(5) = S1_dTUsOut : Units S1_dTUsOut=degC
Alias S1_M1_Data(6) = S1_TsUsOut : Units S1_TsUsOut=degC
Alias S1_M2_Data(1) = S1_TpDsInn : Units S1_TpDsInn=degC
Alias S1_M2_Data(2) = S1_dTDsInn : Units S1_dTDsInn=degC
Alias S1_M2_Data(3) = S1_TsDsInn : Units S1_TsDsInn=degC
Alias S1_M2_Data(4) = S1_TpUsInn : Units S1_TpUsInn=degC
Alias S1_M2_Data(5) = S1_dTUsInn : Units S1_dTUsInn=degC
Alias S1_M2_Data(6) = S1_TsUsInn : Units S1_TsUsInn=degC
Alias S1_M5_Data(1) = S1_tMxTUsO : Units S1_tMxTUsO=sec
Alias S1_M5_Data(2) = S1_tMxTUsI : Units S1_tMxTUsI=sec

'--- DataTable Definition (Single Table for All Sensors) ---
DataTable (SapFlowAll, True, -1)
  DataInterval (0, MEAST_INTERVAL_MIN, Min, 0) ' No output delay
  Sample (1, Batt_volt, FP2)
  Sample (1, PTemp_C, FP2)
  Sample (1, S0_AlpOut, IEEE4)
  Sample (1, S0_AlpInn, IEEE4)
  Sample (1, S0_BetOut, IEEE4)
  Sample (1, S0_BetInn, IEEE4)
  Sample (1, S0_tMxTout, IEEE4)
  Sample (1, S0_tMxTinn, IEEE4)
  Sample (1, S0_TpDsOut, IEEE4)
  Sample (1, S0_dTDsOut, IEEE4)
  Sample (1, S0_TsDsOut, IEEE4)
  Sample (1, S0_TpUsOut, IEEE4)
  Sample (1, S0_dTUsOut, IEEE4)
  Sample (1, S0_TsUsOut, IEEE4)
  Sample (1, S0_TpDsInn, IEEE4)
  Sample (1, S0_dTDsInn, IEEE4)
  Sample (1, S0_TsDsInn, IEEE4)
  Sample (1, S0_TpUsInn, IEEE4)
  Sample (1, S0_dTUsInn, IEEE4)
  Sample (1, S0_TsUsInn, IEEE4)
  Sample (1, S0_tMxTUsO, IEEE4)
  Sample (1, S0_tMxTUsI, IEEE4)
  Sample (1, S1_AlpOut, IEEE4)
  Sample (1, S1_AlpInn, IEEE4)
  Sample (1, S1_BetOut, IEEE4)
  Sample (1, S1_BetInn, IEEE4)
  Sample (1, S1_tMxTout, IEEE4)
  Sample (1, S1_tMxTinn, IEEE4)
  Sample (1, S1_TpDsOut, IEEE4)
  Sample (1, S1_dTDsOut, IEEE4)
  Sample (1, S1_TsDsOut, IEEE4)
  Sample (1, S1_TpUsOut, IEEE4)
  Sample (1, S1_dTUsOut, IEEE4)
  Sample (1, S1_TsUsOut, IEEE4)
  Sample (1, S1_TpDsInn, IEEE4)
  Sample (1, S1_dTDsInn, IEEE4)
  Sample (1, S1_TsDsInn, IEEE4)
  Sample (1, S1_TpUsInn, IEEE4)
  Sample (1, S1_dTUsInn, IEEE4)
  Sample (1, S1_TsUsInn, IEEE4)
  Sample (1, S1_tMxTUsO, IEEE4)
  Sample (1, S1_tMxTUsI, IEEE4)
EndTable

'--- Main Program ---
BeginProg
  Scan (MEAST_INTERVAL_MIN, Min, 1, 0) ' Scan interval, units, buffer=1, count=0 (continuous)
    PanelTemp (PTemp_C, 60) ' Defaulting to 60Hz fnotch, or use PanelTemp(PTemp_C)
    Battery (Batt_volt)

    ' Initialize all sensor data variables to NAN
    S0_AlpOut = NAN
    S0_AlpInn = NAN
    S0_BetOut = NAN
    S0_BetInn = NAN
    S0_tMxTout = NAN
    S0_tMxTinn = NAN
    S0_TpDsOut = NAN
    S0_dTDsOut = NAN
    S0_TsDsOut = NAN
    S0_TpUsOut = NAN
    S0_dTUsOut = NAN
    S0_TsUsOut = NAN
    S0_TpDsInn = NAN
    S0_dTDsInn = NAN
    S0_TsDsInn = NAN
    S0_TpUsInn = NAN
    S0_dTUsInn = NAN
    S0_TsUsInn = NAN
    S0_tMxTUsO = NAN
    S0_tMxTUsI = NAN
    S1_AlpOut = NAN
    S1_AlpInn = NAN
    S1_BetOut = NAN
    S1_BetInn = NAN
    S1_tMxTout = NAN
    S1_tMxTinn = NAN
    S1_TpDsOut = NAN
    S1_dTDsOut = NAN
    S1_TsDsOut = NAN
    S1_TpUsOut = NAN
    S1_dTUsOut = NAN
    S1_TsUsOut = NAN
    S1_TpDsInn = NAN
    S1_dTDsInn = NAN
    S1_TsDsInn = NAN
    S1_TpUsInn = NAN
    S1_dTUsInn = NAN
    S1_TsUsInn = NAN
    S1_tMxTUsO = NAN
    S1_tMxTUsI = NAN

    ' --- Sensor 0 (Address "0") ---
    SDI12Recorder(S0_M_Data(), SDI12_PORT, "0", "M!", 1.0, 0, -1)
    If S0_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S0_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S0_M1_Data(), SDI12_PORT, "0", "M1!", 1.0, 0, -1)
    If S0_M1_Data(1) = NAN Then
      Move (S0_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S0_M2_Data(), SDI12_PORT, "0", "M2!", 1.0, 0, -1)
    If S0_M2_Data(1) = NAN Then
      Move (S0_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S0_M5_Data(), SDI12_PORT, "0", "M5!", 1.0, 0, -1)
    If S0_M5_Data(1) = NAN Then
      Move (S0_M5_Data(), 2, NAN, 1)
    EndIf

    ' --- Sensor 1 (Address "1") ---
    SDI12Recorder(S1_M_Data(), SDI12_PORT, "1", "M!", 1.0, 0, -1)
    If S1_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S1_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S1_M1_Data(), SDI12_PORT, "1", "M1!", 1.0, 0, -1)
    If S1_M1_Data(1) = NAN Then
      Move (S1_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S1_M2_Data(), SDI12_PORT, "1", "M2!", 1.0, 0, -1)
    If S1_M2_Data(1) = NAN Then
      Move (S1_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S1_M5_Data(), SDI12_PORT, "1", "M5!", 1.0, 0, -1)
    If S1_M5_Data(1) = NAN Then
      Move (S1_M5_Data(), 2, NAN, 1)
    EndIf

    CallTable SapFlowAll
  NextScan
EndProg

--- FILE: generated_programs/cr300/sapflux_1sensor_CR300_30min.cr300 ---
' CR300 Series Datalogger Program
' Program to log 20 specified measurements from Implexx Sap Flow Sensors
' Generated by Python Script (cr300_generator.py)
' Number of Sensors: 1
' Measurement Interval: 30 minutes

'--- Constants ---
Const MEAST_INTERVAL_MIN = 30
Const SDI12_PORT = C1 ' Default SDI-12 Port (e.g., C1, C2)

'--- Declare Public Variables ---
Public PTemp_C As Float
Public Batt_volt As Float
Dim N_Loop ' Generic loop counter for error handling

Public S0_M_Data(9) As Float
Public S0_M1_Data(6) As Float
Public S0_M2_Data(6) As Float
Public S0_M5_Data(2) As Float

'--- Alias Declarations (for logged variables) ---
Alias S0_M_Data(4) = S0_AlpOut : Units S0_AlpOut=ratio
Alias S0_M_Data(5) = S0_AlpInn : Units S0_AlpInn=ratio
Alias S0_M_Data(6) = S0_BetOut : Units S0_BetOut=ratio
Alias S0_M_Data(7) = S0_BetInn : Units S0_BetInn=ratio
Alias S0_M_Data(8) = S0_tMxTout : Units S0_tMxTout=sec
Alias S0_M_Data(9) = S0_tMxTinn : Units S0_tMxTinn=sec
Alias S0_M1_Data(1) = S0_TpDsOut : Units S0_TpDsOut=degC
Alias S0_M1_Data(2) = S0_dTDsOut : Units S0_dTDsOut=degC
Alias S0_M1_Data(3) = S0_TsDsOut : Units S0_TsDsOut=degC
Alias S0_M1_Data(4) = S0_TpUsOut : Units S0_TpUsOut=degC
Alias S0_M1_Data(5) = S0_dTUsOut : Units S0_dTUsOut=degC
Alias S0_M1_Data(6) = S0_TsUsOut : Units S0_TsUsOut=degC
Alias S0_M2_Data(1) = S0_TpDsInn : Units S0_TpDsInn=degC
Alias S0_M2_Data(2) = S0_dTDsInn : Units S0_dTDsInn=degC
Alias S0_M2_Data(3) = S0_TsDsInn : Units S0_TsDsInn=degC
Alias S0_M2_Data(4) = S0_TpUsInn : Units S0_TpUsInn=degC
Alias S0_M2_Data(5) = S0_dTUsInn : Units S0_dTUsInn=degC
Alias S0_M2_Data(6) = S0_TsUsInn : Units S0_TsUsInn=degC
Alias S0_M5_Data(1) = S0_tMxTUsO : Units S0_tMxTUsO=sec
Alias S0_M5_Data(2) = S0_tMxTUsI : Units S0_tMxTUsI=sec

'--- DataTable Definition (Single Table for All Sensors) ---
DataTable (SapFlowAll, True, -1)
  DataInterval (0, MEAST_INTERVAL_MIN, Min, 0) ' No output delay
  Sample (1, Batt_volt, FP2)
  Sample (1, PTemp_C, FP2)
  Sample (1, S0_AlpOut, IEEE4)
  Sample (1, S0_AlpInn, IEEE4)
  Sample (1, S0_BetOut, IEEE4)
  Sample (1, S0_BetInn, IEEE4)
  Sample (1, S0_tMxTout, IEEE4)
  Sample (1, S0_tMxTinn, IEEE4)
  Sample (1, S0_TpDsOut, IEEE4)
  Sample (1, S0_dTDsOut, IEEE4)
  Sample (1, S0_TsDsOut, IEEE4)
  Sample (1, S0_TpUsOut, IEEE4)
  Sample (1, S0_dTUsOut, IEEE4)
  Sample (1, S0_TsUsOut, IEEE4)
  Sample (1, S0_TpDsInn, IEEE4)
  Sample (1, S0_dTDsInn, IEEE4)
  Sample (1, S0_TsDsInn, IEEE4)
  Sample (1, S0_TpUsInn, IEEE4)
  Sample (1, S0_dTUsInn, IEEE4)
  Sample (1, S0_TsUsInn, IEEE4)
  Sample (1, S0_tMxTUsO, IEEE4)
  Sample (1, S0_tMxTUsI, IEEE4)
EndTable

'--- Main Program ---
BeginProg
  Scan (MEAST_INTERVAL_MIN, Min, 1, 0) ' Scan interval, units, buffer=1, count=0 (continuous)
    PanelTemp (PTemp_C, 60) ' Defaulting to 60Hz fnotch, or use PanelTemp(PTemp_C)
    Battery (Batt_volt)

    ' Initialize all sensor data variables to NAN
    S0_AlpOut = NAN
    S0_AlpInn = NAN
    S0_BetOut = NAN
    S0_BetInn = NAN
    S0_tMxTout = NAN
    S0_tMxTinn = NAN
    S0_TpDsOut = NAN
    S0_dTDsOut = NAN
    S0_TsDsOut = NAN
    S0_TpUsOut = NAN
    S0_dTUsOut = NAN
    S0_TsUsOut = NAN
    S0_TpDsInn = NAN
    S0_dTDsInn = NAN
    S0_TsDsInn = NAN
    S0_TpUsInn = NAN
    S0_dTUsInn = NAN
    S0_TsUsInn = NAN
    S0_tMxTUsO = NAN
    S0_tMxTUsI = NAN

    ' --- Sensor 0 (Address "0") ---
    SDI12Recorder(S0_M_Data(), SDI12_PORT, "0", "M!", 1.0, 0, -1)
    If S0_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S0_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S0_M1_Data(), SDI12_PORT, "0", "M1!", 1.0, 0, -1)
    If S0_M1_Data(1) = NAN Then
      Move (S0_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S0_M2_Data(), SDI12_PORT, "0", "M2!", 1.0, 0, -1)
    If S0_M2_Data(1) = NAN Then
      Move (S0_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S0_M5_Data(), SDI12_PORT, "0", "M5!", 1.0, 0, -1)
    If S0_M5_Data(1) = NAN Then
      Move (S0_M5_Data(), 2, NAN, 1)
    EndIf

    CallTable SapFlowAll
  NextScan
EndProg

--- FILE: generated_programs/cr300/sapflux_4sensor_CR300_30min.cr300 ---
' CR300 Series Datalogger Program
' Program to log 20 specified measurements from Implexx Sap Flow Sensors
' Generated by Python Script (cr300_generator.py)
' Number of Sensors: 4
' Measurement Interval: 30 minutes

'--- Constants ---
Const MEAST_INTERVAL_MIN = 30
Const SDI12_PORT = C1 ' Default SDI-12 Port (e.g., C1, C2)

'--- Declare Public Variables ---
Public PTemp_C As Float
Public Batt_volt As Float
Dim N_Loop ' Generic loop counter for error handling

Public S0_M_Data(9) As Float
Public S0_M1_Data(6) As Float
Public S0_M2_Data(6) As Float
Public S0_M5_Data(2) As Float
Public S1_M_Data(9) As Float
Public S1_M1_Data(6) As Float
Public S1_M2_Data(6) As Float
Public S1_M5_Data(2) As Float
Public S2_M_Data(9) As Float
Public S2_M1_Data(6) As Float
Public S2_M2_Data(6) As Float
Public S2_M5_Data(2) As Float
Public S3_M_Data(9) As Float
Public S3_M1_Data(6) As Float
Public S3_M2_Data(6) As Float
Public S3_M5_Data(2) As Float

'--- Alias Declarations (for logged variables) ---
Alias S0_M_Data(4) = S0_AlpOut : Units S0_AlpOut=ratio
Alias S0_M_Data(5) = S0_AlpInn : Units S0_AlpInn=ratio
Alias S0_M_Data(6) = S0_BetOut : Units S0_BetOut=ratio
Alias S0_M_Data(7) = S0_BetInn : Units S0_BetInn=ratio
Alias S0_M_Data(8) = S0_tMxTout : Units S0_tMxTout=sec
Alias S0_M_Data(9) = S0_tMxTinn : Units S0_tMxTinn=sec
Alias S0_M1_Data(1) = S0_TpDsOut : Units S0_TpDsOut=degC
Alias S0_M1_Data(2) = S0_dTDsOut : Units S0_dTDsOut=degC
Alias S0_M1_Data(3) = S0_TsDsOut : Units S0_TsDsOut=degC
Alias S0_M1_Data(4) = S0_TpUsOut : Units S0_TpUsOut=degC
Alias S0_M1_Data(5) = S0_dTUsOut : Units S0_dTUsOut=degC
Alias S0_M1_Data(6) = S0_TsUsOut : Units S0_TsUsOut=degC
Alias S0_M2_Data(1) = S0_TpDsInn : Units S0_TpDsInn=degC
Alias S0_M2_Data(2) = S0_dTDsInn : Units S0_dTDsInn=degC
Alias S0_M2_Data(3) = S0_TsDsInn : Units S0_TsDsInn=degC
Alias S0_M2_Data(4) = S0_TpUsInn : Units S0_TpUsInn=degC
Alias S0_M2_Data(5) = S0_dTUsInn : Units S0_dTUsInn=degC
Alias S0_M2_Data(6) = S0_TsUsInn : Units S0_TsUsInn=degC
Alias S0_M5_Data(1) = S0_tMxTUsO : Units S0_tMxTUsO=sec
Alias S0_M5_Data(2) = S0_tMxTUsI : Units S0_tMxTUsI=sec
Alias S1_M_Data(4) = S1_AlpOut : Units S1_AlpOut=ratio
Alias S1_M_Data(5) = S1_AlpInn : Units S1_AlpInn=ratio
Alias S1_M_Data(6) = S1_BetOut : Units S1_BetOut=ratio
Alias S1_M_Data(7) = S1_BetInn : Units S1_BetInn=ratio
Alias S1_M_Data(8) = S1_tMxTout : Units S1_tMxTout=sec
Alias S1_M_Data(9) = S1_tMxTinn : Units S1_tMxTinn=sec
Alias S1_M1_Data(1) = S1_TpDsOut : Units S1_TpDsOut=degC
Alias S1_M1_Data(2) = S1_dTDsOut : Units S1_dTDsOut=degC
Alias S1_M1_Data(3) = S1_TsDsOut : Units S1_TsDsOut=degC
Alias S1_M1_Data(4) = S1_TpUsOut : Units S1_TpUsOut=degC
Alias S1_M1_Data(5) = S1_dTUsOut : Units S1_dTUsOut=degC
Alias S1_M1_Data(6) = S1_TsUsOut : Units S1_TsUsOut=degC
Alias S1_M2_Data(1) = S1_TpDsInn : Units S1_TpDsInn=degC
Alias S1_M2_Data(2) = S1_dTDsInn : Units S1_dTDsInn=degC
Alias S1_M2_Data(3) = S1_TsDsInn : Units S1_TsDsInn=degC
Alias S1_M2_Data(4) = S1_TpUsInn : Units S1_TpUsInn=degC
Alias S1_M2_Data(5) = S1_dTUsInn : Units S1_dTUsInn=degC
Alias S1_M2_Data(6) = S1_TsUsInn : Units S1_TsUsInn=degC
Alias S1_M5_Data(1) = S1_tMxTUsO : Units S1_tMxTUsO=sec
Alias S1_M5_Data(2) = S1_tMxTUsI : Units S1_tMxTUsI=sec
Alias S2_M_Data(4) = S2_AlpOut : Units S2_AlpOut=ratio
Alias S2_M_Data(5) = S2_AlpInn : Units S2_AlpInn=ratio
Alias S2_M_Data(6) = S2_BetOut : Units S2_BetOut=ratio
Alias S2_M_Data(7) = S2_BetInn : Units S2_BetInn=ratio
Alias S2_M_Data(8) = S2_tMxTout : Units S2_tMxTout=sec
Alias S2_M_Data(9) = S2_tMxTinn : Units S2_tMxTinn=sec
Alias S2_M1_Data(1) = S2_TpDsOut : Units S2_TpDsOut=degC
Alias S2_M1_Data(2) = S2_dTDsOut : Units S2_dTDsOut=degC
Alias S2_M1_Data(3) = S2_TsDsOut : Units S2_TsDsOut=degC
Alias S2_M1_Data(4) = S2_TpUsOut : Units S2_TpUsOut=degC
Alias S2_M1_Data(5) = S2_dTUsOut : Units S2_dTUsOut=degC
Alias S2_M1_Data(6) = S2_TsUsOut : Units S2_TsUsOut=degC
Alias S2_M2_Data(1) = S2_TpDsInn : Units S2_TpDsInn=degC
Alias S2_M2_Data(2) = S2_dTDsInn : Units S2_dTDsInn=degC
Alias S2_M2_Data(3) = S2_TsDsInn : Units S2_TsDsInn=degC
Alias S2_M2_Data(4) = S2_TpUsInn : Units S2_TpUsInn=degC
Alias S2_M2_Data(5) = S2_dTUsInn : Units S2_dTUsInn=degC
Alias S2_M2_Data(6) = S2_TsUsInn : Units S2_TsUsInn=degC
Alias S2_M5_Data(1) = S2_tMxTUsO : Units S2_tMxTUsO=sec
Alias S2_M5_Data(2) = S2_tMxTUsI : Units S2_tMxTUsI=sec
Alias S3_M_Data(4) = S3_AlpOut : Units S3_AlpOut=ratio
Alias S3_M_Data(5) = S3_AlpInn : Units S3_AlpInn=ratio
Alias S3_M_Data(6) = S3_BetOut : Units S3_BetOut=ratio
Alias S3_M_Data(7) = S3_BetInn : Units S3_BetInn=ratio
Alias S3_M_Data(8) = S3_tMxTout : Units S3_tMxTout=sec
Alias S3_M_Data(9) = S3_tMxTinn : Units S3_tMxTinn=sec
Alias S3_M1_Data(1) = S3_TpDsOut : Units S3_TpDsOut=degC
Alias S3_M1_Data(2) = S3_dTDsOut : Units S3_dTDsOut=degC
Alias S3_M1_Data(3) = S3_TsDsOut : Units S3_TsDsOut=degC
Alias S3_M1_Data(4) = S3_TpUsOut : Units S3_TpUsOut=degC
Alias S3_M1_Data(5) = S3_dTUsOut : Units S3_dTUsOut=degC
Alias S3_M1_Data(6) = S3_TsUsOut : Units S3_TsUsOut=degC
Alias S3_M2_Data(1) = S3_TpDsInn : Units S3_TpDsInn=degC
Alias S3_M2_Data(2) = S3_dTDsInn : Units S3_dTDsInn=degC
Alias S3_M2_Data(3) = S3_TsDsInn : Units S3_TsDsInn=degC
Alias S3_M2_Data(4) = S3_TpUsInn : Units S3_TpUsInn=degC
Alias S3_M2_Data(5) = S3_dTUsInn : Units S3_dTUsInn=degC
Alias S3_M2_Data(6) = S3_TsUsInn : Units S3_TsUsInn=degC
Alias S3_M5_Data(1) = S3_tMxTUsO : Units S3_tMxTUsO=sec
Alias S3_M5_Data(2) = S3_tMxTUsI : Units S3_tMxTUsI=sec

'--- DataTable Definition (Single Table for All Sensors) ---
DataTable (SapFlowAll, True, -1)
  DataInterval (0, MEAST_INTERVAL_MIN, Min, 0) ' No output delay
  Sample (1, Batt_volt, FP2)
  Sample (1, PTemp_C, FP2)
  Sample (1, S0_AlpOut, IEEE4)
  Sample (1, S0_AlpInn, IEEE4)
  Sample (1, S0_BetOut, IEEE4)
  Sample (1, S0_BetInn, IEEE4)
  Sample (1, S0_tMxTout, IEEE4)
  Sample (1, S0_tMxTinn, IEEE4)
  Sample (1, S0_TpDsOut, IEEE4)
  Sample (1, S0_dTDsOut, IEEE4)
  Sample (1, S0_TsDsOut, IEEE4)
  Sample (1, S0_TpUsOut, IEEE4)
  Sample (1, S0_dTUsOut, IEEE4)
  Sample (1, S0_TsUsOut, IEEE4)
  Sample (1, S0_TpDsInn, IEEE4)
  Sample (1, S0_dTDsInn, IEEE4)
  Sample (1, S0_TsDsInn, IEEE4)
  Sample (1, S0_TpUsInn, IEEE4)
  Sample (1, S0_dTUsInn, IEEE4)
  Sample (1, S0_TsUsInn, IEEE4)
  Sample (1, S0_tMxTUsO, IEEE4)
  Sample (1, S0_tMxTUsI, IEEE4)
  Sample (1, S1_AlpOut, IEEE4)
  Sample (1, S1_AlpInn, IEEE4)
  Sample (1, S1_BetOut, IEEE4)
  Sample (1, S1_BetInn, IEEE4)
  Sample (1, S1_tMxTout, IEEE4)
  Sample (1, S1_tMxTinn, IEEE4)
  Sample (1, S1_TpDsOut, IEEE4)
  Sample (1, S1_dTDsOut, IEEE4)
  Sample (1, S1_TsDsOut, IEEE4)
  Sample (1, S1_TpUsOut, IEEE4)
  Sample (1, S1_dTUsOut, IEEE4)
  Sample (1, S1_TsUsOut, IEEE4)
  Sample (1, S1_TpDsInn, IEEE4)
  Sample (1, S1_dTDsInn, IEEE4)
  Sample (1, S1_TsDsInn, IEEE4)
  Sample (1, S1_TpUsInn, IEEE4)
  Sample (1, S1_dTUsInn, IEEE4)
  Sample (1, S1_TsUsInn, IEEE4)
  Sample (1, S1_tMxTUsO, IEEE4)
  Sample (1, S1_tMxTUsI, IEEE4)
  Sample (1, S2_AlpOut, IEEE4)
  Sample (1, S2_AlpInn, IEEE4)
  Sample (1, S2_BetOut, IEEE4)
  Sample (1, S2_BetInn, IEEE4)
  Sample (1, S2_tMxTout, IEEE4)
  Sample (1, S2_tMxTinn, IEEE4)
  Sample (1, S2_TpDsOut, IEEE4)
  Sample (1, S2_dTDsOut, IEEE4)
  Sample (1, S2_TsDsOut, IEEE4)
  Sample (1, S2_TpUsOut, IEEE4)
  Sample (1, S2_dTUsOut, IEEE4)
  Sample (1, S2_TsUsOut, IEEE4)
  Sample (1, S2_TpDsInn, IEEE4)
  Sample (1, S2_dTDsInn, IEEE4)
  Sample (1, S2_TsDsInn, IEEE4)
  Sample (1, S2_TpUsInn, IEEE4)
  Sample (1, S2_dTUsInn, IEEE4)
  Sample (1, S2_TsUsInn, IEEE4)
  Sample (1, S2_tMxTUsO, IEEE4)
  Sample (1, S2_tMxTUsI, IEEE4)
  Sample (1, S3_AlpOut, IEEE4)
  Sample (1, S3_AlpInn, IEEE4)
  Sample (1, S3_BetOut, IEEE4)
  Sample (1, S3_BetInn, IEEE4)
  Sample (1, S3_tMxTout, IEEE4)
  Sample (1, S3_tMxTinn, IEEE4)
  Sample (1, S3_TpDsOut, IEEE4)
  Sample (1, S3_dTDsOut, IEEE4)
  Sample (1, S3_TsDsOut, IEEE4)
  Sample (1, S3_TpUsOut, IEEE4)
  Sample (1, S3_dTUsOut, IEEE4)
  Sample (1, S3_TsUsOut, IEEE4)
  Sample (1, S3_TpDsInn, IEEE4)
  Sample (1, S3_dTDsInn, IEEE4)
  Sample (1, S3_TsDsInn, IEEE4)
  Sample (1, S3_TpUsInn, IEEE4)
  Sample (1, S3_dTUsInn, IEEE4)
  Sample (1, S3_TsUsInn, IEEE4)
  Sample (1, S3_tMxTUsO, IEEE4)
  Sample (1, S3_tMxTUsI, IEEE4)
EndTable

'--- Main Program ---
BeginProg
  Scan (MEAST_INTERVAL_MIN, Min, 1, 0) ' Scan interval, units, buffer=1, count=0 (continuous)
    PanelTemp (PTemp_C, 60) ' Defaulting to 60Hz fnotch, or use PanelTemp(PTemp_C)
    Battery (Batt_volt)

    ' Initialize all sensor data variables to NAN
    S0_AlpOut = NAN
    S0_AlpInn = NAN
    S0_BetOut = NAN
    S0_BetInn = NAN
    S0_tMxTout = NAN
    S0_tMxTinn = NAN
    S0_TpDsOut = NAN
    S0_dTDsOut = NAN
    S0_TsDsOut = NAN
    S0_TpUsOut = NAN
    S0_dTUsOut = NAN
    S0_TsUsOut = NAN
    S0_TpDsInn = NAN
    S0_dTDsInn = NAN
    S0_TsDsInn = NAN
    S0_TpUsInn = NAN
    S0_dTUsInn = NAN
    S0_TsUsInn = NAN
    S0_tMxTUsO = NAN
    S0_tMxTUsI = NAN
    S1_AlpOut = NAN
    S1_AlpInn = NAN
    S1_BetOut = NAN
    S1_BetInn = NAN
    S1_tMxTout = NAN
    S1_tMxTinn = NAN
    S1_TpDsOut = NAN
    S1_dTDsOut = NAN
    S1_TsDsOut = NAN
    S1_TpUsOut = NAN
    S1_dTUsOut = NAN
    S1_TsUsOut = NAN
    S1_TpDsInn = NAN
    S1_dTDsInn = NAN
    S1_TsDsInn = NAN
    S1_TpUsInn = NAN
    S1_dTUsInn = NAN
    S1_TsUsInn = NAN
    S1_tMxTUsO = NAN
    S1_tMxTUsI = NAN
    S2_AlpOut = NAN
    S2_AlpInn = NAN
    S2_BetOut = NAN
    S2_BetInn = NAN
    S2_tMxTout = NAN
    S2_tMxTinn = NAN
    S2_TpDsOut = NAN
    S2_dTDsOut = NAN
    S2_TsDsOut = NAN
    S2_TpUsOut = NAN
    S2_dTUsOut = NAN
    S2_TsUsOut = NAN
    S2_TpDsInn = NAN
    S2_dTDsInn = NAN
    S2_TsDsInn = NAN
    S2_TpUsInn = NAN
    S2_dTUsInn = NAN
    S2_TsUsInn = NAN
    S2_tMxTUsO = NAN
    S2_tMxTUsI = NAN
    S3_AlpOut = NAN
    S3_AlpInn = NAN
    S3_BetOut = NAN
    S3_BetInn = NAN
    S3_tMxTout = NAN
    S3_tMxTinn = NAN
    S3_TpDsOut = NAN
    S3_dTDsOut = NAN
    S3_TsDsOut = NAN
    S3_TpUsOut = NAN
    S3_dTUsOut = NAN
    S3_TsUsOut = NAN
    S3_TpDsInn = NAN
    S3_dTDsInn = NAN
    S3_TsDsInn = NAN
    S3_TpUsInn = NAN
    S3_dTUsInn = NAN
    S3_TsUsInn = NAN
    S3_tMxTUsO = NAN
    S3_tMxTUsI = NAN

    ' --- Sensor 0 (Address "0") ---
    SDI12Recorder(S0_M_Data(), SDI12_PORT, "0", "M!", 1.0, 0, -1)
    If S0_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S0_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S0_M1_Data(), SDI12_PORT, "0", "M1!", 1.0, 0, -1)
    If S0_M1_Data(1) = NAN Then
      Move (S0_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S0_M2_Data(), SDI12_PORT, "0", "M2!", 1.0, 0, -1)
    If S0_M2_Data(1) = NAN Then
      Move (S0_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S0_M5_Data(), SDI12_PORT, "0", "M5!", 1.0, 0, -1)
    If S0_M5_Data(1) = NAN Then
      Move (S0_M5_Data(), 2, NAN, 1)
    EndIf

    ' --- Sensor 1 (Address "1") ---
    SDI12Recorder(S1_M_Data(), SDI12_PORT, "1", "M!", 1.0, 0, -1)
    If S1_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S1_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S1_M1_Data(), SDI12_PORT, "1", "M1!", 1.0, 0, -1)
    If S1_M1_Data(1) = NAN Then
      Move (S1_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S1_M2_Data(), SDI12_PORT, "1", "M2!", 1.0, 0, -1)
    If S1_M2_Data(1) = NAN Then
      Move (S1_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S1_M5_Data(), SDI12_PORT, "1", "M5!", 1.0, 0, -1)
    If S1_M5_Data(1) = NAN Then
      Move (S1_M5_Data(), 2, NAN, 1)
    EndIf

    ' --- Sensor 2 (Address "2") ---
    SDI12Recorder(S2_M_Data(), SDI12_PORT, "2", "M!", 1.0, 0, -1)
    If S2_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S2_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S2_M1_Data(), SDI12_PORT, "2", "M1!", 1.0, 0, -1)
    If S2_M1_Data(1) = NAN Then
      Move (S2_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S2_M2_Data(), SDI12_PORT, "2", "M2!", 1.0, 0, -1)
    If S2_M2_Data(1) = NAN Then
      Move (S2_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S2_M5_Data(), SDI12_PORT, "2", "M5!", 1.0, 0, -1)
    If S2_M5_Data(1) = NAN Then
      Move (S2_M5_Data(), 2, NAN, 1)
    EndIf

    ' --- Sensor 3 (Address "3") ---
    SDI12Recorder(S3_M_Data(), SDI12_PORT, "3", "M!", 1.0, 0, -1)
    If S3_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S3_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S3_M1_Data(), SDI12_PORT, "3", "M1!", 1.0, 0, -1)
    If S3_M1_Data(1) = NAN Then
      Move (S3_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S3_M2_Data(), SDI12_PORT, "3", "M2!", 1.0, 0, -1)
    If S3_M2_Data(1) = NAN Then
      Move (S3_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S3_M5_Data(), SDI12_PORT, "3", "M5!", 1.0, 0, -1)
    If S3_M5_Data(1) = NAN Then
      Move (S3_M5_Data(), 2, NAN, 1)
    EndIf

    CallTable SapFlowAll
  NextScan
EndProg

--- FILE: src/__init__.py ---

--- FILE: src/cr300_generator.py ---
# src/cr300_generator.py

import sys

# Constants specific to CR300 generation
MAX_SDI12_SENSORS_CR300 = 62
MIN_MEASURE_INTERVAL_MINUTES_IMPLEX = 10 # Implexx general recommendation

# This is the function that main.py will call
def generate_code(num_sensors, measure_interval_min, **kwargs):
    """
    Generates CRBasic code for CR300 series dataloggers to read
    20 specified measurements from multiple Implexx sap flow sensors.
    Uses a single data table for all sensors and the abstracted 7-parameter
    SDI12Recorder syntax found in CR300 Short Cut examples.

    Args:
        num_sensors (int): The number of sensors.
        measure_interval_min (int): The measurement interval in minutes.
        **kwargs: For future expansion if needed.

    Returns:
        str: The generated CRBasic program as a string, or an error string.
    """

    # --- Module-specific Validation ---
    if not (1 <= num_sensors <= MAX_SDI12_SENSORS_CR300):
        return (f"' Error in cr300_generator: Number of sensors must be between 1 and "
                f"{MAX_SDI12_SENSORS_CR300}.")
    if measure_interval_min < MIN_MEASURE_INTERVAL_MINUTES_IMPLEX:
        return (f"' Error in cr300_generator: Implexx sensors recommend a measurement interval of at least "
                f"{MIN_MEASURE_INTERVAL_MINUTES_IMPLEX} minutes for reliable data. Requested: {measure_interval_min} min.")

    # Define the structure for the measurements we want to extract.
    # This will help in creating aliases and assigning data.
    # (alias_suffix, unit, source_sdi_command, array_size_for_sdi_command, index_in_that_array)
    # Note: CRBasic array access is 1-indexed.
    # The SDI12Recorder for "M!" in CR300 example populates 9 values (D0 & D1 combined)
    # The SDI12Recorder for "M1!"/"M2!" should populate 6 values (D0)
    # The SDI12Recorder for "M5!" should populate 2 values (D0)

    # Base names for the Public arrays that SDI12Recorder will populate
    # Suffixes for aliased variables (will become FieldNames)
    # Units for the aliased variables
    # Source SDI Command (M!, M1!, M2!, M5!)
    # Size of the array that the SDI12Recorder call for that command will populate
    # 1-based index within that populated array for this specific data point

    desired_measurements_config = [
        # From M! (which implicitly gets D0 and D1, total 9 values)
        ("AlpOut", "ratio", "M!", 9, 4),  # AlphaOuter is 4th of 9 (D0's 4th)
        ("AlpInn", "ratio", "M!", 9, 5),  # AlphaInner is 5th of 9 (D0's 5th)
        ("BetOut", "ratio", "M!", 9, 6),  # BetaOuter is 6th of 9 (D1's 1st)
        ("BetInn", "ratio", "M!", 9, 7),  # BetaInner is 7th of 9 (D1's 2nd)
        ("tMxTout", "sec", "M!", 9, 8),   # tMaxTouter is 8th of 9 (D1's 3rd)
        ("tMxTinn", "sec", "M!", 9, 9),   # tMaxTinner is 9th of 9 (D1's 4th)
        # From M1! (implicitly gets D0, total 6 values)
        ("TpDsOut", "degC", "M1!", 6, 1), # TpreDsOuter
        ("dTDsOut", "degC", "M1!", 6, 2), # dTmaxDsOuter
        ("TsDsOut", "degC", "M1!", 6, 3), # TpostDsOuter
        ("TpUsOut", "degC", "M1!", 6, 4), # TpreUsOuter
        ("dTUsOut", "degC", "M1!", 6, 5), # dTmaxUsOuter
        ("TsUsOut", "degC", "M1!", 6, 6), # TpostUsOuter
        # From M2! (implicitly gets D0, total 6 values)
        ("TpDsInn", "degC", "M2!", 6, 1), # TpreDsInner
        ("dTDsInn", "degC", "M2!", 6, 2), # dTmaxDsInner
        ("TsDsInn", "degC", "M2!", 6, 3), # TpostDsInner
        ("TpUsInn", "degC", "M2!", 6, 4), # TpreUsInner
        ("dTUsInn", "degC", "M2!", 6, 5), # dTmaxUsInner
        ("TsUsInn", "degC", "M2!", 6, 6), # TpostUsInner
        # From M5! (implicitly gets D0, total 2 values)
        ("tMxTUsO", "sec", "M5!", 2, 1),  # tMaxTusOuter
        ("tMxTUsI", "sec", "M5!", 2, 2)   # tMaxTusInner
    ]

# Helper to get SDI-12 address character (0-9, a-z, A-Z)
    def get_sdi12_address_char(index):
        if 0 <= index <= 9: return str(index)
        elif 10 <= index <= 35: return chr(ord('a') + (index - 10))
        elif 36 <= index <= 61: return chr(ord('A') + (index - 36))
        raise ValueError("Sensor index out of SDI-12 addressable range (0-61)")

    crbasic_code_list = []

    # --- File Header ---
    crbasic_code_list.append("' CR300 Series Datalogger Program")
    crbasic_code_list.append("' Program to log 20 specified measurements from Implexx Sap Flow Sensors")
    crbasic_code_list.append("' Generated by Python Script (cr300_generator.py)")
    crbasic_code_list.append(f"' Number of Sensors: {num_sensors}")
    crbasic_code_list.append(f"' Measurement Interval: {measure_interval_min} minutes")
    crbasic_code_list.append("")

    # --- Constants ---
    crbasic_code_list.append("'--- Constants ---")
    crbasic_code_list.append(f"Const MEAST_INTERVAL_MIN = {measure_interval_min}")
    crbasic_code_list.append(f"Const SDI12_PORT = C1 ' Default SDI-12 Port (e.g., C1, C2)")
    crbasic_code_list.append("")

    # --- Declare Public Variables ---
    crbasic_code_list.append("'--- Declare Public Variables ---")
    crbasic_code_list.append("Public PTemp_C As Float") # Renamed from PTemp for clarity
    crbasic_code_list.append("Public Batt_volt As Float")
    crbasic_code_list.append("Dim N_Loop ' Generic loop counter for error handling") # Single loop counter
    crbasic_code_list.append("")

    # Declare Public arrays for SDI12Recorder for each command type and sensor
    # These arrays will be populated by the abstracted SDI12Recorder calls
    # Example: S0_M_Data(9), S0_M1_Data(6), S0_M2_Data(6), S0_M5_Data(2)
    sdi_command_array_map = {} # To store array names like "S0_M_Data"
    for i in range(num_sensors):
        sdi_char = get_sdi12_address_char(i)
        # Create unique arrays for each sensor and each type of M command that returns data
        for sdi_cmd_base in ["M", "M1", "M2", "M5"]: # "M" implies M!
            # Determine array size based on the first config entry for that command
            array_size = 0
            for _, _, cmd_conf, size_conf, _ in desired_measurements_config:
                if cmd_conf == sdi_cmd_base + "!": # Match M!, M1!, etc.
                    array_size = size_conf
                    break
            if array_size > 0:
                array_name = f"S{sdi_char}_{sdi_cmd_base}_Data" # e.g., S0_M_Data, Sa_M1_Data
                sdi_command_array_map[(i, sdi_cmd_base + "!")] = array_name
                crbasic_code_list.append(f"Public {array_name}({array_size}) As Float")
    crbasic_code_list.append("")

    # Declare Aliases for the specific data points we want to log
    # These Aliases will point into the S{sdi_char}_{sdi_cmd_base}_Data arrays
    crbasic_code_list.append("'--- Alias Declarations (for logged variables) ---")
    all_data_table_vars = [] # For defining the DataTable
    for i in range(num_sensors):
        sdi_char = get_sdi12_address_char(i)
        for alias_suffix, unit, sdi_cmd, _, index_in_sdi_array in desired_measurements_config:
            source_array_name = sdi_command_array_map[(i, sdi_cmd)]
            final_var_name = f"S{sdi_char}_{alias_suffix}" # e.g., S0_AlpOut
            crbasic_code_list.append(f"Alias {source_array_name}({index_in_sdi_array}) = {final_var_name} : Units {final_var_name}={unit}")
            all_data_table_vars.append(final_var_name)
    crbasic_code_list.append("")

# --- DataTable Definition ---
    crbasic_code_list.append("'--- DataTable Definition (Single Table for All Sensors) ---")
    crbasic_code_list.append("DataTable (SapFlowAll, True, -1)")
    crbasic_code_list.append(f"  DataInterval (0, MEAST_INTERVAL_MIN, Min, 0) ' No output delay")
    crbasic_code_list.append("  Sample (1, Batt_volt, FP2)")
    crbasic_code_list.append("  Sample (1, PTemp_C, FP2)")
    for var_name in all_data_table_vars:
        crbasic_code_list.append(f"  Sample (1, {var_name}, IEEE4)") # Use IEEE4 for float precision
    crbasic_code_list.append("EndTable")
    crbasic_code_list.append("")

    # --- Main Program ---
    crbasic_code_list.append("'--- Main Program ---")
    # CR300 example does not use SequentialMode, relies on SDI12Recorder blocking.
    # Let's try without it first, matching the new CR300 Short Cut example.
    # If timing issues arise, SequentialMode can be added.
    crbasic_code_list.append("BeginProg")
    crbasic_code_list.append(f"  Scan (MEAST_INTERVAL_MIN, Min, 1, 0) ' Scan interval, units, buffer=1, count=0 (continuous)")
    crbasic_code_list.append("    PanelTemp (PTemp_C, 60) ' Defaulting to 60Hz fnotch, or use PanelTemp(PTemp_C)")
    crbasic_code_list.append("    Battery (Batt_volt)")
    crbasic_code_list.append("")

    # Initialize all aliased data variables to NAN at the start of each scan
    crbasic_code_list.append("    ' Initialize all sensor data variables to NAN")
    for var_name in all_data_table_vars:
        crbasic_code_list.append(f"    {var_name} = NAN")
    crbasic_code_list.append("")

    for i in range(num_sensors):
        sdi_char = get_sdi12_address_char(i)
        crbasic_code_list.append(f"    ' --- Sensor {sdi_char} (Address \"{sdi_char}\") ---")

        # --- Standard Measurement (M!) ---
        # This call is assumed to be blocking for ~100s and retrieve all 9 values
        sdi_cmd_m = "M!"
        array_name_m = sdi_command_array_map[(i, sdi_cmd_m)]
        num_values_m = 9 # For M! + D0/D1
        crbasic_code_list.append(f"    SDI12Recorder({array_name_m}(), SDI12_PORT, \"{sdi_char}\", \"{sdi_cmd_m}\", 1.0, 0, -1)")
        crbasic_code_list.append(f"    If {array_name_m}(1) = NAN Then ' Check if first value is NAN (measurement failed)")
        crbasic_code_list.append(f"      Move ({array_name_m}(), {num_values_m}, NAN, 1) ' Set all elements of this array to NAN")
        crbasic_code_list.append(f"    EndIf")
        crbasic_code_list.append("")

        # --- Additional Measurements (M1!, M2!, M5!) ---
        # These are assumed to be quick, blocking calls retrieving their respective data
        for sdi_cmd_base in ["M1", "M2", "M5"]:
            sdi_cmd_add = sdi_cmd_base + "!"
            array_name_add = sdi_command_array_map[(i, sdi_cmd_add)]
            num_values_add = 0
            for _, _, cmd_conf, size_conf, _ in desired_measurements_config:
                if cmd_conf == sdi_cmd_add:
                    num_values_add = size_conf
                    break

            crbasic_code_list.append(f"    ' {sdi_cmd_add} Measurement")
            crbasic_code_list.append(f"    SDI12Recorder({array_name_add}(), SDI12_PORT, \"{sdi_char}\", \"{sdi_cmd_add}\", 1.0, 0, -1)")
            crbasic_code_list.append(f"    If {array_name_add}(1) = NAN Then")
            crbasic_code_list.append(f"      Move ({array_name_add}(), {num_values_add}, NAN, 1)")
            crbasic_code_list.append(f"    EndIf")
            crbasic_code_list.append("")

    crbasic_code_list.append("    CallTable SapFlowAll")
    crbasic_code_list.append("  NextScan")
    crbasic_code_list.append("EndProg")

    return "\n".join(crbasic_code_list)

# Optional: Add a section for direct testing of this module
if __name__ == "__main__":
    print("--- Testing cr300_generator.py directly ---")
    # Test case 1: Valid input
    print("\n--- Test Case 1: 1 sensor, 15 min interval ---")
    test_code_1 = generate_code(num_sensors=1, measure_interval_min=15)
    if "' Error:" in test_code_1:
        print(f"Error in generation: {test_code_1}")
    else:
        print(test_code_1)
        # To fully test, save to .cr3 and compile with CR300 compiler
        # with open("temp_cr300_generated.cr3", "w") as f:
        #     f.write(test_code_1)
        # print("\nCR300 code generated to temp_cr300_generated.cr3")

    print("\n--- Test Case 2: Sensor index requiring char 'a' (sensor 11, index 10) ---")
    test_code_11_sensors = generate_code(num_sensors=11, measure_interval_min=15)
    if "' Error:" in test_code_11_sensors:
         print(f"Error in generation: {test_code_11_sensors}")
    else:
        # print(f"\nGenerated code for 11 sensors (showing part for sensor 'a'):")
        # for line in test_code_11_sensors.splitlines():
        #     if "Sa_" in line or "Sensor a" in line: # Quick check
        #         print(line)
        pass # Full printout is too long for quick check

    print("\n--- Test Case 3: Interval too short (e.g., 5 min) ---")
    test_code_interval_fail = generate_code(num_sensors=1, measure_interval_min=5)
    if "' Error:" in test_code_interval_fail:
        print(f"Expected Error: {test_code_interval_fail}")
    else:
        print("Error: Validation for interval too short failed for CR300 in module.")
        print(test_code_interval_fail)

print("\n--- Direct module testing complete ---")

--- FILE: uv.lock ---
version = 1
revision = 2
requires-python = ">=3.13"

[[package]]
name = "sapflux-programs"
version = "0.1.0"
source = { virtual = "." }

--- FILE: README.md ---
# Sapflux Programs

This repository contains Python scripts designed to generate CRBasic firmware for running Implexx sap flux sensors on Campbell Scientific CR200-series and CR300-series dataloggers.

There are many jailable software crimes committed in this repository.

## Motivations & Goals

This project was born out of a desire to:

1.  **Modernize Development:** Write datalogger code using my standard development environment and tools, rather than being confined to ancient, proprietary, Windows-only software.
2.  **Comprehensively Log Data:** Capture all relevant measurements from the Implexx sap flux sensors.
3.  **Scale Up:** Create CRBasic programs that can be easily adapted for an arbitrary number of sensors (within datalogger limits). The Campbell "Short Cut" program refused many of my demands, even though they were well within sensor/logger capabilities.

## Challenges & Workarounds

Working with older datalogger platforms presents unique challenges:

*   **Development Environment:** To address the first goal, I developed [crbrs](https://github.com/RileyLeff/crbrs) (available on [crates.io](https://crates.io/crates/crbrs)). This tool allows you to install and use Campbell Scientific compilers (hosted [here](https://github.com/RileyLeff/campbell-scientific-compilers)) on macOS, Linux, or Windows, bypassing the need for Campbell's traditional software suite.
*   **CR200X Data Table Limitations:**
    *   **Field Limit:** CR200-series dataloggers impose a strict limit of **16 columns (fields)** per data table. This is a significant constraint when aiming to log comprehensive data (e.g., the 20+ desired measurements from each Implexx sensor plus metadata).
    *   **Table Limit:** There's also a limit on the total number of data tables (often 8).
    *   **Workaround:** For the CR200X, the generator script creates **one data table per sensor**. This means each table contains the metadata and the 9 standard Implexx measurements for that specific sensor, fitting within the 16-field limit. Consequently, with this strategy, the CR200X can support a maximum of 8 sensors. This approach, while necessary, can make data management more cumbersome in the field due to multiple output files.
*   **CR300 Data Table Flexibility:** CR300-series dataloggers have much higher limits on fields per table and total tables. The CR300 generator script leverages this by creating a **single, comprehensive data table** for all sensors and all 20 desired Implexx measurements.
*   **Dynamic Code Generation:** CRBasic itself lacks features for dynamically generating table structures or measurement loops based on a variable number of sensors. To achieve scalability, this project uses Python scripts to dynamically generate the static CRBasic source code.

## Project Structure

The core of this project resides in the `src/` directory:

*   `src/main.py`: The main command-line interface (CLI) script. It parses arguments and calls the appropriate generator module.
*   `src/cr200x_generator.py`: A Python module containing the logic to generate CRBasic code specifically for CR200-series dataloggers.
*   `src/cr300_generator.py`: A Python module containing the logic to generate CRBasic code specifically for CR300-series dataloggers.
*   `generated_programs/`: A suggested directory to store the output `.cr2` or `.cr3` files.

## How-To Guide

To generate CRBasic firmware for your Implexx sap flux sensors:

1.  **Clone the Repository:**
    ```bash
    git clone git@github.com:RileyLeff/sapflux_programs.git sapflux_programs
    cd sapflux_programs
    ```

2.  **Prerequisites:**
    *   Python 3.x
    *   (Optional but Recommended) [UV](https://github.com/astral-sh/uv) for running Python scripts in a managed environment.
    *   (Optional but Recommended for CR200X compilation) [crbrs](https://github.com/RileyLeff/crbrs) installed if you want to compile the generated `.cr2` files from your command line.

3.  **Run the Generation Script:**
    The main script is `src/main.py`. You run it from the **project root directory**.

    **Command Structure:**
    ```bash
    # Using UV (recommended)
    uv run python -m src.main --logger-type <TYPE> -n <NUM_SENSORS> -t <INTERVAL_MIN> [-o <OUTPUT_FILE>]

    # Using system Python 3
    python3 -m src/main.py --logger-type <TYPE> -n <NUM_SENSORS> -t <INTERVAL_MIN> [-o <OUTPUT_FILE>]
    ```

    **Arguments:**
    *   `--logger-type <TYPE>`: **Required.** Specify the target datalogger.
        *   `CR200X`: For CR200-series dataloggers.
        *   `CR300`: For CR300-series dataloggers.
    *   `-n <NUM_SENSORS>`, `--num-sensors <NUM_SENSORS>`: **Required.** The number of sensors.
        *   For `CR200X`: 1 to 8 (due to table limits with the one-table-per-sensor strategy).
        *   For `CR300`: 1 to 62 (full SDI-12 address space).
    *   `-t <INTERVAL_MIN>`, `--measure-interval <INTERVAL_MIN>`: **Required.** The measurement interval in minutes.
        *   Minimum 15 minutes recommended for Implexx sensors to avoid overheating when using heating pulses (like the standard `M!` command). The CR300 script currently collects all 20 measurements, including those from `M!`. The CR200X script collects the 9 standard measurements from `M!`.
    *   `-o <OUTPUT_FILE>`, `--output <OUTPUT_FILE>`: **Optional.** File path to save the generated CRBasic code. If not provided, the code will be printed to standard output. It's recommended to use an appropriate extension (e.g., `.cr2` for CR200X, `.cr3` for CR300).

    **Examples:**
    ```bash
    # Generate code for 3 CR200X sensors, 30-min interval, save to file
    uv run python src/main.py --logger-type CR200X -n 3 -t 30 -o generated_programs/sapflux_3_cr200x.cr2

    # Generate code for 5 CR300 sensors, 15-min interval, print to console
    uv run python src/main.py --logger-type CR300 -n 5 -t 15
    ```

4.  **Compile (Optional, using `crbrs`):**
    If you have `crbrs` installed and configured, you can compile the generated file:
    ```bash
    crbrs compile generated_programs/your_generated_file.cr2 --compiler cr2comp
    # or
    crbrs compile generated_programs/your_generated_file.cr300 --compiler cr300comp
    ```
    Refer to the `crbrs` documentation for installation and configuration details. Otherwise, use Campbell Scientific's software to compile and upload the generated source code file.

## Current Status & Notes

*   **CR200X Generator:**
    *   Generates code to measure the **9 standard Implexx sap flux values** (from the `M!` command sequence).
    *   Creates **one data table per sensor** due to the 16-field-per-table limit.
    *   Supports up to 8 sensors due to the 8-table limit.
    *   The generated code has been tested and compiles successfully.
*   **CR300 Generator:**
    *   Generates code to measure all **20 desired Implexx sap flux values** (from `M!`, `M1!`, `M2!`, `M5!` command sequences).
    *   Creates a **single comprehensive data table** for all sensors.
    *   Supports up to 62 sensors.
    *   The generated code compiles successfully on the CR300. Real-world sensor data acquisition is currently being tested/validated.

Let me know if you have any questions big dog.

--- FILE: src/main.py ---
#!/usr/bin/env python3
# /// script
# dependencies:
# ///

import argparse
import sys
import importlib # For dynamic module importing

def main():
    parser = argparse.ArgumentParser(
        description="Generate CRBasic code for Implexx Sap Flow Sensors.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""\
Example usage:
  From project root:
  uv run python -m src.main --logger-type CR200X -n 2 -t 30 -o generated_programs/sapflux_cr200x.cr2
  uv run python src/main.py --logger-type CR300 -n 1 -t 15 # (If CWD is project root)

Notes:
  - CR200X generated code uses one DataTable per sensor.
  - CR300 generated code aims for a single comprehensive DataTable.
"""
    )

    # --- Required Arguments ---
    parser.add_argument(
        "--logger-type",
        type=str.upper,
        required=True,
        choices=["CR200X", "CR300"],
        help="Specify the target datalogger type (e.g., CR200X, CR300)."
    )
    parser.add_argument(
        "-n", "--num-sensors",
        type=int,
        required=True,
        help="The number of sensors (N)."
    )
    parser.add_argument(
        "-t", "--measure-interval",
        type=int,
        required=True,
        help="The measurement interval in minutes (T)."
    )

    # --- Optional Arguments ---
    parser.add_argument(
        "-o", "--output",
        metavar="FILENAME",
        type=str,
        help="Optional: Output filename for the generated CRBasic code."
    )

    args = parser.parse_args()

    # --- Basic Input Validation ---
    if args.num_sensors < 1:
        print("Error: Number of sensors must be at least 1.", file=sys.stderr)
        sys.exit(1)
    if args.measure_interval <= 0:
         print("Error: Measurement interval must be a positive integer.", file=sys.stderr)
         sys.exit(1)

    # --- Dynamically select and call the generator module ---
    generated_code = None
    generator_module_name_short = None # e.g., "cr200x_generator"
    generator_module_full_path = None  # e.g., "src.cr200x_generator"
    generator_kwargs = {}

    if args.logger_type == "CR200X":
        generator_module_name_short = "cr200x_generator"
        # MAX_SENSORS_CR200X_ONE_TABLE_PER_SENSOR is 8
        if not (1 <= args.num_sensors <= 8):
            print(f"Error: For CR200X (one table per sensor), "
                  f"number of sensors must be between 1 and 8. Requested: {args.num_sensors}",
                  file=sys.stderr)
            sys.exit(1)
        # MIN_MEASURE_INTERVAL_MINUTES_IMPLEX_CR200X is 15
        if args.measure_interval < 15:
             print(f"Warning: Implexx sensors on CR200X typically recommend a measurement interval of at least 15 minutes. "
                   f"Requested: {args.measure_interval} min.",
                   file=sys.stderr)

    elif args.logger_type == "CR300":
        generator_module_name_short = "cr300_generator"
        # MAX_SDI12_SENSORS_CR300 is 62
        if not (1 <= args.num_sensors <= 62):
            print(f"Error: Number of sensors for CR300 must be between 1 and 62. Requested: {args.num_sensors}",
                  file=sys.stderr)
            sys.exit(1)
        # MIN_MEASURE_INTERVAL_MINUTES_IMPLEX is 10
        if args.measure_interval < 10:
             print(f"Warning: Implexx sensors recommend a data collect interval of at least 10 minutes. "
                   f"Requested: {args.measure_interval} min.",
                   file=sys.stderr)
    else:
        print(f"Error: Unsupported logger type '{args.logger_type}'.", file=sys.stderr)
        sys.exit(1)

    # Construct the full module path assuming main.py is in 'src' and generators are siblings
    # This relies on 'src' or its parent being effectively on PYTHONPATH
    # which `python -m src.main` or `uv run python src/main.py` (from root) should handle.
    generator_module_full_path = f"src.{generator_module_name_short}"

    try:
        # Dynamically import the module
        module = importlib.import_module(generator_module_full_path)
        # Call the consistent function name 'generate_code'
        generated_code = module.generate_code(
            num_sensors=args.num_sensors,
            measure_interval_min=args.measure_interval,
            **generator_kwargs
        )
    except ImportError as e:
        print(f"Error: Could not import generator module '{generator_module_full_path}'. "
              f"Details: {e}\n"
              f"Ensure '{generator_module_name_short}.py' exists in the 'src' directory "
              f"and you are running from the project root (e.g., 'python -m src.main ...' "
              f"or 'uv run python src/main.py ...').", file=sys.stderr)
        sys.exit(1)
    except AttributeError:
        print(f"Error: The module '{generator_module_full_path}' does not have a 'generate_code' function.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error during code generation with '{generator_module_full_path}': {e}", file=sys.stderr)
        sys.exit(1)

# --- Output Handling ---
    if generated_code:
        if "' Error:" in generated_code: # Check if the generator function returned an error string
            print(f"Error from generator module:\n{generated_code}", file=sys.stderr)
            sys.exit(1)

        if args.output:
            try:
                # Ensure the output directory exists if specified in the path
                import os
                output_dir = os.path.dirname(args.output)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                with open(args.output, "w") as f:
                    f.write(generated_code)
                print(f"CRBasic code generated and saved to '{args.output}'.")
            except IOError as e:
                print(f"Error writing to file '{args.output}': {e}", file=sys.stderr)
                sys.exit(1)
        else:
            print(generated_code)
    else:
        print("Error: Code generation failed for an unknown reason.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

--- FILE: src/cr200x_generator.py ---
# src/cr200x_generator.py

# Constants specific to CR200X generation strategy
MAX_SENSORS_CR200X_ONE_TABLE_PER_SENSOR = 8
MIN_MEASURE_INTERVAL_MINUTES_IMPLEX_CR200X = 15

# This is the function that main.py will call
def generate_code(num_sensors, measure_interval_min, **kwargs):
    """
    Generates CRBasic code for CR200/CR200X dataloggers.
    """
    # --- Module-specific Validation ---
    if not (1 <= num_sensors <= MAX_SENSORS_CR200X_ONE_TABLE_PER_SENSOR):
        return (f"' Error in cr200x_generator: Number of sensors must be between 1 and "
                f"{MAX_SENSORS_CR200X_ONE_TABLE_PER_SENSOR} for CR200X with one table per sensor strategy.")
    # Note: Interval warning/error is handled by main_cli.py for consistency,
    # but could also be strictly enforced here.

    # --- Start of CR200X Generation Logic (from your previous working script) ---
    standard_measurements = [
        ("SapFlwTot", "literPerHour"), ("VhOuter", "heatVelocity"),
        ("VhInner", "heatVelocity"), ("AlphaOut", "logTRatio"),
        ("AlphaIn", "logTRatio"), ("BetaOut", "logTRatio"),
        ("BetaIn", "logTRatio"), ("tMaxTout", "second"),
        ("tMaxTin", "second"),
    ]

    def get_sdi12_address_char(index):
        if 0 <= index <= 7: return str(index)
        raise ValueError(f"Sensor index {index} out of range (0-7).") # Should be caught by main validation

    crbasic_code_list = []
    crbasic_code_list.append("' CR200/CR200X Series")
    crbasic_code_list.append("' Program to log standard data from Implexx Sap Flow Sensors")
    crbasic_code_list.append("' Generated by Python Script (cr200x_generator.py)")
    crbasic_code_list.append(f"' Number of Sensors: {num_sensors}")
    crbasic_code_list.append(f"' Measurement Interval: {measure_interval_min} minutes")
    crbasic_code_list.append("' NOTE: This program uses one DataTable per sensor.")
    crbasic_code_list.append(f"' CR200X supports a maximum of {MAX_SENSORS_CR200X_ONE_TABLE_PER_SENSOR} DataTables with this strategy.")
    crbasic_code_list.append("")

    crbasic_code_list.append("'--- Declare Variables and Units ---")
    if num_sensors == 1:
        crbasic_code_list.append("Dim N")
    else:
        for i in range(num_sensors):
            sdi_address_char = get_sdi12_address_char(i)
            crbasic_code_list.append(f"Dim N_{sdi_address_char} ' Loop counter for Sensor {sdi_address_char} error handling")
    crbasic_code_list.append("Public BattV")
    crbasic_code_list.append("Public id")
    for i in range(num_sensors):
        sdi_address_char = get_sdi12_address_char(i)
        crbasic_code_list.append(f"Public SDIData_Sensor{sdi_address_char}(9)")
        crbasic_code_list.append(f"Public SensorAddress{sdi_address_char}")

    crbasic_code_list.append("\n'--- Alias Declarations (Maps array elements to meaningful names) ---")
    for i in range(num_sensors):
        sdi_address_char = get_sdi12_address_char(i)
        for j in range(len(standard_measurements)):
            alias_name = f"{standard_measurements[j][0]}{sdi_address_char}"
            crbasic_code_list.append(f"Alias SDIData_Sensor{sdi_address_char}({j+1}) = {alias_name}")

    crbasic_code_list.append("\n'--- Units Declarations ---")
    crbasic_code_list.append("Units BattV=Volts")
    for i in range(num_sensors):
        sdi_address_char = get_sdi12_address_char(i)
        for j in range(len(standard_measurements)):
            alias_name = f"{standard_measurements[j][0]}{sdi_address_char}"
            unit = standard_measurements[j][1]
            crbasic_code_list.append(f"Units {alias_name}={unit}")

    crbasic_code_list.append("\n'--- Define Data Tables (One table per sensor due to CR200X field limit) ---")
    crbasic_code_list.append(f"' Note: CR200X dataloggers have a limit of 16 fields per table and {MAX_SENSORS_CR200X_ONE_TABLE_PER_SENSOR} tables total.")
    crbasic_code_list.append("' DataTable names must be <= 12 characters.")
    for i in range(num_sensors):
        sdi_address_char = get_sdi12_address_char(i)
        table_name = f"Table_S{sdi_address_char}"
        crbasic_code_list.append(f"DataTable({table_name},True,-1)")
        crbasic_code_list.append(f"\tDataInterval(0,{measure_interval_min},Min)")
        crbasic_code_list.append("\tMinimum(1,BattV,False,False)")
        crbasic_code_list.append("\tSample(1,id)")
        crbasic_code_list.append(f"\tSample(1,SensorAddress{sdi_address_char})")
        for j in range(len(standard_measurements)):
            alias_name = f"{standard_measurements[j][0]}{sdi_address_char}"
            crbasic_code_list.append(f"\tSample(1,{alias_name})")
        crbasic_code_list.append("EndTable\n")

    crbasic_code_list.append("\n'--- Main Program ---")
    crbasic_code_list.append("BeginProg")
    crbasic_code_list.append(f"\tScan({measure_interval_min},Min)")
    crbasic_code_list.append("\t\t'Default CR200 Series Datalogger Battery Voltage measurement 'BattV'")
    crbasic_code_list.append("\t\tBattery(BattV)")
    crbasic_code_list.append("\t\t'User Entered Calculation (from example)")
    crbasic_code_list.append("\t\tid = Status.PakBusAddress(1,1)")
    for i in range(num_sensors):
        sdi_address_char = get_sdi12_address_char(i)
        crbasic_code_list.append(f"\t\tSensorAddress{sdi_address_char} = {i}")
    crbasic_code_list.append("")

    for i in range(num_sensors):
        sdi_address_char_cmd = get_sdi12_address_char(i)
        sdi_address_char_var = get_sdi12_address_char(i)
        loop_counter = "N" if num_sensors == 1 else f"N_{sdi_address_char_var}"
        crbasic_code_list.append(f"\t\t' --- Collect standard data for Sensor {sdi_address_char_cmd} (Address \"{sdi_address_char_cmd}\") ---")
        crbasic_code_list.append(f"\t\tSDI12Recorder(SDIData_Sensor{sdi_address_char_var}(), \"{sdi_address_char_cmd}M!\", 1, 0)")
        crbasic_code_list.append(f"\t\t'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element")
        crbasic_code_list.append(f"\t\tIf SDIData_Sensor{sdi_address_char_var}(1) = NAN Then")
        crbasic_code_list.append(f"\t\t\tFor {loop_counter} = 1 To 9")
        crbasic_code_list.append(f"\t\t\t\tSDIData_Sensor{sdi_address_char_var}({loop_counter}) = NAN")
        crbasic_code_list.append(f"\t\t\tNext")
        crbasic_code_list.append(f"\t\tEndIf\n")

    crbasic_code_list.append("\t\t'Call Data Tables and Store Data")
    for i in range(num_sensors):
        sdi_address_char = get_sdi12_address_char(i)
        table_name = f"Table_S{sdi_address_char}"
        crbasic_code_list.append(f"\t\tCallTable {table_name}")

    crbasic_code_list.append("\tNextScan")
    crbasic_code_list.append("EndProg")

    return "\n".join(crbasic_code_list)

# Optional: Direct testing block (as before)
if __name__ == "__main__":
    print("--- Testing cr200x_generator.py directly ---")
    test_code = generate_code(num_sensors=1, measure_interval_min=30)
    print(test_code)

--- FILE: generated_programs/other/test_m6.cr300 ---
'CR300/CR310
'Test Program for Implexx Sensor M6 Command (Current Temperatures)

'Declare Variables and Units
Public BattV
Public PTemp_C

Public M6_Sensor0(4) ' For TdsO, TusO, TdsI, TusI from sensor 0

Alias M6_Sensor0(1) = S0_TdsO
Alias M6_Sensor0(2) = S0_TusO
Alias M6_Sensor0(3) = S0_TdsI
Alias M6_Sensor0(4) = S0_TusI

Units BattV=Volts
Units PTemp_C=Deg C
Units S0_TdsO=Deg C
Units S0_TusO=Deg C
Units S0_TdsI=Deg C
Units S0_TusI=Deg C

'Define Data Tables
DataTable(TestM6Data,True,-1)
	DataInterval(0,30,Sec,0) ' Log every 30 seconds, no output delay
	Minimum(1,BattV,FP2,False,False)
	Sample(1,PTemp_C,FP2)
	Sample(1,S0_TdsO,FP2)
	Sample(1,S0_TusO,FP2)
	Sample(1,S0_TdsI,FP2)
	Sample(1,S0_TusI,FP2)
EndTable

'Main Program
BeginProg
    Dim N ' Loop counter for error handling

	'Main Scan
	Scan(30,Sec,1,0) ' Scan every 30 seconds
		Battery(BattV)
		PanelTemp(PTemp_C,60) ' Assuming 60Hz, adjust if needed, or PanelTemp(PTemp_C)

        ' Initialize to NAN before measurement attempt
        Move(M6_Sensor0(),4,NAN,1)

		' Attempt to get M6 data for Sensor 0 using the 7-parameter SDI12Recorder
        ' Syntax: SDI12Recorder(DestArray(), SDIPort, SDI12AddrStr, SDI12CmdStr, Mult, Offset, FillOption)
        ' The Implexx M6! command should return 4 values.
        ' If this syntax works like the M! example, it should handle the D0! implicitly.
		SDI12Recorder(M6_Sensor0(),C1,"0","M6!",1,0,-1)

		' Reset all measurements if NAN is returned to the first element
		If M6_Sensor0(1)=NAN Then Move(M6_Sensor0(),4,NAN,1)

		CallTable TestM6Data
	NextScan
EndProg

--- FILE: generated_programs/cr300/sapflux_3sensor_CR300_30min.cr300 ---
' CR300 Series Datalogger Program
' Program to log 20 specified measurements from Implexx Sap Flow Sensors
' Generated by Python Script (cr300_generator.py)
' Number of Sensors: 3
' Measurement Interval: 30 minutes

'--- Constants ---
Const MEAST_INTERVAL_MIN = 30
Const SDI12_PORT = C1 ' Default SDI-12 Port (e.g., C1, C2)

'--- Declare Public Variables ---
Public PTemp_C As Float
Public Batt_volt As Float
Dim N_Loop ' Generic loop counter for error handling

Public S0_M_Data(9) As Float
Public S0_M1_Data(6) As Float
Public S0_M2_Data(6) As Float
Public S0_M5_Data(2) As Float
Public S1_M_Data(9) As Float
Public S1_M1_Data(6) As Float
Public S1_M2_Data(6) As Float
Public S1_M5_Data(2) As Float
Public S2_M_Data(9) As Float
Public S2_M1_Data(6) As Float
Public S2_M2_Data(6) As Float
Public S2_M5_Data(2) As Float

'--- Alias Declarations (for logged variables) ---
Alias S0_M_Data(4) = S0_AlpOut : Units S0_AlpOut=ratio
Alias S0_M_Data(5) = S0_AlpInn : Units S0_AlpInn=ratio
Alias S0_M_Data(6) = S0_BetOut : Units S0_BetOut=ratio
Alias S0_M_Data(7) = S0_BetInn : Units S0_BetInn=ratio
Alias S0_M_Data(8) = S0_tMxTout : Units S0_tMxTout=sec
Alias S0_M_Data(9) = S0_tMxTinn : Units S0_tMxTinn=sec
Alias S0_M1_Data(1) = S0_TpDsOut : Units S0_TpDsOut=degC
Alias S0_M1_Data(2) = S0_dTDsOut : Units S0_dTDsOut=degC
Alias S0_M1_Data(3) = S0_TsDsOut : Units S0_TsDsOut=degC
Alias S0_M1_Data(4) = S0_TpUsOut : Units S0_TpUsOut=degC
Alias S0_M1_Data(5) = S0_dTUsOut : Units S0_dTUsOut=degC
Alias S0_M1_Data(6) = S0_TsUsOut : Units S0_TsUsOut=degC
Alias S0_M2_Data(1) = S0_TpDsInn : Units S0_TpDsInn=degC
Alias S0_M2_Data(2) = S0_dTDsInn : Units S0_dTDsInn=degC
Alias S0_M2_Data(3) = S0_TsDsInn : Units S0_TsDsInn=degC
Alias S0_M2_Data(4) = S0_TpUsInn : Units S0_TpUsInn=degC
Alias S0_M2_Data(5) = S0_dTUsInn : Units S0_dTUsInn=degC
Alias S0_M2_Data(6) = S0_TsUsInn : Units S0_TsUsInn=degC
Alias S0_M5_Data(1) = S0_tMxTUsO : Units S0_tMxTUsO=sec
Alias S0_M5_Data(2) = S0_tMxTUsI : Units S0_tMxTUsI=sec
Alias S1_M_Data(4) = S1_AlpOut : Units S1_AlpOut=ratio
Alias S1_M_Data(5) = S1_AlpInn : Units S1_AlpInn=ratio
Alias S1_M_Data(6) = S1_BetOut : Units S1_BetOut=ratio
Alias S1_M_Data(7) = S1_BetInn : Units S1_BetInn=ratio
Alias S1_M_Data(8) = S1_tMxTout : Units S1_tMxTout=sec
Alias S1_M_Data(9) = S1_tMxTinn : Units S1_tMxTinn=sec
Alias S1_M1_Data(1) = S1_TpDsOut : Units S1_TpDsOut=degC
Alias S1_M1_Data(2) = S1_dTDsOut : Units S1_dTDsOut=degC
Alias S1_M1_Data(3) = S1_TsDsOut : Units S1_TsDsOut=degC
Alias S1_M1_Data(4) = S1_TpUsOut : Units S1_TpUsOut=degC
Alias S1_M1_Data(5) = S1_dTUsOut : Units S1_dTUsOut=degC
Alias S1_M1_Data(6) = S1_TsUsOut : Units S1_TsUsOut=degC
Alias S1_M2_Data(1) = S1_TpDsInn : Units S1_TpDsInn=degC
Alias S1_M2_Data(2) = S1_dTDsInn : Units S1_dTDsInn=degC
Alias S1_M2_Data(3) = S1_TsDsInn : Units S1_TsDsInn=degC
Alias S1_M2_Data(4) = S1_TpUsInn : Units S1_TpUsInn=degC
Alias S1_M2_Data(5) = S1_dTUsInn : Units S1_dTUsInn=degC
Alias S1_M2_Data(6) = S1_TsUsInn : Units S1_TsUsInn=degC
Alias S1_M5_Data(1) = S1_tMxTUsO : Units S1_tMxTUsO=sec
Alias S1_M5_Data(2) = S1_tMxTUsI : Units S1_tMxTUsI=sec
Alias S2_M_Data(4) = S2_AlpOut : Units S2_AlpOut=ratio
Alias S2_M_Data(5) = S2_AlpInn : Units S2_AlpInn=ratio
Alias S2_M_Data(6) = S2_BetOut : Units S2_BetOut=ratio
Alias S2_M_Data(7) = S2_BetInn : Units S2_BetInn=ratio
Alias S2_M_Data(8) = S2_tMxTout : Units S2_tMxTout=sec
Alias S2_M_Data(9) = S2_tMxTinn : Units S2_tMxTinn=sec
Alias S2_M1_Data(1) = S2_TpDsOut : Units S2_TpDsOut=degC
Alias S2_M1_Data(2) = S2_dTDsOut : Units S2_dTDsOut=degC
Alias S2_M1_Data(3) = S2_TsDsOut : Units S2_TsDsOut=degC
Alias S2_M1_Data(4) = S2_TpUsOut : Units S2_TpUsOut=degC
Alias S2_M1_Data(5) = S2_dTUsOut : Units S2_dTUsOut=degC
Alias S2_M1_Data(6) = S2_TsUsOut : Units S2_TsUsOut=degC
Alias S2_M2_Data(1) = S2_TpDsInn : Units S2_TpDsInn=degC
Alias S2_M2_Data(2) = S2_dTDsInn : Units S2_dTDsInn=degC
Alias S2_M2_Data(3) = S2_TsDsInn : Units S2_TsDsInn=degC
Alias S2_M2_Data(4) = S2_TpUsInn : Units S2_TpUsInn=degC
Alias S2_M2_Data(5) = S2_dTUsInn : Units S2_dTUsInn=degC
Alias S2_M2_Data(6) = S2_TsUsInn : Units S2_TsUsInn=degC
Alias S2_M5_Data(1) = S2_tMxTUsO : Units S2_tMxTUsO=sec
Alias S2_M5_Data(2) = S2_tMxTUsI : Units S2_tMxTUsI=sec

'--- DataTable Definition (Single Table for All Sensors) ---
DataTable (SapFlowAll, True, -1)
  DataInterval (0, MEAST_INTERVAL_MIN, Min, 0) ' No output delay
  Sample (1, Batt_volt, FP2)
  Sample (1, PTemp_C, FP2)
  Sample (1, S0_AlpOut, IEEE4)
  Sample (1, S0_AlpInn, IEEE4)
  Sample (1, S0_BetOut, IEEE4)
  Sample (1, S0_BetInn, IEEE4)
  Sample (1, S0_tMxTout, IEEE4)
  Sample (1, S0_tMxTinn, IEEE4)
  Sample (1, S0_TpDsOut, IEEE4)
  Sample (1, S0_dTDsOut, IEEE4)
  Sample (1, S0_TsDsOut, IEEE4)
  Sample (1, S0_TpUsOut, IEEE4)
  Sample (1, S0_dTUsOut, IEEE4)
  Sample (1, S0_TsUsOut, IEEE4)
  Sample (1, S0_TpDsInn, IEEE4)
  Sample (1, S0_dTDsInn, IEEE4)
  Sample (1, S0_TsDsInn, IEEE4)
  Sample (1, S0_TpUsInn, IEEE4)
  Sample (1, S0_dTUsInn, IEEE4)
  Sample (1, S0_TsUsInn, IEEE4)
  Sample (1, S0_tMxTUsO, IEEE4)
  Sample (1, S0_tMxTUsI, IEEE4)
  Sample (1, S1_AlpOut, IEEE4)
  Sample (1, S1_AlpInn, IEEE4)
  Sample (1, S1_BetOut, IEEE4)
  Sample (1, S1_BetInn, IEEE4)
  Sample (1, S1_tMxTout, IEEE4)
  Sample (1, S1_tMxTinn, IEEE4)
  Sample (1, S1_TpDsOut, IEEE4)
  Sample (1, S1_dTDsOut, IEEE4)
  Sample (1, S1_TsDsOut, IEEE4)
  Sample (1, S1_TpUsOut, IEEE4)
  Sample (1, S1_dTUsOut, IEEE4)
  Sample (1, S1_TsUsOut, IEEE4)
  Sample (1, S1_TpDsInn, IEEE4)
  Sample (1, S1_dTDsInn, IEEE4)
  Sample (1, S1_TsDsInn, IEEE4)
  Sample (1, S1_TpUsInn, IEEE4)
  Sample (1, S1_dTUsInn, IEEE4)
  Sample (1, S1_TsUsInn, IEEE4)
  Sample (1, S1_tMxTUsO, IEEE4)
  Sample (1, S1_tMxTUsI, IEEE4)
  Sample (1, S2_AlpOut, IEEE4)
  Sample (1, S2_AlpInn, IEEE4)
  Sample (1, S2_BetOut, IEEE4)
  Sample (1, S2_BetInn, IEEE4)
  Sample (1, S2_tMxTout, IEEE4)
  Sample (1, S2_tMxTinn, IEEE4)
  Sample (1, S2_TpDsOut, IEEE4)
  Sample (1, S2_dTDsOut, IEEE4)
  Sample (1, S2_TsDsOut, IEEE4)
  Sample (1, S2_TpUsOut, IEEE4)
  Sample (1, S2_dTUsOut, IEEE4)
  Sample (1, S2_TsUsOut, IEEE4)
  Sample (1, S2_TpDsInn, IEEE4)
  Sample (1, S2_dTDsInn, IEEE4)
  Sample (1, S2_TsDsInn, IEEE4)
  Sample (1, S2_TpUsInn, IEEE4)
  Sample (1, S2_dTUsInn, IEEE4)
  Sample (1, S2_TsUsInn, IEEE4)
  Sample (1, S2_tMxTUsO, IEEE4)
  Sample (1, S2_tMxTUsI, IEEE4)
EndTable

'--- Main Program ---
BeginProg
  Scan (MEAST_INTERVAL_MIN, Min, 1, 0) ' Scan interval, units, buffer=1, count=0 (continuous)
    PanelTemp (PTemp_C, 60) ' Defaulting to 60Hz fnotch, or use PanelTemp(PTemp_C)
    Battery (Batt_volt)

    ' Initialize all sensor data variables to NAN
    S0_AlpOut = NAN
    S0_AlpInn = NAN
    S0_BetOut = NAN
    S0_BetInn = NAN
    S0_tMxTout = NAN
    S0_tMxTinn = NAN
    S0_TpDsOut = NAN
    S0_dTDsOut = NAN
    S0_TsDsOut = NAN
    S0_TpUsOut = NAN
    S0_dTUsOut = NAN
    S0_TsUsOut = NAN
    S0_TpDsInn = NAN
    S0_dTDsInn = NAN
    S0_TsDsInn = NAN
    S0_TpUsInn = NAN
    S0_dTUsInn = NAN
    S0_TsUsInn = NAN
    S0_tMxTUsO = NAN
    S0_tMxTUsI = NAN
    S1_AlpOut = NAN
    S1_AlpInn = NAN
    S1_BetOut = NAN
    S1_BetInn = NAN
    S1_tMxTout = NAN
    S1_tMxTinn = NAN
    S1_TpDsOut = NAN
    S1_dTDsOut = NAN
    S1_TsDsOut = NAN
    S1_TpUsOut = NAN
    S1_dTUsOut = NAN
    S1_TsUsOut = NAN
    S1_TpDsInn = NAN
    S1_dTDsInn = NAN
    S1_TsDsInn = NAN
    S1_TpUsInn = NAN
    S1_dTUsInn = NAN
    S1_TsUsInn = NAN
    S1_tMxTUsO = NAN
    S1_tMxTUsI = NAN
    S2_AlpOut = NAN
    S2_AlpInn = NAN
    S2_BetOut = NAN
    S2_BetInn = NAN
    S2_tMxTout = NAN
    S2_tMxTinn = NAN
    S2_TpDsOut = NAN
    S2_dTDsOut = NAN
    S2_TsDsOut = NAN
    S2_TpUsOut = NAN
    S2_dTUsOut = NAN
    S2_TsUsOut = NAN
    S2_TpDsInn = NAN
    S2_dTDsInn = NAN
    S2_TsDsInn = NAN
    S2_TpUsInn = NAN
    S2_dTUsInn = NAN
    S2_TsUsInn = NAN
    S2_tMxTUsO = NAN
    S2_tMxTUsI = NAN

    ' --- Sensor 0 (Address "0") ---
    SDI12Recorder(S0_M_Data(), SDI12_PORT, "0", "M!", 1.0, 0, -1)
    If S0_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S0_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S0_M1_Data(), SDI12_PORT, "0", "M1!", 1.0, 0, -1)
    If S0_M1_Data(1) = NAN Then
      Move (S0_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S0_M2_Data(), SDI12_PORT, "0", "M2!", 1.0, 0, -1)
    If S0_M2_Data(1) = NAN Then
      Move (S0_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S0_M5_Data(), SDI12_PORT, "0", "M5!", 1.0, 0, -1)
    If S0_M5_Data(1) = NAN Then
      Move (S0_M5_Data(), 2, NAN, 1)
    EndIf

    ' --- Sensor 1 (Address "1") ---
    SDI12Recorder(S1_M_Data(), SDI12_PORT, "1", "M!", 1.0, 0, -1)
    If S1_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S1_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S1_M1_Data(), SDI12_PORT, "1", "M1!", 1.0, 0, -1)
    If S1_M1_Data(1) = NAN Then
      Move (S1_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S1_M2_Data(), SDI12_PORT, "1", "M2!", 1.0, 0, -1)
    If S1_M2_Data(1) = NAN Then
      Move (S1_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S1_M5_Data(), SDI12_PORT, "1", "M5!", 1.0, 0, -1)
    If S1_M5_Data(1) = NAN Then
      Move (S1_M5_Data(), 2, NAN, 1)
    EndIf

    ' --- Sensor 2 (Address "2") ---
    SDI12Recorder(S2_M_Data(), SDI12_PORT, "2", "M!", 1.0, 0, -1)
    If S2_M_Data(1) = NAN Then ' Check if first value is NAN (measurement failed)
      Move (S2_M_Data(), 9, NAN, 1) ' Set all elements of this array to NAN
    EndIf

    ' M1! Measurement
    SDI12Recorder(S2_M1_Data(), SDI12_PORT, "2", "M1!", 1.0, 0, -1)
    If S2_M1_Data(1) = NAN Then
      Move (S2_M1_Data(), 6, NAN, 1)
    EndIf

    ' M2! Measurement
    SDI12Recorder(S2_M2_Data(), SDI12_PORT, "2", "M2!", 1.0, 0, -1)
    If S2_M2_Data(1) = NAN Then
      Move (S2_M2_Data(), 6, NAN, 1)
    EndIf

    ' M5! Measurement
    SDI12Recorder(S2_M5_Data(), SDI12_PORT, "2", "M5!", 1.0, 0, -1)
    If S2_M5_Data(1) = NAN Then
      Move (S2_M5_Data(), 2, NAN, 1)
    EndIf

    CallTable SapFlowAll
  NextScan
EndProg

--- FILE: generated_programs/cr200/sapflux_3sensor_CR200X_30min.cr2 ---
' CR200/CR200X Series
' Program to log standard data from Implexx Sap Flow Sensors
' Generated by Python Script (cr200x_generator.py)
' Number of Sensors: 3
' Measurement Interval: 30 minutes
' NOTE: This program uses one DataTable per sensor.
' CR200X supports a maximum of 8 DataTables with this strategy.

'--- Declare Variables and Units ---
Dim N_0 ' Loop counter for Sensor 0 error handling
Dim N_1 ' Loop counter for Sensor 1 error handling
Dim N_2 ' Loop counter for Sensor 2 error handling
Public BattV
Public id
Public SDIData_Sensor0(9)
Public SensorAddress0
Public SDIData_Sensor1(9)
Public SensorAddress1
Public SDIData_Sensor2(9)
Public SensorAddress2

'--- Alias Declarations (Maps array elements to meaningful names) ---
Alias SDIData_Sensor0(1) = SapFlwTot0
Alias SDIData_Sensor0(2) = VhOuter0
Alias SDIData_Sensor0(3) = VhInner0
Alias SDIData_Sensor0(4) = AlphaOut0
Alias SDIData_Sensor0(5) = AlphaIn0
Alias SDIData_Sensor0(6) = BetaOut0
Alias SDIData_Sensor0(7) = BetaIn0
Alias SDIData_Sensor0(8) = tMaxTout0
Alias SDIData_Sensor0(9) = tMaxTin0
Alias SDIData_Sensor1(1) = SapFlwTot1
Alias SDIData_Sensor1(2) = VhOuter1
Alias SDIData_Sensor1(3) = VhInner1
Alias SDIData_Sensor1(4) = AlphaOut1
Alias SDIData_Sensor1(5) = AlphaIn1
Alias SDIData_Sensor1(6) = BetaOut1
Alias SDIData_Sensor1(7) = BetaIn1
Alias SDIData_Sensor1(8) = tMaxTout1
Alias SDIData_Sensor1(9) = tMaxTin1
Alias SDIData_Sensor2(1) = SapFlwTot2
Alias SDIData_Sensor2(2) = VhOuter2
Alias SDIData_Sensor2(3) = VhInner2
Alias SDIData_Sensor2(4) = AlphaOut2
Alias SDIData_Sensor2(5) = AlphaIn2
Alias SDIData_Sensor2(6) = BetaOut2
Alias SDIData_Sensor2(7) = BetaIn2
Alias SDIData_Sensor2(8) = tMaxTout2
Alias SDIData_Sensor2(9) = tMaxTin2

'--- Units Declarations ---
Units BattV=Volts
Units SapFlwTot0=literPerHour
Units VhOuter0=heatVelocity
Units VhInner0=heatVelocity
Units AlphaOut0=logTRatio
Units AlphaIn0=logTRatio
Units BetaOut0=logTRatio
Units BetaIn0=logTRatio
Units tMaxTout0=second
Units tMaxTin0=second
Units SapFlwTot1=literPerHour
Units VhOuter1=heatVelocity
Units VhInner1=heatVelocity
Units AlphaOut1=logTRatio
Units AlphaIn1=logTRatio
Units BetaOut1=logTRatio
Units BetaIn1=logTRatio
Units tMaxTout1=second
Units tMaxTin1=second
Units SapFlwTot2=literPerHour
Units VhOuter2=heatVelocity
Units VhInner2=heatVelocity
Units AlphaOut2=logTRatio
Units AlphaIn2=logTRatio
Units BetaOut2=logTRatio
Units BetaIn2=logTRatio
Units tMaxTout2=second
Units tMaxTin2=second

'--- Define Data Tables (One table per sensor due to CR200X field limit) ---
' Note: CR200X dataloggers have a limit of 16 fields per table and 8 tables total.
' DataTable names must be <= 12 characters.
DataTable(Table_S0,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress0)
	Sample(1,SapFlwTot0)
	Sample(1,VhOuter0)
	Sample(1,VhInner0)
	Sample(1,AlphaOut0)
	Sample(1,AlphaIn0)
	Sample(1,BetaOut0)
	Sample(1,BetaIn0)
	Sample(1,tMaxTout0)
	Sample(1,tMaxTin0)
EndTable

DataTable(Table_S1,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress1)
	Sample(1,SapFlwTot1)
	Sample(1,VhOuter1)
	Sample(1,VhInner1)
	Sample(1,AlphaOut1)
	Sample(1,AlphaIn1)
	Sample(1,BetaOut1)
	Sample(1,BetaIn1)
	Sample(1,tMaxTout1)
	Sample(1,tMaxTin1)
EndTable

DataTable(Table_S2,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SensorAddress2)
	Sample(1,SapFlwTot2)
	Sample(1,VhOuter2)
	Sample(1,VhInner2)
	Sample(1,AlphaOut2)
	Sample(1,AlphaIn2)
	Sample(1,BetaOut2)
	Sample(1,BetaIn2)
	Sample(1,tMaxTout2)
	Sample(1,tMaxTin2)
EndTable

'--- Main Program ---
BeginProg
	Scan(30,Min)
		'Default CR200 Series Datalogger Battery Voltage measurement 'BattV'
		Battery(BattV)
		'User Entered Calculation (from example)
		id = Status.PakBusAddress(1,1)
		SensorAddress0 = 0
		SensorAddress1 = 1
		SensorAddress2 = 2

		' --- Collect standard data for Sensor 0 (Address "0") ---
		SDI12Recorder(SDIData_Sensor0(), "0M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor0(1) = NAN Then
			For N_0 = 1 To 9
				SDIData_Sensor0(N_0) = NAN
			Next
		EndIf

		' --- Collect standard data for Sensor 1 (Address "1") ---
		SDI12Recorder(SDIData_Sensor1(), "1M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor1(1) = NAN Then
			For N_1 = 1 To 9
				SDIData_Sensor1(N_1) = NAN
			Next
		EndIf

		' --- Collect standard data for Sensor 2 (Address "2") ---
		SDI12Recorder(SDIData_Sensor2(), "2M!", 1, 0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to the first element
		If SDIData_Sensor2(1) = NAN Then
			For N_2 = 1 To 9
				SDIData_Sensor2(N_2) = NAN
			Next
		EndIf

		'Call Data Tables and Store Data
		CallTable Table_S0
		CallTable Table_S1
		CallTable Table_S2
	NextScan
EndProg

And here are some older programs used previously in our lab:

'CR300/CR310
'Created by Short Cut (4.4)

'Declare Variables and Units
Public BattV
Public PTemp_C
Public SDI12(9)
Public SDI12_2(9)
Public id
Public SDI0
Public SDI1

Alias SDI12(1)=SapFlwTot0
Alias SDI12(2)=VhOuter0
Alias SDI12(3)=VhInner0
Alias SDI12(4)=AlphaOut0
Alias SDI12(5)=AlphaIn0
Alias SDI12(6)=BetaOut0
Alias SDI12(7)=BetaIn0
Alias SDI12(8)=tMaxTout0
Alias SDI12(9)=tMaxTin0
Alias SDI12_2(1)=SapFlwTot1
Alias SDI12_2(2)=VhOuter1
Alias SDI12_2(3)=VhInner1
Alias SDI12_2(4)=AlphaOut1
Alias SDI12_2(5)=AlphaIn1
Alias SDI12_2(6)=BetaOut1
Alias SDI12_2(7)=BetaIn1
Alias SDI12_2(8)=tMaxTout1
Alias SDI12_2(9)=tMaxTin1

Units BattV=Volts
Units PTemp_C=Deg C
Units SapFlwTot0=literPerHour
Units VhOuter0=heatVelocity
Units VhInner0=heatVelocity
Units AlphaOut0=logTRatio
Units AlphaIn0=logTRatio
Units BetaOut0=logTRatio
Units BetaIn0=logTRatio
Units tMaxTout0=second
Units tMaxTin0=second
Units SapFlwTot1=literPerHour
Units VhOuter1=heatVelocity
Units VhInner1=heatVelocity
Units AlphaOut1=logTRatio
Units AlphaIn1=logTRatio
Units BetaOut1=logTRatio
Units BetaIn1=logTRatio
Units tMaxTout1=second
Units tMaxTin1=second

'Define Data Tables
DataTable(Table1,True,-1)
	DataInterval(0,30,Min,10)
	Minimum(1,BattV,FP2,False,False)
	Sample(1,id,FP2)
	Sample(1,SDI0,FP2)
	Sample(1,SapFlwTot0,FP2)
	Sample(1,VhOuter0,FP2)
	Sample(1,VhInner0,FP2)
	Sample(1,AlphaOut0,FP2)
	Sample(1,AlphaIn0,FP2)
	Sample(1,BetaOut0,FP2)
	Sample(1,BetaIn0,FP2)
	Sample(1,tMaxTout0,FP2)
	Sample(1,tMaxTin0,FP2)
EndTable

DataTable(Table2,True,-1)
	DataInterval(0,30,Min,10)
	Minimum(1,BattV,FP2,False,False)
	Sample(1,id,FP2)
	Sample(1,SDI1,FP2)
	Sample(1,SapFlwTot1,FP2)
	Sample(1,VhOuter1,FP2)
	Sample(1,VhInner1,FP2)
	Sample(1,AlphaOut1,FP2)
	Sample(1,AlphaIn1,FP2)
	Sample(1,BetaOut1,FP2)
	Sample(1,BetaIn1,FP2)
	Sample(1,tMaxTout1,FP2)
	Sample(1,tMaxTin1,FP2)
EndTable

'Main Program
BeginProg
	'Main Scan
	Scan(10,Min,1,0)
		'Default CR300 Datalogger Battery Voltage measurement 'BattV'
		Battery(BattV)
		'Default CR300 Datalogger Processor Temperature measurement 'PTemp_C'
		PanelTemp(PTemp_C,60)
		SDI12Recorder(SDI12(),C1,"0","M!",1,0,-1)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to SDI12(1)
		If SDI12(1)=NAN Then Move(SDI12(),9,NAN,1)
		SDI12Recorder(SDI12_2(),C1,"1","M!",1,0,-1)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to SDI12_2(1)
		If SDI12_2(1)=NAN Then Move(SDI12_2(),9,NAN,1)
		'User Entered Calculation
		id=Status.PakBusAddress(1,1)
		'User Entered Calculation
		SDI0=0
		'User Entered Calculation
		SDI1=1
		'Call Data Tables and Store Data
		CallTable Table1
		CallTable Table2
	NextScan
EndProg

last one: 

'CR200/CR200X Series
'Created by Short Cut (4.3)

'Declare Variables and Units
Dim N
Dim N_2
Public BattV
Public SDI12(9)
Public SDI12_2(9)
Public id
Public SDI0
Public SDI1

Alias SDI12(1)=SapFlwTot0
Alias SDI12(2)=VhOuter0
Alias SDI12(3)=VhInner0
Alias SDI12(4)=AlphaOut0
Alias SDI12(5)=AlphaIn0
Alias SDI12(6)=BetaOut0
Alias SDI12(7)=BetaIn0
Alias SDI12(8)=tMaxTout0
Alias SDI12(9)=tMaxTin0
Alias SDI12_2(1)=SapFlwTot1
Alias SDI12_2(2)=VhOut1
Alias SDI12_2(3)=VhIn1
Alias SDI12_2(4)=AlphaOut1
Alias SDI12_2(5)=AlphaIn1
Alias SDI12_2(6)=BetaOut1
Alias SDI12_2(7)=BetaIn1
Alias SDI12_2(8)=tMaxTout1
Alias SDI12_2(9)=tMaxTin1

Units BattV=Volts
Units SapFlwTot0=literPerHour
Units VhOuter0=heatVelocity
Units VhInner0=heatVelocity
Units AlphaOut0=logTRatio
Units AlphaIn0=logTRatio
Units BetaOut0=logTRatio
Units BetaIn0=logTRatio
Units tMaxTout0=second
Units tMaxTin0=second
Units SapFlwTot1=unit
Units VhOut1=unit
Units VhIn1=unit
Units AlphaOut1=unit
Units AlphaIn1=unit
Units BetaOut1=unit
Units BetaIn1=unit
Units tMaxTout1=unit
Units tMaxTin1=unit

'Define Data Tables
DataTable(Table1,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SDI0)
	Sample(1,SapFlwTot0)
	Sample(1,VhOuter0)
	Sample(1,VhInner0)
	Sample(1,AlphaOut0)
	Sample(1,AlphaIn0)
	Sample(1,BetaOut0)
	Sample(1,BetaIn0)
	Sample(1,tMaxTout0)
	Sample(1,tMaxTin0)
EndTable

DataTable(Table2,True,-1)
	DataInterval(0,30,Min)
	Minimum(1,BattV,False,False)
	Sample(1,id)
	Sample(1,SDI1)
	Sample(1,SapFlwTot1)
	Sample(1,VhOut1)
	Sample(1,VhIn1)
	Sample(1,AlphaOut1)
	Sample(1,AlphaIn1)
	Sample(1,BetaOut1)
	Sample(1,BetaIn1)
	Sample(1,tMaxTout1)
	Sample(1,tMaxTin1)
EndTable

'Main Program
BeginProg
	'Main Scan
	Scan(30,Min)
		'Default CR200 Series Datalogger Battery Voltage measurement 'BattV'
		Battery(BattV)
		'Generic SDI-12 Sensor measurements 'SapFlwTot0', 'VhOuter0', 'VhInner0', 
		'AlphaOut0', 'AlphaIn0', 'BetaOut0', 'BetaIn0', 'tMaxTout0', and 'tMaxTin0'
		SDI12Recorder(SDI12(),"0M!",1,0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to SDI12(1)
		If SDI12(1)=NAN Then
			For N=1 To 9
				SDI12(N)=NAN
			Next
		EndIf
		'Generic SDI-12 Sensor measurements 'SapFlwTot1', 'VhOut1', 'VhIn1', 
		'AlphaOut1', 'AlphaIn1', 'BetaOut1', 'BetaIn1', 'tMaxTout1', and 'tMaxTin1'
		SDI12Recorder(SDI12_2(),"1M!",1,0)
		'Reset all Generic SDI-12 Sensor measurements if NAN is returned to SDI12_2(1)
		If SDI12_2(1)=NAN Then
			For N_2=1 To 9
				SDI12_2(N_2)=NAN
			Next
		EndIf
		'User Entered Calculation
		id=Status.PakBusAddress(1,1)
		'User Entered Calculation
		SDI0=0
		'User Entered Calculation
		SDI1=1
		'Call Data Tables and Store Data
		CallTable Table1
		CallTable Table2
	NextScan
EndProg

From this, can you help me reason about how my parsers should be structured, how my parser's outputs should be structured, and how I should rename the data columns to be more informative?

**Model:**

Of course. This is an excellent and well-documented request. Having the sensor documentation, the data format examples, and the CRBasic programs that generate them is the key to building a robust and accurate parser. Here’s a breakdown of how you can structure your parser, the output data, and a proposed renaming of the data columns.

### 1. Proposed Data Column Names

A major part of making data useful is giving it a clear, self-documenting name. Based on the Implexx SDI-12 Integrator's Guide and the CRBasic programs, here is a mapping from the cryptic source names to more informative ones.

The CRBasic programs use aliases like AlphaOut0, tMaxTout0, S0_AlpOut, and S0_tMxTout. The core measurement name remains the same, with a prefix or suffix indicating the sensor address and thermistor depth. The proposed "Canonical Name" below is the base name for the measurement itself.

Original Name(s)Canonical NameUnitDescription from DocumentationSapFlwTottotal_sap_flowL/hrThe total sap flow of the stem in litres per hour.VhOuter, VhInner, SFDOuter, SFDInnersap_flux_densitycm³/cm²/hrSap flux density at the specific thermistor depth.AlphaOut, AlphaInalpharatioThe natural log of the ratio of temperatures, used in heat ratio calculations.BetaOut, BetaInbetaratioThe natural log of the ratio of maximum temperatures, used in Tmax calculations.tMaxTout, tMaxTintime_to_max_temp_downstreamsecondsTime from the start of the heat pulse to the maximum temperature at the downstream thermistor.TpDsOut, TpDsInnpre_pulse_temp_downstream°CTemperature at the downstream thermistor before the heat pulse.dTDsOut, dTDsInnmax_temp_rise_downstream°CThe maximum rise in temperature at the downstream thermistor.TsDsOut, TsDsInnpost_pulse_temp_downstream°CTemperature at the downstream thermistor after the heat pulse measurement period.TpUsOut, TpUsInnpre_pulse_temp_upstream°CTemperature at the upstream thermistor before the heat pulse.dTUsOut, dTUsInnmax_temp_rise_upstream°CThe maximum rise in temperature at the upstream thermistor.TsUsOut, TsUsInnpost_pulse_temp_upstream°CTemperature at the upstream thermistor after the heat pulse measurement period.tMxTUsO, tMxTUsItime_to_max_temp_upstreamsecondsTime to maximum temperature for the upstream thermistors.

### 2. Proposed Output Structure (Rust Structs)

Your proposed structure is spot-on. We can formalize it into a set of Rust structs that are easy to work with and serialize. Using Option<T> is a good way to handle measurements that may not be present in all data formats.

```
use chrono::{DateTime, NaiveDateTime, Utc};
use thiserror::Error;

// Top-level structure holding all parsed data from a file
pub struct SapFlowData {
    pub metadata: FileMetadata,
    pub records: Vec<DataRecord>,
}

// Holds the file-level metadata from the first header row
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_format: String, // e.g., "TOA5"
    pub logger_name: String,
    pub logger_type: String,
    pub program_name: String,
    pub table_name: String,
}

// Represents a single row of data in the file
#[derive(Debug, Clone)]
pub struct DataRecord {
    pub timestamp: DateTime<Utc>,
    pub record_number: u32,
    pub battery_voltage: f32,
    pub panel_temp_c: Option<f32>, // Only present in CR300 format
    pub sensors: Vec<SensorMeasurement>,
}

// Contains all measurements for a single sensor at a given timestamp
#[derive(Debug, Clone)]
pub struct SensorMeasurement {
    pub sdi12_address: char,
    // The total sap flow is a single value for the sensor
    pub total_sap_flow_lph: Option<f64>, 
    // Data is collected from two depths (inner and outer)
    pub thermistor_pairs: Vec<ThermistorPair>,
}

// An enum to clearly distinguish thermistor depth
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThermistorDepth {
    Inner,
    Outer,
}

// Holds all the values related to a single thermistor pair (inner or outer)
#[derive(Debug, Clone, Default)]
pub struct ThermistorPair {
    pub depth: ThermistorDepth,
    pub sap_flux_density_cmh: Option<f64>,
    pub alpha: Option<f64>,
    pub beta: Option<f64>,
    
    // Downstream measurements
    pub time_to_max_temp_downstream_s: Option<f64>,
    pub pre_pulse_temp_downstream_c: Option<f64>,
    pub max_temp_rise_downstream_c: Option<f64>,
    pub post_pulse_temp_downstream_c: Option<f64>,

    // Upstream measurements
    pub time_to_max_temp_upstream_s: Option<f64>,
    pub pre_pulse_temp_upstream_c: Option<f64>,
    pub max_temp_rise_upstream_c: Option<f64>,
    pub post_pulse_temp_upstream_c: Option<f64>,
}
```

### 3. Proposed Parser Architecture

Your idea to iterate through parsers is a robust, extensible pattern often called the "Chain of Responsibility." Here’s how you can implement that in Rust.

#### a. The Parser Trait and Error Handling

First, define a common interface for all parsers and a comprehensive error type using thiserror.

```
// In a file like src/errors.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("File does not match the expected format for this parser: {0}")]
    FormatMismatch(String),
    
    #[error("Invalid or missing header row {row_index}: {description}")]
    InvalidHeader { row_index: usize, description: String },

    #[error("Could not parse data on line {line}: {description}")]
    DataRowParseError { line: usize, description: String },

    #[error("Data validation failed on line {line}: {description}")]
    ValidationError { line: usize, description: String },
    
    #[error("An I/O error occurred")]
    IoError(#[from] std::io::Error),
}

// In a file like src/parsers/mod.rs
use crate::{SapFlowData, ParserError};

// The trait that all format-specific parsers will implement
pub trait SapflowParser {
    /// Returns the name of the parser format (e.g., "CR300_SINGLE_TABLE")
    fn name(&self) -> &'static str;

    /// Attempts to parse the file content.
    /// Returns a full data structure on success or a specific error on failure.
    fn parse(&self, content: &str) -> Result<SapFlowData, ParserError>;
}
```

#### b. The Main Parsing Function

This function orchestrates the process, trying each known parser until one succeeds.

```
// In a file like src/lib.rs

// Import your parser structs
use crate::parsers::cr200_multi_table::Cr200MultiTableParser;
use crate::parsers::cr300_single_table::Cr300SingleTableParser;
use crate::parsers::SapflowParser;

/// Tries all known parsers on the input content and returns the first successful parse.
///
/// If no parser succeeds, it returns a vector of all errors encountered,
/// allowing the caller to see why each one failed.
pub fn parse_file_content(content: &str) -> Result<SapFlowData, Vec<(String, ParserError)>> {
    
    // Register all your known parsers here.
    // This makes it easy to add or disable formats.
    let parsers: Vec<Box<dyn SapflowParser>> = vec![
        Box::new(Cr300SingleTableParser),
        Box::new(Cr200MultiTableParser),
        // Add legacy parsers here in the future
    ];

    let mut errors = Vec::new();

    for parser in parsers {
        match parser.parse(content) {
            Ok(data) => return Ok(data), // First success wins
            Err(e) => {
                // Collect the error and try the next parser
                errors.push((parser.name().to_string(), e));
            }
        }
    }

    // If we get here, all parsers failed.
    Err(errors)
}```

#### c. Implementation of a Specific Parser (Example: `Cr300SingleTableParser`)

Each parser will contain the logic specific to its format. The most critical part is interpreting the column header row to build a map of what data to expect in which column.

```rust
// In a file like src/parsers/cr300_single_table.rs

// High-level pseudo-code for the parse function
fn parse(&self, content: &str) -> Result<SapFlowData, ParserError> {
    let lines: Vec<&str> = content.lines().collect();

    // 1. Validate Header Rows
    // Check line 1 starts with "TOA5", parse metadata.
    // Check line 3 (units) and 4 (measurement type) match the expected constant format.
    // If anything fails, return Err(ParserError::InvalidHeader { ... }).

    // 2. Build the Column Map from the Column Header Row (line 2)
    // This is the core logic that distinguishes this parser.
    let column_headers: Vec<&str> = lines[1].split(',').map(|s| s.trim_matches('"')).collect();
    
    // A map from the column index to what measurement it represents.
    // e.g., index 5 -> (Sensor 0, Outer, alpha)
    // e.g., index 24 -> (Sensor 1, Inner, time_to_max_temp_upstream)
    let column_map = self.build_column_map(column_headers)?;

    // 3. Iterate and Parse Data Rows
    let mut records = Vec::new();
    for (i, line) in lines.iter().enumerate().skip(4) { // Start after headers
        let values: Vec<&str> = line.split(',').map(|s| s.trim_matches('"')).collect();
        
        // Parse logger-level data (timestamp, record num, batt_v, etc.)
        // Create a Vec<SensorMeasurement> for this record.
        
        // Use the `column_map` to populate the sensor data
        for (col_idx, value_str) in values.iter().enumerate() {
            if let Some(mapping) = column_map.get(&col_idx) {
                // Parse the value (e.g., to f64, handling "NAN")
                // Find the correct SensorMeasurement and ThermistorPair struct
                // in the current record and assign the value to the correct field.
            }
        }

        // Perform validations, e.g., check that tMax... values are positive.
        // If validation fails, return Err(ParserError::ValidationError { ... }).
        
        records.push(parsed_record);
    }
    
    // 4. Return the final structured data
    Ok(SapFlowData { metadata, records })
}
```

This architecture provides the flexibility you need. Adding a new format is as simple as creating a new struct that implements the SapflowParser trait and adding it to the parsers vector in the main function. Toggling formats on and off is just a matter of commenting them out from that list.

---

**User:**

UserGreat, thanks for the help. Here's a few comments:

1. I don't actually want to store the SapFlwTot, Vh, or SFD. Those are derived calculations from default parameters on the sensor itself, I would rather do my own calculations downstream with custom parameters. So we need to parse those to make sure the data file isn't malformed, but I don't want to store them, if that makes sense.I want my parser to strip down the data hierarchically, e.g. for a given file, it pairs the logger-level information to a set of sensor-level informations, which themselves map to a set of thermistor pair level observations. So in terms of what's actually being stored, I think it should be something like a bunch of polars series in a struct or a map of some kind. I think we should preserve the raw data in the output, outputting the parsed-to-thermistor-pairs data, the logger-level data, and the original input text as a data blob. So I do want to have explicit upstream and downstream sensor names, but i don't want to have duplicate sets of columns by depth or inner vs outer. Does that make sense? Just want to be perfectly clear.I love the table of column names with units and descriptions. Can I get that as a toml?I agree with making the "additional" measurements, like max_temp_rise_upstream and similar, options, and keeping a single canonical internal representation of both possible input formats. Is it possible to have multiple possible internal formats (e.g. they implement a trait), and a parser is mapped to a particular possible internal format? Our first couple parsers will just map to the same format, but I want to maintain flexibility in case we get different sensors or something like that in the future.I don't like having each row as a struct. I would much rather parse for validity and then read into a columnar format like a polars series. Should that be a two-step process or a one-step process that returns a result?Interesting idea about an enum for thermistor depth. For now, all of our sensor types have just two thermistor pairs each, so it's simple enough to call them inner and outer and map to depth later. But I would much rather have sensor config as an adaptable or dynamic config that lives outside the code, some lookup table somewhere. I want to basically have a table for sensors that maps a named sensor type to its probe spacing (important for calculation) and each of its thermistor pairs to an installation depth (in the sapwood). We don't need to worry about attributing a specific depth at this stage, but we do need an unambiguous way to ID a column of data to a sensor depth down the road. So I guess a way to map thermistor pairs is specific to a parser? Inner and outer are just a convention in the world of this specific sensor, data format, and parser. I think i've actually come back around a little bit. If we're going to write a parser specific to a sensor and data format, I guess hard-coding inner and outer is fine for now. If we end up buying a new kind of sensor with more or different thermistor names we might just need a new parser for that.
What are your thoughts on this? Let's not sketch out any code yet, let's just think about the implementation.

**Model:**

Excellent points. This feedback is crucial for moving from a generic sketch to a practical, high-performance implementation that truly fits your workflow. Let's refine the design based on your comments. We'll stick to implementation design and not code for now.

### On Points 1 & 5: Columnar Output and Process

You are absolutely right. For data analysis, a columnar format is vastly superior to a row-based one. Forcing each row into a struct is inefficient and unidiomatic when using libraries like Polars.

**Refined Plan:**

* **One-Step Parsing to Columnar Builders:** The most efficient method is a single-pass approach. As the parser iterates through the data rows of the input file, it won't create a DataRecord struct for each row. Instead, it will append the parsed values directly into type-specific vectors (e.g., Vec<DateTime<Utc>>, Vec<u32>, Vec<Option<f64>>). After the last data row is processed, these vectors (builders) will be used to construct the polars::Series objects that form the final DataFrames.**The Output Structure:** The successful result of a parse operation will be a struct that looks something like this:

```
// Notional Rust struct
pub struct ParsedSapflowData {
    pub raw_text: String, // The original, unmodified input file content
    pub metadata: FileMetadata, // As before
    pub logger_data: polars::frame::DataFrame,
    pub sensor_data: polars::frame::DataFrame,
}
```*   **DataFrame Schema:**
*   `logger_data`: Will contain columns like `timestamp`, `record_number`, `battery_voltage`, and `panel_temp_c` (for CR300). The `timestamp` will be the primary key.
*   `sensor_data`: This will be a "long" format DataFrame, which is ideal for analysis. It will have columns like:
    *   `timestamp`: The foreign key to link back to `logger_data`.
    *   `sdi12_address`: The identifier for the sensor.
    *   `thermistor_depth_id`: A string or enum, e.g., `"inner"`, `"outer"`.
    *   And columns for each measurement: `alpha`, `beta`, `time_to_max_temp_downstream_s`, `pre_pulse_temp_downstream_c`, etc.
```

**Handling Derived Values (Point 1):** This model handles your first point perfectly. The parser will identify the SapFlwTot and SFDOuter columns during the header parsing step to validate the format. However, it will simply ignore their values during the data row processing step, and they will never be added to the columnar builders or appear in the final DataFrame.

### On Point 2: Data Hierarchy

The proposed columnar structure achieves the hierarchy you want in a relational, "tidy data" way, which is exactly how Polars and similar tools are designed to work.

* **File Level:** raw_text and metadata in ParsedSapflowData.**Logger Level:** The logger_data DataFrame. Each row is a unique observation from the logger.**Sensor Level:** This is represented by grouping the sensor_data DataFrame by sdi12_address.**Thermistor Pair Level:** This is represented by grouping the sensor_data DataFrame by both sdi12_address and thermistor_depth_id.
This is a much more powerful and flexible representation than nested structs, as it allows for easy filtering, aggregation, and joining operations across any level of the hierarchy.

### On Point 3: TOML for Column Names

Excellent idea. Here is the requested information in TOML format. This could be used for documentation or even to drive parts of a reporting or plotting configuration.

```
# Canonical names and descriptions for Implexx Sap Flow Sensor measurements.

[alpha]
name = "alpha"
unit = "ratio"
description = "The natural log of the ratio of temperatures (ΔT_initial / ΔT_final), used in heat ratio method calculations."

[beta]
name = "beta"
unit = "ratio"
description = "The natural log of the ratio of maximum temperatures (ΔT_max_downstream / ΔT_max_upstream), used in Tmax method calculations."

[time_to_max_temp_downstream]
name = "time_to_max_temp_downstream"
unit = "seconds"
description = "Time from the start of the heat pulse to the maximum temperature at the downstream thermistor."

[time_to_max_temp_upstream]
name = "time_to_max_temp_upstream"
unit = "seconds"
description = "Time from the start of the heat pulse to the maximum temperature at the upstream thermistor."

[pre_pulse_temp_downstream]
name = "pre_pulse_temp_downstream"
unit = "°C"
description = "Temperature at the downstream thermistor immediately before the heat pulse."

[max_temp_rise_downstream]
name = "max_temp_rise_downstream"
unit = "°C"
description = "The maximum rise in temperature (ΔT_max) observed at the downstream thermistor."

[post_pulse_temp_downstream]
name = "post_pulse_temp_downstream"
unit = "°C"
description = "Temperature at the downstream thermistor after the heat pulse measurement period has concluded."

[pre_pulse_temp_upstream]
name = "pre_pulse_temp_upstream"
unit = "°C"
description = "Temperature at the upstream thermistor immediately before the heat pulse."

[max_temp_rise_upstream]
name = "max_temp_rise_upstream"
unit = "°C"
description = "The maximum rise in temperature (ΔT_max) observed at the upstream thermistor."

[post_pulse_temp_upstream]
name = "post_pulse_temp_upstream"
unit = "°C"
description = "Temperature at the upstream thermistor after the heat pulse measurement period has concluded."
```

### On Point 4: Multiple Internal Formats

This is an advanced design concept and you're right to think about it for long-term flexibility.

* **The Generic Trait Approach:** You could define a generic parser trait like trait SapflowParser<OutputFormat>. The parse method would then return a Result<OutputFormat, ParserError>. This would allow you to have a Cr300Parser that could, in theory, output to ParsedSapflowDataV1 or a future ParsedSapflowDataV2.**Pragmatic Recommendation:** I would advise against starting with this level of abstraction. It adds significant complexity (generics, trait bounds, potentially associated types) that isn't necessary yet. The immediate goal is to parse two known formats into one canonical, columnar structure. The "Chain of Responsibility" pattern still works perfectly: [ParserA, ParserB, ParserC] are all tried, and they all agree to produce a ParsedSapflowData struct on success.**Future Flexibility:** If you encounter a sensor that produces fundamentally different kinds of data (e.g., it measures soil moisture and sap flow), that would be the time to introduce a new output format. At that point, you could either create a second, parallel parsing pipeline (parse_soil_file()) or refactor the existing one to use generics. Starting simple preserves the option to add this complexity later when the need is concrete.

### On Point 6: Dynamic vs. Hard-Coded Sensor Config

Your thought process here is excellent, and you've landed on a very robust design principle: **Separation of Concerns**.

* **The Parser's Responsibility:** The parser's job is to faithfully translate a specific, known data format into a structured representation. The CR300 format you provided explicitly uses the labels Inn and Out. Therefore, the Cr300Parser should be hard-coded to look for these labels and output them as, for example, the thermistor_depth_id column with values "inner" and "outer". It is not the parser's job to know that "outer" means 22.5 mm deep. This makes the parser stable and tied only to the file format specification.**The Analyzer's Responsibility:** Your downstream analysis code is responsible for interpreting the data. This is the perfect place for an external lookup table or configuration file (e.g., a CSV or TOML). This config would map a sensor's sdi12_address at a given date_range to a sensor_type (e.g., "Implexx SF30-G2"), which in turn maps the thermistor_depth_id ("inner", "outer") to a physical depth in sapwood (e.g., 10.0, 22.5).
This approach is ideal. It means your parser doesn't need to change unless Campbell Scientific changes the TOA5 format or you change your CRBasic program's column headers. All the experimental metadata lives where it belongs—with the analysis code.

---

**User:**

UserCool sounds good. For the toml of names, for each entry, can I get a vector of the possible internal names, with a placeholder for sensor number / sdi12 address? How do you recommend handling the placeholder value? Should we just call it "A" for address, as it convention in SDI12 world? Or would it be better to do something easy for regex etc?

**Model:**

Excellent questions. This moves us towards a more configuration-driven and robust parser design. Let's tackle these points.

### Placeholder Convention: {addr}

You've hit on a key implementation detail. Using a simple placeholder like "A" is convenient but fragile. If you ever encountered a measurement name that naturally contained the letter "A" (e.g., Alpha_Max_A), a simple string replacement would fail.

A more robust and conventional approach is to use a templating placeholder that is highly unlikely to appear in a real column name.

**I recommend using {addr} as the placeholder.**

* **Clarity:** It's self-documenting. Anyone reading the TOML immediately understands it represents an address.**Safety:** It is extremely unlikely to conflict with a real column name.**Ease of Use:** It's compatible with standard string formatting/templating engines in many languages (like Rust's format!, Python's f-strings, etc.), and is still very easy to use with simple string replacement or regex.
For example, when building the expected header map, your parser logic could do something conceptually like this:

```
// Notional Rust code
let sdi_address = "1";
let pattern = "AlphaOut{addr}";
let expected_column_name = pattern.replace("{addr}", sdi_address); 
// expected_column_name is now "AlphaOut1"
```

### TOML with Internal Names

Yes, absolutely. Here is the updated TOML structure. It now includes an internal_names array for each measurement, capturing all the patterns you've provided and using the {addr} placeholder. This effectively becomes the "dictionary" your parser will use to map raw column headers to their canonical meaning and extract the sensor address.

```
# ===================================================================
# Dictionary of Implexx Sap Flow Sensor Measurement Names
# ===================================================================
# This file maps canonical measurement names to their properties and
# the various raw column header formats ("internal_names") found in
# Campbell Scientific datalogger files.
#
# The placeholder `{addr}` represents the SDI-12 address of the sensor.
# ===================================================================

[alpha]
name = "alpha"
unit = "ratio"
description = "The natural log of the ratio of temperatures (ΔT_initial / ΔT_final), used in heat ratio method calculations."
internal_names = [
    "AlphaOut{addr}", # CR200 format (Outer)
    "AlphaIn{addr}",  # CR200 format (Inner)
    "S{addr}_AlpOut", # CR300 format (Outer)
    "S{addr}_AlpInn", # CR300 format (Inner)
]

[beta]
name = "beta"
unit = "ratio"
description = "The natural log of the ratio of maximum temperatures (ΔT_max_downstream / ΔT_max_upstream), used in Tmax method calculations."
internal_names = [
    "BetaOut{addr}", # CR200 format (Outer)
    "BetaIn{addr}",  # CR200 format (Inner)
    "S{addr}_BetOut", # CR300 format (Outer)
    "S{addr}_BetInn", # CR300 format (Inner)
]

[time_to_max_temp_downstream]
name = "time_to_max_temp_downstream"
unit = "seconds"
description = "Time from the start of the heat pulse to the maximum temperature at the downstream thermistor."
internal_names = [
    "tMaxTout{addr}", # CR200 format (Outer)
    "tMaxTin{addr}",  # CR200 format (Inner)
    "S{addr}_tMxTout", # CR300 format (Outer)
    "S{addr}_tMxTinn", # CR300 format (Inner)
]

[time_to_max_temp_upstream]
name = "time_to_max_temp_upstream"
unit = "seconds"
description = "Time from the start of the heat pulse to the maximum temperature at the upstream thermistor."
internal_names = [
    "S{addr}_tMxTUsO", # CR300 format (Outer)
    "S{addr}_tMxTUsI", # CR300 format (Inner)
]

[pre_pulse_temp_downstream]
name = "pre_pulse_temp_downstream"
unit = "°C"
description = "Temperature at the downstream thermistor immediately before the heat pulse."
internal_names = [
    "S{addr}_TpDsOut", # CR300 format (Outer)
    "S{addr}_TpDsInn", # CR300 format (Inner)
]

[max_temp_rise_downstream]
name = "max_temp_rise_downstream"
unit = "°C"
description = "The maximum rise in temperature (ΔT_max) observed at the downstream thermistor."
internal_names = [
    "S{addr}_dTDsOut", # CR300 format (Outer)
    "S{addr}_dTDsInn", # CR300 format (Inner)
]

[post_pulse_temp_downstream]
name = "post_pulse_temp_downstream"
unit = "°C"
description = "Temperature at the downstream thermistor after the heat pulse measurement period has concluded."
internal_names = [
    "S{addr}_TsDsOut", # CR300 format (Outer)
    "S{addr}_TsDsInn", # CR300 format (Inner)
]

[pre_pulse_temp_upstream]
name = "pre_pulse_temp_upstream"
unit = "°C"
description = "Temperature at the upstream thermistor immediately before the heat pulse."
internal_names = [
    "S{addr}_TpUsOut", # CR300 format (Outer)
    "S{addr}_TpUsInn", # CR300 format (Inner)
]

[max_temp_rise_upstream]
name = "max_temp_rise_upstream"
unit = "°C"
description = "The maximum rise in temperature (ΔT_max) observed at the upstream thermistor."
internal_names = [
    "S{addr}_dTUsOut", # CR300 format (Outer)
    "S{addr}_dTUsInn", # CR300 format (Inner)
]

[post_pulse_temp_upstream]
name = "post_pulse_temp_upstream"
unit = "°C"
description = "Temperature at the upstream thermistor after the heat pulse measurement period has concluded."
internal_names = [
    "S{addr}_TsUsOut", # CR300 format (Outer)
    "S{addr}_TsUsInn", # CR300 format (Inner)
]
```

---

**User:**

UserGreat, thanks! Another question. I want this parsing module to be a relatively standalone thing as part of my data processing pipeline.

The downstream program that calls into this parser is going to basically take the outputs of the parser and stuff them into a postgres db + cloudflare r2 bucket. I'm curious about how we should serialize the output of this parser into something appropriate for that setup, and what should live in the postgres db, and what should live in the r2 bucket. A major concern is duplication. I need to keep the raw files and hashes of the raw files somewhere so the calling program can check for duplication and reject a duplicate before it even hits the parser.

**Model:**

Excellent question. This is a critical architectural decision that will define the robustness and scalability of your entire pipeline. Designing the "seam" between your parsing module and your data-loading service correctly is paramount.

Let's break down the strategy into three parts: the deduplication workflow, the serialization format, and the division of data between Postgres and R2.

### 1. The Deduplication and Ingestion Workflow

Your instinct is spot on: you must prevent duplicates from ever entering the system. The standard and most reliable way to do this is with cryptographic hashing.

Here is the recommended, robust workflow for your downstream data loader program:

1. **File Arrival:** A new raw data file (e.g., Sapflux2sensor.dat) is detected or received by the loader.**Hashing:** **Before any parsing occurs**, the loader computes a cryptographic hash of the entire raw file content. Use a fast, modern algorithm like **BLAKE3** or the standard **SHA-256**. This hash is the file's unique, immutable fingerprint.**Database Check:** The loader queries a central processed_files table in your PostgreSQL database with a simple, fast, indexed query: SELECT id FROM processed_files WHERE file_hash = 'the-calculated-hash' LIMIT 1;.**Decision Point:****If a record is found:** The file is a duplicate. The loader immediately rejects it, logs a "duplicate detected" message, and stops processing this file.**If no record is found:** The file is new. The workflow continues.**Parsing:** The loader passes the raw file content to your Rust parsing module. The parser returns the structured output (we'll define this next).**Object Storage Upload:**The loader takes the **original, raw file content** and uploads it to an R2 bucket (e.g., r2://sapflow-raw-files/). The object key should be something unique and content-addressable, ideally the **file hash itself**. Example key: blake3-hash-of-file.dat. This permanently associates the hash with the raw data.The loader takes the **parsed, serialized data** (see section 2) and uploads it to a separate R2 bucket or prefix (e.g., r2://sapflow-parsed-data/). The key for this object should also be derived from the hash. Example key: blake3-hash-of-file.parquet.**Database Insert:** Only after the file is successfully parsed and both objects are successfully uploaded to R2, the loader inserts a **single new row** into the processed_files table in Postgres. This row contains all the metadata, the hash, and the R2 keys for the raw and parsed files.
This order is crucial. The database insert is the last, atomic step that "commits" the file to the system. If any step before it fails (parsing error, network error during upload), no record is written to the database, and the file can be safely retried later without creating partial state.

### 2. The Serialization Format: Arrow IPC or Parquet

Your parser will produce Polars DataFrames. To send these from the parser to the loader (even if they are in the same application) and to store them in R2, you need a serialization format. You have two excellent, industry-standard choices.

* **Recommendation:** **Apache Parquet**.**Alternative:** **Apache Arrow IPC Stream Format**.
Here’s the breakdown:

FeatureApache ParquetApache Arrow IPC**Primary Use Case****Storage and archival.****Inter-process communication (IPC).****Compression****Excellent.** Columnar compression makes files very small.Good, but not its primary strength.**Speed**Very fast to read and write.**Extremely fast.** Zero-copy reads are possible if memory layouts match.**Ecosystem**Universal. Supported by nearly every data tool (Spark, DuckDB, BigQuery, etc.).The native format for Polars, pandas 2.0+, and many others.**Polars Support**Excellent (ParquetWriter, read_parquet).Excellent (IpcWriter, read_ipc).**Conclusion:**

Use **Parquet** as the format for storing your parsed data in R2. Its superior compression will save on storage costs and transfer times, and its universal compatibility is a huge long-term advantage. Your parser's output function will take the final logger_data and sensor_data DataFrames and serialize them into a single Parquet file (or two, if you prefer).

### 3. What to Store: Postgres (Metadata) vs. R2 (Data)

This is the most important architectural decision. The rule of thumb is:

* **Postgres is for your INDEX (Metadata).** It should be small, structured, and highly queryable.**R2 is for your DATA (Objects).** It stores the immutable "blobs," which can be large.

#### PostgreSQL Schema

Your database should contain one primary table that acts as a catalog or index of all processed files.

processed_files table:| Column Name | Data Type | Constraints | Description || :--- | :--- | :--- | :--- || id | BIGSERIAL | PRIMARY KEY | Unique identifier for this processing event. || file_hash | TEXT | UNIQUE NOT NULL | The BLAKE3 or SHA-256 hash of the raw file. **This is your deduplication key.** || original_filename | TEXT | | The original name of the file (e.g., sapflux_2sensor_CR300_30min.dat). || processed_at | TIMESTAMPTZ | NOT NULL | When the file was processed and ingested. || logger_name | TEXT | NOT NULL | Parsed from the header (e.g., CR300Series_420). || logger_type | TEXT | NOT NULL | Parsed from the header (e.g., CR300). || program_name | TEXT | | Parsed from the header. || table_name | TEXT | | Parsed from the header. || record_count | INTEGER | NOT NULL | Number of data rows parsed from the file. || raw_file_r2_key | TEXT | NOT NULL | The full key/path to the raw file in R2. || parsed_data_r2_key| TEXT | NOT NULL | The full key/path to the parsed Parquet file in R2. |

**Why this design is powerful:**

* **Lean Database:** Your database stays small and incredibly fast. You can query for files from a specific logger, in a specific date range, without ever touching R2.**Data Immutability:** The raw data and parsed data in R2 are treated as immutable. If you need to re-process a file with a new version of the parser, you would read the raw file from R2, run it through the new parser, and save a new parsed object in R2. You could then update the parsed_data_r2_key in the database or use a separate versioning table.**Scalability:** This pattern scales almost infinitely. R2 can handle petabytes of data, and Postgres can handle billions of metadata records with ease.
When you need to perform an analysis, your workflow would be:

1. Query Postgres to find the parsed_data_r2_keys for the files you're interested in.Use those keys to fetch the relevant Parquet files from R2 directly into a Polars DataFrame.Perform your calculations.

---

**User:**

UserA couple notes: I'm using postgres 17 and don't mind using postgres 17 features. Does that influence how we design our tables at all or keep it same?

Quick question, i kinda have this vision of a single dataframe per thermistor pair, which are hierarchically nested inside their sensor, which are hierarchically nested inside their logger, which itself has a "master" logger-level dataframe. So the whole structure isn't really a dataframe, right, it's some kind of nested struct that contains a few dataframes of identical length, right? Are you suggesting we convert this into a single df or am I misunderstanding the storage structure?

**Model:**

Excellent clarifying questions. They get to the heart of the trade-offs between a strict relational model and a more document-oriented one, and how to best represent hierarchical data for analysis.

Let's break it down.

### 1. The Impact of PostgreSQL 17

Yes, the fact that you're using a modern version of Postgres absolutely influences the design. It gives us a powerful tool that maps very well to your hierarchical vision for metadata: the JSONB data type.

While the previous schema is robust and works anywhere, we can create a more flexible and arguably more elegant "hybrid" schema that leverages JSONB.

**Proposed Hybrid Schema for processed_files:**

Column NameData TypeConstraintsDescriptionidBIGSERIALPRIMARY KEYUnique identifier.file_hashTEXTUNIQUE NOT NULL**Indexed.** The primary key for deduplication.processed_atTIMESTAMPTZNOT NULL**Indexed.** For time-series queries.logger_nameTEXTNOT NULL**Indexed.** For fast queries on specific loggers.metadataJSONBNOT NULLA JSON object containing all other file-level metadata.record_countINTEGERNOT NULLNumber of data rows parsed.r2_keysJSONBNOT NULLA JSON object with keys to the raw and parsed files in R2.**How this is different and why it's better for you:**

* **The metadata column:** Instead of having separate logger_type, program_name, table_name columns, we bundle them into a single JSONB object. For example:

```
{
  "logger_type": "CR300",
  "program_name": "CPU:sapflux_2sensor_CR300_30min.cr300",
  "table_name": "SapFlowAll",
  "file_format": "TOA5"
}
```

**The r2_keys column:** Similarly, we can bundle the object keys:

```
{
  "raw": "blake3-hash.dat",
  "parsed": "blake3-hash.parquet"
}
```
**Pros of this Hybrid JSONB Approach:**

1. **Flexibility:** If a new data format adds a new piece of metadata (e.g., "firmware_version"), you **do not need to run a database schema migration (ALTER TABLE)**. Your parser just adds the new key to the JSON object, and it gets stored automatically. This is a massive long-term maintenance advantage.**Aligns with Hierarchy:** It stores the metadata as a single, semi-structured document, which is closer to your mental model.**Powerful Querying:** Postgres has excellent support for JSONB. You can still query inside the JSON blob (WHERE metadata->>'logger_type' = 'CR300') and even create GIN indexes on the JSONB column for high performance.
**Recommendation:** Use this hybrid model. Keep the most critical, high-cardinality fields that you will always filter on (file_hash, processed_at, logger_name) as top-level, indexed columns. Put everything else in the metadata JSONB column.

### 2. The Nested DataFrame Vision vs. Tidy Data for Storage

This is a critical point. Your vision of a nested hierarchy is a perfect description of the **conceptual model** of the data. However, for storage and analysis, translating this into a literal nested structure of DataFrames is an anti-pattern that creates significant problems.

**You are suggesting we should not convert this into a single DataFrame, and you are correct. We should convert it into two flat DataFrames.**

Let's clarify. The goal is to produce a "tidy" or "long format" dataset. This is the gold standard for data analysis and the format that tools like Polars, SQL, and virtually all stats packages are optimized to work with.

A nested structure is hard to work with:How would you get the average alpha for all "outer" thermistors across all loggers and sensors? You'd have to write a complex loop to traverse the nested structure, extract the outer_df from each sensor, and then concatenate them before you could even begin the calculation.

A tidy, relational structure makes this trivial:

**logger_data DataFrame (one row per timestamp per logger):**| timestamp | record_number | battery_voltage ||---|---|---|| 2025-07-29 20:00:00 | 3420 | 13.03 || 2025-07-29 20:30:00 | 3421 | 13.02 |

**sensor_data DataFrame (one row per timestamp per thermistor pair):**| timestamp | sdi12_address | thermistor_id | alpha | beta | ... ||---|---|---|---|---|---|| 2025-07-29 20:00:00 | '0' | "outer" | 0.04496 | 0.04836 | ... || 2025-07-29 20:00:00 | '0' | "inner" | 0.06800 | 0.06304 | ... || 2025-07-29 20:00:00 | '1' | "outer" | 0.08498 | 0.08798 | ... || 2025-07-29 20:00:00 | '1' | "inner" | 0.15103 | 0.16097 | ... || 2025-07-29 20:30:00 | '0' | "outer" | 0.02968 | 0.03360 | ... || ... | ... | ... | ... | ... | ... |

With this structure, the same query becomes a simple, high-performance operation:

```
# Polars pseudo-code
sensor_data.filter(
    pl.col("thermistor_id") == "outer"
).group_by(
    "sdi12_address"
).agg(
    pl.col("alpha").mean()
)
```

**So, to be perfectly clear:**

The parser's output **is not a single DataFrame**. It is a struct containing multiple, flat, relational DataFrames that are linked by common keys (timestamp).

```
// Notional Output Structure
pub struct ParsedSapflowData {
    pub raw_text: String,
    pub metadata: FileMetadata, // Struct that can be serialized to JSONB
    pub logger_data: polars::frame::DataFrame,
    pub sensor_data: polars::frame::DataFrame,
}
```

This is the structure you will serialize to a single Parquet file (Parquet can store multiple tables, but it's often simpler to just join them on load or store them as separate files referenced by the same DB entry). This structure gives you the best of all worlds: it captures the full hierarchy through relational keys, avoids data duplication, and is optimized for the entire downstream pipeline—from serialization to storage to analysis.

---

**User:**

UserRegarding point 2, I understand where you're coming from but I actually disagree. The purpose here is not to prepare data for analysis, the purpose is to organize data in a semantically meaningful way so that I can process it downstream. There are a lot of things you don't know about this dataset. For example, the timestamps are wrong, and I can only fix them by considering the entire dataset together at once, using only the timestamps and logger-level information. So my jump isn't going to be from data storage -> analysis, there are a ton of intermediate steps there that you don't know about yet. Semantically meaningful storage is also going to help me map deployment metadata where appropriate. I would much rather build some kind of hierarchical data type that has a clear rust api, but I'm just not sure how it should be stored. Can I just straight up serialize it to binary and then reload the binary in another rust process? Can I serialize to a struct of things stored as dataframes, but with some additional mapping describing how they're related? What I'm trying to say is that I actually lose information if I merge the file's contents back into a single table here.

**Model:**

You are absolutely right to push back, and thank you for the clarification. This is a crucial distinction. My previous proposal was optimized for an "analysis-ready" state, but your actual next step is a complex, stateful pre-processing stage. You have correctly identified that flattening the data prematurely would destroy the very context you need for that stage.

You are not losing information in a theoretical sense, but you are losing the semantic structure and isolation of the file's contents, which is a form of information that is critical to your workflow. Let's design a system that preserves this.

Your vision is the correct one for this problem. The file is the atomic unit of work, and its internal hierarchy must be preserved.

### The New Design: A Hierarchical Data Container

Instead of thinking about the output as just DataFrames, we will think of it as a **Serializable Data Container**. This container is a Rust struct that perfectly mirrors the hierarchy you described.

#### 1. The Rust API (The In-Memory Structure)

This defines the clear, hierarchical structure your pre-processing code will interact with.

```
// Notional Rust code
use polars::prelude::*;

/// The top-level output of the parser.
/// Represents the complete, structured content of a single source file.
pub struct ParsedFileData {
    pub raw_text: String,
    pub file_metadata: FileMetadata, // The TOA5 header info
    pub logger: LoggerData,
}

/// Contains the logger-level data and owns the collection of sensors from the file.
pub struct LoggerData {
    /// DataFrame with logger-specific columns: timestamp, record_number, batt_v, etc.
    pub df: DataFrame, 
    /// A vector of sensors, preserving the order they appeared in the file.
    pub sensors: Vec<SensorData>,
}

/// Contains sensor-level metadata and owns the thermistor pair data for that sensor.
pub struct SensorData {
    pub sdi12_address: char,
    // Other future sensor-level metadata could go here.
    pub thermistor_pairs: Vec<ThermistorPairData>,
}

/// Contains the thermistor ID and the actual time-series data for that pair.
pub struct ThermistorPairData {
    /// The identifier for this thermistor pair, e.g., "inner" or "outer".
    pub id: String, 
    /// DataFrame containing only the columns for this specific thermistor pair.
    /// Columns: alpha, beta, time_to_max_temp_downstream, etc.
    /// This DataFrame will have the same number of rows as the logger.df.
    pub df: DataFrame,
}
```

This structure is a direct translation of your mental model. It is not a single DataFrame; it's a tree-like structure where the leaves are DataFrames. It explicitly preserves the one-to-many relationships: File -> Logger -> N Sensors -> M Thermistor Pairs.

### 2. The Storage and Serialization Strategy

This is the core of your question. How do we write this hierarchical structure to a file, send it across a process boundary, store it in R2, and read it back without losing the structure?

You absolutely can serialize it. Here are the best options, with a strong recommendation.

#### Option A: The Self-Contained Archive (ZIP + Parquet + JSON Manifest) - **RECOMMENDED**

This is the most robust, transparent, and future-proof method.

**How it works:**The "serialized" output is not a single binary blob, but a standard .zip archive. When your parser "saves" the ParsedFileData struct, it performs the following steps:

1. Creates a temporary directory.Writes the logger.df to a Parquet file: logger.parquet.Iterates through the sensors and thermistor pairs, writing each thermistor_pair.df to a named Parquet file (e.g., sensor_0_inner.parquet, sensor_0_outer.parquet, sensor_1_inner.parquet, etc.).Creates a manifest.json file that describes the hierarchy and metadata. This is the glue that reconstructs the structure.Zips the contents of the directory into a single file, e.g., blake3-hash-of-original-file.zip.
**The manifest.json would look like this:**

```
{
  "file_metadata": {
    "file_format": "TOA5",
    "logger_name": "CR300Series_420",
    "logger_type": "CR300",
    // ...etc
  },
  "structure": {
    "logger_df_path": "logger.parquet",
    "sensors": [
      {
        "sdi12_address": "0",
        "thermistor_pairs": [
          {
            "id": "inner",
            "df_path": "sensor_0_inner.parquet"
          },
          {
            "id": "outer",
            "df_path": "sensor_0_outer.parquet"
          }
        ]
      },
      {
        "sdi12_address": "1",
        "thermistor_pairs": [
           // ... etc
        ]
      }
    ]
  }
}
```

**Why this is the best approach:**

* **Language Agnostic:** It's not a Rust-specific format. A Python script, a Java program, or any other tool can easily unzip the archive, read the JSON manifest, and load the specific Parquet files it needs. This is a massive win for long-term data accessibility.**Transparent & Debuggable:** You can literally unzip the file on your machine and inspect the individual data components with any Parquet-aware tool. There is no "black box."**Uses Industry Standards:** ZIP, JSON, and Parquet are stable, well-supported formats.**Preserves Hierarchy:** The manifest explicitly describes the hierarchical relationships you need to preserve.

#### Option B: A Hierarchical Binary Format (like HDF5 or Zarr)

These formats are designed for this. You could store each DataFrame as a "dataset" and use "groups" to create the directory-like structure.

* **Pros:** Technically elegant; a single file solution.**Cons:** Less common in the general data engineering ecosystem than Parquet. The libraries can be heavy and sometimes complex to install (especially HDF5). It's more of an opaque binary blob than the ZIP archive approach.

#### Option C: Pure Rust Binary Serialization (serde + bincode)

This is what you asked about: "Can I just straight up serialize it to binary and then reload the binary in another rust process?"

* **Answer:** Yes, you can. It would be incredibly fast.**Strong Recommendation:** **Do not do this for archival storage.** This approach creates a tight coupling between your data and the specific version of the Rust structs you used to write it. If you change a struct definition in your code a year from now, you may not be able to read your old data. It also locks you into the Rust ecosystem. This is a great strategy for temporary caching or IPC, but a poor one for long-term storage in R2.

### Summary

1. **Embrace the hierarchical ParsedFileData struct in Rust.** It perfectly matches your semantic needs.**Use the "Self-Contained Archive" (ZIP + Parquet + JSON Manifest) pattern for serialization and storage.** This gives you the structural preservation you need without sacrificing transparency, accessibility, or long-term viability.Your overall Postgres/R2 architecture remains the same. The processed_files table in Postgres will store metadata, and the parsed_data_r2_key will now simply point to a .zip file in R2 instead of a .parquet file. Your pre-processing service can then fetch this zip file, deconstruct it, and perform its complex operations with the full context of the original file perfectly preserved.

---

**User:**

UserFrom here on out, can I see code with absolutely no comments?

I think ThermistorPair id field should be a struct, probably an enum, like we had earlier. If we use string internally we have to worry about whether it's valid. I think the best solution is to put in the sensor config some mapping of strings that "belong" to each enum variant, like "in" and "inn" and "i" for inner. Something like that. So we can go back and forth from config but keep it nicely typed within rust.

I like the zip archive format idea that's cool.

Regarding the postgres table from earlier, I don't like the idea of having a jsonb table for some metadata and putting some metadata at the top level. Let's just make that stuff all flat. I do like the jsonb for r2 pointers though, that's a nice sensible group to put together.

So is this zip archive something we could, say, serde serialize into and crucially deserialize out of in my preprocessing library?

**Model:**

### Rust API and Data Structures

```
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ThermistorDepth {
    Inner,
    Outer,
}

#[derive(Debug, Error)]
#[error("'{0}' is not a valid thermistor depth identifier")]
pub struct InvalidThermistorDepthError(String);

impl TryFrom<&str> for ThermistorDepth {
    type Error = InvalidThermistorDepthError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let lower = value.to_lowercase();
        match lower.as_str() {
            "in" | "inn" | "inner" => Ok(Self::Inner),
            "out" | "outer" => Ok(Self::Outer),
            _ => Err(InvalidThermistorDepthError(value.to_string())),
        }
    }
}

impl AsRef<str> for ThermistorDepth {
    fn as_ref(&self) -> &str {
        match self {
            Self::Inner => "inner",
            Self::Outer => "outer",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_format: String,
    pub logger_name: String,
    pub logger_type: String,
    pub program_name: String,
    pub table_name: String,
}

#[derive(Debug)]
pub struct ParsedFileData {
    pub raw_text: String,
    pub file_metadata: FileMetadata,
    pub logger: LoggerData,
}

#[derive(Debug)]
pub struct LoggerData {
    pub df: DataFrame,
    pub sensors: Vec<SensorData>,
}

#[derive(Debug)]
pub struct SensorData {
    pub sdi12_address: char,
    pub thermistor_pairs: Vec<ThermistorPairData>,
}

#[derive(Debug)]
pub struct ThermistorPairData {
    pub id: ThermistorDepth,
    pub df: DataFrame,
}
```

### PostgreSQL Schema

```
CREATE TABLE processed_files (
    id BIGSERIAL PRIMARY KEY,
    file_hash TEXT UNIQUE NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    original_filename TEXT,
    file_format TEXT NOT NULL,
    logger_name TEXT NOT NULL,
    logger_type TEXT NOT NULL,
    program_name TEXT,
    table_name TEXT,
    record_count INTEGER NOT NULL,
    r2_keys JSONB NOT NULL
);

CREATE INDEX idx_processed_files_hash ON processed_files (file_hash);
CREATE INDEX idx_processed_files_logger_name ON processed_files (logger_name);
CREATE INDEX idx_processed_files_processed_at ON processed_files (processed_at);
```

### Serialization into a ZIP Archive

No, you cannot directly use serde's derive macros to serialize the ParsedFileData struct into a ZIP archive. serde is a framework for data format mapping (like struct-to-JSON), whereas creating a ZIP archive is an I/O orchestration task involving multiple files and formats.

However, you can easily create custom serialization and deserialization functions that use serde for the manifest and Polars' native functions for the DataFrames. This provides a clean API that feels like serde while correctly implementing the archive logic.

Here is the implementation code for those functions.

```
use std::io::{Cursor, Read, Write};
use zip::{write::FileOptions, ZipArchive, ZipWriter};

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    file_metadata: FileMetadata,
    structure: ManifestLogger,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestLogger {
    logger_df_path: String,
    sensors: Vec<ManifestSensor>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestSensor {
    sdi12_address: char,
    thermistor_pairs: Vec<ManifestThermistorPair>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestThermistorPair {
    id: ThermistorDepth,
    df_path: String,
}

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("Polars operation failed: {0}")]
    Polars(#[from] PolarsError),
    #[error("JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ZIP operation failed: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("IO operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Manifest is missing or corrupt")]
    MissingManifest,
    #[error("Data file '{0}' is missing from archive")]
    MissingDataFile(String),
}

impl ParsedFileData {
    pub fn to_zip_archive(&self) -> Result<Vec<u8>, ArchiveError> {
        let mut manifest_sensors = Vec::new();
        for sensor in &self.logger.sensors {
            let mut manifest_pairs = Vec::new();
            for pair in &sensor.thermistor_pairs {
                manifest_pairs.push(ManifestThermistorPair {
                    id: pair.id,
                    df_path: format!(
                        "sensor_{}_{}.parquet",
                        sensor.sdi12_address,
                        pair.id.as_ref()
                    ),
                });
            }
            manifest_sensors.push(ManifestSensor {
                sdi12_address: sensor.sdi12_address,
                thermistor_pairs: manifest_pairs,
            });
        }

        let manifest = Manifest {
            file_metadata: self.file_metadata.clone(),
            structure: ManifestLogger {
                logger_df_path: "logger.parquet".to_string(),
                sensors: manifest_sensors,
            },
        };

        let manifest_bytes = serde_json::to_vec(&manifest)?;
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let mut zip = ZipWriter::new(cursor);
        let options = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        zip.start_file("manifest.json", options)?;
        zip.write_all(&manifest_bytes)?;

        let mut logger_df_clone = self.logger.df.clone();
        let mut logger_parquet_buffer = Vec::new();
        ParquetWriter::new(&mut logger_parquet_buffer).finish(&mut logger_df_clone)?;
        zip.start_file(&manifest.structure.logger_df_path, options)?;
        zip.write_all(&logger_parquet_buffer)?;

        for sensor in &self.logger.sensors {
            for pair in &sensor.thermistor_pairs {
                let path = format!(
                    "sensor_{}_{}.parquet",
                    sensor.sdi12_address,
                    pair.id.as_ref()
                );
                let mut pair_df_clone = pair.df.clone();
                let mut pair_parquet_buffer = Vec::new();
                ParquetWriter::new(&mut pair_parquet_buffer).finish(&mut pair_df_clone)?;
                zip.start_file(&path, options)?;
                zip.write_all(&pair_parquet_buffer)?;
            }
        }

        zip.finish()?;
        Ok(buffer)
    }

    pub fn from_zip_archive(
        zip_bytes: &[u8],
        raw_text: String,
    ) -> Result<Self, ArchiveError> {
        let cursor = Cursor::new(zip_bytes);
        let mut archive = ZipArchive::new(cursor)?;

        let mut manifest_file = archive
            .by_name("manifest.json")
            .map_err(|_| ArchiveError::MissingManifest)?;
        let mut manifest_bytes = Vec::new();
        manifest_file.read_to_end(&mut manifest_bytes)?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;

        let mut logger_df_file = archive
            .by_name(&manifest.structure.logger_df_path)
            .map_err(|_| ArchiveError::MissingDataFile(manifest.structure.logger_df_path.clone()))?;
        let mut logger_df_bytes = Vec::new();
        logger_df_file.read_to_end(&mut logger_df_bytes)?;
        let logger_df = ParquetReader::new(Cursor::new(logger_df_bytes)).finish()?;

        let mut sensors = Vec::new();
        for manifest_sensor in manifest.structure.sensors {
            let mut thermistor_pairs = Vec::new();
            for manifest_pair in manifest_sensor.thermistor_pairs {
                let mut df_file = archive
                    .by_name(&manifest_pair.df_path)
                    .map_err(|_| ArchiveError::MissingDataFile(manifest_pair.df_path.clone()))?;
                let mut df_bytes = Vec::new();
                df_file.read_to_end(&mut df_bytes)?;
                let df = ParquetReader::new(Cursor::new(df_bytes)).finish()?;

                thermistor_pairs.push(ThermistorPairData {
                    id: manifest_pair.id,
                    df,
                });
            }
            sensors.push(SensorData {
                sdi12_address: manifest_sensor.sdi12_address,
                thermistor_pairs,
            });
        }

        Ok(ParsedFileData {
            raw_text,
            file_metadata: manifest.file_metadata,
            logger: LoggerData {
                df: logger_df,
                sensors,
            },
        })
    }
}
```

---

**User:**

UserLooking good.

Couple more things to handle here. The campbell loggers hard-code -99 as an error or missing data value. I would like to replace instances of -99 with a proper missing data type in polars framework. Also, I would like to have some threshold for striking instances of what I call "time travel". Sometimes the logger malfunctions and sends us back in time (e.g. to the unix epoch) or to the future, which obviously can't be true. So I want the parsers to have some configurable bounds on the minimum date (let's say they read from a live config somewhere) and not accept any data that occurs at a timepoint beyond right now. I also think we should flag jumps in the data greater than some bounded number of years, e.g. 2 or 3 years of discontinuity. Thoughts? I think these time travels should be preserved in the raw data but struck from the parsed data, such that all the logger dataframe and thermistorpair dataframes all end up with the same number of rows and keep the same indices. I also think the -99's should stay in the raw data, but be converted to missing numbers in the parsed. Thoughts?

**Model:**

You've identified critical data quality issues that are common in real-world sensor data. Your proposed approach is exactly right: the raw, "dirty" data should be preserved faithfully in one artifact (raw_text), while the parsed, cleaned data should be a separate, analysis-ready artifact. This separation is key to a robust and auditable pipeline.

Let's refine the implementation details for these cleaning and validation steps.

### The Core Principle: A Two-Pass Cleaning Process

The process for handling this will happen in two stages after the initial text-to-DataFrame conversion but before the final ParsedFileData struct is returned.

1. **Initial Parse:** Convert the raw text into the hierarchical structure of Polars DataFrames. This is where -99 is handled.**Validation & Filtering Pass:** Operate on the in-memory DataFrames to handle the timestamp issues.
This ensures all filtering logic is applied consistently across all the DataFrames in the structure.

### 1. Handling -99 as Missing Data

This is a classic data cleaning step that fits perfectly into the initial parsing phase. Polars is designed to handle this efficiently. When reading the data into a DataFrame (whether from a CSV representation or by building Series manually), you will specify that -99 should be interpreted as a null value.

* **Implementation:** The Polars CSV reader has a with_null_values option. When you are constructing the DataFrames, you would configure it to treat -99 as null.**Result:** The df fields in your LoggerData and ThermistorPairData structs will contain Polars' native null representation from the very beginning.**Raw Data:** The raw_text field in ParsedFileData is completely unaffected and will still contain the original -99 values, satisfying your requirement.

### 2. Handling Time Travel and Jumps

This requires a more sophisticated, stateful approach. The parser will need configuration, and its output structure should be enhanced to include validation results.

#### a. Configuration

The parser function's signature will change to accept a configuration object. This makes the bounds and thresholds explicit and easy to manage externally.

```
use chrono::{DateTime, Duration, Utc};

pub struct ParserConfig {
    pub min_valid_date: DateTime<Utc>,
    pub max_time_jump: Duration,
}
```

#### b. Enhanced Output Structure

To "flag" jumps without failing the entire parse, we will add a warnings field to the output struct. This allows the downstream process to be aware of data quality issues and decide how to handle them.

```
// ... existing struct definitions

pub struct ParsedFileData {
    pub raw_text: String,
    pub file_metadata: FileMetadata,
    pub logger: LoggerData,
    pub warnings: Vec<String>, // New field for quality flags
}
```

#### c. The Validation and Filtering Workflow

This is the critical logic that will run after the initial parse and before returning.

**Step 1: Identify Rows to Remove**

The logic will focus only on the logger's timestamp column (logger.df.column("timestamp")).

1. It creates a boolean mask for all rows in the DataFrame.The mask will be false for any timestamp that is: < config.min_valid_date > Utc::now()The mask will be true for all valid timestamps.
**Step 2: Apply the Filter Consistently**

Crucially, this single mask is now used to filter every DataFrame in the structure.

```
// Notional Rust code
let mask = create_time_travel_mask(&logger.df, &config)?;

// Apply the mask to the logger dataframe
logger.df = logger.df.filter(&mask)?;

// Apply the *exact same mask* to every nested dataframe
for sensor in &mut logger.sensors {
    for pair in &mut sensor.thermistor_pairs {
        pair.df = pair.df.filter(&mask)?;
    }
}
```

This is the only way to guarantee that all DataFrames remain the same length and that their rows correspond to the same moments in time.

**Step 3: Detect and Flag Large Jumps**

This check is performed after the invalid "time travel" rows have been removed.

1. The logic gets the timestamp column from the newly filtered logger.df.It iterates through consecutive pairs of timestamps.If the duration between timestamp[n+1] and timestamp[n] is greater than config.max_time_jump, it generates a descriptive warning.This warning is pushed into the warnings vector of the ParsedFileData struct that will be returned.
Example warning: Time jump of 3.1 years detected between 2022-07-08 11:30:00 and 2025-08-20 14:00:00.

### Summary of the Final Process

1. **Ingest:** The parser receives the raw text and a ParserConfig object.**Parse & Nullify:** It parses the text into the hierarchical ParsedFileData structure, converting all instances of -99 in numeric columns to null in the DataFrames.**Filter Time Travel:** It generates a boolean mask based on invalid timestamps in the master logger.df. This mask is applied to the logger.df and all child thermistor_pair.dfs, removing the invalid rows consistently.**Flag Jumps:** It inspects the filtered timestamps for discontinuities exceeding the configured threshold and records them as strings in the warnings field.**Return:** It returns the final ParsedFileData object, which contains the original raw text, the cleaned and filtered DataFrames, and a list of data quality warnings.
This approach fully meets your requirements. It cleanly separates raw from processed, handles data quality issues in a configurable way, guarantees the integrity of the parsed data structure, and enriches the output with actionable quality flags.

---

**User:**

Useractually you know what, let's leave time travel filtering until the rest of the preprocessing steps. That shouldn't be the responsibility of the parser.

Can you write up a detailed plan for the parser, provide the necessary information and context, and describe a little bit about the downstream plans for the data for clarity?

**Model:**

Of course. Moving the time-series validation downstream is a wise design choice. It adheres to the principle of single responsibility: the parser's job is to accurately translate the source format into a structured, clean representation, not to apply complex domain-specific business logic.

Here is a detailed plan for the parser module, including the necessary context and a description of the downstream data flow.

### **Project Plan: Sap Flow Data Parsing Module**

#### 1. Overview and Goals

This document outlines the design and implementation plan for a standalone Rust module responsible for parsing proprietary data files generated by Campbell Scientific dataloggers logging Implexx Sap Flow sensors.

The primary goal is to create a robust, extensible, and high-performance parser that serves as the foundational data ingestion point for a larger data processing pipeline.

**Core Objectives:**

1. **High Fidelity Parsing:** Accurately translate known Campbell TOA5 file formats (specifically CR200-series and CR300-series variants) into a well-defined, hierarchical Rust data structure.**Data Cleaning at Source:** Handle known data quality artifacts, such as the -99 missing data convention, by converting them to a standard null representation during parsing.**Preservation of Source:** The original, unmodified raw text of the input file must be preserved within the parser's output for archival and auditing purposes.**Structural Integrity:** The output format must preserve the semantic hierarchy of the data: a single file contains data from one logger, which has one or more sensors, each of which has multiple thermistor pairs.**Extensibility:** The architecture must allow for new file formats to be added in the future with minimal changes to the core logic.**Standardized Output:** The final output of the module will be a language-agnostic, self-contained ZIP archive, ensuring interoperability with downstream processes.

#### 2. The Downstream Data Pipeline: Context for Design

The parser is the first step in a multi-stage data pipeline. Understanding the downstream requirements is critical to justifying the parser's design, particularly its hierarchical output format.

The planned data flow is as follows:

1. **Ingestion Service:** A service monitors for new data files. Upon arrival, it computes a hash of the file and checks a PostgreSQL database to prevent duplicate processing.**Parsing (This Module):** If the file is new, it is passed to this Rust parser. The parser validates the format and produces a structured, cleaned ZIP archive.**Pre-processing Service:** This is the next critical step. This service consumes the ZIP archive from the parser. It performs complex, context-aware validation and enrichment that cannot be done by the parser alone. Its responsibilities include:**Timestamp Correction:** Using the full logger-level timestamp vector (logger.df), it will detect and correct clock drift or resets (e.g., jumps to the Unix epoch). This requires analyzing the entire time-series as a whole.**Gap Filling & Interpolation:** Identify missing records and apply appropriate interpolation strategies.**Metadata Enrichment:** Join the sensor data with external deployment metadata (e.g., from a separate database) to map sdi12_address and ThermistorDepth to physical installation depths, species information, and experimental treatment groups.**Derived Calculations:** Using the cleaned, enriched data, it will perform the final sap flow calculations with custom, up-to-date parameters, rather than relying on the sensor's default onboard calculations.**Archival & Storage:** The final, processed data is written to a long-term analytical store (e.g., a data warehouse or another set of Parquet files). The PostgreSQL database is updated to record the successful completion of all stages.
This context makes it clear why a prematurely flattened "tidy" DataFrame from the parser is inappropriate. The pre-processing service requires the data to be isolated and structured by its original file hierarchy to correctly perform operations like timestamp correction.

#### 3. Parser Module Architecture

The module will be built around a "Chain of Responsibility" pattern. A central function will try a list of registered parsers in sequence, returning the result from the first one that succeeds.

**Core Components:**

* **SapflowParser Trait:** An interface that all specific format parsers (e.g., Cr300Parser) must implement.

```
pub trait SapflowParser {
    fn name(&self) -> &'static str;
    fn parse(&self, content: &str) -> Result<ParsedFileData, ParserError>;
}
```

**ParserError Enum:** A comprehensive thiserror-based enum to provide clear, actionable error messages for format mismatches, header validation failures, or data row parsing issues.**Orchestration Function:** The main entry point that takes the raw file content, iterates through the registered SapflowParser implementations, and returns the first successful ParsedFileData object or a collection of all errors if none succeed.
**Specific Parser Implementation Logic (for each format):**

Each parser will execute the following steps:

1. **Header Validation:**Confirm the first line begins with "TOA5".Parse file-level metadata from the first header row into the FileMetadata struct.Strictly validate the contents of the "units" and "measurement characteristics" rows (rows 3 and 4) against the expected constant format for that parser. A mismatch results in a FormatMismatch error.**Column Mapping:**Read the column names from the second header row.For each column name, determine what it represents. This logic will identify:Logger-level columns (TIMESTAMP, RECORD, BattV_Min, etc.).Sensor-level columns by matching against known patterns (e.g., S{addr}_AlpOut, AlphaOut{addr}). This match will extract the sdi12_address, the canonical measurement name (alpha), and the ThermistorDepth (Outer).Columns to be ignored (e.g., SapFlwTot1, VhOut1).The result of this step is an internal map from a column's index to its semantic meaning.**Data Ingestion into Columnar Builders:**Initialize a set of mutable vectors (Vec<T>), one for each column that will be kept.Iterate through the data rows of the file (line 5 onwards).For each row, parse the values into their target types.**Crucially, when parsing numeric values, any instance of -99.0 (or -99) will be pushed as a None value into a Vec<Option<f64>>, effectively converting it to a null.Append the parsed values to their corresponding builder vectors.**DataFrame and Hierarchy Construction:**Convert the builder vectors into polars::Series, and then into polars::DataFrames.Assemble the final ParsedFileData struct, nesting the DataFrames according to the hierarchy discovered in the column mapping step.

#### 4. Data Structures & Serialization Format

**In-Memory Rust Structures:**

The primary output is the ParsedFileData struct, which contains the raw text, file metadata, and a nested LoggerData struct. This hierarchy of structs and DataFrames provides the clean, typed API for the pre-processing service. The ThermistorDepth enum provides compile-time safety for identifying thermistor pairs.

**On-Disk/Wire Format (The ZIP Archive):**

The ParsedFileData object is serialized into a single .zip archive. This archive is the atomic output of the parser module.

* **manifest.json:** A JSON file at the root of the archive that describes the entire hierarchy and contains the FileMetadata. It provides pointers (filenames) to the Parquet files for the logger and each thermistor pair.**logger.parquet:** A Parquet file containing the logger-level DataFrame.**sensor_{addr}_{depth}.parquet:** A separate Parquet file for each thermistor pair's DataFrame, named according to its SDI-12 address and depth (e.g., sensor_0_inner.parquet, sensor_1_outer.parquet).
This format is self-describing, language-agnostic, and allows for partial reads (a consumer could read only the manifest and the data for a single sensor if desired).

#### 5. Database Schema

The metadata for each successfully processed file will be stored in a flat relational table in PostgreSQL.

**processed_files Table:**

```
CREATE TABLE processed_files (
    id BIGSERIAL PRIMARY KEY,
    file_hash TEXT UNIQUE NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    original_filename TEXT,
    file_format TEXT NOT NULL,
    logger_name TEXT NOT NULL,
    logger_type TEXT NOT NULL,
    program_name TEXT,
    table_name TEXT,
    record_count INTEGER NOT NULL,
    r2_keys JSONB NOT NULL
);
```

This schema uses top-level, indexed columns for primary query fields (file_hash, logger_name, processed_at) and a JSONB column for the less-frequently queried R2 object keys. This provides a good balance of performance and flexibility.

