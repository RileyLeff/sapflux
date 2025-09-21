## Sapflux Pipeline: Project Overview

### Introduction

Sap flux data is invaluable for understanding plant water use, ecosystem hydrology, and the impacts of climate change. However, processing this data from raw logger files into a scientifically valid, analysis-ready dataset is a complex and error-prone task. The process involves handling inconsistent file formats, correcting for unreliable logger clocks, enriching measurements with rich metadata, and applying complex, context-sensitive calculations.

The Sapflux Pipeline project is an initiative to create a robust, transparent, and auditable system to solve these challenges. It is designed from the ground up to transform raw data into verifiable scientific outputs, providing a complete, end-to-end solution for data management and processing.

### Core Goals

The architecture is built on a foundation of four primary goals:

1.  **Verifiability**: It is not enough for a result to be reproducible; it must be independently verifiable. The pipeline's ultimate goal is to produce a **"Reproducibility Cartridge"** for every output, a self-contained package that allows any scientist to re-run the exact processing environment on their own machine and get a bit-for-bit identical result.
2.  **Auditability**: Every change to the system's data or configuration is a permanent, auditable event. The pipeline is built on an immutable ledger, ensuring a complete, linear history of the dataset's evolution can be reviewed and understood at any time. Nothing is ever truly deleted.
3.  **Extensibility**: The pipeline is designed as a modular, component-based system. New file formats (Parsers), new correction and enrichment workflows (Processing Pipelines), and new scientific calculations can be added to the system over time without breaking historical reproducibility.
4.  **Accessibility**: The system provides multiple user interaction surfaces to meet the needs of different users. A powerful Command-Line Interface (CLI) serves data managers and automators, while a user-friendly Web GUI provides tools for visual exploration and guided data management. Both are powered by a single, unified API.

### Architectural Philosophy

To achieve these goals, the system is built on several key architectural principles:

#### The Immutable Ledger

The core of the system is a transaction log that functions like an immutable bank ledger. Every change—from adding a new raw data file to updating a single piece of metadata—is recorded as a permanent, atomic **Transaction**. This approach guarantees that the state of the entire system can be perfectly rebuilt to any point in its history by replaying the transactions in order.

#### Declarative, Transaction-Based Changes

Users do not interact with the database directly. Instead, they create a human-readable **Transaction Manifest** (a `.toml` file) that declaratively describes the desired state changes. They specify *what* they want the end state to be, not *how* to achieve it. The system then validates and applies these changes atomically. "Deleting" or "undoing" data is achieved by submitting a new "reversal transaction" that archives the old data, preserving the full, linear history.

#### Separation of Implementation and Configuration

The system makes a clear distinction between the roles of a Developer and a Data Manager.
*   **Implementation (Code)**: The logic for all components (Parsers, Processing Pipelines, Calculators) is written in Rust and compiled into the application binary. Each component has a unique, hard-coded identifier.
*   **Configuration (Database)**: The database acts as a dynamic "control panel." It does not store logic; it stores an inventory of the compiled-in components and contains flags that a Data Manager can toggle (via transactions) to activate, deactivate, or wire these components together. This "Developer Implements, Manager Activates" workflow is safe, auditable, and highly flexible.

#### The Verifiable Processing Flow

The journey of data through the pipeline is a clear, sequential process:
1.  **Ingestion**: Raw files are submitted in a transaction. They are first deduplicated by their content hash.
2.  **Parsing**: An active `Parser` is selected to transform the raw text into a standardized, in-memory `Data Format`.
3.  **Processing**: The data is handed to an active `Processing Pipeline`, which executes a series of steps:
    *   **Timestamp Correction**: The algorithm corrects for unreliable clocks and DST using the "implied visit" chunking method.
    *   **Metadata Enrichment**: The data is joined with the full metadata hierarchy from the database.
    *   **Calculation**: The final scientific calculations are performed using a hierarchical parameter cascade.
4.  **Output**: The pipeline produces a final `.parquet` data file and its corresponding `Reproducibility Cartridge`, which are stored in the object store.

### How to Learn More

This document is a high-level overview. The other documents in the "planning" folder (e.g. see planning/writeups, planning/notes, and planning/reference_implementations in that order) contain more detailed information.