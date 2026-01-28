# Unified-data-onboarding


The Unified Data Onboarding Platform is a configurable, The project ingests real-time cryptocurrency transaction data from Binance using Kafka and WebSocket APIs to local storage and ingested by batch to ADLS manually and processes it through a medallion architecture (Bronze/Silver). It supports multiple-format ingestion (JSON, CSV),  schema validation, data quality enforcement, and Bronze/Silver layering using Apache Spark on Databricks, orchestrated by Azure Data Factory.

1.1 Business Objectives

  - Rapid onboarding of new datasets without code changes
  - Isolation of bad data without pipeline failure
  - Environment portability (local → dev → prod)
  - Observability and auditability
  - Analytics ready data
  
1.2 High-Level Architecture

Source Systems → ADLS (Raw) → Databricks (Bronze) → Databricks (Silver)
                           ↘ Metadata Store
ADF orchestrates execution and passes runtime configuration.

1.3 Architecture Diagram (Conceptual)


![unified-data-onboarding](https://github.com/user-attachments/assets/ce5f1bb0-3a34-44d0-8f5f-906d0bbda8e5)




2. Data Source

Raw data is sourced from Binance transaction streams via the Binance WebSocket API. A Kafka producer consumes real-time market and transaction events and publishes them to Kafka topics. A Kafka consumer persists the raw events to a local landing folder for downstream batch-style processing. (https://github.com/divin-di/Binance_Transaction_Streaming)

3. Low Level Design (LLD)
   
3.1 Code Structure
src/readers        → Input readers (json, csv, universal)
src/validation     → Schema & DQ validation
src/transforms     → Bronze → Silver transformations
src/writers        → Output writers
src/observability  → Metadata logging
src/utils              → Configuration loader
configs/           → Dataset configurations
schemas/           → Schema definitions
src/pipeline/main.py            → Pipeline entrypoint
src/pipeline_logic/dataset_processor.py     →Processing dataset

3.2 process_layer() Design

process_layer() is a reusable execution unit responsible for:
- Reading data
- Applying validation/transformation
- Writing outputs
- Collecting metadata

It supports both Bronze and Silver layers via configuration-driven logic.

3.3 Configuration Model

Each dataset is defined declaratively in YAML. ADF injects the appropriate config path at runtime. This enables onboarding of unlimited datasets without modifying code.

3.4 Error Handling Strategy

- Schema failures → Bronze bad
- DQ failures → Bronze bad
- Pipeline continues processing valid records
- Critical failures logged with status = CRITICAL_ERROR
- 
3.5 Metadata & Observability
  
Each run records:
- run_id
- dataset name
- layer
- record counts
- duration
- status
- records_good
- records_processed
- records_bad
- environment

Metadata is written as parquet for downstream monitoring.

4. Non-Functional Requirements
   
- Scalability: Spark-based execution
- Reliability: Bad data isolation
- Maintainability: Modular design
- Portability: No hard-coded paths
- Cost Efficiency: Batch-first execution
  
5. Deployment & Orchestration
- GitHub as source control
- Databricks Repo sync
- ADLS mounted to Databricks
- ADF triggers Databricks jobs with environment configs
  
6. Future Enhancements
  
- Kafka / streaming ingestion
- Delta Lake
- Gold layer
- Data quality dashboards
- Schema versioning & evolution
