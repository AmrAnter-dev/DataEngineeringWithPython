# DataEngineeringWithPython

An end-to-end ETL project built with **Python**, **PySpark**, and **Delta Lake**, following a modular and production-ready project structure.

## Project Structure

```text
project/
│
├── config/
│   ├── settings.py          # Global configuration variables
│   └── tables.json          # Table metadata and pipeline configuration
│
├── utils/
│   ├── spark_utils.py       # SparkSession initialization
│   ├── schema_utils.py      # Schema extraction and management
│   ├── merge_utils.py       # Delta Lake MERGE operations
│   └── logger.py            # Logging utilities
│
├── pipelines/
│   └── silver_pipeline.py   # Bronze → Silver ETL pipeline
│
└── run_pipeline.py          # Application entry point
```

## Project Layers

- **Bronze Layer**
  - Stores raw data ingested from the source.
  - Preserves the original schema and records.

- **Silver Layer**
  - Cleans, validates, and transforms Bronze data.
  - Applies business rules and data quality checks.
  - Performs incremental upserts using Delta Lake `MERGE`.

## Technologies

- Python 3.x
- PySpark
- Delta Lake
- JSON Configuration
- SQL Server (Source)
- Apache Spark

## Features

- Modular architecture
- Configuration-driven pipelines
- Automatic schema extraction
- Incremental loading
- Delta Lake merge support
- Reusable utility modules
- Production-ready project structure
