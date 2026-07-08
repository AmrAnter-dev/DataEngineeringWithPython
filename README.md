# DataEngineeringWithPython

An end-to-end **Data Engineering** project that captures **SQL Server CDC** changes using **Debezium** and **Apache Kafka**, processes the data with **PySpark**, and stores it in a **Delta Lake** Medallion Architecture (Bronze → Silver).

## Architecture

```text
SQL Server
    │
    │ CDC
    ▼
Debezium Connector
    │
    ▼
Apache Kafka
    │
    ▼
Bronze Layer (Raw Kafka Messages)
    │
    ▼
Silver Layer (Structured Delta Tables)
```

---

## Project Structure

```text
project/
│
├── config/
│   ├── settings.py          # Global configuration
│   └── tables.json          # Tables metadata
│
├── utils/
│   ├── spark_utils.py       # SparkSession initialization
│   ├── schema_utils.py      # SQL Server schema extraction
│   ├── merge_utils.py       # Delta MERGE logic
│   └── logger.py            # Logging utilities
│
├── pipelines/
│   └── silver_pipeline.py   # Bronze → Silver pipeline
│
└── run_pipeline.py          # Application entry point
```

---

## Pipeline Flow

### 1. SQL Server CDC

- Change Data Capture (CDC) is enabled on SQL Server tables.
- SQL Server tracks **INSERT**, **UPDATE**, and **DELETE** operations without modifying the application.

### 2. Debezium

- Debezium continuously monitors SQL Server CDC tables.
- Converts database changes into Kafka events.
- Preserves metadata such as:
  - Database name
  - Schema name
  - Table name
  - Operation type (`c`, `u`, `d`, `r`)
  - Timestamp
  - Before image
  - After image

### 3. Apache Kafka

- Acts as the streaming platform.
- Each SQL Server table is published to its own Kafka topic.
- Guarantees ordered and durable event delivery.

### 4. Bronze Layer

- Consumes raw Kafka messages.
- Stores the complete Debezium event as received.
- No transformations are applied.
- Maintains an immutable history of all events.

### 5. Silver Layer

- Reads raw Bronze data.
- Parses JSON payloads.
- Extracts the actual table schema.
- Applies CDC operations:
  - Insert
  - Update
  - Delete
- Performs incremental upserts using Delta Lake `MERGE`.
- Produces clean, queryable tables.

---

## Technologies

- Python
- PySpark
- Apache Spark
- SQL Server
- SQL Server CDC
- Debezium
- Apache Kafka
- Delta Lake
- JSON Configuration

---

## Features

- End-to-end CDC pipeline
- Real-time data ingestion
- SQL Server Change Data Capture
- Debezium Kafka integration
- Raw Bronze data storage
- Structured Silver tables
- Automatic schema extraction
- Incremental Delta MERGE
- Configuration-driven pipelines
- Modular and reusable codebase
- Production-ready architecture
