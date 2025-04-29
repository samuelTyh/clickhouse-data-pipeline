# AdTech Data Pipeline
[![CI Status](https://github.com/samuelTyh/clickhouse-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/samuelTyh/clickhouse-data-pipeline/actions/workflows/ci.yml)

## Overview

This project implements a data pipeline for an advertising platform that extracts data from PostgreSQL (operational database), transforms it, and loads it into ClickHouse (analytical database) for efficient reporting and KPI analysis.

The pipeline supports both batch processing (ETL) and real-time streaming (CDC) approaches:
1. **Batch ETL**: Runs at configurable intervals to sync data incrementally
2. **Real-time CDC**: Uses PostgreSQL WAL (Write-Ahead Logging) and Kafka to stream changes in near real-time

## Features

- **Dual Pipeline Approach**:
  - Batch ETL for reliability and consistency
  - Streaming CDC for real-time analytics
  
- **PostgreSQL Change Data Capture**:
  - Uses native PostgreSQL logical replication (pgoutput)
  - Captures database changes as they happen
  - Minimal impact on source database performance

- **Kafka-based Streaming Architecture**:
  - Reliable message delivery and fault tolerance
  - Decouples source and target systems
  - Enables parallel processing of events
  
- **ClickHouse Analytics**:
  - Optimized schema for high-performance analytical queries
  - Pre-calculated metrics using materialized views
  - Designed for efficient ad-hoc reporting

- **Containerized Architecture**:
  - Easy deployment with Docker Compose
  - Scalable and reproducible setup
  - Isolated components with clear boundaries

- **Comprehensive Testing**:
  - Unit tests for all components
  - Integration tests for end-to-end validation
  - CI pipeline for quality assurance

## Project Structure

```
├── .github/
│   ├── workflows/
│   │   └── ci.yml             # CI script
├── etl/                       # Batch ETL pipeline code
│   ├── clickhouse_schema/
│   │   ├── init.sql           # ClickHouse schema definition
│   │   └── kpi_views.sql      # Materialized views for KPIs
│   ├── core/                  # Core ETL components
│   │   ├── config.py          # Configuration management
│   │   ├── db.py              # Database connectors
│   │   ├── pipeline.py        # ETL pipeline implementation
│   │   └── schema.py          # Schema management
│   ├── Dockerfile.etl         # Docker build definition
│   ├── logging_config.py      # Logging setup
│   ├── main.py                # Main service entry point
│   └── requirements.txt       # Python dependencies
├── scripts/                   # Utility scripts
│   ├── run_tests.sh           # Test running script
│   └── service.sh             # Script to control docker services
├── seeder/                    # Data generation utilities
│   ├── main.py                # Seeder CLI
│   ├── migrations/            # Database migrations
│   │   └── V1__create_schema.sql
│   └── seed.py                # Test data generation
├── stream_etl/                # Streaming ETL pipeline code
│   ├── core/                  # Core streaming components
│   │   ├── config.py          # Configuration management
│   │   ├── consumer.py        # Kafka consumer implementation
│   │   └── processor.py       # Data processor implementation
│   ├── utils/                 # Utility modules
│   │   └── logging_config.py  # Logging setup
│   ├── Dockerfile             # Docker build definition
│   ├── main.py                # Main streaming service entry point
│   └── requirements.txt       # Python dependencies
├── tests/                     # Tests module
│   ├── test_etl.py            # Unit tests for batch ETL
│   ├── test_stream_etl.py     # Unit tests for streaming ETL
│   ├── test_integration.py    # Integration tests
│   └── test_schema.py         # Schema tests
├── .env.tmpl                  # Environment variables template
├── docker-compose.yaml        # Docker services configuration
├── Makefile                   # Unified control plane
└── README.md                  # Project documentation
```

### Components

1. **Source Database (PostgreSQL)**
   - Contains operational data for the advertising platform
   - Configured with logical replication for Change Data Capture (CDC)
   - Tables: advertiser, campaign, impressions, clicks

2. **Batch ETL Pipeline**
   - Extracts data from PostgreSQL using incremental updates
   - Transforms data for analytical use
   - Loads data into ClickHouse dimensional model
   - Runs at configurable intervals (default: 5 minutes)

3. **Streaming ETL Pipeline**
   - Captures database changes in real-time using PostgreSQL pgoutput
   - Uses Debezium for CDC integration with PostgreSQL
   - Processes changes and loads them into ClickHouse in near real-time

4. **Message Queue (Kafka)**
   - Acts as a buffer between PostgreSQL and ClickHouse
   - Enables decoupling and resilience in the pipeline
   - Managed by Zookeeper for coordination

5. **Analytical Database (ClickHouse)**
   - Dimension tables: dim_advertiser, dim_campaign
   - Fact tables: fact_impressions, fact_clicks
   - Materialized views for KPI calculations

## Data Model

### Target Schema (ClickHouse)

```
dim_advertiser
├── advertiser_id
├── name
├── updated_at
└── created_at

dim_campaign
├── campaign_id
├── name
├── bid
├── budget
├── start_date
├── end_date
├── advertiser_id
├── updated_at
└── created_at

fact_impressions
├── impression_id
├── campaign_id
├── event_date
├── event_time
└── created_at

fact_clicks
├── click_id
├── campaign_id
├── event_date
├── event_time
└── created_at
```

## Key KPIs

The following KPIs are implemented as materialized views in ClickHouse:

1. **Campaign CTR (Click-Through Rate)**
   - Measures the ratio of clicks to impressions for each campaign
   - `mv_campaign_ctr`

2. **Daily Performance Metrics**
   - Tracks impressions, clicks, and CTR by date
   - `mv_daily_performance`

3. **Campaign Daily Performance**
   - Provides campaign performance metrics broken down by date
   - `mv_campaign_daily_performance`

4. **Campaign Cost Efficiency**
   - Calculates cost per click based on bid amounts and impressions
   - `mv_campaign_efficiency`

5. **Advertiser Performance Overview**
   - Aggregates performance metrics at the advertiser level
   - `mv_advertiser_performance`

## Setup & Usage

### Prerequisites

* [uv](https://docs.astral.sh/uv/getting-started/installation/)
* [docker](https://docs.docker.com/engine/install/)
* [docker-compose](https://docs.docker.com/compose/install/)
* [GNU Make](https://www.gnu.org/software/make/)

### Installation

#### Development
1. Clone the repository:
   ```bash
   git clone https://github.com/samuelTyh/clickhouse-data-pipeline
   cd clickhouse-data-pipeline
   ```

2. Install Python dependencies:
   ```bash
   uv sync
   ```

3. Grant scripts access for local execution:
   ```bash
   chmod +x scripts/*.sh
   ```

4. Start the services with Docker Compose:
   ```bash
   make start
   ```

#### Run testing
   ```bash
   # Unit tests
   make test

   # Integration tests
   make test type=integration

   # Test schema management
   make test type=schema
   
   # Test streaming ETL
   make test type=streaming
   ```

### Usage

#### Service Management
   ```bash
   # Start services including Kafka, Zookeeper, and Debezium for CDC
   make start

   # Stop services and kill docker images, volumes
   make stop

   # Restart services
   make restart

   # Check status of services
   make status

   # Check log of a specific service
   # Available services are: etl, stream_etl, postgres, clickhouse, 
   # kafka, zookeeper, debezium, seeder
   make logs service=stream_etl
   
   # Check Kafka and Debezium status
   ./scripts/service.sh streaming_status
   ```

#### Accessing Resources

- **PostgreSQL**: Available at `localhost:6543` (credentials in .env.tmpl)
- **ClickHouse**: HTTP interface at `localhost:8124`, native protocol at `localhost:9001`
- **Kafka**: Available at `localhost:9092`
- **Debezium Connect**: REST API at `localhost:8083/connectors`

## Example Queries

### ClickHouse Queries

#### Campaign Performance Overview

```sql
SELECT 
    campaign_id,
    campaign_name,
    advertiser_name,
    impressions,
    clicks,
    ctr
FROM analytics.mv_campaign_ctr
ORDER BY ctr DESC;
```

#### Daily Performance Trend

```sql
SELECT 
    event_date,
    impressions,
    clicks,
    daily_ctr
FROM analytics.mv_daily_performance
ORDER BY event_date DESC;
```

#### Cost Per Click Analysis

```sql
SELECT 
    campaign_id,
    campaign_name,
    bid_amount,
    impressions,
    clicks,
    cost_per_click
FROM analytics.mv_campaign_efficiency
ORDER BY cost_per_click ASC;
```

#### Advertiser Summary

```sql
SELECT
    advertiser_id,
    advertiser_name,
    campaign_count,
    total_budget,
    impressions,
    clicks,
    overall_ctr
FROM analytics.mv_advertiser_performance
ORDER BY overall_ctr DESC;
```

## Change Data Capture (CDC) Setup

The project uses PostgreSQL's logical replication with pgoutput to implement CDC:

1. **PostgreSQL Configuration**:
   - `wal_level = logical`: Enables logical decoding of the WAL
   - `max_wal_senders = 10`: Allows multiple clients to connect to WAL
   - `max_replication_slots = 10`: Supports multiple replication slots

2. **Debezium Connector Setup**:
   - Creates a PostgreSQL publication for target tables
   - Establishes a replication slot named 'debezium'
   - Configures Debezium to use the pgoutput plugin
   - Sets `decimal.handling.mode = string` for proper numeric handling

3. **Kafka Topic Structure**:
   - Each table has its own Kafka topic: 
     - `postgres.public.advertiser`
     - `postgres.public.campaign`
     - `postgres.public.impressions`
     - `postgres.public.clicks`

4. **Stream Processing**:
   - Stream ETL service consumes messages from Kafka topics
   - Processes messages based on operation type (create, update, delete)
   - Loads data into ClickHouse in near real-time

## Design Decisions

### Dual Pipeline Approach

1. **Batch ETL** provides:
   - Reliability and consistency
   - Historical data loading capabilities
   - Resilience against temporary failures

2. **Streaming CDC** provides:
   - Near real-time analytics
   - Lower latency for time-sensitive metrics
   - Reduced load on source database

### PostgreSQL CDC Configuration

1. **Native pgoutput Plugin**:
   - Built into PostgreSQL 10+
   - No external plugins required
   - Official PostgreSQL feature with long-term support

2. **Replication Slots**:
   - Ensures no changes are lost if the consumer is down
   - Maintains position in WAL for resuming
   - Provides durability guarantees

### Kafka as Message Broker

1. **Decoupling**:
   - Source and target systems operate independently
   - Changes in one system don't impact the other
   - Allows for independent scaling

2. **Reliability**:
   - Persistent message storage
   - Exactly-once delivery semantics
   - Built-in partitioning and replication

3. **Scalability**:
   - Handles high-volume data streams
   - Supports multiple consumers
   - Allows for parallel processing

### ClickHouse Schema Design

1. **ReplacingMergeTree for Dimensions**:
   - Efficiently handles updates to dimension data
   - Maintains history with timestamp versioning
   - Performs well for analytical queries

2. **MergeTree with Partitioning for Facts**:
   - Partitioning by month for efficient querying
   - Optimized for append-heavy workloads
   - Good performance for time-series data

3. **Materialized Views for KPIs**:
   - Pre-calculation of common metrics
   - Significantly faster query response times
   - Updates automatically as new data arrives
