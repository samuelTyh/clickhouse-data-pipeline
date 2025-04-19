# AdTech Data Pipeline

## Overview

This project implements a data pipeline for an advertising platform that extracts data from PostgreSQL (operational database), transforms it, and loads it into ClickHouse (analytical database) for efficient reporting and KPI analysis.

## Features

- **Incremental ETL**: Efficiently syncs only new or modified data
- **Optimized Analytics Schema**: ClickHouse schema designed for high-performance analytical queries
- **Real-time KPIs**: Pre-calculated metrics using materialized views
- **Containerized Architecture**: Easy deployment with Docker
- **Configurable**: Customizable sync intervals and parameters


## Project Structure

```
├── docker-compose.yaml      # Docker services configuration
├── etl/                     # ETL pipeline code
│   ├── clickhouse_schema/
│   │   ├── init.sql         # ClickHouse schema definition
│   │   └── kpi_views.sql    # Materialized views for KPIs
│   ├── core/                # Core ETL components
│   │   ├── config.py        # Configuration management
│   │   ├── db.py            # Database connectors
│   │   ├── pipeline.py      # ETL pipeline implementation
│   │   └── schema.py        # Schema management
│   ├── logging_config.py    # Logging setup
│   ├── main.py              # Main service entry point
│   └── requirements.txt     # Python dependencies
├── scripts/                 # Data generation utilities
│   ├── run_tests.py         # Test running script
│   └── service.sh           # Script to control docker services
├── seeder/                  # Data generation utilities
│   ├── main.py              # Seeder CLI
│   ├── migrations/          # Database migrations
│   │   └── V1__create_schema.sql
│   └── seed.py              # Test data generation
├── tests/                   # Tests module
│   ├── test_etl.py          # Unit tests module
│   ├── test_integration.py  # Integration tests module
│   └── test_schema.py       # Schema tests module
└── README.md                # Project documentation
```

### Components

1. **Source Database (PostgreSQL)**
   - Contains operational data for the advertising platform
   - Tables: advertiser, campaign, impressions, clicks

2. **ETL Pipeline**
   - Extracts data from PostgreSQL using incremental updates
   - Transforms data for analytical use
   - Loads data into ClickHouse dimensional model

3. **Analytical Database (ClickHouse)**
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
* [compose](https://docs.docker.com/compose/install/)

### Installation

#### Development
1. Clone the repository:
   ```bash
   git clone https://github.com/samuelTyh/clickhouse-data-pipeline
   cd clickhouse-data-pipeline
   ```

2. Install Python:
   ```bash
   uv python install
   ```

3. Install dependencies:
   ```bash
   uv sync
   ```

4. Grant scripts access
   ```bash
   chmod +x scripts/*.sh
   ```

#### Run testing
   ```bash
   # Unit tests
   ./scripts/run_tests.sh

   # Integration tests
   ./scripts/run_tests.sh --integration

   # Test schema management
   ./scripts/run_tests.sh --schema
   ```


### Usage

#### Control services up or down
   ```bash
   # Start services to create seed data, kick-off initialization and start ETL scheduling
   ./scirpts/service.sh start

   # Stop services and kill docker images, volumes
   ./scripts/service.sh stop

   # Restart services
   ./scripts/service.sh restart

   # Chechk log of service, available services are: etl, postgres, clickhouse, seeder
   ./scripts/service.sh logs [service_name]
   ```

#### Accessing Databases via client

- **PostgreSQL**: Available at `localhost:6543` (credentials in .env.tmpl)
- **ClickHouse**: HTTP interface at `localhost:8124`, native protocol at `localhost:9001`

## Running Queries

### Example ClickHouse Queries

#### Campaign Performance

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

#### Daily Performance

```sql
SELECT 
    event_date,
    impressions,
    clicks,
    daily_ctr
FROM analytics.mv_daily_performance
ORDER BY event_date DESC;
```

#### Campaign Cost Analysis

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


## Design Decisions

### ClickHouse Schema Design

1. **ReplacingMergeTree for Dimensions**
   - Handles updates to dimension data efficiently
   - Maintains history with timestamp versioning

2. **MergeTree with Partitioning for Facts**
   - Partitioned by month for efficient querying and data management
   - Optimized for analytical workloads

3. **Materialized Views for KPIs**
   - Pre-calculation of common metrics
   - Significantly faster query response times

### ETL Process

1. **Incremental Updates**
   - Tracks last sync timestamps for each table
   - Only extracts new or modified data since last sync

2. **Error Handling**
   - Comprehensive logging
   - Graceful recovery from failures

3. **Modular Design**
   - Separation of extract, transform, and load responsibilities
   - Easy to extend with new data sources or targets
