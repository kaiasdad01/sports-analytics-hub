# NFL Analytics Platform - Architecture Diagram

## System Architecture Overview

```mermaid
graph TB
    subgraph "Data Sources"
        A1[NFL.com]
        A2[Pro Football Reference]
        A3[ESPN]
        A4[Other NFLverse Sources]
    end

    subgraph "Ingestion Layer - Python"
        B1[nflreadpy Library]
        B2[NFLExtractor Class]
        B3[Polars DataFrames]
    end

    subgraph "Landing Zone - GCS"
        C1[GCS Bucket: nfl-analytics-dev]
        C2[Path: raw/nfl/data_type/season=year/]
        C3[Format: Snappy Parquet]
    end

    subgraph "Raw Data Warehouse - BigQuery"
        D1[Dataset: nfl_raw]
        D2[Tables: play_by_play, player_stats,<br/>schedules, rosters, teams, etc.]
    end

    subgraph "Transformation Layer - dbt"
        E1[Staging Models - 18 models]
        E2[stg_play_by_play, stg_player_stats,<br/>stg_schedules, etc.]
        E3[Marts Layer - 4 models]
        E4[player_game_performance_mart]
        E5[player_season_stats_mart]
        E6[team_game_performance_mart]
        E7[team_season_stats_mart]
    end

    subgraph "Analytics Layer - BigQuery"
        F1[Dataset: nfl_analytics]
        F2[Business-Ready Tables]
    end

    subgraph "Consumption Layer - Planned"
        G1[Jupyter Notebooks]
        G2[FastAPI - REST API]
        G3[Streamlit - Dashboards]
        G4[ML Models - MLflow]
    end

    subgraph "Infrastructure as Code"
        H1[Terraform]
        H2[GCP Resources:<br/>BigQuery Datasets,<br/>GCS Buckets,<br/>IAM/Service Accounts]
    end

    A1 & A2 & A3 & A4 --> B1
    B1 --> B2
    B2 --> B3
    B3 --> C1
    C1 --> C2
    C2 --> C3
    C3 --> D1
    D1 --> D2
    D2 --> E1
    E1 --> E2
    E2 --> E3
    E3 --> E4 & E5 & E6 & E7
    E4 & E5 & E6 & E7 --> F1
    F1 --> F2
    F2 --> G1 & G2 & G3 & G4
    H1 -.manages.-> H2
    H2 -.provisions.-> C1
    H2 -.provisions.-> D1
    H2 -.provisions.-> F1

    style B2 fill:#4285F4,color:#fff
    style C1 fill:#FBBC04,color:#000
    style D1 fill:#4285F4,color:#fff
    style E1 fill:#34A853,color:#fff
    style E3 fill:#34A853,color:#fff
    style F1 fill:#4285F4,color:#fff
    style G2 fill:#EA4335,color:#fff,stroke-dasharray: 5 5
    style G3 fill:#EA4335,color:#fff,stroke-dasharray: 5 5
    style G4 fill:#EA4335,color:#fff,stroke-dasharray: 5 5
    style H1 fill:#7B68EE,color:#fff
```

## Detailed Data Flow

```mermaid
flowchart LR
    subgraph "1. Extraction"
        A[nfl_data_ingest.py<br/>Script Execution]
        B[NFLExtractor<br/>20+ Data Types<br/>Seasons 2020-2025]
    end

    subgraph "2. Storage"
        C[GCSWriter<br/>Upload Parquet]
        D[GCS Bucket<br/>Timestamped Files<br/>Lifecycle: 90d → COLDLINE]
    end

    subgraph "3. Loading"
        E[GCSToBigQueryLoader<br/>Schema Autodetection<br/>WRITE_TRUNCATE]
        F[BigQuery nfl_raw<br/>17+ Raw Tables]
    end

    subgraph "4. Transformation"
        G[dbt run<br/>Staging Layer]
        H[dbt run<br/>Marts Layer]
    end

    subgraph "5. Analytics"
        I[BigQuery nfl_analytics<br/>Marts Tables]
        J[Direct SQL Queries<br/>Jupyter Analysis<br/>Future: API/Dashboards]
    end

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    I --> J

    style A fill:#4285F4,color:#fff
    style D fill:#FBBC04,color:#000
    style F fill:#4285F4,color:#fff
    style G fill:#34A853,color:#fff
    style H fill:#34A853,color:#fff
    style I fill:#4285F4,color:#fff
```

## Data Model Layers

```mermaid
graph LR
    subgraph "Raw Layer"
        R1[play_by_play<br/>387 fields]
        R2[player_stats<br/>123 fields]
        R3[schedules]
        R4[rosters]
        R5[teams]
        R6[players]
        R7[nextgen_stats]
        R8[combine]
        R9[contracts]
        R10[draft_picks]
        R11[+ 7 more tables]
    end

    subgraph "Staging Layer"
        S1[stg_play_by_play]
        S2[stg_player_stats]
        S3[stg_schedules]
        S4[stg_rosters]
        S5[stg_teams]
        S6[stg_players]
        S7[stg_nextgen_stats]
        S8[+ 11 more models]
    end

    subgraph "Marts Layer"
        M1[player_game_performance_mart<br/>Join: stats + schedules + players<br/>+ nextgen + fantasy]
        M2[player_season_stats_mart<br/>Aggregate: player season totals]
        M3[team_game_performance_mart<br/>Source: play-by-play<br/>Metrics: EPA, success rate, efficiency]
        M4[team_season_stats_mart<br/>Aggregate: team season totals]
    end

    R1 --> S1
    R2 --> S2
    R3 --> S3
    R4 --> S4
    R5 --> S5
    R6 --> S6
    R7 --> S7

    S1 & S2 & S3 & S6 & S7 --> M1
    M1 --> M2
    S1 & S3 & S5 --> M3
    M3 --> M4

    style R1 fill:#E8F0FE,color:#000
    style R2 fill:#E8F0FE,color:#000
    style S1 fill:#E6F4EA,color:#000
    style S2 fill:#E6F4EA,color:#000
    style M1 fill:#FFF4E5,color:#000
    style M2 fill:#FFF4E5,color:#000
    style M3 fill:#FFF4E5,color:#000
    style M4 fill:#FFF4E5,color:#000
```

## Technology Stack

```mermaid
graph TB
    subgraph "Languages & Core"
        T1[Python 3.11+]
        T2[SQL - dbt Models]
    end

    subgraph "Data Processing"
        T3[Polars<br/>High-performance DataFrames]
        T4[nflreadpy<br/>NFL Data Acquisition]
        T5[httpx<br/>HTTP Client]
    end

    subgraph "Cloud Platform - GCP"
        T6[BigQuery<br/>Data Warehouse]
        T7[Cloud Storage<br/>Data Lake]
        T8[Service Accounts<br/>IAM]
    end

    subgraph "Infrastructure"
        T9[Terraform<br/>IaC for GCP]
    end

    subgraph "Transformation"
        T10[dbt-core 1.10.13]
        T11[dbt-bigquery 1.10.2]
        T12[dbt-utils]
    end

    subgraph "Planned/Future"
        T13[FastAPI<br/>REST API]
        T14[Streamlit<br/>Dashboards]
        T15[MLflow<br/>ML Lifecycle]
        T16[Prefect<br/>Orchestration]
        T17[scikit-learn<br/>XGBoost]
    end

    subgraph "Development Tools"
        T18[uv<br/>Package Manager]
        T19[Ruff<br/>Linting]
        T20[pytest<br/>Testing]
        T21[Jupyter<br/>Notebooks]
    end

    style T6 fill:#4285F4,color:#fff
    style T7 fill:#FBBC04,color:#000
    style T10 fill:#34A853,color:#fff
    style T13 fill:#EA4335,color:#fff,stroke-dasharray: 5 5
    style T14 fill:#EA4335,color:#fff,stroke-dasharray: 5 5
    style T15 fill:#EA4335,color:#fff,stroke-dasharray: 5 5
    style T16 fill:#EA4335,color:#fff,stroke-dasharray: 5 5
```

## Data Types Ingested

### Core Game Data
- **play_by_play** - Play-level data with EPA, WPA metrics
- **schedules** - Game schedules and results

### Player & Roster Data
- **rosters** - Season-level rosters
- **rosters_weekly** - Week-by-week roster changes
- **players** - Player biographical data
- **depth_charts** - Position depth
- **injuries** - Injury reports
- **trades** - Player trades

### Performance Data
- **player_stats** - Offensive/defensive/special teams stats
- **snap_counts** - Playing time metrics
- **nextgen_stats** - Advanced tracking metrics
- **participation** - Player participation data

### League Context Data
- **officials** - Referee assignments
- **combine** - Pre-draft measurements
- **draft_picks** - Draft history
- **contracts** - Player contracts

### Fantasy Football Data
- **ff_player_ids** - Fantasy platform mapping
- **ff_opportunity** - Fantasy opportunity metrics
- **ff_rankings** - Fantasy rankings

### Teams Data
- **teams** - Franchise information

## Execution Workflow

```mermaid
sequenceDiagram
    actor User
    participant Script as nfl_data_ingest.py
    participant Extractor as NFLExtractor
    participant GCS as Google Cloud Storage
    participant Loader as GCSToBigQueryLoader
    participant BQ as BigQuery (nfl_raw)
    participant dbt as dbt
    participant Analytics as BigQuery (nfl_analytics)

    User->>Script: Execute manually
    loop For each data type
        Script->>Extractor: extract_data(data_type, seasons)
        Extractor->>Extractor: Call nflreadpy
        Extractor-->>Script: Polars DataFrame
        Script->>GCS: write_parquet(df, path)
        GCS-->>Script: Success
        Script->>Loader: load_to_bigquery(gcs_uri, table)
        Loader->>BQ: Load with schema autodetect
        BQ-->>Loader: Success
        Loader-->>Script: Row count validation
    end
    Script-->>User: Ingestion complete

    User->>dbt: dbt run
    dbt->>BQ: Query nfl_raw tables
    dbt->>Analytics: Create staging models
    dbt->>Analytics: Create mart models
    dbt-->>User: Transformation complete

    User->>Analytics: Query marts or run notebooks
    Analytics-->>User: Analysis results
```

## Key Design Patterns

### ELT (Extract, Load, Transform)
- Extract data from sources via nflreadpy
- Load raw data to GCS and BigQuery
- Transform data using dbt SQL models in BigQuery

### Layered Architecture
1. **Raw Layer** - Immutable source data
2. **Staging Layer** - Cleaned and standardized
3. **Marts Layer** - Business logic and aggregations
4. **ML Layer** - Feature engineering (planned)

### Data Quality & Governance
- **Immutability** - GCS serves as immutable data lake
- **Versioning** - GCS versioning enabled
- **Lineage** - dbt tracks data lineage through refs
- **Idempotency** - Full refresh strategy (reproducible)

### Infrastructure Management
- **Infrastructure as Code** - Terraform manages all GCP resources
- **Environment Separation** - Dev/prod via Terraform variables
- **Security** - Service accounts with minimal permissions
- **Cost Optimization** - Lifecycle policies (90d → COLDLINE)

## Current State & Roadmap

### ✅ Implemented
- Data ingestion from 20+ NFL data sources
- GCS data lake with Parquet storage
- BigQuery raw data warehouse
- dbt staging models (18 models)
- dbt mart models (4 models)
- Terraform infrastructure provisioning
- Player and team performance analytics

### 🚧 Planned/In Progress
- Incremental loading (currently full refresh only)
- Workflow orchestration with Prefect
- Data quality tests in dbt
- FastAPI REST API layer
- Streamlit dashboards
- ML models with MLflow
- CI/CD pipeline
- Enhanced error handling and monitoring

## File References

### Key Configuration Files
- `ingestion/config.py` - Ingestion settings
- `dbt_project/dbt_project.yml` - dbt configuration
- `dbt_project/profiles.yml` - BigQuery connection
- `.env` - Environment variables
- `pyproject.toml` - Python dependencies

### Main Scripts
- `scripts/nfl_data_ingest.py` - Main ingestion script
- `ingestion/nfl/extractor.py` - NFLExtractor class
- `ingestion/writers/gcs_writer.py` - GCS writer
- `ingestion/loaders/gcs_to_bigquery.py` - BigQuery loader

### dbt Models
- `dbt_project/models/staging/nfl/*.sql` - Staging models (18)
- `dbt_project/models/marts/nfl/*.sql` - Mart models (4)
- `dbt_project/models/staging/nfl/sources.yml` - Source definitions

### Infrastructure
- `infrastructure/terraform/*.tf` - Terraform configurations
