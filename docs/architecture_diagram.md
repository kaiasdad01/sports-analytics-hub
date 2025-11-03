## Data Pipeline Architecture

Sanity check (should render a single arrow):

```mermaid
graph TD
  A[Start] --> B[End]
```

```mermaid
flowchart TD
  subgraph Airflow
    A1[nfl_data_extract.py]
    A2[nfl_data_load.py]
    A3[nfl_dbt_transform.py]
  end

  subgraph Sources
    S1[NFL APIs]
    S2[NFL fines website]
  end

  subgraph Ingestion
    I1[Extractor - ingestion/nfl/extractor.py]
    I2[Scraper - ingestion/scrapers/nfl_fines_scraper.py]
    I3[GCS Writer - ingestion/storage/gcs_writer.py]
  end

  subgraph Storage
    GCS[(GCS bucket)]
  end

  subgraph Warehouse
    L1[GCS to BigQuery - ingestion/storage/gcs_to_bq_loader.py]
    STG[(BigQuery staging)]
    MART[(BigQuery marts)]
  end

  subgraph dbt
    D2[staging models]
    D3[marts models]
  end

  subgraph Consumers
    NBs[notebooks/]
    ML[Downstream apps / ML]
  end

  S1 --> I1
  S2 --> I2
  A1 --> I1
  A1 --> I2
  I1 --> I3
  I2 --> I3
  I3 --> GCS
  A2 --> L1
  GCS --> L1
  L1 --> STG
  A3 --> D2
  A3 --> D3
  STG --> D2
  D2 --> D3
  D3 --> MART
  MART --> NBs
  MART --> ML
```

To view this diagram:
- Open `docs/architecture_diagram.md` in Cursor (it should render Mermaid).
- Or paste the code block into the Mermaid Live Editor (`https://mermaid.live`).
