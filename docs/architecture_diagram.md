## Data Pipeline Architecture (minimal reset)

Paste ONLY the lines between the fences into Mermaid Live, not the backticks.

```mermaid
graph TD
  A[Sources] --> B[Ingestion]
  B --> C[GCS]
  C --> D[BigQuery Staging]
  D --> E[dbt Models]
  E --> F[BigQuery Marts]
  F --> G[Consumers]
```

To view this diagram:
- Open `docs/architecture_diagram.md` in Cursor (it should render Mermaid).
- Or paste the code block into the Mermaid Live Editor (`https://mermaid.live`).
