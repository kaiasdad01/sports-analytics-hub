from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class GCSToBigQueryLoader:

    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def load_from_gcs(self, gcs_uri: str, table_id: str, write_mode: str = "replace"):
        """Load from GCS to BigQuery"""
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if write_mode == "replace"
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
            autodetect=True,
        )

        job = self.client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        job.result()

    def get_row_count(self, table_id: str) -> int:
        """Get row count for a table."""
        try:
            table = self.client.get_table(table_id)
            return table.num_rows
        except NotFound:
            return 0