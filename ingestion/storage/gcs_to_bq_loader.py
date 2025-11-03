from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from urllib.parse import urlparse

class GCSToBigQueryLoader:

    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def _infer_source_format(self, gcs_uri: str) -> bigquery.SourceFormat:
        """
        Infer BigQuery source format from file extension.
        
        Args:
            gcs_uri: GCS URI of the file to load
            
        Returns:
            BigQuery SourceFormat enum
        """
        path = urlparse(gcs_uri).path.lower()
        if path.endswith('.parquet'):
            return bigquery.SourceFormat.PARQUET
        if path.endswith('.ndjson') or path.endswith('.json'):
            return bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        if path.endswith('.csv'):
            return bigquery.SourceFormat.CSV
        # Default to PARQUET if unknown
        return bigquery.SourceFormat.PARQUET

    def load_from_gcs(self, gcs_uri: str, table_id: str, write_mode: str = "replace"):
        """
        Load data from GCS to BigQuery with automatic format detection.
        
        Args:
            gcs_uri: GCS URI of the file to load
            table_id: BigQuery table ID (format: project.dataset.table)
            write_mode: "replace" to truncate table, "append" to add rows
        """
        source_format = self._infer_source_format(gcs_uri)
        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if write_mode == "replace"
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
            autodetect=True,
            ignore_unknown_values=True,
        )
        
        # Add CSV-specific configuration for future use 
        if source_format == bigquery.SourceFormat.CSV:
            job_config.field_delimiter = ','
            job_config.skip_leading_rows = 0
            job_config.quote_character = '"'

        job = self.client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        job.result()

    def get_row_count(self, table_id: str) -> int:
        """Get row count for a table."""
        try:
            table = self.client.get_table(table_id)
            return table.num_rows
        except NotFound:
            return 0