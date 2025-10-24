from google.cloud import storage
from datetime import datetime, timezone
import polars as pl

class GCSWriter:
    def __init__(self, bucket_name: str, project_id: str = None):
        self.client = storage.Client(project=project_id) if project_id else storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def write(self, data: pl.DataFrame, path: str) -> str:
        """Write dataframe to GCS as parquet"""
        import io

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        gcs_path = f"{path}/{timestamp}.parquet"

        # Write to in-memory buffer
        buffer = io.BytesIO()
        data.write_parquet(buffer, compression="snappy")
        parquet_bytes = buffer.getvalue()

        blob = self.bucket.blob(gcs_path)
        blob.upload_from_string(parquet_bytes)

        return f"gs://{self.bucket.name}/{gcs_path}"