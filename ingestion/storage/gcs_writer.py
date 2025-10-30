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

    def write_raw_data(self, data: str | bytes, path: str, filename: str, 
                       include_timestamp: bool = True) -> str:
            """
            Write raw file (any format - json, csv, etc.) to GCS
            
            Args: 
                data: File content as string or bytes
                path: GCS path
                filename: Base filename
                include_timestamp: Whether to append timestamp to filename

            Returns:
                Full GCS URI
            """

            if include_timestamp:
                 timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
                 gcs_path = f"{path}/{timestamp}/{filename}"
            else:
                 gcs_path = f"{path}/{filename}"
        
            blob = self.bucket.blob(gcs_path)

            if isinstance(data, str):
                 blob.upload_from_string(data, content_type='application/json')
            else: 
                 blob.upload_from_string(data)
            
            return f"gs://{self.bucket.name}/{gcs_path}"