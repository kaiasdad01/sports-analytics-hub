import logging
from typing import Literal
import polars as pl
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ingestion.config import get_bigquery_config

logger = logging.getLogger(__name__)

WriteMode = Literal["append", "replace", "truncate"]


class BigQueryLoader:
    """Load data into BigQuery."""
    
    def __init__(self):
        """Initialize BigQuery loader with configuration."""
        self.config = get_bigquery_config()
        self.client = bigquery.Client(
            project=self.config.project_id,
            location=self.config.location
        )
        logger.info(f"Initialized BigQuery client for project: {self.config.project_id}")
    
    def _build_table_id(self, table_name: str, dataset: str) -> str:
        """Build BigQuery table ID from components."""
        return f"{self.config.project_id}.{dataset}.{table_name}"
    
    def load_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        dataset: str,
        write_mode: WriteMode = "append"
    ) -> None:
        """
        Load a Polars DataFrame into BigQuery.
        
        Args:
            df: Polars DataFrame to load
            table_name: Name of the BigQuery table
            dataset: Name of the BigQuery dataset
            write_mode: How to write data ('append', 'replace', or 'truncate')
        """
        if df.is_empty():
            logger.warning(f"DataFrame is empty, skipping load to {dataset}.{table_name}")
            return
        
        table_id = self._build_table_id(table_name, dataset)
        logger.info(f"Loading {len(df)} rows to {table_id} (mode: {write_mode})")
        
        try:
            # Convert Polars to Arrow (BigQuery supports Arrow natively)
            arrow_table = df.to_arrow()
            
            # Configure write disposition
            if write_mode == "append":
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:  # replace or truncate
                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            
            # Configure job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            
            # Load data
            job = self.client.load_table_from_dataframe(
                arrow_table.to_pandas(),  # BigQuery Python client expects pandas
                table_id,
                job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            # Get final table info
            table = self.client.get_table(table_id)
            logger.info(
                f"Successfully loaded {table.num_rows} rows to {table_id}"
            )
            
        except Exception as e:
            logger.error(f"Error loading data to {table_id}: {e}")
            raise
    
    def table_exists(self, table_name: str, dataset: str) -> bool:
        """
        Check if a table exists in BigQuery.
        
        Args:
            table_name: Name of the table
            dataset: Name of the dataset
            
        Returns:
            True if table exists, False otherwise
        """
        table_id = self._build_table_id(table_name, dataset)
        
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False
    
    def delete_table(self, table_name: str, dataset: str) -> None:
        """
        Delete a table from BigQuery.
        
        Args:
            table_name: Name of the table to delete
            dataset: Name of the dataset
        """
        table_id = self._build_table_id(table_name, dataset)
        
        try:
            self.client.delete_table(table_id)
            logger.info(f"Deleted table {table_id}")
        except NotFound:
            logger.warning(f"Table {table_id} not found, nothing to delete")
    
    def get_row_count(self, table_name: str, dataset: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            dataset: Name of the dataset
            
        Returns:
            Number of rows in the table
        """
        table_id = self._build_table_id(table_name, dataset)
        
        try:
            table = self.client.get_table(table_id)
            return table.num_rows
        except NotFound:
            logger.warning(f"Table {table_id} not found")
            return 0