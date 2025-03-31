"""Stream definitions for tap-google-drive."""

from __future__ import annotations

import csv
import io
import re
from typing import Any, Dict, Iterable, Optional

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
    IntegerType,
)

from tap_google_drive.client import GoogleDriveClient


class CSVFileStream(Stream):
    """Stream for reading CSV files from Google Drive."""

    # Add replication key for state management
    replication_key = "_gn_last_modified"
    is_timestamp_replication_key = True
    primary_keys = ["_gn_file_id", "_gn_row_number"]  # Add row_number to uniquely identify each record

    def __init__(
        self,
        tap: Tap,
        file_id: str,
        file_name: str,
    ):
        """Initialize the stream.

        Args:
            tap: The Tap instance.
            file_id: The Google Drive file ID.
            file_name: The file name.
        """
        # Store file info
        self.file_id = file_id
        self.file_name = file_name
        
        # Initialize client
        self.client = GoogleDriveClient(tap.config)
        
        # Get initial schema from CSV headers
        content = self.client.get_file_content(self.file_id)
        reader = csv.reader(io.StringIO(content))
        self._headers = next(reader)  # Get the headers
        
        # Convert file name to BigQuery-compliant name
        name = self._convert_to_bigquery_name(file_name)
        
        # Initialize parent class
        super().__init__(tap, name=name)

    @property
    def schema(self) -> dict:
        """Get stream schema.

        Returns:
            Stream schema.
        """
        # Convert headers to BigQuery-compliant names
        headers = [self._convert_to_bigquery_name(header) for header in self._headers]

        # Create schema properties
        properties = {
            header: Property(header, StringType, required=True)
            for header in headers
        }

        # Add metadata fields
        properties.update({
            "_gn_file_id": Property("_gn_file_id", StringType, required=True),
            "_gn_filename": Property("_gn_filename", StringType, required=True),
            "_gn_last_modified": Property("_gn_last_modified", DateTimeType, required=True),
            "_gn_row_number": Property("_gn_row_number", IntegerType, required=True),  # Add row_number to schema
        })

        return PropertiesList(*properties.values()).to_dict()

    @staticmethod
    def _convert_to_bigquery_name(name: str) -> str:
        """Convert a name to BigQuery-compliant format.

        Args:
            name: The original name.

        Returns:
            The BigQuery-compliant name.
        """
        # Remove file extension
        name = name.replace('.csv', '')
        # Replace special characters with underscore
        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure it starts with a letter
        if not name[0].isalpha():
            name = 'table_' + name
        # Convert to lowercase
        return name.lower()

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Get records from the stream.

        Args:
            context: The context for the stream.

        Yields:
            A dictionary for each record.
        """
        # Get file metadata first to check if we need to process
        file_metadata = self.client.service.files().get(
            fileId=self.file_id,
            fields="modifiedTime"
        ).execute()
        
        current_modified_time = file_metadata["modifiedTime"]
        self.logger.info(f"Processing file: {self.file_name}")
        self.logger.info(f"Current modified time: {current_modified_time}")
        
        # Get the last processed timestamp from state
        start_time = None
        self.logger.info(f"Context received: {context}")
        start_time = self.get_starting_replication_key_value(context)
        self.logger.info(f"Start time from state: {start_time}")
        
        if start_time and current_modified_time <= start_time:
            self.logger.info(f"Skipping file {self.file_name} - no changes since last run")
            return
        else:
            self.logger.info(f"Processing file {self.file_name} - changes detected or no previous state")

        # Get file content if it's new or modified
        content = self.client.get_file_content(self.file_id)
        
        # Parse CSV content
        reader = csv.DictReader(io.StringIO(content))
        record_count = 0
        for row_number, row in enumerate(reader, start=1):
            # Convert column names to BigQuery format
            record = {
                self._convert_to_bigquery_name(k): v
                for k, v in row.items()
            }
            # Add metadata
            record.update({
                "_gn_file_id": self.file_id,
                "_gn_filename": self.file_name,
                "_gn_last_modified": current_modified_time,
                "_gn_row_number": row_number,  # Add row number to uniquely identify each record
            })
            record_count += 1
            yield record
            
        self.logger.info(f"Processed {record_count} records from {self.file_name}")
