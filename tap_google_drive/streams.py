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
)

from tap_google_drive.client import GoogleDriveClient


class CSVFileStream(Stream):
    """Stream for reading CSV files from Google Drive."""

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
            "file_id": Property("file_id", StringType, required=True),
            "file_name": Property("file_name", StringType, required=True),
            "last_modified": Property("last_modified", DateTimeType, required=True),
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
        # Get file content
        content = self.client.get_file_content(self.file_id)
        
        # Get file metadata
        file_metadata = self.client.service.files().get(
            fileId=self.file_id,
            fields="modifiedTime"
        ).execute()

        # Parse CSV content
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            # Convert column names to BigQuery format
            record = {
                self._convert_to_bigquery_name(k): v
                for k, v in row.items()
            }
            # Add metadata
            record.update({
                "file_id": self.file_id,
                "file_name": self.file_name,
                "last_modified": file_metadata["modifiedTime"],
            })
            yield record
