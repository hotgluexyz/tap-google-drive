"""Stream definitions for tap-google-drive."""

from __future__ import annotations

import csv
import io
from typing import Any, Dict, Iterable, Optional

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_google_drive.client import GoogleDriveClient


class CSVFileStream(Stream):
    """Stream for reading CSV files from Google Drive."""

    name = "csv_files"
    schema = PropertiesList(
        Property("file_id", StringType, required=True),
        Property("file_name", StringType, required=True),
        Property("content", StringType, required=True),
        Property("last_modified", DateTimeType, required=True),
    ).to_dict()

    def __init__(
        self,
        tap: Tap,
        name: Optional[str] = None,
        schema: Optional[Dict] = None,
        key_properties: Optional[list[str]] = None,
    ):
        """Initialize the stream.

        Args:
            tap: The Tap instance.
            name: The name of the stream.
            schema: The schema of the stream.
            key_properties: The key properties of the stream.
        """
        super().__init__(tap, name, schema, key_properties)
        self.client = GoogleDriveClient(tap.config)

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Get records from the stream.

        Args:
            context: The context for the stream.

        Yields:
            A dictionary for each record.
        """
        # Get folder ID from URL
        folder_id = self.client.get_folder_id_from_url(self.config["folder_url"])

        # List CSV files in the folder
        files = self.client.list_csv_files(folder_id)

        for file in files:
            # Get file content
            content = self.client.get_file_content(file["id"])
            
            # Get file metadata
            file_metadata = self.client.service.files().get(
                fileId=file["id"],
                fields="modifiedTime"
            ).execute()

            yield {
                "file_id": file["id"],
                "file_name": file["name"],
                "content": content,
                "last_modified": file_metadata["modifiedTime"],
            }
