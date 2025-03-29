"""Stream type classes for tap-google-drive."""

from __future__ import annotations

import re
import typing as t
from typing import ClassVar

import pandas as pd
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_google_drive.client import GoogleDriveStream


class CSVFileStream(GoogleDriveStream):
    """Stream for a specific CSV file in Google Drive."""

    def __init__(
        self,
        tap,
        file_id: str,
        file_name: str,
        schema: dict,
    ):
        """Initialize the stream.

        Args:
            tap: The tap instance
            file_id: The Google Drive file ID
            file_name: The name of the file
            schema: The schema for this stream
        """
        super().__init__(tap)
        self.file_id = file_id
        self.name = file_name.replace(".csv", "").lower()
        self.schema = schema
        self.primary_keys: ClassVar[list[str]] = ["id"]
        self.replication_key = "modifiedTime"

    @property
    def path(self) -> str:
        """Return the API path for this stream."""
        return f"/files/{self.file_id}"

    def get_url_params(
        self,
        context: t.Optional[dict] = None,
        next_page_token: t.Optional[str] = None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page token.

        Returns:
            A dictionary of URL query parameters.
        """
        params = super().get_url_params(context, next_page_token)
        params["fields"] = "id, name, modifiedTime"
        return params

    def _get_file_content(self) -> pd.DataFrame:
        """Get the content of the CSV file.

        Returns:
            A pandas DataFrame containing the file content.
        """
        response = self._request(
            context=None,
            url=f"{self.url_base}/files/{self.file_id}",
            params={"alt": "media"},
        )
        return pd.read_csv(pd.io.common.BytesIO(response.content))

    def get_records(self, context: t.Optional[dict] = None) -> t.Iterable[dict]:
        """Get records from the stream.

        Args:
            context: The stream context.

        Yields:
            Each record from the source.
        """
        df = self._get_file_content()
        
        # Convert column names to BigQuery format
        df.columns = [self._convert_to_bigquery_column_name(col) for col in df.columns]
        
        # Add metadata columns
        df["id"] = self.file_id
        df["modifiedTime"] = self._get_modified_time()
        
        # Convert DataFrame to records
        for _, row in df.iterrows():
            yield row.to_dict()

    def _get_modified_time(self) -> str:
        """Get the last modified time of the file.

        Returns:
            The last modified time as an ISO format string.
        """
        response = self._request(
            context=None,
            url=f"{self.url_base}/files/{self.file_id}",
            params={"fields": "modifiedTime"},
        )
        return response.json()["modifiedTime"]

    def _convert_to_bigquery_column_name(self, column_name: str) -> str:
        """Convert column name to BigQuery compliant format.

        Args:
            column_name: The original column name.

        Returns:
            The converted column name.
        """
        # Remove special characters and spaces
        column_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
        # Ensure it starts with a letter
        if not column_name[0].isalpha():
            column_name = 'col_' + column_name
        # Convert to lowercase
        column_name = column_name.lower()
        return column_name


def get_csv_streams(tap) -> list[CSVFileStream]:
    """Get a list of CSV file streams from the configured folder.

    Args:
        tap: The tap instance.

    Returns:
        A list of CSVFileStream instances.
    """
    # Create a temporary stream to get the service
    temp_stream = CSVFileStream(tap, "", "", {})
    service = temp_stream.authenticator.service

    # List all CSV files in the folder
    folder_id = tap.folder_id
    results = service.files().list(
        q=f"'{folder_id}' in parents and mimeType='text/csv'",
        fields="files(id, name)",
        spaces='drive'
    ).execute()
    
    files = results.get('files', [])
    streams = []
    
    for file in files:
        # Read CSV file to get schema
        file_content = service.files().get_media(fileId=file['id']).execute()
        df = pd.read_csv(pd.io.common.BytesIO(file_content))
        
        # Convert column names to BigQuery format
        df.columns = [temp_stream._convert_to_bigquery_column_name(col) for col in df.columns]
        
        # Create schema from DataFrame
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "modifiedTime": {"type": "string", "format": "date-time"},
                **{col: {"type": "string"} for col in df.columns}
            },
            "required": ["id", "modifiedTime"] + list(df.columns)
        }
        
        # Create stream for this file
        stream = CSVFileStream(
            tap=tap,
            file_id=file['id'],
            file_name=file['name'],
            schema=schema
        )
        streams.append(stream)
    
    return streams
