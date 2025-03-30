"""tap-google-drive target sink."""

from __future__ import annotations

import typing as t

from singer_sdk import Tap
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_google_drive.streams import CSVFileStream
from tap_google_drive.client import GoogleDriveClient


class TapGoogleDrive(Tap):
    """tap-google-drive target class."""

    name = "tap-google-drive"

    config_jsonschema = PropertiesList(
        Property(
            "client_id",
            StringType,
            required=True,
            description="The OAuth 2.0 Client ID",
        ),
        Property(
            "client_secret",
            StringType,
            required=True,
            description="The OAuth 2.0 Client Secret",
        ),
        Property(
            "refresh_token",
            StringType,
            required=True,
            description="The OAuth 2.0 Refresh Token",
        ),
        Property(
            "folder_url",
            StringType,
            required=True,
            description="The Google Drive folder URL containing CSV files",
        ),
    ).to_dict()

    def discover_streams(self) -> t.Sequence[CSVFileStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        # Create a temporary client to list files
        client = GoogleDriveClient(self.config)
        
        # Get folder ID from URL
        folder_id = client.get_folder_id_from_url(self.config["folder_url"])
        
        # List CSV files in the folder
        files = client.list_csv_files(folder_id)
        
        streams = []
        for file in files:
            # Create a stream for each CSV file
            stream = CSVFileStream(
                tap=self,
                file_id=file["id"],
                file_name=file["name"]
            )
            streams.append(stream)
        
        return streams


if __name__ == "__main__":
    TapGoogleDrive.cli()
