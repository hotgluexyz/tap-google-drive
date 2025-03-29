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
        return [CSVFileStream(tap=self)]


if __name__ == "__main__":
    TapGoogleDrive.cli()
