"""Google Drive tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_google_drive import streams


class TapGoogleDrive(Tap):
    """Google Drive tap class."""

    name = "tap-google-drive"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="OAuth2 client ID for Google Drive API",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="OAuth2 client secret for Google Drive API",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="OAuth2 refresh token for Google Drive API",
        ),
        th.Property(
            "folder_url",
            th.StringType,
            required=True,
            description="The URL of the Google Drive folder to sync (e.g., https://drive.google.com/drive/folders/your-folder-id)",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.CSVFileStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return streams.get_csv_streams(self)


if __name__ == "__main__":
    TapGoogleDrive.cli()
