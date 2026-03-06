"""Google Drive tap class."""

from typing import List

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th

from tap_google_drive.auth import GoogleOAuthAuthenticator
from tap_google_drive.client import download

TOKEN_URI = "https://oauth2.googleapis.com/token"


class GoogleDriveTap(Tap):
    """Tap for importing files from Google Drive."""

    name = "tap-google-drive"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="Google OAuth client ID.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="Google OAuth client secret.",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            description="Google OAuth refresh token.",
        ),
        th.Property(
            "access_token",
            th.StringType,
            description="Google OAuth access token (refreshed automatically).",
        ),
        th.Property(
            "files",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType, required=True),
                    th.Property("name", th.StringType),
                )
            ),
            required=True,
            description="List of files to download, each with 'id' and optional 'name'.",
        ),
        th.Property(
            "target_dir",
            th.StringType,
            required=True,
            description="Local directory path where downloaded files are saved.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        return []

    def run(self, catalog=None, state=None):
        download(self.config)

    @classmethod
    def access_token_support(cls, connector=None):
        return (GoogleOAuthAuthenticator, TOKEN_URI)


if __name__ == "__main__":
    GoogleDriveTap.cli()
