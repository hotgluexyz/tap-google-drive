#!/usr/bin/env python3
import io
import json
import argparse
import hashlib
import logging

from pathlib import Path

from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk import Tap

from tap_google_drive.auth import GoogleOAuthAuthenticator

logger = logging.getLogger("tap-google-drive")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

TOKEN_URI = "https://oauth2.googleapis.com/token"


class GoogleDriveTap(Tap):
    name = "tap-google-drive"
    config_jsonschema = {}

    def discover_streams(self):
        return []

    @classmethod
    def access_token_support(cls, connector=None):
        return (GoogleOAuthAuthenticator, TOKEN_URI)


def download_file(real_file_id, creds, config_file_name=None):

    returned_files = {}

    try:
        # create drive api client
        service = build("drive", "v3", credentials=creds)

        file_id = real_file_id

        folders = (
            service.files()
            .list(q="mimeType='application/vnd.google-apps.folder'")
            .execute()
        )

        for folder in folders["files"]:
            if folder["id"] == file_id:
                files_in_folder = (
                    service.files().list(q=f"'{file_id}' in parents").execute()
                )
                for file in files_in_folder["files"]:
                    file, file_name = download_file_data(service, file["id"])
                    returned_files[file_name] = file

        if returned_files == {}:
            file, file_name = download_file_data(service, file_id, config_file_name)
            returned_files[file_name] = file

    except HttpError as error:
        if error.resp.status in (401, 403):
            raise InvalidCredentialsError(
                f"Invalid or expired credentials: {error}"
            ) from error
        logger.exception(f"An error occurred: {error}")
        return {}
    except RefreshError as error:
        raise InvalidCredentialsError(
            f"Failed to refresh Google OAuth token: {error}"
        ) from error

    return returned_files


def download_file_data(service, file_id, config_file_name=None):
    try:
        return download_file_d(service, file_id, config_file_name)
    except Exception:
        return export_file_d(service, file_id)


def download_file_d(service, file_id, config_file_name):
    # Try to get metadata, but if it fails, use file_id as fallback name
    try:
        file_metadata = service.files().get(fileId=file_id).execute()
        file_name = file_metadata["name"]
    except Exception as e:
        logger.warning(f"Could not fetch metadata for {file_id}: {e}. Using config file name as name.")
        file_name = config_file_name

    request = service.files().get_media(fileId=file_id)

    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        logger.info(f'Downloading {file_name} {int(status.progress() * 100)}.')

    return file.getvalue(), file_name


def export_file_d(service, file_id):
    file_metadata = service.files().get(fileId=file_id).execute()
    file_name = file_metadata["name"]
    mime_type = file_metadata.get("mimeType", "")

    # Map Google Workspace mimeTypes to export formats
    mime_type_map = {
        "application/vnd.google-apps.spreadsheet": {
            "export_mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "extension": ".xlsx"
        },
        "application/vnd.google-apps.document": {
            "export_mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "extension": ".docx"
        },
        "application/vnd.google-apps.presentation": {
            "export_mime": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "extension": ".pptx"
        },
        "application/vnd.google-apps.drawing": {
            "export_mime": "application/pdf",
            "extension": ".pdf"
        },
        "application/vnd.google-apps.form": {
            "export_mime": "application/pdf",
            "extension": ".pdf"
        }
    }

    # Get the appropriate export format or default to PDF
    export_config = mime_type_map.get(mime_type, {
        "export_mime": "application/pdf",
        "extension": ".pdf"
    })

    request = service.files().export_media(fileId=file_id, mimeType=export_config["export_mime"])

    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        logger.info(f'Downloading {file_name} {int(status.progress() * 100)}.')

    return file.getvalue(), file_name + export_config["extension"]


def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-s", "--state", help="State file", required=False)
    parser.add_argument(
        "--access-token",
        action="store_true",
        help="Refresh the OAuth access token and update the config file.",
    )

    args = parser.parse_args()
    if args.config:
        setattr(args, "config_path", args.config)
        args.config = load_json(args.config)

    if args.state:
        setattr(args, "state_path", args.state)
        args.state = load_json(args.state)

    return args


def calculate_md5(file_path):
    """Calculate the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def download(args):
    logger.debug("Downloading data...")
    config = args.config
    access_token = config.get("access_token")
    refresh_token = config["refresh_token"]
    client_id = config["client_id"]
    client_secret = config["client_secret"]

    files = config.get("files")
    output_path = config["target_dir"]

    creds = Credentials(
        access_token or None,
        refresh_token=refresh_token,
        token_uri=TOKEN_URI,
        client_id=client_id,
        client_secret=client_secret,
    )

    for file in files:
        file_id = file.get("id")
        file_name = file.get("name")
        files = download_file(file_id, creds, file_name)
        file_name = None

        for k, v in files.items():
            if output_path:
                if output_path[-1] != "/":
                    output_path = output_path + "/"
                file_name = Path(output_path + k)

            with open(file_name or k, "wb") as f:
                f.write(v)

    logger.info("Data downloaded.")


def main():
    # Parse command line arguments
    args = parse_args()

    if args.access_token:
        tap = GoogleDriveTap(config=[Path(args.config_path)], validate_config=False)
        GoogleDriveTap.fetch_access_token(connector=tap)
    else:
        download(args)


if __name__ == "__main__":
    main()
