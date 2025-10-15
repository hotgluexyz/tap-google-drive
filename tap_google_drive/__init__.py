#!/usr/bin/env python3
import os
import json
import argparse
import logging

from pathlib import Path
from io import StringIO
import hashlib

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


logger = logging.getLogger("tap-google-drive")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

import io

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


def download_file(real_file_id, creds, config_file_name=None):
    """Downloads a file
    Args:
        real_file_id: ID of the file to download
    Returns : IO object with location.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """

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
        logger.exception(f"An error occurred: {error}")
        file = None

    return returned_files


def download_file_data(service, file_id, config_file_name=None):
    try:
        return download_file_d(service, file_id, config_file_name)
    except Exception as e:
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
    """Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", help="Config file", required=True)

    parser.add_argument("-s", "--state", help="State file", required=False)

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
    logger.debug(f"Downloading data...")
    config = args.config
    access_token = config.get("access_token")
    refresh_token = config["refresh_token"]
    client_id = config["client_id"]
    client_secret = config["client_secret"]

    files = config.get("files")
    output_path = config["target_dir"]

    creds = Credentials(
        access_token or "",
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
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

    logger.info(f"Data downloaded.")


def main():
    # Parse command line arguments
    args = parse_args()

    # Download the data
    download(args)


if __name__ == "__main__":
    main()
