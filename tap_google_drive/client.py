"""Google Drive API client and file download utilities."""

import io
import logging

from pathlib import Path

from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from hotglue_etl_exceptions import InvalidCredentialsError

from tap_google_drive.auth import build_credentials

logger = logging.getLogger("tap-google-drive")

WORKSPACE_MIME_TYPE_MAP = {
    "application/vnd.google-apps.spreadsheet": {
        "export_mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "extension": ".xlsx",
    },
    "application/vnd.google-apps.document": {
        "export_mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "extension": ".docx",
    },
    "application/vnd.google-apps.presentation": {
        "export_mime": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "extension": ".pptx",
    },
    "application/vnd.google-apps.drawing": {
        "export_mime": "application/pdf",
        "extension": ".pdf",
    },
    "application/vnd.google-apps.form": {
        "export_mime": "application/pdf",
        "extension": ".pdf",
    },
}


def download(config):
    logger.debug("Downloading data...")
    files = config.get("files")
    output_path = config["target_dir"]

    creds = build_credentials(config)

    for file in files:
        file_id = file.get("id")
        file_name = file.get("name")
        downloaded_files = download_file(file_id, creds, file_name)
        file_name = None

        for k, v in downloaded_files.items():
            if output_path:
                if output_path[-1] != "/":
                    output_path = output_path + "/"
                file_name = Path(output_path + k)

            with open(file_name or k, "wb") as f:
                f.write(v)

    logger.info("Data downloaded.")


def download_file(real_file_id, creds, config_file_name=None):
    try:
        service = build("drive", "v3", credentials=creds)
        return _resolve_and_download(service, real_file_id, config_file_name)
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


def _resolve_and_download(service, file_id, config_file_name=None):
    """Detect whether file_id is a folder and download accordingly."""
    folders = (
        service.files()
        .list(q="mimeType='application/vnd.google-apps.folder'")
        .execute()
    )

    for folder in folders["files"]:
        if folder["id"] == file_id:
            return _download_folder_contents(service, file_id)

    file, file_name = download_file_data(service, file_id, config_file_name)
    return {file_name: file}


def _download_folder_contents(service, folder_id):
    files_in_folder = service.files().list(q=f"'{folder_id}' in parents").execute()
    result = {}
    for file in files_in_folder["files"]:
        data, file_name = download_file_data(service, file["id"])
        result[file_name] = data
    return result


def download_file_data(service, file_id, config_file_name=None):
    try:
        return _download_binary(service, file_id, config_file_name)
    except Exception:
        return _export_workspace_file(service, file_id)


def _stream_download(request, file_name):
    """Execute a MediaIoBaseDownload request and return the raw bytes."""
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(f"Downloading {file_name} {int(status.progress() * 100)}.")
    return file.getvalue()


def _download_binary(service, file_id, config_file_name):
    try:
        file_metadata = service.files().get(fileId=file_id).execute()
        file_name = file_metadata["name"]
    except Exception as e:
        logger.warning(f"Could not fetch metadata for {file_id}: {e}. Using config file name as name.")
        file_name = config_file_name

    request = service.files().get_media(fileId=file_id)
    return _stream_download(request, file_name), file_name


def _export_workspace_file(service, file_id):
    file_metadata = service.files().get(fileId=file_id).execute()
    file_name = file_metadata["name"]
    mime_type = file_metadata.get("mimeType", "")

    export_config = WORKSPACE_MIME_TYPE_MAP.get(mime_type, {
        "export_mime": "application/pdf",
        "extension": ".pdf",
    })

    request = service.files().export_media(fileId=file_id, mimeType=export_config["export_mime"])
    return _stream_download(request, file_name), file_name + export_config["extension"]
