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

def unique_list(items, key=None):
    seen = set()
    result = []
    for item in items:
        val = key(item) if key else item
        if val not in seen:
            seen.add(val)
            result.append(item)
    return result


def download_file(real_file_id, creds):
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
            file, file_name = download_file_data(service, file_id)
            returned_files[file_name] = file

    except HttpError as error:
        logger.exception(f"An error occurred: {error}")
        file = None

    return returned_files


def download_file_data(service, file_id):
    try:
        return download_file_d(service, file_id)
    except:
        return export_file_d(service, file_id)


def download_file_d(service, file_id):
    file_name = service.files().get(fileId=file_id).execute()

    request = service.files().get_media(fileId=file_id)

    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        logger.info(f'Downloading {file_name["name"]} {int(status.progress() * 100)}.')

    return file.getvalue(), file_name["name"]


def export_file_d(service, file_id):
    file_name = service.files().get(fileId=file_id).execute()

    request = service.files().export_media(fileId=file_id, mimeType="application/pdf")

    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        logger.info(f'Downloading {file_name["name"]} {int(status.progress() * 100)}.')

    return file.getvalue(), file_name["name"] + ".pdf"


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

def build_hierarchy(items):
    item_map = {}
    root_items = []
    
    # First pass: create a map of all items
    for item in items:
        item_map[item['id']] = {
            **item,
            'children': []
        }
    
    # Second pass: assign children to parents
    for item in items:
        parent_id = item['parents'][0] if item['parents'] else None
        
        if parent_id in item_map:
            # Parent exists in our data - add as child
            item_map[parent_id]['children'].append(item_map[item['id']])
        elif parent_id:
            # Parent doesn't exist in our data - treat as root item
            root_items.append(item_map[item['id']])
        else:
            # No parent specified - add to root items
            root_items.append(item_map[item['id']])
    
    return root_items

def create_structure(data, target_dir):
    """
    Recursively create directory structure from hierarchical data
    
    Args:
        data: The hierarchical data structure
        target_dir: The root directory where to create the structure
    """
    # Create target directory if it doesn't exist
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    
    for item in data:
        current_path = os.path.join(target_dir, item['name'])
        
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            # Create directory
            os.makedirs(current_path, exist_ok=True)
            print(f"Created directory: {current_path}")
            
            # Process children recursively
            if item['children']:
                create_structure(item['children'], current_path)
        else:
            for v in item['file'].values():
                file_name = Path(current_path)

                with open(file_name, "wb") as f:
                    f.write(v)
            
            print(f"Created file: {current_path}")

def get_files_in_folder(folder_id, creds):
    files = []
    page_token = None
    service = build("drive", "v3", credentials=creds)
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token
        ).execute()
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return files


def download_hierarchy(file_ids, creds):
    hierarchy = []

    def download_hierarchy_from_drive(file_ids):
        service = build("drive", "v3", credentials=creds)
        for file_id in file_ids:
            metadata = service.files().get(fileId=file_id, fields='id,name,mimeType,parents').execute()

            if metadata['mimeType'] != "application/vnd.google-apps.folder":
                files = download_file(file_id, creds)
                metadata['file'] = files
            else:
                list_of_files = get_files_in_folder(metadata["id"], creds)
                download_hierarchy_from_drive((f.get("id") for f in list_of_files))

            hierarchy.append(metadata)
    
    download_hierarchy_from_drive(file_ids)

    return unique_list(hierarchy, key=lambda x: x['id'])
    
def download(args):
    logger.debug(f"Downloading data...")
    config = args.config
    access_token = config.get("access_token")
    refresh_token = config["refresh_token"]
    client_id = config["client_id"]
    client_secret = config["client_secret"]
    file_ids = [f.get("id") for f in config.get("files")]
    output_path = config["target_dir"]

    creds = Credentials(
        access_token or "",
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
    )

    hierarchy = download_hierarchy(file_ids, creds)
    hierarchy = build_hierarchy(hierarchy)
    create_structure(hierarchy, output_path)
    logger.info(f"Data downloaded.")

def main():
    # Parse command line arguments
    args = parse_args()

    # Download the data
    download(args)


if __name__ == "__main__":
    main()
