#!/usr/bin/env python3
import os
import json
import argparse
import logging

from pathlib import Path
import hashlib

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


logger = logging.getLogger("tap-google-drive")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def unique_list(items, key=None):
    seen = set()
    result = []
    for item in items:
        val = key(item) if key else item
        if val not in seen:
            seen.add(val)
            result.append(item)
    return result


def download_file(real_file_id, creds, target_path=None):
    """Downloads a file
    Args:
        real_file_id: ID of the file to download
        creds: Load pre-authorized user credentials
        TODO(developer) - See https://developers.google.com/identity
        for guides on implementing OAuth2 for the application.
        target_path: Path to save the file to (including filename)
    Returns : dict of {filename: filepath} for downloaded files
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
                    file_path = download_file_data(service, file["id"], target_path)
                    if file_path:
                        returned_files[file_path.name] = str(file_path)

        if returned_files == {}:
            file_path = download_file_data(service, file_id, target_path)
            if file_path:
                returned_files[file_path.name] = str(file_path)

    except HttpError as error:
        logger.exception(f"An error occurred: {error}")

    return returned_files


def download_file_data(service, file_id, target_path=None):
    try:
        return download_file_d(service, file_id, target_path)
    except:
        return export_file_d(service, file_id, target_path)


def download_file_d(service, file_id, target_path=None):
    file_metadata = service.files().get(fileId=file_id).execute()
    file_name = file_metadata["name"]
    
    if target_path:
        # Ensure the target directory exists
        target_dir = Path(target_path).parent
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = Path(target_path) / file_name
    else:
        file_path = Path(file_name)
    
    request = service.files().get_media(fileId=file_id)

    with open(file_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(f'Downloading {file_name} {int(status.progress() * 100)}%')
    
    return file_path


def export_file_d(service, file_id, target_path=None):
    file_metadata = service.files().get(fileId=file_id).execute()
    file_name = file_metadata["name"] + ".pdf"
    
    if target_path:
        # Ensure the target directory exists
        target_dir = Path(target_path).parent
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = Path(target_path) / file_name
    else:
        file_path = Path(file_name)

    request = service.files().export_media(fileId=file_id, mimeType="application/pdf")

    with open(file_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(f'Downloading {file_name} {int(status.progress() * 100)}%')
    
    return file_path


def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    """Parse standard command-line args."""
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
            item_map[parent_id]['children'].append(item_map[item['id']])
        elif parent_id:
            root_items.append(item_map[item['id']])
        else:
            root_items.append(item_map[item['id']])
    
    return root_items

def create_structure(data, target_dir):
    """
    Recursively create directory structure from hierarchical data
    
    Args:
        data: The hierarchical data structure
        target_dir: The root directory where to create the structure
    """
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    
    for item in data:
        current_path = os.path.join(target_dir, item['name'])
        
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            os.makedirs(current_path, exist_ok=True)
            print(f"Created directory: {current_path}")
            
            if item['children']:
                create_structure(item['children'], current_path)
        else:
            # For files, we've already downloaded them during hierarchy building
            # so we just need to move them to the correct location
            if 'file_path' in item:
                target_path = Path(current_path)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                Path(item['file_path']).rename(target_path)
                print(f"Moved file to: {current_path}")

def get_files_in_folder(folder_id, creds, parent_path=None):
    files = []
    page_token = None
    service = build("drive", "v3", credentials=creds)
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, parents)',
            pageToken=page_token
        ).execute()
        for file in response.get('files', []):
            if parent_path:
                file_path = parent_path / file['name']
            else:
                file_path = None
            files.append((file, file_path))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return files


def download_hierarchy(file_ids, creds, base_path=None):
    hierarchy = []

    def download_hierarchy_from_drive(file_ids, parent_path=None):
        service = build("drive", "v3", credentials=creds)
        for file_id in file_ids:
            metadata = service.files().get(fileId=file_id, fields='id,name,mimeType,parents').execute()

            file_path = None
            if parent_path:
                file_path = parent_path / metadata['name']

            if metadata['mimeType'] != "application/vnd.google-apps.folder":
                # Download file directly to disk
                if file_path:
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    downloaded_files = download_file(metadata['id'], creds, file_path.parent)
                    if downloaded_files:
                        metadata['file_path'] = next(iter(downloaded_files.values()))
            else:
                # Create folder and process children
                if file_path:
                    file_path.mkdir(parents=True, exist_ok=True)
                
                list_of_files = get_files_in_folder(metadata["id"], creds, file_path)
                child_ids = [f[0]['id'] for f in list_of_files]
                download_hierarchy_from_drive(child_ids, file_path)

            hierarchy.append(metadata)
    
    download_hierarchy_from_drive(file_ids, base_path)

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

    flat_hierarchy = download_hierarchy(file_ids, creds, Path(output_path))
    hierarchy = build_hierarchy(flat_hierarchy)
    create_structure(hierarchy, output_path)
    
    logger.info(f"Data downloaded.")

def main():
    args = parse_args()
    download(args)

if __name__ == "__main__":
    main()