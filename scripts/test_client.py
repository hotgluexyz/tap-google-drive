"""Test script for Google Drive authentication and file listing."""

import os
import traceback
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def test_drive_auth():
    """Test Google Drive authentication and file listing."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Get config from environment variables
    config = {
        "client_id": os.getenv("TAP_GOOGLE_DRIVE_CLIENT_ID"),
        "client_secret": os.getenv("TAP_GOOGLE_DRIVE_CLIENT_SECRET"),
        "refresh_token": os.getenv("TAP_GOOGLE_DRIVE_REFRESH_TOKEN"),
        "folder_url": os.getenv("TAP_GOOGLE_DRIVE_FOLDER_URL")
    }
    
    # Print all config values for debugging
    print("Config:")
    for key, value in config.items():
        print(f"{key}: {value}")
    
    try:
        # Create credentials
        creds = Credentials(
            token=None,
            refresh_token=config["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            scopes=SCOPES
        )
        
        # Build the service
        service = build("drive", "v3", credentials=creds)
        
        # Extract folder ID from URL
        folder_url = config["folder_url"]
        if "folders/" in folder_url:
            folder_id = folder_url.split("folders/")[1].split("?")[0]
        else:
            raise ValueError("Invalid folder URL. Must be a Google Drive folder URL.")
        
        print(f"\nFolder ID: {folder_id}")
        
        # List CSV files in the folder
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv'",
            fields="files(id, name)",
            spaces="drive"
        ).execute()
        
        files = results.get("files", [])
        
        if not files:
            print("\nNo CSV files found in the folder.")
        else:
            print("\nCSV files found:")
            for file in files:
                print(f"- {file['name']} ({file['id']})")
                
                # Get file content
                file_content = service.files().get_media(fileId=file["id"]).execute()
                print(f"  Content length: {len(file_content)} bytes")
                
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("\nFull traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_drive_auth() 