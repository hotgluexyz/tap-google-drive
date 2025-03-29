"""Google Drive OAuth2 authentication module."""

import os
from typing import Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_NAME = 'drive'
VERSION = 'v3'

class GoogleDriveAuth:
    """Handles Google Drive OAuth2 authentication and service creation."""

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        """Initialize the auth handler.

        Args:
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            refresh_token: OAuth2 refresh token
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._credentials: Optional[Credentials] = None
        self._service = None

    @property
    def credentials(self) -> Credentials:
        """Get or refresh Google Drive credentials.

        Returns:
            Credentials object for Google Drive API.
        """
        if self._credentials is None:
            self._credentials = self._get_credentials()
        return self._credentials

    @property
    def service(self):
        """Get or create Google Drive service.

        Returns:
            Google Drive service object.
        """
        if self._service is None:
            self._service = build(
                SERVICE_NAME,
                VERSION,
                credentials=self.credentials
            )
        return self._service

    def _get_credentials(self) -> Credentials:
        """Get or refresh Google Drive credentials.

        Returns:
            Credentials object for Google Drive API.
        """
        creds = Credentials(
            token=None,
            refresh_token=self.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=SCOPES
        )
        
        # Refresh the token
        creds.refresh(Request())
        return creds

    def revoke_credentials(self) -> None:
        """Revoke the current credentials."""
        if self._credentials:
            self._credentials.revoke(Request())
            self._credentials = None
        self._service = None
