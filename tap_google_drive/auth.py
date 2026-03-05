import json
import logging

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials

from hotglue_etl_exceptions import InvalidCredentialsError

logger = logging.getLogger("tap-google-drive")


class GoogleOAuthAuthenticator:
    def __init__(self, stream, config_file: str, auth_endpoint: str):
        self._stream = stream
        self._config_file = config_file
        self._auth_endpoint = auth_endpoint
        self.logger = getattr(stream, "logger", logger)

    def is_token_valid(self) -> bool:
        return False

    def update_access_token(self) -> None:
        config = self._stream.config
        try:
            creds = Credentials(
                token=config.get("access_token", ""),
                refresh_token=config["refresh_token"],
                token_uri=self._auth_endpoint,
                client_id=config["client_id"],
                client_secret=config["client_secret"],
            )
            creds.refresh(Request())
        except RefreshError as e:
            raise InvalidCredentialsError(
                f"Failed to refresh Google OAuth token: {e}"
            ) from e

        # When stream comes from the SDK's DummyStream it has _tap with a mutable _config.
        # When stream comes from our own DummyStream, config is already a plain dict.
        if hasattr(self._stream, "_tap"):
            self._stream._tap._config["access_token"] = creds.token
        else:
            config["access_token"] = creds.token

        if self._config_file:
            full_config = {**dict(config), "access_token": creds.token}
            with open(self._config_file, "w") as f:
                json.dump(full_config, f, indent=4)
