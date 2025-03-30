# tap-google-drive

A Meltano tap for Google Drive that reads CSV files from a specified Google Drive folder.

## Features

- OAuth2 authentication with Google Drive
- Magic folder mode - each CSV file creates a new table
- Automatic column name conversion to BigQuery-compliant format
- CSV file filtering
- Secure credential management using environment variables

## Installation

1. Clone this repository
2. Install Poetry if you haven't already:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

3. Install dependencies using Poetry:
```bash
poetry install
```

## Configuration

The tap requires the following configuration:

1. Configure the tap using Meltano:
```bash
meltano config tap-google-drive set client_id <your_oauth_client_id>
meltano config tap-google-drive set folder_url <your google drive URL>
meltano config tap-google-drive set client_secret <your client secret>
meltano config tap-google-drive set refresh_token <your refresh token>
```
replace the <> and the values within with your configuration values.

2. Create a `.env` file in the root directory with the following variables:
```
TAP_GOOGLE_DRIVE_CLIENT_ID=your_oauth_client_id
TAP_GOOGLE_DRIVE_CLIENT_SECRET=your_oauth_client_secret
TAP_GOOGLE_DRIVE_REFRESH_TOKEN=your_oauth_refresh_token
TAP_GOOGLE_DRIVE_FOLDER_URL=your_google_drive_folder_url
```

Note: The `client_secret` and `refresh_token` are sensitive credentials and should not be stored in the Meltano configuration. They should only be stored in the `.env` file.

## Authentication

1. Go to the Google Cloud Console
2. Create a new project or select an existing one
3. Enable the Google Drive API
4. Create OAuth 2.0 credentials
5. Download the client secrets file
6. Place the client secrets file in a secure location
7. Update the `.env` file with the path to your client secrets file

## Usage

Run the tap:
```bash
meltano run tap-google-drive target-jsonl
```

## Column Name Conversion

The tap automatically converts CSV column names to BigQuery-compliant format by:
- Removing special characters
- Converting spaces to underscores
- Ensuring the column name starts with a letter
- Converting to lowercase

Example:
- "First Name" → "first_name"
- "1st Column" → "col_1st_column"
- "User's Email" → "users_email"

## Development

### Testing

```bash
poetry run pytest
```

### Linting

```bash
poetry run black .
poetry run isort .
poetry run flake8
```

### Code Style

This project uses:
- Black for code formatting
- isort for import sorting
- flake8 for linting
- mypy for type checking

Run pre-commit hooks:
```bash
poetry run pre-commit run --all-files
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
