# tap-google-drive

A Meltano tap for syncing CSV files from Google Drive. This tap operates in magic folder mode, where each CSV file in the specified Google Drive folder creates a new table.

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

Create a `.env` file in the root directory with the following variables:

```env
GOOGLE_DRIVE_CLIENT_SECRETS_FILE=path/to/your/client_secrets.json
```

The `meltano.yml` file should contain:

```yaml
plugins:
  extractors:
    - name: tap-google-drive
      namespace: tap_google_drive
      pip_url: tap-google-drive
      config:
        folder_id: your_google_drive_folder_id
        client_secrets_file: ${GOOGLE_DRIVE_CLIENT_SECRETS_FILE}
```

## Authentication

1. Go to the Google Cloud Console
2. Create a new project or select an existing one
3. Enable the Google Drive API
4. Create OAuth 2.0 credentials
5. Download the client secrets file
6. Place the client secrets file in a secure location
7. Update the `.env` file with the path to your client secrets file

## Usage

1. Configure the tap using Meltano:
```bash
meltano elt tap-google-drive target-jsonl
```

2. The tap will:
   - Authenticate with Google Drive using OAuth2
   - List all CSV files in the specified folder
   - Create a new table for each CSV file
   - Convert column names to BigQuery-compliant format
   - Stream the data to the target

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

### Running Tests

```bash
poetry run pytest
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
