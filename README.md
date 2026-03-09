# tap-google-drive

`tap-google-drive` is a Singer tap for [Google Drive](https://drive.google.com/), a cloud file storage and collaboration platform.

Built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK) for Singer Taps.

## Installation

```bash
pip install tap-google-drive
```

Or install directly from the repository:

```bash
pip install git+https://github.com/hotgluexyz/tap-google-drive.git
```

## Configuration

### Accepted Config Options

| Setting         | Required | Description                                                                                      |
|-----------------|----------|--------------------------------------------------------------------------------------------------|
| `client_id`     | Yes      | Google OAuth2 client ID                                                                          |
| `client_secret` | Yes      | Google OAuth2 client secret                                                                      |
| `refresh_token` | Yes      | Google OAuth2 refresh token                                                                      |
| `access_token`  | No       | Google OAuth2 access token (optional; refreshed automatically if not provided)                   |
| `files`         | Yes      | List of file objects, each with an `id` (Google Drive file or folder ID) and optional `name`    |
| `target_dir`    | Yes      | Local directory path where downloaded files will be written                                      |

Example `config.json`:

```json
{
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "refresh_token": "your_refresh_token",
  "access_token": "your_access_token",
  "target_dir": "/path/to/output/",
  "files": [
    { "id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms", "name": "my_sheet.xlsx" }
  ]
}
```

### Source Authentication and Authorization

This tap uses **OAuth 2.0** to authenticate with the Google Drive API v3. You will need a Google Cloud project with the Drive API enabled, and OAuth 2.0 credentials (client ID and client secret) configured for your application. A refresh token is required to allow the tap to obtain access tokens automatically.

## Supported File Types

The tap downloads files directly from Google Drive. Google Workspace files are automatically exported to an equivalent Office or PDF format:

| Google Workspace Type        | Exported Format |
|------------------------------|-----------------|
| Google Sheets                | `.xlsx`         |
| Google Docs                  | `.docx`         |
| Google Slides                | `.pptx`         |
| Google Drawings              | `.pdf`          |
| Google Forms                 | `.pdf`          |
| Other Google Workspace types | `.pdf`          |

Non-Google files (e.g. `.csv`, `.pdf`, `.png`) are downloaded as-is.

If a folder ID is provided in `files`, all files within that folder are downloaded.

## Usage

You can easily run `tap-google-drive` by itself or in a pipeline.

### Executing the Tap Directly

```bash
tap-google-drive --version
tap-google-drive --help
tap-google-drive --config CONFIG --discover > ./catalog.json
tap-google-drive --config CONFIG --catalog CATALOG > ./data.singer
```
