# Bluesky Jetstream Ingestion

A Python application for ingesting Bluesky's [Jetstream API](https://github.com/bluesky-social/jetstream) data and writing it to Parquet format.

## Prerequisites

This project uses [uv](https://github.com/astral-sh/uv) for Python package management. Install uv first:

```bash
# On macOS and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

## Installation

1. Clone the repository
2. Install dependencies and create a virtual environment:
   ```bash
   uv sync
   ```

This will automatically:
- Create a virtual environment in `.venv/`
- Install all project dependencies
- Install the package in editable mode

## Usage

Run the ingestor with a local output directory:
```bash
uv run jetstream-ingest /path/to/output/dir
```

Or with a GCS bucket:
```bash
uv run jetstream-ingest gs://bucket-name/path
```

Optional arguments:
- `--batch-size`: Number of messages to batch before writing (default: 1000)

## Development

1. Install with development dependencies:
   ```bash
   uv sync --dev
   ```

2. Run tests:
   ```bash
   uv run pytest tests/
   ```

3. Add new dependencies:
   ```bash
   # Add a runtime dependency
   uv add <package-name>

   # Add a development dependency
   uv add --dev <package-name>
   ```

## Output Format

The application writes Parquet files with the following schema:
- `receivedTimestamp`: ISO 8601 timestamp when the message was received
- `payload`: Raw JSON message from Jetstream

Files are named using the pattern: `jetstream_YYYYMMDD_HHMMSS.parquet`

## Monitoring

The application logs to stdout with INFO level by default. Monitor the logs for:
- Connection status
- Batch writing success/failure
- Error conditions

## Error Handling

The application implements:
- Automatic reconnection on connection loss
- Graceful shutdown on SIGTERM/SIGINT
- Batch retry logic for failed writes

## License

MIT License
