# Bluesky Jetstream Ingestion

A Python application for ingesting Bluesky's [Jetstream API](https://github.com/bluesky-social/jetstream) data and writing it to Parquet format.

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the ingestor with a local output directory:
```bash
python -m src.ingestor /path/to/output/dir
```

Or with a GCS bucket:
```bash
python -m src.ingestor gs://bucket-name/path
```

Optional arguments:
- `--batch-size`: Number of messages to batch before writing (default: 1000)

## Development

1. Install development dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run tests:
   ```bash
   pytest tests/
   ```

## Installation

```
pip install -e .
```

## Running the application

Using the local file system for output:

```
jetstream-ingest ./scratch  # For local output
```

or for GCS output

```
jetstream-ingest gs://bucket-name/path
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
