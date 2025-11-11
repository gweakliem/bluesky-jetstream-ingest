# Bluesky Jetstream Ingestion

A Python application for ingesting Bluesky's [Jetstream API](https://github.com/bluesky-social/jetstream) data and writing it to DuckDB.

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

### Local Installation

Run the ingestor with a DuckDB database file path:
```bash
uv run jetstream-ingest /path/to/jetstream.db
```

Optional arguments:
- `--batch-size`: Number of messages to batch before writing (default: 1000)

Example:
```bash
uv run jetstream-ingest data/jetstream.db --batch-size 500
```

### Docker Development Environment

A Docker Compose setup is provided for easy development:

1. Build and start the ingestor:
   ```bash
   docker-compose up -d ingestor
   ```

2. View logs:
   ```bash
   docker-compose logs -f ingestor
   ```

3. Query the database using Python and DuckDB:
   ```bash
   docker-compose run --rm duckdb-cli
   ```

4. Stop the services:
   ```bash
   docker-compose down
   ```

The database file will be persisted in the `./data` directory.

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

## Database Schema

The application writes to a DuckDB table named `jetstream_messages` with the following schema:
- `receivedTimestamp`: TIMESTAMP - when the message was received
- `payload`: VARCHAR - raw JSON message from Jetstream

## Querying the Data

You can query the DuckDB database using the DuckDB CLI or any DuckDB client:

```python
import duckdb

# Connect to the database
conn = duckdb.connect('data/jetstream.db', read_only=True)

# Query the data
result = conn.execute("""
    SELECT
        COUNT(*) as total_messages,
        MIN(receivedTimestamp) as first_message,
        MAX(receivedTimestamp) as last_message
    FROM jetstream_messages
""").fetchall()

print(result)
conn.close()
```

Example queries:

```sql
-- Count total messages
SELECT COUNT(*) FROM jetstream_messages;

-- View recent messages
SELECT receivedTimestamp, payload
FROM jetstream_messages
ORDER BY receivedTimestamp DESC
LIMIT 10;

-- Export to Parquet (if needed)
COPY jetstream_messages TO 'output.parquet' (FORMAT PARQUET);
```

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
- Proper DuckDB connection cleanup on shutdown

## License

MIT License
