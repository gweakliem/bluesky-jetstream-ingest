import pytest
import duckdb
from pathlib import Path
from jetstream_ingestion.writer import DuckDBWriter

def test_local_writer(tmp_path):
    """Test writing to local DuckDB database."""
    db_path = tmp_path / "test.db"
    writer = DuckDBWriter(db_path)
    messages = [
        {
            'receivedTimestamp': '2024-01-01T00:00:00',
            'payload': '{"test": "data"}'
        }
    ]

    writer.write_batch(messages)
    writer.close()

    # Verify data was written
    conn = duckdb.connect(str(db_path), read_only=True)
    result = conn.execute("SELECT COUNT(*) FROM jetstream_messages").fetchone()
    assert result[0] == 1

    # Verify content
    row = conn.execute("SELECT receivedTimestamp, payload FROM jetstream_messages").fetchone()
    assert row[1] == '{"test": "data"}'
    conn.close()

def test_batch_write(tmp_path):
    """Test writing multiple batches."""
    db_path = tmp_path / "test.db"
    writer = DuckDBWriter(db_path)

    # Write first batch
    messages1 = [
        {'receivedTimestamp': '2024-01-01T00:00:00', 'payload': '{"batch": 1}'},
        {'receivedTimestamp': '2024-01-01T00:01:00', 'payload': '{"batch": 1}'}
    ]
    writer.write_batch(messages1)

    # Write second batch
    messages2 = [
        {'receivedTimestamp': '2024-01-01T00:02:00', 'payload': '{"batch": 2}'},
        {'receivedTimestamp': '2024-01-01T00:03:00', 'payload': '{"batch": 2}'}
    ]
    writer.write_batch(messages2)
    writer.close()

    # Verify total count
    conn = duckdb.connect(str(db_path), read_only=True)
    result = conn.execute("SELECT COUNT(*) FROM jetstream_messages").fetchone()
    assert result[0] == 4
    conn.close()

def test_empty_batch(tmp_path):
    """Test writing an empty batch."""
    db_path = tmp_path / "test.db"
    writer = DuckDBWriter(db_path)

    # Write empty batch - should not error
    writer.write_batch([])
    writer.close()

    # Verify no data was written
    conn = duckdb.connect(str(db_path), read_only=True)
    result = conn.execute("SELECT COUNT(*) FROM jetstream_messages").fetchone()
    assert result[0] == 0
    conn.close()

def test_context_manager(tmp_path):
    """Test using DuckDBWriter as a context manager."""
    db_path = tmp_path / "test.db"

    with DuckDBWriter(db_path) as writer:
        messages = [
            {'receivedTimestamp': '2024-01-01T00:00:00', 'payload': '{"test": "context"}'}
        ]
        writer.write_batch(messages)

    # Verify data was written and connection was closed
    conn = duckdb.connect(str(db_path), read_only=True)
    result = conn.execute("SELECT COUNT(*) FROM jetstream_messages").fetchone()
    assert result[0] == 1
    conn.close()
