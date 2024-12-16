import pytest
import pandas as pd
from pathlib import Path
from src.writer import ParquetWriter

def test_local_writer(tmp_path):
    """Test writing to local filesystem."""
    writer = ParquetWriter(tmp_path)
    messages = [
        {
            'receivedTimestamp': '2024-01-01T00:00:00',
            'payload': '{"test": "data"}'
        }
    ]
    
    writer.write_batch(messages)
    
    # Verify file was created
    parquet_files = list(tmp_path.glob('*.parquet'))
    assert len(parquet_files) == 1
    
    # Verify content
    df = pd.read_parquet(parquet_files[0])
    assert len(df) == 1
    assert 'receivedTimestamp' in df.columns
    assert 'payload' in df.columns

def test_gcs_writer():
    """Test writing to Google Cloud Storage."""
    writer = ParquetWriter('gs://test-bucket/jetstream')
    messages = [
        {
            'receivedTimestamp': '2024-01-01T00:00:00',
            'payload': '{"test": "data"}'
        }
    ]
    
    writer.write_batch(messages)
    # Add GCS-specific assertions here

def test_s3_writer():
    """Test writing to AWS S3."""
    writer = ParquetWriter('s3://test-bucket/jetstream')
    messages = [
        {
            'receivedTimestamp': '2024-01-01T00:00:00',
            'payload': '{"test": "data"}'
        }
    ]
    
    writer.write_batch(messages)
    # Add S3-specific assertions here
