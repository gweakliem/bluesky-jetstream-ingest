import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Union
import logging

import duckdb

logger = logging.getLogger(__name__)

class DuckDBWriter:
    def __init__(self, db_path: Union[str, Path], table_name: str = "jetstream_messages"):
        """Initialize the DuckDB writer.

        Args:
            db_path: Path to the DuckDB database file
            table_name: Name of the table to create/use (default: jetstream_messages)
        """
        self.db_path = str(db_path)
        self.table_name = table_name

        # Ensure parent directory exists
        parent_dir = os.path.dirname(self.db_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        # Connect to DuckDB and create table if it doesn't exist
        self.conn = duckdb.connect(self.db_path)
        self._create_table()

    def _create_table(self):
        """Create the jetstream messages table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            receivedTimestamp TIMESTAMP,
            payload VARCHAR
        )
        """
        self.conn.execute(create_table_sql)
        logger.info(f"Table {self.table_name} ready in database {self.db_path}")

    def write_batch(self, messages: List[Dict]):
        """Write a batch of messages to DuckDB.

        Args:
            messages: List of message dictionaries with receivedTimestamp and payload
        """
        if not messages:
            return

        try:
            # Prepare data for insertion
            # Convert ISO format timestamps to proper timestamp format
            insert_data = [
                (msg['receivedTimestamp'], msg['payload'])
                for msg in messages
            ]

            # Insert using executemany for better performance
            insert_sql = f"""
            INSERT INTO {self.table_name} (receivedTimestamp, payload)
            VALUES (?, ?)
            """
            self.conn.executemany(insert_sql, insert_data)

            logger.debug(f"Inserted {len(messages)} messages into {self.table_name}")

        except Exception as e:
            logger.error(f"Error writing batch to DuckDB: {e}")
            raise

    def close(self):
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
