import argparse
import logging
from pathlib import Path

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Ingest Jetstream data to DuckDB')
    parser.add_argument(
        'output_path',
        type=str,
        help='Path to DuckDB database file (e.g., data/jetstream.db)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of messages to batch before writing (default: 1000)'
    )
    return parser.parse_args()
