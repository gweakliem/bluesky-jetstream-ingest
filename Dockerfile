# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install uv package manager
RUN pip install uv

# Copy project files
COPY pyproject.toml ./
COPY jetstream_ingestion ./jetstream_ingestion/

# Install dependencies using uv
RUN uv pip install --system -e .

# Create directory for DuckDB database
RUN mkdir -p /data

# Set default command
CMD ["jetstream-ingest", "/data/jetstream.db"]
