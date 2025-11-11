# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN curl https://install.duckdb.org | sh
RUN echo 'export PATH='/root/.duckdb/cli/latest':$PATH' >> ~/.bashrc

# Install uv package manager
RUN pip install uv

# Copy project files
COPY pyproject.toml README.md ./
COPY jetstream_ingestion ./jetstream_ingestion/

# Install dependencies using uv
RUN uv pip install --system -e .

# Create directory for DuckDB database
RUN mkdir -p /data

# Set default command
CMD ["jetstream-ingest", "/data/jetstream.db"]
