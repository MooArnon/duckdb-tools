FROM debian:latest

# Install dependencies
RUN apt update && apt install -y wget unzip

# Download and install the ARM64-compatible DuckDB binary
RUN wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-aarch64.zip \
    && unzip duckdb_cli-linux-aarch64.zip \
    && chmod +x duckdb \
    && mv duckdb /usr/local/bin/

# Default command
ENTRYPOINT ["duckdb"]
