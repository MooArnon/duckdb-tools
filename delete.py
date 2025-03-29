from datetime import datetime, timezone
import requests
import polars as pl

from duckdb_tools.__main import DuckDB

db = DuckDB()

df = None

# Scrape data
# Get today's date and convert to Unix timestamp (milliseconds)
start_date = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
start_timestamp = int(start_date.timestamp() * 1000)
params = {
    "interval": "15m",
    "limit": 24,  # Last 24 candles (6 hours of data)
    "startTime": start_timestamp
}
params={
    "interval": "15m", 
    "startTime": start_timestamp
}

db.delete_dynamo_record(
    table_name=table_name,
    record_id=record_id
)
