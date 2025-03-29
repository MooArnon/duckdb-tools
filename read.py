from datetime import datetime, timezone
import requests
import polars as pl

from duckdb_tools.__main import DuckDB

columns = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_asset_volume', 'number_of_trades',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
]
s3_bucket="space-time-lake-house"
s3_prefix="duck_db/historical_binance_future"
partitions=[
    'asset',
    'open_date',
]

db = DuckDB()

# data = db.read_partitioned_data_from_s3(
#     s3_bucket=s3_bucket,
#     s3_prefix=s3_prefix,
#     partition_lenght=2
# )

data = db.read_partitioned_data_from_s3(
    s3_bucket=s3_bucket,
    s3_prefix=s3_prefix,
    # partition_lenght=2
    partition_filter="asset=BTCUSDT/open_date=2025-03-29"
)

data.to_pandas().to_csv('historical_binance_future__btcusdt.csv')

print(data)