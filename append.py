from datetime import datetime, timezone
import requests
import polars as pl

from duckdb_tools.__main import DuckDB

columns = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_asset_volume', 'number_of_trades',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
]
assets = ["BTCUSDT", "ADAUSDT", "ETHUSDT"]
s3_bucket="space-time-lake-house"
s3_prefix="duck_db/sample"
partitions=[
    'asset',
    'open_date',
]

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

dfs=[]

for asset in assets:
    
    params['symbol']=asset
    response = requests.get(
        "https://fapi.binance.com/fapi/v1/klines", 
        params=params
    )
    response.raise_for_status()
    data = response.json()

    mapped_data = [dict(zip(columns, row)) for row in data]

    df = pl.DataFrame(mapped_data)
    df = df.with_columns(
        pl.lit(asset).alias('asset'),
        (pl.col("open_time")).cast(pl.Datetime(time_unit="ms")).cast(pl.Date).alias("open_date"),
    )
    
    dfs.append(df)
    
final_df = pl.concat(dfs) if dfs else None
print(final_df.head(1000))

db.append_partitioned_data_to_s3(
    df=final_df,
    s3_bucket=s3_bucket,
    s3_prefix=s3_prefix,
    partitions=partitions,
)
