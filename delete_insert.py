from datetime import datetime, timezone, timedelta
import requests
import polars as pl

from duckdb_tools.__main import DuckDB

columns = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_asset_volume', 'number_of_trades',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
]
assets = ["BTCUSDT", "ADAUSDT", "ETHUSDT", "BNBUSDT"]
s3_bucket="space-time-lake-house"
s3_prefix="duck_db/historical_binance_future"
partitions=[
    'asset',
    'open_date',
]

db = DuckDB()

df = None

# Scrape data
# Get today's date and convert to Unix timestamp (milliseconds)
start_date = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
# Define start date and end date (today)
# start_date = datetime(year=2022, month=1, day=1, tzinfo=timezone.utc)
end_date = datetime.now(tz=timezone.utc)

start_timestamp = int(start_date.timestamp() * 1000)

params={
    "interval": "15m", 
    "startTime": start_timestamp,
    "limit": 96,
}

dfs=[]

# Iterate over each day
while start_date < end_date:
    dfs=[]
    start_timestamp = int(start_date.timestamp() * 1000)  # Convert to milliseconds
    print(f"📅 Fetching data for: {start_date.date()}")

    for asset in assets:
        params["symbol"] = asset
        params["startTime"] = start_timestamp

        # Fetch data from Binance
        response = requests.get("https://fapi.binance.com/fapi/v1/klines", params=params)
        response.raise_for_status()
        data = response.json()

        # Convert data into a Polars DataFrame
        mapped_data = [dict(zip(columns, row)) for row in data]
        df = pl.DataFrame(mapped_data)

        # Add asset and formatted date column
        df = df.with_columns(
            pl.lit(asset).alias("asset"),
            pl.col("open_time").cast(pl.Datetime(time_unit="ms")).cast(pl.Date).alias("open_date"),
        )

        # Append to list
        dfs.append(df)
    
    final_df = pl.concat(dfs) if dfs else None
    print(final_df.shape)

    db.write_partitioned_data_to_s3(
        df=final_df,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        partitions=partitions,
    )
    
    # Increment to the next day
    start_date += timedelta(days=1)
