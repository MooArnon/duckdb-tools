##########
# Import #
##############################################################################

import duckdb
import polars as pl
import boto3
import os

import requests

###########
# Classes #
##############################################################################

class DuckDB:
    """
    A class to handle DuckDB operations, including writing partitioned data to S3.
    """

    def __init__(self):
        pass

    ##########################################################################

    @staticmethod
    def write_partitioned_data_to_s3(
            df: pl.DataFrame, 
            s3_bucket: str, 
            s3_prefix: str, 
            partitions: list[str],
    ):
        """ Receives a DataFrame, dynamically partitions it, 
        writes Parquet files, and uploads to S3.

        Parameters
        ----------
        df: pl.DataFrame: 
            The input dataframe containing Binance data.
        s3_bucket: str: 
            The S3 bucket name.
        s3_prefix: str: 
            The prefix (folder path) inside the S3 bucket.
        partitions: list[str]: 
            List of column names to partition 
            data by (e.g., ["asset", "scraped_date"]).
        """

        # Convert partition columns to string format
        for col in partitions:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

        # Initialize S3 client
        s3 = boto3.client("s3")

        # Process each partition
        partition_df = df.select(partitions).unique()
        print(partition_df)
        
        for row in partition_df.iter_rows():
            # Construct filter condition dynamically
            condition = pl.fold(
                acc=True,
                function=lambda acc, expr: acc & expr,
                exprs=[pl.col(col) == value for col, value in zip(partitions, row)]
            )

            # Apply filter in a single step
            partitioned_data = df.filter(condition)

            # Construct the partitioned file paths
            partition_path = "/".join([f"{col}={val}" for col, val in zip(partitions, row)])
            local_temp_path = f"temp_{'_'.join(map(str, row))}.parquet"
            local_file_path = f"binance_data_{'_'.join(map(str, row))}.parquet"
            s3_file_path = f"{s3_prefix}/{partition_path}/binance_data.parquet"
            s3_file_path_prefix = f"{s3_prefix}/{partition_path}/"

            # Delete existing files in the partition path
            print(f"🚨 Clearing existing files in s3://{s3_bucket}/{s3_file_path_prefix}...")
            objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_file_path_prefix)

            if "Contents" in objects:
                delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
                s3.delete_objects(Bucket=s3_bucket, Delete={"Objects": delete_keys})
                print(
                    f"✅ Deleted {len(delete_keys)} " \
                        f"existing files from s3://{s3_bucket}/{s3_file_path_prefix}"
                )
            
            partitioned_data.write_parquet(local_temp_path)
            duckdb.sql(f"""
                COPY (SELECT * FROM read_parquet('{local_temp_path}')) 
                TO '{local_file_path}' 
                (FORMAT PARQUET, COMPRESSION GZIP);
            """)
            
            # Upload to S3
            s3.upload_file(local_file_path, s3_bucket, s3_file_path)
            print(f"✅ Uploaded: s3://{s3_bucket}/{s3_file_path}")

            # Cleanup local file
            os.remove(local_file_path)

    ##########################################################################
    
    @staticmethod
    def read_partitioned_data_from_s3(
            s3_bucket: str, 
            s3_prefix: str, 
            partition_lenght: int = None,
            partition_filter: str = None,
            filters: dict = None,
            region: str = "ap-southeast-1"
    ) -> pl.DataFrame:
        """ Reads partitioned data from S3 into DuckDB with optional filters.

        Parameters
        ----------
        s3_bucket: str: 
            The S3 bucket name.
        s3_prefix: str: 
            The prefix (folder path) inside the S3 bucket.
        partition_lenght: int
            Number of partition column,
            select all data from table
        partition_filter: str
            Partition prefix
            `asset=ADAUSDT/*`
        filters: dict: 
            Dictionary of filters (e.g., {"asset": "BTCUSDT", "open_date": "2025-03-24"}).

        Returns
        -------
            polars.DataFrame: The retrieved data as a Polars DataFrame.
        """

        # Ensure DuckDB has HTTPFS for S3 access
        duckdb.sql("INSTALL httpfs; LOAD httpfs;")
        
        # Set AWS credentials (ensure they are set correctly)
        duckdb.sql(f"""
            SET s3_region = '{region}';
            SET s3_access_key_id = '{os.environ['AWS_ACCESS_KEY_ID']}';
            SET s3_secret_access_key = '{os.environ['AWS_SECRET_ACCESS_KEY']}';
        """)

        # Construct S3 path
        if partition_lenght:
            s3_path = f"s3://{s3_bucket}/{s3_prefix}/{'*'*partition_lenght}/*.parquet"
        elif partition_filter:
            s3_path = f"s3://{s3_bucket}/{s3_prefix}/{partition_filter}/*.parquet"

        # Apply optional partition filters
        if filters:
            filter_conditions = " AND ".join([f"{key} = '{value}'" for key, value in filters.items()])
            query = f"SELECT * FROM read_parquet('{s3_path}') WHERE {filter_conditions}"
        else:
            query = f"SELECT * FROM read_parquet('{s3_path}')"

        # Execute query
        print(f"📥 Querying: {query}")
        result = duckdb.sql(query).pl()

        return result

    
    ##########################################################################

##############################################################################
