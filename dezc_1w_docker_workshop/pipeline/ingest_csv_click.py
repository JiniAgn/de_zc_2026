"""
python ingest_csv_click.py \
  --pg-user root \
  --pg-password xxxx \
  --pg-host localhost \
  --pg-port 5432 \
  --pg-database ny_taxi \
  --year 2021 \
  --month 1
"""

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# column dtypes
DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


@click.command()
@click.option("--pg-user", required=True, help="Postgres user")
@click.option("--pg-password", required=True, help="Postgres password")
@click.option("--pg-host", required=True, help="Postgres host")
@click.option("--pg-port", default=5432, show_default=True, help="Postgres port")
@click.option("--pg-database", required=True, help="Postgres database name")
@click.option("--year", type=int, required=True, help="Data year (e.g. 2021)")
@click.option("--month", type=int, required=True, help="Data month (1-12)")
@click.option(
    "--table-name",
    default="yellow_taxi_data_click",
    show_default=True,
    help="Target table name",
)
def main(
    pg_user,
    pg_password,
    pg_host,
    pg_port,
    pg_database,
    year,
    month,
    table_name,
):
    """
    Ingest NYC Yellow Taxi CSV data into Postgres using chunked loading.
    """

    # build url
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    click.echo(f"Downloading data from: {url}")

    # create db engine
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
    )

    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=100_000,
    )

    first = True
    for df_chunk in tqdm(df_iter, desc="Ingesting data"):
        if first:
            # create / replace table schema
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

    click.echo("Ingestion completed successfully.")


if __name__ == "__main__":
    main()
