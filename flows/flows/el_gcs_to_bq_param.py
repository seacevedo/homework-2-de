from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return pd.read_parquet(Path(f"../data/{gcs_path}"))


@task()
def write_bq(df: pd.DataFrame) -> int:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-375814",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

    return len(df)


@flow()
def el_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""
    df = extract_from_gcs(color, year, month)
    res = write_bq(df)
    return res

@flow(log_prints=True)
def el_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    total_rows = 0
    for month in months:
        num_rows = el_gcs_to_bq(year, month, color)
        total_rows += num_rows
    print(f"Total Rows: {total_rows}")



if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    el_parent_flow(months, year, color)