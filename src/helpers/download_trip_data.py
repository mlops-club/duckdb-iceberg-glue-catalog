"""
The data downloaded by this file comes from here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

import datetime
from pathlib import Path
from typing import Iterator, Literal

import httpx
from dateutil.relativedelta import relativedelta

THIS_DIR = Path(__file__).parent
DATA_DIR = THIS_DIR / "../data"

TTripTypes = Literal["yellow", "green", "fhv", "fhvhv"]
TRIP_TYPES: list[TTripTypes] = ["yellow", "green", "fhv", "fhvhv"]


def make_download_yellow_tripdata_url(year: int, month: int) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


def make_download_green_tripdata_url(year: int, month: int) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"


def make_download_fhv_tripdata_url(year: int, month: int) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month:02d}.parquet"


def make_download_fhvhv_tripdata_url(year: int, month: int) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet"


def make_outfile_fpath(
    trip_type: TTripTypes,
    year: int,
    month: int,
    extension: str,
    base_dir: Path = DATA_DIR,
) -> Path:
    return base_dir / trip_type / f"{year}-{month:02d}.{extension}"


def download_last_n_months_of_data_if_not_already_downloaded(
    n_months: int = 3,
) -> Iterator[None]:
    # the TLC publishes data with a delay of 3 months
    tlc_upload_delay_months = 3

    today = datetime.date.today()
    most_recently_published_month = today - relativedelta(months=tlc_upload_delay_months)

    for i in range(n_months):
        target_date = most_recently_published_month - relativedelta(months=i)
        year = target_date.year
        month = target_date.month
        for trip_type in TRIP_TYPES:
            download_month_if_not_already_downloaded(year, month, trip_type)
        yield


def download_month_if_not_already_downloaded(
    year: int,
    month: int,
    trip_type: TTripTypes,
):
    outfile_fpath: Path = make_outfile_fpath(trip_type, year, month, "parquet")
    if outfile_fpath.exists():
        print(f"{outfile_fpath} already exists. Skipping download.")
        return

    match trip_type:
        case "yellow":
            url = make_download_yellow_tripdata_url(year, month)
        case "green":
            url = make_download_green_tripdata_url(year, month)
        case "fhv":
            url = make_download_fhv_tripdata_url(year, month)
        case "fhvhv":
            url = make_download_fhvhv_tripdata_url(year, month)
        case _:
            raise ValueError("Invalid trip type")

    print(url)

    outfile_fpath.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {outfile_fpath}...")
    response = httpx.get(url)
    response.raise_for_status()
    outfile_fpath.write_bytes(response.content)

if __name__ == "__main__":
    for _ in download_last_n_months_of_data_if_not_already_downloaded(n_months=3):
        pass