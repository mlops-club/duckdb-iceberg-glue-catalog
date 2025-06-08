from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb


@contextmanager
def connect_to_duckdb(db_path: str | Path) -> Generator[duckdb.DuckDBPyConnection]:
    conn = None
    try:
        conn = duckdb.connect(db_path)
        yield conn
    except Exception:
        raise
    finally:
        if conn:
            conn.close()


def load_parquet_files_into_db(directory: str | Path, table_name: str, db_path: str | Path) -> None:
    directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError(f"Directory {directory} does not exist.")

    parquet_glob = str(directory / "*.parquet")
    if not list(directory.glob("*.parquet")):
        raise FileNotFoundError(f"No parquet files found in {directory}.")

    # Connect to DuckDB and create the table
    with connect_to_duckdb(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_glob}')")


def create_all_tables(data_dir: str = "data", db_path: str = "nyc_taxi_data.duckdb") -> None:
    """
    Create all four NYC taxi data tables in DuckDB based on the actual directory structure.

    Directory structure:
    data/
    ├── fhv/
    ├── fhvhv/
    ├── green/
    └── yellow/

    Args:
        data_dir: Directory containing the parquet files
        db_path: Path to the DuckDB database file

    """
    data_path = Path(data_dir)
    table_configs = [
        {"path": data_path / "yellow", "name": "yellow_trips"},
        {"path": data_path / "green", "name": "green_trips"},
        {"path": data_path / "fhv", "name": "for_hire_vehicles"},
        {"path": data_path / "fhvhv", "name": "fhvhv"},
    ]

    for config in table_configs:
        load_parquet_files_into_db(config["path"], config["name"], db_path)


if __name__ == "__main__":
    create_all_tables()
