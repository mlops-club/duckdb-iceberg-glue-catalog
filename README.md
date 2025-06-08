# DuckDB, Iceberg in S3, and AWS Glue Catalog

## Step 1 - Download some NYC taxi data

```bash
uv run ./src/helpers/download_trip_data.py
```

## Step 2 - Auth with AWS

```bash
aws configure sso --profile duckdb
```

You will need a role that has access to

1. create an S3 bucket
2. create a Glue Catalog database
3. read/write to both of these ^^^
4. do an Athena query

I just gave mine `AdministratorAccess` like a fool.

## Step 3 - Run [the `iceberg-glue.ipynb` notebook](./notebooks/iceberg/iceberg-glue.ipynb)

```bash
cd ./nyc-taxi-demand-forecast/notebooks
uv sync
```

1. open the notebook in VS Code or PyCharm or JupyterLab
2. You will probably need to point VS Code / PyCharm at the right `.venv`

It will

1. use `pulumi` to create an S3 bucket and Glue Catalog database
2. use `pyiceberg` register the nyc taxi data as an iceberg table in the catalog with
3. attempt to connect to the Glue Catalog using `duckdb` (fails here ❌)
