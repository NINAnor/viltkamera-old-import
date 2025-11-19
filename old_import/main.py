#!/usr/bin/env python3

"""Main script."""

import logging
import pathlib

import click
import duckdb
import environ
import s3fs
from sqlmodel import Session, create_engine

from .logger import configure_logger
from .parquet import clean_dataset, get_dataset_by_id
from .utils import get_labels

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent
environ.Env.read_env(str(BASE_DIR / ".env"))


@click.command()
@click.argument("dataset_id", nargs=1)
@click.option("--verbose", "-v", is_flag=True, default=False, help="Print debug")
@click.option(
    "--vverbose", "-vv", is_flag=True, default=False, help="Print used queries"
)
@click.option("--clean", "-c", is_flag=True, default=False, help="Delete old")
@click.option(
    "--single",
    "-s",
    is_flag=True,
    default=False,
    help="Import the first timeseries and stop",
)
def start(dataset_id, verbose, vverbose, clean, single) -> None:
    CONNECTION_STRING = env.str("DATABASE_URL")
    EXPORT_BASE_PATH = env.str("EXPORT_BASE_PATH")

    project_path = EXPORT_BASE_PATH.replace("TABLE", "projects")
    timeseries_path = EXPORT_BASE_PATH.replace("TABLE", "timeseries")
    image_path = EXPORT_BASE_PATH.replace("TABLE", "images_metadata")

    engine = create_engine(CONNECTION_STRING, echo=vverbose)
    log = configure_logger(logging.DEBUG if verbose else logging.INFO)

    duck_conn = duckdb.connect()

    duck_conn.sql("SET memory_limit = 500MB;")

    if clean:
        with Session(engine) as s:
            clean_dataset(s, dataset_id)
            s.commit()
        return

    label_map = get_labels(engine)

    s3 = s3fs.S3FileSystem(
        key=env("FSSPEC_S3_KEY"),
        secret=env("FSSPEC_S3_SECRET"),
        endpoint_url=env("FSSPEC_S3_ENDPOINT_URL"),
    )

    # get the dataset by id
    get_dataset_by_id(
        dataset_id=dataset_id,
        connection=duck_conn,
        project_path=project_path,
        timeseries_path=timeseries_path,
        image_path=image_path,
        engine=engine,
        label_map=label_map,
        image_target_path=f"s3://{env('S3_BUCKET')}/media/",
        image_source_path=f"{env('API_URL')}/images/",
        log=log,
        api_user=env("API_USER"),
        api_password=env("API_PASSWORD"),
        api_base=env("API_URL"),
        single=single,
        s3=s3,
    )


if __name__ == "__main__":
    start()
