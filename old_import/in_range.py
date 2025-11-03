import logging
import pathlib

import click
import duckdb
import environ

from .logger import configure_logger
from .main import start as import_start

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent
environ.Env.read_env(str(BASE_DIR / ".env"))


@click.command()
@click.argument("dataset_ids", nargs=2)
@click.option("--verbose", "-v", is_flag=True, default=False, help="Print debug")
@click.option(
    "--vverbose", "-vv", is_flag=True, default=False, help="Print used queries"
)
@click.option(
    "--single",
    "-s",
    is_flag=True,
    default=False,
    help="Import the first timeseries and stop",
)
@click.pass_context
def start_range(ctx, dataset_ids, verbose, vverbose, single) -> None:
    EXPORT_BASE_PATH = env.str("EXPORT_BASE_PATH")
    project_path = EXPORT_BASE_PATH.replace("TABLE", "projects")
    log = configure_logger(logging.DEBUG if verbose else logging.INFO)
    duck_conn = duckdb.connect()
    datasets = (
        duck_conn.execute(
            f"select id from read_parquet('{project_path}') where id BETWEEN $1 AND $2",  # noqa: E501, S608
            [dataset_ids[0], dataset_ids[1]],
        )
        .fetch_arrow_table()
        .to_pylist()
    )

    log.debug("Found datasets", datasets=datasets)

    for dataset in datasets:
        dataset_id = dataset["id"]
        log.debug(f"Importing dataset {dataset_id}")
        ctx.invoke(
            import_start,
            dataset_id=dataset_id,
            verbose=verbose,
            vverbose=vverbose,
            single=single,
            clean=False,
        )


if __name__ == "__main__":
    start_range()
