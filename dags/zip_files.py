"""XComArg.zip(): merge lists from several tasks into tuples."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "mapping", "taskflow"],
    description="Zip path, filename, and extension XComs; print each triple.",
)
def zip_files():

    @task
    def get_path() -> list[str]:
        return ["/usr/local/", "/bin/test/", "/home/me/"]

    @task
    def get_filenames() -> list[str]:
        return ["file_a", "file_b", "file_c"]

    @task
    def get_extensions() -> list[str]:
        return [".txt", ".zip", ".parquet"]

    @task
    def download(zipped):
        print([f"{path} {name} {ext}" for path, name, ext in zipped])

    download(get_path().zip(get_filenames(), get_extensions()))


zip_files()
