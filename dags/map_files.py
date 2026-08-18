"""Dynamic task mapping: expand() plus partial()."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE

DOWNLOAD_FOLDER = "/usr/local"


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "mapping"],
    description="One mapped task per file; folder is fixed with partial().",
)
def map_files():

    @task
    def get_files() -> list[str]:
        return [f"file_{nb}" for nb in range(3, 6)]

    @task
    def download_file(folder: str, file: str) -> str:
        return f"{folder}/{file}"

    @task.bash
    def print_files():
        return "echo '{{ ti.xcom_pull(task_ids='download_file', key='return_value') | list }}'"

    file_list = get_files()
    files = download_file.partial(folder=DOWNLOAD_FOLDER).expand(file=file_list)
    file_list >> files >> print_files()


map_files()
