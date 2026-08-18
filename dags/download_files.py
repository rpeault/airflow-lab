import random

from airflow.sdk import dag, task
from pendulum import datetime

DOWNLOAD_FOLDER = "/usr/local"


@dag(
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
)
def download_files():

    @task
    def get_files() -> list[str]:
        return [f"file_{nb}" for nb in range(random.randint(3, 5))]

    @task
    def download_file(folder: str, file: str) -> str:
        return f"{folder}/{file}"

    @task.bash
    def print_files():
        return "echo '{{ ti.xcom_pull(task_ids='download_file', key='return_value') | list }}'"

    file_list = get_files()
    files = download_file.partial(folder=DOWNLOAD_FOLDER).expand(file=file_list)
    file_list >> files >> print_files()


download_files()
