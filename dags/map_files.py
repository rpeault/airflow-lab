"""Two mapping patterns in one DAG.

.map(): transform each list item in XCom. Still two tasks.
.expand() / .partial(): one task instance per item; partial() binds constants.
"""

from airflow.sdk import chain, dag, task

from common.defaults import DEFAULT_ARGS, START_DATE

DOWNLOAD_FOLDER = "/usr/local"


def append_data(path: str) -> str:
    return path + "data/"


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "mapping"],
    description=".map() transforms a list in XCom; expand()/partial() create one task per file.",
)
def map_files():

    @task
    def list_paths() -> list[str]:
        return ["/usr/folder_a/", "/usr/folder_b/", "/usr/folder_c/"]

    @task
    def print_paths(new_list):
        print(new_list)

    print_paths(list_paths().map(append_data))

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
    chain(file_list, files, print_files())


map_files()
