"""Inline @task.bash: create, test, then read a file."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "bash"],
    description="Bash tasks write and test /tmp/dummy, then a Python task reads it.",
)
def bash_tmp_file():

    @task.bash
    def create_file():
        return 'echo "Hi there!" >/tmp/dummy'

    @task.bash
    def check_file_exists():
        return "test -f /tmp/dummy"

    @task
    def read_file():
        with open("/tmp/dummy", "rb") as f:
            print(f.read())

    create_file() >> check_file_exists() >> read_file()


bash_tmp_file()
