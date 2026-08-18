from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="DAG to check data",
    tags=["data_engineering"],
)
def check_tmp_file():

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


check_tmp_file()
