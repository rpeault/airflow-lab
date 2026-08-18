from airflow.sdk import dag, task
from pendulum import datetime

from common.paths import data_dir


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["sensor"],
    catchup=False,
)
def sensor_wait_for_csv():

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")
    def wait_for_files(filepath: str) -> bool:
        return (data_dir() / filepath).is_file()

    @task
    def process_file(ti):
        files = ti.xcom_pull(task_ids="wait_for_files", key="return_value")
        print(f"I processed the files: {files}")

    wait_for_files.expand(filepath=["data_1.csv", "data_2.csv", "data_3.csv"]) >> process_file()


sensor_wait_for_csv()
