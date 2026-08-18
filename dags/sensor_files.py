"""Mapped file sensor (reschedule mode)."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE
from common.paths import data_dir


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "sensor", "mapping"],
    description="Wait for CSVs under data/, one mapped sensor per file.",
)
def sensor_files():

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")
    def wait_for_file(filepath: str) -> bool:
        return (data_dir() / filepath).is_file()

    @task
    def process_files(ti):
        files = ti.xcom_pull(task_ids="wait_for_file", key="return_value")
        print(f"Processed: {files}")

    wait_for_file.expand(filepath=["data_1.csv", "data_2.csv", "data_3.csv"]) >> process_files()


sensor_files()
