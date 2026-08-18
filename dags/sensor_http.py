"""HTTP sensor with PokeReturnValue (poke mode)."""

import requests
from airflow.sdk import PokeReturnValue, dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "sensor"],
    description="Poke an HTTP endpoint and pass the URL downstream via PokeReturnValue.",
)
def sensor_http():

    @task.sensor(poke_interval=30, timeout=3600, mode="poke")
    def check_google() -> PokeReturnValue:
        response = requests.get("https://www.google.com", timeout=10)
        print(response.status_code)
        if response.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=response.url)
        print(f"Google returned {response.status_code}")
        return PokeReturnValue(is_done=False, xcom_value=None)

    @task
    def print_url(ti):
        print(ti.xcom_pull(task_ids="check_google", key="return_value"))

    check_google() >> print_url()


sensor_http()
