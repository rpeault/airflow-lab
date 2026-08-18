import requests
from airflow.sdk import PokeReturnValue, dag, task
from pendulum import datetime


@dag(start_date=datetime(2022, 12, 1), schedule="@daily", catchup=False)
def sensor_http_google():

    @task.sensor(poke_interval=30, timeout=3600, mode="poke")
    def check_google_availability() -> PokeReturnValue:
        r = requests.get("https://www.google.com", timeout=10)
        print(r.status_code)

        if r.status_code == 200:
            condition_met = True
            operator_return_value = r.url
        else:
            condition_met = False
            operator_return_value = None
            print(f"Google returned the status code {r.status_code}")

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    @task
    def print_google_url(ti):
        url = ti.xcom_pull(task_ids="check_google_availability", key="return_value")
        print(url)

    check_google_availability() >> print_google_url()


sensor_http_google()
