from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def sensor_always_false():

    @task.sensor(poke_interval=60, timeout=7 * 24 * 60 * 60, mode="reschedule")
    def waiting_for_condition():
        return False

    waiting_for_condition()


sensor_always_false()
