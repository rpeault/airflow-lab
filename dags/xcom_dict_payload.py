from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def xcom_dict_payload():

    @task
    def task_a(ti):
        val = {"val_1": 42, "val_2": 43}
        ti.xcom_push(key="my_key", value=val)

    @task
    def task_b(ti):
        val = ti.xcom_pull(task_ids="task_a", key="my_key")
        print(val)

    task_a() >> task_b()


xcom_dict_payload()
