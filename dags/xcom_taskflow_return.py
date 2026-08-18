from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def xcom_taskflow_return():

    @task
    def task_a():
        return 42

    @task
    def task_b(ti):
        val = ti.xcom_pull(task_ids="task_a", key="return_value")
        print(val)

    task_a() >> task_b()


xcom_taskflow_return()
