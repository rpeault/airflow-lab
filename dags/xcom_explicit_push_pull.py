from airflow.sdk import Context, dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def xcom_explicit_push_pull():

    @task
    def task_a(**context: Context):
        val = 42
        context["ti"].xcom_push(key="my_key", value=val)

    @task
    def task_b(**context: Context):
        val = context["ti"].xcom_pull(task_ids="task_a", key="my_key")
        print(val)

    task_a() >> task_b()


xcom_explicit_push_pull()
