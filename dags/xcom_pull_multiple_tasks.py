from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def xcom_pull_multiple_tasks():

    @task
    def task_a(ti):
        val = 42
        ti.xcom_push(key="my_key", value=val)

    @task
    def task_c(ti):
        val = 43
        ti.xcom_push(key="my_key", value=val)

    @task
    def task_b(ti):
        vals = ti.xcom_pull(task_ids=["task_a", "task_c"], key="my_key")
        print(vals)

    task_a() >> task_c() >> task_b()


xcom_pull_multiple_tasks()
