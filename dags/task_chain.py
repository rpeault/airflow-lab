from airflow.sdk import chain, dag, task
from pendulum import datetime

default_args = {
    "retries": 3,
}


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    description="A simple DAG to test the Airflow scheduler",
    tags=["team_a", "test"],
    max_consecutive_failed_dag_runs=3,
)
def task_chain():

    @task
    def task_a():
        print("Task A")

    @task
    def task_b():
        print("Task B")

    @task
    def task_c():
        print("Task C")

    @task
    def task_d():
        print("Task D")

    @task
    def task_e():
        print("Task E")

    chain(task_a(), [task_b(), task_d()], [task_c(), task_e()])
    # a=task_a()
    # a >> task_b() >> task_c()
    # a >> task_d() >> task_e()


task_chain()
