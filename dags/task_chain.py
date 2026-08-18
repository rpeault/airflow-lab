"""DAG options, retries, and `chain()` for mixed linear / parallel dependencies."""

from airflow.sdk import chain, dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule="@daily",
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "chain"],
    description="Scheduled DAG: chain() with a fan-out then fan-in.",
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


task_chain()
