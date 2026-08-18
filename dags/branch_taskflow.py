"""Branching with TaskFlow: the branch task receives the upstream return value."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "branch", "taskflow"],
    description="@task.branch chooses success or failure from the previous return value.",
)
def branch_taskflow():

    @task
    def start() -> str:
        return "run_success"

    @task.branch
    def choose_task(next_task: str) -> str:
        return next_task

    @task
    def run_success():
        print("success")

    @task
    def run_failure():
        print("failure")

    choose_task(start()) >> [run_success(), run_failure()]


branch_taskflow()
