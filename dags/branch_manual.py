"""Branching with xcom_pull, then join skipped tasks with a trigger rule."""

from airflow.sdk import dag, task
from pendulum import now

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "branch", "manual"],
    description="@task.branch reads XCom, then a join task uses none_failed_min_one_success.",
)
def branch_manual():

    @task
    def get_day_type() -> str:
        if now().weekday() < 5:
            return "weekday"
        return "weekend"

    @task.branch
    def choose_branch(ti) -> str:
        day_type = ti.xcom_pull(task_ids="get_day_type", key="return_value")
        if day_type == "weekday":
            return "run_weekday_job"
        return "run_weekend_job"

    @task
    def run_weekday_job():
        print("Weekday branch")

    @task
    def run_weekend_job():
        print("Weekend branch")

    @task(trigger_rule="none_failed_min_one_success")
    def finalize():
        print("Join after the chosen branch")

    get_day_type() >> choose_branch() >> [run_weekday_job(), run_weekend_job()] >> finalize()


branch_manual()
