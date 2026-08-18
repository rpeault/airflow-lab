from airflow.sdk import dag, task
from pendulum import datetime, now


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["branch"],
)
def branch_by_condition():

    @task
    def get_day_type() -> str:
        today = now()
        if today.weekday() < 5:
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


branch_by_condition()
