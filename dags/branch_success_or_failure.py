from airflow.sdk import dag, task
from pendulum import datetime


@dag(start_date=datetime(2022, 1, 1), schedule="@once")
def branch_success_or_failure():

    @task
    def start(retries=3):
        if retries > 3:
            return "failure"
        return "success"

    @task.branch
    def choose_task(next_task: str):
        return next_task

    @task
    def success():
        print("success")

    @task
    def failure():
        print("failure")

    choose_task(start()) >> [success(), failure()]


branch_success_or_failure()
