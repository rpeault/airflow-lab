from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dynamic"],
)
def process_file_a():

    @task
    def extract() -> str:
        return "file_a.csv"

    @task
    def process(ti) -> str:
        filename = ti.xcom_pull(task_ids="extract", key="return_value")
        return filename

    @task
    def send_email(ti):
        filename = ti.xcom_pull(task_ids="process", key="return_value")
        print(filename)

    extract() >> process() >> send_email()


process_file_a()
