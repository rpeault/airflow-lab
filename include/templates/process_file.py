from pendulum import datetime

from airflow.sdk import dag, task


@dag(
    start_date=datetime(2022, 1, 1),
    schedule="SCHEDULE_INTERVAL_HOLDER",
    catchup=False,
    tags=["dynamic"],
)
def process_DAG_ID_HOLDER():

    @task
    def extract() -> str:
        return "INPUT_HOLDER"

    @task
    def process(ti) -> str:
        filename = ti.xcom_pull(task_ids="extract", key="return_value")
        return filename

    @task
    def send_email(ti):
        filename = ti.xcom_pull(task_ids="process", key="return_value")
        print(filename)

    extract() >> process() >> send_email()


process_DAG_ID_HOLDER()
