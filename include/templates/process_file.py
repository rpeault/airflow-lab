from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    start_date=START_DATE,
    schedule="SCHEDULE_INTERVAL_HOLDER",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "generated"],
    description="Generated DAG: extract, process, and print INPUT_HOLDER.",
)
def process_DAG_ID_HOLDER():

    @task
    def extract() -> str:
        return "INPUT_HOLDER"

    @task
    def process(filename: str) -> str:
        return filename

    @task
    def send_email(filename: str):
        print(filename)

    send_email(process(extract()))


process_DAG_ID_HOLDER()
