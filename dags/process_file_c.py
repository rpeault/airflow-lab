from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    start_date=START_DATE,
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "generated"],
    description="Generated DAG: extract, process, and print file_c.csv.",
)
def process_file_c():

    @task
    def extract() -> str:
        return "file_c.csv"

    @task
    def process(filename: str) -> str:
        return filename

    @task
    def send_email(filename: str):
        print(filename)

    send_email(process(extract()))


process_file_c()
