"""XCom via TaskFlow: return values and multiple_outputs."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "xcom", "taskflow"],
    description="Pass data by returning values (TaskFlow). multiple_outputs splits a dict.",
)
def xcom_taskflow():

    @task
    def produce() -> int:
        return 42

    @task(multiple_outputs=True)
    def split(value: int) -> dict:
        return {"doubled": value * 2, "squared": value**2}

    @task
    def consume(doubled: int, squared: int):
        print(doubled, squared)

    parts = split(produce())
    consume(parts["doubled"], parts["squared"])


xcom_taskflow()
