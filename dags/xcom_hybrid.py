"""XCom hybrid: TaskFlow return, then xcom_pull of return_value."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "xcom", "hybrid"],
    description="Return a value from one task, pull it by task_id in the next.",
)
def xcom_hybrid():

    @task
    def produce() -> int:
        return 42

    @task
    def consume(ti):
        val = ti.xcom_pull(task_ids="produce", key="return_value")
        print(val)

    produce() >> consume()


xcom_hybrid()
