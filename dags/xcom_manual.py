"""XCom via explicit push/pull (manual wiring)."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "xcom", "manual"],
    description="Pass data with ti.xcom_push / xcom_pull, including a dict and multiple upstreams.",
)
def xcom_manual():

    @task
    def produce(ti):
        ti.xcom_push(key="value", value=42)
        ti.xcom_push(key="payload", value={"val_1": 42, "val_2": 43})

    @task
    def also_produce(ti):
        ti.xcom_push(key="value", value=43)

    @task
    def consume(ti):
        one = ti.xcom_pull(task_ids="produce", key="value")
        payload = ti.xcom_pull(task_ids="produce", key="payload")
        many = ti.xcom_pull(task_ids=["produce", "also_produce"], key="value")
        print(one, payload, many)

    [produce(), also_produce()] >> consume()


xcom_manual()
