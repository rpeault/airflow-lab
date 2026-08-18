"""HTTP GET with HttpOperator (classic provider operator)."""

import json

from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "http", "api"],
    description="GET a public JSON API with HttpOperator; print the XCom downstream.",
)
def http_get():

    get_api_result = HttpOperator(
        task_id="get_api",
        http_conn_id="api",
        method="GET",
        endpoint="/todos/1",
        extra_options={"timeout": 10},
    )

    @task
    def parse_results(api_result):
        print(json.loads(api_result))

    parse_results(get_api_result.output)


http_get()
