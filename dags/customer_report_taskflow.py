"""Same customer report pipeline, wired with TaskFlow (return values as arguments)."""

from airflow.sdk import dag, task

from common import customer_report as report
from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "example", "taskflow"],
    description="Customer report: TaskFlow passes data as function arguments.",
)
def customer_report_taskflow():

    @task
    def extract_customers() -> list[str]:
        return report.SAMPLE_CUSTOMERS

    @task
    def extract_orders() -> list[dict]:
        return report.SAMPLE_ORDERS

    @task
    def clean_customers(customers: list[str]) -> list[str]:
        return report.clean_customers(customers)

    @task
    def clean_orders(orders: list[dict]) -> list[dict]:
        return report.clean_orders(orders)

    @task
    def build_report(customers: list[str], orders: list[dict]) -> dict:
        return report.build_report(customers, orders)

    @task
    def publish_report(report_data: dict):
        print(f"Publishing report: {report_data}")

    customers = clean_customers(extract_customers())
    orders = clean_orders(extract_orders())
    publish_report(build_report(customers, orders))


customer_report_taskflow()
