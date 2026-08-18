"""Same customer report pipeline, hybrid: return values, then xcom_pull."""

from airflow.sdk import dag, task

from common import customer_report as report
from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "example", "hybrid"],
    description="Customer report: tasks return values; downstream tasks pull them by task_id.",
)
def customer_report_hybrid():

    @task
    def extract_customers() -> list[str]:
        return report.SAMPLE_CUSTOMERS

    @task
    def extract_orders() -> list[dict]:
        return report.SAMPLE_ORDERS

    @task
    def clean_customers(ti) -> list[str]:
        customers = ti.xcom_pull(task_ids="extract_customers")
        return report.clean_customers(customers)

    @task
    def clean_orders(ti) -> list[dict]:
        orders = ti.xcom_pull(task_ids="extract_orders")
        return report.clean_orders(orders)

    @task
    def build_report(ti) -> dict:
        customers = ti.xcom_pull(task_ids="clean_customers")
        orders = ti.xcom_pull(task_ids="clean_orders")
        return report.build_report(customers, orders)

    @task
    def publish_report(ti):
        report_data = ti.xcom_pull(task_ids="build_report")
        print(f"Publishing report: {report_data}")

    extract_customers_task = extract_customers()
    extract_orders_task = extract_orders()
    clean_customers_task = clean_customers()
    clean_orders_task = clean_orders()
    build_report_task = build_report()
    publish_report_task = publish_report()

    extract_customers_task >> clean_customers_task
    extract_orders_task >> clean_orders_task
    [clean_customers_task, clean_orders_task] >> build_report_task >> publish_report_task


customer_report_hybrid()
