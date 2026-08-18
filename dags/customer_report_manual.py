"""Same customer report pipeline, wired with explicit xcom_push / xcom_pull."""

from airflow.sdk import dag, task

from common import customer_report as report
from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "example", "manual"],
    description="Customer report: each task pushes and pulls named XCom keys.",
)
def customer_report_manual():

    @task
    def extract_customers(ti):
        ti.xcom_push(key="customers", value=report.SAMPLE_CUSTOMERS)

    @task
    def extract_orders(ti):
        ti.xcom_push(key="orders", value=report.SAMPLE_ORDERS)

    @task
    def clean_customers(ti):
        customers = ti.xcom_pull(task_ids="extract_customers", key="customers")
        ti.xcom_push(key="clean_customers", value=report.clean_customers(customers))

    @task
    def clean_orders(ti):
        orders = ti.xcom_pull(task_ids="extract_orders", key="orders")
        ti.xcom_push(key="clean_orders", value=report.clean_orders(orders))

    @task
    def build_report(ti):
        customers = ti.xcom_pull(task_ids="clean_customers", key="clean_customers")
        orders = ti.xcom_pull(task_ids="clean_orders", key="clean_orders")
        ti.xcom_push(key="report", value=report.build_report(customers, orders))

    @task
    def publish_report(ti):
        report_data = ti.xcom_pull(task_ids="build_report", key="report")
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


customer_report_manual()
