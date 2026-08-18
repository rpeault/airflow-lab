from airflow.sdk import dag, task
from pendulum import datetime

from common import customer_report as report_logic


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "manual-routing"],
)
def customer_report_manual_routing():
    @task
    def extract_customers(ti):
        customers = ["alice", "bob", "charlie"]
        ti.xcom_push(key="customers", value=customers)

    @task
    def extract_orders(ti):
        orders = [
            {"customer": "alice", "amount": 100},
            {"customer": "bob", "amount": 50},
            {"customer": "alice", "amount": 25},
        ]
        ti.xcom_push(key="orders", value=orders)

    @task
    def clean_customers(ti):
        customers = ti.xcom_pull(task_ids="extract_customers", key="customers")
        cleaned = report_logic.clean_customers(customers)
        ti.xcom_push(key="clean_customers", value=cleaned)

    @task
    def clean_orders(ti):
        orders = ti.xcom_pull(task_ids="extract_orders", key="orders")
        cleaned = report_logic.clean_orders(orders)
        ti.xcom_push(key="clean_orders", value=cleaned)

    @task
    def build_report(ti):
        customers = ti.xcom_pull(task_ids="clean_customers", key="clean_customers")
        orders = ti.xcom_pull(task_ids="clean_orders", key="clean_orders")
        report = report_logic.build_report(customers, orders)
        ti.xcom_push(key="report", value=report)

    @task
    def publish_report(ti):
        report = ti.xcom_pull(task_ids="build_report", key="report")
        print(f"Publishing report: {report}")

    extract_customers_task = extract_customers()
    extract_orders_task = extract_orders()
    clean_customers_task = clean_customers()
    clean_orders_task = clean_orders()
    build_report_task = build_report()
    publish_report_task = publish_report()

    extract_customers_task >> clean_customers_task
    extract_orders_task >> clean_orders_task
    [clean_customers_task, clean_orders_task] >> build_report_task >> publish_report_task


customer_report_manual_routing()
