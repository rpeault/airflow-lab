from airflow.sdk import dag, task
from pendulum import datetime

from common import customer_report as report_logic


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "hybrid"],
)
def customer_report_hybrid_return_pull():
    @task
    def extract_customers():
        return ["alice", "bob", "charlie"]

    @task
    def extract_orders():
        return [
            {"customer": "alice", "amount": 100},
            {"customer": "bob", "amount": 50},
            {"customer": "alice", "amount": 25},
        ]

    @task
    def clean_customers(ti):
        customers = ti.xcom_pull(task_ids="extract_customers")
        return report_logic.clean_customers(customers)

    @task
    def clean_orders(ti):
        orders = ti.xcom_pull(task_ids="extract_orders")
        return report_logic.clean_orders(orders)

    @task
    def build_report(ti):
        customers = ti.xcom_pull(task_ids="clean_customers")
        orders = ti.xcom_pull(task_ids="clean_orders")
        return report_logic.build_report(customers, orders)

    @task
    def publish_report(ti):
        report = ti.xcom_pull(task_ids="build_report")
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


customer_report_hybrid_return_pull()
