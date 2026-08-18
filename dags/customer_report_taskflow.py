from airflow.sdk import dag, task
from pendulum import datetime

from common import customer_report as report_logic


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example", "taskflow"],
)
def customer_report_taskflow():
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
    def clean_customers(customers):
        return report_logic.clean_customers(customers)

    @task
    def clean_orders(orders):
        return report_logic.clean_orders(orders)

    @task
    def build_report(customers, orders):
        return report_logic.build_report(customers, orders)

    @task
    def publish_report(report):
        print(f"Publishing report: {report}")

    customers = clean_customers(extract_customers())
    orders = clean_orders(extract_orders())
    publish_report(build_report(customers, orders))


customer_report_taskflow()
