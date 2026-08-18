"""Pure Python transforms for the customer report example DAGs.

Keep Airflow wiring in the DAG file. Put logic here so you can:
- unit test it with pytest
- set breakpoints in Cursor
- prototype it in a notebook
"""

from __future__ import annotations

SAMPLE_CUSTOMERS = [" alice", "BOB", "charlie "]
SAMPLE_ORDERS = [
    {"customer": "alice", "amount": 100},
    {"customer": "bob", "amount": 0},
    {"customer": "alice", "amount": 25},
]


def clean_customers(customers: list[str]) -> list[str]:
    return [name.strip().title() for name in customers]


def clean_orders(orders: list[dict]) -> list[dict]:
    return [order for order in orders if order["amount"] > 0]


def build_report(customers: list[str], orders: list[dict]) -> dict:
    totals: dict[str, int] = {}
    for order in orders:
        customer = order["customer"].title()
        totals[customer] = totals.get(customer, 0) + order["amount"]
    return {
        "customer_count": len(customers),
        "sales_totals": totals,
    }


if __name__ == "__main__":
    cleaned_customers = clean_customers(SAMPLE_CUSTOMERS)
    cleaned_orders = clean_orders(SAMPLE_ORDERS)
    print(build_report(cleaned_customers, cleaned_orders))
