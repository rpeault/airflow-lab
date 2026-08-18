from common.customer_report import build_report, clean_customers, clean_orders


def test_clean_customers_strips_and_titles_names():
    assert clean_customers([" alice", "BOB"]) == ["Alice", "Bob"]


def test_clean_orders_drops_non_positive_amounts():
    orders = [
        {"customer": "alice", "amount": 100},
        {"customer": "bob", "amount": 0},
        {"customer": "charlie", "amount": -5},
    ]
    assert clean_orders(orders) == [{"customer": "alice", "amount": 100}]


def test_build_report_aggregates_sales():
    customers = ["Alice", "Bob"]
    orders = [
        {"customer": "alice", "amount": 100},
        {"customer": "bob", "amount": 50},
        {"customer": "alice", "amount": 25},
    ]
    assert build_report(customers, orders) == {
        "customer_count": 2,
        "sales_totals": {"Alice": 125, "Bob": 50},
    }
