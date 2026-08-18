from airflow.sdk import dag, task
from pendulum import datetime


@dag(start_date=datetime(2022, 1, 1), schedule="@once")
def xcom_share_multiple():

    @task
    def t1():
        return 42

    @task(multiple_outputs=True)
    def t2(value: int):
        print(value)
        return {"my_val": 42, "my_second_val": 56}

    @task()
    def t3(first_value: int, second_value: int):
        print(first_value)
        print(second_value)

    values = t2(t1())
    t3(values["my_val"], values["my_second_val"])


xcom_share_multiple()
