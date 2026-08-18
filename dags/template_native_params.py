from airflow.sdk import Param, dag, task
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "numbers": Param(
            [1, 2, 3],
            type=["null", "array"],
            items={"type": "number"},
            title="Numbers to sum",
            description="List of numbers passed into the task as a native Python list.",
        ),
    },
)
def template_native_params():

    @task
    def sum_nb(numbers):
        total = 0
        for val in numbers:
            total += val
        return total

    sum_nb("{{ params.numbers }}")


template_native_params()
