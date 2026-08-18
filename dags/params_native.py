"""DAG params rendered as native Python objects."""

from airflow.sdk import Param, dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "params"],
    description="Trigger with a numbers param; render_template_as_native_obj keeps it a list.",
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
def params_native():

    @task
    def sum_numbers(numbers):
        return sum(numbers)

    sum_numbers("{{ params.numbers }}")


params_native()
