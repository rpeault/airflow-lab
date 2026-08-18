"""Task groups with @task_group.

Related tasks nest in the Graph view. Pass a value into the group and
return from it like any TaskFlow task.
"""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "task_group"],
    description="@task_group nests related tasks; return values flow in and out.",
)
def task_group():
    from airflow.sdk import task_group

    @task
    def start() -> int:
        return 10

    @task_group(tooltip="Add 5, then double")  # type: ignore[arg-type]
    def process(value):
        @task
        def add_five(n) -> int:
            return n + 5

        @task
        def times_two(n) -> int:
            return n * 2

        return times_two(add_five(value))

    @task
    def finish(result):
        print(result)

    finish(process(start()))


task_group()
