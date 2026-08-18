"""Task groups with @task_group.

Related tasks nest in the Graph view. The group function returns None;
wire start and finish to the group with chain().
"""

from airflow.sdk import chain, dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "task_group"],
    description="@task_group nests related tasks; wire the group with chain().",
)
def task_group():
    from airflow.sdk import task_group

    @task
    def start():
        print("start")

    @task
    def finish():
        print("done")

    @task_group
    def process(value: int) -> None:
        """Add 5, then double."""

        @task
        def add_five(n: int):
            return n + 5

        @task
        def times_two(n):
            result = n * 2
            print(result)
            return result

        times_two(add_five(value))

    chain(start(), process(10), finish())


task_group()
