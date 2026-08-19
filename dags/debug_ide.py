"""Same-process DAG run for IDE breakpoints.

Open this file, set a breakpoint in add_five or times_two, then F5
(or: python dags/debug_ide.py).
"""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["lab", "debug"],
    description="dag.test() in __main__: F5 hits breakpoints inside @task.",
)
def debug_ide():
    @task
    def add_five(n: int) -> int:
        result = n + 5
        print(result)
        return result

    @task
    def times_two(n):
        result = n * 2
        print(result)
        return result

    times_two(add_five(10))


dag = debug_ide()

if __name__ == "__main__":
    from common.dag_test import run

    run(dag)
