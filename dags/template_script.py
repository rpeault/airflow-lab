"""Jinja-templated bash script from include/."""

from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE
from common.paths import include_dir


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "template"],
    description="Run include/scripts/script.sh via template_searchpath.",
    template_searchpath=[str(include_dir())],
)
def template_script():

    @task.bash
    def run_script():
        return "scripts/script.sh"

    run_script()


template_script()
