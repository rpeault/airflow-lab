from airflow.sdk import dag, task
from pendulum import datetime

from common.paths import include_dir


@dag(
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    template_searchpath=[str(include_dir())],
)
def template_bash_script():

    @task.bash
    def run_this():
        return "scripts/script.sh"

    run_this()


template_bash_script()
