"""SOQL via SalesforceHook. Needs connection salesforce_default."""

from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.sdk import dag, task

from common.defaults import DEFAULT_ARGS, START_DATE


@dag(
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab", "salesforce"],
    description="Query a few Accounts with SalesforceHook. Create connection salesforce_default first.",
)
def salesforce_query():

    @task
    def query_accounts():
        hook = SalesforceHook(salesforce_conn_id="salesforce_default")
        result = hook.make_query("SELECT Id, Name FROM Account LIMIT 5")
        records = result.get("records", [])
        print(result.get("totalSize"), records)
        return records

    query_accounts()


salesforce_query()
