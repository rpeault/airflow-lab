"""Local ``dag.test()`` setup for IDE debugging (SQLite under ``.airflow-local/``).

Call from a DAG file:

    if __name__ == "__main__":
        from common.dag_test import run

        run(dag)
"""

from __future__ import annotations

import json
import logging
import os
import warnings

from common.paths import project_root


def run(dag) -> None:
    """Point Airflow at the lab SQLite home, migrate it, then run ``dag.test()``."""
    root = project_root()
    home = root / ".airflow-local"
    dags = root / "dags"
    home.mkdir(exist_ok=True)
    db = home / "airflow.db"

    os.environ["AIRFLOW_HOME"] = str(home)
    os.environ["AIRFLOW_CONFIG"] = str(home / "airflow.cfg")
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
    os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] = "True"
    os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "lab-jwt-secret-local"
    os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "WARNING"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{db}"
    os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps(
        [
            {
                "name": "dags-folder",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(dags)},
            }
        ]
    )

    import airflow.configuration as configuration
    import airflow.settings as settings
    from airflow.logging_config import configure_logging

    configuration.AIRFLOW_HOME = str(home)
    configuration.AIRFLOW_CONFIG = str(home / "airflow.cfg")
    settings.configure_vars()
    settings.reconfigure_orm()
    configure_logging()
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=PendingDeprecationWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    logging.getLogger("py.warnings").setLevel(logging.ERROR)

    from airflow.utils.db import upgradedb

    upgradedb()
    dag.test()
