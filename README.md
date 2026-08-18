# Airflow lab

Run Airflow with **Docker Compose** (one `apache/airflow` container + Postgres).
A local **`.venv` is optional** and only for Cursor (debug, Jupyter, Ruff, pytest). It is generated and gitignored.

Airflow version is the image tag in `docker-compose.yaml` (`apache/airflow:3.3.1`).

## Start Airflow

Needs Docker Compose v2.

```bash
mkdir -p dags logs plugins config
docker compose up -d
```

UI: http://localhost:8080 (no login)

```bash
docker compose logs -f airflow-standalone
docker compose exec airflow-standalone airflow dags list
docker compose exec airflow-standalone bash
docker compose down          # stop
docker compose down -v       # stop and wipe the database
```

In Cursor, the **Docker** extension can do the same: Compose Up / Down, logs, exec.

The container runs as uid **50000** by default (no `.env` file). On Linux/WSL, if `logs/` is not writable, create a `.env` with only:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d --force-recreate
```

## IDE: debug and Jupyter

`.venv` is **not** a setup file. Create it when you want breakpoints or notebooks.

**Create**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pip install "apache-airflow==3.3.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
```

Match the constraints file to your Python (`python3 --version`): `3.12`, `3.13`, or `3.14`.

**Recreate** (safe anytime)

```bash
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pip install "apache-airflow==3.3.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
```

**In Cursor**

1. Extensions if prompted: Python, Ruff, Jupyter, Docker.
2. **Python: Select Interpreter** → `.venv/bin/python`.
3. **Debug:** open `dags/common/customer_report.py` or a test, breakpoint, **F5** (*Debug current file* or *Pytest current file*).
4. **Jupyter:** open `notebooks/lab.ipynb` → kernel **Python (.venv)**.

```bash
ruff check dags tests
ruff check dags tests --fix
pytest
```

## Change Airflow version

1. Set the image tag in `docker-compose.yaml` (`apache/airflow:X.Y.Z`).
2. `docker compose pull && docker compose up -d`
3. If you use `.venv`, recreate it (commands above) with the same `X.Y.Z` and constraints URL.

Major upgrades may need `docker compose down -v`.

## Write DAGs

- `from airflow.sdk import dag, task`
- File stem = function name = DAG id (no `_dag` suffix)
- Shared code: `from common.paths import data_dir`
- Put logic in `dags/common/` so you can test and debug it without the scheduler
- Custom operators / hooks / triggers: `plugins/`

## Layout

```
dags/                 DAGs + common/ helpers
logs/                 container logs (gitignored)
plugins/
config/               generated airflow.cfg (gitignored)
include/              SQL, templates
data/                 sample files
tests/
notebooks/lab.ipynb
requirements-dev.txt  Ruff, pytest, Jupyter kernel (IDE only)
docker-compose.yaml   image tag = Airflow version
```

`.venv/` is generated locally. Do not commit it.
