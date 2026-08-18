# Airflow lab

Learning lab for **Airflow 3** DAG patterns. Not a production template (standalone container, open UI, lab secrets).

Airflow runs in **Docker Compose** (image tag in `docker-compose.yaml`, currently `apache/airflow:3.3.1`).
A local **`.venv` is optional** (Cursor only: debug, Jupyter, Ruff, pytest). It is gitignored.

**Need:** Docker Compose. Optional IDE venv: Python 3.10+.

## Start Airflow

```bash
docker compose up -d
```

First start takes about a minute. UI: http://localhost:8080 (no login).

```bash
docker compose logs -f airflow-standalone
docker compose exec airflow-standalone airflow dags list
docker compose down          # stop
docker compose down -v       # stop and wipe the database
```

The **Docker** extension in Cursor can do the same (Compose Up / Down, logs, exec).

If `logs/` is not writable on Linux/WSL:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d --force-recreate
```

## Run a DAG

1. Open http://localhost:8080 and wait until DAGs appear.
2. Pick a DAG with `schedule=None` (for example `params_native`) so it only runs when you trigger it.
3. Unpause if it is paused, then **Trigger**.
4. Open the run → task → logs.

`task_chain` and `process_file_*` are `@daily`; the scheduler can also start them.

## Write DAGs

- `from airflow.sdk import dag, task`
- File stem = function name = DAG id (no `_dag` suffix)
- Shared defaults: `from common.defaults import DEFAULT_ARGS, START_DATE`
- Shared code: `from common.paths import data_dir`
- Put logic in `dags/common/` so you can pytest and F5-debug it without the scheduler
- Custom operators / hooks / triggers: `plugins/`

Most examples use `schedule=None` (trigger in the UI). `task_chain` and `process_file_*` are `@daily`.
Longer notes (CSVs, generator, what each DAG does): [EXAMPLES.md](EXAMPLES.md).

| DAG | What it shows |
|---|---|
| `task_chain` | DAG options, `chain()`, fan-out |
| `xcom_taskflow` / `xcom_manual` / `xcom_hybrid` | Pass data: return values, push/pull, mixed |
| `branch_taskflow` / `branch_manual` | `@task.branch` (arg vs `xcom_pull`) + join |
| `map_files` | `.expand()` / `.partial()` |
| `sensor_files` / `sensor_http` | `@task.sensor` (reschedule vs poke + `PokeReturnValue`) |
| `bash_tmp_file` | `@task.bash` |
| `template_script` | Jinja script under `include/` |
| `params_native` | DAG params as native Python |
| `customer_report_taskflow` / `_manual` / `_hybrid` | Same pipeline, three wiring styles |
| `process_file_*` | Generated from `include/data/*.json` |

Regenerate `process_file_*.py` after changing `include/data/*.json` or `include/templates/process_file.py` (do not edit the generated files by hand):

```bash
python3 include/scripts/generate_dag.py
```

## Layout

```
dags/                 DAGs + common/ helpers
logs/                 container logs (gitignored)
plugins/
config/               generated airflow.cfg (gitignored)
include/              templates, JSON for generated DAGs
data/                 sample files
tests/                pytest for dags/common/ (not full DAG runs)
notebooks/lab.ipynb
EXAMPLES.md           what each DAG shows (files, generator)
requirements-dev.txt  pendulum, Ruff, pytest, Jupyter (IDE only)
docker-compose.yaml   image tag = Airflow version
```

## IDE (optional)

Only if you want breakpoints, notebooks, Ruff, or pytest. Airflow itself still runs in Docker.

**Create** `.venv`:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

**Recreate** (safe anytime: broken venv, missing `notebook`/`ipykernel`, Python upgrade):

```bash
rm -rf .venv
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

In Cursor, after create or recreate:

1. Install recommended extensions if prompted (Python, Ruff, Jupyter, Docker).
2. **Python: Select Interpreter** (`Ctrl+Shift+P` / `Cmd+Shift+P`) → `.venv/bin/python`.
3. **Debug:** breakpoint in `dags/common/customer_report.py` or a test → **F5**. That runs the current file in `.venv`. A breakpoint inside `@task` in a DAG file will not hit: importing the file only parses the DAG; the task body runs in Docker.
4. **Jupyter:** `notebooks/lab.ipynb` → kernel **Python (.venv)**. If Cursor says the `notebook` package is required, click **Install** or run `pip install -r requirements-dev.txt` again.

```bash
.venv/bin/ruff check dags tests
.venv/bin/pytest
```

To make `from airflow.sdk` resolve in DAG files, also install Airflow into `.venv` (same version as the image), after the steps above. Do not add Airflow to `requirements-dev.txt` (it needs the constraint file):

```bash
PY=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
.venv/bin/pip install "apache-airflow==3.3.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-${PY}.txt"
```

If you recreated `.venv` and still need DAG imports, run that Airflow install again.

## Change Airflow version

1. Set the image tag in `docker-compose.yaml` (`apache/airflow:X.Y.Z`).
2. `docker compose pull && docker compose up -d`
3. If `.venv` has Airflow installed, recreate it and use `apache-airflow==X.Y.Z` with `constraints-X.Y.Z`.

Major upgrades may need `docker compose down -v`.
