# Airflow lab

Learning lab for **Airflow 3** DAG patterns. Not a production template (standalone container, open UI, lab secrets).

Airflow runs in **Docker Compose** (image `airflow-lab:3.3.1`, built from `apache/airflow:3.3.1` plus `requirements-docker.txt`).
A local **`.venv` is optional** (Cursor: debug, Jupyter, Ruff, pytest). It is gitignored.

| | |
|---|---|
| **Need** | Docker Compose. Linux/WSL so `logs/` and `config/` are writable. |
| **Optional** | Python 3.10+ for the IDE venv. |
| **UI** | http://localhost:8080 (no login) |
| **Per-DAG notes** | [EXAMPLES.md](EXAMPLES.md) |

## Quick reference

| Do this | Command / action |
|---|---|
| First start | `echo "AIRFLOW_UID=$(id -u)" > .env` then `docker compose up -d --build` |
| Stop | `docker compose down` |
| Wipe metadata DB | `docker compose down -v` |
| Logs | `docker compose logs -f airflow-standalone` |
| List DAGs | `docker compose exec airflow-standalone airflow dags list` |
| Trigger a DAG | UI → unpause → **Trigger** (`schedule=None` DAGs only run this way) |
| New DAG | `from airflow.sdk import dag, task` · file stem = function name = DAG id |
| Lint / tests | `.venv/bin/ruff check dags tests` · `.venv/bin/pytest` |
| Breakpoint in `@task` | Open `dags/debug_ide.py` → F5 (or `python dags/debug_ide.py`) |
| Breakpoint in helpers | Breakpoint in `dags/common/` or `tests/` → F5 **Debug current file** |
| Regen `process_file_*` | `python3 include/scripts/generate_dag.py` (do not edit generated files) |
| Rebuild image | After `requirements-docker.txt`: `docker compose up -d --build` |

Conventions: `from common.defaults import DEFAULT_ARGS, START_DATE`. Shared logic in `dags/common/` (pytest + F5). Plugins in `plugins/`.

---

## Contents

1. [Start, stop, logs](#start-stop-logs)
2. [Run a DAG in the UI](#run-a-dag-in-the-ui)
3. [Write a DAG](#write-a-dag)
4. [Debug (IDE breakpoints)](#debug-ide-breakpoints)
5. [Lint, pytest, Jupyter](#lint-pytest-jupyter)
6. [Install Airflow in `.venv`](#install-airflow-in-venv)
7. [Connections](#connections)
8. [Generated DAGs](#generated-dags)
9. [DAG catalog](#dag-catalog)
10. [Layout](#layout)
11. [Change Airflow version](#change-airflow-version)
12. [Troubleshooting](#troubleshooting)

---

## Start, stop, logs

Once on Linux/WSL (so `logs/` and `config/` are writable in the IDE):

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d --build
```

First start takes about a minute. UI: http://localhost:8080 (no login). `.env` is gitignored.

```bash
docker compose logs -f airflow-standalone
docker compose exec airflow-standalone airflow dags list
docker compose down          # stop
docker compose down -v       # stop and wipe the Postgres metadata DB
```

The **Docker** extension in Cursor can do the same (Compose Up / Down, logs, exec).

Task logs: `logs/` (gitignored). Generated `airflow.cfg`: `config/` (gitignored). If Explorer hides them, disable **Explorer: Exclude Git Ignore**.

Compose mounts `dags/`, `plugins/`, `include/`, `data/`, `logs/`, and `config/` into `/opt/airflow/…`. Edit a DAG on the host; the container sees it after the next parse.

## Run a DAG in the UI

1. Open http://localhost:8080 and wait until DAGs appear (no import errors in the UI).
2. Prefer a DAG with `schedule=None` (for example `params_native`) so it only runs when you trigger it.
3. Unpause if it is paused, then **Trigger**.
4. Open the run → task → logs (or the files under `logs/`).

`task_chain` and `process_file_*` are `@daily`; the scheduler can also start them.

To pass trigger params (see `params_native`), use **Trigger DAG** → config JSON in the UI.

## Write a DAG

- `from airflow.sdk import dag, task` (Airflow 3 Task SDK; do not use Airflow 2 `airflow.decorators` in new files)
- File stem = function name = DAG id (no `_dag` suffix)
- Shared defaults: `from common.defaults import DEFAULT_ARGS, START_DATE`
- Paths that work in Docker and in the IDE: `from common.paths import data_dir, include_dir`
- Put business logic in `dags/common/` so you can pytest and F5-debug it without the scheduler
- Custom operators / hooks / triggers: `plugins/`
- `dags/.airflowignore` skips `common/` so helpers are not parsed as DAGs

Most examples use `schedule=None` (trigger in the UI). `task_chain` and `process_file_*` are `@daily`.

At parse time, calling a `@task` returns an `XComArg`, not the Python return type. Annotate task **bodies** with runtime types; leave downstream parameters untyped if the type checker complains (`XComArg` vs `int`). See `dags/task_group.py` / `dags/debug_ide.py`.

Longer notes (CSVs, generator, what each DAG does): [EXAMPLES.md](EXAMPLES.md).

## Debug (IDE breakpoints)

Airflow in Docker does **not** hit IDE breakpoints. Task bodies run in the container. Use `.venv` for debug.

### 1. Helpers and tests (usual path)

Put logic in `dags/common/` (see `customer_report.py`). Set a breakpoint there or in `tests/`, then F5 **Debug current file** / **Pytest current file**.

Importing a DAG file only **parses** it. A breakpoint inside `@task` will not hit with that launch config.

### 2. Breakpoints inside `@task` (`dag.test()`)

`dags/debug_ide.py` is a tiny DAG whose `__main__` calls `common.dag_test.run(dag)`. That runs tasks **in the same Python process** (SQLite under `.airflow-local/`, gitignored). No Docker, no `airflow standalone`.

```bash
# .venv with Airflow installed (see below)
.venv/bin/python dags/debug_ide.py
# prints 15 then 30
```

In Cursor: open `dags/debug_ide.py`, breakpoint on `result = n * 2`, F5 (**Debug current file** is enough; `run()` sets `AIRFLOW_HOME`).

To do the same on another DAG, keep the file a normal DAG and add:

```python
dag = my_dag()

if __name__ == "__main__":
    from common.dag_test import run

    run(dag)
```

Do **not** run `airflow standalone` in `.venv` for this: LocalExecutor still executes tasks out of process, so breakpoints in `@task` will miss.

### Launch configs (`.vscode/launch.json`)

| Config | Use |
|---|---|
| **Debug current file** | Current file in `.venv` (helpers, `debug_ide.py`, scripts) |
| **DAG test (current file)** | Same, plus Airflow env pointing at `.airflow-local` |
| **Pytest current file** | `pytest` on the current test file |

## Lint, pytest, Jupyter

**Create** `.venv`:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-local.txt
```

**Recreate** (safe anytime: broken venv, missing `notebook`/`ipykernel`, Python upgrade):

```bash
rm -rf .venv
python3 -m venv .venv
.venv/bin/pip install -r requirements-local.txt
```

In Cursor, after create or recreate:

1. Install recommended extensions if prompted (Python, Ruff, Jupyter, Docker).
2. **Python: Select Interpreter** (`Ctrl+Shift+P` / `Cmd+Shift+P`) → `.venv/bin/python`.
3. **Jupyter:** `notebooks/lab.ipynb` → kernel **Python (.venv)**. If Cursor asks for the `notebook` package, click **Install** or run `pip install -r requirements-local.txt` again.

```bash
.venv/bin/ruff check dags tests
.venv/bin/pytest
```

`tests/` covers `dags/common/` only, not full DAG runs in Docker.

## Install Airflow in `.venv`

Needed so `from airflow.sdk` and provider operators resolve in the IDE, and so `debug_ide.py` can run. Do **not** add `apache-airflow` to `requirements-local.txt` (it needs the constraint file).

`apache-airflow==3.3.1` already pulls **standard**, **smtp**, and **common.sql**. This lab also needs **`[http]`** (`http_get`) and **`[salesforce]`** (`salesforce_query`):

```bash
PY=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
.venv/bin/pip install "apache-airflow[http,salesforce]==3.3.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-${PY}.txt"
```

Add extras later only when a DAG imports that provider **and** you want the IDE to resolve it (same constraint line, comma-separated extras). If you recreated `.venv`, run this install again.

Salesforce is not in the stock `apache/airflow` image. The lab `Dockerfile` installs it from `requirements-docker.txt`. After changing that file: `docker compose up -d --build`.

## Connections

Compose already defines these (recreate the stack after changing them):

| Env / conn id | Type | Host / extra |
|---|---|---|
| `AIRFLOW_CONN_FS_DEFAULT` | fs | `/opt/airflow/data` |
| `AIRFLOW_CONN_HTTP_DEFAULT` | http | `https://jsonplaceholder.typicode.com` |
| `AIRFLOW_CONN_API` | http | same (used by `http_get`) |

**Salesforce** (`salesforce_query`): create connection **`salesforce_default`** in the UI (type Salesforce): login, password, Extra `{"security_token": "...", "domain": "login"}` (use `"test"` for a sandbox). Do not commit credentials. Trigger only after it exists.

## Generated DAGs

`dags/process_file_a.py` (b, c) are **generated**. Do not edit them by hand.

```
include/data/file_*.json  +  include/templates/process_file.py
        └── python3 include/scripts/generate_dag.py  →  dags/process_file_*.py
```

Details: [EXAMPLES.md](EXAMPLES.md).

## DAG catalog

| DAG | What it shows |
|---|---|
| `debug_ide` | Local `dag.test()` so IDE breakpoints hit `@task` bodies |
| `task_chain` | DAG options, `chain()`, fan-out (`@daily`) |
| `task_group` | `@task_group` nests tasks; `chain()` around the group |
| `xcom_taskflow` / `xcom_manual` / `xcom_hybrid` | Pass data: return values, push/pull, mixed |
| `branch_taskflow` / `branch_manual` | `@task.branch` (arg vs `xcom_pull`) + join |
| `map_files` | `.map()` vs `.expand()` / `.partial()` |
| `zip_files` | `XComArg.zip()` — merge lists into tuples |
| `sensor_files` / `sensor_http` | `@task.sensor` (reschedule vs poke + `PokeReturnValue`) |
| `http_get` | `HttpOperator` GET + XCom to a TaskFlow task |
| `salesforce_query` | `SalesforceHook` SOQL (connection `salesforce_default`) |
| `bash_tmp_file` | `@task.bash` |
| `template_script` | Jinja script under `include/` |
| `params_native` | DAG params as native Python |
| `customer_report_taskflow` / `_manual` / `_hybrid` | Same pipeline, three wiring styles |
| `process_file_*` | Generated from `include/data/*.json` (`@daily`) |

## Layout

```
dags/                 DAGs + common/ helpers
dags/common/          defaults, paths, customer_report, dag_test (local dag.test())
dags/debug_ide.py     smallest DAG for IDE breakpoints
logs/                 task logs (gitignored)
config/               generated airflow.cfg (gitignored)
.airflow-local/       SQLite home for dag.test() (gitignored)
plugins/
include/              templates, JSON for generated DAGs
data/                 sample files
tests/                pytest for dags/common/ (not full DAG runs)
notebooks/lab.ipynb
EXAMPLES.md           what each DAG shows (files, generator)
requirements-local.txt    IDE .venv: pendulum, Ruff, pytest, Jupyter
requirements-docker.txt   extra providers in the image (Salesforce)
Dockerfile                official image + requirements-docker.txt
docker-compose.yaml       build args = Airflow version
```

## Change Airflow version

1. Set `AIRFLOW_VERSION` in `docker-compose.yaml` (build args) and the `FROM` default in `Dockerfile`.
2. Match provider pins in `requirements-docker.txt` to that version’s constraint file.
3. `docker compose build --no-cache && docker compose up -d`
4. If `.venv` has Airflow installed, recreate it and use `apache-airflow[http,salesforce]==X.Y.Z` with `constraints-X.Y.Z`.

Major upgrades may need `docker compose down -v`.

## Troubleshooting

| Symptom | What to do |
|---|---|
| `logs/` or `config/` not writable | Run on Linux/WSL; set `AIRFLOW_UID=$(id -u)` in `.env` |
| DAGs missing / import error | UI import errors; `docker compose logs airflow-standalone`; fix the DAG, wait for parse |
| Breakpoint in `@task` never hits | Do not F5-parse the DAG and do not use Docker. Use `debug_ide.py` or `common.dag_test.run` |
| `no such table: task_instance` | `dag.test()` without `.airflow-local`. Use `run(dag)` (sets home + migrate), not a bare `dag.test()` |
| `from airflow.sdk` unresolved in the IDE | Install Airflow into `.venv` with the constraint command above |
| `http_get` / `sensor_http` fail | Container needs outbound network; `api` host must be jsonplaceholder (already in Compose) |
| Salesforce DAG fails | Create `salesforce_default`; rebuild if the provider is missing from the image |
| Stale metadata after a major bump | `docker compose down -v` then `up -d --build` |
