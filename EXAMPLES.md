# DAG examples

What each example shows, which files it uses, and how generated DAGs are built.
Trigger `schedule=None` DAGs in the UI. `@daily` DAGs can also be started by the scheduler.

How to run Airflow: [README.md](README.md).

## Shared pieces

| Path | Role |
|---|---|
| `dags/common/defaults.py` | `START_DATE`, `DEFAULT_ARGS` (retries) |
| `dags/common/paths.py` | `data_dir()`, `include_dir()` (repo root in the IDE, `/opt/airflow` in Docker) |
| `dags/common/customer_report.py` | Pure Python for the customer-report DAGs (pytest + F5) |
| `dags/.airflowignore` | Ignores `common/` so helpers are not parsed as DAGs |
| `data/data_1.csv`, `data_2.csv`, `data_3.csv` | Tiny sample files (`id,value`) for `sensor_files` |
| `include/` | Templates, JSON configs, bash script used by DAGs |

## Core patterns

### `task_chain`

- **File:** `dags/task_chain.py` · **schedule:** `@daily`
- DAG options (`max_consecutive_failed_dag_runs`, retries via defaults) and `chain()`.
- Shape: `A` then fan-out `[B, D]` then `[C, E]`. Tasks only print labels.

### `task_group`

- **File:** `dags/task_group.py` · **schedule:** none
- `@task_group` from `airflow.sdk`. Inner tasks nest under `process` in the Graph view (`process.add_five` → `process.times_two`).
- Same TaskFlow wiring as other examples: `start` returns `10`, the group adds 5 then doubles, `finish` prints `30`.

### XCom (same idea, three wirings)

All three: `schedule=None`. No extra data files.

| DAG | File | What to look at |
|---|---|---|
| `xcom_taskflow` | `dags/xcom_taskflow.py` | Return a value; `multiple_outputs=True` also XComs each dict key (`doubled`, `squared`) plus `return_value` |
| `xcom_manual` | `dags/xcom_manual.py` | `ti.xcom_push` / `xcom_pull` (named keys, dict, two upstreams) |
| `xcom_hybrid` | `dags/xcom_hybrid.py` | TaskFlow `return`, then `xcom_pull(..., key="return_value")` |

### Branching

| DAG | File | Schedule | What to look at |
|---|---|---|---|
| `branch_taskflow` | `dags/branch_taskflow.py` | none | `@task.branch` gets the upstream return (`run_success` vs `run_failure`) |
| `branch_manual` | `dags/branch_manual.py` | none | Branch reads XCom (weekday vs weekend via pendulum); `finalize` uses `none_failed_min_one_success` so the skipped branch does not block the join |

### `map_files`

- **File:** `dags/map_files.py` · **schedule:** none
- Two independent pipelines in one DAG (`.map()` is not the same as dynamic task mapping):
  - `list_paths().map(append_data)` transforms each path in XCom. Still two tasks (`list_paths` → `print_paths`), not one per item.
  - `get_files` returns `file_3` … `file_5` (not the CSVs in `data/`). `download_file.partial(folder=...).expand(file=...)` creates one mapped task instance per name; folder is fixed. Downstream bash task prints the mapped XCom list.

### Sensors

| DAG | File | What to look at | Files |
|---|---|---|---|
| `sensor_files` | `dags/sensor_files.py` | Mapped `@task.sensor` in **reschedule** mode | Waits for `data/data_1.csv`, `data_2.csv`, `data_3.csv` (already present, so it should succeed quickly) |
| `sensor_http` | `dags/sensor_http.py` | `@task.sensor` in **poke** mode + `PokeReturnValue` | GET `https://www.google.com`; passes the URL downstream. Needs outbound network from the container |

### `http_get`

- **File:** `dags/http_get.py` · **schedule:** none
- `HttpOperator` GET `/posts` on connection `api`.
- Connection host should be `https://jsonplaceholder.typicode.com` (`AIRFLOW_CONN_API` in Compose; already created in the DB). `api.publicapis.org` no longer exists (DNS fails). Recreate the stack after changing Compose env, or edit the connection in the UI.
- `response_filter` XComs the JSON body; `print_post` reads it via `.output`. Needs outbound network from the container.

### `salesforce_query`

- **File:** `dags/salesforce_query.py` · **schedule:** none
- `SalesforceHook.make_query` runs `SELECT Id, Name FROM Account LIMIT 5`.
- Provider is **not** in the stock Airflow image; the lab `Dockerfile` installs `apache-airflow-providers-salesforce` from `requirements-docker.txt`. Rebuild after changing that file.
- Create connection **`salesforce_default`** in the UI (type Salesforce): login, password, Extra `{"security_token": "...", "domain": "login"}` (use `"test"` for a sandbox). Do not commit real credentials. Trigger only after the connection exists.

### Bash and templates

| DAG | File | What to look at | Files |
|---|---|---|---|
| `bash_tmp_file` | `dags/bash_tmp_file.py` | `@task.bash` then a Python task | Writes/tests `/tmp/dummy` **inside the container** |
| `template_script` | `dags/template_script.py` | `template_searchpath` + Jinja | Runs `include/scripts/script.sh` (`Today is {{ data_interval_start.format('dddd') }}`) |

### `params_native`

- **File:** `dags/params_native.py` · **schedule:** none
- Trigger param `numbers` (default `[1, 2, 3]`).
- `render_template_as_native_obj=True` so `"{{ params.numbers }}"` is a Python list, not a string. Task returns `sum(numbers)`.

## Customer report (same pipeline, three wirings)

Logic lives in `dags/common/customer_report.py` (in-memory sample customers/orders, not CSVs). Tests: `tests/test_customer_report.py`. Notebook: `notebooks/lab.ipynb`.

Flow: extract customers ∥ extract orders → clean each → build report → print.

| DAG | File | Wiring |
|---|---|---|
| `customer_report_taskflow` | `dags/customer_report_taskflow.py` | Return values as function arguments |
| `customer_report_manual` | `dags/customer_report_manual.py` | Named `xcom_push` / `xcom_pull` |
| `customer_report_hybrid` | `dags/customer_report_hybrid.py` | Return values, then pull by `task_id` |

## Generated DAGs (`process_file_*`)

These files are **generated**. Do not edit `dags/process_file_a.py` (b, c) by hand.

```
include/data/file_a.json   ──►  generate_dag.py  ──►  dags/process_file_a.py
include/data/file_b.json        + template            dags/process_file_b.py
include/data/file_c.json   include/templates/         dags/process_file_c.py
                           process_file.py
```

Each JSON has `dag_id`, `schedule_interval` (`@daily`), and `input` (a filename string). The template replaces `DAG_ID_HOLDER`, `SCHEDULE_INTERVAL_HOLDER`, and `INPUT_HOLDER`.

| Config | Generated DAG | Prints (does not read a real file) |
|---|---|---|
| `include/data/file_a.json` | `process_file_a` | `file_a.csv` |
| `include/data/file_b.json` | `process_file_b` | `file_b.csv` |
| `include/data/file_c.json` | `process_file_c` | `file_c.csv` |

Those `file_*.csv` names are placeholders. The on-disk samples used by sensors are `data/data_*.csv`.

After changing JSON or the template:

```bash
python3 include/scripts/generate_dag.py
```
