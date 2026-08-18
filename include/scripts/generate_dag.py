"""Generate process_file_*.py DAGs from include/data/*.json."""

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_FILE = PROJECT_ROOT / "include" / "templates" / "process_file.py"
DATA_DIR = PROJECT_ROOT / "include" / "data"
DAGS_DIR = PROJECT_ROOT / "dags"


def generate() -> None:
    template = TEMPLATE_FILE.read_text()
    for path in sorted(DATA_DIR.glob("*.json")):
        config = json.loads(path.read_text())
        content = (
            template.replace("DAG_ID_HOLDER", config["dag_id"])
            .replace("SCHEDULE_INTERVAL_HOLDER", config["schedule_interval"])
            .replace("INPUT_HOLDER", config["input"])
        )
        (DAGS_DIR / f"process_{config['dag_id']}.py").write_text(content)


if __name__ == "__main__":
    generate()
