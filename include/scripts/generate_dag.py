import json
import os
from pathlib import Path
import shutil
import fileinput

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_FILE = PROJECT_ROOT / "include" / "templates" / "process_file.py"
DATA_DIR = PROJECT_ROOT / "include" / "data"
DAGS_DIR = PROJECT_ROOT / "dags"

for filename in os.listdir(DATA_DIR):
    if not filename.endswith(".json"):
        continue

    config = json.load(open(DATA_DIR / filename))
    new_dagfile = DAGS_DIR / f"process_{config['dag_id']}.py"
    shutil.copyfile(TEMPLATE_FILE, new_dagfile)

    for line in fileinput.input(new_dagfile, inplace=True):
        line = line.replace("DAG_ID_HOLDER", config["dag_id"])
        line = line.replace("SCHEDULE_INTERVAL_HOLDER", config["schedule_interval"])
        line = line.replace("INPUT_HOLDER", config["input"])
        print(line, end="")
