from pathlib import Path


def project_root() -> Path:
    # dags/common/paths.py -> repo root (also /opt/airflow in Docker)
    return Path(__file__).resolve().parents[2]


def data_dir() -> Path:
    return project_root() / "data"


def include_dir() -> Path:
    return project_root() / "include"
