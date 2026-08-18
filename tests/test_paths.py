from common.paths import data_dir, include_dir, project_root


def test_paths_are_under_the_repo():
    root = project_root()
    assert (root / "dags").is_dir()
    assert data_dir() == root / "data"
    assert include_dir() == root / "include"
