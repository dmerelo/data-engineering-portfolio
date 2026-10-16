import importlib.util
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "incremental_customers_dag.py"
EXPECTED_ORDER = [
    "read_watermark",
    "run_incremental_batch",
    "quality_gate",
    "publish_target",
    "commit_watermark",
]


def load_dag():
    pytest.importorskip("airflow", reason="Airflow not installed")
    from airflow.models.dag import DAG

    spec = importlib.util.spec_from_file_location("incremental_customers_dag", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dags = [obj for obj in vars(module).values() if isinstance(obj, DAG)]
    if not dags:
        dags = [module.incremental_customers()]
    return dags[0]


def test_dag_has_expected_settings_and_task_order():
    dag = load_dag()

    assert dag.dag_id == "incremental_customers"
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert [task.task_id for task in dag.topological_sort()] == EXPECTED_ORDER

    for upstream, downstream in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids
