"""Airflow DAG for the ETL part of the pipeline."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG

CLUSTER_NAME = "otg-etl"
SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "dag.yaml"


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # Parse and define all steps and their prerequisites.
    tasks = {}
    steps = common.read_yaml_config(SOURCE_CONFIG_FILE_PATH)
    for step in steps:
        # Define task for the current step.
        step_id = step["id"]
        this_task = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id=step_id,
            task_id=step_id,
        )
        # Chain prerequisites.
        tasks[step_id] = this_task
        for prerequisite in step.get("prerequisites", []):
            this_task.set_upstream(tasks[prerequisite])
    # Construct the DAG with all tasks.
    dag = common.generate_dag(cluster_name=CLUSTER_NAME, tasks=list(tasks.values()))