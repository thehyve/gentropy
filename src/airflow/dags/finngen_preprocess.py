"""Airflow DAG for the Preprocess part of the pipeline."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-finngen"
AUTOSCALING = "finngen-preprocess"

RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"
SUMSTATS = "{RELEASEBUCKET}/summary_statistics/finngen"
WINDOWBASED_CLUMPED = (
    "{RELEASEBUCKET}/study_locus/from_sumstats_study_locus_window_clumped/finngen"
)
LD_CLUMPED = "{RELEASEBUCKET}/study_locus/from_sumstats_study_locus_ld_clumped/finngen"
PICSED = "{RELEASEBUCKET}/credible_set/from_sumstats_study_locus/finngen"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    study_and_sumstats = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="finngen",
        task_id="finngen_sumstats_and_study_index",
    )

    window_based_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="finngen_window_based_clumping",
        other_args=[
            "step.input_path={SUMSTATS}",
            "step.clumped_study_locus_path={WINDOWBASED_CLUMPED}",
        ],
    )
    ld_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="finngen_ld_clumping",
        other_args=[
            "step.input_path={WINDOWBASED_CLUMPED}",
            "step.clumped_study_locus_path={LD_CLUMPED}",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pics = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="pics",
        task_id="finngen_pics",
        other_args=[
            f"step.study_locus_ld_annotated_in={LD_CLUMPED}",
            f"step.picsed_study_locus_out={PICSED}",
        ],
        # This allows to attempt running the task when above step fails do to failifexists
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        common.create_cluster(
            CLUSTER_NAME, autoscaling_policy=AUTOSCALING, master_disk_size=2000
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> study_and_sumstats
        >> window_based_clumping
        >> ld_clumping
        >> pics
        >> common.delete_cluster(CLUSTER_NAME)
    )