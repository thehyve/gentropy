"""Cloud Batch pipeline to preprocess and partition the eQTL Catalogue data."""

from __future__ import annotations

import sys

import pandas as pd
from batch_common import generate_job_config
from spark_prep import SparkPrep

EQTL_CATALOGUE_IMPORTED_PATH = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"
EQTL_CATALOGUE_OUTPUT_BASE = (
    "gs://genetics_etl_python_playground/1-smart-mirror/summary_stats"
)


if __name__ == "__main__":
    # Are we running on Google Cloud?
    args = sys.argv[1:]
    if args:
        # We are running inside Google Cloud, let's process one file.
        batch_index = int(args[0])
        # Read the study index and select one study.
        df = pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH)
        record = df.loc[batch_index].to_dict()
        # Process the study.
        worker = SparkPrep(
            input_uri=record["ftp_path"],
            analysis_type="eQTL",
            source_id="eQTL_Catalogue",
            project_id="GTEx_V8",
            study_id=record["qtl_group"],
            output_base_path=EQTL_CATALOGUE_OUTPUT_BASE,
        )
        worker.process()
    else:
        # We are running locally. Let's generate the job config.
        # For this, we only really need the total number of jobs to run.
        number_of_tasks = len(pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH))
        print(
            generate_job_config(
                step_name="eqtl_catalogue", number_of_tasks=number_of_tasks
            )
        )
