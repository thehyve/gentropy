"""Cloud Batch pipeline to preprocess and partition the eQTL Catalogue data."""

from __future__ import annotations

import json
import sys

import pandas as pd
from spark_prep import SparkPrep

EQTL_CATALOGUE_IMPORTED_PATH = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"
EQTL_CATALOGUE_OUPUT_BASE = (
    "gs://genetics_etl_python_playground/1-smart-mirror/summary_stats"
)


def generate_job_config(number_of_tasks: int, max_parallelism: int = 50) -> str:
    """Generate configuration for a Google Batch job.

    Args:
        number_of_tasks (int): How many tasks are there going to be in the batch.
        max_parallelism (int): The maximum number of concurrently running tasks.

    Returns:
        str: Google Batch job config in JSON format.
    """
    config = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "script": {
                                "text": "bash /mnt/share/eqtl_catalogue.sh",
                            }
                        }
                    ],
                    "computeResource": {"cpuMilli": 16000, "memoryMib": 32000},
                    "volumes": [
                        {
                            "gcs": {
                                "remotePath": "genetics_etl_python_playground/batch/eqtl_catalogue"
                            },
                            "mountPath": "/mnt/share",
                        }
                    ],
                    "maxRetryCount": 1,
                    "maxRunDuration": "3600s",
                },
                "taskCount": number_of_tasks,
                "parallelism": min(number_of_tasks, max_parallelism),
            }
        ],
        "allocationPolicy": {
            "instances": [
                {
                    "policy": {
                        "machineType": "n2d-standard-16",
                        "provisioningModel": "SPOT",
                    }
                }
            ]
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
    }
    return json.dumps(config, indent=4)


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
            output_base_path=EQTL_CATALOGUE_OUPUT_BASE,
        )
        worker.process()
    else:
        # We are running locally. Let's generate the job config.
        # For this, we only really need the number of jobs to run.
        number_of_tasks = len(pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH))
        print(generate_job_config(number_of_tasks))
