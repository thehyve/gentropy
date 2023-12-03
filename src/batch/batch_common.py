"""Shared utilities for running non-Spark ingestion on Google Batch."""

from __future__ import annotations

import json


def generate_job_config(
    step_name: str,
    number_of_tasks: int,
    cpu_per_job: int = 16,
    max_parallelism: int = 50,
) -> str:
    """Generate configuration for a Google Batch job.

    Args:
        step_name (str): Name of the module which will run the step, without the ".py" extension.
        number_of_tasks (int): How many tasks are there going to be in the batch.
        cpu_per_job (int): How many CPUs to allocate for each VM worker for each job. Must be a power of 2, maximum value is 64.
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
                                "text": f"bash /mnt/share/code/runner.sh {step_name}",
                            }
                        }
                    ],
                    "computeResource": {
                        "cpuMilli": cpu_per_job * 1000,
                        "memoryMib": cpu_per_job * 3200,
                    },
                    "volumes": [
                        {
                            "gcs": {
                                "remotePath": "genetics_etl_python_playground/batch"
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
                        "machineType": f"n2d-standard-{cpu_per_job}",
                        "provisioningModel": "SPOT",
                    }
                }
            ]
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
    }
    return json.dumps(config, indent=4)
