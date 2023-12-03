"""Utilities for Spark preprocessing."""

from __future__ import annotations

import json
from multiprocessing import Queue
from multiprocessing.pool import Pool
from queue import Empty
from typing import Any, Callable, List


def process_in_pool(
    q_in: Queue[Any],
    q_out: Queue[Any] | None,
    function: Callable[..., Any],
    args: List[Any] | None = None,
    number_of_workers: int = 4,
) -> None:
    """Processes a Queue in a Pool using multiple workers, making sure that the order of inputs and outputs is respected.

    Args:
        q_in (Queue[Any]): input queue.
        q_out (Queue[Any] | None): output queue.
        function (Callable[..., Any]): function which is used to process input queue objects into output queue objects, one at a time.
        args (List[Any] | None): list of additional function arguments.
        number_of_workers (int): number of workers in the pool.
    """
    if args is None:
        args = []
    pool = Pool(number_of_workers)
    pool_blocks: list[Any] = []
    upstream_queue_finished = False
    while True:
        # If the upstream queue hasn't finished, let's check if it has any new data blocks.
        if not upstream_queue_finished:
            try:
                input_data = q_in.get(block=False)
                # If we are here then there's something in the queue, because otherwise it'd throw an exception.
                if input_data is not None:
                    # Submit the new input data block for processing.
                    full_args = tuple([input_data] + args)
                    new_process = pool.apply_async(function, full_args)
                    pool_blocks.append(new_process)
                else:
                    # The upstream queue has completed.
                    upstream_queue_finished = True
            except Empty:
                # There's nothing in the queue so far (we need to wait more).
                pass
        # If we have submitted some blocks which are currently pending, let's see if the *next one* has completed.
        if pool_blocks:
            if pool_blocks[0].ready():
                # It has completed, let's pass this to the next step and remove from the list of pending jobs.
                if q_out:
                    q_out.put(pool_blocks[0].get())
                pool_blocks = pool_blocks[1:]
        # If the upstream queue has ended *and* there are no pending jobs, let's close the stream.
        if upstream_queue_finished and not pool_blocks:
            if q_out:
                q_out.put(None)
            break


def generate_job_config(
    step_name: str,
    number_of_tasks: int,
    cpu_per_job: int = 8,
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
