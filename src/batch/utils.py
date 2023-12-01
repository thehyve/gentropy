"""Utilities for Spark preprocessing."""

from __future__ import annotations

from multiprocessing import Queue
from multiprocessing.pool import Pool
from queue import Empty
from typing import Any, Callable


def process_in_pool(
    q_in: Queue[Any],
    q_out: Queue[Any] | None,
    function: Callable[[Any], Any],
    number_of_workers: int = 4,
) -> None:
    """Processes a Queue in a Pool using multiple workers, making sure that the order of inputs and outputs is respected.

    Args:
        q_in (Queue[Any]): input queue.
        q_out (Queue[Any] | None): output queue.
        function (Callable[[Any], Any]): function which is used to process input queue objects into output queue objects, one at a time.
        number_of_workers (int): number of workers in the pool.
    """
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
                    pool_blocks.append(pool.apply_async(function, (input_data,)))
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
