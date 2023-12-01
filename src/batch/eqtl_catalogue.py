#!/usr/bin/env python
"""Cloud Batch pipeline to preprocess and partition the eQTL Catalogue."""

from __future__ import annotations

import ftplib
import gzip
import io
import json
import math
import os.path
import random
import sys
import time
import typing
import urllib.parse
import urllib.request
from multiprocessing import Process, Queue
from multiprocessing.pool import Pool
from queue import Empty
from typing import Any, Callable, Dict, List

import ftputil
import pandas as pd
from typing_extensions import Never

EQTL_CATALOGUE_IMPORTED_PATH = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"
EQTL_CATALOGUE_OUPUT_BASE = (
    "gs://genetics_etl_python_playground/1-smart-mirror/summary_stats"
)


class resilient_urlopen:
    """A resilient wrapper around urllib.request.urlopen."""

    delay_initial = 1.0
    delay_increase_factor = 1.5
    delay_max = 120.0
    delay_jitter = 3.0
    delay_give_up = 3600.0

    fetch_block_size = 200 * 1024 * 1024

    def __init__(self, uri: str):
        """Initialise the class.

        Args:
            uri (str): The URI to read the data from.

        Raises:
            NotImplementedError: If the protocol is not HTTP(s) or FTP.
        """
        self.uri = uri
        self.buffer = b""
        self.buffer_position = 0
        self.url_position = 0
        while True:
            try:
                if uri.startswith("http"):
                    self.content_length = int(
                        urllib.request.urlopen(uri).getheader("Content-Length")
                    )
                elif uri.startswith("ftp"):
                    parsed_uri = urllib.parse.urlparse(uri)
                    self.ftp_server = parsed_uri.netloc
                    self.ftp_path, self.ftp_filename = os.path.split(
                        parsed_uri.path[1:]
                    )
                    with ftplib.FTP(self.ftp_server) as ftp:
                        ftp.login()
                        ftp.cwd(self.ftp_path)
                        length = ftp.size(self.ftp_filename)
                        assert (
                            length is not None
                        ), f"FTP server returned no length for {uri}."
                        self.content_length = length
                else:
                    raise NotImplementedError(f"Unsupported URI schema: {uri}.")
                break
            except Exception:
                time.sleep(5 + random.random())
        assert self.content_length > 0

    def __enter__(self) -> resilient_urlopen:
        """Stream reading entry point.

        Returns:
            resilient_urlopen: An instance of the class
        """
        return self

    def __exit__(self, *args: Any) -> None:
        """Stream reading exit point (empty).

        Args:
            *args (Any): ignored.
        """
        pass

    def read(self, size: int) -> bytes:
        """Stream reading method.

        Args:
            size(int): How many bytes to read.

        Returns:
            bytes: A block of data from the requested position and length.

        Raises:
            Exception: If a block could not be read from the URI exceeding the maximum delay time.
            NotImplementedError: If the protocol is not HTTP(s) or FTP.
        """
        # Trim spent part of the buffer, if necessary
        if self.buffer_position > self.fetch_block_size:
            self.buffer_position -= self.fetch_block_size
            self.buffer = self.buffer[self.fetch_block_size :]

        # If the buffer isn't enough to serve next block, we need to extend it first.
        while (size > len(self.buffer) - self.buffer_position) and (
            self.url_position != self.content_length
        ):
            delay = self.delay_initial
            total_delay = 0.0
            while True:
                try:
                    if self.uri.startswith("http"):
                        byte_range = f"bytes={self.url_position}-{self.url_position + self.fetch_block_size - 1}"
                        request = urllib.request.Request(
                            self.uri, headers={"Range": byte_range}
                        )
                        block = urllib.request.urlopen(request).read()
                    elif self.uri.startswith("ftp"):
                        with ftputil.FTPHost(
                            self.ftp_server, "anonymous", "anonymous"
                        ) as ftp_host:
                            with ftp_host.open(
                                f"{self.ftp_path}/{self.ftp_filename}",
                                mode="rb",
                                rest=self.url_position,
                            ) as stream:
                                block = stream.read(self.fetch_block_size)
                    else:
                        raise NotImplementedError(
                            f"Unsupported URI schema: {self.uri}."
                        )
                    self.buffer += block
                    self.url_position += len(block)
                    break
                except Exception as e:
                    total_delay += delay
                    if total_delay > self.delay_give_up:
                        raise Exception(
                            f"Could not fetch URI {self.uri} at position {self.url_position}, length {size} after {total_delay} seconds"
                        ) from e
                    time.sleep(delay)
                    delay = (
                        min(delay * self.delay_increase_factor, self.delay_max)
                        + self.delay_jitter * random.random()
                    )

        # Return next block from the buffer.
        data = self.buffer[self.buffer_position : self.buffer_position + size]
        self.buffer_position += size
        return data


def process_in_pool(
    q_in: Queue[Any],
    q_out: Queue[Any],
    function: Callable[[Any], Any],
    number_of_workers: int = 4,
) -> None:
    """Processes a Queue in a Pool using multiple workers, making sure that the order of inputs and outputs is respected.

    Args:
        q_in (Queue[Any]): input queue.
        q_out (Queue[Any]): output queue.
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
        # If we have submitted some blocks which currently pending, let's see if the *next one* has completed.
        if pool_blocks:
            if pool_blocks[0].ready():
                # It has completed, let's pass this to the next step and remove from the list of pending jobs.
                q_out.put(pool_blocks[0].get())
                pool_blocks = pool_blocks[1:]
        # If the upstream queue has ended *and* there are no pending jobs, let's close the stream.
        if upstream_queue_finished and not pool_blocks:
            q_out.put(None)
            break


class ParseData:
    """Parse data."""

    # How many bytes of raw (uncompressed) data to fetch and parse at once.
    fetch_chunk_size = 100_000_000

    # How many records, on average, to try and keep in each Parquet partition.
    emit_block_size = 500_000
    # How much of a look-ahead buffer to keep (times the `emit_block_size`)
    # Increasing this value increases memory footprint but decreases spread in the final parquet partitions.
    # 4.0 value means that an average error for the final partitions is ~5% and the maximum possible error is ~10%.
    emit_look_ahead_factor = 4.0
    # Denotes when the look-ahead buffer is long enough to emit a block from it.
    emit_ready_buffer = emit_block_size * (emit_look_ahead_factor + 1)

    # List of field names of the data. Populated by the first step (_p1_fetch_data_from_uri).
    field_names = None

    def _p1_fetch_data_from_uri(self, q_out: Queue[str | None], uri: str) -> None:
        """Fetch data from the URI in blocks.

        Args:
            q_out (Queue[str | None]): Output queue with uncompressed text blocks.
            uri (str): URI to fetch the data from.
        """

        def cast_to_bytes(x: Any) -> typing.IO[bytes]:
            """Casts a given object to bytes. For rationale, see: https://stackoverflow.com/a/58407810.

            Args:
                x (Any): object to cast to bytes.

            Returns:
                typing.IO[bytes]: object cast to bytes.
            """
            return typing.cast(typing.IO[bytes], x)

        with cast_to_bytes(resilient_urlopen(uri)) as gzip_stream:
            with cast_to_bytes(gzip.GzipFile(fileobj=gzip_stream)) as bytes_stream:
                with io.TextIOWrapper(bytes_stream) as text_stream:
                    # Process field names.
                    self.field_names = text_stream.readline().split("\t")
                    # Process data.
                    while True:
                        # Read more data from the URI source.
                        text_block = text_stream.read(self.fetch_chunk_size)
                        if text_block:
                            # Emit block for downstream processing.
                            q_out.put(text_block)
                        else:
                            # We have reached end of stream.
                            q_out.put(None)
                            break

    def _p2_emit_complete_line_blocks(
        self,
        q_in: Queue[str | None],
        q_out: Queue[str | None],
    ) -> None:
        """Given text blocks, emit blocks which contain complete text lines.

        Args:
            q_in (Queue[str | None]): Input queue with data blocks.
            q_out (Queue[str | None]): Output queue with data blocks which are guaranteed to contain only complete lines.
        """
        # Initialise buffer for storing incomplete lines.
        buffer = ""
        # Process data.
        while True:
            # Get more data from the input queue.
            text_block = q_in.get()
            if text_block is not None:
                # Process text block.
                buffer += text_block
                # Find the rightmost newline so that we always emit blocks of complete records.
                rightmost_newline_split = buffer.rfind("\n") + 1
                q_out.put(buffer[:rightmost_newline_split])
                buffer = buffer[rightmost_newline_split:]
            else:
                # We have reached end of stream. Because buffer only contains *incomplete* lines, it should be empty.
                assert (
                    not buffer
                ), "Expected buffer to be empty at the end of stream, but incomplete lines are found."
                q_out.put(None)
                break

    def _p3_parse_data(
        self,
        q_in: Queue[str | None],
        q_out: Queue[pd.DataFrame | None],
    ) -> None:
        """Parse complete-line data blocks into Pandas dataframes, utilising multiple workers.

        Args:
            q_in (Queue[str | None]): Input queue with complete-line text data blocks.
            q_out (Queue[pd.DataFrame | None]): Output queue with Pandas dataframes.
        """

        def parse_data(lines_block: str) -> pd.DataFrame:
            """Parse a data block with complete lines into a Pandas dataframe.

            Args:
                lines_block (str): A text block containing complete lines.

            Returns:
                pd.DataFrame: a Pandas dataframe with parsed data.
            """
            data_io = io.StringIO(lines_block)
            df_block = pd.read_table(
                data_io, names=self.field_names, header=None, dtype=str
            )
            return df_block

        process_in_pool(q_in=q_in, q_out=q_out, function=parse_data)

    def _p4_split_by_chromosome(
        self,
        q_in: Queue[pd.DataFrame | None],
        q_out: Queue[tuple[str, pd.DataFrame | None]],
    ) -> None:
        """Split Pandas dataframes by chromosome.

        Args:
            q_in (Queue[pd.DataFrame | None]): Input queue with Pandas dataframes, possibly containing more than one chromosome.
            q_out (Queue[tuple[str, pd.DataFrame | None]]): Output queue with Pandas dataframes, containing a part of exactly one chromosome.
        """
        while True:
            # Get new data from the input queue.
            df_block = q_in.get()
            if df_block is not None:
                # Split data block by chromosome.
                grouped_data = df_block.groupby("chromosome", sort=False)
                for chromosome_id, chromosome_data in grouped_data:
                    q_out.put((chromosome_id, chromosome_data))
            else:
                # We have reached end of stream.
                q_out.put(("", None))
                break

    def _p5_partition_data_blocks(
        self,
        q_in: Queue[tuple[str, pd.DataFrame | None]],
        q_out: Queue[tuple[str, str, int, pd.DataFrame] | None],
        qtl_group: str,
    ) -> None:
        """Process stream of data blocks and partition them.

        Args:
            q_in (Queue[tuple[str, pd.DataFrame | None]]): Input queue with Pandas dataframes split by chromosome.
            q_out (Queue[tuple[str, str, int, pd.DataFrame] | None]): Output queue with ready to output metadata + data.
            qtl_group (str): QTL group identifier, used as study identifier for output.
        """
        # Initialise counters.
        current_chromosome = ""
        current_block_index = 0
        current_data_block = pd.DataFrame(columns=self.field_names)

        # Process blocks.
        while True:
            # Get more data from the queue.
            df_block_by_chromosome = q_in.get()
            chromosome_id, chromosome_data = df_block_by_chromosome

            # If this is the first block we ever see, initialise "current_chromosome".
            if not current_chromosome:
                current_chromosome = chromosome_id

            # If chromosome has changed (including to None incicating end of stream), we need to emit the previous one.
            if chromosome_id != current_chromosome:
                # Calculate the optimal number of blocks.
                number_of_blocks = max(
                    round(len(current_data_block) / self.emit_block_size), 1
                )
                records_per_block = math.ceil(
                    len(current_data_block) / number_of_blocks
                )
                # Emit remaining blocks one by one.
                for index in range(0, len(current_data_block), records_per_block):
                    q_out.put(
                        (
                            qtl_group,
                            current_chromosome,
                            current_block_index,
                            current_data_block[index : index + records_per_block],
                        )
                    )
                    current_block_index += 1
                # Reset everything for the next chromosome.
                current_chromosome = chromosome_id
                current_block_index = 0
                current_data_block = pd.DataFrame(columns=self.field_names)

            if current_chromosome is not None:
                # We have a new chromosome to process.
                # We should now append new data to the chromosome buffer.
                current_data_block = pd.concat(
                    [current_data_block, chromosome_data],
                    ignore_index=True,
                )
                # If we have enough data for the chromosome we are currently processing, we can emit some blocks already.
                while len(current_data_block) >= self.emit_ready_buffer:
                    emit_block = current_data_block[: self.emit_block_size]
                    q_out.put(
                        (qtl_group, current_chromosome, current_block_index, emit_block)
                    )
                    current_block_index += 1
                    current_data_block = current_data_block[self.emit_block_size :]
            else:
                # We have reached end of stream.
                q_out.put(None)
                break

    def _p6_write_parquet(
        self, q_in: Queue[tuple[str, str, int, pd.DataFrame] | None]
    ) -> None:
        """Write a Parquet file for a given input file.

        Args:
            q_in (Queue[tuple[str, str, int, pd.DataFrame] | None]): Input queue with Pandas metadata + data to output.
        """

        def __write_parquet(
            qtl_group: str, chromosome: str, chunk_number: int, df: pd.DataFrame
        ) -> None:
            """Write a single Parquet file.

            Args:
                qtl_group (str): QTL group ID, used as study ID.
                chromosome (str): Chromosome ID.
                chunk_number (int): Parquet chunk number to add into the output file name.
                df (pd.DataFrame): Pandas dataframe with data for the block.
            """
            output_filename = (
                f"{EQTL_CATALOGUE_OUPUT_BASE}/"
                "analysisType=eQTL/"
                "sourceId=eQTL_Catalogue/"
                "projectId=GTEx_V8/"
                f"studyId={qtl_group}/"  # Example: "Adipose_Subcutaneous".
                f"chromosome={chromosome}/"  # Example: 13.
                f"part-{chunk_number:05}.snappy.parquet"
            )
            df.to_parquet(output_filename, compression="snappy")

        parquet_pool = Pool(8)
        pool_blocks = []
        upstream_queue_finished = False
        t1 = time.time()

        while True:
            # If the upstream queue hasn't finished, let's check it.
            if not upstream_queue_finished:
                try:
                    element = q_in.get(block=False)
                    # If we are here then there's something in the queue.
                    if element is None:
                        # The upstream queue has completed.
                        upstream_queue_finished = True
                    else:
                        # Submit the block for processing.
                        pool_blocks.append(
                            parquet_pool.apply_async(__write_parquet, element)
                        )
                        sys.stderr.write(f"p6 [{int((time.time() - t1)*1000)}]\n")
                        t1 = time.time()
                except Empty:
                    # There's nothing in the queue so far.
                    pass
            # If we have submitted some blocks currently pending, let's see if the next one has completed.
            if pool_blocks:
                if pool_blocks[0].ready():
                    # It has completed, let's remove from the list of pending jobs.
                    pool_blocks = pool_blocks[1:]
            # If the upstream queue has ended *and* there are no pending jobs, let's close the stream.
            if upstream_queue_finished and not pool_blocks:
                break

    def process(
        self,
        record: Dict[str, Any],
    ) -> None:
        """Process one input file start to finish.

        Args:
            record (Dict[str, Any]): A record describing one input file and its attributes.
        """
        # Set up queues for process exchange.
        q: List[Queue[Never]] = [Queue(maxsize=32) for _ in range(5)]
        processes = [
            Process(
                target=self._p1_fetch_data_from_uri, args=(q[0], record["ftp_path"])
            ),
            Process(target=self._p2_emit_complete_line_blocks, args=(q[0], q[1])),
            Process(target=self._p3_parse_data, args=(q[1], q[2])),
            Process(target=self._p4_split_by_chromosome, args=(q[2], q[3])),
            Process(
                target=self._p5_partition_data_blocks,
                args=(q[3], q[4], record["qtl_group"]),
            ),
            Process(target=self._p6_write_parquet, args=(q[4],)),
        ]
        for p in processes:
            p.start()

        # Wait until the final process completes.
        processes[-1].join()


def process(batch_index: int) -> None:
    """Process one input file.

    Args:
        batch_index (int): The index the current job among all batch jobs.
    """
    # Read the study index and select one study.
    df = pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH)
    record = df.loc[batch_index].to_dict()

    # Process the study.
    ParseData().process(record)


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
        process(int(args[0]))
    else:
        # We are running locally. Need to generate job config.
        number_of_tasks = len(pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH))
        print(generate_job_config(number_of_tasks))
