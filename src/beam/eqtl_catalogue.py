#!/usr/bin/env python
"""Cloud Batch pipeline to preprocess and partition the eQTL Catalogue."""

from __future__ import annotations

import json
import os
import sys
from multiprocessing import Process, Queue
from typing import Any, Dict, Iterator

import pandas as pd
import pyarrow

EQTL_CATALOGUE_IMPORTED_PATH = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"
EQTL_CATALOGUE_OUPUT_BASE = (
    "gs://genetics_etl_python_playground/1-smart-mirror/summary_stats"
)
FIELDS = [
    "variant",
    "r2",
    "pvalue",
    "molecular_trait_object_id",
    "molecular_trait_id",
    "maf",
    "gene_id",
    "median_tpm",
    "beta",
    "se",
    "an",
    "ac",
    "chromosome",
    "position",
    "ref",
    "alt",
    "type",
    "rsid",
]
PYARROW_SCHEMA = pyarrow.schema(
    [(field_name, pyarrow.string()) for field_name in FIELDS]
)


class resilient_urlopen:
    """A resilient wrapper around urllib.request.urlopen."""

    delay_initial = 1.0
    delay_increase_factor = 1.5
    delay_max = 120.0
    delay_jitter = 3.0
    delay_give_up = 3600.0

    # Value obtained by trial and error experimentation.
    # Slower values bring too much delay into the computation cycle.
    # Larger values cause slower buffer turnaround & slicing when serving data.
    block_size = 20 * 1024 * 1024

    def __init__(self, uri: str):
        """Initialise the class.

        Args:
            uri (str): The URI to read the data from.

        Raises:
            NotImplementedError: If the protocol is not HTTP(s) or FTP.
        """
        import ftplib
        import os.path
        import random
        import time
        import urllib.parse
        import urllib.request

        self.uri = uri
        self.buffer = b""
        self.position = 0
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

    def __enter__(self) -> "ParseData.resilient_urlopen":
        """Stream reading entry point.

        Returns:
            ParseData.resilient_urlopen: An instance of the class
        """
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        """Stream reading exit point (empty).

        Args:
            *args (Any): ignored.
            **kwargs (Any): ignored.
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
        import random
        import time
        import urllib.request

        import ftputil

        # If the buffer isn't enough to serve next block, we need to extend it first.
        while (size > len(self.buffer)) and (self.position != self.content_length):
            delay = self.delay_initial
            total_delay = 0.0
            while True:
                try:
                    if self.uri.startswith("http"):
                        byte_range = f"bytes={self.position}-{self.position + self.block_size - 1}"
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
                                rest=self.position,
                            ) as stream:
                                block = stream.read(self.block_size)
                    else:
                        raise NotImplementedError(
                            f"Unsupported URI schema: {self.uri}."
                        )
                    self.buffer += block
                    self.position += len(block)
                    break
                except Exception as e:
                    total_delay += delay
                    if total_delay > self.delay_give_up:
                        raise Exception(
                            f"Could not fetch URI {self.uri} at position {self.position}, length {size} after {total_delay} seconds"
                        ) from e
                    time.sleep(delay)
                    delay = (
                        min(delay * self.delay_increase_factor, self.delay_max)
                        + self.delay_jitter * random.random()
                    )

        # Return next block from the buffer.
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return data


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

    def _split_final_data(
        self, qtl_group: str, chromosome: str, block_index: int, df: pd.DataFrame
    ) -> Iterator[tuple[str, str, int, pd.DataFrame]]:
        """Process the final chunk of the data and split into partitions as close to self.emit_block_size as possible.

        Args:
            qtl_group (str): QTL group field used for study ID.
            chromosome (str): Chromosome identifier.
            block_index (int): Starting number of the block to emit.
            df (pd.DataFrame): Remaining chunk data to split.

        Yields:
            tuple[str, str, int, pd.DataFrame]: Tuple of values to generate the final Parquet file.
        """
        import math

        number_of_blocks = max(round(len(df) / self.emit_block_size), 1)
        records_per_block = math.ceil(len(df) / number_of_blocks)
        for index in range(0, len(df), records_per_block):
            yield (
                qtl_group,
                chromosome,
                block_index,
                df[index : index + records_per_block],
            )
            block_index += 1

    def _p1_fetch_data_from_uri(self, q_out: Queue, uri: str) -> None:
        import gzip
        import io
        import typing

        with resilient_urlopen(uri) as compressed_stream:
            # See: https://stackoverflow.com/a/58407810.
            compressed_stream_typed = typing.cast(typing.IO[bytes], compressed_stream)
            with gzip.GzipFile(fileobj=compressed_stream_typed) as uncompressed_stream:
                uncompressed_stream_typed = typing.cast(
                    typing.IO[bytes], uncompressed_stream
                )
                with io.TextIOWrapper(uncompressed_stream_typed) as text_stream:
                    # Skip header.
                    text_stream.readline()
                    while True:
                        # Read more data from the URI source.
                        text_block = text_stream.read(self.fetch_chunk_size)
                        if not text_block:
                            # End of stream.
                            q_out.put(None)
                            break
                        q_out.put(text_block)
                        sys.stderr.write(
                            f"p1 emitted > {len(text_block)} uncompressed bytes\n"
                        )

    def _p2_emit_complete_line_blocks(self, q_in: Queue, q_out: Queue) -> None:
        # Initialise buffer.
        buffer = ""
        while True:
            text_block = q_in.get()
            if text_block is None:
                # End of stream.
                if buffer:
                    q_out.put(buffer)
                q_out.put(None)
                break
            buffer += text_block
            # Find the rightmost newline so that we always emit blocks of complete records.
            rightmost_newline_split = buffer.rfind("\n") + 1
            q_out.put(buffer[:rightmost_newline_split])
            sys.stderr.write(
                f"p2 emitted > {rightmost_newline_split} uncompressed bytes\n"
            )
            buffer = buffer[rightmost_newline_split:]

    def _p3_parse_data(self, q_in: Queue, q_out: Queue) -> None:
        import io

        import pandas as pd

        while True:
            lines_block = q_in.get()
            if lines_block is None:
                # End of stream.
                q_out.put(None)
                break
            # Parse data block.
            data_io = io.StringIO(lines_block)
            df_block = pd.read_table(data_io, names=FIELDS, header=None)
            q_out.put(df_block)
            sys.stderr.write(f"p3 emitted >> {len(df_block)} Pandas records\n")

    def _p4_split_by_chromosome(self, q_in: Queue, q_out: Queue) -> None:
        while True:
            df_block = q_in.get()
            if df_block is None:
                # End of stream.
                q_out.put(None)
                break
            # Split data block by chromosome.
            df_block_by_chromosome = [
                (key, group)
                for key, group in df_block.groupby("chromosome", sort=False)
            ]
            q_out.put(df_block_by_chromosome)
            sys.stderr.write(f"p4 emitted >> {len(df_block_by_chromosome)} blocks\n")

    def _p5_emit_final_blocks(self, q_in: Queue, q_out: Queue, qtl_group: str) -> None:
        # Initialise counters.
        current_chromosome = ""
        current_block_index = 0
        current_data_block = pd.DataFrame(columns=FIELDS)

        # Process.
        while True:
            df_block_by_chromosome = q_in.get()
            if df_block_by_chromosome is None:
                # End of stream.
                break

            first_chromosome_in_block = df_block_by_chromosome[0][0]

            # If this is the first block we ever see, initialise "current_chromosome".
            if not current_chromosome:
                current_chromosome = first_chromosome_in_block

            # If the block starts with the chromosome we are currently processing (this is going to almost always be the case),
            # we append the data from the block to the current chromosome block.
            if first_chromosome_in_block == current_chromosome:
                current_data_block = pd.concat(
                    [current_data_block, df_block_by_chromosome[0][1]],
                    ignore_index=True,
                )
                df_block_by_chromosome = df_block_by_chromosome[1:]

            # If we have any more chromosomes in the block, then we should:
            if df_block_by_chromosome:
                # Emit the current chromosome.
                for block in self._split_final_data(
                    qtl_group,
                    current_chromosome,
                    current_block_index,
                    current_data_block,
                ):
                    q_out.put(block)
                # Emit all chromosomes in the block except the last one (if any are present) because they are complete.
                for chromosome, chromosome_block in df_block_by_chromosome[:-1]:
                    for block in self._split_final_data(
                        qtl_group, chromosome, 0, chromosome_block
                    ):
                        q_out.put(block)
                # And then set current chromosome to the last chromosome in the block.
                current_chromosome, current_data_block = df_block_by_chromosome[-1]
                current_block_index = 0

            # If we have enough data for the chromosome we are currently processing, we can emit some blocks already.
            while len(current_data_block) >= self.emit_ready_buffer:
                emit_block = current_data_block[: self.emit_block_size]
                q_out.put(
                    (qtl_group, current_chromosome, current_block_index, emit_block)
                )
                current_block_index += 1
                current_data_block = current_data_block[self.emit_block_size :]

        # Finally, if we have any data remaining at the end of processing, we should emit it.
        if len(current_data_block):
            for block in self._split_final_data(
                qtl_group, current_chromosome, current_block_index, current_data_block
            ):
                q_out.put(block)

        # Indicate to the next step that the processing is done.
        q_out.put(None)

    def _p6_write_parquet(self, q_in: Queue) -> None:
        """Write a Parquet file for a given input file.

        Args:
            element (tuple[str, str, int, pd.DataFrame]): key and grouped values.
        """
        while True:
            element = q_in.get()
            if element is None:
                # End of stream.
                break
            qtl_group, chromosome, chunk_number, df = element
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
            sys.stderr.write(f"OUTPUT > {output_filename}\n")

    def process(
        self,
        record: Dict[str, Any],
    ) -> None:
        """Process one input file and yield block records ready for Parquet writing.

        Args:
            record (Dict[str, Any]): A record describing one input file and its attributes.

        Yields:
            tuple[str, str, int, pd.DataFrame]: Attribute and data list.
        """
        # Set up concurrent execution.
        q1, q2, q3, q4, q5 = [Queue() for _ in range(5)]
        processes = [
            Process(target=self._p1_fetch_data_from_uri, args=(q1, record["ftp_path"])),
            Process(target=self._p2_emit_complete_line_blocks, args=(q1, q2)),
            Process(target=self._p3_parse_data, args=(q2, q3)),
            Process(target=self._p4_split_by_chromosome, args=(q3, q4)),
            Process(
                target=self._p5_emit_final_blocks, args=(q4, q5, record["qtl_group"])
            ),
            Process(target=self._p6_write_parquet, args=(q5,)),
        ]
        for p in processes:
            p.start()

        # Wait until the final process completes.
        processes[-1].join()


def process(batch_index: int) -> None:
    """Process one input file."""

    # Read the study index and select one study.
    df = pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH)
    record = df.loc[batch_index].to_dict()

    # Process the study.
    ParseData().process(record)


def generate_job_config(number_of_tasks: int, max_parallelism: int = 50) -> str:
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
                    "computeResource": {"cpuMilli": 4000, "memoryMib": 6000},
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
                        "machineType": "n2d-standard-4",
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
