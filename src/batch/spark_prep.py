"""Quick and resilient framework to preprocess data for subsequent Spark processing."""

from __future__ import annotations

import gzip
import io
import math
import typing
from dataclasses import dataclass
from multiprocessing import Process, Queue
from typing import Any, List

import pandas as pd
from resilient_fetch import ResilientFetch
from typing_extensions import Never

from utils import process_in_pool


@dataclass
class SparkPrep:
    """Fetch, decompress, parse, partition, and save the data."""

    # Configuration for step 1: fetching data from URI.
    # How many bytes of raw (uncompressed) data to fetch and parse at once.
    fetch_chunk_size = 100_000_000

    # Configuration for step 5: partitioning data blocks.
    # How many records, on average, to try and keep in each Parquet partition.
    emit_block_size = 500_000
    # How much of a look-ahead buffer to keep (times the `emit_block_size`)
    # Increasing this value increases memory footprint but decreases spread in the final parquet partitions.
    # 4.0 value means that an average error for the final partitions is ~5% and the maximum possible error is ~10%.
    emit_look_ahead_factor = 4.0
    # Denotes when the look-ahead buffer is long enough to emit a block from it.
    emit_ready_buffer = emit_block_size * (emit_look_ahead_factor + 1)

    # List of field names of the data. Populated by the first step (_p1_fetch_data).
    field_names = None

    # Processing parameters, to be set by during class init.
    input_uri: str
    analysis_type: str
    source_id: str
    project_id: str
    study_id: str
    output_base_path: str

    # File parsing parameters, can be overridden during init as well.
    separator = "\t"
    chromosome_column_name = "chromosome"

    def _p1_fetch_data(self, q_out: Queue[str | None]) -> None:
        """Fetch data from the URI in blocks.

        Args:
            q_out (Queue[str | None]): Output queue with uncompressed text blocks.
        """

        def cast_to_bytes(x: Any) -> typing.IO[bytes]:
            """Casts a given object to bytes. For rationale, see: https://stackoverflow.com/a/58407810.

            Args:
                x (Any): object to cast to bytes.

            Returns:
                typing.IO[bytes]: object cast to bytes.
            """
            return typing.cast(typing.IO[bytes], x)

        with cast_to_bytes(ResilientFetch(self.input_uri)) as gzip_stream:
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
            df_block = pd.read_csv(
                data_io,
                sep=self.separator,
                names=self.field_names,
                header=None,
                dtype=str,
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
                grouped_data = df_block.groupby(self.chromosome_column_name, sort=False)
                for chromosome_id, chromosome_data in grouped_data:
                    q_out.put((chromosome_id, chromosome_data))
            else:
                # We have reached end of stream.
                q_out.put(("", None))
                break

    def _p5_partition_data(
        self,
        q_in: Queue[tuple[str, pd.DataFrame | None]],
        q_out: Queue[tuple[str, int, pd.DataFrame] | None],
    ) -> None:
        """Process stream of data blocks and partition them.

        Args:
            q_in (Queue[tuple[str, pd.DataFrame | None]]): Input queue with Pandas dataframes split by chromosome.
            q_out (Queue[tuple[str, int, pd.DataFrame] | None]): Output queue with ready to output metadata + data.
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
                    q_out.put((current_chromosome, current_block_index, emit_block))
                    current_block_index += 1
                    current_data_block = current_data_block[self.emit_block_size :]
            else:
                # We have reached end of stream.
                q_out.put(None)
                break

    def _p6_write_parquet(
        self, q_in: Queue[tuple[str, int, pd.DataFrame] | None]
    ) -> None:
        """Write a Parquet file for a given input file.

        Args:
            q_in (Queue[tuple[str, int, pd.DataFrame] | None]): Input queue with Pandas metadata + data to output.
        """

        def write_parquet(data: tuple[str, int, pd.DataFrame]) -> None:
            """Write a single Parquet file.

            Args:
                data(tuple[str, int, pd.DataFrame]): Tuple of current chromosome, chunk number, and data to emit.
            """
            chromosome, chunk_number, df = data
            output_filename = (
                f"{self.output_base_path}/"
                f"analysisType={self.analysis_type}/"
                f"sourceId={self.source_id}/"
                f"projectId={self.source_id}/"
                f"studyId={self.study_id}/"
                f"chromosome={chromosome}/"
                f"part-{chunk_number:05}.snappy.parquet"
            )
            df.to_parquet(output_filename, compression="snappy")

        process_in_pool(q_in=q_in, q_out=None, function=write_parquet)

    def process(self) -> None:
        """Process one input file start to finish."""
        # Set up queues for process exchange.
        q: List[Queue[Never]] = [Queue(maxsize=32) for _ in range(5)]
        processes = [
            Process(target=self._p1_fetch_data, args=(q[0],)),
            Process(target=self._p2_emit_complete_line_blocks, args=(q[0], q[1])),
            Process(target=self._p3_parse_data, args=(q[1], q[2])),
            Process(target=self._p4_split_by_chromosome, args=(q[2], q[3])),
            Process(target=self._p5_partition_data, args=(q[3], q[4])),
            Process(target=self._p6_write_parquet, args=(q[4],)),
        ]
        for p in processes:
            p.start()
        # Wait until the final process completes.
        processes[-1].join()
