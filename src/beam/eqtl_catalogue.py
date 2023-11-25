"""Apache Beam pipeline to preprocess and partition the eQTL Catalogue."""

from __future__ import annotations

from typing import Any, Dict, Iterator, List

import apache_beam as beam
import pandas as pd
import pyarrow
from apache_beam.options.pipeline_options import PipelineOptions

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


def get_input_files() -> List[Dict[str, Any]]:
    """Generate the list of input records.

    Returns:
        List[Dict[str, Any]]: list of input file attribute dictionaries.
    """
    df = pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH)
    return df.to_dict(orient="records")


class ParseData(beam.DoFn):
    """Parse data."""

    FIELDS = FIELDS

    # How many lines of raw data to fetch and parse at once.
    fetch_chunk_size = 50_000

    # How many records, on average, to try and keep in each Parquet partition.
    emit_block_size = 500_000
    # How much of a look-ahead buffer to keep (times the `emit_block_size`)
    # Increasing this value increases memory footprint but decreases spread in the final parquet partitions.
    # 4.0 value means that an average error for the final partitions is ~5% and the maximum possible error is ~10%.
    emit_look_ahead_factor = 4.0
    # Denotes when the look-ahead buffer is long enough to emit a block from it.
    emit_ready_buffer = emit_block_size * (emit_look_ahead_factor + 1)

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
        block_size = 2 * 1024 * 1024

        def __init__(self, uri: str):
            """Initialise the class.

            Args:
                uri (str): The URI to read the data from.
            """
            import urllib.request

            self.uri = uri
            self.buffer = b""
            self.position = 0
            self.content_length = int(
                urllib.request.urlopen(uri).getheader("Content-Length")
            )
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
            """
            import random
            import time
            import urllib.request

            # If the buffer isn't enough to serve next block, we need to extend it first.
            if (size > len(self.buffer)) and (self.position != self.content_length):
                byte_range = (
                    f"bytes={self.position}-{self.position + self.block_size - 1}"
                )
                request = urllib.request.Request(
                    self.uri, headers={"Range": byte_range}
                )
                delay = self.delay_initial
                total_delay = 0.0
                while True:
                    try:
                        block = urllib.request.urlopen(request).read()
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

    def _fetch_data_in_blocks(self, uri: str) -> Iterator[str]:
        """Fetches data in complete-line blocks of approximate size self.fetch_block_size.

        Args:
            uri (str): URI to fetch the data from.

        Yields:
            str: complete-line blocks of raw data.
        """
        import gzip
        import io
        import typing

        with self.resilient_urlopen(uri) as compressed_stream:
            # See: https://stackoverflow.com/a/58407810.
            compressed_stream_typed = typing.cast(typing.IO[bytes], compressed_stream)
            with gzip.GzipFile(fileobj=compressed_stream_typed) as uncompressed_stream:
                uncompressed_stream_typed = typing.cast(
                    typing.IO[bytes], uncompressed_stream
                )
                with io.TextIOWrapper(uncompressed_stream_typed) as text_stream:
                    # Skip header.
                    text_stream.readline()
                    # Initialise buffer.
                    buffer = ""
                    while True:
                        # Read more data from the URI source.
                        buffer += text_stream.read(self.chunk_size)
                        # If we don't have any data, this means we reached the end of the stream.
                        if not buffer:
                            break
                        # Find the rightmost newline so that we always yield blocks of complete records.
                        rightmost_newline_split = buffer.rfind("\n") + 1
                        yield buffer[:rightmost_newline_split]
                        buffer = buffer[rightmost_newline_split:]

    def _split_final_data(
        self, qtl_group: str, chromosome: str, block_index: int, data: List[str]
    ) -> Iterator[tuple[str, str, int, List[str]]]:
        """Process the final chunk of the data and split into partitions as close to self.emit_block_size as possible.

        Args:
            qtl_group (str): QTL group field used for study ID.
            chromosome (str): Chromosome identifier.
            block_index (int): Starting number of the block to emit.
            data (List[str]): Remaining chunk data to split.

        Yields:
            tuple[str, str, int, List[str]]: Tuple of values to generate the final Parquet file.
        """
        import math

        number_of_blocks = max(round(len(data) / self.emit_block_size), 1)
        records_per_block = math.ceil(len(data) / number_of_blocks)
        for index in range(0, len(data), records_per_block):
            yield (
                qtl_group,
                chromosome,
                block_index,
                data[index : index + records_per_block],
            )
            block_index += 1

    def process(
        self,
        record: Dict[str, Any],
    ) -> Iterator[tuple[str, str, int, List[str]]]:
        """Process one input file and yield per-chromosome blocks of records.

        Args:
            record (Dict[str, Any]): A record describing one input file and its attributes.

        Yields:
            tuple[str, str, int, List[str]]: Attribute and data list.
        """
        import gzip
        import io
        import typing

        assert (
            record["study"] == "GTEx_V8"
        ), "Only GTEx_V8 studies are currently supported."
        http_path = record["ftp_path"].replace("ftp://", "http://")
        with self.resilient_urlopen(http_path) as compressed_stream:
            # See: https://stackoverflow.com/a/58407810.
            compressed_stream_typed = typing.cast(typing.IO[bytes], compressed_stream)
            with gzip.GzipFile(fileobj=compressed_stream_typed) as uncompressed_stream:
                uncompressed_stream_typed = typing.cast(
                    typing.IO[bytes], uncompressed_stream
                )
                with io.TextIOWrapper(uncompressed_stream_typed) as text_stream:
                    current_chromosome = ""
                    current_data_block: List[Any] = []
                    current_block_index = 0
                    observed_chromosomes = set()
                    chromosome_index = self.FIELDS.index("chromosome")
                    for i, row in enumerate(text_stream):
                        if i == 0:
                            # Skip header.
                            continue
                        data = row.split("\t")
                        # Perform actions depending on the chromosome.
                        chromosome = data[chromosome_index]
                        if not current_chromosome:
                            # Initialise for the first record.
                            current_chromosome = chromosome
                        if len(current_data_block) >= self.emit_ready_buffer:
                            data_block = current_data_block[: self.emit_block_size]
                            yield (
                                record["qtl_group"],
                                current_chromosome,
                                current_block_index,
                                data_block,
                            )
                            current_data_block = current_data_block[
                                self.emit_block_size :
                            ]
                            current_block_index += 1
                        if chromosome != current_chromosome:
                            # Yield the block(s) and reset everything.
                            for block in self._split_final_data(
                                record["qtl_group"],
                                current_chromosome,
                                current_block_index,
                                current_data_block,
                            ):
                                yield block
                            current_data_block = []
                            current_block_index = 0
                            observed_chromosomes.add(current_chromosome)
                            assert (
                                chromosome not in observed_chromosomes
                            ), f"Chromosome {chromosome} appears twice in data"
                            current_chromosome = chromosome
                        # Expand existing block.
                        current_data_block.append(data)
                    # Yield any remaining block(s).
                    if current_data_block:
                        for block in self._split_final_data(
                            record["qtl_group"],
                            current_chromosome,
                            current_block_index,
                            current_data_block,
                        ):
                            yield block


class WriteData(beam.DoFn):
    """Write a block of records to Parquet format."""

    EQTL_CATALOGUE_OUPUT_BASE = EQTL_CATALOGUE_OUPUT_BASE
    PYARROW_SCHEMA = PYARROW_SCHEMA
    FIELDS = FIELDS

    def process(self, element: tuple[str, str, int, List[str]]) -> None:
        """Write a Parquet file for a given input file.

        Args:
            element (tuple[str, str, int, List[str]]): key and grouped values.
        """
        import pandas as pd

        qtl_group, chromosome, chunk_number, records = element
        output_filename = (
            f"{self.EQTL_CATALOGUE_OUPUT_BASE}/"
            "analysisType=eQTL/"
            "sourceId=eQTL_Catalogue/"
            "projectId=GTEx_V8/"
            f"studyId={qtl_group}/"  # Example: "Adipose_Subcutaneous".
            f"chromosome={chromosome}/"  # Example: 13.
            f"part-{chunk_number:05}.snappy.parquet"
        )
        pd.DataFrame(records, columns=self.FIELDS).to_parquet(
            output_filename, compression="snappy"
        )


def run_pipeline() -> None:
    """Define and run the Apache Beam pipeline."""
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        (
            pipeline
            | "List input files" >> beam.Create(get_input_files())
            | "Parse data" >> beam.ParDo(ParseData())
            | "Write to Parquet" >> beam.ParDo(WriteData())
        )


if __name__ == "__main__":
    run_pipeline()
