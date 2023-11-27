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
    fetch_chunk_size = 5_000_000

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
            import ftplib
            import random
            import time
            import urllib.request

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
                            with ftplib.FTP(self.ftp_server) as ftp:
                                ftp.login()
                                ftp.cwd(self.ftp_path)
                                with ftp.transfercmd(
                                    f"RETR {self.ftp_filename}", rest=self.position
                                ) as server_socket:
                                    block = server_socket.recv(self.block_size)
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
                        buffer += text_stream.read(self.fetch_chunk_size)
                        # If we don't have any data, this means we reached the end of the stream.
                        if not buffer:
                            break
                        # Find the rightmost newline so that we always yield blocks of complete records.
                        rightmost_newline_split = buffer.rfind("\n") + 1
                        yield buffer[:rightmost_newline_split]
                        buffer = buffer[rightmost_newline_split:]

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

    def process(
        self,
        record: Dict[str, Any],
    ) -> Iterator[tuple[str, str, int, pd.DataFrame]]:
        """Process one input file and yield block records ready for Parquet writing.

        Args:
            record (Dict[str, Any]): A record describing one input file and its attributes.

        Yields:
            tuple[str, str, int, pd.DataFrame]: Attribute and data list.
        """
        import io

        import pandas as pd

        assert (
            record["study"] == "GTEx_V8"
        ), "Only GTEx_V8 studies are currently supported."
        # http_path = record["ftp_path"].replace("ftp://", "http://")
        qtl_group = record["qtl_group"]

        current_chromosome = ""
        current_block_index = 0
        current_data_block = pd.DataFrame(columns=self.FIELDS)

        for data_block in self._fetch_data_in_blocks(record["ftp_path"]):
            # Parse data block.
            data_io = io.StringIO(data_block)
            df_block = pd.read_table(data_io, names=self.FIELDS, header=None)

            # Split data block by chromosome.
            df_block_by_chromosome = [
                (key, group)
                for key, group in df_block.groupby("chromosome", sort=False)
            ]
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
                    yield block
                # Emit all chromosomes in the block except the last one (if any are present) because they are complete.
                for chromosome, chromosome_block in df_block_by_chromosome[:-1]:
                    for block in self._split_final_data(
                        qtl_group, chromosome, 0, chromosome_block
                    ):
                        yield block
                # And then set current chromosome to the last chromosome in the block.
                current_chromosome, current_data_block = df_block_by_chromosome[-1]
                current_block_index = 0

            # If we have enough data for the chromosome we are currently processing, we can emit some blocks already.
            while len(current_data_block) >= self.emit_ready_buffer:
                emit_block = current_data_block[: self.emit_block_size]
                yield (qtl_group, current_chromosome, current_block_index, emit_block)
                current_block_index += 1
                current_data_block = current_data_block[self.emit_block_size :]

        # Finally, if we have any data remaining at the end of processing, we should emit it.
        if len(current_data_block):
            for block in self._split_final_data(
                qtl_group, current_chromosome, current_block_index, current_data_block
            ):
                yield block


class WriteData(beam.DoFn):
    """Write a block of records to Parquet format."""

    EQTL_CATALOGUE_OUPUT_BASE = EQTL_CATALOGUE_OUPUT_BASE
    PYARROW_SCHEMA = PYARROW_SCHEMA
    FIELDS = FIELDS

    def process(self, element: tuple[str, str, int, pd.DataFrame]) -> None:
        """Write a Parquet file for a given input file.

        Args:
            element (tuple[str, str, int, pd.DataFrame]): key and grouped values.
        """
        qtl_group, chromosome, chunk_number, df = element
        output_filename = (
            f"{self.EQTL_CATALOGUE_OUPUT_BASE}/"
            "analysisType=eQTL/"
            "sourceId=eQTL_Catalogue/"
            "projectId=GTEx_V8/"
            f"studyId={qtl_group}/"  # Example: "Adipose_Subcutaneous".
            f"chromosome={chromosome}/"  # Example: 13.
            f"part-{chunk_number:05}.snappy.parquet"
        )
        df.to_parquet(output_filename, compression="snappy")


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
