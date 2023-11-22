"""Apache Beam pipeline to preprocess and partition the eQTL Catalogue."""

from __future__ import annotations

import gzip
import io
import typing
import urllib.request
from typing import IO, Any, Dict, Iterator, List

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
    """Process one input file and yield parsed rows."""

    def process(self, record: Dict[str, Any]) -> Iterator[List[str]]:
        """A non-blocking line-by-line iterator from a remote source.

        Args:
            record (Dict[str, Any]): A record describing one input file and its attributes.

        Yields:
            List[str]: List of field values for each row.
        """
        assert (
            record["study"] == "GTEx_V8"
        ), "Only GTEx_V8 studies are currently supported."
        http_path = record["ftp_path"].replace("ftp://", "http://")
        with urllib.request.urlopen(http_path) as compressed_stream:
            with gzip.GzipFile(fileobj=compressed_stream) as uncompressed_stream:
                # See: https://stackoverflow.com/a/58407810.
                typed_stream = typing.cast(IO[bytes], uncompressed_stream)
                with io.TextIOWrapper(typed_stream) as text_stream:
                    for i, line in enumerate(text_stream):
                        if i == 0:
                            # Skip header.
                            continue
                        if i == 5:
                            break
                        print(line)
                        yield line.strip().split("\t")


class ProcessRecord(beam.DoFn):
    """Process one input record."""

    def process(self, record: Dict[str, Any]) -> None:
        """Read one file, transform, save as Parquet.

        Args:
            record (Dict[str, Any]): Attribute dictionary for one file.
        """
        # Output into Parquet, partitioning by chromosome.
        output_filename = (
            f"{EQTL_CATALOGUE_OUPUT_BASE}/"
            "analysisType=eQTL/"
            "sourceId=eQTL_Catalogue/"
            f"projectId={record['study']}/"  # Example: "GTEx_V8".
            f"studyId={record['qtl_group']}/"  # Example: "Adipose_Subcutaneous".
        )
        print(output_filename)
        # df.to_parquet(output_filename, partition_cols=["chromosome"])


def run_pipeline() -> None:
    """Define and run the Apache Beam pipeline."""
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        (
            pipeline
            | beam.Create(get_input_files()[:2])
            | beam.ParDo(ParseData())
            # | beam.ParDo(ProcessRecord())
        )


if __name__ == "__main__":
    run_pipeline()
