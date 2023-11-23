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

    def process(
        self,
        record: Dict[str, Any],
    ) -> Iterator[tuple[tuple[Any, str | None], list[Any]]]:
        """Process one input file and yield per-chromosome blocks of records.

        Args:
            record (Dict[str, Any]): A record describing one input file and its attributes.

        Yields:
            tuple[tuple[Any, str | None], list[Any]]: QTL group and record dictionary.
        """
        import gzip
        import io
        import typing
        import urllib.request

        assert (
            record["study"] == "GTEx_V8"
        ), "Only GTEx_V8 studies are currently supported."
        http_path = record["ftp_path"].replace("ftp://", "http://")
        with urllib.request.urlopen(http_path) as compressed_stream:
            with gzip.GzipFile(fileobj=compressed_stream) as uncompressed_stream:
                # See: https://stackoverflow.com/a/58407810.
                typed_stream = typing.cast(typing.IO[bytes], uncompressed_stream)
                with io.TextIOWrapper(typed_stream) as text_stream:
                    current_chromosome = None
                    current_data_block: List[Any] = []
                    observed_chromosomes = set()
                    for i, line in enumerate(text_stream):
                        if i == 0:
                            # Skip header.
                            continue
                        # if i == 1000000:
                        #     break
                        data = dict(
                            zip(self.FIELDS, line.strip().split("\t"), strict=True)
                        )

                        # Perform actions depending on the chromosome.
                        chromosome = data["chromosome"]
                        if current_chromosome is None:
                            # Initialise for the first record.
                            current_chromosome = chromosome
                        if chromosome != current_chromosome:
                            # Yield the block and start a new one.
                            yield (
                                (record["qtl_group"], current_chromosome),
                                current_data_block,
                            )
                            current_data_block = []
                            observed_chromosomes.add(current_chromosome)
                            assert (
                                chromosome not in observed_chromosomes
                            ), f"Chromosome {chromosome} appears twice in data"
                            current_chromosome = chromosome
                        # Expand existing block.
                        current_data_block.append(data)
                    # Yield last block.
                    if current_data_block:
                        yield (
                            (record["qtl_group"], current_chromosome),
                            current_data_block,
                        )


class WriteData(beam.DoFn):
    """Write a block of records to Parquet format."""

    EQTL_CATALOGUE_OUPUT_BASE = EQTL_CATALOGUE_OUPUT_BASE
    PYARROW_SCHEMA = PYARROW_SCHEMA

    def process(self, element: tuple[Any, Any]) -> None:
        """Write a Parquet file for a given input file.

        Args:
            element (tuple[Any, Any]): key and grouped values.
        """
        from apache_beam.io import WriteToParquet

        (qtl_group, chromosome), records = element
        output_filename = (
            f"{self.EQTL_CATALOGUE_OUPUT_BASE}/"
            "analysisType=eQTL/"
            "sourceId=eQTL_Catalogue/"
            "projectId=GTEx_V8/"
            f"studyId={qtl_group}/"  # Example: "Adipose_Subcutaneous".
            f"chromosome={chromosome}/"  # Example: 13.
            "part"
        )
        records | WriteToParquet(
            file_path_prefix=output_filename,
            file_name_suffix=".parquet",
            schema=self.PYARROW_SCHEMA,
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
