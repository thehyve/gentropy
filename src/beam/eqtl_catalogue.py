"""Apache Beam pipeline to preprocess and partition the eQTL Catalogue."""

from __future__ import annotations

from typing import Any, Dict, List

import apache_beam as beam
import pandas as pd

EQTL_CATALOGUE_IMPORTED_PATH = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"
EQTL_CATALOGUE_OUPUT_BASE = (
    "gs://genetics_etl_python_playground/1-smart-mirror/summary_stats"
)


def get_input_files() -> List[Dict[str, Any]]:
    """Generate the list of input records.

    Returns:
        List[Dict[str, Any]]: list of input file attribute dictionaries.
    """
    df = pd.read_table(EQTL_CATALOGUE_IMPORTED_PATH)
    return df.to_dict(orient="records")


class ProcessRecord(beam.DoFn):
    """Process one input record."""

    def process(self, record: Dict[str, Any]) -> None:
        """Read one file, transform, save as Parquet.

        Args:
            record (Dict[str, Any]): Attribute dictionary for one file.
        """
        # Read the file specified by the record.
        assert (
            record["study"] == "GTEx_V8"
        ), "Only GTEx_V8 studies are currently supported."
        df = pd.read_table(record["ftp_path"])
        # Output into Parquet, partitioning by chromosome.
        output_filename = (
            f"{EQTL_CATALOGUE_OUPUT_BASE}/"
            "analysisType=eQTL/"
            "sourceId=eQTL_Catalogue/"
            f"projectId={record['study']}/"  # Example: "GTEx_V8".
            f"studyId={record['qtl_group']}/"  # Example: "Adipose_Subcutaneous".
        )
        df.to_parquet(output_filename, partition_cols=["chromosome"])


def run_pipeline() -> None:
    """Define and run the Apache Beam pipeline."""
    with beam.Pipeline() as pipeline:
        (pipeline | beam.Create(get_input_files()) | beam.ParDo(ProcessRecord()))


if __name__ == "__main__":
    run_pipeline()
