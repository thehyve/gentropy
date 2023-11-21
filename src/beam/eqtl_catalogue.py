"""Apache Beam pipeline to preprocess and partition the eQTL Catalogue."""

from __future__ import annotations

import urllib.request
from io import BytesIO
from typing import Any, Dict, List

import apache_beam as beam
import pandas as pd

EQTL_CATALOGUE_IMPORTED_PATH = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"


def get_input_files() -> List[Dict[str, Any]]:
    """Generate the list of input records.

    Returns:
        List[Dict[str, Any]]: list of input file attribute dictionaries.
    """
    with urllib.request.urlopen(EQTL_CATALOGUE_IMPORTED_PATH) as response:
        assert response.getcode() == 200
        content_bytes = BytesIO(response.read())
        df = pd.read_csv(content_bytes, sep="\t")
        return df.to_dict(orient="records")


class ProcessFile(beam.DoFn):
    """Process one file."""

    def process(self, element: Dict[str, Any]) -> None:
        """Read one file, transform, save as Parquet.

        Args:
            element (Dict[str, Any]): Attribute dictionary for one file.
        """
        print(element)


def run_pipeline() -> None:
    """Define and run the Apache Beam pipeline."""
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Create" >> beam.Create(get_input_files())
            | "ProcessFiles" >> beam.ParDo(ProcessFile())
        )


if __name__ == "__main__":
    run_pipeline()
