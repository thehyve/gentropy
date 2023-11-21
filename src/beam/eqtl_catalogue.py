"""Apache Beam pipeline to preprocess and partition the eQTL Catalogue."""

from __future__ import annotations

from typing import List

import apache_beam as beam


def get_input_files() -> List[str]:
    """Generate the list of input files.

    Returns:
        List[str]: list of input files.
    """
    return ["file1.txt", "file2.txt", "file3.txt"]


class ProcessFile(beam.DoFn):
    """Process one file."""

    def process(self, element: str) -> None:
        """Read one file, transform, save as Parquet.

        Args:
            element (str): URI for one input file.
        """
        with open(f"{element}.output.txt", "w") as f:
            f.write("Test\n")


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
