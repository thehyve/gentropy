"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from pyspark.sql import SparkSession

from otg.dataset.summary_statistics import SummaryStatistics
from otg.datasource.finngen.summary_stats import FinnGenSummaryStats


def test_finngen_summary_stats_from_source(spark: SparkSession) -> None:
    """Test summary statistics from source."""
    assert isinstance(
        FinnGenSummaryStats.from_source(
            spark=spark,
            raw_files=["tests/data_samples/finngen_R9_AB1_ACTINOMYCOSIS.gz"],
        ),
        SummaryStatistics,
    )