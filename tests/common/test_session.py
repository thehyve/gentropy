"""Tests GWAS Catalog study splitter."""
from __future__ import annotations

from typing import TYPE_CHECKING

from otg.common.session import Log4j, Session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_session_creation() -> None:
    """Test sessio creation with mock data."""
    assert isinstance(Session(spark_uri="local[1]"), Session)


def test_log4j_creation(spark: SparkSession) -> None:
    """Test session log4j."""
    assert isinstance(Log4j(spark=spark), Log4j)