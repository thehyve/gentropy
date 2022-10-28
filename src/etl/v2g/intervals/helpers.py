"""Interval helper functions."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def prepare_gene_interval_lut(gene_index: DataFrame) -> DataFrame:
    """Gene symbol lookup table.

    Pre-processess gene/target dataset to create lookup table of gene symbols, including
    obsoleted gene symbols.

    Args:
        gene_index (DataFrame): gene/target DataFrame

    Returns:
        DataFrame: Gene LUT for symbol mapping
    """
    # Prepare gene set:
    genes = (
        # Include TSS
        gene_index.withColumn(
            "tss",
            f.when(
                f.col("genomicLocation.strand") == 1, f.col("genomicLocation.start")
            ).otherwise(
                f.col("genomicLocation.end"),
            ),
        )
        # Consider also obsoleted symbols (explode)
        .withColumn(
            "symbols",
            f.array_union(f.array("approvedSymbol"), f.col("obsoleteSymbols.label")),
        )
        .withColumn("symbols", f.explode("symbols"))
        .withColumnRenamed("id", "geneId")
        .withColumn("chromosome", f.col("genomicLocation.chromosome"))
    )
    return genes


def get_variants_in_intervals(
    interval_df: DataFrame, variants_df: DataFrame
) -> DataFrame:
    """Explodes the interval dataset to find all variants in the region.

    Args:
        interval_df (DataFrame): Interval dataset
        variants_df (DataFrame): DataFrame with a set of variants of interest with the columns "variantId", "chromosome" and "postion"

    Returns:
        DataFrame: V2G evidence based on all the variants found in the intervals
    """
    return (
        interval_df.join(variants_df, on="chromosome", how="inner")
        .filter(f.col("position").between(f.col("start"), f.col("end")))
        .drop("chromosome", "position", "start", "end")
    )
