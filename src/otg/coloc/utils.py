"""Utils to process the datasets used to generate colocation results."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _add_coloc_sumstats_info(coloc: DataFrame, sumstats: DataFrame) -> DataFrame:
    """Adds relevant metadata to colocalisation results from summary stats.

    Args:
        coloc (DataFrame): Colocalisation results
        sumstats (DataFrame): Summary stats dataset

    Returns:
        DataFrame: Colocalisation results with summary stats metadata added
    """
    sumstats_leftvar_rightstudy = (
        # sumstats_path ~250Gb dataset
        sumstats.repartition("chrom")
        .withColumn(
            "right_studyId",
            f.xxhash64(*["type", "study_id", "phenotype_id", "bio_feature"]),
        )
        .withColumn(
            "left_lead_variant_id",
            f.concat_ws("_", f.col("chrom"), f.col("pos"), f.col("ref"), f.col("alt")),
        )
        .withColumnRenamed("chrom", "left_chrom")
        .withColumnRenamed("beta", "left_var_right_study_beta")
        .withColumnRenamed("se", "left_var_right_study_se")
        .withColumnRenamed("pval", "left_var_right_study_pval")
        .withColumnRenamed("is_cc", "left_var_right_isCC")
        # Only keep required columns
        .select(
            "left_chrom",
            "left_lead_variant_id",
            "right_studyId",
            "left_var_right_study_beta",
            "left_var_right_study_se",
            "left_var_right_study_pval",
            "left_var_right_isCC",
        )
    )

    return (
        # join info from sumstats
        sumstats_leftvar_rightstudy.join(
            f.broadcast(coloc),
            on=["left_chrom", "left_lead_variant_id", "right_studyId"],
            how="right",
        )
        # clean unnecessary columns
        .drop("left_lead_variant_id", "right_lead_variant_id")
        .drop("left_bio_feature", "left_phenotype")
        .drop("left_studyId", "right_studyId")
    )


def _extract_credible_sets(
    study_locus: DataFrame,
) -> DataFrame:
    """Extract credible sets from the study/locus associations df.

    Args:
        study_locus: DataFrame with study/locus associations.

    Returns:
        DataFrame with lead/tag variant IDs, study ID and logABF.
    """
    return (
        study_locus.withColumn("credibleSet", f.explode("credibleSets"))
        .withColumn("credibleVariant", f.explode("credibleSet.credibleSet"))
        .filter(f.col("credibleVariant.is95CredibleSet"))
        .select(
            f.col("variantId").alias("leadVariantId"),
            "studyId",
            "credibleVariant.tagVariantId",
            "credibleVariant.logABF",
            "credibleVariant.posteriorProbability",
            f.split(f.col("variantId"), "_")[0].alias("chromosome"),
            "credibleSet.method",
        )
    )
