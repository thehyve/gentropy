"""Summary satistics dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
import scipy as sc
from pyspark.sql.functions import log10

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.utils import parse_region, split_pvalue
from gentropy.dataset.dataset import Dataset
from gentropy.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_locus import StudyLocus


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the SummaryStatistics dataset.

        Returns:
            StructType: Schema for the SummaryStatistics dataset
        """
        return parse_spark_schema("summary_statistics.json")

    def pvalue_filter(self: SummaryStatistics, pvalue: float) -> SummaryStatistics:
        """Filter summary statistics based on the provided p-value threshold.

        Args:
            pvalue (float): upper limit of the p-value to be filtered upon.

        Returns:
            SummaryStatistics: summary statistics object containing single point associations with p-values at least as significant as the provided threshold.
        """
        # Converting p-value to mantissa and exponent:
        (mantissa, exponent) = split_pvalue(pvalue)

        # Applying filter:
        df = self._df.filter(
            (f.col("pValueExponent") < exponent)
            | (
                (f.col("pValueExponent") == exponent)
                & (f.col("pValueMantissa") <= mantissa)
            )
        )
        return SummaryStatistics(_df=df, _schema=self._schema)

    def window_based_clumping(
        self: SummaryStatistics,
        distance: int = 500_000,
        gwas_significance: float = 5e-8,
        baseline_significance: float = 0.05,
        locus_collect_distance: int | None = None,
    ) -> StudyLocus:
        """Generate study-locus from summary statistics by distance based clumping + collect locus.

        Args:
            distance (int): Distance in base pairs to be used for clumping. Defaults to 500_000.
            gwas_significance (float, optional): GWAS significance threshold. Defaults to 5e-8.
            baseline_significance (float, optional): Baseline significance threshold for inclusion in the locus. Defaults to 0.05.
            locus_collect_distance (int | None): The distance to collect locus around semi-indices. If not provided, locus is not collected.

        Returns:
            StudyLocus: Clumped study-locus containing variants based on window.
        """
        return (
            WindowBasedClumping.clump_with_locus(
                self,
                window_length=distance,
                p_value_significance=gwas_significance,
                p_value_baseline=baseline_significance,
                locus_window_length=locus_collect_distance,
            )
            if locus_collect_distance
            else WindowBasedClumping.clump(
                self,
                window_length=distance,
                p_value_significance=gwas_significance,
            )
        )

    def exclude_region(self: SummaryStatistics, region: str) -> SummaryStatistics:
        """Exclude a region from the summary stats dataset.

        Args:
            region (str): region given in "chr##:#####-####" format

        Returns:
            SummaryStatistics: filtered summary statistics.
        """
        (chromosome, start_position, end_position) = parse_region(region)

        return SummaryStatistics(
            _df=(
                self.df.filter(
                    ~(
                        (f.col("chromosome") == chromosome)
                        & (
                            (f.col("position") >= start_position)
                            & (f.col("position") <= end_position)
                        )
                    )
                )
            ),
            _schema=SummaryStatistics.get_schema(),
        )

    def sumstat_qc_beta_check(
        self: SummaryStatistics,
        threshold: float = 0.05,
    ) -> bool:
        """The mean beta check for QC of GWAS summary statstics.

        Args:
            threshold (float): The threshold for mean beta check.

        Returns:
            bool: Boolean whether this study passed the QC step or not.
        """
        GWAS = self._df
        QC = (
            GWAS.agg(
                f.mean("beta").alias("mean_beta"),
            )
            .select(
                "mean_beta",
            )
            .collect()[0]
        )

        return (np.abs(QC) <= threshold)[0]

    def sumstat_qc_pz_check(
        self: SummaryStatistics,
        threshold_b: float = 0.05,
        threshold_intercept: float = 0.05,
    ) -> bool:
        """The PZ check for QC of GWAS summary statstics. It runs linear regression between reorted p-values and p-values infered from z-scores.

        Args:
            threshold_b (float): The threshold for b coeffcicient in linear regression.
            threshold_intercept (float): The threshold for intercept in linear regression.

        Returns:
            bool: Boolean whether this study passed the QC step or not.
        """

        def calculate_logpval(z2: float) -> float:
            """Calculate negative log10-pval from Z-score.

            Args:
                z2 (float): Z-score squared.

            Returns:
                float: log10-pval.

            Examples:
                >>> calculate_logpval(1.0)
                0.3010299956639812
            """
            logpval = -np.log10(sc.stats.chi2.sf((z2), 1))
            return float(logpval)

        def calculate_lin_reg(y: list[float], x: list[float]) -> list[float]:
            """Calculate linear regression.

            Args:
                y (list[float]): y values.
                x (list[float]): x values.

            Returns:
                list[float]: slope, slope_stderr, intercept, intercept_stderr.

            Examples:
                >>> calculate_lin_reg([1,2,3], [1,2,3])
                [1.0, 0.0, 0.0, 0.0]
            """
            lin_reg = sc.stats.linregress(y, x)
            return [
                float(lin_reg.slope),
                float(lin_reg.stderr),
                float(lin_reg.intercept),
                float(lin_reg.intercept_stderr),
            ]

        GWAS = self._df

        linear_reg_Schema = t.StructType(
            [
                t.StructField("beta", t.FloatType(), False),
                t.StructField("beta_stderr", t.FloatType(), False),
                t.StructField("intercept", t.FloatType(), False),
                t.StructField("intercept_stderr", t.FloatType(), False),
            ]
        )

        calculate_logpval_udf = f.udf(calculate_logpval, t.DoubleType())
        lin_udf = f.udf(calculate_lin_reg, linear_reg_Schema)

        QC = (
            GWAS.limit(1000)
            .withColumn("zscore", f.col("beta") / f.col("standardError"))
            .withColumn("new_logpval", calculate_logpval_udf(f.col("zscore") ** 2))
            .withColumn("log_mantissa", log10("pValueMantissa"))
            .withColumn("logpval", -f.col("log_mantissa") - f.col("pValueExponent"))
            .agg(
                f.collect_list("logpval").alias("pval_vector"),
                f.collect_list("new_logpval").alias("new_pval_vector"),
            )
            .withColumn(
                "result_lin_reg",
                lin_udf(f.col("pval_vector"), f.col("new_pval_vector")),
            )
            .select(
                "result_lin_reg",
            )
        )

        lin_results = QC.toPandas().iloc[0, 0]
        beta = lin_results[0]
        interc = lin_results[2]
        return np.abs(beta - 1) <= threshold_b and np.abs(interc) <= threshold_intercept
