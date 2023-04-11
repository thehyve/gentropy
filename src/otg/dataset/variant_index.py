"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import nullify_empty_array
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import Session
    from otg.dataset.variant_annotation import VariantAnnotation


@dataclass
class VariantIndex(Dataset):
    """Variant index dataset.

    Variant index dataset is the result of intersecting the variant annotation (gnomad) dataset with the variants with V2D available information.
    """

    _schema: StructType = parse_spark_schema("variant_index.json")

    @classmethod
    def from_parquet(
        cls: type[VariantIndex], session: Session, path: str
    ) -> VariantIndex:
        """Initialise VariantIndex from parquet file.

        Args:
            session (Session): ETL session
            path (str): Path to parquet file

        Returns:
            VariantIndex: VariantIndex dataset
        """
        df = session.read_parquet(path=path, schema=cls._schema)
        return cls(_df=df, _schema=cls._schema)

    @classmethod
    def from_variant_annotation(
        cls: type[VariantIndex],
        variant_annotation: VariantAnnotation,
    ) -> VariantIndex:
        """Initialise VariantIndex from pre-existing variant annotation dataset."""
        unchanged_cols = [
            "variantId",
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
            "chromosomeB37",
            "positionB37",
            "alleleType",
            "alleleFrequencies",
            "cadd",
        ]
        vi = cls(
            _df=variant_annotation.df.select(
                *unchanged_cols,
                f.col("vep.mostSevereConsequence").alias("mostSevereConsequence"),
                # filters/rsid are arrays that can be empty, in this case we convert them to null
                nullify_empty_array(f.col("filters")).alias("filters"),
                nullify_empty_array(f.col("rsIds")).alias("rsIds"),
                f.lit(True).alias("variantInGnomad"),
            ),
        )
        return VariantIndex(
            _df=vi.df.repartition(
                400,
                "chromosome",
            ).sortWithinPartitions("chromosome", "position")
        )

    def persist(self: VariantIndex) -> VariantIndex:
        """Persist DataFrame included in the Dataset."""
        self.df = self._df.persist()
        return self