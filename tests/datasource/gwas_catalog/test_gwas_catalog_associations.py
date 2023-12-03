"""Test GWAS Catalog associations import."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import LongType

from otg.dataset.variant_annotation import VariantAnnotation
from otg.datasource.gwas_catalog.associations import GWASCatalogAssociations


def test_study_locus_gwas_catalog_creation(
    mock_study_locus_gwas_catalog: GWASCatalogAssociations,
) -> None:
    """Test study locus creation with mock data."""
    assert isinstance(mock_study_locus_gwas_catalog, GWASCatalogAssociations)


def test_qc_all(sample_gwas_catalog_associations: DataFrame) -> None:
    """Test qc all with some hard-coded values."""
    assert isinstance(
        sample_gwas_catalog_associations.withColumn(
            # Perform all quality control checks:
            "qualityControls",
            GWASCatalogAssociations._qc_all(
                f.array().alias("qualityControls"),
                f.col("CHR_ID"),
                f.col("CHR_POS"),
                f.lit("A").alias("referenceAllele"),
                f.lit("T").alias("referenceAllele"),
                f.col("STRONGEST SNP-RISK ALLELE"),
                *GWASCatalogAssociations._parse_pvalue(f.col("P-VALUE")),
                5e-8,
            ),
        ),
        DataFrame,
    )


def test_qc_ambiguous_study(
    mock_study_locus_gwas_catalog: GWASCatalogAssociations,
) -> None:
    """Test qc ambiguous."""
    assert isinstance(
        mock_study_locus_gwas_catalog._qc_ambiguous_study(), GWASCatalogAssociations
    )


def test_qc_unresolved_ld(
    mock_study_locus_gwas_catalog: GWASCatalogAssociations,
) -> None:
    """Test qc unresolved LD by making sure the flag is added when ldSet is null."""
    mock_study_locus_gwas_catalog.df = mock_study_locus_gwas_catalog.df.filter(
        f.col("ldSet").isNull()
    )
    observed_df = (
        mock_study_locus_gwas_catalog._qc_unresolved_ld()
        .df.limit(1)
        .select(
            f.array_contains(
                f.col("qualityControls"), "Variant not found in LD reference"
            )
        )
    )
    expected = True
    assert observed_df.collect()[0][0] is expected


def test_study_locus_gwas_catalog_from_source(
    mock_variant_annotation: VariantAnnotation,
    sample_gwas_catalog_associations: DataFrame,
) -> None:
    """Test study locus from gwas catalog mock data."""
    assert isinstance(
        GWASCatalogAssociations.from_source(
            sample_gwas_catalog_associations, mock_variant_annotation
        ),
        GWASCatalogAssociations,
    )


def test__map_to_variant_annotation_variants(
    sample_gwas_catalog_associations: DataFrame,
    mock_variant_annotation: VariantAnnotation,
) -> None:
    """Test mapping to variant annotation variants."""
    assert isinstance(
        GWASCatalogAssociations._map_to_variant_annotation_variants(
            sample_gwas_catalog_associations.withColumn(
                "studyLocusId", f.monotonically_increasing_id().cast(LongType())
            ),
            mock_variant_annotation,
        ),
        DataFrame,
    )