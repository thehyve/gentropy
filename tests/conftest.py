"""Unit test configuration."""
from __future__ import annotations

from pathlib import Path

import dbldatagen as dg
import hail as hl
import pytest
from pyspark.sql import DataFrame, SparkSession

from otg.common.Liftover import LiftOverSpark
from otg.dataset.colocalisation import Colocalisation
from otg.dataset.gene_index import GeneIndex
from otg.dataset.intervals import Intervals
from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.dataset.l2g_prediction import L2GPrediction
from otg.dataset.ld_index import LDIndex
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.summary_statistics import SummaryStatistics
from otg.dataset.v2g import V2G
from otg.dataset.variant_annotation import VariantAnnotation
from otg.dataset.variant_index import VariantIndex
from otg.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex
from otg.datasource.eqtl_catalogue.summary_stats import EqtlCatalogueSummaryStats
from otg.datasource.finngen.study_index import FinnGenStudyIndex
from otg.datasource.finngen.summary_stats import FinnGenSummaryStats
from otg.datasource.gwas_catalog.associations import GWASCatalogAssociations
from otg.datasource.gwas_catalog.study_index import GWASCatalogStudyIndex
from otg.datasource.ukbiobank.study_index import UKBiobankStudyIndex
from utils.spark import get_spark_testing_conf


@pytest.fixture(scope="session", autouse=True)
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    """Local spark session for testing purposes.

    Args:
        tmp_path_factory (pytest.TempPathFactory): pytest fixture

    Returns:
        SparkSession: local spark session
    """
    return (
        SparkSession.builder.config(conf=get_spark_testing_conf())
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )


@pytest.fixture()
def hail_home() -> str:
    """Return the path to the Hail home directory."""
    return Path(hl.__file__).parent.as_posix()


@pytest.fixture()
def mock_colocalisation(spark: SparkSession) -> Colocalisation:
    """Mock colocalisation dataset."""
    coloc_schema = Colocalisation.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(coloc_schema)
        .withColumnSpec("h0", percentNulls=0.1)
        .withColumnSpec("h1", percentNulls=0.1)
        .withColumnSpec("h2", percentNulls=0.1)
        .withColumnSpec("h3", percentNulls=0.1)
        .withColumnSpec("h4", percentNulls=0.1)
        .withColumnSpec("log2h4h3", percentNulls=0.1)
        .withColumnSpec("clpp", percentNulls=0.1)
    )
    return Colocalisation(_df=data_spec.build(), _schema=coloc_schema)


def mock_study_index_data(spark: SparkSession) -> DataFrame:
    """Mock study index dataset."""
    si_schema = StudyIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(si_schema)
        .withColumnSpec(
            "traitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "backgroundTraitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "discoverySamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "replicationSamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec("geneId", percentNulls=0.1)
        .withColumnSpec("pubmedId", percentNulls=0.1)
        .withColumnSpec("publicationFirstAuthor", percentNulls=0.1)
        .withColumnSpec("publicationDate", percentNulls=0.1)
        .withColumnSpec("publicationJournal", percentNulls=0.1)
        .withColumnSpec("publicationTitle", percentNulls=0.1)
        .withColumnSpec("initialSampleSize", percentNulls=0.1)
        .withColumnSpec("nCases", percentNulls=0.1)
        .withColumnSpec("nControls", percentNulls=0.1)
        .withColumnSpec("nSamples", percentNulls=0.1)
        .withColumnSpec("summarystatsLocation", percentNulls=0.1)
        .withColumnSpec("studyType", percentNulls=0.0, values=["eqtl", "pqtl", "sqtl"])
    )
    return data_spec.build()


@pytest.fixture()
def mock_study_index(spark: SparkSession) -> StudyIndex:
    """Mock StudyIndex dataset."""
    return StudyIndex(
        _df=mock_study_index_data(spark),
        _schema=StudyIndex.get_schema(),
    )


@pytest.fixture()
def mock_study_index_gwas_catalog(spark: SparkSession) -> GWASCatalogStudyIndex:
    """Mock GWASCatalogStudyIndex dataset."""
    return GWASCatalogStudyIndex(
        _df=mock_study_index_data(spark),
        _schema=StudyIndex.get_schema(),
    )


@pytest.fixture()
def mock_study_index_finngen(spark: SparkSession) -> FinnGenStudyIndex:
    """Mock FinnGenStudyIndex dataset."""
    return FinnGenStudyIndex(
        _df=mock_study_index_data(spark),
        _schema=StudyIndex.get_schema(),
    )


@pytest.fixture()
def mock_summary_stats_finngen(spark: SparkSession) -> FinnGenSummaryStats:
    """Mock FinnGenSummaryStats dataset."""
    return FinnGenSummaryStats(
        _df=mock_summary_statistics_data(spark),
        _schema=SummaryStatistics.get_schema(),
    )


@pytest.fixture()
def mock_study_index_eqtl_catalogue(spark: SparkSession) -> EqtlCatalogueStudyIndex:
    """Mock EqtlCatalogueStudyIndex dataset."""
    return EqtlCatalogueStudyIndex(
        _df=mock_study_index_data(spark),
        _schema=StudyIndex.get_schema(),
    )


@pytest.fixture()
def mock_summary_stats_eqtl_catalogue(spark: SparkSession) -> EqtlCatalogueSummaryStats:
    """Mock EqtlCatalogueSummaryStats dataset."""
    return EqtlCatalogueSummaryStats(
        _df=mock_summary_statistics_data(spark),
        _schema=SummaryStatistics.get_schema(),
    )


@pytest.fixture()
def mock_study_index_ukbiobank(spark: SparkSession) -> UKBiobankStudyIndex:
    """Mock StudyIndexUKBiobank dataset."""
    return UKBiobankStudyIndex(
        _df=mock_study_index_data(spark),
        _schema=UKBiobankStudyIndex.get_schema(),
    )


@pytest.fixture()
def mock_study_locus_overlap(spark: SparkSession) -> StudyLocusOverlap:
    """Mock StudyLocusOverlap dataset."""
    overlap_schema = StudyLocusOverlap.get_schema()

    data_spec = dg.DataGenerator(
        spark,
        rows=400,
        partitions=4,
        randomSeedMethod="hash_fieldname",
    ).withSchema(overlap_schema)

    return StudyLocusOverlap(_df=data_spec.build(), _schema=overlap_schema)


def mock_study_locus_data(spark: SparkSession) -> DataFrame:
    """Mock study_locus dataset."""
    sl_schema = StudyLocus.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(sl_schema)
        .withColumnSpec("chromosome", percentNulls=0.1)
        .withColumnSpec("position", percentNulls=0.1)
        .withColumnSpec("beta", percentNulls=0.1)
        .withColumnSpec("oddsRatio", percentNulls=0.1)
        .withColumnSpec("oddsRatioConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("oddsRatioConfidenceIntervalUpper", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalUpper", percentNulls=0.1)
        .withColumnSpec("effectAlleleFrequencyFromSource", percentNulls=0.1)
        .withColumnSpec("standardError", percentNulls=0.1)
        .withColumnSpec("subStudyDescription", percentNulls=0.1)
        .withColumnSpec("pValueMantissa", minValue=1, percentNulls=0.1)
        .withColumnSpec("pValueExponent", minValue=1, percentNulls=0.1)
        .withColumnSpec(
            "qualityControls",
            expr="array(cast(rand() as string))",
            percentNulls=0.1,
        )
        .withColumnSpec("finemappingMethod", percentNulls=0.1)
        .withColumnSpec(
            "locus",
            expr='array(named_struct("is95CredibleSet", cast(rand() > 0.5 as boolean), "is99CredibleSet", cast(rand() > 0.5 as boolean), "logABF", rand(), "posteriorProbability", rand(), "variantId", cast(rand() as string), "beta", rand(), "standardError", rand(), "betaConditioned", rand(), "standardErrorConditioned", rand(), "r2Overall", rand(), "pValueMantissaConditioned", rand(), "pValueExponentConditioned", rand(), "pValueMantissa", rand(), "pValueExponent", rand()))',
            percentNulls=0.1,
        )
    )
    return data_spec.build()


@pytest.fixture()
def mock_study_locus(spark: SparkSession) -> StudyLocus:
    """Mock study_locus dataset."""
    return StudyLocus(
        _df=mock_study_locus_data(spark),
        _schema=StudyLocus.get_schema(),
    )


@pytest.fixture()
def mock_study_locus_gwas_catalog(spark: SparkSession) -> StudyLocus:
    """Mock study_locus dataset."""
    return GWASCatalogAssociations(
        _df=mock_study_locus_data(spark),
        _schema=GWASCatalogAssociations.get_schema(),
    )


@pytest.fixture()
def mock_intervals(spark: SparkSession) -> Intervals:
    """Mock intervals dataset."""
    interval_schema = Intervals.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(interval_schema)
        .withColumnSpec("pmid", percentNulls=0.1)
        .withColumnSpec("resourceScore", percentNulls=0.1)
        .withColumnSpec("score", percentNulls=0.1)
        .withColumnSpec("biofeature", percentNulls=0.1)
    )

    return Intervals(_df=data_spec.build(), _schema=interval_schema)


@pytest.fixture()
def mock_v2g(spark: SparkSession) -> V2G:
    """Mock v2g dataset."""
    v2g_schema = V2G.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(v2g_schema)
        .withColumnSpec("distance", percentNulls=0.1)
        .withColumnSpec("resourceScore", percentNulls=0.1)
        .withColumnSpec("score", percentNulls=0.1)
        .withColumnSpec("pmid", percentNulls=0.1)
        .withColumnSpec("biofeature", percentNulls=0.1)
        .withColumnSpec("variantFunctionalConsequenceId", percentNulls=0.1)
        .withColumnSpec("isHighQualityPlof", percentNulls=0.1)
    )

    return V2G(_df=data_spec.build(), _schema=v2g_schema)


@pytest.fixture()
def mock_variant_annotation(spark: SparkSession) -> VariantAnnotation:
    """Mock variant annotation."""
    va_schema = VariantAnnotation.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(va_schema)
        .withColumnSpec("alleleType", percentNulls=0.1)
        .withColumnSpec("chromosomeB37", percentNulls=0.1)
        .withColumnSpec("positionB37", percentNulls=0.1)
        # Nested column handling workaround
        # https://github.com/databrickslabs/dbldatagen/issues/135
        # It's a workaround for nested column handling in dbldatagen.
        .withColumnSpec(
            "alleleFrequencies",
            expr='array(named_struct("alleleFrequency", rand(), "populationName", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "cadd",
            expr='named_struct("phred", cast(rand() as float), "raw", cast(rand() as float))',
            percentNulls=0.1,
        )
        .withColumnSpec("rsIds", expr="array(cast(rand() AS string))", percentNulls=0.1)
        .withColumnSpec(
            "vep",
            expr='named_struct("mostSevereConsequence", cast(rand() as string), "transcriptConsequences", array(named_struct("aminoAcids", cast(rand() as string), "consequenceTerms", array(cast(rand() as string)), "geneId", cast(rand() as string), "lof", cast(rand() as string), "polyphenPrediction", cast(rand() as string), "polyphenScore", cast(rand() as float), "siftPrediction", cast(rand() as string), "siftScore", cast(rand() as float))))',
            percentNulls=0.1,
        )
    )
    return VariantAnnotation(_df=data_spec.build(), _schema=va_schema)


@pytest.fixture()
def mock_variant_index(spark: SparkSession) -> VariantIndex:
    """Mock gene index."""
    vi_schema = VariantIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(vi_schema)
        .withColumnSpec("chromosomeB37", percentNulls=0.1)
        .withColumnSpec("positionB37", percentNulls=0.1)
        .withColumnSpec("mostSevereConsequence", percentNulls=0.1)
        # Nested column handling workaround
        # https://github.com/databrickslabs/dbldatagen/issues/135
        # It's a workaround for nested column handling in dbldatagen.
        .withColumnSpec(
            "alleleFrequencies",
            expr='array(named_struct("alleleFrequency", rand(), "populationName", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "cadd",
            expr='named_struct("phred", cast(rand() AS float), "raw", cast(rand() AS float))',
            percentNulls=0.1,
        )
        .withColumnSpec("rsIds", expr="array(cast(rand() AS string))", percentNulls=0.1)
    )

    return VariantIndex(_df=data_spec.build(), _schema=vi_schema)


@pytest.fixture()
def mock_summary_statistics_data(spark: SparkSession) -> DataFrame:
    """Generating mock summary statistics data.

    Args:
        spark (SparkSession): Spark session

    Returns:
        DataFrame: Mock summary statistics data
    """
    ss_schema = SummaryStatistics.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="summaryStats",
        )
        .withSchema(ss_schema)
        # Allowing missingness in effect allele frequency and enforce upper limit:
        .withColumnSpec(
            "effectAlleleFrequencyFromSource", percentNulls=0.1, maxValue=1.0
        )
        # Allowing missingness:
        .withColumnSpec("betaConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalUpper", percentNulls=0.1)
        .withColumnSpec("standardError", percentNulls=0.1)
        # Making sure p-values are below 1:
    ).build()

    # Because some of the columns are not strictly speaking required, they are dropped now:
    data_spec = data_spec.drop(
        "betaConfidenceIntervalLower", "betaConfidenceIntervalUpper"
    )

    return data_spec


@pytest.fixture()
def mock_summary_statistics(
    mock_summary_statistics_data: DataFrame,
) -> SummaryStatistics:
    """Generating a mock summary statistics dataset."""
    return SummaryStatistics(
        _df=mock_summary_statistics_data, _schema=SummaryStatistics.get_schema()
    )


@pytest.fixture()
def mock_ld_index(spark: SparkSession) -> LDIndex:
    """Mock gene index."""
    ld_schema = LDIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(ld_schema)
        .withColumnSpec(
            "ldSet",
            expr="array(named_struct('tagVariantId', cast(rand() as string), 'rValues', array(named_struct('population', cast(rand() as string), 'r', cast(rand() as double)))))",
        )
    )

    return LDIndex(_df=data_spec.build(), _schema=ld_schema)


@pytest.fixture()
def sample_gwas_catalog_studies(spark: SparkSession) -> DataFrame:
    """Sample GWAS Catalog studies."""
    return spark.read.csv(
        "tests/data_samples/gwas_catalog_studies_sample-r2022-11-29.tsv",
        sep="\t",
        header=True,
    )


@pytest.fixture()
def sample_gwas_catalog_ancestries_lut(spark: SparkSession) -> DataFrame:
    """Sample GWAS ancestries sample data."""
    return spark.read.csv(
        "tests/data_samples/gwas_catalog_ancestries_sample_v1.0.3-r2022-11-29.tsv",
        sep="\t",
        header=True,
    )


@pytest.fixture()
def sample_gwas_catalog_harmonised_sumstats(spark: SparkSession) -> DataFrame:
    """Sample GWAS harmonised sumstats sample data."""
    return spark.read.csv(
        "tests/data_samples/gwas_summary_stats_sample.tsv.gz",
        sep="\t",
        header=True,
    )


@pytest.fixture()
def sample_gwas_catalog_harmonised_sumstats_list(spark: SparkSession) -> DataFrame:
    """Sample GWAS harmonised sumstats sample data."""
    return spark.read.csv(
        "tests/data_samples/gwas_catalog_harmonised_list.txt",
        sep="\t",
        header=False,
    )


@pytest.fixture()
def sample_gwas_catalog_associations(spark: SparkSession) -> DataFrame:
    """Sample GWAS raw associations sample data."""
    return spark.read.csv(
        "tests/data_samples/gwas_catalog_associations_sample_e107_r2022-11-29.tsv",
        sep="\t",
        header=True,
    )


@pytest.fixture()
def sample_summary_satistics(spark: SparkSession) -> SummaryStatistics:
    """Sample GWAS raw associations sample data."""
    return SummaryStatistics(
        _df=spark.read.parquet("tests/data_samples/GCST005523_chr18.parquet"),
        _schema=SummaryStatistics.get_schema(),
    )


@pytest.fixture()
def sample_finngen_studies(spark: SparkSession) -> DataFrame:
    """Sample FinnGen studies."""
    # For reference, the sample file was generated with the following command:
    # curl https://r9.finngen.fi/api/phenos | jq '.[:10]' > tests/data_samples/finngen_studies_sample.json
    with open("tests/data_samples/finngen_studies_sample.json") as finngen_studies:
        json_data = finngen_studies.read()
        rdd = spark.sparkContext.parallelize([json_data])
        return spark.read.json(rdd)


@pytest.fixture()
def sample_finngen_summary_stats(spark: SparkSession) -> DataFrame:
    """Sample FinnGen summary stats."""
    # For reference, the sample file was generated with the following command:
    # gsutil cat gs://finngen-public-data-r9/summary_stats/finngen_R9_AB1_ACTINOMYCOSIS.gz | gzip -cd | head -n11 | gzip -c > tests/data_samples/finngen_R9_AB1_ACTINOMYCOSIS.gz
    # It's important for the test file to be named in exactly this way, because FinnGen study ID is populated based on input file name.
    return spark.read.option("delimiter", "\t").csv(
        "tests/data_samples/finngen_R9_AB1_ACTINOMYCOSIS.gz", header=True
    )


@pytest.fixture()
def sample_eqtl_catalogue_studies(spark: SparkSession) -> DataFrame:
    """Sample eQTL Catalogue studies."""
    # For reference, the sample file was generated with the following command:
    # curl https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv | head -n11 > tests/data_samples/eqtl_catalogue_studies_sample.tsv
    with open(
        "tests/data_samples/eqtl_catalogue_studies_sample.json"
    ) as eqtl_catalogue:
        tsv = eqtl_catalogue.read()
        rdd = spark.sparkContext.parallelize([tsv])
        return spark.read.csv(rdd, sep="\t", header=True)


@pytest.fixture()
def sample_ukbiobank_studies(spark: SparkSession) -> DataFrame:
    """Sample UKBiobank manifest."""
    # Sampled 10 rows of the UKBB manifest tsv
    return spark.read.csv(
        "tests/data_samples/neale2_saige_study_manifest.samples.tsv",
        sep="\t",
        header=True,
        inferSchema=True,
    )


@pytest.fixture()
def sample_target_index(spark: SparkSession) -> DataFrame:
    """Sample target index sample data."""
    return spark.read.parquet(
        "tests/data_samples/target_sample.parquet",
    )


@pytest.fixture()
def mock_gene_index(spark: SparkSession) -> GeneIndex:
    """Mock gene index dataset."""
    gi_schema = GeneIndex.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(gi_schema)
        .withColumnSpec("approvedSymbol", percentNulls=0.1)
        .withColumnSpec("biotype", percentNulls=0.1)
        .withColumnSpec("approvedName", percentNulls=0.1)
        .withColumnSpec("tss", percentNulls=0.1)
        .withColumnSpec("start", percentNulls=0.1)
        .withColumnSpec("end", percentNulls=0.1)
        .withColumnSpec("strand", percentNulls=0.1)
    )

    return GeneIndex(_df=data_spec.build(), _schema=gi_schema)


@pytest.fixture()
def liftover_chain_37_to_38(spark: SparkSession) -> LiftOverSpark:
    """Sample liftover chain file."""
    return LiftOverSpark("tests/data_samples/grch37_to_grch38.over.chain")


@pytest.fixture()
def sample_l2g_gold_standard(spark: SparkSession) -> DataFrame:
    """Sample L2G gold standard curation."""
    return spark.read.json(
        "tests/data_samples/l2g_gold_standard_curation_sample.json.gz",
    )


@pytest.fixture()
def sample_otp_interactions(spark: SparkSession) -> DataFrame:
    """Sample OTP gene-gene interactions dataset."""
    return spark.read.parquet(
        "tests/data_samples/otp_interactions_sample.parquet",
    )


@pytest.fixture()
def mock_l2g_feature_matrix(spark: SparkSession) -> L2GFeatureMatrix:
    """Mock l2g feature matrix dataset."""
    schema = L2GFeatureMatrix.get_schema()

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=50,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(schema)
        .withColumnSpec("distanceTssMean", percentNulls=0.1)
        .withColumnSpec("distanceTssMinimum", percentNulls=0.1)
        .withColumnSpec("eqtlColocClppLocalMaximum", percentNulls=0.1)
        .withColumnSpec("eqtlColocClppNeighborhoodMaximum", percentNulls=0.1)
        .withColumnSpec("eqtlColocLlrLocalMaximum", percentNulls=0.1)
        .withColumnSpec("eqtlColocLlrNeighborhoodMaximum", percentNulls=0.1)
        .withColumnSpec("pqtlColocClppLocalMaximum", percentNulls=0.1)
        .withColumnSpec("pqtlColocClppNeighborhoodMaximum", percentNulls=0.1)
        .withColumnSpec("pqtlColocLlrLocalMaximum", percentNulls=0.1)
        .withColumnSpec("pqtlColocLlrNeighborhoodMaximum", percentNulls=0.1)
        .withColumnSpec("sqtlColocClppLocalMaximum", percentNulls=0.1)
        .withColumnSpec("sqtlColocClppNeighborhoodMaximum", percentNulls=0.1)
        .withColumnSpec("sqtlColocLlrLocalMaximum", percentNulls=0.1)
        .withColumnSpec("sqtlColocLlrNeighborhoodMaximum", percentNulls=0.1)
        .withColumnSpec(
            "goldStandardSet", percentNulls=0.0, values=["positive", "negative"]
        )
    )

    return L2GFeatureMatrix(_df=data_spec.build(), _schema=schema)


@pytest.fixture()
def mock_l2g_gold_standard(spark: SparkSession) -> L2GGoldStandard:
    """Mock l2g gold standard dataset."""
    schema = L2GGoldStandard.get_schema()
    data_spec = dg.DataGenerator(
        spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
    ).withSchema(schema)

    return L2GGoldStandard(_df=data_spec.build(), _schema=schema)


@pytest.fixture()
def mock_l2g_predictions(spark: SparkSession) -> L2GPrediction:
    """Mock l2g predictions dataset."""
    schema = L2GPrediction.get_schema()
    data_spec = dg.DataGenerator(
        spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
    ).withSchema(schema)

    return L2GPrediction(_df=data_spec.build(), _schema=schema)
