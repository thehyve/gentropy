"""Variant index dataset."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, Window

from otg.assets import data
from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import column2camel_case
from otg.common.utils import parse_efos
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class StudyIndex(Dataset):
    """Study index dataset.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    @classmethod
    def get_schema(cls: type[StudyIndex]) -> StructType:
        """Provides the schema for the StudyIndex dataset."""
        return parse_spark_schema("studies.json")

    def study_type_lut(self: StudyIndex) -> DataFrame:
        """Return a lookup table of study type.

        Returns:
            DataFrame: A dataframe containing `studyId` and `studyType` columns.
        """
        return self.df.select("studyId", "studyType")


@dataclass
class StudyIndexGWASCatalog(StudyIndex):
    """Study index dataset from GWAS Catalog.

    The following information is harmonised from the GWAS Catalog:

    - All publication related information retained.
    - Mapped measured and background traits parsed.
    - Flagged if harmonized summary statistics datasets available.
    - If available, the ftp path to these files presented.
    - Ancestries from the discovery and replication stages are structured with sample counts.
    - Case/control counts extracted.
    - The number of samples with European ancestry extracted.

    """

    @staticmethod
    def _gwas_ancestry_to_gnomad(gwas_catalog_ancestry: Column) -> Column:
        """Normalised ancestry column from GWAS Catalog into Gnomad ancestry.

        Args:
            gwas_catalog_ancestry (Column): GWAS Catalog ancestry

        Returns:
            Column: mapped Gnomad ancestry using LUT
        """
        # GWAS Catalog to p-value mapping
        json_dict = json.loads(
            pkg_resources.read_text(
                data, "gwascat_2_gnomad_superpopulation_map.json", encoding="utf-8"
            )
        )
        map_expr = f.create_map(*[f.lit(x) for x in chain(*json_dict.items())])

        return f.transform(gwas_catalog_ancestry, lambda x: map_expr[x])

    @classmethod
    def get_schema(cls: type[StudyIndexGWASCatalog]) -> StructType:
        """Provides the schema for the StudyIndexGWASCatalog dataset.

        This method is a duplication from the parent class, but by definition, the use of abstract methods require that every child class implements them.

        Returns:
            StructType: Spark schema for the StudyIndexGWASCatalog dataset.
        """
        return parse_spark_schema("studies.json")

    @classmethod
    def _parse_study_table(
        cls: type[StudyIndexGWASCatalog], catalog_studies: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Harmonise GWASCatalog study table with `StudyIndex` schema.

        Args:
            catalog_studies (DataFrame): GWAS Catalog study table

        Returns:
            StudyIndexGWASCatalog:
        """
        return cls(
            _df=catalog_studies.select(
                f.coalesce(
                    f.col("STUDY ACCESSION"), f.monotonically_increasing_id()
                ).alias("studyId"),
                f.lit("GCST").alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.col("PUBMED ID").alias("pubmedId"),
                f.col("FIRST AUTHOR").alias("publicationFirstAuthor"),
                f.col("DATE").alias("publicationDate"),
                f.col("JOURNAL").alias("publicationJournal"),
                f.col("STUDY").alias("publicationTitle"),
                f.coalesce(f.col("DISEASE/TRAIT"), f.lit("Unreported")).alias(
                    "traitFromSource"
                ),
                f.col("INITIAL SAMPLE SIZE").alias("initialSampleSize"),
                parse_efos(f.col("MAPPED_TRAIT_URI")).alias("traitFromSourceMappedIds"),
                parse_efos(f.col("MAPPED BACKGROUND TRAIT URI")).alias(
                    "backgroundTraitFromSourceMappedIds"
                ),
            ),
            _schema=cls.get_schema(),
        )

    @classmethod
    def from_source(
        cls: type[StudyIndexGWASCatalog],
        catalog_studies: DataFrame,
        ancestry_file: DataFrame,
        sumstats_lut: DataFrame,
    ) -> StudyIndexGWASCatalog:
        """This function ingests study level metadata from the GWAS Catalog.

        Args:
            catalog_studies (DataFrame): GWAS Catalog raw study table
            ancestry_file (DataFrame): GWAS Catalog ancestry table.
            sumstats_lut (DataFrame): GWAS Catalog summary statistics list.

        Returns:
            StudyIndexGWASCatalog: Parsed and annotated GWAS Catalog study table.
        """
        # Read GWAS Catalogue raw data
        return (
            cls._parse_study_table(catalog_studies)
            ._annotate_ancestries(ancestry_file)
            ._annotate_sumstats_info(sumstats_lut)
            ._annotate_discovery_sample_sizes()
        )

    def get_gnomad_population_structure(self: StudyIndexGWASCatalog) -> DataFrame:
        """Get the population structure (ancestry normalised to gnomAD and population sizes) for every study.

        Returns:
            DataFrame: containing `studyId` and `populationsStructure`, where each element of the array represents a population
        """
        # Study ancestries
        w_study = Window.partitionBy("studyId")
        return (
            self.df
            # Excluding studies where no sample discription is provided:
            .filter(f.col("discoverySamples").isNotNull())
            # Exploding sample description and study identifier:
            .withColumn("discoverySample", f.explode(f.col("discoverySamples")))
            # Splitting sample descriptions further:
            .withColumn(
                "ancestries",
                f.split(f.col("discoverySample.ancestry"), r",\s(?![^()]*\))"),
            )
            # Dividing sample sizes assuming even distribution
            .withColumn(
                "adjustedSampleSize",
                f.col("discoverySample.sampleSize") / f.size(f.col("ancestries")),
            )
            # mapped to gnomAD superpopulation and exploded
            .withColumn(
                "population",
                f.explode(
                    StudyIndexGWASCatalog._gwas_ancestry_to_gnomad(f.col("ancestries"))
                ),
            )
            # Group by studies and aggregate for major population:
            .groupBy("studyId", "population")
            .agg(f.sum(f.col("adjustedSampleSize")).alias("sampleSize"))
            # Calculate proportions for each study
            .withColumn(
                "relativeSampleSize",
                f.col("sampleSize") / f.sum("sampleSize").over(w_study),
            )
            .withColumn(
                "populationStructure",
                f.struct("population", "relativeSampleSize"),
            )
            .groupBy("studyId")
            .agg(f.collect_set("populationStructure").alias("populationsStructure"))
        )

    def update_study_id(
        self: StudyIndexGWASCatalog, study_annotation: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Update studyId with a dataframe containing study.

        Args:
            study_annotation (DataFrame): Dataframe containing `updatedStudyId`, `traitFromSource`, `traitFromSourceMappedIds` and key column `studyId`.

        Returns:
            StudyIndexGWASCatalog: Updated study table.
        """
        self.df = (
            self._df.join(
                study_annotation.select(
                    *[
                        f.col(c).alias(f"updated{c}")
                        if c not in ["studyId", "updatedStudyId"]
                        else f.col(c)
                        for c in study_annotation.columns
                    ]
                ),
                on="studyId",
                how="left",
            )
            .withColumn(
                "studyId",
                f.coalesce(f.col("updatedStudyId"), f.col("studyId")),
            )
            .withColumn(
                "traitFromSource",
                f.coalesce(f.col("updatedtraitFromSource"), f.col("traitFromSource")),
            )
            .withColumn(
                "traitFromSourceMappedIds",
                f.coalesce(
                    f.col("updatedtraitFromSourceMappedIds"),
                    f.col("traitFromSourceMappedIds"),
                ),
            )
            .select(self._df.columns)
        )

        return self

    def _annotate_ancestries(
        self: StudyIndexGWASCatalog, ancestry_lut: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Extracting sample sizes and ancestry information.

        This function parses the ancestry data. Also get counts for the europeans in the same
        discovery stage.

        Args:
            ancestry_lut (DataFrame): Ancestry table as downloaded from the GWAS Catalog

        Returns:
            StudyIndexGWASCatalog: Slimmed and cleaned version of the ancestry annotation.
        """
        ancestry = (
            ancestry_lut
            # Convert column headers to camelcase:
            .transform(
                lambda df: df.select(
                    *[f.expr(column2camel_case(x)) for x in df.columns]
                )
            ).withColumnRenamed(
                "studyAccession", "studyId"
            )  # studyId has not been split yet
        )

        # Get a high resolution dataset on experimental stage:
        ancestry_stages = (
            ancestry.groupBy("studyId")
            .pivot("stage")
            .agg(
                f.collect_set(
                    f.struct(
                        f.col("numberOfIndividuals").alias("sampleSize"),
                        f.col("broadAncestralCategory").alias("ancestry"),
                    )
                )
            )
            .withColumnRenamed("initial", "discoverySamples")
            .withColumnRenamed("replication", "replicationSamples")
            .persist()
        )

        # Generate information on the ancestry composition of the discovery stage, and calculate
        # the proportion of the Europeans:
        europeans_deconvoluted = (
            ancestry
            # Focus on discovery stage:
            .filter(f.col("stage") == "initial")
            # Sorting ancestries if European:
            .withColumn(
                "ancestryFlag",
                # Excluding finnish:
                f.when(
                    f.col("initialSampleDescription").contains("Finnish"),
                    f.lit("other"),
                )
                # Excluding Icelandic population:
                .when(
                    f.col("initialSampleDescription").contains("Icelandic"),
                    f.lit("other"),
                )
                # Including European ancestry:
                .when(f.col("broadAncestralCategory") == "European", f.lit("european"))
                # Exclude all other population:
                .otherwise("other"),
            )
            # Grouping by study accession and initial sample description:
            .groupBy("studyId")
            .pivot("ancestryFlag")
            .agg(
                # Summarizing sample sizes for all ancestries:
                f.sum(f.col("numberOfIndividuals"))
            )
            # Do arithmetics to make sure we have the right proportion of european in the set:
            .withColumn(
                "initialSampleCountEuropean",
                f.when(f.col("european").isNull(), f.lit(0)).otherwise(
                    f.col("european")
                ),
            )
            .withColumn(
                "initialSampleCountOther",
                f.when(f.col("other").isNull(), f.lit(0)).otherwise(f.col("other")),
            )
            .withColumn(
                "initialSampleCount",
                f.col("initialSampleCountEuropean") + f.col("other"),
            )
            .drop(
                "european",
                "other",
                "initialSampleCount",
                "initialSampleCountEuropean",
                "initialSampleCountOther",
            )
        )

        parsed_ancestry_lut = ancestry_stages.join(
            europeans_deconvoluted, on="studyId", how="outer"
        )

        self.df = self.df.join(parsed_ancestry_lut, on="studyId", how="left")
        return self

    def _annotate_sumstats_info(
        self: StudyIndexGWASCatalog, sumstats_lut: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Annotate summary stat locations.

        Args:
            sumstats_lut (DataFrame): listing GWAS Catalog summary stats paths

        Returns:
            StudyIndexGWASCatalog: including `summarystatsLocation` and `hasSumstats` columns
        """
        gwas_sumstats_base_uri = (
            "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/"
        )

        parsed_sumstats_lut = sumstats_lut.withColumn(
            "summarystatsLocation",
            f.concat(
                f.lit(gwas_sumstats_base_uri),
                f.regexp_replace(f.col("_c0"), r"^\.\/", ""),
            ),
        ).select(
            f.regexp_extract(f.col("summarystatsLocation"), r"\/(GCST\d+)\/", 1).alias(
                "studyId"
            ),
            "summarystatsLocation",
            f.lit(True).alias("hasSumstats"),
        )

        self.df = (
            self.df.drop("hasSumstats")
            .join(parsed_sumstats_lut, on="studyId", how="left")
            .withColumn("hasSumstats", f.coalesce(f.col("hasSumstats"), f.lit(False)))
        )
        return self

    def _annotate_discovery_sample_sizes(
        self: StudyIndexGWASCatalog,
    ) -> StudyIndexGWASCatalog:
        """Extract the sample size of the discovery stage of the study as annotated in the GWAS Catalog.

        For some studies that measure quantitative traits, nCases and nControls can't be extracted. Therefore, we assume these are 0.

        Returns:
            StudyIndexGWASCatalog: object with columns `nCases`, `nControls`, and `nSamples` per `studyId` correctly extracted.
        """
        sample_size_lut = (
            self.df.select(
                "studyId",
                f.explode_outer(f.split(f.col("initialSampleSize"), r",\s+")).alias(
                    "samples"
                ),
            )
            # Extracting the sample size from the string:
            .withColumn(
                "sampleSize",
                f.regexp_extract(
                    f.regexp_replace(f.col("samples"), ",", ""), r"[0-9,]+", 0
                ).cast(t.IntegerType()),
            )
            .select(
                "studyId",
                "sampleSize",
                f.when(f.col("samples").contains("cases"), f.col("sampleSize"))
                .otherwise(f.lit(0))
                .alias("nCases"),
                f.when(f.col("samples").contains("controls"), f.col("sampleSize"))
                .otherwise(f.lit(0))
                .alias("nControls"),
            )
            # Aggregating sample sizes for all ancestries:
            .groupBy("studyId")  # studyId has not been split yet
            .agg(
                f.sum("nCases").alias("nCases"),
                f.sum("nControls").alias("nControls"),
                f.sum("sampleSize").alias("nSamples"),
            )
        )
        self.df = self.df.join(sample_size_lut, on="studyId", how="left")
        return self


@dataclass
class StudyIndexFinnGen(StudyIndex):
    """Study index dataset from FinnGen.

    The following information is aggregated/extracted:

    - Study ID in the special format (FINNGEN_R9_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location

    Some fields are also populated as constants, such as study type and the initial sample size.
    """

    @classmethod
    def get_schema(cls: type[StudyIndexFinnGen]) -> StructType:
        """Provides the schema for the StudyIndexFinnGen dataset.

        This method is a duplication from the parent class, but by definition, the use of abstract methods require that every child class implements them.

        Returns:
            StructType: Spark schema for the StudyIndexFinnGen dataset.
        """
        return parse_spark_schema("studies.json")

    @classmethod
    def from_source(
        cls: type[StudyIndexFinnGen],
        finngen_studies: DataFrame,
        finngen_release_prefix: str,
        finngen_sumstat_url_prefix: str,
        finngen_sumstat_url_suffix: str,
    ) -> StudyIndexFinnGen:
        """This function ingests study level metadata from FinnGen.

        Args:
            finngen_studies (DataFrame): FinnGen raw study table
            finngen_release_prefix (str): Release prefix pattern.
            finngen_sumstat_url_prefix (str): URL prefix for summary statistics location.
            finngen_sumstat_url_suffix (str): URL prefix suffix for summary statistics location.

        Returns:
            StudyIndexFinnGen: Parsed and annotated FinnGen study table.
        """
        return cls(
            _df=finngen_studies.select(
                f.concat(f.lit(f"{finngen_release_prefix}_"), f.col("phenocode")).alias(
                    "studyId"
                ),
                f.col("phenostring").alias("traitFromSource"),
                f.col("num_cases").alias("nCases"),
                f.col("num_controls").alias("nControls"),
                f.lit(finngen_release_prefix).alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.lit(True).alias("hasSumstats"),
                f.lit("377,277 (210,870 females and 166,407 males)").alias(
                    "initialSampleSize"
                ),
            )
            .withColumn("nSamples", f.col("nCases") + f.col("nControls"))
            .withColumn(
                "summarystatsLocation",
                f.concat(
                    f.lit(finngen_sumstat_url_prefix),
                    f.col("studyId"),
                    f.lit(finngen_sumstat_url_suffix),
                ),
            ),
            _schema=cls.get_schema(),
        )


@dataclass
class StudyIndexUKBiobank(StudyIndex):
    """Study index dataset from UKBiobank.

    The following information is extracted:

    - studyId
    - pubmedId
    - publicationDate
    - publicationJournal
    - publicationTitle
    - publicationFirstAuthor
    - traitFromSource
    - ancestry_discoverySamples
    - ancestry_replicationSamples
    - initialSampleSize
    - nCases
    - replicationSamples

    Some fields are populated as constants, such as projectID, studyType, and initial sample size.
    """

    @classmethod
    def get_schema(cls: type[StudyIndexUKBiobank]) -> StructType:
        """Provides the schema for the StudyIndexFinnGen dataset.

        This method is a duplication from the parent class, but by definition, the use of abstract methods require that every child class implements them.

        Returns:
            StructType: Spark schema for the StudyIndexUKBiobank dataset.
        """
        return parse_spark_schema("studies.json")

    @classmethod
    def from_source(
        cls: type[StudyIndexUKBiobank],
        ukbiobank_studies: DataFrame,
    ) -> StudyIndexUKBiobank:
        """This function ingests study level metadata from UKBiobank.

        The University of Michigan SAIGE analysis (N=1281) utilized PheCode derived phenotypes and a novel method that ensures accurate P values, even with highly unbalanced case-control ratios (Zhou et al., 2018).

        The Neale lab Round 2 study (N=2139) used GWAS with imputed genotypes from HRC to analyze all data fields in UK Biobank, excluding ICD-10 related traits to reduce overlap with the SAIGE results.

        Args:
            ukbiobank_studies (DataFrame): UKBiobank study manifest file loaded in spark session.

        Returns:
            StudyIndexUKBiobank: Annotated UKBiobank study table.
        """
        return cls(
            _df=(
                ukbiobank_studies.select(
                    f.col("code").alias("studyId"),
                    f.lit("UKBiobank").alias("projectId"),
                    f.lit("gwas").alias("studyType"),
                    f.col("trait").alias("traitFromSource"),
                    # Make publication and ancestry schema columns.
                    f.when(f.col("code").startswith("SAIGE_"), "30104761").alias(
                        "pubmedId"
                    ),
                    f.when(
                        f.col("code").startswith("SAIGE_"),
                        "Efficiently controlling for case-control imbalance and sample relatedness in large-scale genetic association studies",
                    )
                    .otherwise(None)
                    .alias("publicationTitle"),
                    f.when(f.col("code").startswith("SAIGE_"), "Wei Zhou").alias(
                        "publicationFirstAuthor"
                    ),
                    f.when(f.col("code").startswith("NEALE2_"), "2018-08-01")
                    .otherwise("2018-10-24")
                    .alias("publicationDate"),
                    f.when(f.col("code").startswith("SAIGE_"), "Nature Genetics").alias(
                        "publicationJournal"
                    ),
                    f.col("n_total").cast("string").alias("initialSampleSize"),
                    f.col("n_cases").cast("long").alias("nCases"),
                    f.array(
                        f.struct(
                            f.col("n_total").cast("string").alias("sampleSize"),
                            f.concat(f.lit("European="), f.col("n_total")).alias(
                                "ancestry"
                            ),
                        )
                    ).alias("discoverySamples"),
                    f.col("in_path").alias("summarystatsLocation"),
                    f.lit(True).alias("hasSumstats"),
                ).withColumn(
                    "traitFromSource",
                    f.when(
                        f.col("traitFromSource").contains(":"),
                        f.concat(
                            f.initcap(
                                f.split(f.col("traitFromSource"), ": ").getItem(1)
                            ),
                            f.lit(" | "),
                            f.lower(f.split(f.col("traitFromSource"), ": ").getItem(0)),
                        ),
                    ).otherwise(f.col("traitFromSource")),
                )
            ),
            _schema=cls.get_schema(),
        )