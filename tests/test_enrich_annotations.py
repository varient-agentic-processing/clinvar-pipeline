"""Unit tests for images/enricher/enrich_annotations.py pure functions."""
import gzip
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "images/enricher"))

from enrich_annotations import add_chr_prefix, parse_date_safe, parse_variant_summary  # noqa: E402


# ---------------------------------------------------------------------------
# add_chr_prefix (enricher version — has "na" special case)
# ---------------------------------------------------------------------------

class TestAddChrPrefix:
    def test_plain_autosome(self):
        assert add_chr_prefix("1") == "chr1"

    def test_plain_x(self):
        assert add_chr_prefix("X") == "chrX"

    def test_mt_becomes_chrmt(self):
        assert add_chr_prefix("MT") == "chrMT"

    def test_already_prefixed_passthrough(self):
        assert add_chr_prefix("chr1") == "chr1"

    def test_na_returns_empty(self):
        assert add_chr_prefix("na") == ""

    def test_high_autosome(self):
        assert add_chr_prefix("22") == "chr22"


# ---------------------------------------------------------------------------
# parse_date_safe
# ---------------------------------------------------------------------------

class TestParseDateSafe:
    def test_iso_format(self):
        assert parse_date_safe("2024-03-15") == date(2024, 3, 15)

    def test_slash_format(self):
        assert parse_date_safe("2023/06/01") == date(2023, 6, 1)

    def test_month_name_format(self):
        # "%b %d, %Y" e.g. "Jan 05, 2023"
        assert parse_date_safe("Jan 05, 2023") == date(2023, 1, 5)

    def test_invalid_string_falls_back(self):
        assert parse_date_safe("not-a-date") == date(2000, 1, 1)

    def test_empty_string_falls_back(self):
        assert parse_date_safe("") == date(2000, 1, 1)

    def test_dash_falls_back(self):
        assert parse_date_safe("-") == date(2000, 1, 1)

    def test_whitespace_only_falls_back(self):
        # strip() → empty string, hits `not date_str` guard after strip inside strptime loop
        # Actually "-" is caught by the `== "-"` guard; whitespace falls through strptime
        assert parse_date_safe("   ") == date(2000, 1, 1)


# ---------------------------------------------------------------------------
# parse_variant_summary (generator)
# ---------------------------------------------------------------------------

_TSV_HEADER = (
    "AlleleID\tType\tName\tGeneID\tGeneSymbol\tHGNC_ID\t"
    "ClinicalSignificance\tClinSigSimple\tLastEvaluated\tRS# (dbSNP)\t"
    "nsv/esv (dbVar)\tRCVaccession\tPhenotypeIDS\tPhenotypeList\t"
    "Origin\tOriginSimple\tAssembly\tChromosomeAccession\tChromosome\t"
    "Start\tStop\tReferenceAllele\tAlternateAllele\tCytogenetic\t"
    "ReviewStatus\tNumberSubmitters\tGuidelines\tTestedInGTR\t"
    "OtherIDs\tSubmitterCategories\tVariationID\tPositionVCF\t"
    "ReferenceAlleleVCF\tAlternateAlleleVCF\n"
)


def _make_row(
    assembly: str,
    chrom: str,
    start: str,
    gene: str = "BRCA1",
    phenotype: str = "Breast cancer",
    review: str = "criteria_provided,_single_submitter",
    last_eval: str = "2023-01-15",
) -> str:
    cols = [""] * 34
    cols[16] = assembly
    cols[18] = chrom
    cols[19] = start
    cols[4] = gene
    cols[13] = phenotype
    cols[24] = review
    cols[8] = last_eval
    return "\t".join(cols) + "\n"


def _write_tsv(tmp_path: Path, rows: list[str]) -> Path:
    tsv_path = tmp_path / "variant_summary.txt.gz"
    with gzip.open(tsv_path, "wt") as fh:
        fh.write(_TSV_HEADER)
        for row in rows:
            fh.write(row)
    return tsv_path


class TestParseVariantSummary:
    def test_grch38_row_is_yielded(self, tmp_path):
        rows = [_make_row("GRCh38", "17", "43057035")]
        tsv = _write_tsv(tmp_path, rows)
        results = list(parse_variant_summary(tsv))
        assert len(results) == 1

    def test_grch37_row_is_filtered(self, tmp_path):
        rows = [
            _make_row("GRCh37", "17", "41197694"),
            _make_row("GRCh38", "17", "43057035"),
        ]
        tsv = _write_tsv(tmp_path, rows)
        results = list(parse_variant_summary(tsv))
        assert len(results) == 1
        assert results[0]["chromosome"] == "chr17"

    def test_non_standard_chromosome_filtered(self, tmp_path):
        rows = [
            _make_row("GRCh38", "na", "1000"),   # na → "" → filtered
            _make_row("GRCh38", "17", "43057035"),
        ]
        tsv = _write_tsv(tmp_path, rows)
        results = list(parse_variant_summary(tsv))
        assert len(results) == 1

    def test_non_digit_start_filtered(self, tmp_path):
        rows = [
            _make_row("GRCh38", "17", ""),        # empty start
            _make_row("GRCh38", "17", "43057035"),
        ]
        tsv = _write_tsv(tmp_path, rows)
        results = list(parse_variant_summary(tsv))
        assert len(results) == 1

    def test_yielded_dict_shape(self, tmp_path):
        rows = [_make_row("GRCh38", "17", "43057035", gene="BRCA1", phenotype="Hereditary breast cancer", last_eval="2023-01-15")]
        tsv = _write_tsv(tmp_path, rows)
        results = list(parse_variant_summary(tsv))
        r = results[0]
        assert r["chromosome"] == "chr17"
        assert r["position"] == 43057035
        assert r["gene_symbol"] == "BRCA1"
        assert r["condition_name"] == "Hereditary breast cancer"
        assert r["clinvar_last_evaluated"] == date(2023, 1, 15)

    def test_empty_tsv_yields_nothing(self, tmp_path):
        tsv = _write_tsv(tmp_path, [])
        results = list(parse_variant_summary(tsv))
        assert results == []

    def test_multiple_grch38_rows_all_yielded(self, tmp_path):
        rows = [
            _make_row("GRCh38", "1", "100"),
            _make_row("GRCh38", "2", "200"),
            _make_row("GRCh38", "X", "300"),
            _make_row("GRCh37", "1", "100"),  # filtered
        ]
        tsv = _write_tsv(tmp_path, rows)
        results = list(parse_variant_summary(tsv))
        assert len(results) == 3

    def test_review_status_underscore_replaced(self, tmp_path):
        rows = [_make_row("GRCh38", "1", "100", review="criteria_provided,_single_submitter")]
        tsv = _write_tsv(tmp_path, rows)
        results = list(parse_variant_summary(tsv))
        assert "_" not in results[0]["review_status"]
