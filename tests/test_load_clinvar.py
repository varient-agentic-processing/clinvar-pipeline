"""Unit tests for images/loader/load_clinvar.py pure functions."""
import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "images/loader"))

from load_clinvar import add_chr_prefix, parse_geneinfo, parse_info, parse_clinvar_vcf  # noqa: E402


# ---------------------------------------------------------------------------
# add_chr_prefix
# ---------------------------------------------------------------------------

class TestAddChrPrefix:
    def test_plain_autosome(self):
        assert add_chr_prefix("1") == "chr1"

    def test_plain_x(self):
        assert add_chr_prefix("X") == "chrX"

    def test_plain_y(self):
        assert add_chr_prefix("Y") == "chrY"

    def test_mt_becomes_chrmt(self):
        assert add_chr_prefix("MT") == "chrMT"

    def test_already_prefixed_passthrough(self):
        assert add_chr_prefix("chr1") == "chr1"

    def test_already_prefixed_mt_passthrough(self):
        assert add_chr_prefix("chrMT") == "chrMT"

    def test_high_autosome(self):
        assert add_chr_prefix("22") == "chr22"


# ---------------------------------------------------------------------------
# parse_info
# ---------------------------------------------------------------------------

class TestParseInfo:
    def test_basic_key_value_pairs(self):
        info = "ALLELEID=123;CLNSIG=Pathogenic;CLNDN=Breast_cancer"
        result = parse_info(info)
        assert result["ALLELEID"] == "123"
        assert result["CLNSIG"] == "Pathogenic"
        assert result["CLNDN"] == "Breast_cancer"

    def test_flag_field_becomes_true(self):
        info = "AF_TGP=0.01;CLNVC_SO=SO:0001483;VRS"
        result = parse_info(info)
        assert result["VRS"] is True

    def test_value_with_equals_sign_preserved(self):
        # value itself contains '=' — split(=, 1) must keep it
        info = "CLNHGVS=NC_000017.11:g.43057035C>T"
        result = parse_info(info)
        assert result["CLNHGVS"] == "NC_000017.11:g.43057035C>T"

    def test_empty_info_string(self):
        result = parse_info("")
        # single empty-string key with no '=' → treated as flag
        assert result == {"": True}

    def test_multiple_fields(self):
        info = "ALLELEID=99;CLNSIG=Benign;MC=SO:0001819|synonymous_variant;GENEINFO=BRCA2:675"
        result = parse_info(info)
        assert result["ALLELEID"] == "99"
        assert result["GENEINFO"] == "BRCA2:675"
        assert result["MC"] == "SO:0001819|synonymous_variant"


# ---------------------------------------------------------------------------
# parse_geneinfo
# ---------------------------------------------------------------------------

class TestParseGeneinfo:
    def test_single_gene(self):
        assert parse_geneinfo("BRCA1:672") == "BRCA1"

    def test_multi_gene_returns_first(self):
        assert parse_geneinfo("BRCA1:672|BRCA1-AS1:100379562") == "BRCA1"

    def test_dot_returns_empty(self):
        assert parse_geneinfo(".") == ""

    def test_empty_string_returns_empty(self):
        assert parse_geneinfo("") == ""

    def test_none_returns_empty(self):
        assert parse_geneinfo(None) == ""  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# parse_clinvar_vcf (generator)
# ---------------------------------------------------------------------------

_VCF_HEADER = """\
##fileformat=VCFv4.1
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description="ClinVar Allele ID">
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease name">
##INFO=<ID=GENEINFO,Number=1,Type=String,Description="Gene+ID">
##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="Review status">
##INFO=<ID=MC,Number=.,Type=String,Description="Molecular consequence">
##INFO=<ID=AF_ESP,Number=1,Type=Float,Description="Allele freq ESP">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
"""

_VCF_RECORDS = [
    # standard pathogenic record — should be yielded
    "17\t43057035\t123456\tC\tT\t.\t.\tALLELEID=99;CLNSIG=Pathogenic;CLNDN=Breast-ovarian_cancer;GENEINFO=BRCA1:672;CLNREVSTAT=criteria_provided,_single_submitter;MC=SO:0001819|synonymous_variant;AF_ESP=0.001\n",
    # no CLNSIG — should be skipped
    "13\t32315474\t.\tG\tA\t.\t.\tALLELEID=200\n",
    # non-standard chromosome — should be skipped
    "GL000220.1\t1000\t.\tA\tG\t.\t.\tALLELEID=300;CLNSIG=Uncertain_significance\n",
    # second valid record
    "13\t32315474\t789\tG\tA\t.\t.\tALLELEID=42;CLNSIG=Benign;CLNDN=Hereditary_breast_and_ovarian_cancer;GENEINFO=BRCA2:675;CLNREVSTAT=reviewed_by_expert_panel\n",
]


def _write_vcf(tmp_path: Path, records: list[str]) -> Path:
    vcf_path = tmp_path / "clinvar.vcf.gz"
    with gzip.open(vcf_path, "wt") as fh:
        fh.write(_VCF_HEADER)
        for rec in records:
            fh.write(rec)
    return vcf_path


class TestParseClinvarVcf:
    def test_yields_only_valid_records(self, tmp_path):
        vcf = _write_vcf(tmp_path, _VCF_RECORDS)
        results = list(parse_clinvar_vcf(vcf, "2024-01-01"))
        assert len(results) == 2

    def test_first_record_fields(self, tmp_path):
        vcf = _write_vcf(tmp_path, _VCF_RECORDS)
        results = list(parse_clinvar_vcf(vcf, "v2024"))
        r = results[0]
        assert r["chromosome"] == "chr17"
        assert r["position"] == 43057035
        assert r["ref"] == "C"
        assert r["alt"] == "T"
        assert r["rsid"] == "123456"
        assert r["gene_symbol"] == "BRCA1"
        assert r["clinical_significance"] == "Pathogenic"
        assert r["condition_name"] == "Breast-ovarian cancer"
        assert r["clinvar_variation_id"] == 99
        assert r["annotation_version"] == "v2024"
        assert abs(r["allele_frequency"] - 0.001) < 1e-9

    def test_second_record_benign(self, tmp_path):
        vcf = _write_vcf(tmp_path, _VCF_RECORDS)
        results = list(parse_clinvar_vcf(vcf, "v2"))
        r = results[1]
        assert r["chromosome"] == "chr13"
        assert r["clinical_significance"] == "Benign"
        assert r["gene_symbol"] == "BRCA2"

    def test_rsid_dot_becomes_empty(self, tmp_path):
        records = [
            "1\t100\t.\tA\tG\t.\t.\tALLELEID=1;CLNSIG=Pathogenic\n",
        ]
        vcf = _write_vcf(tmp_path, records)
        results = list(parse_clinvar_vcf(vcf, "v1"))
        assert results[0]["rsid"] == ""

    def test_consequence_parsed_from_mc_field(self, tmp_path):
        records = [
            "1\t100\t.\tA\tG\t.\t.\tALLELEID=1;CLNSIG=Pathogenic;MC=SO:0001819|synonymous_variant\n",
        ]
        vcf = _write_vcf(tmp_path, records)
        results = list(parse_clinvar_vcf(vcf, "v1"))
        assert results[0]["consequence"] == "synonymous_variant"

    def test_empty_vcf_yields_nothing(self, tmp_path):
        vcf = _write_vcf(tmp_path, [])
        results = list(parse_clinvar_vcf(vcf, "v1"))
        assert results == []

    def test_line_with_too_few_columns_skipped(self, tmp_path):
        records = [
            "1\t100\t.\tA\n",  # only 4 cols
            "2\t200\t.\tC\tT\t.\t.\tALLELEID=5;CLNSIG=Pathogenic\n",
        ]
        vcf = _write_vcf(tmp_path, records)
        results = list(parse_clinvar_vcf(vcf, "v1"))
        assert len(results) == 1
        assert results[0]["chromosome"] == "chr2"
