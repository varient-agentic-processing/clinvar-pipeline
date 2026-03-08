#!/usr/bin/env python3
"""
Load ClinVar VCF into the ClickHouse annotations table.

Downloads clinvar.vcf.gz from GCS, parses each record, adds the chr prefix
to chromosome values, filters to standard chromosomes, and batch-inserts
into the annotations table.

Designed to run as a Cloud Batch container job. All configuration is via
environment variables (set by the batch job template). CLI args are also
accepted for local testing.

Environment variables (set by Cloud Batch job template):
    BUCKET              GCS bucket name (no gs:// prefix)
    CLICKHOUSE_HOST     ClickHouse host IP or hostname
    CLICKHOUSE_PORT     ClickHouse native TCP port (default: 9000)
    CLICKHOUSE_PASSWORD Explicit password (skips Secret Manager if set)
    GCP_PROJECT         GCP project ID (for Secret Manager + Firestore)
    PIPELINE_VERSION    Pipeline version tag (default: v2)
    CLINVAR_PREFIX      GCS prefix for ClinVar files (default: raw/clinvar)

Usage (local):
    python load_clinvar.py --bucket BUCKET --host HOST --project PROJECT
"""
import argparse
import gzip
import os
import sys
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from clickhouse_driver import Client as CHClient
from google.cloud import storage

from track import record_run

BATCH_SIZE = 100_000
STANDARD_CHROMOSOMES = {f"chr{c}" for c in list(range(1, 23)) + ["X", "Y", "MT"]}


def get_clickhouse_password(project: str) -> str:
    password = os.environ.get("CLICKHOUSE_PASSWORD")
    if password:
        return password

    if not project:
        print("Error: --project is required for Secret Manager lookup (or set CLICKHOUSE_PASSWORD)", file=sys.stderr)
        sys.exit(1)

    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project}/secrets/clickhouse-default-password/versions/latest"
        return client.access_secret_version(name=name).payload.data.decode("utf-8")
    except Exception as exc:
        print(f"Could not retrieve password from Secret Manager: {exc}", file=sys.stderr)
        sys.exit(1)


def add_chr_prefix(chrom: str) -> str:
    if chrom.startswith("chr"):
        return chrom
    if chrom == "MT":
        return "chrMT"
    return f"chr{chrom}"


def parse_info(info_str: str) -> dict:
    result = {}
    for field in info_str.split(";"):
        if "=" in field:
            k, v = field.split("=", 1)
            result[k] = v
        else:
            result[field] = True
    return result


def parse_geneinfo(geneinfo: str) -> str:
    """Extract gene symbol from GENEINFO field (e.g. 'BRCA1:672|BRCA1-AS1:100379562')."""
    if not geneinfo or geneinfo == ".":
        return ""
    return geneinfo.split(":")[0]


def parse_clinvar_vcf(vcf_path: Path, annotation_version: str):
    """Yield annotation dicts from ClinVar VCF, one per record."""
    with gzip.open(vcf_path, "rt") as fh:
        for line in fh:
            if line.startswith("#"):
                continue

            cols = line.rstrip("\n").split("\t")
            if len(cols) < 8:
                continue

            chrom_raw, pos, rsid, ref, alt, _qual, _filt, info_str = cols[:8]

            chrom = add_chr_prefix(chrom_raw)
            if chrom not in STANDARD_CHROMOSOMES:
                continue

            info = parse_info(info_str)

            clnsig = info.get("CLNSIG", "")
            if not clnsig or clnsig == ".":
                continue

            alleleid = info.get("ALLELEID", "0")
            clinvar_variation_id = int(alleleid) if alleleid.isdigit() else 0

            yield {
                "chromosome": chrom,
                "position": int(pos),
                "ref": ref,
                "alt": alt,
                "rsid": rsid if rsid != "." else "",
                "gene_symbol": parse_geneinfo(info.get("GENEINFO", "")),
                "consequence": info.get("MC", "").split("|")[-1] if info.get("MC") else "",
                "clinical_significance": clnsig.replace("_", " "),
                "review_status": info.get("CLNREVSTAT", "").replace("_", " "),
                "condition_name": info.get("CLNDN", "").replace("_", " "),
                "allele_frequency": float(info.get("AF_ESP", "0") or "0"),
                "hgvs_c": "",
                "hgvs_p": "",
                "clinvar_variation_id": clinvar_variation_id,
                "clinvar_last_evaluated": date(2000, 1, 1),
                "annotation_version": annotation_version,
            }


def load_annotations(ch: CHClient, vcf_path: Path, annotation_version: str) -> int:
    batch = []
    total = 0

    for record in parse_clinvar_vcf(vcf_path, annotation_version):
        batch.append(record)
        total += 1

        if len(batch) >= BATCH_SIZE:
            ch.execute("INSERT INTO annotations VALUES", batch)
            print(f"  inserted {total:,} annotations...")
            batch = []

    if batch:
        ch.execute("INSERT INTO annotations VALUES", batch)

    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ClinVar VCF into ClickHouse annotations table")
    parser.add_argument("--bucket", default=os.environ.get("BUCKET", ""), help="GCS bucket name")
    parser.add_argument("--host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"), help="ClickHouse host")
    parser.add_argument("--port", type=int, default=int(os.environ.get("CLICKHOUSE_PORT", "9000")), help="ClickHouse native TCP port")
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT", ""), help="GCP project ID (for Secret Manager + Firestore)")
    parser.add_argument("--pipeline-version", default=os.environ.get("PIPELINE_VERSION", "v2"), help="Pipeline version tag")
    parser.add_argument("--clinvar-prefix", default=os.environ.get("CLINVAR_PREFIX", "raw/clinvar"), help="GCS prefix for ClinVar files")
    args = parser.parse_args()

    if not args.bucket:
        print("Error: --bucket is required (or set BUCKET env var)", file=sys.stderr)
        sys.exit(1)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    started_at = datetime.now(timezone.utc)
    password = get_clickhouse_password(args.project)

    vcf_gcs_path = f"{args.clinvar_prefix}/clinvar.vcf.gz"
    source_vcf = f"gs://{args.bucket}/{vcf_gcs_path}"

    print(f"[{started_at.isoformat()}] ClinVar loader starting  run={run_id}")
    print(f"  bucket: {args.bucket}")
    print(f"  vcf:    {source_vcf}")
    print(f"  host:   {args.host}:{args.port}")

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(args.bucket)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        try:
            # Read annotation version written by download function
            annotation_version = args.pipeline_version
            try:
                version_blob = gcs_bucket.blob(f"{args.clinvar_prefix}/clinvar_version.txt")
                annotation_version = version_blob.download_as_text().strip()
                print(f"  annotation version: {annotation_version}")
            except Exception:
                print("  Warning: could not read clinvar_version.txt, using pipeline_version")

            vcf_blob = gcs_bucket.blob(vcf_gcs_path)
            vcf_local = tmp_dir / "clinvar.vcf.gz"
            print(f"Downloading {source_vcf}...")
            vcf_blob.download_to_filename(str(vcf_local))
            size_mb = vcf_local.stat().st_size / (1024 * 1024)
            print(f"  downloaded {size_mb:.1f} MB")

            ch = CHClient(host=args.host, port=args.port, password=password, database="default")
            ch_version = ch.execute("SELECT version()")[0][0]
            print(f"Connected to ClickHouse {ch_version}")

            total = load_annotations(ch, vcf_local, annotation_version)

            count = ch.execute("SELECT count() FROM annotations")[0][0]

            record_run(
                individual_id="clinvar",
                run_id=run_id,
                stage="load_clinvar",
                status="completed",
                started_at=started_at,
                input_path=source_vcf,
                output_path=f"clickhouse://{args.host}/default/annotations",
                record_count=total,
                pipeline_version=annotation_version,
            )

            elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
            print(f"[{datetime.now(timezone.utc).isoformat()}] Done: {total:,} annotations loaded in {elapsed:.0f}s")
            print(f"  annotations table count: {count:,}")

        except Exception as exc:
            record_run(
                individual_id="clinvar",
                run_id=run_id,
                stage="load_clinvar",
                status="failed",
                started_at=started_at,
                error_message=str(exc),
                pipeline_version=annotation_version if "annotation_version" in dir() else "",
            )
            print(f"[{datetime.now(timezone.utc).isoformat()}] FAILED: {exc}", file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
