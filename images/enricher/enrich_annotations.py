#!/usr/bin/env python3
"""
Enrich ClickHouse annotations table with ClinVar variant_summary TSV data.

Downloads variant_summary.txt.gz from GCS, loads GRCh38 rows into a ClickHouse
Memory staging table, then re-inserts enriched annotation rows using a JOIN
query. ReplacingMergeTree deduplicates on (chromosome, position, ref, alt)
after OPTIMIZE TABLE annotations FINAL.

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
    python enrich_annotations.py --bucket BUCKET --host HOST --project PROJECT
"""
import argparse
import csv
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
STAGING_TABLE = "_variant_summary_staging"


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
    if chrom == "na":
        return ""
    return f"chr{chrom}"


def parse_date_safe(date_str: str) -> date:
    if not date_str or date_str == "-":
        return date(2000, 1, 1)
    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return date(2000, 1, 1)


def parse_variant_summary(tsv_path: Path):
    """Yield enrichment dicts from variant_summary.txt.gz, filtered to GRCh38 + standard chroms."""
    with gzip.open(tsv_path, "rt") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            if row.get("Assembly") != "GRCh38":
                continue

            chrom = add_chr_prefix(row.get("Chromosome", ""))
            if not chrom or chrom not in STANDARD_CHROMOSOMES:
                continue

            start = row.get("Start", "")
            if not start or not start.isdigit():
                continue

            yield {
                "chromosome": chrom,
                "position": int(start),
                "gene_symbol": row.get("GeneSymbol", ""),
                "condition_name": row.get("PhenotypeList", ""),
                "review_status": row.get("ReviewStatus", "").replace("_", " "),
                "clinvar_last_evaluated": parse_date_safe(row.get("LastEvaluated", "")),
            }


def create_staging_table(ch: CHClient) -> None:
    ch.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
    ch.execute(f"""
        CREATE TABLE {STAGING_TABLE} (
            chromosome              LowCardinality(String),
            position                UInt32,
            gene_symbol             LowCardinality(String),
            condition_name          String,
            review_status           LowCardinality(String),
            clinvar_last_evaluated  Date
        ) ENGINE = Memory
    """)


def load_staging(ch: CHClient, tsv_path: Path) -> int:
    batch = []
    total = 0

    for record in parse_variant_summary(tsv_path):
        batch.append(record)
        total += 1

        if len(batch) >= BATCH_SIZE:
            ch.execute(f"INSERT INTO {STAGING_TABLE} VALUES", batch)
            print(f"  staging: {total:,} rows...")
            batch = []

    if batch:
        ch.execute(f"INSERT INTO {STAGING_TABLE} VALUES", batch)

    return total


def enrich_via_join(ch: CHClient) -> int:
    """
    Re-insert annotation rows with enriched fields from the staging table.

    ReplacingMergeTree deduplicates on (chromosome, position, ref, alt) — the
    re-inserted rows carry updated gene_symbol, condition_name, review_status,
    and clinvar_last_evaluated from variant_summary. After OPTIMIZE TABLE FINAL,
    the newer (enriched) row replaces the original.
    """
    ch.execute(f"""
        INSERT INTO annotations
        SELECT
            a.chromosome,
            a.position,
            a.ref,
            a.alt,
            a.rsid,
            vs.gene_symbol,
            a.consequence,
            a.clinical_significance,
            vs.review_status,
            vs.condition_name,
            a.allele_frequency,
            a.hgvs_c,
            a.hgvs_p,
            a.clinvar_variation_id,
            vs.clinvar_last_evaluated,
            a.annotation_version
        FROM annotations a
        INNER JOIN {STAGING_TABLE} vs
            ON a.chromosome = vs.chromosome AND a.position = vs.position
    """)

    result = ch.execute(f"""
        SELECT count()
        FROM annotations a
        INNER JOIN {STAGING_TABLE} vs
            ON a.chromosome = vs.chromosome AND a.position = vs.position
    """)
    return result[0][0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich annotations with variant_summary TSV data")
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

    tsv_gcs_path = f"{args.clinvar_prefix}/variant_summary.txt.gz"
    source_tsv = f"gs://{args.bucket}/{tsv_gcs_path}"

    print(f"[{started_at.isoformat()}] ClinVar enrichment starting  run={run_id}")
    print(f"  bucket: {args.bucket}")
    print(f"  tsv:    {source_tsv}")
    print(f"  host:   {args.host}:{args.port}")

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(args.bucket)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        try:
            tsv_blob = gcs_bucket.blob(tsv_gcs_path)
            tsv_local = tmp_dir / "variant_summary.txt.gz"
            print(f"Downloading {source_tsv}...")
            tsv_blob.download_to_filename(str(tsv_local))
            size_mb = tsv_local.stat().st_size / (1024 * 1024)
            print(f"  downloaded {size_mb:.1f} MB")

            ch = CHClient(host=args.host, port=args.port, password=password, database="default")
            ch_version = ch.execute("SELECT version()")[0][0]
            print(f"Connected to ClickHouse {ch_version}")

            pre_count = ch.execute("SELECT count() FROM annotations")[0][0]
            print(f"  annotations before enrichment: {pre_count:,}")

            print("Creating staging table...")
            create_staging_table(ch)

            print("Loading variant_summary into staging...")
            staging_rows = load_staging(ch, tsv_local)
            print(f"  staging table: {staging_rows:,} rows")

            print("Enriching annotations via JOIN...")
            enriched = enrich_via_join(ch)
            print(f"  enriched rows re-inserted: {enriched:,}")

            print("Running OPTIMIZE TABLE annotations FINAL (deduplication)...")
            ch.execute("OPTIMIZE TABLE annotations FINAL")

            post_count = ch.execute("SELECT count() FROM annotations")[0][0]
            print(f"  annotations after enrichment: {post_count:,}")

            print("Dropping staging table...")
            ch.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

            record_run(
                individual_id="clinvar",
                run_id=run_id,
                stage="enrich_annotations",
                status="completed",
                started_at=started_at,
                input_path=source_tsv,
                output_path=f"clickhouse://{args.host}/default/annotations",
                record_count=enriched,
                pipeline_version=args.pipeline_version,
            )

            elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
            print(f"[{datetime.now(timezone.utc).isoformat()}] Done: {enriched:,} annotations enriched in {elapsed:.0f}s")

        except Exception as exc:
            try:
                ch_cleanup = CHClient(host=args.host, port=args.port, password=password, database="default")
                ch_cleanup.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
            except Exception:
                pass

            record_run(
                individual_id="clinvar",
                run_id=run_id,
                stage="enrich_annotations",
                status="failed",
                started_at=started_at,
                error_message=str(exc),
                pipeline_version=args.pipeline_version,
            )
            print(f"[{datetime.now(timezone.utc).isoformat()}] FAILED: {exc}", file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
