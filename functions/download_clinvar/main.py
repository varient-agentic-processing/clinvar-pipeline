"""
Cloud Function: download-clinvar

Downloads ClinVar files from NCBI HTTPS to GCS raw/clinvar/.
Streams directly to GCS — no local disk required.

Files downloaded:
  clinvar.vcf.gz          ClinVar VCF (GRCh38)
  clinvar.vcf.gz.tbi      Tabix index
  variant_summary.txt.gz  ClinVar variant summary TSV
  clinvar_version.txt     Release date (YYYYMMDD) written by this function

Invoked synchronously by the clinvar-refresh Cloud Workflow.
Returns when all uploads are complete.

Required body fields:
  bucket        GCS bucket name (no gs:// prefix)

Optional body fields:
  force         Re-download even if files already exist in GCS (default: false)
  clinvar_prefix  GCS prefix (default: raw/clinvar)
"""
import json
import os
import shutil
from datetime import date

import functions_framework
from google.cloud import storage

NCBI_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar"

CLINVAR_FILES = [
    ("vcf_GRCh38/clinvar.vcf.gz",              "clinvar.vcf.gz"),
    ("vcf_GRCh38/clinvar.vcf.gz.tbi",          "clinvar.vcf.gz.tbi"),
    ("tab_delimited/variant_summary.txt.gz",    "variant_summary.txt.gz"),
]


def _stream_to_gcs(url: str, gcs_bucket, gcs_path: str, force: bool) -> int:
    """Stream a URL directly to GCS. Returns bytes transferred."""
    import urllib.request

    blob = gcs_bucket.blob(gcs_path)
    if not force and blob.exists():
        size = blob.size or 0
        print(f"  already exists, skipping: gs://{gcs_bucket.name}/{gcs_path} ({size / (1024**2):.1f} MB)")
        return size

    print(f"  downloading {url} → gs://{gcs_bucket.name}/{gcs_path}")
    with urllib.request.urlopen(url, timeout=1800) as resp:
        with blob.open("wb") as gcs_file:
            shutil.copyfileobj(resp, gcs_file)

    blob.reload()
    size = blob.size or 0
    print(f"  done: {size / (1024**2):.1f} MB")
    return size


@functions_framework.http
def download_clinvar(request):
    body = request.get_json(silent=True) or {}

    bucket_name = body.get("bucket") or os.environ.get("BUCKET", "")
    if not bucket_name:
        return (json.dumps({"error": "bucket is required"}), 400, {"Content-Type": "application/json"})

    force = bool(body.get("force", False))
    clinvar_prefix = body.get("clinvar_prefix", os.environ.get("CLINVAR_PREFIX", "raw/clinvar"))

    print(f"ClinVar downloader starting")
    print(f"  bucket:  {bucket_name}")
    print(f"  prefix:  {clinvar_prefix}")
    print(f"  force:   {force}")

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(bucket_name)

    results = {}
    total_bytes = 0
    for ncbi_path, filename in CLINVAR_FILES:
        url = f"{NCBI_BASE}/{ncbi_path}"
        gcs_path = f"{clinvar_prefix}/{filename}"
        size = _stream_to_gcs(url, gcs_bucket, gcs_path, force)
        results[filename] = f"gs://{bucket_name}/{gcs_path}"
        total_bytes += size

    # Write version file with today's date (YYYYMMDD)
    version = date.today().strftime("%Y%m%d")
    version_path = f"{clinvar_prefix}/clinvar_version.txt"
    version_blob = gcs_bucket.blob(version_path)
    if force or not version_blob.exists():
        version_blob.upload_from_string(version)
        print(f"  wrote version: {version} → gs://{bucket_name}/{version_path}")

    print(f"Download complete. Total: {total_bytes / (1024**2):.1f} MB")
    return (
        json.dumps({
            "status": "ok",
            "version": version,
            "files": results,
            "total_mb": round(total_bytes / (1024**2), 1),
        }),
        200,
        {"Content-Type": "application/json"},
    )
