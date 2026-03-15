"""
Cloud Function: download-clinvar

Downloads ClinVar files from NCBI HTTPS to GCS raw/clinvar/.
Streams directly to GCS — no local disk I/O required.

Always re-downloads on every invocation. Reads the ##fileDate= line from
the VCF header to extract the actual ClinVar release date, which is returned
in the response and written to clinvar_version.txt. The caller (Cloud Workflow)
uses this to decide whether to proceed with loading.

Files downloaded:
  clinvar.vcf.gz          ClinVar VCF (GRCh38)
  clinvar.vcf.gz.tbi      Tabix index
  variant_summary.txt.gz  ClinVar variant summary TSV
  clinvar_version.txt     Release date (YYYY-MM-DD) from VCF ##fileDate= header

Invoked synchronously by the clinvar-refresh Cloud Workflow.
Returns when all uploads are complete.

Required body fields:
  bucket        GCS bucket name (no gs:// prefix)

Optional body fields:
  clinvar_prefix  GCS prefix (default: raw/clinvar)
"""
import gzip
import io
import json
import os
import shutil
import urllib.request
from datetime import date

import functions_framework
from google.cloud import storage

NCBI_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar"

CLINVAR_FILES = [
    ("vcf_GRCh38/clinvar.vcf.gz",              "clinvar.vcf.gz"),
    ("vcf_GRCh38/clinvar.vcf.gz.tbi",          "clinvar.vcf.gz.tbi"),
    ("tab_delimited/variant_summary.txt.gz",    "variant_summary.txt.gz"),
]


def _peek_vcf_version(url: str) -> str:
    """Return the ClinVar release date from ##fileDate= in the VCF header.

    Reads the first 512 KB of the compressed VCF (enough to cover any ClinVar
    header) and decompresses in memory to find the ##fileDate= line.
    Falls back to today's date if the line cannot be found.
    """
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            compressed = resp.read(524288)  # 512 KB
        with gzip.open(io.BytesIO(compressed), "rt", encoding="ascii", errors="replace") as gz:
            for line in gz:
                if line.startswith("##fileDate="):
                    return line.strip().split("=", 1)[1]
                if not line.startswith("#"):
                    break
    except Exception as exc:
        print(f"  Warning: could not read VCF release date: {exc}")
    return date.today().strftime("%Y-%m-%d")


def _stream_to_gcs(url: str, gcs_bucket, gcs_path: str) -> int:
    """Stream a URL directly to GCS. Returns bytes transferred."""
    blob = gcs_bucket.blob(gcs_path)
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

    clinvar_prefix = body.get("clinvar_prefix", os.environ.get("CLINVAR_PREFIX", "raw/clinvar"))

    print("ClinVar downloader starting")
    print(f"  bucket:  {bucket_name}")
    print(f"  prefix:  {clinvar_prefix}")

    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(bucket_name)

    # Peek at the VCF header to get the actual ClinVar release date before
    # streaming the full file to GCS.
    vcf_url = f"{NCBI_BASE}/vcf_GRCh38/clinvar.vcf.gz"
    print("Checking ClinVar VCF release date...")
    version = _peek_vcf_version(vcf_url)
    print(f"  ClinVar release date: {version}")

    results = {}
    total_bytes = 0
    for ncbi_path, filename in CLINVAR_FILES:
        url = f"{NCBI_BASE}/{ncbi_path}"
        gcs_path = f"{clinvar_prefix}/{filename}"
        size = _stream_to_gcs(url, gcs_bucket, gcs_path)
        results[filename] = f"gs://{bucket_name}/{gcs_path}"
        total_bytes += size

    # Write version file with the actual ClinVar release date from the VCF header.
    version_path = f"{clinvar_prefix}/clinvar_version.txt"
    gcs_bucket.blob(version_path).upload_from_string(version)
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
