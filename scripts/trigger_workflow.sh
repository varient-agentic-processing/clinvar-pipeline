#!/usr/bin/env bash
#
# Manually trigger the clinvar-refresh Cloud Workflow.
#
# The download-clinvar Cloud Function URL is fetched automatically from gcloud
# (or can be passed explicitly with --download-clinvar-url).
#
set -euo pipefail

PROJECT_ID="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="us-central1"
BUCKET=""
CLICKHOUSE_HOST=""
DOWNLOAD_CLINVAR_URL=""
PIPELINE_VERSION="v2"
CLINVAR_PREFIX="raw/clinvar"
FORCE_DOWNLOAD=false
FORCE_LOAD=false
FORCE_ENRICH=false
DRY_RUN=false

usage() {
  cat <<EOF
Usage: $0 --bucket BUCKET --clickhouse-host HOST [OPTIONS]

Trigger the clinvar-refresh Cloud Workflow.

Required:
  --bucket NAME          GCS bucket name (no gs:// prefix)
  --clickhouse-host IP   ClickHouse internal IP (e.g. 10.128.0.3)

Options:
  --download-clinvar-url URL  Cloud Function URL (auto-detected from gcloud if not set)
  --pipeline-version TAG      Pipeline version tag written to Firestore (default: v2)
  --clinvar-prefix PREFIX     GCS prefix for ClinVar files (default: raw/clinvar)
  --force-download            Re-download even if files already exist in GCS
  --force-load                Re-load VCF even if Firestore record exists
  --force-enrich              Re-enrich even if Firestore record exists
  -p, --project ID            GCP project ID (default: from gcloud config)
  -r, --region REGION         GCP region (default: us-central1)
  -n, --dry-run               Print workflow data without executing
  -h, --help                  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)               BUCKET="$2";               shift 2 ;;
    --clickhouse-host)      CLICKHOUSE_HOST="$2";      shift 2 ;;
    --download-clinvar-url) DOWNLOAD_CLINVAR_URL="$2"; shift 2 ;;
    --pipeline-version)     PIPELINE_VERSION="$2";     shift 2 ;;
    --clinvar-prefix)       CLINVAR_PREFIX="$2";       shift 2 ;;
    --force-download)       FORCE_DOWNLOAD=true;       shift   ;;
    --force-load)           FORCE_LOAD=true;           shift   ;;
    --force-enrich)         FORCE_ENRICH=true;         shift   ;;
    -p|--project)           PROJECT_ID="$2";           shift 2 ;;
    -r|--region)            REGION="$2";               shift 2 ;;
    -n|--dry-run)           DRY_RUN=true;              shift   ;;
    -h|--help)              usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

[[ -z "$BUCKET"          ]] && { echo "Error: --bucket is required" >&2; exit 1; }
[[ -z "$CLICKHOUSE_HOST" ]] && { echo "Error: --clickhouse-host is required" >&2; exit 1; }
[[ -z "$PROJECT_ID"      ]] && { echo "Error: No project ID. Set GCP_PROJECT or use -p/--project" >&2; exit 1; }

# Resolve Cloud Function URL if not provided
if [[ -z "$DOWNLOAD_CLINVAR_URL" ]]; then
  echo "Resolving download-clinvar Cloud Function URL..."
  DOWNLOAD_CLINVAR_URL=$(gcloud functions describe download-clinvar \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --gen2 \
    --format="value(serviceConfig.uri)" 2>/dev/null || true)

  if [[ -z "$DOWNLOAD_CLINVAR_URL" ]]; then
    echo "Error: Could not resolve download-clinvar URL from gcloud." >&2
    echo "Deploy the function first with 'poe deploy', or pass --download-clinvar-url explicitly." >&2
    exit 1
  fi
  echo "  URL: $DOWNLOAD_CLINVAR_URL"
fi

WORKFLOW_DATA=$(cat <<EOF
{
  "bucket": "${BUCKET}",
  "project_id": "${PROJECT_ID}",
  "clickhouse_host": "${CLICKHOUSE_HOST}",
  "download_clinvar_url": "${DOWNLOAD_CLINVAR_URL}",
  "pipeline_version": "${PIPELINE_VERSION}",
  "clinvar_prefix": "${CLINVAR_PREFIX}",
  "force_download": ${FORCE_DOWNLOAD},
  "force_load": ${FORCE_LOAD},
  "force_enrich": ${FORCE_ENRICH}
}
EOF
)

echo ""
echo "Workflow input:"
echo "$WORKFLOW_DATA" | python3 -m json.tool 2>/dev/null || echo "$WORKFLOW_DATA"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[dry-run] would run: gcloud workflows run clinvar-refresh --project=$PROJECT_ID --location=$REGION --data=..."
  exit 0
fi

echo "Triggering clinvar-refresh workflow..."
gcloud workflows run clinvar-refresh \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --data="$WORKFLOW_DATA"
