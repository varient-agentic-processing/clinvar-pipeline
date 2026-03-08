#!/usr/bin/env bash
#
# Build Docker images via Cloud Build and push to Artifact Registry.
# Uses Cloud Build so you don't need Docker installed locally.
#
# Each image lives in its own directory under images/:
#   images/loader/    → genomic-pipeline/clinvar-loader:TAG
#   images/enricher/  → genomic-pipeline/clinvar-enricher:TAG
#
# Prerequisites:
#   - Artifact Registry repo "genomic-pipeline" exists (managed by infra repo)
#   - Cloud Build API enabled
#
set -euo pipefail

PROJECT_ID="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
LOCATION="us-central1"
REPOSITORY="genomic-pipeline"
IMAGE_NAME=""
TAG="v1"
DRY_RUN=false

VALID_IMAGES="loader enricher"

declare -A IMAGE_REGISTRY=(
    ["loader"]="clinvar-loader"
    ["enricher"]="clinvar-enricher"
)

usage() {
  cat <<EOF
Usage: $0 --image IMAGE [OPTIONS]

Build a Docker image via Cloud Build and push to Artifact Registry.

Required:
  --image NAME         Image to build: loader | enricher

Options:
  -p, --project ID     GCP project ID (default: from gcloud config)
  -l, --location LOC   Artifact Registry location (default: us-central1)
  -t, --tag TAG        Image tag (default: v1)
  -n, --dry-run        Print commands without executing
  -h, --help           Show this help

Images and their source directories:
  loader               images/loader/    → clinvar-loader:TAG
  enricher             images/enricher/  → clinvar-enricher:TAG

The image is pushed to:
  LOCATION-docker.pkg.dev/PROJECT/genomic-pipeline/IMAGE_REGISTRY_NAME:TAG
EOF
}

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)        IMAGE_NAME="$2";  shift 2 ;;
    -p|--project)   PROJECT_ID="$2";  shift 2 ;;
    -l|--location)  LOCATION="$2";    shift 2 ;;
    -t|--tag)       TAG="$2";         shift 2 ;;
    -n|--dry-run)   DRY_RUN=true;     shift   ;;
    -h|--help)      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${PROJECT_ID:-}" ]]; then
  echo "Error: No project ID. Set GCP_PROJECT, use -p/--project, or run 'gcloud config set project PROJECT_ID'" >&2
  exit 1
fi

if [[ -z "$IMAGE_NAME" ]]; then
  echo "Error: --image is required. Valid images: $VALID_IMAGES" >&2
  exit 1
fi

REGISTRY_NAME="${IMAGE_REGISTRY[$IMAGE_NAME]:-}"
if [[ -z "$REGISTRY_NAME" ]]; then
  echo "Error: Unknown image '${IMAGE_NAME}'. Valid images: $VALID_IMAGES" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_CONTEXT="${REPO_ROOT}/images/${IMAGE_NAME}"

if [[ ! -d "$BUILD_CONTEXT" ]]; then
  echo "Error: Build context not found at ${BUILD_CONTEXT}" >&2
  exit 1
fi

if [[ ! -f "${BUILD_CONTEXT}/Dockerfile" ]]; then
  echo "Error: No Dockerfile found at ${BUILD_CONTEXT}/Dockerfile" >&2
  exit 1
fi

FULL_IMAGE="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${REGISTRY_NAME}:${TAG}"

echo "Project:  $PROJECT_ID"
echo "Image:    $FULL_IMAGE"
echo "Context:  $BUILD_CONTEXT"
[[ "$DRY_RUN" == "true" ]] && echo "(dry-run mode)"
echo ""

echo "Enabling Cloud Build API..."
run gcloud services enable cloudbuild.googleapis.com --project="$PROJECT_ID" 2>/dev/null || true

echo ""
echo "Submitting Cloud Build..."
run gcloud builds submit "${BUILD_CONTEXT}" \
  --project="$PROJECT_ID" \
  --tag="$FULL_IMAGE"

echo ""
echo "Done. Image available at: $FULL_IMAGE"
