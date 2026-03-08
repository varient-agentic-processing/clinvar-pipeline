# clinvar-pipeline

ClinVar annotation pipeline for the Genomic Variant Platform. Downloads ClinVar data from NCBI, loads it into the ClickHouse `annotations` table, and enriches it with the `variant_summary` TSV. Runs on a monthly Cloud Scheduler trigger or on demand.

## Architecture

```
Trigger (Cloud Scheduler monthly, or manual)
  │
  ▼
┌─────────────────────────────────────────────────────────────┐
│  Cloud Workflow: clinvar-refresh                             │
│                                                             │
│  Step 1: Download ClinVar                                   │
│  Cloud Function → download-clinvar                          │
│  NCBI HTTPS → GCS raw/clinvar/                              │
│  (clinvar.vcf.gz, .tbi, variant_summary.txt.gz ~350MB)      │
│                                                             │
│  Step 2: Load ClinVar VCF                                   │
│  Cloud Batch → clinvar-loader image                         │
│  raw/clinvar/ → ClickHouse annotations table                │
│  (parse INFO fields, add chr prefix, batch insert 100k rows)│
│                                                             │
│  Step 3: Enrich with variant_summary                        │
│  Cloud Batch → clinvar-enricher image                       │
│  raw/clinvar/variant_summary.txt.gz → annotations enrichment│
│  (Memory staging table + INSERT...JOIN + OPTIMIZE FINAL)    │
│                                                             │
│  Step 4: Record completion                                  │
│  Write Firestore pipeline_runs documents                    │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- [Poetry](https://python-poetry.org/docs/#installation)
- [Pulumi CLI](https://www.pulumi.com/docs/install/)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) authenticated
- Base infrastructure deployed from the `infra` repo
- `genomic-pipeline` Artifact Registry repository exists

## Setup

```bash
cp .env.example .env
# Edit .env with your PULUMI_CONFIG_PASSPHRASE_FILE path
```

Install dependencies:

```bash
poetry install
```

Initialize Pulumi (first time only):

```bash
poetry run poe login
poetry run poe stack-init
```

## Build Docker images

```bash
poetry run poe build-all
# or individually:
poetry run poe build-loader
poetry run poe build-enricher
```

## Deploy

```bash
poetry run poe deploy
```

Deploys:
- `download-clinvar` Cloud Function (Gen2, Python 3.11, 60-min timeout, 1GB memory)
- `clinvar-refresh` Cloud Workflow
- `clinvar-monthly-refresh` Cloud Scheduler job (1st of each month, 06:00 UTC)

## Trigger manually

```bash
poetry run poe trigger -- --bucket genomic-variant-prototype-variant-processing --clickhouse-host 10.128.0.3
```

Force flags (individual steps can be forced independently):

```bash
# Re-download files from NCBI even if they exist in GCS
poetry run poe trigger -- --bucket BUCKET --clickhouse-host HOST --force-download

# Re-load VCF even if Firestore record exists
poetry run poe trigger -- --bucket BUCKET --clickhouse-host HOST --force-load

# Re-enrich even if Firestore record exists
poetry run poe trigger -- --bucket BUCKET --clickhouse-host HOST --force-enrich
```

## Idempotency

Each step checks a cache before running:

| Step | Cache check | Skip condition |
|------|-------------|----------------|
| Download | GCS object `raw/clinvar/clinvar.vcf.gz` exists | File present + `force_download=false` |
| Load | Firestore doc `pipeline_runs/clinvar-load` exists | Doc present + `force_load=false` |
| Enrich | Firestore doc `pipeline_runs/clinvar-enrich` exists | Doc present + `force_enrich=false` |

Running the workflow after a completed refresh is a no-op that returns immediately with all jobs as `skipped`.

## Verify results

```bash
gcloud compute ssh clickhouse-db --zone=us-central1-a --project=variant-processing --command="PW=\$(gcloud secrets versions access latest --secret=clickhouse-default-password --project=variant-processing) && /usr/bin/clickhouse-client --password=\$PW --query=\"SELECT count() FROM annotations FINAL\""
```

## Project structure

```
clinvar-pipeline/
├── functions/
│   └── download_clinvar/
│       ├── main.py              ← Cloud Function: NCBI → GCS streaming
│       └── requirements.txt
├── images/
│   ├── loader/
│   │   ├── Dockerfile
│   │   ├── load_clinvar.py      ← Parse ClinVar VCF → annotations table
│   │   ├── track.py             ← Firestore run tracking
│   │   └── requirements.txt
│   └── enricher/
│       ├── Dockerfile
│       ├── enrich_annotations.py ← variant_summary JOIN enrichment
│       ├── track.py
│       └── requirements.txt
├── workflows/
│   └── clinvar_refresh.yaml     ← Cloud Workflow definition
├── deploy/
│   ├── __main__.py              ← Pulumi: CF + Workflow + Scheduler
│   ├── Pulumi.yaml
│   └── Pulumi.dev.yaml
├── scripts/
│   ├── build_images.sh          ← Cloud Build image builder
│   └── trigger_workflow.sh      ← Manual workflow trigger
├── pyproject.toml               ← Poetry + Poe tasks
└── README.md
```

## Implementation status

| Phase | Description | Status |
|-------|-------------|--------|
| 3.1 | Download function (NCBI → GCS) | Done |
| 3.2 | ClinVar VCF loader (Cloud Batch) | Done |
| 3.3 | variant_summary enricher (Cloud Batch) | Done |
| 3.4 | Cloud Workflow with idempotency | Done |
| 3.5 | Pulumi deploy + Cloud Scheduler | Done |
| 3.6 | End-to-end test | Done |
