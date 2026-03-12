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
- [poethepoet](https://github.com/nat-n/poethepoet) — task runner (`pipx install poethepoet`)
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
poe login
poe stack-init
```

## Build Docker images

```bash
poe build-all
# or individually:
poe build-loader
poe build-enricher
```

## Deploy

```bash
poe deploy
```

Deploys:
- `download-clinvar` Cloud Function (Gen2, Python 3.11, 60-min timeout, 1GB memory)
- `clinvar-refresh` Cloud Workflow
- `clinvar-monthly-refresh` Cloud Scheduler job (1st of each month, 06:00 UTC)

## Trigger manually

The easiest way is the **vap-ui Submit Pipeline → ClinVar Refresh** tab. Or via CLI:

```bash
poe trigger -- --bucket genomic-variant-prototype-variant-processing --clickhouse-host 10.128.0.3
```

## Version-aware refresh

The workflow always re-downloads all ClinVar files from NCBI. The download function reads the `##fileDate=` line from the VCF header to get the actual ClinVar release date.

After downloading, the workflow reads `loaded_version` from the `pipeline_runs/clinvar-refresh` Firestore doc and compares it to the downloaded version:

- **Same version** → returns `{"status": "up_to_date"}` immediately. No Batch jobs run.
- **New version** → runs the load and enrich Batch jobs, then writes `loaded_version` back to Firestore.

This means running the workflow twice in a row is always safe — the second run is a fast no-op.

## Verify results

```bash
gcloud compute ssh clickhouse-db --zone=us-central1-a --project=variant-processing --command="PW=\$(gcloud secrets versions access latest --secret=clickhouse-default-password --project=variant-processing) && /usr/bin/clickhouse-client --password=\$PW --query=\"SELECT count() FROM annotations FINAL\""
```

## End-to-end join query

Once both `variant-pipeline` and `clinvar-pipeline` have completed a run, use the following queries to confirm the data is consistent and the join key resolves correctly. Run these via `clickhouse-client` using the same SSH pattern shown in **Verify results** above.

**1. Row counts — confirm both pipelines produced data**

```sql
SELECT 'variants'    AS table, count() AS rows FROM variants    FINAL
UNION ALL
SELECT 'annotations' AS table, count() AS rows FROM annotations FINAL;
```

**2. Annotated pathogenic variants per individual**

The core join. Returns every variant call that has a ClinVar entry with `Pathogenic` or `Likely_pathogenic` significance, enriched with gene, condition, HGVS notation, and review status.

```sql
SELECT
    v.individual_id,
    v.chromosome,
    v.position,
    v.ref,
    v.alt,
    v.genotype,
    v.depth,
    a.gene_symbol,
    a.clinical_significance,
    a.condition_name,
    a.review_status,
    a.hgvs_c,
    a.hgvs_p,
    a.rsid,
    a.clinvar_variation_id,
    a.clinvar_last_evaluated
FROM variants AS v FINAL
INNER JOIN annotations AS a FINAL
    ON  v.chromosome = a.chromosome
    AND v.position   = a.position
    AND v.ref        = a.ref
    AND v.alt        = a.alt
WHERE a.clinical_significance IN (
    'Pathogenic',
    'Likely_pathogenic',
    'Pathogenic/Likely_pathogenic'
)
ORDER BY v.individual_id, v.chromosome, v.position
LIMIT 50;
```

**3. Per-individual summary — pathogenic hit counts by gene**

Useful for a quick overview of which individuals carry annotated pathogenic variants and in which genes.

```sql
SELECT
    v.individual_id,
    a.gene_symbol,
    a.clinical_significance,
    count() AS variant_count
FROM variants AS v FINAL
INNER JOIN annotations AS a FINAL
    ON  v.chromosome = a.chromosome
    AND v.position   = a.position
    AND v.ref        = a.ref
    AND v.alt        = a.alt
WHERE a.clinical_significance IN (
    'Pathogenic',
    'Likely_pathogenic',
    'Pathogenic/Likely_pathogenic'
)
GROUP BY v.individual_id, a.gene_symbol, a.clinical_significance
ORDER BY variant_count DESC
LIMIT 25;
```

A non-zero result from query 2 or 3 confirms that both pipelines ran successfully, the schemas are compatible, and the join key (`chromosome`, `position`, `ref`, `alt`) resolves across the two tables.

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

---

## License

[CC BY-NC 4.0](LICENSE) — © 2025 Ryan Ratcliff. Free for non-commercial use with attribution. Commercial use requires prior written consent.
