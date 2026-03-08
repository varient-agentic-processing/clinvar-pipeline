"""
Pulumi deploy — clinvar-pipeline

Deploys:
  - Cloud Function (Gen2): download-clinvar
      Python 3.11, 60-min timeout, ALLOW_ALL ingress (IAM/OIDC auth)
      Source zip uploaded to GCS from functions/download_clinvar/

  - Cloud Workflow: clinvar-refresh
      Source from workflows/clinvar_refresh.yaml
      Runs as pipeline-sa service account

  - Cloud Scheduler: clinvar-monthly-refresh
      Triggers clinvar-refresh on the 1st of each month at 06:00 UTC

Does NOT create base infrastructure (VPC, service accounts, buckets, Artifact
Registry, ClickHouse). Those are managed by the infra repo.

Stack config (set in Pulumi.dev.yaml):
  project_id              GCP project ID
  region                  GCP region (default: us-central1)
  pipeline_sa_email       pipeline-sa service account email
  bucket                  GCS bucket for source zip upload + pipeline data
"""
import base64
import hashlib
from pathlib import Path

import pulumi
import pulumi_gcp as gcp


def _dir_hash(directory: Path) -> str:
    """MD5 of all file contents in a directory (sorted for determinism)."""
    h = hashlib.md5()
    for f in sorted(directory.rglob("*")):
        if f.is_file():
            h.update(f.read_bytes())
    return h.hexdigest()[:12]


config = pulumi.Config()
project_id = config.require("project_id")
region = config.get("region") or "us-central1"
pipeline_sa_email = config.require("pipeline_sa_email")
bucket_name = config.require("bucket")

repo_root = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Cloud Function: download-clinvar
# ---------------------------------------------------------------------------

_fn_src = repo_root / "functions" / "download_clinvar"
source_object = gcp.storage.BucketObject(
    "download-clinvar-source",
    bucket=bucket_name,
    # Content-addressed name: changes whenever source files change, which
    # updates the Cloud Function's storage_source reference and forces rebuild.
    name=f"deploy/download_clinvar_source_{_dir_hash(_fn_src)}.zip",
    source=pulumi.FileArchive(str(_fn_src)),
)

download_clinvar_fn = gcp.cloudfunctionsv2.Function(
    "download-clinvar",
    name="download-clinvar",
    location=region,
    build_config=gcp.cloudfunctionsv2.FunctionBuildConfigArgs(
        runtime="python311",
        entry_point="download_clinvar",
        source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceArgs(
            storage_source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceStorageSourceArgs(
                bucket=bucket_name,
                object=source_object.name,
            ),
        ),
    ),
    service_config=gcp.cloudfunctionsv2.FunctionServiceConfigArgs(
        max_instance_count=3,
        available_memory="1G",        # variant_summary.txt.gz is ~200MB
        timeout_seconds=3600,
        service_account_email=pipeline_sa_email,
        # ALLOW_ALL: Cloud Workflows runs on Google managed infra (not in your
        # VPC) so ALLOW_INTERNAL_ONLY blocks it. IAM (OIDC) is the auth layer.
        ingress_settings="ALLOW_ALL",
    ),
    opts=pulumi.ResourceOptions(depends_on=[source_object]),
)

# Allow Batch VMs to pull Docker images from Artifact Registry.
gcp.projects.IAMMember(
    "clinvar-pipeline-sa-ar-reader",
    project=project_id,
    role="roles/artifactregistry.reader",
    member=f"serviceAccount:{pipeline_sa_email}",
)

# Allow the Batch agent running on the VM to report task state back to GCP.
gcp.projects.IAMMember(
    "clinvar-pipeline-sa-batch-agent-reporter",
    project=project_id,
    role="roles/batch.agentReporter",
    member=f"serviceAccount:{pipeline_sa_email}",
)

# Allow pipeline-sa to submit Cloud Batch jobs that run as pipeline-sa.
gcp.serviceaccount.IAMMember(
    "clinvar-pipeline-sa-act-as-self",
    service_account_id=f"projects/{project_id}/serviceAccounts/{pipeline_sa_email}",
    role="roles/iam.serviceAccountUser",
    member=f"serviceAccount:{pipeline_sa_email}",
)

# Allow pipeline-sa (workflow identity) to invoke the Cloud Function.
gcp.cloudrun.IamMember(
    "download-clinvar-run-invoker",
    project=project_id,
    location=region,
    service=download_clinvar_fn.name,
    role="roles/run.invoker",
    member=f"serviceAccount:{pipeline_sa_email}",
    opts=pulumi.ResourceOptions(depends_on=[download_clinvar_fn]),
)

# ---------------------------------------------------------------------------
# Cloud Workflow: clinvar-refresh
# ---------------------------------------------------------------------------

workflow_source = (repo_root / "workflows" / "clinvar_refresh.yaml").read_text()

clinvar_refresh_workflow = gcp.workflows.Workflow(
    "clinvar-refresh",
    name="clinvar-refresh",
    region=region,
    service_account=pipeline_sa_email,
    source_contents=workflow_source,
    description="End-to-end ClinVar annotation refresh: NCBI download → ClickHouse load → variant_summary enrichment",
)

# ---------------------------------------------------------------------------
# Cloud Scheduler: monthly trigger
# ---------------------------------------------------------------------------

clinvar_scheduler = gcp.cloudscheduler.Job(
    "clinvar-monthly-refresh",
    name="clinvar-monthly-refresh",
    region=region,
    description="Trigger clinvar-refresh workflow on the 1st of each month",
    schedule="0 6 1 * *",          # 06:00 UTC on the 1st
    time_zone="UTC",
    http_target=gcp.cloudscheduler.JobHttpTargetArgs(
        http_method="POST",
        uri=pulumi.Output.concat(
            "https://workflowexecutions.googleapis.com/v1/projects/",
            project_id,
            "/locations/",
            region,
            "/workflows/clinvar-refresh/executions",
        ),
        body=pulumi.Output.concat(
            '{"argument":"{\\"bucket\\":\\"', bucket_name,
            '\\",\\"project_id\\":\\"', project_id,
            '\\",\\"clickhouse_host\\":\\"10.128.0.3\\",\\"download_clinvar_url\\":\\"',
            download_clinvar_fn.service_config.uri,
            '\\"}"}'
        ).apply(lambda s: base64.b64encode(s.encode()).decode()),
        oidc_token=gcp.cloudscheduler.JobHttpTargetOidcTokenArgs(
            service_account_email=pipeline_sa_email,
            audience=pulumi.Output.concat(
                "https://workflowexecutions.googleapis.com/v1/projects/",
                project_id,
                "/locations/",
                region,
                "/workflows/clinvar-refresh/executions",
            ),
        ),
    ),
    opts=pulumi.ResourceOptions(depends_on=[clinvar_refresh_workflow, download_clinvar_fn]),
)

# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

pulumi.export("download_clinvar_url", download_clinvar_fn.service_config.uri)
pulumi.export("workflow_name", clinvar_refresh_workflow.name)
pulumi.export(
    "trigger_example",
    pulumi.Output.concat(
        "poetry run poe trigger -- --bucket ",
        bucket_name,
        " --clickhouse-host 10.128.0.3",
    ),
)
