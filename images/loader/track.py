"""Firestore pipeline run tracking.

Writes a document per stage execution to the ``pipeline_runs`` collection.
Falls back to a console log if Firestore is unavailable (e.g. local testing).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def record_run(
    *,
    individual_id: str,
    run_id: str,
    stage: str,
    status: str,
    started_at: datetime,
    input_path: str = "",
    output_path: str = "",
    record_count: int = 0,
    error_message: str = "",
    pipeline_version: str = "",
) -> None:
    doc_id = f"{individual_id}_{stage}_{run_id}"
    completed_at: Optional[datetime] = (
        datetime.now(timezone.utc) if status in ("completed", "failed") else None
    )

    payload = {
        "individual_id": individual_id,
        "run_id": run_id,
        "stage": stage,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "input_path": input_path,
        "output_path": output_path,
        "record_count": record_count,
        "error_message": error_message,
        "pipeline_version": pipeline_version,
    }

    try:
        from google.cloud import firestore

        db = firestore.Client()
        db.collection("pipeline_runs").document(doc_id).set(payload)
        print(f"Firestore: wrote pipeline_runs/{doc_id} (status={status})")
    except Exception as exc:
        print(f"Firestore unavailable ({exc}); logging run record to stdout instead.")
        for k, v in payload.items():
            print(f"  {k}: {v}")
