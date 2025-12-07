# firebase_client.py
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import os

from google.cloud import firestore

logger = logging.getLogger(__name__)

# Use env var if provided, otherwise "jobs"
JOBS_COLLECTION = os.getenv("FIREBASE_JOBS_COLLECTION", "jobs")
logger.info("Firestore jobs collection set to: %s", JOBS_COLLECTION)

db = firestore.Client()


def _jobs_collection():
    return db.collection(JOBS_COLLECTION)


def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    SUPER SAFE:
    - Scan the whole jobs collection
    - Filter in Python:
        status == "pending"
        claimed is not True (False or missing)
    """
    logger.info("🔍 Scanning Firestore jobs collection for pending jobs...")

    docs = list(_jobs_collection().stream())
    logger.info("🔍 Found %d document(s) in '%s'", len(docs), JOBS_COLLECTION)

    jobs: List[Tuple[str, Dict[str, Any]]] = []

    for doc in docs:
        data = doc.to_dict() or {}
        logger.info("   • Doc %s => %s", doc.id, data)

        status = data.get("status")
        claimed = data.get("claimed")

        if status == "pending" and claimed is not True:
            logger.info("   ✅ Doc %s matches pending+unclaimed filter", doc.id)
            jobs.append((doc.id, data))
            if len(jobs) >= limit:
                break

    logger.info("➡️ Returning %d pending job(s)", len(jobs))
    return jobs


def get_rendering_jobs(limit: int = 20) -> List[Tuple[str, Dict[str, Any]]]:
    logger.info("Fetching rendering jobs from Firestore...")
    query = (
        _jobs_collection()
        .where("status", "==", "rendering")
        .where("claimed", "==", True)
        .limit(limit)
    )
    docs = list(query.stream())
    logger.info("Fetched %d rendering job(s) from Firestore", len(docs))
    return [(doc.id, doc.to_dict()) for doc in docs]


def update_job(job_id: str, data: Dict[str, Any]) -> None:
    logger.info("Firestore update_job(%s, %s)", job_id, data)
    _jobs_collection().document(job_id).update(data)


def add_event(job_id: str, event: Dict[str, Any]) -> None:
    logger.info("Firestore add_event for job %s: %s", job_id, event)
    event["created_at"] = datetime.now(timezone.utc)
    _jobs_collection().document(job_id).collection("events").add(event)


def mark_job_claimed(job_id: str) -> None:
    update_job(job_id, {"claimed": True})


def mark_job_completed(job_id: str, output_url: str, finished_at: datetime) -> None:
    iso_finished = finished_at.astimezone(timezone.utc).isoformat()
    update_job(
        job_id,
        {
            "status": "completed",
            "output_path": output_url,
            "metadata.finished_at": iso_finished,
            "metadata.output_url": output_url,
            "metadata.status": "completed",
        },
    )
    add_event(
        job_id,
        {
            "type": "completed",
            "message": f"Render completed, output_url={output_url}",
        },
    )
