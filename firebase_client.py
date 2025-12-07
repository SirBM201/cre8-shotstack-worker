# firebase_client.py
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import os

from google.cloud import firestore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Name of the jobs collection (default: "jobs")
JOBS_COLLECTION = os.getenv("FIREBASE_JOBS_COLLECTION", "jobs")

# Firestore client (uses GOOGLE_APPLICATION_CREDENTIALS)
db = firestore.Client()


def _jobs_collection():
    return db.collection(JOBS_COLLECTION)


def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch jobs that are:
      - status == "pending"
      - claimed is not True (either False or missing)

    We fetch by status only, then filter claimed in Python.
    This is simpler and avoids any index issues.
    """
    logger.info("Fetching pending jobs from Firestore (status=='pending')...")

    query = _jobs_collection().where("status", "==", "pending").limit(limit)
    docs = list(query.stream())

    logger.info("Fetched %d raw pending job(s) from Firestore", len(docs))

    jobs: List[Tuple[str, Dict[str, Any]]] = []

    for doc in docs:
        data = doc.to_dict() or {}
        claimed = data.get("claimed")

        # Only process jobs that are not explicitly claimed True
        if claimed is True:
            logger.info("Skipping job %s because claimed == True", doc.id)
            continue

        jobs.append((doc.id, data))

    logger.info("Returning %d unclaimed pending job(s)", len(jobs))
    return jobs


def get_rendering_jobs(limit: int = 20) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch jobs that are:
      - status == "rendering"
      - claimed == True
      - have metadata.render_id (validated later)
    """
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
    """
    Update a job document.
    Supports nested fields via dot notation (e.g. "metadata.output_url").
    """
    logger.info("Firestore update_job(%s, %s)", job_id, data)
    _jobs_collection().document(job_id).update(data)


def add_event(job_id: str, event: Dict[str, Any]) -> None:
    """
    Append an event to the job's 'events' subcollection.
    """
    logger.info("Firestore add_event for job %s: %s", job_id, event)
    event["created_at"] = datetime.now(timezone.utc)
    _jobs_collection().document(job_id).collection("events").add(event)


def mark_job_claimed(job_id: str) -> None:
    """
    Mark a job as claimed by a worker.
    """
    update_job(job_id, {"claimed": True})


def mark_job_completed(job_id: str, output_url: str, finished_at: datetime) -> None:
    """
    Called when Shotstack render finishes successfully.
    Saves the output_url + finished time.
    """
    iso_finished = finished_at.astimezone(timezone.utc).isoformat()

    update_job(
        job_id,
        {
            "status": "completed",
            "output_path": output_url,  # easy for frontend to read
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
