import json
import logging
import os
from typing import Any, Dict, List, Tuple

from google.cloud import firestore

# Simple logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Firestore setup
# ---------------------------------------------------------------------------

# Use env var for collection name if provided, otherwise "jobs"
JOBS_COLLECTION = os.getenv("FIREBASE_JOBS_COLLECTION", "jobs")

# Firestore client (uses GOOGLE_APPLICATION_CREDENTIALS)
db = firestore.Client()

# Keep this global so any existing imports from main.py still work
jobs_collection = db.collection(JOBS_COLLECTION)
logger.info("Firestore jobs collection set to: %s", JOBS_COLLECTION)

# ---------------------------------------------------------------------------
# Pending jobs
# ---------------------------------------------------------------------------

def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch pending, unclaimed jobs from Firestore.

    To keep things simple (and avoid index issues), we:
      - Stream ALL documents in the jobs collection
      - Log each document's data
      - Filter in Python for:
            status == "pending"
            claimed is missing or False
    """
    logger.info("🔍 Scanning Firestore collection '%s' for pending jobs...", JOBS_COLLECTION)

    docs = list(jobs_collection.stream())
    logger.info("🔍 Found %d document(s) total in '%s'", len(docs), JOBS_COLLECTION)

    pending: List[Tuple[str, Dict[str, Any]]] = []

    for doc in docs:
        data = doc.to_dict() or {}

        # Log a trimmed JSON version so logs stay readable
        try:
            data_json = json.dumps(data, default=str)
        except TypeError:
            data_json = str(data)
        logger.info("   • Doc %s => %s", doc.id, data_json[:600])

        status = data.get("status")
        claimed = data.get("claimed", False)

        if status == "pending" and not claimed:
            logger.info("   ✅ Doc %s qualifies as pending + unclaimed", doc.id)
            pending.append((doc.id, data))
        else:
            logger.info(
                "   ↩︎ Doc %s skipped (status=%r, claimed=%r)",
                doc.id,
                status,
                claimed,
            )

    logger.info("✅ Returning %d pending job(s)", len(pending))
    return pending[:limit]


# ---------------------------------------------------------------------------
# Job helpers used by the worker
# ---------------------------------------------------------------------------

def claim_job(job_id: str) -> None:
    """Mark a job as claimed so other workers ignore it."""
    logger.info("Firestore claim_job(%s)", job_id)
    jobs_collection.document(job_id).set({"claimed": True}, merge=True)


def update_job(job_id: str, data: Dict[str, Any]) -> None:
    """Merge updates into a job document."""
    logger.info("Firestore update_job(%s, %s)", job_id, data)
    jobs_collection.document(job_id).set(data, merge=True)


def add_event(job_id: str, event: Dict[str, Any]) -> None:
    """Append an event to the job's 'events' subcollection."""
    logger.info("Firestore add_event for job %s: %s", job_id, event)
    events_ref = jobs_collection.document(job_id).collection("events")
    events_ref.add(
        {
            **event,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
    )
