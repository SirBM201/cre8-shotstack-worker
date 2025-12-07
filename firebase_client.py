import logging
from typing import Dict, Any, List, Tuple

from google.cloud import firestore

logger = logging.getLogger("cre8-shotstack-worker")

# -------------------------------------------------------------------
# FIRESTORE CLIENT
# -------------------------------------------------------------------

db = firestore.Client()
logger.info(f"Connected to Firestore project: {db.project}")

JOBS_COLLECTION = "jobs"
EVENTS_SUBCOLLECTION = "events"

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------


def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch jobs from the top-level 'jobs' collection where:
      - status == 'pending'
      - claimed == False

    Returns: list of (job_id, job_data)
    """

    jobs_ref = db.collection(JOBS_COLLECTION)

    query = (
        jobs_ref
        .where("status", "==", "pending")
        .where("claimed", "==", False)
        .order_by("created_at")
        .limit(limit)
    )

    docs = list(query.stream())
    logger.info("Fetched %d pending job(s) from Firestore", len(docs))

    jobs: List[Tuple[str, Dict[str, Any]]] = []

    for doc in docs:
        data = doc.to_dict() or {}
        jobs.append((doc.id, data))

        # Mark as claimed immediately so other workers don't pick it
        try:
            jobs_ref.document(doc.id).update({"claimed": True})
        except Exception as exc:
            logger.error("Error marking job %s as claimed: %s", doc.id, exc)

    return jobs


def update_job(job_id: str, fields: Dict[str, Any]) -> None:
    """
    Update a job document in the 'jobs' collection.
    Example: update_job("test-job-001", {"status": "processing"})
    """

    logger.info("Firestore update_job(%s, %s)", job_id, fields)

    jobs_ref = db.collection(JOBS_COLLECTION).document(job_id)
    jobs_ref.update(fields)


def add_event(job_id: str, event: Dict[str, Any]) -> None:
    """
    Add an event document under:
      jobs/{job_id}/events/{auto_id}
    """

    logger.info("Firestore add_event for job %s: %s", job_id, event)

    events_ref = (
        db.collection(JOBS_COLLECTION)
        .document(job_id)
        .collection(EVENTS_SUBCOLLECTION)
    )

    events_ref.add({
        **event,
        "created_at": firestore.SERVER_TIMESTAMP,
    })
