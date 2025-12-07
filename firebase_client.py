import json
import logging
import os
from typing import Any, Dict, List, Tuple

from google.cloud import firestore

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Service account key handling
# ---------------------------------------------------------------------------

# Where we want the key JSON file inside the container
FIREBASE_KEY_PATH = "/app/firebase-key.json"

# This env var will contain the full JSON of your service account
SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_KEY_JSON")

if SERVICE_ACCOUNT_JSON:
    try:
        # Write the JSON to /app/firebase-key.json on startup
        with open(FIREBASE_KEY_PATH, "w") as f:
            f.write(SERVICE_ACCOUNT_JSON)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = FIREBASE_KEY_PATH
        logger.info("✅ Wrote FIREBASE_KEY_JSON to %s", FIREBASE_KEY_PATH)
    except Exception as e:
        logger.exception("❌ Failed to write FIREBASE_KEY_JSON: %s", e)
else:
    logger.warning(
        "⚠️ FIREBASE_KEY_JSON env var is not set; "
        "Firestore will rely on default Google auth."
    )

# ---------------------------------------------------------------------------
# Firestore setup
# ---------------------------------------------------------------------------

# Root collection for jobs (can be overridden from env)
JOBS_COLLECTION = os.getenv("FIREBASE_JOBS_COLLECTION", "jobs")

# Firestore client (uses GOOGLE_APPLICATION_CREDENTIALS we just set)
db = firestore.Client()

# Keep this name for compatibility with main.py
jobs_collection = db.collection(JOBS_COLLECTION)
logger.info("Firestore jobs collection set to: %s", JOBS_COLLECTION)

# ---------------------------------------------------------------------------
# Fetch pending jobs
# ---------------------------------------------------------------------------

def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch pending, unclaimed jobs from Firestore.

    We stream all docs from the jobs collection and filter in Python for:
      - status == "pending"
      - claimed is missing or False
    """
    logger.info("🔍 Scanning Firestore collection '%s' for pending jobs...", JOBS_COLLECTION)

    docs = list(jobs_collection.stream())
    logger.info("🔍 Found %d document(s) total in '%s'", len(docs), JOBS_COLLECTION)

    pending: List[Tuple[str, Dict[str, Any]]] = []

    for doc in docs:
        data = doc.to_dict() or {}

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
