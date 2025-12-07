import os
import json
import logging
from typing import Dict, Any, List, Tuple

from google.cloud import firestore
from google.oauth2 import service_account

logger = logging.getLogger("cre8-shotstack-worker")

JOBS_COLLECTION = "jobs"
EVENTS_SUBCOLLECTION = "events"


# -------------------------------------------------------------------
# FIRESTORE CLIENT (with service-account JSON)
# -------------------------------------------------------------------

def init_db() -> firestore.Client:
    """
    Initialize Firestore using a service-account JSON stored in an
    environment variable, with a safe fallback to ADC if it exists.
    """

    sa_json = (
        os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        or os.getenv("GOOGLE_CLOUD_SERVICE_ACCOUNT")
        or os.getenv("FIREBASE_SERVICE_ACCOUNT")
    )

    if sa_json:
        logger.info("Initializing Firestore from service-account JSON env var...")
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        project_id = info.get("project_id")
        client = firestore.Client(project=project_id, credentials=creds)
        logger.info(
            "Connected to Firestore project (service-account): %s",
            client.project,
        )
        return client

    # Fallback: Application Default Credentials (if running inside GCP)
    logger.info("No service-account JSON env var found, using default credentials...")
    client = firestore.Client()
    logger.info("Connected to Firestore project (ADC): %s", client.project)
    return client


db = init_db()


# -------------------------------------------------------------------
# QUERIES
# -------------------------------------------------------------------

def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch jobs from the top-level 'jobs' collection where:
      - status == 'pending'
      - claimed == False
    Mark them as claimed so multiple workers don't double-process.
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

        # Mark as claimed so another worker can't pick it
        try:
            jobs_ref.document(doc.id).update({"claimed": True})
        except Exception as exc:
            logger.error("Error marking job %s as claimed: %s", doc.id, exc)

    return jobs


def get_rendering_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch jobs where:
      - status == 'rendering'
      - metadata.render_id exists (we check later in code)
    """

    jobs_ref = db.collection(JOBS_COLLECTION)

    query = (
        jobs_ref
        .where("status", "==", "rendering")
        .limit(limit)
    )

    docs = list(query.stream())
    logger.info("Fetched %d rendering job(s) from Firestore", len(docs))

    jobs: List[Tuple[str, Dict[str, Any]]] = []

    for doc in docs:
        data = doc.to_dict() or {}
        jobs.append((doc.id, data))

    return jobs


def update_job(job_id: str, fields: Dict[str, Any]) -> None:
    logger.info("Firestore update_job(%s, %s)", job_id, fields)
    db.collection(JOBS_COLLECTION).document(job_id).update(fields)


def add_event(job_id: str, event: Dict[str, Any]) -> None:
    logger.info("Firestore add_event for job %s: %s", job_id, event)
    (
        db.collection(JOBS_COLLECTION)
        .document(job_id)
        .collection(EVENTS_SUBCOLLECTION)
        .add({**event, "created_at": firestore.SERVER_TIMESTAMP})
    )
