import json
import logging
import os
from typing import Any, Dict, List, Optional

from google.cloud import firestore
from google.oauth2 import service_account

LOG = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
JOBS_COLLECTION = os.getenv("FIREBASE_JOBS_COLLECTION", "video_jobs")

if not PROJECT_ID:
    raise Exception("Missing GOOGLE_PROJECT_ID environment variable")

# Service account JSON is stored directly in the env var FIREBASE_SERVICE_ACCOUNT
SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT")
if not SERVICE_ACCOUNT_JSON:
    raise Exception("Missing FIREBASE_SERVICE_ACCOUNT secret")

try:
    SERVICE_ACCOUNT_INFO = json.loads(SERVICE_ACCOUNT_JSON)
except json.JSONDecodeError as e:
    raise Exception("FIREBASE_SERVICE_ACCOUNT is not valid JSON") from e

credentials = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO
)

db = firestore.Client(project=PROJECT_ID, credentials=credentials)


def create_job(payload: Dict[str, Any]) -> str:
    """
    Insert a new job document into Firestore and return its ID.
    Used by insert_job.py when you manually queue work.
    """
    collection = db.collection(JOBS_COLLECTION)
    doc_ref = collection.document()

    data = {
        **payload,
        "status": payload.get("status", "pending"),
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }

    LOG.info("Creating job in Firestore: %s", data)
    doc_ref.set(data)
    return doc_ref.id


def get_pending_jobs(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Return a list of pending jobs (status == 'pending'), oldest first.
    Each item includes an 'id' field with the document ID.
    """
    query = (
        db.collection(JOBS_COLLECTION)
        .where("status", "==", "pending")
        .order_by("created_at")
        .limit(limit)
    )

    docs = query.stream()
    jobs: List[Dict[str, Any]] = []

    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        jobs.append(data)

    LOG.info("Fetched %d pending job(s) from Firestore", len(jobs))
    return jobs


def update_job_status(
    job_id: str, status: str, extra: Optional[Dict[str, Any]] = None
) -> None:
    """
    Update the status of a job and optionally attach extra fields.
    """
    doc_ref = db.collection(JOBS_COLLECTION).document(job_id)

    update_data: Dict[str, Any] = {
        "status": status,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }
    if extra:
        update_data.update(extra)

    LOG.info("Updating job %s with %s", job_id, update_data)
    doc_ref.update(update_data)

def update_job(job_id: str, data: Dict[str, Any]) -> None:
    """
    Generic job updater used by main.py.

    If 'status' is present in data, we use update_job_status so that
    status changes are always logged with updated_at. Any other fields
    in 'data' are merged into the document.
    """
    # If the caller wants to change status, use the standard helper
    if "status" in data:
        status = data.pop("status")
        update_job_status(job_id, status, data)
        return

    # Otherwise just update arbitrary fields
    doc_ref = db.collection(JOBS_COLLECTION).document(job_id)
    update_data: Dict[str, Any] = {
        **data,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }

    LOG.info("Updating job %s with fields %s", job_id, update_data)
    doc_ref.update(update_data)

def add_event(job_id: str, event: Dict[str, Any]) -> None:
    """
    Attach an event record under a job's 'events' subcollection.
    main.py calls this to log things like 'render_submitted', 'render_failed', etc.
    """
    events_ref = (
        db.collection(JOBS_COLLECTION)
        .document(job_id)
        .collection("events")
    )

    event_data = {
        **event,
        "created_at": firestore.SERVER_TIMESTAMP,
    }

    LOG.info("Adding event for job %s: %s", job_id, event_data)
    events_ref.add(event_data)

