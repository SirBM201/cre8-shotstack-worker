import os
import json
import logging
from typing import Dict, Any, List, Tuple

from google.cloud import firestore
from google.oauth2 import service_account

logger = logging.getLogger("cre8-firebase")


# -------------------------------------------------------------------
# FIRESTORE CLIENT
# -------------------------------------------------------------------

# Option 1: JSON string in env (Koyeb)
#   GOOGLE_APPLICATION_CREDENTIALS_JSON = contents of the service account file
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")

if credentials_json:
    info = json.loads(credentials_json)
    creds = service_account.Credentials.from_service_account_info(info)
    project_id = info.get("project_id")
    db = firestore.Client(project=project_id, credentials=creds)
    logger.info(
        "Initialised Firestore with explicit service account JSON, project=%s",
        project_id,
    )
else:
    # Option 2: Default ADC (if you've mounted the .json file path in Koyeb)
    db = firestore.Client()
    logger.info("Initialised Firestore with default application credentials")

JOBS_COLLECTION = "jobs"


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch jobs that should be picked up by the worker.

    We look for:
      status == "pending"
      claimed == False
    ordered by created_at.
    """
    logger.info("Fetching up to %d pending job(s) from Firestore", limit)

    query = (
        db.collection(JOBS_COLLECTION)
        .where("status", "==", "pending")
        .where("claimed", "==", False)
        .order_by("created_at")
        .limit(limit)
    )

    docs = list(query.stream())
    jobs: List[Tuple[str, Dict[str, Any]]] = [(doc.id, doc.to_dict()) for doc in docs]

    logger.info("Fetched %d pending job(s) from Firestore", len(jobs))
    return jobs


def get_rendering_jobs(limit: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Fetch jobs that are currently 'rendering' at Shotstack.
    We will poll Shotstack and, when done, save the output_url.
    """
    logger.info("Fetching up to %d rendering job(s) from Firestore", limit)

    query = (
        db.collection(JOBS_COLLECTION)
        .where("status", "==", "rendering")
        .order_by("created_at")
        .limit(limit)
    )

    docs = list(query.stream())
    jobs: List[Tuple[str, Dict[str, Any]]] = [(doc.id, doc.to_dict()) for doc in docs]

    logger.info("Fetched %d rendering job(s) from Firestore", len(jobs))
    return jobs


def update_job(job_id: str, update_fields: Dict[str, Any]) -> None:
    logger.info("Firestore update_job(%s, %s)", job_id, update_fields)
    db.collection(JOBS_COLLECTION).document(job_id).update(update_fields)


def add_event(job_id: str, event: Dict[str, Any]) -> None:
    """
    Append a small log entry under jobs/{job_id}/events.
    """
    logger.info("Firestore add_event for job %s: %s", job_id, event)
    db.collection(JOBS_COLLECTION).document(job_id).collection("events").add(event)
