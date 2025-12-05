import os

# Ensure Google credentials are set
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    raise Exception(
        "Missing GOOGLE_APPLICATION_CREDENTIALS environment variable.\n"
        "Run:\n"
        "  setx GOOGLE_APPLICATION_CREDENTIALS \"C:\\Users\\sirbm\\Desktop\\cre8-shotstack-worker\\cre8-studio-firebase-key.json\""
    )

import os
import datetime
from typing import Dict, Any, List, Tuple

from google.cloud import firestore
from google.oauth2 import service_account
from dotenv import load_dotenv

# ---------------------------------------------------------
# Load .env so we can read FIREBASE_PROJECT_ID and path
# ---------------------------------------------------------
load_dotenv()

# Path to your service account
SERVICE_ACCOUNT_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "./firebase-service-account.json",  # fallback – same as we used before
)

if not os.path.isfile(SERVICE_ACCOUNT_PATH):
    raise RuntimeError(
        f"Service account file not found at: {SERVICE_ACCOUNT_PATH}. "
        "Make sure firebase-service-account.json is in this folder "
        "or set GOOGLE_APPLICATION_CREDENTIALS in your .env."
    )

# Build credentials from JSON
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_PATH
)

# Project ID – can come from env or from the JSON file
PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")

# Firestore client using explicit credentials
db = firestore.Client(project=PROJECT_ID, credentials=credentials)


# ============================================================
# Create a new job
# ============================================================

def create_job(data: Dict[str, Any]) -> str:
    """
    Insert a job document into 'jobs' collection.
    Firestore auto-generates the ID.
    Returns the newly-created job_id.
    """
    ref = db.collection("jobs").add(data)
    job_id = ref[1].id
    return job_id


# ============================================================
# Query for pending jobs
# ============================================================

def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Returns: list of (job_id, job_data) for jobs with status='pending',
    ordered by created_at.
    """
    jobs_ref = (
        db.collection("jobs")
        .where("status", "==", "pending")
        .order_by("created_at")
        .limit(limit)
        .stream()
    )

    result: List[Tuple[str, Dict[str, Any]]] = []
    for doc in jobs_ref:
        result.append((doc.id, doc.to_dict()))
    return result


# ============================================================
# Update job fields (partial update)
# ============================================================

def update_job(job_id: str, fields: Dict[str, Any]) -> None:
    """
    Partially update a job document.
    Example:
        update_job("abc123", {"status": "processing"})
    """
    # Always update the timestamp as well
    fields.setdefault(
        "updated_at", datetime.datetime.now(datetime.timezone.utc)
    )

    (
        db.collection("jobs")
        .document(job_id)
        .update(fields)
    )


# ============================================================
# Add event to job sub-collection
# ============================================================

def add_event(job_id: str, event: Dict[str, Any]) -> None:
    """
    Append an event under jobs/{job_id}/events/.
    Automatically adds created_at if missing.
    """
    event.setdefault(
        "created_at", datetime.datetime.now(datetime.timezone.utc)
    )

    (
        db.collection("jobs")
        .document(job_id)
        .collection("events")
        .add(event)
    )
