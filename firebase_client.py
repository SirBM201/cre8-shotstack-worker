import os
from typing import Dict, Any, List, Tuple
from google.cloud import firestore
from google.oauth2 import service_account

# ----------------------------------------
# FIREBASE INITIALIZATION (SAFE)
# ----------------------------------------

SERVICE_KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not SERVICE_KEY_PATH:
    raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is NOT set in environment.")

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_KEY_PATH
)

db = firestore.Client(credentials=credentials)

JOBS_COLLECTION = "jobs"

# ----------------------------------------
# FETCH PENDING JOBS
# ----------------------------------------

def get_pending_jobs(limit: int = 5) -> List[Tuple[str, Dict[str, Any]]]:
    query = (
        db.collection(JOBS_COLLECTION)
        .where("status", "==", "pending")
        .limit(limit)
    )

    docs = list(query.stream())
    results = []

    for doc in docs:
        results.append((doc.id, doc.to_dict()))

    return results

# ----------------------------------------
# UPDATE JOB
# ----------------------------------------

def update_job(job_id: str, fields: Dict[str, Any]) -> None:
    db.collection(JOBS_COLLECTION).document(job_id).update(fields)

# ----------------------------------------
# ADD EVENT
# ----------------------------------------

def add_event(job_id: str, event: Dict[str, Any]) -> None:
    db.collection(JOBS_COLLECTION).document(job_id).collection("events").add(event)
