import json
import os
from google.cloud import firestore
from google.oauth2 import service_account
from datetime import datetime, timezone

if "FIREBASE_SERVICE_ACCOUNT" not in os.environ:
    raise Exception("Missing FIREBASE_SERVICE_ACCOUNT secret")

# Load service account JSON from environment variable (as text)
service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])

credentials = service_account.Credentials.from_service_account_info(
    service_account_info
)

db = firestore.Client(
    project=service_account_info["project_id"],
    credentials=credentials
)

JOBS_COLLECTION = os.getenv("FIREBASE_JOBS_COLLECTION", "jobs")

def create_job(data):
    ref = db.collection(JOBS_COLLECTION).add(data)
    return ref[1].id

def update_job(job_id, data):
    db.collection(JOBS_COLLECTION).document(job_id).update(data)

def add_job_event(job_id, event):
    event["created_at"] = datetime.now(timezone.utc)
    db.collection(JOBS_COLLECTION).document(job_id)\
        .collection("events").add(event)

def get_pending_jobs():
    return (
        db.collection(JOBS_COLLECTION)
        .where("status", "==", "pending")
        .order_by("created_at")
        .limit(5)
        .stream()
    )
