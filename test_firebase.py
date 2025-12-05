# test_firebase.py

from firebase_client import create_job, get_pending_jobs, update_job_status

def main():
    print("ENV CHECK:", os.getenv("FIREBASE_CREDENTIALS_PATH"))

    print("Creating test job in Firestore...")
    job_id = create_job(
        video_url="https://example.com/test.mp4",
        template="demo-template",
        job_type="render",
        platforms=["youtube"],
        shotstack_payload={"timeline": {}, "output": {}},
        metadata={"source": "firebase-test"},
    )
    print(f"✅ Created job with ID: {job_id}")

    print("\nFetching pending jobs...")
    jobs = get_pending_jobs(limit=5)
    print(f"Found {len(jobs)} pending job(s).")
    for doc in jobs:
        print(f"- {doc.id}: {doc.to_dict()}")

    if jobs:
        first = jobs[0]
        print(f"\nUpdating first job {first.id} to status 'completed-test'...")
        update_job_status(first.id, "completed-test", {"note": "Updated by test_firebase.py"})
        print("✅ Update sent. Check Firestore to confirm.")


if __name__ == "__main__":
    import os
    main()
