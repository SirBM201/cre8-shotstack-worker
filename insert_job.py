import datetime
from firebase_client import create_job


def queue_demo_job() -> None:
    """
    Insert a simple 'demo-title' job into Firestore.
    The worker will render this using the Shotstack title template.
    """

    now = datetime.datetime.now(datetime.timezone.utc)

    job_data = {
        "video_url": "https://example.com/placeholder.mp4",  # not used for title only
        "template": "demo-title",
        "asset": {
            "type": "title",
            "text": "Cre8 Studio Test Render",
            "style": "minimal",
            "effect": "zoomIn",  # ✅ valid Shotstack effect
            "start": 0,
            "length": 5,
        },
        "status": "pending",
        "max_retries": 3,
        "metadata": {
            "source": "firebase-test",
        },
        "created_at": now,
        "updated_at": now,
    }

    job_id = create_job(job_data)
    print(f"✅ Queued demo job with ID: {job_id}")


if __name__ == "__main__":
    queue_demo_job()
