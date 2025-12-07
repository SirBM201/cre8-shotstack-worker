# main.py
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from firebase_client import (
    get_pending_jobs,
    get_rendering_jobs,
    update_job,
    add_event,
    mark_job_claimed,
    mark_job_completed,
)
from shotstack_client import submit_render, get_render_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PENDING_POLL_SECONDS = 15        # how often to look for new jobs
RENDER_STATUS_POLL_SECONDS = 60  # how often to check Shotstack


def build_render_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a Firestore job into a Shotstack render payload.

    For now we use the demo-title template and replace VIDEO_URL.
    You can extend this later for other templates.
    """
    template = job.get("template", "demo-title")
    video_url = job.get("video_url")

    payload: Dict[str, Any] = {
        "templateId": template,
        "merge": [
            {
                "find": "VIDEO_URL",
                "replace": video_url,
            }
        ],
    }

    return payload


def process_pending_jobs() -> int:
    """
    Fetch 'pending' jobs, claim them, send to Shotstack,
    and update Firestore to 'rendering'.
    """
    jobs: List[Tuple[str, Dict[str, Any]]] = get_pending_jobs(limit=5)
    if not jobs:
        logger.info("No pending jobs found.")
        return 0

    logger.info("Found %d pending job(s)", len(jobs))

    for job_id, job in jobs:
        logger.info("Processing job %s: %s", job_id, job)

        # 1. Mark job as claimed + processing
        mark_job_claimed(job_id)
        update_job(job_id, {"status": "processing"})
        add_event(
            job_id,
            {"type": "processing", "message": "Worker picked up job"},
        )

        # 2. Build payload and submit render to Shotstack
        payload = build_render_payload(job)
        render_id = submit_render(payload)

        logger.info("✅ Job %s submitted to Shotstack, render_id=%s", job_id, render_id)

        # 3. Save render_id and mark as rendering
        update_job(
            job_id,
            {
                "status": "rendering",
                "metadata.render_id": render_id,
                "metadata.status": "rendering",
            },
        )

        add_event(
            job_id,
            {
                "type": "render_submitted",
                "message": f"Render submitted to Shotstack with id {render_id}",
            },
        )

    return len(jobs)


def process_rendering_jobs() -> int:
    """
    Look for jobs already submitted to Shotstack (status='rendering')
    and update them when Shotstack is DONE or FAILED.
    """
    jobs: List[Tuple[str, Dict[str, Any]]] = get_rendering_jobs(limit=20)
    if not jobs:
        logger.info("No rendering jobs to check.")
        return 0

    logger.info("Checking %d rendering job(s) with Shotstack", len(jobs))

    updated = 0

    for job_id, job in jobs:
        metadata = job.get("metadata", {})
        render_id = metadata.get("render_id")

        if not render_id:
            logger.warning(
                "Job %s has status 'rendering' but no metadata.render_id; skipping",
                job_id,
            )
            continue

        status_info = get_render_status(render_id)
        render_status = (status_info.get("status") or "").lower()
        output_url = status_info.get("url")

        # Still in progress
        if render_status in ("queued", "fetching", "rendering"):
            logger.info("Job %s still rendering (%s)", job_id, render_status)
            continue

        # Finished successfully
        if render_status == "done" and output_url:
            logger.info(
                "Job %s render DONE, saving output_url=%s", job_id, output_url
            )
            mark_job_completed(
                job_id,
                output_url,
                finished_at=datetime.now(timezone.utc),
            )
            updated += 1
            continue

        # Failed or unknown state
        logger.warning(
            "Job %s render status is %s (no URL). Marking as failed.",
            job_id,
            render_status,
        )
        update_job(
            job_id,
            {
                "status": "failed",
                "metadata.status": render_status,
            },
        )
        add_event(
            job_id,
            {
                "type": "failed",
                "message": f"Shotstack render failed or unknown status: {render_status}",
            },
        )
        updated += 1

    return updated


def main() -> None:
    logger.info("🚀 Starting Cre8 Firebase + Shotstack worker (auto-save mode)...")

    last_render_check = 0.0

    while True:
        # 1) Handle new pending jobs
        processed = process_pending_jobs()

        # 2) Periodically check rendering jobs
        now = time.time()
        if now - last_render_check >= RENDER_STATUS_POLL_SECONDS:
            process_rendering_jobs()
            last_render_check = now

        # 3) Sleep if nothing to do
        if processed == 0:
            logger.info("No work right now. Sleeping %s seconds...", PENDING_POLL_SECONDS)
            time.sleep(PENDING_POLL_SECONDS)


if __name__ == "__main__":
    main()
