import os
import time
import logging
from typing import Dict, Any, List, Tuple

import requests
from dotenv import load_dotenv

from firebase_client import (
    get_pending_jobs,
    update_job,
    add_event,
)

# -------------------------------------------------------------------
# ENV + LOGGING
# -------------------------------------------------------------------

load_dotenv()

SHOTSTACK_API_URL = os.getenv(
    "SHOTSTACK_API_URL",
    "https://api.shotstack.io/stage/render",  # stage by default
)
SHOTSTACK_API_KEY = os.getenv("SHOTSTACK_API_KEY")

if not SHOTSTACK_API_KEY:
    raise RuntimeError("SHOTSTACK_API_KEY is not set in .env")

HEADERS = {
    "x-api-key": SHOTSTACK_API_KEY,
    "Content-Type": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("cre8-shotstack-worker")

# Valid Shotstack effects (from the API error message)
VALID_EFFECTS = {
    "none",
    "zoomIn",
    "zoomInSlow",
    "zoomInFast",
    "zoomOut",
    "zoomOutSlow",
    "zoomOutFast",
    "slideLeft",
    "slideLeftSlow",
    "slideLeftFast",
    "slideRight",
    "slideRightSlow",
    "slideRightFast",
    "slideUp",
    "slideUpSlow",
    "slideUpFast",
    "slideDown",
    "slideDownSlow",
    "slideDownFast",
}

# -------------------------------------------------------------------
# TEMPLATE BUILDERS
# -------------------------------------------------------------------


def build_demo_title_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple 5-second title template.

    Reads job["asset"]:
      - text: title text
      - style: Shotstack title style (e.g. "minimal")
      - length: seconds (default 5)
      - effect: one of VALID_EFFECTS (fallback to "zoomIn")
    """

    asset_cfg = job.get("asset", {}) or {}

    text = asset_cfg.get("text", "Cre8 Studio Test Render")
    style = asset_cfg.get("style", "minimal")
    length = float(asset_cfg.get("length", 5))
    start = float(asset_cfg.get("start", 0))

    requested_effect = asset_cfg.get("effect", "zoomIn")
    effect = requested_effect if requested_effect in VALID_EFFECTS else "zoomIn"

    timeline = {
        "tracks": [
            {
                "clips": [
                    {
                        "asset": {
                            "type": "title",
                            "text": text,
                            "style": style,
                        },
                        "start": start,
                        "length": length,
                        "effect": effect,
                    }
                ]
            }
        ]
    }

    output = {
        "format": "mp4",
        "resolution": "hd",
        "aspectRatio": "16:9",
    }

    payload = {
        "timeline": timeline,
        "output": output,
    }

    return payload


def build_shotstack_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Entry point to choose which template to use.
    For now we only have 'demo-title', but this is ready
    for future sports, movie-review, etc.
    """

    template = job.get("template", "demo-title")

    if template == "demo-title":
        return build_demo_title_payload(job)

    # Default: fall back to demo-title so jobs never break
    logger.warning("Unknown template '%s', using demo-title fallback", template)
    return build_demo_title_payload(job)


# -------------------------------------------------------------------
# SHOTSTACK CALL
# -------------------------------------------------------------------


def submit_to_shotstack(payload: Dict[str, Any]) -> str:
    """Send render request to Shotstack and return render_id."""

    url = SHOTSTACK_API_URL
    logger.info("Submitting render to Shotstack: %s", url)
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=60)

    # Log raw response for debugging
    logger.info("Shotstack response [%s]: %s", resp.status_code, resp.text)

    resp.raise_for_status()

    data = resp.json()
    render_id = data["response"]["id"]
    return render_id


# -------------------------------------------------------------------
# JOB PROCESSING
# -------------------------------------------------------------------


def process_job(job_id: str, job: Dict[str, Any]) -> None:
    logger.info("Processing job %s: %s", job_id, job)

    # Mark as processing
    update_fields = {
        "status": "processing",
    }
    logger.info("Updating job %s with %s", job_id, update_fields)
    update_job(job_id, update_fields)

    # Add event: worker picked job
    event = {
        "type": "processing",
        "message": "Worker picked up job",
    }
    logger.info("Adding event for job %s: %s", job_id, event)
    add_event(job_id, event)

    # Build Shotstack payload from template
    payload = build_shotstack_payload(job)

    try:
        render_id = submit_to_shotstack(payload)
    except Exception as exc:
        logger.error("Error processing job %s: %s", job_id, exc)

        # Put back to pending & record error for retry
        update_fields = {
            "status": "pending",
            "metadata.error": str(exc),
            "metadata.retry_count": job.get("metadata", {}).get("retry_count", 0) + 1,
        }
        logger.info("Updating job %s with %s", job_id, update_fields)
        update_job(job_id, update_fields)

        error_event = {
            "type": "error",
            "message": f"Error processing job, status set to 'pending'. Error: {exc}",
        }
        logger.info("Adding event for job %s: %s", job_id, error_event)
        add_event(job_id, error_event)
        return

    # Success – save render_id & mark as “rendering”
    metadata_update = job.get("metadata", {}).copy()
    metadata_update["render_id"] = render_id
    metadata_update["status"] = "rendering"

    update_fields = {
        "status": "rendering",
        "metadata": metadata_update,
    }
    logger.info("✅ Job %s submitted to Shotstack, render_id=%s", job_id, render_id)
    update_job(job_id, update_fields)

    event = {
        "type": "render_submitted",
        "message": f"Render submitted to Shotstack with id {render_id}",
    }
    logger.info("Adding event for job %s: %s", job_id, event)
    add_event(job_id, event)


# -------------------------------------------------------------------
# MAIN LOOP
# -------------------------------------------------------------------


def main() -> None:
    logger.info("🚀 Starting Cre8 Firebase + Shotstack worker...")

    while True:
        jobs: List[Tuple[str, Dict[str, Any]]] = get_pending_jobs(limit=5)

        if not jobs:
            logger.info("No pending jobs found. Sleeping 15 seconds...")
            time.sleep(15)
            continue

        logger.info("Found %d pending job(s).", len(jobs))

        for job_id, job in jobs:
            process_job(job_id, job)

        # Short pause between batches
        time.sleep(5)


if __name__ == "__main__":
    main()
