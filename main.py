import os
import time
import logging
import requests
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv

from firebase_client import get_pending_jobs, update_job, add_event

# ----------------------------------------
# ENV + LOGGING
# ----------------------------------------

load_dotenv()

SHOTSTACK_API_URL = os.getenv(
    "SHOTSTACK_API_URL",
    "https://api.shotstack.io/stage/render"
)

SHOTSTACK_API_KEY = os.getenv("SHOTSTACK_API_KEY")

if not SHOTSTACK_API_KEY:
    raise RuntimeError("SHOTSTACK_API_KEY is not set.")

HEADERS = {
    "x-api-key": SHOTSTACK_API_KEY,
    "Content-Type": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("cre8-shotstack-worker")

# ----------------------------------------
# VALID EFFECTS
# ----------------------------------------

VALID_EFFECTS = {
    "none", "zoomIn", "zoomOut", "slideLeft", "slideRight",
    "slideUp", "slideDown", "zoomInSlow", "zoomOutSlow"
}

# ----------------------------------------
# TEMPLATE SYSTEM
# ----------------------------------------

def build_demo_title_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    asset_cfg = job.get("asset", {}) or {}

    text = asset_cfg.get("text", "CRE8 STUDIO TEST RENDER")
    style = "minimal"
    length = 5
    effect = asset_cfg.get("effect", "zoomIn")

    if effect not in VALID_EFFECTS:
        effect = "zoomIn"

    return {
        "timeline": {
            "tracks": [
                {
                    "clips": [
                        {
                            "asset": {
                                "type": "title",
                                "text": text,
                                "style": style
                            },
                            "start": 0,
                            "length": length,
                            "effect": effect
                        }
                    ]
                }
            ]
        },
        "output": {
            "format": "mp4",
            "resolution": "hd",
            "aspectRatio": "16:9"
        }
    }

def build_shotstack_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    template = job.get("template", "demo-title")

    if template == "demo-title":
        return build_demo_title_payload(job)

    logger.warning("Unknown template '%s', fallback to demo-title", template)
    return build_demo_title_payload(job)

# ----------------------------------------
# SHOTSTACK SUBMISSION
# ----------------------------------------

def submit_to_shotstack(payload: Dict[str, Any]) -> str:
    logger.info("Submitting render to Shotstack...")
    resp = requests.post(
        SHOTSTACK_API_URL,
        headers=HEADERS,
        json=payload,
        timeout=60
    )

    logger.info("Shotstack response [%s]: %s", resp.status_code, resp.text)

    resp.raise_for_status()

    data = resp.json()
    return data["response"]["id"]

# ----------------------------------------
# PROCESS SINGLE JOB
# ----------------------------------------

def process_job(job_id: str, job: Dict[str, Any]) -> None:
    logger.info("Processing job %s", job_id)

    update_job(job_id, {"status": "processing"})

    add_event(job_id, {
        "type": "processing",
        "message": "Worker picked up job"
    })

    payload = build_shotstack_payload(job)

    try:
        render_id = submit_to_shotstack(payload)
    except Exception as exc:
        logger.error("Render failed: %s", exc)

        update_job(job_id, {
            "status": "pending",
            "error": str(exc)
        })

        add_event(job_id, {
            "type": "error",
            "message": str(exc)
        })
        return

    update_job(job_id, {
        "status": "rendering",
        "metadata": {
            "render_id": render_id,
            "status": "rendering"
        }
    })

    add_event(job_id, {
        "type": "render_submitted",
        "message": f"Render ID {render_id} submitted"
    })

    logger.info("✅ Job %s submitted successfully", job_id)

# ----------------------------------------
# MAIN LOOP (PRODUCTION SAFE)
# ----------------------------------------

def main() -> None:
    logger.info("🚀 Cre8 Shotstack Worker LIVE")

    while True:
        jobs: List[Tuple[str, Dict[str, Any]]] = get_pending_jobs(limit=5)

        if not jobs:
            logger.info("No jobs found. Sleeping 15s...")
            time.sleep(15)
            continue

        logger.info("Found %d pending job(s)", len(jobs))

        for job_id, job in jobs:
            process_job(job_id, job)

        time.sleep(5)

if __name__ == "__main__":
    main()
