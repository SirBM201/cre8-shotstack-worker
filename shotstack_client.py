# shotstack_client.py
import logging
import os
from typing import Any, Dict

import requests

logger = logging.getLogger(__name__)

SHOTSTACK_API_KEY = os.getenv("SHOTSTACK_API_KEY")
SHOTSTACK_ENV = os.getenv("SHOTSTACK_ENV", "stage")  # "stage" or "production"

BASE_URL = f"https://api.shotstack.io/{SHOTSTACK_ENV}"


def submit_render(payload: Dict[str, Any]) -> str:
    """
    Submit a render job to Shotstack and return the render_id.
    """
    if not SHOTSTACK_API_KEY:
        raise RuntimeError("SHOTSTACK_API_KEY is not set")

    url = f"{BASE_URL}/render"
    headers = {
        "x-api-key": SHOTSTACK_API_KEY,
        "content-type": "application/json",
    }

    logger.info("Submitting render to Shotstack: %s", url)
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    logger.info("Shotstack response [%s]: %s", resp.status_code, data)

    render_id = data["response"]["id"]
    return render_id


def get_render_status(render_id: str) -> Dict[str, Any]:
    """
    Check the status of a Shotstack render by ID.

    Returns:
        {
          "status": "queued|fetching|rendering|done|failed|... ",
          "url": "https://...mp4 or None",
          "raw": {... full Shotstack response ...}
        }
    """
    if not SHOTSTACK_API_KEY:
        raise RuntimeError("SHOTSTACK_API_KEY is not set")

    url = f"{BASE_URL}/render/{render_id}"
    headers = {
        "x-api-key": SHOTSTACK_API_KEY,
        "content-type": "application/json",
    }

    logger.info("Checking Shotstack status for render_id=%s", render_id)
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    response = data.get("response", {})
    status = response.get("status")
    output_url = response.get("url")  # final mp4 URL

    logger.info("Shotstack status for %s: %s, url=%s", render_id, status, output_url)

    return {
        "status": status,
        "url": output_url,
        "raw": response,
    }
